import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import cors from "cors";
import fetch from "node-fetch";
import fs from "fs";
import {
 Connection,
 Keypair,
 PublicKey,
 VersionedTransaction,
 LAMPORTS_PER_SOL,
} from "@solana/web3.js";
import bs58 from "bs58";

const PORT = process.env.PORT || 3001;
const SOL_PER_TRADE = 0.05;
const MAX_REAL_TRADES = 0;
const STATE_FILE = "/tmp/solscanbot_state.json";

// ── CONFIG MIGRACIÓN ───────────────────────────────────────────
const MIG_TP = 1.80;
const MIG_SL = 0.82;
const MIG_DURATION_MS = 15 * 60 * 1000;
const MIG_WINDOW_MS = 60_000;
const MIG_MIN_VOL_FAST = 2_000;
const MIG_MIN_VOL_SLOW = 5_000;
const MIG_FAST_WINDOW_MS = 20_000;
const MIG_MIN_MC = 50_000;
const MIG_MAX_MC = 2_000_000;
const MIG_BREAKEVEN_AT = 0.22;
const MIG_BREAKEVEN_MARGIN = 0.03;
const MIG_LOCK_AT = 0.20;
const MIG_FOLLOW_PCT = 0.20;
const MIG_MAX_PRICE_RATIO = 5.0;   // Fix 1: relajado de 2x a 5x para capturar caídas
const MIG_EXPIRED_WIN_PCT = 2;
const MIG_ENTRY_DELAY_MS = 3_000;
const MIG_MAX_DROP_IN_DELAY = 0.05;
const MIG_MAX_DROP_VERTICAL = 0.15;

// ── CONFIG MOMENTUM ────────────────────────────────────────────
const MOM_TP = 1.06;
const MOM_SL = 0.97;
const MOM_DURATION_MS = 45 * 60 * 1000;
const MOM_MIN_PCT_1H = 10;
const MOM_MAX_PCT_1H = 30;
const MOM_MIN_VOL_1H = 100_000;
const MOM_MIN_MC = 100_000;
const MOM_MAX_MC = 1_000_000;
const MOM_SCAN_MS = 30_000;
const MOM_BREAKEVEN_AT = 0.03;
const MOM_LOCK_AT = 0.05;
const MOM_FOLLOW_PCT = 0.02;
const MOM_PENDING_TIMEOUT_MS = 30_000;
const MOM_SIGNAL_COOLDOWN_MS = 3 * 60 * 1000;
const MOM_EXPIRED_WIN_PCT = 2;

const HELIUS_API_KEY = "86268796-07db-4bab-8e4f-abc4f697f64d";
const HELIUS_RPC = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data?api-key=e12mybvnahb5cx2uahup8y1rahn4ewbp99rn4j2u6h6mmy37f1c7cdakf5432kbkcctmmwkcdd37cgke718qey9ne96mpy1mdncmjmut6crkeeb5f5n7ac1gf137auudd56m4u1tcwyku6h130u3m9164cdad99rmuxjpd8b9qq4d3bddu76wu7ad270k2h7155gnbm5x0kuf8";
const GECKO_PUMPSWAP = "https://api.geckoterminal.com/api/v2/networks/solana/dexes/pumpswap/pools";

let wallet = null;
let connection = null;
let pumpPortalWs = null;

function initWallet() {
 try {
   const privateKeyStr = process.env.WALLET_PRIVATE_KEY;
   if (!privateKeyStr) { addLog("⚠️ Sin WALLET_PRIVATE_KEY — modo demo", "warn"); return; }
   const privateKeyBytes = bs58.decode(privateKeyStr);
   wallet = Keypair.fromSecretKey(privateKeyBytes);
   connection = new Connection(HELIUS_RPC, "confirmed");
   addLog(`✅ Wallet: ${wallet.publicKey.toString()}`, "info");
 } catch (e) { addLog(`❌ Wallet error: ${e.message}`, "error"); }
}

async function getWalletBalance() {
 if (!wallet || !connection) return 0;
 try { return (await connection.getBalance(wallet.publicKey)) / LAMPORTS_PER_SOL; } catch { return 0; }
}

async function getTokenBalance(mint) {
 if (!wallet || !connection) return 0;
 try {
   const accounts = await connection.getParsedTokenAccountsByOwner(wallet.publicKey, { mint: new PublicKey(mint) });
   if (!accounts.value.length) return 0;
   return accounts.value[0].account.data.parsed.info.tokenAmount.uiAmount || 0;
 } catch { return 0; }
}

const state = {
 migWatching: new Map(),
 migMonitored: new Map(),
 momPending: new Map(),
 momMonitored: new Map(),
 signals: [],
 demoTrades: [],
 realTrades: [],
 movements: [],
 log: [],
 stats: {
   mig_migrations: 0, mig_watched: 0, mig_entered: 0, mig_rejected: 0,
   mig_aborted: 0,
   mig_demoWins: 0, mig_demoLosses: 0, mig_demoExpired: 0, mig_demoPnL: 0,
   mig_realWins: 0, mig_realLosses: 0, mig_realPnL: 0, mig_realPnLSol: 0,
   mig_closedCount: 0, mig_maxGainSum: 0, mig_maxLossSum: 0,
   mig_avgMaxGain: 0, mig_avgMaxLoss: 0,
   mom_scanned: 0, mom_signals: 0, mom_pending: 0, mom_cancelled: 0,
   mom_demoWins: 0, mom_demoLosses: 0, mom_demoExpired: 0, mom_demoPnL: 0,
   mom_realWins: 0, mom_realLosses: 0, mom_realPnL: 0, mom_realPnLSol: 0,
   mom_closedCount: 0, mom_maxGainSum: 0, mom_maxLossSum: 0,
   mom_avgMaxGain: 0, mom_avgMaxLoss: 0,
   demoOpen: 0, realOpen: 0, walletBalance: 0,
 },
};

function serializeMigWatching() {
 return Array.from(state.migWatching.values()).map(w => ({
   mint: w.mint, symbol: w.symbol, name: w.name,
   volumeUSD: w.volumeUSD, tradeCount: w.tradeCount,
   migratedMcUsd: w.migratedMcUsd,
   timeLeft: Math.max(0, MIG_WINDOW_MS - (Date.now() - w.startTime)),
 }));
}

function saveState() {
 try {
   fs.writeFileSync(STATE_FILE, JSON.stringify({
     demoTrades: state.demoTrades,
     realTrades: state.realTrades,
     movements: state.movements,
     stats: state.stats,
   }));
 } catch (e) { console.log("Error guardando estado:", e.message); }
}

function loadState() {
 try {
   if (!fs.existsSync(STATE_FILE)) return;
   const saved = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
   if (saved.demoTrades) state.demoTrades = saved.demoTrades;
   if (saved.realTrades) state.realTrades = saved.realTrades;
   if (saved.movements) state.movements = saved.movements;
   if (saved.stats) state.stats = { ...state.stats, ...saved.stats };
   for (const trade of state.demoTrades) {
     if (trade.status === "OPEN" || trade.status === "CLOSING") {
       trade.status = "CLOSED"; trade.result = "EXPIRED";
       trade.pnlPct = trade.pnlPct || 0; trade.closeTime = Date.now();
     }
   }
   for (const trade of state.realTrades) {
     if (trade.status === "OPEN" || trade.status === "CLOSING") {
       trade.status = "CLOSED"; trade.result = "EXPIRED";
       trade.pnlPct = trade.pnlPct || 0; trade.closeTime = Date.now();
     }
   }
   state.stats.demoOpen = 0; state.stats.realOpen = 0;
   addLog(`✅ Estado cargado: ${state.demoTrades.length} demo, ${state.realTrades.length} real`, "info");
 } catch (e) { addLog(`⚠️ Error cargando estado: ${e.message}`, "warn"); }
}

const frontendClients = new Set();
const seenMigMints = new Set();
const seenMomPools = new Set();
const momSignalCooldown = new Map();

function addLog(msg, type = "info") {
 const entry = { msg, type, time: Date.now() };
 state.log.unshift(entry);
 if (state.log.length > 300) state.log.pop();
 broadcast({ event: "log", data: entry });
 console.log(`[${type.toUpperCase()}] ${msg}`);
}

function broadcast(payload) {
 const str = JSON.stringify(payload);
 for (const c of frontendClients) {
   if (c.readyState === WebSocket.OPEN) c.send(str);
 }
}

function shortAddr(a) { return a ? `${a.slice(0,4)}…${a.slice(-4)}` : "—"; }
function formatMC(n) {
 if (!n) return "$0";
 if (n >= 1_000_000) return `$${(n/1_000_000).toFixed(2)}M`;
 if (n >= 1_000) return `$${(n/1_000).toFixed(1)}K`;
 return `$${Math.round(n)}`;
}

function unsubscribeToken(mint) {
 if (pumpPortalWs?.readyState === WebSocket.OPEN) {
   pumpPortalWs.send(JSON.stringify({ method: "unsubscribeTokenTrade", keys: [mint] }));
 }
}

function calcPrice(data) {
 if (data.marketCapSol && data.marketCapSol > 0) {
   return (data.marketCapSol * solPriceUSD) / 1_000_000_000;
 }
 const sol = data.solAmount || 0;
 const tok = data.tokenAmount || 0;
 if (sol > 0 && tok > 0) return (sol / tok) * solPriceUSD;
 return 0;
}

function isPriceValid(newPrice, knownPrice) {
 if (!knownPrice || knownPrice === 0) return newPrice > 0;
 const ratio = newPrice / knownPrice;
 return ratio >= (1 / MIG_MAX_PRICE_RATIO) && ratio <= MIG_MAX_PRICE_RATIO;
}

let solPriceUSD = 150;
async function updateSolPrice() {
 try {
   const r = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd");
   const d = await r.json();
   solPriceUSD = d?.solana?.usd ?? 150;
 } catch {}
}
setInterval(updateSolPrice, 60_000);
updateSolPrice();

setInterval(async () => {
 if (wallet) {
   state.stats.walletBalance = await getWalletBalance();
   broadcast({ event: "stats", data: state.stats });
 }
}, 30_000);

// ════════════════════════════════════════════════════════════════
// ESTRATEGIA 1: SNIPER DE MIGRACIÓN
// ════════════════════════════════════════════════════════════════

function migStartWatching(coin) {
 if (seenMigMints.has(coin.mint)) return;
 seenMigMints.add(coin.mint);
 state.stats.mig_migrations++;
 const mcUsd = (coin.marketCapSol || 0) * solPriceUSD;
 if (mcUsd > 0 && (mcUsd < MIG_MIN_MC || mcUsd > MIG_MAX_MC)) {
   addLog(`⛔ MIG MC fuera rango (${formatMC(mcUsd)}): ${coin.symbol}`, "filter");
   broadcast({ event: "stats", data: state.stats });
   return;
 }
 const entry = {
   mint: coin.mint, name: coin.name || "Unknown", symbol: coin.symbol || "???",
   startTime: Date.now(), migratedMcUsd: mcUsd,
   volumeUSD: 0, tradeCount: 0, firstPrice: null, lastPrice: null,
   priceHistory: [],
   timer: null, entered: false,
 };
 state.migWatching.set(coin.mint, entry);
 state.stats.mig_watched++;
 broadcast({ event: "stats", data: state.stats });
 addLog(`🌉 MIGRACIÓN: ${coin.symbol} | MC ${mcUsd > 0 ? formatMC(mcUsd) : "?"} — ${MIG_WINDOW_MS/1000}s`, "accept");
 if (pumpPortalWs?.readyState === WebSocket.OPEN) {
   pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [coin.mint] }));
 }
 entry.timer = setTimeout(() => migEvaluate(coin.mint), MIG_WINDOW_MS);
}

function migUpdateWatching(mint, price, solAmount, entry) {
 if (entry.entered) return;

 // ── Fix 1: guardar en historial ANTES de filtrar ───────────
 if (price > 0) {
   entry.priceHistory.push({ price, time: Date.now() });
   if (entry.priceHistory.length > 30) entry.priceHistory.shift();
 }

 // Filtro anti-glitch extremo (5x en vez de 2x)
 if (!isPriceValid(price, entry.lastPrice)) return;

 entry.volumeUSD += solAmount * solPriceUSD;
 entry.tradeCount++;
 entry.lastPrice = price;
 if (!entry.firstPrice && price > 0) entry.firstPrice = price;
 const elapsed = Date.now() - entry.startTime;

 if (elapsed < MIG_FAST_WINDOW_MS && entry.volumeUSD >= MIG_MIN_VOL_FAST) {
   clearTimeout(entry.timer);
   entry.entered = true;
   state.migWatching.delete(mint);
   addLog(`⚡ MIG RÁPIDA: ${entry.symbol} | $${Math.round(entry.volumeUSD)} en ${(elapsed/1000).toFixed(1)}s — validando...`, "accept");
   state.stats.mig_entered++;
   broadcast({ event: "stats", data: state.stats });
   migValidateAndEnter(entry);
   return;
 }

 broadcast({ event: "migWatchUpdate", data: {
   mint, symbol: entry.symbol, volumeUSD: entry.volumeUSD,
   tradeCount: entry.tradeCount,
   needed: elapsed < MIG_FAST_WINDOW_MS ? MIG_MIN_VOL_FAST : MIG_MIN_VOL_SLOW,
   timeLeft: Math.max(0, MIG_WINDOW_MS - elapsed),
   mc: price * 1_000_000_000,
 }});
}

function migEvaluate(mint) {
 const entry = state.migWatching.get(mint);
 if (!entry || entry.entered) return;
 state.migWatching.delete(mint);
 const elapsed = ((Date.now() - entry.startTime) / 1000).toFixed(1);
 if (entry.volumeUSD >= MIG_MIN_VOL_SLOW && entry.lastPrice) {
   addLog(`✅ MIG LENTA: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol | ${elapsed}s — validando...`, "accept");
   state.stats.mig_entered++;
   broadcast({ event: "stats", data: state.stats });
   migValidateAndEnter(entry);
 } else {
   unsubscribeToken(mint);
   addLog(`❌ MIG RECHAZADO: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol en ${elapsed}s`, "filter");
   state.stats.mig_rejected++;
   broadcast({ event: "stats", data: state.stats });
 }
}

// ── VALIDACIÓN COMPLETA ANTES DE ENTRAR ────────────────────────
function migValidateAndEnter(entry) {
 const priceAtTrigger = entry.lastPrice;

 setTimeout(() => {
   const now = Date.now();
   const priceNow = entry.lastPrice || priceAtTrigger;

   // ── Check 1: caída durante el delay ───────────────────────
   if (priceAtTrigger > 0 && priceNow > 0) {
     const dropInDelay = (priceAtTrigger - priceNow) / priceAtTrigger;
     if (dropInDelay > MIG_MAX_DROP_IN_DELAY) {
       addLog(`⛔ MIG ABORTADA [delay]: ${entry.symbol} cayó ${(dropInDelay*100).toFixed(1)}% en 3s`, "filter");
       state.stats.mig_aborted++;
       unsubscribeToken(entry.mint);
       broadcast({ event: "stats", data: state.stats });
       return;
     }
   }

   // ── Fix 2: check vertical vs MÁXIMO reciente (no oldest) ──
   const recent3s = entry.priceHistory.filter(p => now - p.time <= 3000);
   if (recent3s.length >= 2) {
     const maxRecent = Math.max(...recent3s.map(p => p.price));
     const newest = recent3s[recent3s.length - 1].price;
     const verticalDrop = (maxRecent - newest) / maxRecent;
     if (verticalDrop > MIG_MAX_DROP_VERTICAL) {
       addLog(`⛔ MIG ABORTADA [vertical]: ${entry.symbol} colapso ${(verticalDrop*100).toFixed(1)}% desde pico`, "filter");
       state.stats.mig_aborted++;
       unsubscribeToken(entry.mint);
       broadcast({ event: "stats", data: state.stats });
       return;
     }
   }

   // ── Fix 3: exigir señal alcista — vela de entrada verde ───
   const lookback5s = entry.priceHistory.filter(p => now - p.time <= 5000);
   if (lookback5s.length >= 2) {
     const priceAgo = lookback5s[0].price;
     const priceNowLB = lookback5s[lookback5s.length - 1].price;
     const trend = (priceNowLB - priceAgo) / priceAgo;
     if (trend < 0) {
       addLog(`⛔ MIG ABORTADA [bajista]: ${entry.symbol} tendencia ${(trend*100).toFixed(1)}% en 5s`, "filter");
       state.stats.mig_aborted++;
       unsubscribeToken(entry.mint);
       broadcast({ event: "stats", data: state.stats });
       return;
     }
   }

   // ── Todo OK — entrar ──────────────────────────────────────
   entry.firstPrice = priceNow;
   addLog(`✅ MIG ENTRADA VALIDADA: ${entry.symbol} @ MC ${formatMC(priceNow * 1_000_000_000)}`, "accept");
   migOpenTrades(entry);

 }, MIG_ENTRY_DELAY_MS);
}

function migOpenTrades(entry) {
 const price = entry.firstPrice;
 if (!price || price <= 0) return;
 const signal = {
   id: `mig-${entry.mint}-${Date.now()}`,
   strategy: "migration",
   mint: entry.mint, name: entry.name, symbol: entry.symbol,
   price, tp: +(price * MIG_TP).toFixed(12), sl: +(price * MIG_SL).toFixed(12),
   mcUsd: price * 1_000_000_000, volumeUSD: entry.volumeUSD, time: Date.now(),
 };
 state.signals.unshift(signal);
 if (state.signals.length > 100) state.signals.pop();
 broadcast({ event: "newSignal", data: signal });
 if (!state.migMonitored.has(entry.mint)) {
   state.migMonitored.set(entry.mint, {
     mint: entry.mint, name: entry.name, symbol: entry.symbol,
     price, mc: price * 1_000_000_000,
     priceHigh: price, priceLow: price,
     tradeCount: entry.tradeCount, volumeUSD: entry.volumeUSD,
     detectedAt: entry.startTime, lastUpdate: Date.now(),
   });
   broadcast({ event: "newMigToken", data: state.migMonitored.get(entry.mint) });
 }
 openDemoTrade(signal);
 openRealTrade(signal);
}

function migCleanup(mint, symbol) {
 unsubscribeToken(mint);
 state.migMonitored.delete(mint);
 broadcast({ event: "removeToken", data: { mint } });
 addLog(`🗑️ ${symbol} eliminado`, "info");
}

function migUpdatePrice(mint, price, solAmount) {
 const entry = state.migWatching.get(mint);
 if (entry) { migUpdateWatching(mint, price, solAmount, entry); return; }
 const token = state.migMonitored.get(mint);
 if (!token) return;
 if (!isPriceValid(price, token.price)) return;
 token.price = price; token.mc = price * 1_000_000_000;
 token.priceHigh = Math.max(token.priceHigh, price);
 token.priceLow = Math.min(token.priceLow, price);
 token.tradeCount++; token.volumeUSD += solAmount * solPriceUSD;
 token.lastUpdate = Date.now();
 updateDemoTrades(mint, price, "migration");
 updateRealTrades(mint, price, "migration");
 broadcast({ event: "migTokenUpdate", data: token });
}

// ════════════════════════════════════════════════════════════════
// ESTRATEGIA 2: MOMENTUM
// ════════════════════════════════════════════════════════════════

async function momentumScan() {
 seenMomPools.clear();
 let totalScanned = 0;
 let totalSignals = 0;
 try {
   // ── Fix momentum 2: 2 páginas + retry backoff en 429 ──────
   for (let page = 1; page <= 2; page++) {
     let res;
     let attempts = 0;
     while (attempts < 3) {
       try {
         res = await fetch(
           `${GECKO_PUMPSWAP}?page=${page}&order=h24_volume_usd_desc`,
           { headers: { "Accept": "application/json;version=20230302" }, signal: AbortSignal.timeout(10000) }
         );
         if (res.status === 429) {
           attempts++;
           const wait = 2000 * attempts;
           addLog(`⏳ Gecko 429 (pág ${page}) — esperando ${wait/1000}s`, "warn");
           await new Promise(r => setTimeout(r, wait));
           continue;
         }
         break;
       } catch (e) {
         attempts++;
         await new Promise(r => setTimeout(r, 2000));
       }
     }
     if (!res || !res.ok) { addLog(`⚠️ Gecko pág ${page} falló — saltando`, "warn"); continue; }

     const json = await res.json();
     const pools = json?.data || [];
     for (const pool of pools) {
       const attr = pool.attributes || {};
       const poolAddr = attr.address || pool.id?.replace("solana_", "");
       if (!poolAddr) continue;
       const mc = parseFloat(attr.fdv_usd || 0);
       if (mc < MOM_MIN_MC || mc > MOM_MAX_MC) continue;
       const vol1h = parseFloat(attr.volume_usd?.h1 || 0);
       const pct1h = parseFloat(attr.price_change_percentage?.h1 || 0);
       const geckoPrice = parseFloat(attr.base_token_price_usd || 0);
       if (geckoPrice <= 0) continue;
       const relationships = pool.relationships || {};
       const mint = (relationships.base_token?.data?.id || "").replace("solana_", "");
       if (!mint || mint.length < 32) continue;
       totalScanned++;
       if (state.momMonitored.has(mint)) {
         const token = state.momMonitored.get(mint);
         token.vol1h = vol1h; token.pct1h = pct1h;
         if (geckoPrice > 0) momUpdatePrice(mint, geckoPrice, 0);
         seenMomPools.add(poolAddr);
         continue;
       }
       if (seenMomPools.has(poolAddr)) continue;
       if (vol1h < MOM_MIN_VOL_1H) continue;
       if (pct1h < MOM_MIN_PCT_1H) continue;
       if (pct1h > MOM_MAX_PCT_1H) continue;
       const lastSig = momSignalCooldown.get(mint) || 0;
       if (Date.now() - lastSig < MOM_SIGNAL_COOLDOWN_MS) continue;
       seenMomPools.add(poolAddr);
       state.stats.mom_scanned++;
       const symbol = (attr.name || "").split(" / ")[0] || mint.slice(0, 8);
       momSignalCooldown.set(mint, Date.now());
       state.stats.mom_signals++;
       totalSignals++;

       state.momPending.set(mint, { mint, symbol, name: symbol, geckoPrice, mc, vol1h, pct1h, pendingSince: Date.now() });
       state.stats.mom_pending = state.momPending.size;

       // Suscribir a PumpPortal para recibir precio real
       if (pumpPortalWs?.readyState === WebSocket.OPEN) {
         pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
       }

       addLog(`⚡ MOMENTUM: ${symbol} | +${pct1h.toFixed(1)}% 1h | Vol ${formatMC(vol1h)} | MC ${formatMC(mc)}`, "signal");

       // ── Fix momentum 1: cancelar si no llega precio real en 30s ──
       setTimeout(() => {
         if (state.momPending.has(mint)) {
           const pending = state.momPending.get(mint);
           addLog(`⛔ MOM CANCELADA [timeout]: ${pending.symbol} — sin precio real en 30s`, "filter");
           state.momPending.delete(mint);
           state.stats.mom_pending = state.momPending.size;
           state.stats.mom_cancelled++;
           unsubscribeToken(mint);
           broadcast({ event: "stats", data: state.stats });
         }
       }, MOM_PENDING_TIMEOUT_MS);

       broadcast({ event: "stats", data: state.stats });
     }
     await new Promise(r => setTimeout(r, 500));
   }
   addLog(`⚡ Scan: ${totalScanned} candidatos, ${totalSignals} señales nuevas`, "info");
   broadcast({ event: "stats", data: state.stats });
 } catch (e) { addLog(`❌ Momentum scan error: ${e.message}`, "error"); }
}

function momActivateFromPending(mint, entryPrice, solAmount) {
 // Solo activar con precio real de PumpPortal (solAmount > 0)
 if (solAmount <= 0) return;
 const pending = state.momPending.get(mint);
 if (!pending) return;
 state.momPending.delete(mint);
 state.stats.mom_pending = state.momPending.size;
 const signal = {
   id: `mom-${mint}-${Date.now()}`, strategy: "momentum",
   mint, name: pending.name, symbol: pending.symbol,
   price: entryPrice, tp: +(entryPrice * MOM_TP).toFixed(12), sl: +(entryPrice * MOM_SL).toFixed(12),
   mcUsd: pending.mc, vol1h: pending.vol1h, pct1h: pending.pct1h, time: Date.now(),
 };
 addLog(`⚡ ENTRADA [real]: ${pending.symbol} @ $${entryPrice.toFixed(8)} | TP +6% SL -3%`, "accept");
 state.signals.unshift(signal);
 if (state.signals.length > 100) state.signals.pop();
 broadcast({ event: "newSignal", data: signal });
 state.momMonitored.set(mint, {
   mint, symbol: pending.symbol, name: pending.name, mc: pending.mc, price: entryPrice,
   priceHigh: entryPrice, priceLow: entryPrice, pct1h: pending.pct1h, vol1h: pending.vol1h,
   tradeCount: 1, volumeUSD: solAmount * solPriceUSD,
   detectedAt: Date.now(), lastUpdate: Date.now(),
 });
 broadcast({ event: "newMomToken", data: state.momMonitored.get(mint) });
 openDemoTrade(signal);
 openRealTrade(signal);
}

function momUpdatePrice(mint, price, solAmount) {
 if (state.momPending.has(mint)) {
   // Solo activar con precio real (solAmount > 0)
   if (solAmount > 0) momActivateFromPending(mint, price, solAmount);
   return;
 }
 const token = state.momMonitored.get(mint);
 if (!token) return;
 token.price = price; token.mc = price * 1_000_000_000;
 token.priceHigh = Math.max(token.priceHigh, price);
 token.priceLow = Math.min(token.priceLow, price);
 if (solAmount > 0) { token.tradeCount++; token.volumeUSD += solAmount * solPriceUSD; }
 token.lastUpdate = Date.now();
 updateDemoTrades(mint, price, "momentum");
 updateRealTrades(mint, price, "momentum");
 broadcast({ event: "momTokenUpdate", data: token });
}

function momCleanup(mint) {
 state.momMonitored.delete(mint);
 broadcast({ event: "removeToken", data: { mint } });
}

// ════════════════════════════════════════════════════════════════
// TRADING COMPARTIDO
// ════════════════════════════════════════════════════════════════

async function buyToken(mint, solAmount) {
 if (!wallet || !connection) return null;
 try {
   const response = await fetch("https://pumpportal.fun/api/trade-local", {
     method: "POST", headers: { "Content-Type": "application/json" },
     body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "buy", mint, denominatedInSol: "true", amount: solAmount, slippage: 15, priorityFee: 0.0005, pool: "pump" }),
     signal: AbortSignal.timeout(10000),
   });
   if (!response.ok) { addLog(`❌ Compra error: ${response.status}`, "error"); return null; }
   const tx = VersionedTransaction.deserialize(new Uint8Array(await response.arrayBuffer()));
   tx.sign([wallet]);
   const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: "confirmed" });
   await connection.confirmTransaction(sig, "confirmed");
   addLog(`✅ COMPRA: ${shortAddr(mint)} | ${sig}`, "real");
   return sig;
 } catch (e) { addLog(`❌ Compra: ${e.message}`, "error"); return null; }
}

async function sellToken(mint) {
 if (!wallet || !connection) return null;
 try {
   const bal = await getTokenBalance(mint);
   if (bal <= 0) { addLog(`⚠️ Sin tokens: ${shortAddr(mint)}`, "warn"); return null; }
   const response = await fetch("https://pumpportal.fun/api/trade-local", {
     method: "POST", headers: { "Content-Type": "application/json" },
     body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "sell", mint, denominatedInSol: "false", amount: bal, slippage: 15, priorityFee: 0.0005, pool: "pump" }),
     signal: AbortSignal.timeout(10000),
   });
   if (!response.ok) { addLog(`❌ Venta error: ${response.status}`, "error"); return null; }
   const tx = VersionedTransaction.deserialize(new Uint8Array(await response.arrayBuffer()));
   tx.sign([wallet]);
   const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: "confirmed" });
   await connection.confirmTransaction(sig, "confirmed");
   addLog(`✅ VENTA: ${shortAddr(mint)} | ${sig}`, "real");
   return sig;
 } catch (e) { addLog(`❌ Venta: ${e.message}`, "error"); return null; }
}

async function openRealTrade(signal) {
 if (!wallet) return;
 const openReal = state.realTrades.filter(t => t.status === "OPEN");
 const stratOpen = openReal.filter(t => t.strategy === signal.strategy).length;
 if (stratOpen >= 1) { addLog(`⚠️ Ya hay real abierta (${signal.strategy})`, "warn"); return; }
 if (openReal.length >= MAX_REAL_TRADES) return;
 const balance = await getWalletBalance();
 if (balance < SOL_PER_TRADE + 0.01) { addLog(`⚠️ Balance insuficiente: ${balance.toFixed(3)} SOL`, "warn"); return; }
 const sig = await buyToken(signal.mint, SOL_PER_TRADE);
 if (!sig) return;
 const duration = signal.strategy === "migration" ? MIG_DURATION_MS : MOM_DURATION_MS;
 const trade = {
   id: `real-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
   strategy: signal.strategy, mint: signal.mint, symbol: signal.symbol, name: signal.name,
   entryPrice: signal.price, tp: signal.tp, sl: signal.sl, solAmount: SOL_PER_TRADE,
   buySignature: sig, sellSignature: null,
   openTime: Date.now(), closeTime: null, closePrice: null,
   result: null, pnlPct: null, pnlSol: null,
   maxGainPct: 0, maxLossPct: 0, currentPct: 0,
   trailingPhase: "INITIAL", status: "OPEN",
   expiresAt: Date.now() + duration, sellRetries: 0,
 };
 state.realTrades.unshift(trade);
 if (state.realTrades.length > 200) state.realTrades.pop();
 state.stats.realOpen++;
 state.stats.walletBalance = await getWalletBalance();
 broadcast({ event: "newRealTrade", data: trade });
 broadcast({ event: "stats", data: state.stats });
 addLog(`🔴 REAL [${signal.strategy}]: ${signal.symbol} | ${SOL_PER_TRADE} SOL`, "real");
 saveState();
}

async function closeRealTrade(trade, price, reason) {
 if (trade.status !== "OPEN") return;
 trade.status = "CLOSING";
 const sig = await sellToken(trade.mint);
 if (!sig) {
   trade.sellRetries = (trade.sellRetries || 0) + 1;
   if (trade.sellRetries <= 3) { trade.status = "OPEN"; setTimeout(() => closeRealTrade(trade, price, reason), 15000); return; }
   trade.status = "SELL_FAILED"; broadcast({ event: "realTradeClosed", data: trade }); return;
 }
 trade.sellSignature = sig; trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
 const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
 trade.pnlPct = +pnlPct.toFixed(2);
 trade.pnlSol = +(trade.solAmount * pnlPct / 100).toFixed(4);
 const dur = Math.round((trade.closeTime - trade.openTime) / 1000);
 const prefix = trade.strategy === "migration" ? "mig" : "mom";
 const expWinPct = trade.strategy === "migration" ? MIG_EXPIRED_WIN_PCT : MOM_EXPIRED_WIN_PCT;
 if (reason === "TP" || (reason === "SL" && trade.pnlPct >= 0)) {
   trade.result = "WIN"; state.stats[`${prefix}_realWins`]++;
   state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
   addLog(`✅ REAL WIN [${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "realwin");
 } else if (reason === "SL") {
   trade.result = "LOSS"; state.stats[`${prefix}_realLosses`]++;
   state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
   addLog(`❌ REAL LOSS [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "realloss");
 } else {
   state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
   if (trade.pnlPct >= expWinPct) {
     trade.result = "WIN"; state.stats[`${prefix}_realWins`]++;
     addLog(`✅ REAL WIN [EXP+]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "realwin");
   } else {
     trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS";
     addLog(`⏱️ REAL EXP: ${trade.symbol} ${trade.pnlPct > 0 ? "+" : ""}${trade.pnlPct}%`, "real");
   }
 }
 state.stats.realOpen = Math.max(0, state.stats.realOpen - 1);
 state.stats.walletBalance = await getWalletBalance();
 if (trade.strategy === "migration") migCleanup(trade.mint, trade.symbol);
 if (trade.strategy === "momentum") momCleanup(trade.mint);
 broadcast({ event: "realTradeClosed", data: trade });
 broadcast({ event: "stats", data: state.stats });
 saveState();
}

function updateRealTrades(mint, price, strategy) {
 const now = Date.now();
 const breakeven = strategy === "migration" ? MIG_BREAKEVEN_AT : MOM_BREAKEVEN_AT;
 const breakevenMargin = strategy === "migration" ? MIG_BREAKEVEN_MARGIN : 0;
 const lock = strategy === "migration" ? MIG_LOCK_AT : MOM_LOCK_AT;
 const follow = strategy === "migration" ? MIG_FOLLOW_PCT : MOM_FOLLOW_PCT;
 for (const trade of state.realTrades) {
   if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
   const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
   trade.currentPct = +currentPct.toFixed(2);
   trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
   trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
   const gainPct = (price - trade.entryPrice) / trade.entryPrice;
   if (trade.trailingPhase === "FOLLOWING") {
     const newSl = price * (1 - follow);
     if (newSl > trade.sl) trade.sl = +newSl.toFixed(12);
   } else if (gainPct >= lock) {
     trade.trailingPhase = "FOLLOWING";
     trade.sl = +Math.max(trade.sl, price * (1 - follow)).toFixed(12);
   } else if (gainPct >= breakeven && trade.trailingPhase === "INITIAL") {
     trade.trailingPhase = "BREAKEVEN";
     trade.sl = +(trade.entryPrice * (1 - breakevenMargin)).toFixed(12);
   }
   if (price >= trade.tp) closeRealTrade(trade, price, "TP");
   else if (price <= trade.sl) closeRealTrade(trade, price, "SL");
   else if (now >= trade.expiresAt) closeRealTrade(trade, price, "EXPIRED");
   else broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
 }
}

function updateDemoTrades(mint, price, strategy) {
 const now = Date.now();
 const tp_pct = strategy === "migration" ? MIG_TP : MOM_TP;
 const breakeven = strategy === "migration" ? MIG_BREAKEVEN_AT : MOM_BREAKEVEN_AT;
 const breakevenMargin = strategy === "migration" ? MIG_BREAKEVEN_MARGIN : 0;
 const lock = strategy === "migration" ? MIG_LOCK_AT : MOM_LOCK_AT;
 const follow = strategy === "migration" ? MIG_FOLLOW_PCT : MOM_FOLLOW_PCT;
 for (const trade of state.demoTrades) {
   if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
   const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
   trade.currentPct = +currentPct.toFixed(2);
   trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
   trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
   const gainPct = (price - trade.entryPrice) / trade.entryPrice;
   if (trade.trailingPhase === "FOLLOWING") {
     const newSl = price * (1 - follow);
     if (newSl > trade.sl) trade.sl = +newSl.toFixed(12);
   } else if (gainPct >= lock) {
     trade.trailingPhase = "FOLLOWING";
     trade.sl = +Math.max(trade.sl, price * (1 - follow)).toFixed(12);
     addLog(`🔄 FOLLOWING [${strategy}]: ${trade.symbol}`, "trail");
   } else if (gainPct >= breakeven && trade.trailingPhase === "INITIAL") {
     trade.trailingPhase = "BREAKEVEN";
     trade.sl = +(trade.entryPrice * (1 - breakevenMargin)).toFixed(12);
     addLog(`⚖️ BREAKEVEN [${strategy}]: ${trade.symbol}`, "trail");
   }
   if (price >= trade.tp) closeDemoTrade(trade, price, "TP", tp_pct);
   else if (price <= trade.sl) closeDemoTrade(trade, price, "SL", tp_pct);
   else if (now >= trade.expiresAt) closeDemoTrade(trade, price, "EXPIRED", tp_pct);
   else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
 }
}

// ── SL en tiempo real cada 5s (Fuga B) ────────────────────────
setInterval(() => {
 const now = Date.now();
 for (const trade of state.realTrades) {
   if (trade.status !== "OPEN") continue;
   const token = state.migMonitored.get(trade.mint) || state.momMonitored.get(trade.mint);
   if (!token) continue;
   if (now >= trade.expiresAt) { closeRealTrade(trade, token.price, "EXPIRED"); continue; }
   if (token.price <= trade.sl) {
     addLog(`🚨 SL FORZADO [${trade.strategy}]: ${trade.symbol}`, "warn");
     closeRealTrade(trade, token.price, "SL");
   }
 }
 for (const trade of state.demoTrades) {
   if (trade.status !== "OPEN") continue;
   const token = state.migMonitored.get(trade.mint) || state.momMonitored.get(trade.mint);
   if (!token) continue;
   if (now >= trade.expiresAt) {
     const tp_pct = trade.strategy === "migration" ? MIG_TP : MOM_TP;
     closeDemoTrade(trade, token.price, "EXPIRED", tp_pct);
   }
 }
}, 5_000);

function openDemoTrade(signal) {
 const duration = signal.strategy === "migration" ? MIG_DURATION_MS : MOM_DURATION_MS;
 const trade = {
   id: `demo-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
   strategy: signal.strategy, mint: signal.mint, symbol: signal.symbol, name: signal.name,
   entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
   openTime: Date.now(), closeTime: null, closePrice: null,
   result: null, pnlPct: null, maxGainPct: 0, maxLossPct: 0, currentPct: 0,
   trailingPhase: "INITIAL", status: "OPEN",
   expiresAt: Date.now() + duration,
 };
 state.demoTrades.unshift(trade);
 if (state.demoTrades.length > 500) state.demoTrades.pop();
 state.stats.demoOpen++;
 broadcast({ event: "newDemoTrade", data: trade });
 broadcast({ event: "stats", data: state.stats });
 const tpPct = signal.strategy === "migration" ? "+80%" : "+6%";
 const slPct = signal.strategy === "migration" ? "-18%" : "-3%";
 addLog(`📝 DEMO [${signal.strategy}]: ${signal.symbol} | TP ${tpPct} SL ${slPct}`, "demo");
}

function closeDemoTrade(trade, price, reason, tp_pct) {
 trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
 const dur = Math.round((trade.closeTime - trade.openTime) / 1000);
 const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
 trade.pnlPct = +pnlPct.toFixed(2);
 const prefix = trade.strategy === "migration" ? "mig" : "mom";
 const expWinPct = trade.strategy === "migration" ? MIG_EXPIRED_WIN_PCT : MOM_EXPIRED_WIN_PCT;
 if (reason === "TP") {
   trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++;
   state.stats[`${prefix}_demoPnL`] += (tp_pct - 1) * 100;
   addLog(`✅ WIN [TP][${trade.strategy}]: ${trade.symbol} +${((tp_pct-1)*100).toFixed(0)}% en ${dur}s`, "win");
 } else if (reason === "SL") {
   state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
   if (trade.pnlPct >= 0) { trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++; addLog(`✅ WIN [${trade.trailingPhase}][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win"); }
   else { trade.result = "LOSS"; state.stats[`${prefix}_demoLosses`]++; addLog(`❌ LOSS [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "loss"); }
 } else {
   state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
   if (trade.pnlPct >= expWinPct) {
     trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++;
     addLog(`✅ WIN [EXP+][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win");
   } else if (trade.pnlPct <= -expWinPct) {
     trade.result = "LOSS"; state.stats[`${prefix}_demoLosses`]++;
     addLog(`❌ LOSS [EXP-][${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "loss");
   } else {
     trade.result = "EXPIRED"; state.stats[`${prefix}_demoExpired`]++;
     addLog(`⏱️ EXP [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct > 0 ? "+" : ""}${trade.pnlPct}%`, "expire");
   }
 }
 state.stats.demoOpen = Math.max(0, state.stats.demoOpen - 1);
 state.stats[`${prefix}_maxGainSum`] += trade.maxGainPct || 0;
 state.stats[`${prefix}_maxLossSum`] += Math.abs(trade.maxLossPct || 0);
 state.stats[`${prefix}_closedCount`]++;
 state.stats[`${prefix}_avgMaxGain`] = +(state.stats[`${prefix}_maxGainSum`] / state.stats[`${prefix}_closedCount`]).toFixed(1);
 state.stats[`${prefix}_avgMaxLoss`] = +(state.stats[`${prefix}_maxLossSum`] / state.stats[`${prefix}_closedCount`]).toFixed(1);
 if (trade.strategy === "migration") migCleanup(trade.mint, trade.symbol);
 if (trade.strategy === "momentum") momCleanup(trade.mint);
 broadcast({ event: "demoTradeClosed", data: trade });
 broadcast({ event: "stats", data: state.stats });
 saveState();
}

// ── PUMPPORTAL WS ──────────────────────────────────────────────
function connectPumpPortal() {
 addLog("🔌 Conectando a PumpPortal...", "info");
 pumpPortalWs = new WebSocket(PUMPPORTAL_WS);
 pumpPortalWs.on("open", () => {
   addLog("✅ PumpPortal conectado", "info");
   pumpPortalWs.send(JSON.stringify({ method: "subscribeMigration" }));
   for (const [mint] of state.migWatching.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
   for (const [mint] of state.migMonitored.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
   for (const [mint] of state.momPending.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
 });
 pumpPortalWs.on("message", async (raw) => {
   try {
     const data = JSON.parse(raw.toString());
     if (data.message || data.errors) return;
     if (data.txType === "migrate" && data.mint) {
       addLog(`🌉 Migración: ${data.symbol || data.mint.slice(0,8)} mcSol:${data.marketCapSol ?? "?"}`, "info");
       migStartWatching({ mint: data.mint, name: data.name || "Unknown", symbol: data.symbol || "???", marketCapSol: data.marketCapSol || 0 });
       return;
     }
     if ((data.txType === "buy" || data.txType === "sell") && data.mint) {
       const walletPubkey = wallet?.publicKey?.toString();
       if (walletPubkey && data.traderPublicKey === walletPubkey) return;
       const price = calcPrice(data);
       if (price <= 0) return;
       const sol = data.solAmount || 0;
       if (state.migWatching.has(data.mint) || state.migMonitored.has(data.mint)) migUpdatePrice(data.mint, price, sol);
       if (state.momPending.has(data.mint) || state.momMonitored.has(data.mint)) momUpdatePrice(data.mint, price, sol);
     }
   } catch (e) { console.log("PP:", e.message); }
 });
 pumpPortalWs.on("error", (err) => addLog(`❌ PumpPortal: ${err.message}`, "error"));
 pumpPortalWs.on("close", () => { addLog("🔄 PumpPortal reconectando...", "warn"); setTimeout(connectPumpPortal, 5000); });
}

function connectHelius() {
 addLog("ℹ️ Helius desactivado — precios via PumpPortal + Gecko", "info");
}

// ── EXPRESS + WS ───────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

app.post("/api/movement", (req, res) => {
 const { date, amount, type, note } = req.body;
 if (!date || amount === undefined || !type) return res.status(400).json({ error: "Faltan campos" });
 const movement = { id: `mov-${Date.now()}`, date, amount: parseFloat(amount), type, note: note || "", createdAt: Date.now() };
 state.movements.push(movement);
 saveState();
 broadcast({ event: "newMovement", data: movement });
 res.json({ ok: true, movement });
});

app.delete("/api/movement/:id", (req, res) => {
 state.movements = state.movements.filter(m => m.id !== req.params.id);
 saveState();
 broadcast({ event: "movementDeleted", data: { id: req.params.id } });
 res.json({ ok: true });
});

app.get("/api/state", (req, res) => {
 res.json({
   migWatching: serializeMigWatching(),
   migMonitored: Array.from(state.migMonitored.values()),
   momMonitored: Array.from(state.momMonitored.values()),
   signals: state.signals.slice(0, 50),
   demoTrades: state.demoTrades.slice(0, 200),
   realTrades: state.realTrades.slice(0, 200),
   movements: state.movements,
   log: state.log.slice(0, 100),
   stats: state.stats,
 });
});

const server = createServer(app);
const wss = new WebSocketServer({ server });
wss.on("connection", (ws) => {
 frontendClients.add(ws);
 ws.send(JSON.stringify({
   event: "fullState",
   data: {
     migWatching: serializeMigWatching(),
     migMonitored: Array.from(state.migMonitored.values()),
     momMonitored: Array.from(state.momMonitored.values()),
     signals: state.signals.slice(0, 50),
     demoTrades: state.demoTrades.slice(0, 200),
     realTrades: state.realTrades.slice(0, 200),
     movements: state.movements,
     log: state.log.slice(0, 100),
     stats: state.stats,
     wsStatus: "connected",
   }
 }));
 ws.on("close", () => frontendClients.delete(ws));
});

server.listen(PORT, () => {
 console.log(`🚀 SolScanBot v6.3 — 5 fixes: MIG validación entrada + MOM precio real + Gecko 429`);
 loadState();
 initWallet();
 connectPumpPortal();
 connectHelius();
 setTimeout(momentumScan, 5000);
 setInterval(momentumScan, MOM_SCAN_MS);
});
