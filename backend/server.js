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
const MIG_TP = 1.80;              // +80%
const MIG_SL = 0.82;              // -18%
const MIG_DURATION_MS = 15 * 60 * 1000; // 15 min — más tiempo para el movimiento
const MIG_WINDOW_MS = 60_000;
const MIG_MIN_VOL_FAST = 2_000;
const MIG_MIN_VOL_SLOW = 5_000;
const MIG_FAST_WINDOW_MS = 20_000;
const MIG_MIN_MC = 50_000;
const MIG_MAX_MC = 2_000_000;
const MIG_BREAKEVEN_AT = 0.22;    // breakeven a +22%
const MIG_BREAKEVEN_MARGIN = 0.03; // SL en breakeven = entrada -3%
const MIG_LOCK_AT = 0.20;         // following a +20%
const MIG_FOLLOW_PCT = 0.20;      // trailing -20%
const MIG_MAX_PRICE_RATIO = 2.0;
const MIG_SL_CONFIRM_TICKS = 2;   // v6.7: nº de ticks consecutivos bajo el SL inicial necesarios para cerrar. Un tick basura aislado (DUR 0-2s, -47% imposible) no se confirma; una caída real sí. Solo aplica al SL bajo entrada, no al piso/trailing en positivo
const MIG_EXPIRED_WIN_PCT = 2;
const MIG_ENTRY_DELAY_MS = 3_000; // delay 3s antes de entrar
// ── NUEVO v6.2: escalón de beneficio (de v8.4) ─────────────────
const MIG_STEP_TRIGGER = 0.20;    // al tocar +20% de beneficio...
const MIG_STEP_FLOOR = 0.13;      // ...asegurar piso de +13%
const MIG_FOLLOW_PCT_STEP = 0.15; // v6.5: trailing ceñido (-15%) cuando el escalón está armado (maxGain>=+20%). El trailing supera el piso +13% a partir de +15.3% (vs +41% con -20%), capturando las medianas-altas +25-40% que antes se cortaban en +13%

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
const MOM_MIN_LIQUIDITY = 25_000;     // v6.4: liquidez mínima del pool en USD (reserve_in_usd). Filtra pools finos tipo SPCX (vol alto, liquidez <$1)
const MOM_MUTE_TIMEOUT_MS = 90_000;   // v6.4: 90s sin un solo movimiento => feed mudo, cancelar y liberar capital
const MOM_HARD_CAP_LOSS = -10;        // v6.8: tope de pérdida duro (%). Si currentPct <= esto, cerrar YA. Red de seguridad para caídas verticales en pools ilíquidos donde el SL -3% no se ejecuta porque el precio salta por encima del nivel entre ticks. Corta un -62% a ~-10%
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
// v6.6: scan de momentum por Birdeye (API key, no sufre el 429 de IP compartida de Gecko)
const BIRDEYE_API_KEY = "cffc98f5aed04ad3ae4115c5e900ddbd";
const BIRDEYE_TOKEN_LIST = "https://public-api.birdeye.so/defi/v3/token/list";

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
    mig_demoWins: 0, mig_demoLosses: 0, mig_demoExpired: 0, mig_demoPnL: 0,
    mig_realWins: 0, mig_realLosses: 0, mig_realPnL: 0, mig_realPnLSol: 0,
    mig_closedCount: 0, mig_maxGainSum: 0, mig_maxLossSum: 0,
    mig_avgMaxGain: 0, mig_avgMaxLoss: 0,
    mom_scanned: 0, mom_signals: 0, mom_pending: 0,
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

// ── calcPrice (v6.0 intacto): supply FIJO 1e9 ──────────────────
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

// ── NUEVO v6.2: SOL price real (de v8.4) ───────────────────────
// CoinGecko + respaldo Jupiter. solPriceReady evita operar sin precio real.
// NOTA: cambiar SOL de 150 fijo a precio real NO altera el trading (TP/SL son
// % sobre el precio de entrada, que escala igual con cualquier SOL); solo hace
// que los MC mostrados en logs sean reales.
let solPriceUSD = 68;          // default realista; se actualiza por API
let solPriceReady = false;     // no operar hasta tener un precio real de SOL
async function updateSolPrice() {
  // Intento 1: CoinGecko
  try {
    const r = await fetch(
      "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
      { signal: AbortSignal.timeout(8000) });
    const d = await r.json();
    if (d?.solana?.usd > 0) {
      solPriceUSD = d.solana.usd;
      solPriceReady = true;
      return;
    }
  } catch (e) {
    addLog(`⚠️ SOL price CoinGecko falló: ${e.message}`, "warn");
  }
  // Intento 2 (respaldo): Jupiter price API
  try {
    const r = await fetch(
      "https://price.jup.ag/v6/price?ids=SOL",
      { signal: AbortSignal.timeout(8000) });
    const d = await r.json();
    const px = d?.data?.SOL?.price;
    if (px > 0) {
      solPriceUSD = px;
      solPriceReady = true;
      addLog(`ℹ️ SOL price vía Jupiter (respaldo): $${px}`, "info");
      return;
    }
  } catch (e) {
    addLog(`⚠️ SOL price Jupiter también falló: ${e.message}`, "warn");
  }
  // Ambas fallaron: mantener el valor anterior y AVISAR
  addLog(`⚠️ SOL price sin actualizar, sigo con $${solPriceUSD}`, "warn");
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
  // ── NUEVO v6.2: no operar hasta tener precio real de SOL ──
  if (!solPriceReady) {
    addLog("⏳ Esperando precio real de SOL antes de operar", "warn");
    return;
  }
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
  if (!isPriceValid(price, entry.lastPrice)) return;
  entry.volumeUSD += solAmount * solPriceUSD;
  entry.tradeCount++;
  entry.lastPrice = price;
  if (!entry.firstPrice && price > 0) entry.firstPrice = price;
  const elapsed = Date.now() - entry.startTime;

  // ── Entrada rápida: $2K en <20s ───────────────────────────
  if (elapsed < MIG_FAST_WINDOW_MS && entry.volumeUSD >= MIG_MIN_VOL_FAST) {
    clearTimeout(entry.timer);
    entry.entered = true;
    state.migWatching.delete(mint);
    addLog(`⚡ MIG RÁPIDA: ${entry.symbol} | $${Math.round(entry.volumeUSD)} en ${(elapsed/1000).toFixed(1)}s — delay 3s`, "accept");
    state.stats.mig_entered++;
    broadcast({ event: "stats", data: state.stats });
    // ── Delay 3s antes de entrar ──────────────────────────
    setTimeout(() => {
      entry.firstPrice = entry.lastPrice;
      addLog(`⚡ MIG ENTRADA: ${entry.symbol} @ MC ${formatMC(entry.lastPrice * 1_000_000_000)}`, "accept");
      migOpenTrades(entry);
    }, MIG_ENTRY_DELAY_MS);
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
    addLog(`✅ MIG LENTA: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol | ${elapsed}s — delay 3s`, "accept");
    state.stats.mig_entered++;
    broadcast({ event: "stats", data: state.stats });
    // ── Delay 3s antes de entrar ──────────────────────────
    setTimeout(() => {
      entry.firstPrice = entry.lastPrice;
      addLog(`✅ MIG ENTRADA: ${entry.symbol} @ MC ${formatMC(entry.lastPrice * 1_000_000_000)}`, "accept");
      migOpenTrades(entry);
    }, MIG_ENTRY_DELAY_MS);
  } else {
    unsubscribeToken(mint);
    addLog(`❌ MIG RECHAZADO: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol en ${elapsed}s`, "filter");
    state.stats.mig_rejected++;
    broadcast({ event: "stats", data: state.stats });
  }
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
// ESTRATEGIA 2: MOMENTUM (v6.0 intacto: Gecko + subida 10-30%, TP +6%)
// ════════════════════════════════════════════════════════════════

async function momentumScan() {
  seenMomPools.clear();
  let totalScanned = 0;
  let totalSignals = 0;
  try {
    // v6.6: una sola llamada a Birdeye (Token List V3) ordenada por volumen 1h.
    // Sustituye las 3 páginas de Gecko (que daban 429 por IP compartida de Railway).
    // Birdeye identifica por API key, no por IP. limit:50 = 1 petición por scan.
    const url = `${BIRDEYE_TOKEN_LIST}?sort_by=volume_1h_usd&sort_type=desc`
      + `&min_liquidity=${MOM_MIN_LIQUIDITY}`
      + `&min_market_cap=${MOM_MIN_MC}&max_market_cap=${MOM_MAX_MC}`
      + `&offset=0&limit=50`;
    const res = await fetch(url, {
      headers: { "accept": "application/json", "x-chain": "solana", "X-API-KEY": BIRDEYE_API_KEY },
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) {
      addLog(`❌ Birdeye scan HTTP ${res.status}`, "error");
      return;
    }
    const json = await res.json();
    const tokens = json?.data?.items || json?.data?.tokens || [];
    for (const tok of tokens) {
      const mint = tok.address || "";
      if (!mint || mint.length < 32) continue;
      const poolAddr = mint; // en Birdeye agrupamos por token (no por pool)
      const mc = parseFloat(tok.market_cap || tok.fdv || 0);
      if (mc < MOM_MIN_MC || mc > MOM_MAX_MC) continue;
      const vol1h = parseFloat(tok.volume_1h_usd || 0);
      const pct1h = parseFloat(tok.price_change_1h_percent || 0);
      const bePrice = parseFloat(tok.price || 0);
      if (bePrice <= 0) continue;
      const liquidity = parseFloat(tok.liquidity || 0);
      totalScanned++;
      if (state.momMonitored.has(mint)) {
        const token = state.momMonitored.get(mint);
        token.vol1h = vol1h; token.pct1h = pct1h;
        if (bePrice > 0) momUpdatePrice(mint, bePrice, 0);
        seenMomPools.add(poolAddr);
        continue;
      }
      if (seenMomPools.has(poolAddr)) continue;
      if (vol1h < MOM_MIN_VOL_1H) continue;
      // ── v6.4 Fix 1: filtro de liquidez mínima (descarta pools finos tipo SPCX) ──
      // liquidity > 0 evita falso 0 si el campo faltara puntualmente: solo
      // descartamos cuando Birdeye reporta liquidez real y baja.
      if (liquidity > 0 && liquidity < MOM_MIN_LIQUIDITY) {
        addLog(`⛔ MOM liquidez baja (${formatMC(liquidity)}): ${tok.symbol || mint.slice(0,8)}`, "filter");
        continue;
      }
      if (pct1h < MOM_MIN_PCT_1H) continue;
      if (pct1h > MOM_MAX_PCT_1H) continue;
      const lastSig = momSignalCooldown.get(mint) || 0;
      if (Date.now() - lastSig < MOM_SIGNAL_COOLDOWN_MS) continue;
      seenMomPools.add(poolAddr);
      state.stats.mom_scanned++;
      const symbol = tok.symbol || mint.slice(0, 8);
      momSignalCooldown.set(mint, Date.now());
      state.stats.mom_signals++;
      totalSignals++;
      state.momPending.set(mint, {
        mint, symbol, name: symbol,
        geckoPrice: bePrice, mc, vol1h, pct1h,
        pendingSince: Date.now(),
      });
      state.stats.mom_pending = state.momPending.size;
      addLog(`⚡ MOMENTUM: ${symbol} | +${pct1h.toFixed(1)}% 1h | Vol ${formatMC(vol1h)} | MC ${formatMC(mc)}`, "signal");
      setTimeout(() => {
        if (state.momPending.has(mint)) {
          const pending = state.momPending.get(mint);
          addLog(`⚡ ENTRADA birdeye: ${pending.symbol} @ $${pending.geckoPrice.toFixed(8)}`, "accept");
          momActivateFromPending(mint, pending.geckoPrice, 0);
        }
      }, MOM_PENDING_TIMEOUT_MS);
      broadcast({ event: "stats", data: state.stats });
    }
    addLog(`⚡ Scan: ${totalScanned} candidatos, ${totalSignals} señales nuevas`, "info");
    broadcast({ event: "stats", data: state.stats });
  } catch (e) {
    addLog(`❌ Momentum scan error: ${e.message}`, "error");
  }
}

function momActivateFromPending(mint, entryPrice, solAmount) {
  const pending = state.momPending.get(mint);
  if (!pending) return;
  state.momPending.delete(mint);
  state.stats.mom_pending = state.momPending.size;
  const signal = {
    id: `mom-${mint}-${Date.now()}`,
    strategy: "momentum",
    mint, name: pending.name, symbol: pending.symbol,
    price: entryPrice,
    tp: +(entryPrice * MOM_TP).toFixed(12),
    sl: +(entryPrice * MOM_SL).toFixed(12),
    mcUsd: pending.mc, vol1h: pending.vol1h, pct1h: pending.pct1h,
    time: Date.now(),
  };
  const source = solAmount > 0 ? "real" : "gecko";
  addLog(`⚡ ENTRADA [${source}]: ${pending.symbol} @ $${entryPrice.toFixed(8)} | TP +6% SL -3% | 45min`, "accept");
  state.signals.unshift(signal);
  if (state.signals.length > 100) state.signals.pop();
  broadcast({ event: "newSignal", data: signal });
  state.momMonitored.set(mint, {
    mint, symbol: pending.symbol, name: pending.name,
    mc: pending.mc, price: entryPrice,
    priceHigh: entryPrice, priceLow: entryPrice,
    pct1h: pending.pct1h, vol1h: pending.vol1h,
    tradeCount: solAmount > 0 ? 1 : 0,
    volumeUSD: solAmount * solPriceUSD,
    detectedAt: Date.now(), lastUpdate: Date.now(),
  });
  broadcast({ event: "newMomToken", data: state.momMonitored.get(mint) });
  openDemoTrade(signal);
  openRealTrade(signal);
}

function momUpdatePrice(mint, price, solAmount) {
  if (state.momPending.has(mint) && solAmount > 0) {
    momActivateFromPending(mint, price, solAmount);
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

function momCleanup(mint, symbol) {
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
  // ── v6.2: STEP cuenta como WIN (igual que TP) ──
  if (reason === "TP" || reason === "STEP" || (reason === "SL" && trade.pnlPct >= 0)) {
    trade.result = "WIN"; state.stats[`${prefix}_realWins`]++;
    state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
    const tag = reason === "STEP" ? "🪜 ESCALÓN" : trade.strategy;
    addLog(`✅ REAL WIN [${tag}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "realwin");
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
  if (trade.strategy === "momentum") momCleanup(trade.mint, trade.symbol);
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
    // ── v6.8: cap loss duro (solo Momentum) ──
    if (strategy === "momentum" && currentPct <= MOM_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [momentum real]: ${trade.symbol} ${currentPct.toFixed(1)}%`, "realloss");
      closeRealTrade(trade, price, "SL");
      continue;
    }
    const gainPct = (price - trade.entryPrice) / trade.entryPrice;
    // ── v6.5: trailing ceñido cuando el escalón está armado ──
    // Si la migración ya tocó +20%, el trailing pasa de -20% a -15% para que
    // supere el piso +13% antes (a partir de +15.3% de MAX en vez de +41%) y
    // capture las medianas-altas. Para el resto, trailing normal.
    const stepArmed = strategy === "migration" && trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9;
    const followEff = (strategy === "migration" && stepArmed) ? MIG_FOLLOW_PCT_STEP : follow;
    if (trade.trailingPhase === "FOLLOWING") {
      const newSl = price * (1 - followEff);
      if (newSl > trade.sl) trade.sl = +newSl.toFixed(12);
    } else if (gainPct >= lock) {
      trade.trailingPhase = "FOLLOWING";
      trade.sl = +Math.max(trade.sl, price * (1 - followEff)).toFixed(12);
    } else if (gainPct >= breakeven && trade.trailingPhase === "INITIAL") {
      trade.trailingPhase = "BREAKEVEN";
      trade.sl = +(trade.entryPrice * (1 - breakevenMargin)).toFixed(12);
    }
    // ── v6.3: escalón como SUELO, no override (solo migración) ──
    // El stop NUNCA baja de +13% una vez armado, pero el escalón NO secuestra
    // la fase: FOLLOWING sigue actualizando el trailing arriba. Aquí solo
    // elevamos el suelo a +13% si el trailing ceñido aún no lo superó.
    const stepFloorPrice = trade.entryPrice * (1 + MIG_STEP_FLOOR);
    if (stepArmed && stepFloorPrice > trade.sl) {
      trade.sl = +stepFloorPrice.toFixed(12);
    }
    if (price >= trade.tp) { trade._slBelowCount = 0; closeRealTrade(trade, price, "TP"); }
    else if (price <= trade.sl) {
      const stopProtegeGanancia = trade.sl >= trade.entryPrice;
      if (stopProtegeGanancia) {
        // Piso o trailing en positivo: cierre inmediato (sin filtro de tick).
        const reason = (stepArmed && Math.abs(trade.sl - stepFloorPrice) < 1e-9) ? "STEP" : "SL";
        closeRealTrade(trade, price, reason);
      } else {
        // ── v6.7: SL inicial bajo entrada → confirmación de 2 ticks ──
        trade._slBelowCount = (trade._slBelowCount || 0) + 1;
        if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS) {
          closeRealTrade(trade, price, "SL");
        } else {
          addLog(`⏳ SL sin confirmar [real] (${trade._slBelowCount}/${MIG_SL_CONFIRM_TICKS}): ${trade.symbol}`, "trail");
          broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
        }
      }
    }
    else { trade._slBelowCount = 0;
      if (now >= trade.expiresAt) closeRealTrade(trade, price, "EXPIRED");
      else broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
    }
  }
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.realTrades) {
    if (trade.status !== "OPEN") continue;
    // ── v6.4 Fix 2: cancelar Momentum real con feed mudo ──
    if (trade.strategy === "momentum") {
      const aliveMs = now - trade.openTime;
      const sinMovimiento = (trade.maxGainPct === 0 && trade.maxLossPct === 0);
      if (aliveMs >= MOM_MUTE_TIMEOUT_MS && sinMovimiento) {
        const tk = state.momMonitored.get(trade.mint);
        addLog(`🔇 MOM FEED MUDO [real]: ${trade.symbol} — sin trades en ${Math.round(aliveMs/1000)}s, cancelando`, "warn");
        closeRealTrade(trade, tk?.price || trade.entryPrice, "EXPIRED");
        continue;
      }
    }
    if (now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint) || state.momMonitored.get(trade.mint);
    closeRealTrade(trade, token?.price || trade.entryPrice, "EXPIRED");
  }
}, 30_000);

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
    // ── v6.8: cap loss duro (solo Momentum) ──
    // Red ADICIONAL para caídas verticales en pools ilíquidos donde el SL -3%
    // no se ejecuta (el precio salta por encima del nivel entre ticks). Cierre
    // INMEDIATO al precio actual. reason="SL" => cuenta como LOSS (pnl <= -10%).
    if (strategy === "momentum" && currentPct <= MOM_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [momentum]: ${trade.symbol} ${currentPct.toFixed(1)}% (tope ${MOM_HARD_CAP_LOSS}%)`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct);
      continue;
    }
    const gainPct = (price - trade.entryPrice) / trade.entryPrice;
    // ── v6.5: trailing ceñido (-15%) cuando el escalón está armado ──
    const stepArmed = strategy === "migration" && trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9;
    const followEff = (strategy === "migration" && stepArmed) ? MIG_FOLLOW_PCT_STEP : follow;
    if (trade.trailingPhase === "FOLLOWING") {
      const newSl = price * (1 - followEff);
      if (newSl > trade.sl) trade.sl = +newSl.toFixed(12);
    } else if (gainPct >= lock) {
      trade.trailingPhase = "FOLLOWING";
      trade.sl = +Math.max(trade.sl, price * (1 - followEff)).toFixed(12);
      addLog(`🔄 FOLLOWING [${strategy}]: ${trade.symbol}`, "trail");
    } else if (gainPct >= breakeven && trade.trailingPhase === "INITIAL") {
      trade.trailingPhase = "BREAKEVEN";
      trade.sl = +(trade.entryPrice * (1 - breakevenMargin)).toFixed(12);
      addLog(`⚖️ BREAKEVEN [${strategy}]: ${trade.symbol}`, "trail");
    }
    // ── v6.3: escalón como SUELO, no override (solo migración) ──
    // Si tocó +20%, el stop NUNCA baja de +13%, pero el escalón NO secuestra
    // la fase: FOLLOWING sigue subiendo el trailing (ahora ceñido -15%). Aquí
    // solo elevamos el suelo a +13% si el trailing aún no lo superó.
    const stepFloorPrice = trade.entryPrice * (1 + MIG_STEP_FLOOR);
    if (stepArmed && stepFloorPrice > trade.sl) {
      if (!trade._stepLogged) {
        trade._stepLogged = true;
        addLog(`🪜 ESCALÓN +13% suelo [${strategy}]: ${trade.symbol} (tocó +${trade.maxGainPct.toFixed(0)}%)`, "trail");
      }
      trade.sl = +stepFloorPrice.toFixed(12);
    }
    if (price >= trade.tp) { trade._slBelowCount = 0; closeDemoTrade(trade, price, "TP", tp_pct); }
    else if (price <= trade.sl) {
      const stopProtegeGanancia = trade.sl >= trade.entryPrice;
      if (stopProtegeGanancia) {
        // Piso o trailing en positivo: cierre inmediato al nivel del stop.
        // Aquí NO hay tick basura que filtrar y retrasar perdería ganancia.
        const reason = (stepArmed && Math.abs(trade.sl - stepFloorPrice) < 1e-9) ? "STEP" : "SL";
        closeDemoTrade(trade, trade.sl, reason, tp_pct);
      } else {
        // ── v6.7: SL inicial bajo entrada → confirmación de 2 ticks ──
        // Un tick basura aislado (-47% imposible en DUR 0-2s) no se confirma:
        // el siguiente tick vuelve arriba y reseteamos. Una caída real sí
        // confirma (dos ticks consecutivos bajo el SL) y cierra a precio real.
        trade._slBelowCount = (trade._slBelowCount || 0) + 1;
        if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS) {
          closeDemoTrade(trade, price, "SL", tp_pct);
        } else {
          addLog(`⏳ SL sin confirmar (${trade._slBelowCount}/${MIG_SL_CONFIRM_TICKS}): ${trade.symbol} @ ${(trade.currentPct||0).toFixed(1)}%`, "trail");
          broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
        }
      }
    }
    else { trade._slBelowCount = 0;
      if (now >= trade.expiresAt) closeDemoTrade(trade, price, "EXPIRED", tp_pct);
      else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
    }
  }
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
  } else if (reason === "STEP") {
    // ── NUEVO v6.2: cierre por el piso del escalón (+13%): siempre WIN ──
    trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++;
    state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
    addLog(`✅ WIN [🪜 ESCALÓN][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win");
  } else if (reason === "SL") {
    state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
    // v6.0 contaba pnl>=0 como WIN. Mantengo v6.0 aquí (incluye 0% plano como WIN).
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
  if (trade.strategy === "momentum") momCleanup(trade.mint, trade.symbol);
  broadcast({ event: "demoTradeClosed", data: trade });
  broadcast({ event: "stats", data: state.stats });
  saveState();
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.status !== "OPEN") continue;
    // ── v6.4 Fix 2: cancelar Momentum con feed mudo ──
    // Si tras MOM_MUTE_TIMEOUT_MS el precio nunca se movió (max y min en 0),
    // el feed de PumpPortal no manda trades para ese pool: cancelar y liberar
    // capital en vez de esperar a los 45 min. Cierra a precio de entrada (P&L 0).
    if (trade.strategy === "momentum") {
      const aliveMs = now - trade.openTime;
      const sinMovimiento = (trade.maxGainPct === 0 && trade.maxLossPct === 0);
      if (aliveMs >= MOM_MUTE_TIMEOUT_MS && sinMovimiento) {
        const tk = state.momMonitored.get(trade.mint);
        addLog(`🔇 MOM FEED MUDO: ${trade.symbol} — sin trades en ${Math.round(aliveMs/1000)}s, cancelando`, "warn");
        closeDemoTrade(trade, tk?.price || trade.entryPrice, "EXPIRED", MOM_TP);
        continue;
      }
    }
    // Expiración normal (45 min)
    if (now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint) || state.momMonitored.get(trade.mint);
    const tp_pct = trade.strategy === "migration" ? MIG_TP : MOM_TP;
    closeDemoTrade(trade, token?.price || trade.entryPrice, "EXPIRED", tp_pct);
  }
}, 30_000);

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
        if (state.momPending.has(data.mint)) momActivateFromPending(data.mint, price, sol);
      }
    } catch (e) { console.log("PP:", e.message); }
  });
  pumpPortalWs.on("error", (err) => addLog(`❌ PumpPortal: ${err.message}`, "error"));
  pumpPortalWs.on("close", () => { addLog("🔄 PumpPortal reconectando...", "warn"); setTimeout(connectPumpPortal, 5000); });
}

// ── HELIUS — desactivado ───────────────────────────────────────
function connectHelius() {
  addLog("ℹ️ Helius desactivado — precios via PumpPortal + Gecko", "info");
}

// ── EXPRESS + WS ───────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

// ── MOVIMIENTOS MANUALES (de v6.1) ─────────────────────────────
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
  console.log(`🚀 SolScanBot v6.8 — v6.7 + cap loss duro momentum (-10%) para caídas verticales: filtro liquidez ${MOM_MIN_LIQUIDITY/1000}K + cancelar feed mudo ${MOM_MUTE_TIMEOUT_MS/1000}s`);
  loadState();
  initWallet();
  connectPumpPortal();
  connectHelius();
  setTimeout(momentumScan, 5000);
  setInterval(momentumScan, MOM_SCAN_MS);
});
