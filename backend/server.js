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
const MIG_STEP_TRIGGER = 0.20;     // al tocar +20% de beneficio...
const MIG_STEP_FLOOR = 0.13;       // ...asegurar piso de +13%
const MIG_MAX_PRICE_RATIO = 1.5;   // saltos de escala residuales quedan filtrados
const MIG_REJECT_STREAK = 2;       // ticks consecutivos a la baja para aceptar caída real
const MIG_EXPIRED_WIN_PCT = 2;
const MIG_ENTRY_DELAY_MS = 3_000;
const MIG_MAX_DROP_IN_DELAY = 0.05;
const MIG_MAX_DROP_VERTICAL = 0.15;
const MIG_TREND_THRESHOLD = -0.03;  // tendencia bajista en 5s: tolera ruido hasta -3% (antes 0)
const MIG_COLLAPSE_DROP = 0.20;       // stop de colapso: caída >20% desde el pico reciente...
const MIG_COLLAPSE_WINDOW_MS = 4000;  // ...en <=4s -> cierre inmediato reason=COLLAPSE
const MIG_GRACE_MS = 10000;           // SL de gracia: primeros 10s de vida del trade...
const MIG_GRACE_SL = 0.75;            // ...permite caer hasta -25% (aguanta el lavado inicial)

// ── CONFIG MOMENTUM ────────────────────────────────────────────
const MOM_TP = 1.045;              // 1.06 -> 1.045: TP +4.5%, bajo el techo natural +4-6%
const MOM_SL = 0.97;
const MOM_DURATION_MS = 45 * 60 * 1000;
const MOM_MIN_PCT_1H = 10;        // (sin uso desde v8.2: la selección es por volumen, no por subida)
const MOM_MAX_PCT_1H = 30;        // (sin uso desde v8.2: ahora solo se descarta pct1h < -5)
const MOM_MIN_VOL_1H = 100_000;
const MOM_MIN_MC = 100_000;
const MOM_MAX_MC = 1_000_000;
const MOM_MAX_OPEN = 3;
const MOM_SCAN_MS = 30_000;
const MOM_BREAKEVEN_AT = 0.04;     // 0.03 -> 0.04: breakeven más tarde, fuera de la zona de ruido
const MOM_LOCK_AT = 0.035;         // 0.05 -> 0.035: trailing arranca antes del TP +4.5%
const MOM_FOLLOW_PCT = 0.03;       // 0.02 -> 0.03: trailing aguanta el microdip normal
const MOM_PENDING_TIMEOUT_MS = 30_000;
const MOM_SIGNAL_COOLDOWN_MS = 3 * 60 * 1000;
const MOM_EXPIRED_WIN_PCT = 2;
const MOM_LAST_TRADE_WINDOW_MS = 10 * 60 * 1000; // solo tokens con trade en últimos 10min
const MOM_CONFIRM_MS = 5000;       // ventana de confirmación de tendencia antes de entrar

// ── OBSERVACIÓN DE ABORTOS (instrumentación, no toca trading) ──
const ABORT_WATCH_MS = 5 * 60 * 1000;  // observar 5 min tras abortar
const ABORT_WIN_THRESHOLD = 0.20;      // +20% = "ganador perdido"

// ── OBSERVACIÓN POST-CIERRE (instrumentación, no toca trading) ──
// Misma maquinaria que los abortos pero aplicada a los CIERRES: tras cerrar
// un trade de migración, observamos 5 min el precio para detectar "ganadores
// perdidos" (cerró en pérdida pero el token despegó después).
const POST_CLOSE_WATCH_MS = 5 * 60 * 1000;   // observar 5 min tras cerrar
const POST_CLOSE_WIN_THRESHOLD = 0.20;       // +20% tras salir = ganador perdido

// ── APIs ───────────────────────────────────────────────────────
const HELIUS_API_KEY = "86268796-07db-4bab-8e4f-abc4f697f64d";
const HELIUS_RPC = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data?api-key=e12mybvnahb5cx2uahup8y1rahn4ewbp99rn4j2u6h6mmy37f1c7cdakf5432kbkcctmmwkcdd37cgke718qey9ne96mpy1mdncmjmut6crkeeb5f5n7ac1gf137auudd56m4u1tcwyku6h130u3m9164cdad99rmuxjpd8b9qq4d3bddu76wu7ad270k2h7155gnbm5x0kuf8";
const BIRDEYE_API_KEY = "cffc98f5aed04ad3ae4115c5e900ddbd";
const BIRDEYE_MEME_URL = "https://public-api.birdeye.so/defi/v3/token/meme/list";

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
  abortWatch: new Map(),   // mint -> {symbol, abortPrice, abortReason, abortTime}
  postCloseWatch: new Map(),  // mint -> {symbol, closePrice, closePnl, closeReason, closeTime, lastPrice}
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
    abort_correct: 0, abort_missed: 0,
    abort_correct_bajista: 0, abort_correct_vertical: 0, abort_correct_delay: 0,
    abort_missed_bajista: 0, abort_missed_vertical: 0, abort_missed_delay: 0,
    mig_collapse_count: 0, mig_collapse_pnlSum: 0,  // stop de colapso: nº disparos y suma de pnl%
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

// ── calcPrice: PRECIO DE MERCADO (marketCapSol/supply) ─────────
function calcPrice(data, knownSupply) {
  if (data.marketCapSol > 0 && knownSupply && knownSupply > 0) {
    return (data.marketCapSol * solPriceUSD) / knownSupply;
  }
  const sol = data.solAmount || 0;
  const tok = data.tokenAmount || 0;
  if (sol > 0 && tok > 0) {
    return (sol / tok) * solPriceUSD;
  }
  return 0;
}

function calibrateSupply(data) {
  const sol = data.solAmount || 0;
  const tok = data.tokenAmount || 0;
  if (data.marketCapSol > 0 && sol > 0 && tok > 0) {
    const supply = data.marketCapSol / (sol / tok);
    addLog(`🔎 CALIBRA SUPPLY: mcSol=${data.marketCapSol} sol=${sol} tok=${tok} => supply=${supply.toFixed(0)}`, "info");
    return supply;
  }
  return null;
}

function isPriceValid(newPrice, knownPrice) {
  if (!knownPrice || knownPrice === 0) return newPrice > 0;
  const ratio = newPrice / knownPrice;
  return ratio >= (1 / MIG_MAX_PRICE_RATIO) && ratio <= MIG_MAX_PRICE_RATIO;
}

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
    priceHistory: [], calSupply: null,
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
  if (price > 0) {
    entry.priceHistory.push({ price, time: Date.now() });
    if (entry.priceHistory.length > 30) entry.priceHistory.shift();
  }
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
    mc: price * (entry.calSupply || 1_000_000_000),
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

// Registra un aborto en observación (instrumentación pura). En vez de
// desuscribir y borrar al instante, mantiene el token 5 min recibiendo
// precio para medir si el filtro acertó o costó un ganador.
function registerAbort(mint, symbol, abortPrice, reason) {
  state.stats.mig_aborted++;
  state.abortWatch.set(mint, {
    symbol, abortPrice: abortPrice || 0, abortReason: reason,
    abortTime: Date.now(), lastPrice: abortPrice || 0,
  });
  addLog(`🔭 MIG ABORTO EN OBSERVACIÓN: ${symbol} @ ${reason} (vigilando 5 min)`, "filter");
  // NO desuscribir todavía: el token sigue recibiendo precio.
}

function migValidateAndEnter(entry) {
  const priceAtTrigger = entry.lastPrice;
  setTimeout(() => {
    const now = Date.now();
    const priceNow = entry.lastPrice || priceAtTrigger;
    if (priceAtTrigger > 0 && priceNow > 0) {
      const dropInDelay = (priceAtTrigger - priceNow) / priceAtTrigger;
      if (dropInDelay > MIG_MAX_DROP_IN_DELAY) {
        addLog(`⛔ MIG ABORTADA [delay]: ${entry.symbol} cayó ${(dropInDelay*100).toFixed(1)}% en 3s`, "filter");
        registerAbort(entry.mint, entry.symbol, priceNow, "delay");
        broadcast({ event: "stats", data: state.stats }); return;
      }
    }
    const recent3s = entry.priceHistory.filter(p => now - p.time <= 3000);
    if (recent3s.length >= 2) {
      const maxRecent = Math.max(...recent3s.map(p => p.price));
      const newest = recent3s[recent3s.length - 1].price;
      const verticalDrop = (maxRecent - newest) / maxRecent;
      if (verticalDrop > MIG_MAX_DROP_VERTICAL) {
        addLog(`⛔ MIG ABORTADA [vertical]: ${entry.symbol} colapso ${(verticalDrop*100).toFixed(1)}% desde pico`, "filter");
        registerAbort(entry.mint, entry.symbol, newest, "vertical");
        broadcast({ event: "stats", data: state.stats }); return;
      }
    }
    const lookback5s = entry.priceHistory.filter(p => now - p.time <= 5000);
    if (lookback5s.length >= 2) {
      const priceAgo = lookback5s[0].price;
      const priceNowLB = lookback5s[lookback5s.length - 1].price;
      const trend = (priceNowLB - priceAgo) / priceAgo;
      if (trend < MIG_TREND_THRESHOLD) {
        addLog(`⛔ MIG ABORTADA [bajista]: ${entry.symbol} tendencia ${(trend*100).toFixed(1)}% en 5s`, "filter");
        registerAbort(entry.mint, entry.symbol, priceNowLB, "bajista");
        broadcast({ event: "stats", data: state.stats }); return;
      }
    }
    const sorted = entry.priceHistory
      .slice()
      .sort((a, b) => b.time - a.time);
    const truePriceNow = sorted.length ? sorted[0].price : priceAtTrigger;

    entry.firstPrice = truePriceNow;
    addLog(`✅ MIG ENTRADA VALIDADA: ${entry.symbol} @ MC ${formatMC(truePriceNow * (entry.calSupply || 1_000_000_000))} (precio de mercado)`, "accept");
    migOpenTrades(entry);
  }, MIG_ENTRY_DELAY_MS);
}

function migOpenTrades(entry) {
  const price = entry.firstPrice;
  if (!price || price <= 0) return;
  const supply = entry.calSupply || 1_000_000_000;
  const signal = {
    id: `mig-${entry.mint}-${Date.now()}`, strategy: "migration",
    mint: entry.mint, name: entry.name, symbol: entry.symbol,
    price, tp: +(price * MIG_TP).toFixed(12), sl: +(price * MIG_SL).toFixed(12),
    mcUsd: price * supply, volumeUSD: entry.volumeUSD, time: Date.now(),
  };
  addLog(`🔎 MIG ENTRY DEBUG: ${entry.symbol} | entryPrice=$${price.toFixed(10)} | calSupply=${entry.calSupply ?? "NO-CALIBRADO(1e9)"} | mcCalc=${formatMC(price * supply)}`, "info");
  state.signals.unshift(signal);
  if (state.signals.length > 100) state.signals.pop();
  broadcast({ event: "newSignal", data: signal });
  if (!state.migMonitored.has(entry.mint)) {
    state.migMonitored.set(entry.mint, {
      mint: entry.mint, name: entry.name, symbol: entry.symbol,
      price, mc: price * supply, priceHigh: price, priceLow: price,
      calSupply: entry.calSupply, downRejectStreak: 0,
      tradeCount: entry.tradeCount, volumeUSD: entry.volumeUSD,
      detectedAt: entry.startTime, lastUpdate: Date.now(),
    });
    broadcast({ event: "newMigToken", data: state.migMonitored.get(entry.mint) });
  }
  openDemoTrade(signal);
  openRealTrade(signal);
}

function migCleanup(mint, symbol) {
  // Si el token está en observación post-cierre, NO desuscribir: necesitamos
  // que el precio siga llegando 5 min para medir el post5m (igual que abortos).
  if (!state.postCloseWatch.has(mint)) unsubscribeToken(mint);
  state.migMonitored.delete(mint);
  broadcast({ event: "removeToken", data: { mint } });
  addLog(`🗑️ ${symbol} eliminado`, "info");
}

// Registra un cierre de migración en observación (instrumentación pura).
// A los 5 min mide el precio para clasificar el cierre automáticamente:
// ganador perdido (subió +20% tras salir), muerto, o reversión normal.
function registerPostClose(mint, symbol, closePrice, closePnl, closeReason) {
  if (!closePrice || closePrice <= 0) return;
  state.postCloseWatch.set(mint, {
    symbol, closePrice, closePnl, closeReason,
    closeTime: Date.now(), lastPrice: closePrice,
  });
  // Mantener suscripción 5 min para recibir precio (la cancela evaluarPostCierres).
  if (pumpPortalWs?.readyState === WebSocket.OPEN) {
    pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
  }
}

function migUpdatePrice(mint, price, solAmount) {
  const entry = state.migWatching.get(mint);
  if (entry) { migUpdateWatching(mint, price, solAmount, entry); return; }
  const token = state.migMonitored.get(mint);
  if (!token) return;

  // ── Fix precio congelado (Fase 2) ─────────────────────
  if (!isPriceValid(price, token.price)) {
    if (price > 0 && price < token.price) {
      token.downRejectStreak = (token.downRejectStreak || 0) + 1;
      if (token.downRejectStreak < MIG_REJECT_STREAK) return;
      addLog(`⚠️ MIG caída real aceptada: ${token.symbol} (${token.downRejectStreak} ticks a la baja)`, "warn");
    } else {
      token.downRejectStreak = 0;
      return;
    }
  } else {
    token.downRejectStreak = 0;
  }

  const supply = token.calSupply || 1_000_000_000;
  token.price = price; token.mc = price * supply;
  token.priceHigh = Math.max(token.priceHigh, price);
  token.priceLow = Math.min(token.priceLow, price);
  token.tradeCount++; token.volumeUSD += solAmount * solPriceUSD;
  token.lastUpdate = Date.now();
  updateDemoTrades(mint, price, "migration");
  updateRealTrades(mint, price, "migration");
  broadcast({ event: "migTokenUpdate", data: token });
}

// ════════════════════════════════════════════════════════════════
// ESTRATEGIA 2: MOMENTUM — Birdeye Meme List (PumpSwap) + supply real
// ════════════════════════════════════════════════════════════════

async function momentumScan() {
  let totalScanned = 0;
  let totalSignals = 0;
  if (!solPriceReady) {
    addLog("⏳ Esperando precio real de SOL antes de escanear momentum", "warn");
    return;
  }
  try {
    const minLastTrade = Math.floor((Date.now() - MOM_LAST_TRADE_WINDOW_MS) / 1000);
    // Birdeye cuenta graduated + cada min/max como "filtro" y limita a 5 total.
    // En la query: graduated + 3 min/max (MC min, MC max, volumen) = 4.
    // El orden es por VOLUMEN 1h (estilo Gecko): selecciona tokens más líquidos
    // y con actividad sostenida, donde los stops ejecutan limpio. El criterio
    // anterior (price_change_1h) cogía los más extendidos y volátiles -> entradas
    // tardías y pérdidas grandes. min_last_trade se filtra en el código (abajo).
    const params = new URLSearchParams({
      sort_by: "volume_1h_usd",         // orden por volumen, no por subida
      sort_type: "desc",
      source: "pump_dot_fun",          // solo ecosistema pump.fun/PumpSwap
      graduated: "true",                // solo los ya migrados a PumpSwap
      offset: "0",
      limit: "50",
      min_volume_1h_usd: String(MOM_MIN_VOL_1H),  // el volumen es el filtro rey
      min_market_cap: String(MOM_MIN_MC),
      max_market_cap: String(MOM_MAX_MC),
    });

    const res = await fetch(`${BIRDEYE_MEME_URL}?${params}`, {
      headers: {
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain": "solana",
        "accept": "application/json",
      },
      signal: AbortSignal.timeout(10000),
    });

    if (!res.ok) {
      const errText = await res.text().catch(() => "");
      addLog(`⚠️ Birdeye error: ${res.status} — ${errText.slice(0, 120)}`, "warn");
      return;
    }

    const json = await res.json();
    const tokens = json?.data?.items || [];

    if (!Array.isArray(tokens)) {
      addLog(`⚠️ Birdeye respuesta inesperada`, "warn");
      return;
    }

    for (const token of tokens) {
      const mint = token.address;
      if (!mint || mint.length < 32) continue;

      const mc = token.market_cap || token.fdv || 0;
      const vol1h = token.volume_1h_usd || 0;
      const pct1h = token.price_change_1h_percent || 0;
      const price = token.price || 0;
      const supply = token.total_supply || token.circulating_supply || 1_000_000_000;
      const liquidity = token.liquidity || 0;
      const lastTrade = token.last_trade_unix_time || 0;
      const symbol = token.symbol || mint.slice(0, 8);
      const name = token.name || symbol;

      if (price <= 0) continue;
      // Selección estilo Gecko: el volumen manda, NO el momentum reciente.
      // Ya no filtramos por "que haya subido +10-30%" (eso causaba entradas
      // tardías en el pico). Solo descartamos los claramente bajistas.
      if (pct1h < -5) continue;
      // Filtros movidos de la query al código (límite de 5 filtros en Birdeye):
      if (vol1h < MOM_MIN_VOL_1H) continue;                       // volumen 1h mínimo
      if (lastTrade > 0 && lastTrade < minLastTrade) continue;    // trade en últimos 10min
      if (liquidity > 0 && liquidity < 30000) continue;           // liquidez mínima (10k->30k: SL ejecuta limpio)

      totalScanned++;

      if (state.momMonitored.has(mint)) {
        momUpdatePrice(mint, price, 0);
        continue;
      }

      const lastSig = momSignalCooldown.get(mint) || 0;
      if (Date.now() - lastSig < MOM_SIGNAL_COOLDOWN_MS) continue;

      const momOpen = state.demoTrades.filter(t => t.status === "OPEN" && t.strategy === "momentum").length;
      if (momOpen >= MOM_MAX_OPEN) continue;

      state.stats.mom_scanned++;
      momSignalCooldown.set(mint, Date.now());
      state.stats.mom_signals++;
      totalSignals++;

      state.momPending.set(mint, {
        mint, symbol, name,
        birdeyePrice: price,
        supply,                         // supply real guardado
        calSupply: null,                // supply calibrado (se rellena con trades)
        mc, vol1h, pct1h,
        pendingSince: Date.now(),
        priceHistory: [],               // {price, time} durante ventana de confirmación
        firstTradeAt: null,             // cuándo llegó el primer trade real
      });
      state.stats.mom_pending = state.momPending.size;

      if (pumpPortalWs?.readyState === WebSocket.OPEN) {
        pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
      }

      addLog(`⚡ MOMENTUM: ${symbol} | +${pct1h.toFixed(1)}% 1h | Vol ${formatMC(vol1h)} | MC ${formatMC(mc)}`, "signal");

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

    addLog(`⚡ Birdeye scan: ${totalScanned} candidatos, ${totalSignals} señales nuevas`, "info");
    broadcast({ event: "stats", data: state.stats });
  } catch (e) {
    addLog(`❌ Momentum scan error: ${e.message}`, "error");
  }
}

function momActivateFromPending(mint, entryPrice, solAmount) {
  if (solAmount <= 0) return;
  const pending = state.momPending.get(mint);
  if (!pending) return;

  const momOpen = state.demoTrades.filter(t => t.status === "OPEN" && t.strategy === "momentum").length;
  if (momOpen >= MOM_MAX_OPEN) {
    addLog(`⛔ MOM límite: ya hay ${momOpen} abiertas — cancelando ${pending.symbol}`, "filter");
    state.momPending.delete(mint);
    state.stats.mom_pending = state.momPending.size;
    unsubscribeToken(mint);
    broadcast({ event: "stats", data: state.stats });
    return;
  }

  state.momPending.delete(mint);
  state.stats.mom_pending = state.momPending.size;

  const signal = {
    id: `mom-${mint}-${Date.now()}`, strategy: "momentum",
    mint, name: pending.name, symbol: pending.symbol,
    price: entryPrice, tp: +(entryPrice * MOM_TP).toFixed(12), sl: +(entryPrice * MOM_SL).toFixed(12),
    mcUsd: pending.mc, vol1h: pending.vol1h, pct1h: pending.pct1h, time: Date.now(),
  };
  addLog(`⚡ ENTRADA [real PP]: ${pending.symbol} @ $${entryPrice.toFixed(8)} | TP +4.5% SL -3%`, "accept");
  state.signals.unshift(signal);
  if (state.signals.length > 100) state.signals.pop();
  broadcast({ event: "newSignal", data: signal });
  state.momMonitored.set(mint, {
    mint, symbol: pending.symbol, name: pending.name, mc: pending.mc, price: entryPrice,
    supply: pending.supply,             // supply Birdeye (fallback)
    calSupply: pending.calSupply,       // supply calibrado si ya se calculó
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
    if (solAmount > 0) {
      const pending = state.momPending.get(mint);
      if (!pending.firstTradeAt) pending.firstTradeAt = Date.now();
      pending.priceHistory.push({ price, time: Date.now() });
      if (pending.priceHistory.length > 30) pending.priceHistory.shift();
      // ¿ya pasó la ventana de confirmación de tendencia?
      if (Date.now() - pending.firstTradeAt >= MOM_CONFIRM_MS) {
        momTryConfirmEntry(mint);
      }
    }
    return;
  }
  const token = state.momMonitored.get(mint);
  if (!token) return;
  const supply = token.calSupply || token.supply || 1_000_000_000;
  token.price = price; token.mc = price * supply;
  token.priceHigh = Math.max(token.priceHigh, price);
  token.priceLow = Math.min(token.priceLow, price);
  if (solAmount > 0) { token.tradeCount++; token.volumeUSD += solAmount * solPriceUSD; }
  token.lastUpdate = Date.now();
  updateDemoTrades(mint, price, "momentum");
  updateRealTrades(mint, price, "momentum");
  broadcast({ event: "momTokenUpdate", data: token });
}

// Tras la ventana de confirmación, decide si entrar según la tendencia.
// Aborta si el precio en la ventana viene plano o bajista (entrada en pico).
function momTryConfirmEntry(mint) {
  const pending = state.momPending.get(mint);
  if (!pending) return;
  const hist = pending.priceHistory;
  if (hist.length < 2) return; // esperar algo más de datos

  const first = hist[0].price;
  const last = hist[hist.length - 1].price;
  const trend = (last - first) / first;

  if (trend <= 0) {
    addLog(`⛔ MOM DESCARTADA [tendencia ${(trend*100).toFixed(1)}%]: ${pending.symbol} no sube en ventana`, "filter");
    state.momPending.delete(mint);
    state.stats.mom_pending = state.momPending.size;
    state.stats.mom_cancelled++;
    unsubscribeToken(mint);
    broadcast({ event: "stats", data: state.stats });
    return;
  }

  addLog(`✅ MOM CONFIRMADA [tendencia +${(trend*100).toFixed(1)}%]: ${pending.symbol}`, "accept");
  momActivateFromPending(mint, last, 1);
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
    recentPrices: [],   // {price, time} de los últimos ~5s, para el stop de colapso
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
  if (reason === "TP" || reason === "STEP" || (reason === "SL" && trade.pnlPct > 0)) {
    trade.result = "WIN"; state.stats[`${prefix}_realWins`]++;
    state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
    const tag = reason === "STEP" ? "🪜 ESCALÓN" : trade.strategy;
    addLog(`✅ REAL WIN [${tag}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "realwin");
  } else if (reason === "COLLAPSE") {
    trade.result = "LOSS"; state.stats[`${prefix}_realLosses`]++;
    state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
    state.stats.mig_collapse_count++;
    state.stats.mig_collapse_pnlSum += trade.pnlPct;
    addLog(`🛑 REAL COLLAPSE [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s (desplome veloz)`, "realloss");
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
  if (trade.strategy === "migration" && reason !== "TP") {
    registerPostClose(trade.mint, trade.symbol, price, trade.pnlPct, reason);
  }
  if (trade.strategy === "migration") migCleanup(trade.mint, trade.symbol);
  if (trade.strategy === "momentum") momCleanup(trade.mint);
  broadcast({ event: "realTradeClosed", data: trade });
  broadcast({ event: "stats", data: state.stats });
  saveState();
}

// Stop de colapso vertical (solo migración): registra el precio en el
// mini-historial reciente del trade y devuelve true si el precio actual
// cayó más de MIG_COLLAPSE_DROP desde el pico de los últimos
// MIG_COLLAPSE_WINDOW_MS. Es un "trailing rápido" para el caso de pánico:
// la diferencia con el trailing FOLLOWING (-20% del máximo) es la VELOCIDAD,
// solo dispara si ese desplome ocurre en la ventana corta. No solapa con
// FOLLOWING (sin componente temporal) ni con el escalón.
function detectCollapse(trade, price, now) {
  if (trade.strategy !== "migration") return false;
  // El registro del precio reciente ocurre SIEMPRE (aunque COLLAPSE esté
  // desactivado en la gracia), para tener historial cuando la gracia acabe.
  trade.recentPrices.push({ price, time: now });
  while (trade.recentPrices.length && now - trade.recentPrices[0].time > 5000) {
    trade.recentPrices.shift();
  }
  const window = trade.recentPrices.filter(p => now - p.time <= MIG_COLLAPSE_WINDOW_MS);
  if (window.length < 2) return false;
  const peak = Math.max(...window.map(p => p.price));
  if (peak <= 0) return false;
  const dropFromPeak = (peak - price) / peak;
  return dropFromPeak > MIG_COLLAPSE_DROP;
}

// SL efectivo con ventana de gracia (solo migración): durante los primeros
// MIG_GRACE_MS de vida del trade, el suelo del stop es más holgado (-25%)
// para aguantar el lavado inicial — muchas ganadoras caen -15/-20% en la
// sacudida previa al despegue y el SL -18% las sacaba justo antes de subir.
// Pasada la ventana, el suelo vuelve a -18%. El trailing/escalón mandan si
// ya subieron el stop por encima (nunca reducimos protección ganada).
function effectiveSL(trade, now) {
  if (trade.strategy !== "migration") return trade.sl;
  const inGrace = (now - trade.openTime) < MIG_GRACE_MS;
  if (!inGrace) return trade.sl;
  const graceFloor = +(trade.entryPrice * MIG_GRACE_SL).toFixed(12);
  // En la gracia, el stop es el MÁS BAJO entre el SL normal y el suelo de gracia
  // (damos más margen). Pero si el trailing ya lo subió por encima de la entrada,
  // ese manda — la gracia solo relaja el SL inicial, no toca ganancias bloqueadas.
  if (trade.sl >= trade.entryPrice) return trade.sl;
  return Math.min(trade.sl, graceFloor);
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
    // Stop de colapso vertical (el más rápido): si dispara, cierra ya.
    // detectCollapse se llama siempre (registra el precio reciente), pero
    // durante la ventana de gracia ignoramos su disparo: ahí manda el SL de
    // gracia (-25%), para no sacar ganadoras en el lavado inicial.
    const inGrace = strategy === "migration" && (now - trade.openTime) < MIG_GRACE_MS;
    const collapsed = detectCollapse(trade, price, now);
    if (!inGrace && collapsed) {
      closeRealTrade(trade, price, "COLLAPSE");
      continue;
    }
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
    // Escalón de beneficio (solo migración): si en algún momento tocó
    // +20%, el stop nunca baja de +13%. Usa maxGainPct (máximo alcanzado),
    // no el beneficio instantáneo, para que el piso se mantenga aunque el
    // precio caiga luego. El -1e-9 evita que +20.0% exacto se escape por
    // redondeo de coma flotante. El stop es el MÁS ALTO de los candidatos,
    // así que el trailing -20% sigue mandando en las grandes y el escalón
    // actúa solo como suelo mínimo.
    if (strategy === "migration" && trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9) {
      const stepStop = trade.entryPrice * (1 + MIG_STEP_FLOOR);
      if (stepStop > trade.sl) {
        if (trade.trailingPhase !== "STEP") trade.trailingPhase = "STEP";
        trade.sl = +stepStop.toFixed(12);
      }
    }
    if (price >= trade.tp) closeRealTrade(trade, price, "TP");
    else if (price <= effectiveSL(trade, now)) {
      // En real el cierre se ejecuta al precio real de venta (con su slippage),
      // NO al nivel teórico del stop. Solo ajustamos el reason para contabilizar
      // bien: STEP si cerró por el piso del escalón, SL en otro caso.
      const reason = trade.trailingPhase === "STEP" ? "STEP" : "SL";
      closeRealTrade(trade, price, reason);
    }
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
    // Stop de colapso vertical (el más rápido): si dispara, cierra ya.
    // detectCollapse se llama siempre (registra el precio reciente), pero
    // durante la ventana de gracia ignoramos su disparo: ahí manda el SL de
    // gracia (-25%), para no sacar ganadoras en el lavado inicial.
    const inGrace = strategy === "migration" && (now - trade.openTime) < MIG_GRACE_MS;
    const collapsed = detectCollapse(trade, price, now);
    if (!inGrace && collapsed) {
      closeDemoTrade(trade, price, "COLLAPSE", tp_pct);
      continue;
    }
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
    // Escalón de beneficio (solo migración): si tocó +20%, piso de +13%.
    // El -1e-9 evita que +20.0% exacto se escape por redondeo de coma flotante.
    if (strategy === "migration" && trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9) {
      const stepStop = trade.entryPrice * (1 + MIG_STEP_FLOOR);
      if (stepStop > trade.sl) {
        if (trade.trailingPhase !== "STEP") {
          trade.trailingPhase = "STEP";
          addLog(`🪜 ESCALÓN +13% [${strategy}]: ${trade.symbol} (tocó +${trade.maxGainPct.toFixed(0)}%)`, "trail");
        }
        trade.sl = +stepStop.toFixed(12);
      }
    }
    if (price >= trade.tp) closeDemoTrade(trade, price, "TP", tp_pct);
    else if (price <= effectiveSL(trade, now)) {
      // Si el stop protege ganancia (piso del escalón o trailing en positivo),
      // el cierre se ejecuta AL NIVEL DEL STOP, no al precio del tick que lo cruzó:
      // el stop es la orden que se habría disparado a ese nivel. En una caída
      // vertical entre ticks (memecoin), usar el precio del tick infravaloraría
      // el cierre (cerraba +1.17% en vez del piso +13%). Para el SL inicial bajo
      // entrada (incl. SL de gracia), cerrar al precio real (más honesto en caída
      // sin liquidez).
      const stopProtegeGanancia = trade.sl >= trade.entryPrice;
      const closePrice = stopProtegeGanancia ? trade.sl : price;
      const reason = trade.trailingPhase === "STEP" ? "STEP" : "SL";
      closeDemoTrade(trade, closePrice, reason, tp_pct);
    }
    else if (now >= trade.expiresAt) closeDemoTrade(trade, price, "EXPIRED", tp_pct);
    else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
  }
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.realTrades) {
    if (trade.status !== "OPEN") continue;
    const token = state.migMonitored.get(trade.mint) || state.momMonitored.get(trade.mint);
    if (!token) continue;
    if (now >= trade.expiresAt) { closeRealTrade(trade, token.price, "EXPIRED"); continue; }
    if (token.price <= effectiveSL(trade, now)) {
      const reason = trade.trailingPhase === "STEP" ? "STEP" : "SL";
      addLog(`🚨 ${reason === "STEP" ? "ESCALÓN" : "SL"} FORZADO [${trade.strategy}]: ${trade.symbol}`, "warn");
      closeRealTrade(trade, token.price, reason);
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
  evaluarAbortos();
  evaluarPostCierres();
}, 5_000);

// Evalúa los abortos en observación: a los 5 min, clasifica si el filtro
// acertó (token cayó o plano) o costó un ganador (subió +20%).
function evaluarAbortos() {
  const now = Date.now();
  for (const [mint, a] of state.abortWatch.entries()) {
    if (now - a.abortTime < ABORT_WATCH_MS) continue;
    const cur = a.lastPrice;
    if (cur && a.abortPrice > 0) {
      const change = (cur - a.abortPrice) / a.abortPrice;
      const r = a.abortReason; // "delay" | "vertical" | "bajista"
      if (change >= ABORT_WIN_THRESHOLD) {
        state.stats.abort_missed++;
        state.stats[`abort_missed_${r}`] = (state.stats[`abort_missed_${r}`] || 0) + 1;
        addLog(`❌ ABORTO ERRÓNEO: ${a.symbol} subió ${(change*100).toFixed(0)}% tras abortar [${r}]`, "warn");
      } else {
        state.stats.abort_correct++;
        state.stats[`abort_correct_${r}`] = (state.stats[`abort_correct_${r}`] || 0) + 1;
        addLog(`✅ ABORTO CORRECTO: ${a.symbol} ${(change*100).toFixed(0)}% tras abortar [${r}]`, "filter");
      }
    }
    state.abortWatch.delete(mint);
    unsubscribeToken(mint);
    broadcast({ event: "stats", data: state.stats });
  }
}

// Evalúa los cierres en observación: a los 5 min, clasifica el cierre y lo
// loguea. Detecta "ganadores perdidos" (cerró en pérdida pero el token
// despegó +20% después). Instrumentación pura: solo añade datos al log.
function evaluarPostCierres() {
  const now = Date.now();
  for (const [mint, c] of state.postCloseWatch.entries()) {
    if (now - c.closeTime < POST_CLOSE_WATCH_MS) continue;
    const cur = c.lastPrice;
    if (cur && c.closePrice > 0) {
      const post5m = (cur - c.closePrice) / c.closePrice;  // % desde el precio de salida
      const post5mPct = (post5m * 100).toFixed(0);
      // Clasificación automática:
      let etiqueta;
      if (post5m >= POST_CLOSE_WIN_THRESHOLD && c.closePnl < 0) {
        etiqueta = "🔴 GANADOR PERDIDO";  // cerró en pérdida y luego despegó
      } else if (post5m >= POST_CLOSE_WIN_THRESHOLD) {
        etiqueta = "🟡 SIGUIÓ SUBIENDO";  // cerró en verde y siguió (dejamos algo en la mesa)
      } else if (post5m <= -0.20) {
        etiqueta = "🟢 MUERTO CONFIRMADO"; // siguió cayendo: el cierre acertó
      } else {
        etiqueta = "⚪ PLANO/REVERSIÓN";   // ni subió ni se hundió
      }
      addLog(`🔭 POST-CIERRE 5m: ${c.symbol} | cerró ${c.closePnl}% [${c.closeReason}] | post5m=${post5m>=0?"+":""}${post5mPct}% | ${etiqueta}`, "filter");
    }
    state.postCloseWatch.delete(mint);
    unsubscribeToken(mint);
  }
}

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
    recentPrices: [],   // {price, time} de los últimos ~5s, para el stop de colapso
  };
  state.demoTrades.unshift(trade);
  if (state.demoTrades.length > 500) state.demoTrades.pop();
  state.stats.demoOpen++;
  broadcast({ event: "newDemoTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  const tpPct = signal.strategy === "migration" ? "+80%" : "+4.5%";
  const slPct = signal.strategy === "migration" ? "-18%" : "-3%";
  addLog(`📝 DEMO [${signal.strategy}]: ${signal.symbol} | TP ${tpPct} SL ${slPct}`, "demo");
}

function closeDemoTrade(trade, price, reason, tp_pct) {
  trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
  const dur = Math.round((trade.closeTime - trade.openTime) / 1000);
  if (trade.strategy === "migration") {
    addLog(`🔎 MIG CLOSE DEBUG: ${trade.symbol} | entryPrice=$${trade.entryPrice.toFixed(10)} | closePrice=$${price.toFixed(10)} | pnl=${((price - trade.entryPrice) / trade.entryPrice * 100).toFixed(2)}% | min=${(trade.maxLossPct||0).toFixed(1)}% | reason=${reason}`, "info");
  }
  const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
  trade.pnlPct = +pnlPct.toFixed(2);
  const prefix = trade.strategy === "migration" ? "mig" : "mom";
  const expWinPct = trade.strategy === "migration" ? MIG_EXPIRED_WIN_PCT : MOM_EXPIRED_WIN_PCT;
  if (reason === "TP") {
    trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++;
    state.stats[`${prefix}_demoPnL`] += (tp_pct - 1) * 100;
    addLog(`✅ WIN [TP][${trade.strategy}]: ${trade.symbol} +${((tp_pct-1)*100).toFixed(0)}% en ${dur}s`, "win");
  } else if (reason === "STEP") {
    // Cierre por el piso del escalón (+13%): siempre es win
    trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++;
    state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
    addLog(`✅ WIN [🪜 ESCALÓN][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win");
  } else if (reason === "COLLAPSE") {
    // Stop de colapso vertical: cierre por desplome veloz. Cuenta como LOSS
    // (cierra en negativo) pero se mide aparte para evaluar su efecto.
    trade.result = "LOSS"; state.stats[`${prefix}_demoLosses`]++;
    state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
    state.stats.mig_collapse_count++;
    state.stats.mig_collapse_pnlSum += trade.pnlPct;
    addLog(`🛑 COLLAPSE [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s (desplome veloz)`, "loss");
  } else if (reason === "SL") {
    state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
    if (trade.pnlPct > 0) { trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++; addLog(`✅ WIN [${trade.trailingPhase}][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win"); }
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
  // Observación post-cierre (solo migración, cierres que NO son TP): detecta
  // ganadores perdidos. Se registra ANTES del cleanup para que no desuscriba.
  if (trade.strategy === "migration" && reason !== "TP") {
    registerPostClose(trade.mint, trade.symbol, price, trade.pnlPct, reason);
  }
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
    for (const [mint] of state.momMonitored.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
    for (const [mint] of state.abortWatch.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
    for (const [mint] of state.postCloseWatch.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
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

        const calSupply = calibrateSupply(data);

        // ── ABORTOS EN OBSERVACIÓN: solo actualizar último precio ──
        const aborted = state.abortWatch.get(data.mint);
        if (aborted) {
          const price = calcPrice(data, calSupply || 1_000_000_000);
          if (price > 0) aborted.lastPrice = price;
        }

        // ── POST-CIERRE EN OBSERVACIÓN: solo actualizar último precio ──
        const postClosed = state.postCloseWatch.get(data.mint);
        if (postClosed) {
          const price = calcPrice(data, calSupply || 1_000_000_000);
          if (price > 0) postClosed.lastPrice = price;
        }

        const migEntry = state.migWatching.get(data.mint) || state.migMonitored.get(data.mint);
        if (migEntry) {
          if (calSupply && !migEntry.calSupply) migEntry.calSupply = calSupply;
          const price = calcPrice(data, migEntry.calSupply);
          if (price > 0) migUpdatePrice(data.mint, price, data.solAmount || 0);
        }

        const momEntry = state.momPending.get(data.mint) || state.momMonitored.get(data.mint);
        if (momEntry) {
          if (calSupply && !momEntry.calSupply) momEntry.calSupply = calSupply;
          const supplyToUse = momEntry.calSupply || momEntry.supply;
          const price = calcPrice(data, supplyToUse);
          if (price > 0) momUpdatePrice(data.mint, price, data.solAmount || 0);
        }
      }
    } catch (e) { console.log("PP:", e.message); }
  });
  pumpPortalWs.on("error", (err) => addLog(`❌ PumpPortal: ${err.message}`, "error"));
  pumpPortalWs.on("close", () => { addLog("🔄 PumpPortal reconectando...", "warn"); setTimeout(connectPumpPortal, 5000); });
}

function connectHelius() {
  addLog("ℹ️ Helius desactivado — precios via PumpPortal", "info");
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

app.get("/api/reset-stats", (req, res) => {
  state.demoTrades = [];
  state.realTrades = [];
  state.signals = [];
  state.stats = {
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
    demoOpen: 0, realOpen: 0, walletBalance: state.stats.walletBalance || 0,
    abort_correct: 0, abort_missed: 0,
    abort_correct_bajista: 0, abort_correct_vertical: 0, abort_correct_delay: 0,
    abort_missed_bajista: 0, abort_missed_vertical: 0, abort_missed_delay: 0,
    mig_collapse_count: 0, mig_collapse_pnlSum: 0,
  };
  saveState();
  broadcast({ event: "stats", data: state.stats });
  addLog("🔄 Stats y trades reseteados", "info");
  res.json({ ok: true, message: "Stats reseteadas — trades y señales borrados, movimientos conservados" });
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
  console.log(`🚀 SolScanBot v8.4 — Instrumentación: min (MIN ↓) + post-cierre 5min (ganador perdido)`);
  loadState();
  initWallet();
  connectPumpPortal();
  connectHelius();
  setTimeout(momentumScan, 5000);
  setInterval(momentumScan, MOM_SCAN_MS);
});
