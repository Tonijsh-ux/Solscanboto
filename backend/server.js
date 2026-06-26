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
import { TOKEN_PROGRAM_ID } from "@solana/spl-token";
import bs58 from "bs58";

const PORT = process.env.PORT || 3001;
const SOL_PER_TRADE_MIG = 0.15;
const SOL_PER_TRADE = 0.15;
const MAX_REAL_TRADES = 7;   // v6.18.4: 6 (migración) + 1 (momentum prueba)
const MAX_MIG_REAL = 6;
// v6.18.4: momentum real ACTIVADO con límite estricto de 1 operación y lote pequeño, solo para medir el costo real on-chain.
const SOL_PER_TRADE_MOM = 0.05;
const MAX_MOM_REAL = 1;
// Momentum ahora también opera real (límite 1) — migration opera en real
const REAL_STRATEGIES = ["migration", "momentum"];
// CAMBIO: estado fuera de /tmp (que se borra al reiniciar en muchos hosts).
// Se puede sobreescribir con STATE_FILE en el entorno. Si /var/data no existe,
// cae a ./solscanbot_state.json en el cwd (persistente si el cwd lo es).
const STATE_FILE = process.env.STATE_FILE
  || (fs.existsSync("/var/data") ? "/var/data/solscanbot_state.json" : "./solscanbot_state.json");

// ── CONFIG MIGRACIÓN ───────────────────────────────────────────
const MIG_TP = 4.00;
const MIG_SL = 0.80;
const MIG_DURATION_MS = 15 * 60 * 1000;
const MIG_WINDOW_MS = 60_000;
const MIG_MIN_VOL_FAST = 1_500;
const MIG_MIN_VOL_SLOW = 2_000;
const MIG_FAST_WINDOW_MS = 20_000;
const MIG_MIN_MC = 0;
const MIG_MAX_MC = 2_000_000;

// ── MODO OBSERVADOR ────────────────────────────────────────────
const OBSERVER_MODE = false;
const LIVE_RECORD = true;
const LIVE_REC_DENSE_MS = 60_000;
const LIVE_REC_DENSE_INTERVAL = 2_000;
const LIVE_REC_NORMAL_INTERVAL = 5_000;
const OBS_MIN_VOL = 2_000;
const OBS_MIN_MC = 20_000;
const OBS_RECORD_MS = 600_000;
const OBS_T1_MS = 60_000;
const OBS_T1_INTERVAL = 2_000;
const OBS_T2_MS = 300_000;
const OBS_T2_INTERVAL = 3_000;
const OBS_T3_INTERVAL = 5_000;

const MIG_BREAKEVEN_AT = 0.99;
const MIG_BREAKEVEN_MARGIN = 0.03;
const MIG_LOCK_AT = 0.70;
const MIG_FOLLOW_PCT = 0.20;
const MIG_MAX_PRICE_RATIO = 2.0;
const MIG_SL_CONFIRM_TICKS = 2;
const MIG_EXPIRED_WIN_PCT = 2;
const MIG_ENTRY_DELAY_MS = 3_000;
const MIG_QUAL_GATE = true;
const MIG_QUAL_WINDOW_MS = 15_000;
const MIG_MAX_CAIDA_DELAY = 0.25;
const MIG_STEP_TRIGGER = 0.70;
const MIG_STEP_FLOOR = 0.13;
const MIG_FOLLOW_PCT_STEP = 0.15;
const MIG_HARD_CAP_LOSS = -20;
const MIG_VELO_DROP = 0.10;
const MIG_VELO_MS = 2_000;
const MIG_TRAIL_T1 = 40;  const MIG_TRAIL_P1 = 0.15;
const MIG_TRAIL_T2 = 60;  const MIG_TRAIL_P2 = 0.12;
const MIG_TRAIL_T3 = 100; const MIG_TRAIL_P3 = 0.08;
const MIG_TRAIL_P4 = 0.05;
const MIG_TOP_FLOOR_TRIGGER = 100;
const MIG_TOP_FLOOR = 0.65;

// ── CONFIG MOMENTUM ────────────────────────────────────────────
const MOM_TP = 1.12;   // v6.18.4: subido de +6% a +12%. El costo round-trip real medido on-chain (~8% en migración) hacía que +6% diera neto negativo. +12% deja margen sobre el costo.
const MOM_SL = 0.97;
const MOM_DURATION_MS = 45 * 60 * 1000;
const MOM_MIN_PCT_1H = 10;
const MOM_MAX_PCT_1H = 30;
const MOM_MIN_VOL_1H = 100_000;
const MOM_MIN_MC = 100_000;
const MOM_MAX_MC = 1_000_000;
const MOM_SCAN_MS = 30_000;
const MOM_MIN_LIQUIDITY = 50_000;     // v6.18.3: subido de 20K a 50K. Con 20K el slippage de la propia venta mueve el precio 5-10%, así que el SL/TP no se ejecutan a los precios previstos. 50K = ejecución más limpia (informe estrategia).
const MOM_MUTE_TIMEOUT_MS = 90_000;
const MOM_HARD_CAP_LOSS = -10;
const MOM_MAX_ENTRY_DRIFT = 0.04;
const MOM_MUTE_COOLDOWN_MS = 15 * 60_000;
const MOM_MUTE_CHECK_MS = 5_000;
const MOM_MUTE_MIN_MOVE = 0.0003;  // v6.18.1: <0.03% en 5s = feed muerto (bajado de 0.05%)
const BIRDEYE_PRICE = "https://public-api.birdeye.so/defi/price";
const MOM_BREAKEVEN_AT = 0.03;
const MOM_LOCK_AT = 0.05;   // v6.18.1: vuelto a +5% (params v6.6 — deja correr más hacia el TP en vez de estrangular en +3%)
const MOM_FOLLOW_PCT = 0.05;   // v6.18.3: ensanchado de 2% a 5%. El 2% se dispara con ruido normal de memecoins y cierra trades buenos antes de tiempo (informe estrategia).
const MOM_PENDING_TIMEOUT_MS = 15_000;
const MOM_SIGNAL_COOLDOWN_MS = 3 * 60 * 1000;
const MOM_EXPIRED_WIN_PCT = 2;
const MOM_RECORD = true;   // v6.15.4: grabar [MOMREC]
// CAMBIO (fix momentum): seguimiento de momentum SOLO con precio de Birdeye,
// coherente con el precio de entrada (que también es de Birdeye). Cada cuántos ms
// se repregunta el precio de las posiciones de momentum abiertas. Una sola llamada
// batch por ciclo (multi_price) para todas las posiciones de momentum a la vez.
const MOM_TRACK_MS = 15_000;   // tracker de posiciones abiertas de momentum. A 5s da mejor resolución pero revienta el cupo de Birdeye en continuo; para tick fino permanente, pasar a DexScreener.
const BIRDEYE_MULTI_PRICE = "https://public-api.birdeye.so/defi/multi_price";

// ── KILL-SWITCH DE PORTAFOLIO (v6.18) ──────────────────────────
// Límites elegidos: -0.5 SOL de pérdida diaria, 8 pérdidas reales seguidas.
// Al saltar por racha, pausa 1h. Al saltar por pérdida diaria, pausa hasta el
// cambio de día (UTC). El estado se persiste para que un reinicio no lo resetee.
const RISK = {
  maxDailyLossSol: 0.5,
  maxConsecutiveLosses: 8,
  cooldownAfterStreakMs: 60 * 60 * 1000, // 1h tras racha
};

// ── SECRETOS: ahora desde el entorno (rotar las keys expuestas) ──
// Antes estaban hardcodeadas en el archivo. Si falta alguna, se avisa al arrancar.
const HELIUS_API_KEY = process.env.HELIUS_API_KEY || "";
const SOLANA_RPC = process.env.SOLANA_RPC
  || (HELIUS_API_KEY ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : "https://api.mainnet-beta.solana.com");
const PUMPPORTAL_API_KEY = process.env.PUMPPORTAL_API_KEY || "";
const PUMPPORTAL_WS = PUMPPORTAL_API_KEY
  ? `wss://pumpportal.fun/api/data?api-key=${PUMPPORTAL_API_KEY}`
  : "wss://pumpportal.fun/api/data";
const GECKO_PUMPSWAP = "https://api.geckoterminal.com/api/v2/networks/solana/dexes/pumpswap/pools";
const BIRDEYE_API_KEY = process.env.BIRDEYE_API_KEY || "";
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
    connection = new Connection(SOLANA_RPC, "confirmed");
    addLog(`✅ Wallet: ${wallet.publicKey.toString()}`, "info");
  } catch (e) { addLog(`❌ Wallet error: ${e.message}`, "error"); }
}

let cachedBalance = 0;
let lastBalanceFetch = 0;
const BALANCE_CACHE_MS = 30_000;

async function getWalletBalance(force = false) {
  if (!wallet || !connection) return cachedBalance;
  const now = Date.now();
  if (!force && now - lastBalanceFetch < BALANCE_CACHE_MS) return cachedBalance;
  for (let i = 0; i < 3; i++) {
    try {
      cachedBalance = (await connection.getBalance(wallet.publicKey)) / LAMPORTS_PER_SOL;
      lastBalanceFetch = Date.now();
      return cachedBalance;
    }
    catch (e) {
      if (i < 2) { await new Promise(r => setTimeout(r, 2000)); }
      else { addLog(`⚠️ getWalletBalance falló 3 veces: ${e.message}`, "warn"); return cachedBalance; }
    }
  }
  return cachedBalance;
}

async function getTokenBalance(mint) {
  if (!wallet || !connection) return 0;
  try {
    const accounts = await connection.getParsedTokenAccountsByOwner(wallet.publicKey, { mint: new PublicKey(mint) });
    if (!accounts.value.length) return 0;
    return accounts.value[0].account.data.parsed.info.tokenAmount.uiAmount || 0;
  } catch { return 0; }
}

// ── KILL-SWITCH: estado de riesgo (persistente) ──
const riskState = {
  dayKey: null,
  dailyPnlSol: 0,
  consecutiveLosses: 0,
  pausedUntil: 0,
};

function todayKeyUTC() { return new Date().toISOString().slice(0, 10); }

function riskRolloverDay() {
  const k = todayKeyUTC();
  if (riskState.dayKey !== k) {
    riskState.dayKey = k;
    riskState.dailyPnlSol = 0;
    riskState.consecutiveLosses = 0;
    // no se toca pausedUntil aquí: una pausa por racha puede cruzar medianoche
  }
}

// ¿Está pausada la operativa real? (consultar antes de cada entrada real)
function tradingHalted() {
  riskRolloverDay();
  if (Date.now() < riskState.pausedUntil) return true;
  if (riskState.dailyPnlSol <= -RISK.maxDailyLossSol) {
    if (!riskState._dailyLogged) {
      riskState._dailyLogged = true;
      addLog(`🛑 KILL-SWITCH: pérdida diaria ${riskState.dailyPnlSol.toFixed(3)} SOL ≥ tope ${RISK.maxDailyLossSol} — operativa real pausada hasta cambio de día (UTC)`, "error");
      broadcast({ event: "risk", data: riskSnapshot() });
    }
    return true;
  }
  if (riskState.consecutiveLosses >= RISK.maxConsecutiveLosses) {
    if (Date.now() >= riskState.pausedUntil) {
      riskState.pausedUntil = Date.now() + RISK.cooldownAfterStreakMs;
      riskState.consecutiveLosses = 0; // reset de la racha tras pausar
      addLog(`🛑 KILL-SWITCH: ${RISK.maxConsecutiveLosses} pérdidas reales seguidas — pausa 1h`, "error");
      broadcast({ event: "risk", data: riskSnapshot() });
    }
    return true;
  }
  return false;
}

// Registrar el resultado de un trade real cerrado (llamar desde closeRealTrade)
function riskRecordClose(pnlSol) {
  riskRolloverDay();
  riskState.dailyPnlSol = +(riskState.dailyPnlSol + pnlSol).toFixed(6);
  if (pnlSol < 0) riskState.consecutiveLosses++;
  else riskState.consecutiveLosses = 0;
  if (riskState.dailyPnlSol > -RISK.maxDailyLossSol) riskState._dailyLogged = false;
  broadcast({ event: "risk", data: riskSnapshot() });
}

function riskSnapshot() {
  riskRolloverDay();
  const pausedMsLeft = Math.max(0, riskState.pausedUntil - Date.now());
  return {
    dayKey: riskState.dayKey,
    dailyPnlSol: riskState.dailyPnlSol,
    maxDailyLossSol: RISK.maxDailyLossSol,
    consecutiveLosses: riskState.consecutiveLosses,
    maxConsecutiveLosses: RISK.maxConsecutiveLosses,
    pausedMsLeft,
    halted: pausedMsLeft > 0 || riskState.dailyPnlSol <= -RISK.maxDailyLossSol,
  };
}

const state = {
  migWatching: new Map(),
  migMonitored: new Map(),
  obsRecordings: new Map(),
  liveRecordings: new Map(),
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
    mig_mov_up_win: 0, mig_mov_up_loss: 0,
    mig_mov_flat_win: 0, mig_mov_flat_loss: 0,
    mig_mov_down_win: 0, mig_mov_down_loss: 0,
    mom_scanned: 0, mom_signals: 0, mom_pending: 0,
    mom_entered: 0, mom_disc_liquidity: 0, mom_disc_drift: 0,
    mom_disc_mute: 0, mom_disc_noprice: 0,
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
    // CAMBIO: escritura atómica (tmp + rename) para no corromper el archivo
    // si el proceso muere a mitad de escritura. Incluye riskState.
    const tmp = STATE_FILE + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify({
      demoTrades: state.demoTrades,
      realTrades: state.realTrades,
      movements: state.movements,
      stats: state.stats,
      riskState: {
        dayKey: riskState.dayKey,
        dailyPnlSol: riskState.dailyPnlSol,
        consecutiveLosses: riskState.consecutiveLosses,
        pausedUntil: riskState.pausedUntil,
      },
    }));
    fs.renameSync(tmp, STATE_FILE);
  } catch (e) { console.log("Error guardando estado:", e.message); }
}

// CAMBIO: loadState ya NO marca las posiciones reales OPEN como EXPIRED a ciegas.
// Solo carga del disco. La decisión sobre las reales abiertas la toma
// reconcileStateOnBoot() leyendo la wallet on-chain. Las DEMO sí se cierran
// (no hay nada on-chain que reconciliar para demo).
function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const saved = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    if (saved.demoTrades) state.demoTrades = saved.demoTrades;
    if (saved.realTrades) state.realTrades = saved.realTrades;
    if (saved.movements) state.movements = saved.movements;
    if (saved.stats) state.stats = { ...state.stats, ...saved.stats };
    // restaurar kill-switch
    if (saved.riskState) {
      riskState.dayKey = saved.riskState.dayKey ?? null;
      riskState.dailyPnlSol = saved.riskState.dailyPnlSol ?? 0;
      riskState.consecutiveLosses = saved.riskState.consecutiveLosses ?? 0;
      riskState.pausedUntil = saved.riskState.pausedUntil ?? 0;
      riskRolloverDay(); // si cambió el día (UTC), resetea el diario
    }
    // DEMO: cerrar las que quedaron abiertas (sin contraparte on-chain)
    for (const trade of state.demoTrades) {
      if (trade.status === "OPEN" || trade.status === "CLOSING") {
        trade.status = "CLOSED"; trade.result = "EXPIRED";
        trade.pnlPct = trade.pnlPct || 0; trade.closeTime = Date.now();
      }
    }
    // REAL: NO tocar aquí. Quedan como están para que reconcileStateOnBoot decida.
    state.stats.demoOpen = 0;
    addLog(`✅ Estado cargado: ${state.demoTrades.length} demo, ${state.realTrades.length} real (real pendiente de reconciliar)`, "info");
  } catch (e) { addLog(`⚠️ Error cargando estado: ${e.message}`, "warn"); }
}

// CAMBIO (Parte 1 del documento): reconciliación de estado al arrancar.
// Lee los token accounts reales de la wallet y decide qué hacer con cada
// posición real que el estado creía abierta:
//   - token sigue en wallet  → REANUDAR gestión (re-suscribir feed, no expirar)
//   - token ya no está        → cerrar como RECONCILED_GONE (se vendió fuera del bot)
// Además detecta HUÉRFANOS: tokens en wallet sin trade asociado (de un crash previo).
async function reconcileStateOnBoot() {
  if (!wallet || !connection) {
    // modo demo: no hay nada on-chain; cerrar las reales abiertas como antes
    let n = 0;
    for (const trade of state.realTrades) {
      if (trade.status === "OPEN" || trade.status === "CLOSING") {
        trade.status = "CLOSED"; trade.result = "EXPIRED";
        trade.pnlPct = trade.pnlPct || 0; trade.closeTime = Date.now(); n++;
      }
    }
    state.stats.realOpen = 0;
    if (n) addLog(`ℹ️ Sin wallet — ${n} reales marcadas EXPIRED (modo demo)`, "info");
    saveState();
    return;
  }

  const declaredOpen = state.realTrades.filter(
    t => t.status === "OPEN" || t.status === "CLOSING"
  );

  // Leer realidad on-chain
  let onChain = new Map(); // mint -> uiAmount
  try {
    const resp = await connection.getParsedTokenAccountsByOwner(
      wallet.publicKey, { programId: TOKEN_PROGRAM_ID }
    );
    for (const acc of resp.value) {
      const info = acc.account.data.parsed.info;
      const amt = info.tokenAmount.uiAmount || 0;
      if (amt > 0) onChain.set(info.mint, amt);
    }
  } catch (e) {
    addLog(`⚠️ Reconciliación: no se pudo leer la wallet (${e.message}). Por seguridad, NO expiro reales; reintento en 30s.`, "warn");
    setTimeout(reconcileStateOnBoot, 30_000);
    return;
  }

  let resumed = 0, gone = 0;
  for (const trade of declaredOpen) {
    if (onChain.has(trade.mint)) {
      // Posición REAL viva → reanudar gestión
      trade.status = "OPEN";
      // recolocar en el mapa de monitoreo para que el feed la actualice
      const monitor = {
        mint: trade.mint, name: trade.name, symbol: trade.symbol,
        price: trade.entryPrice, mc: trade.entryPrice * 1_000_000_000,
        priceHigh: trade.entryPrice, priceLow: trade.entryPrice,
        tradeCount: 0, volumeUSD: 0,
        detectedAt: trade.openTime, lastUpdate: Date.now(),
      };
      if (isMig(trade.strategy)) state.migMonitored.set(trade.mint, monitor);
      else state.momMonitored.set(trade.mint, monitor);
      // re-suscribir al feed de precio
      resubscribePrice(trade.mint);
      resumed++;
      addLog(`♻️ Reanudada posición real: ${trade.symbol} (${shortAddr(trade.mint)}) — ${onChain.get(trade.mint)} tokens en wallet`, "real");
      onChain.delete(trade.mint);
    } else {
      // El token ya no está: se vendió fuera del bot o nunca llenó
      trade.status = "CLOSED"; trade.result = "RECONCILED_GONE";
      trade.closeTime = Date.now();
      gone++;
      addLog(`🔁 Reconciliado (ya no en wallet): ${trade.symbol} → cerrado como RECONCILED_GONE`, "warn");
    }
  }

  // Huérfanos: tokens en wallet sin trade abierto asociado
  for (const [mint, amt] of onChain.entries()) {
    addLog(`⚠️ HUÉRFANO en wallet: ${shortAddr(mint)} (${amt} tokens) — sin trade asociado. Revisar/liquidar manualmente.`, "warn");
  }

  // Recontar posiciones reales abiertas
  state.stats.realOpen = state.realTrades.filter(t => t.status === "OPEN").length;
  addLog(`✅ Reconciliación: ${resumed} reanudadas, ${gone} cerradas (gone), ${onChain.size} huérfanos. Reales abiertas: ${state.stats.realOpen}`, "info");
  broadcast({ event: "stats", data: state.stats });
  saveState();
}

// Re-suscribe el feed de precio según la estrategia.
// Migración → PumpPortal (su precio es PumpPortal). Momentum → Birdeye (tracker propio).
function resubscribePrice(mint) {
  const isMomentum = state.momMonitored.has(mint) && !state.migMonitored.has(mint);
  if (isMomentum) return; // momentum se sigue por el tracker de Birdeye (momTrackTick)
  if (pumpPortalWs?.readyState === WebSocket.OPEN) {
    pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
  }
}

const frontendClients = new Set();
const seenMigMints = new Set();
const seenMomPools = new Set();
const momSignalCooldown = new Map();
const momMuteCooldown = new Map();

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

let solPriceUSD = 68;
let solPriceReady = false;
async function updateSolPrice() {
  try {
    const r = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd", { signal: AbortSignal.timeout(8000) });
    const d = await r.json();
    if (d?.solana?.usd > 0) { solPriceUSD = d.solana.usd; solPriceReady = true; return; }
  } catch (e) { addLog(`⚠️ SOL price CoinGecko falló: ${e.message}`, "warn"); }
  try {
    const r = await fetch("https://price.jup.ag/v6/price?ids=SOL", { signal: AbortSignal.timeout(8000) });
    const d = await r.json();
    const px = d?.data?.SOL?.price;
    if (px > 0) { solPriceUSD = px; solPriceReady = true; addLog(`ℹ️ SOL price vía Jupiter: $${px}`, "info"); return; }
  } catch (e) { addLog(`⚠️ SOL price Jupiter falló: ${e.message}`, "warn"); }
  addLog(`⚠️ SOL price sin actualizar, sigo con $${solPriceUSD}`, "warn");
  if (!solPriceReady) { solPriceReady = true; addLog(`⚠️ Usando SOL price fallback $${solPriceUSD} — operativa desbloqueada`, "warn"); }
}
setInterval(updateSolPrice, 60_000);
updateSolPrice();

setInterval(async () => {
  if (wallet) { state.stats.walletBalance = await getWalletBalance(); broadcast({ event: "stats", data: state.stats }); }
}, 30_000);

// ── v6.15.4: GRABACIÓN DE MOMENTUM ([MOMREC]) ──
function momRecStart(mint, symbol, entryPrice, meta) {
  if (!MOM_RECORD || entryPrice <= 0) return;
  state.liveRecordings.set(mint, {
    mint, symbol, entryPrice,
    t0: Date.now(), puntos: [{ t: 0, p: 0 }],
    lastSample: Date.now(), mov2s: null, finished: false,
    mc: meta?.mc || 0, vol: meta?.vol || 0, pct1h: meta?.pct1h || 0,
  });
}

function momRecSample(mint, price) {
  if (!MOM_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished || price <= 0) return;
  const dt = Date.now() - rec.t0;
  if (Date.now() - rec.lastSample < 15_000) return;
  rec.lastSample = Date.now();
  const pct = +((price - rec.entryPrice) / rec.entryPrice * 100).toFixed(2);
  rec.puntos.push({ t: Math.round(dt / 1000), p: pct });
  if (rec.mov2s === null && dt >= 2000) rec.mov2s = pct;
}

function momRecFinish(mint, cierreRealPct) {
  if (!MOM_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished) return;
  rec.finished = true;
  state.liveRecordings.delete(mint);
  const pts = rec.puntos;
  if (pts.length < 2) return;
  let min = pts[0], max = pts[0];
  for (const pt of pts) { if (pt.p < min.p) min = pt; if (pt.p > max.p) max = pt; }
  const mov2s = rec.mov2s === null ? "n/a" : `${rec.mov2s >= 0 ? "+" : ""}${rec.mov2s}%`;
  const cr = +(+cierreRealPct).toFixed(1);
  const ptsRaw = pts.map(p => `${p.t}:${p.p}`).join(",");
  addLog(
    `[MOMREC] sym=${rec.symbol} MC=${formatMC(rec.mc)} vol=${formatMC(rec.vol)} pct1h=${rec.pct1h?.toFixed(1)}% ` +
    `mov2s=${mov2s} MIN=${min.p}%@${min.t}s MAX=${max.p}%@${max.t}s cierre_real=${cr >= 0 ? "+" : ""}${cr}% ` +
    `pts=${ptsRaw}`,
    "rec"
  );
}

// ════════════════════════════════════════════════════════════════
// ESTRATEGIA 1: SNIPER DE MIGRACIÓN  (sin cambios funcionales)
// ════════════════════════════════════════════════════════════════

function migStartWatching(coin) {
  if (seenMigMints.has(coin.mint)) return;
  if (!solPriceReady) { addLog("⏳ Esperando precio real de SOL antes de operar", "warn"); return; }
  seenMigMints.add(coin.mint);
  state.stats.mig_migrations++;
  const mcUsd = (coin.marketCapSol || 0) * solPriceUSD;
  const mcMin = OBSERVER_MODE ? OBS_MIN_MC : MIG_MIN_MC;
  const mcMax = OBSERVER_MODE ? Infinity : MIG_MAX_MC;
  if (mcUsd > 0 && (mcUsd < mcMin || mcUsd > mcMax)) {
    addLog(`⛔ MIG MC fuera rango (${formatMC(mcUsd)}): ${coin.symbol}`, "filter");
    broadcast({ event: "stats", data: state.stats }); return;
  }
  const entry = {
    mint: coin.mint, name: coin.name || "Unknown", symbol: coin.symbol || "???",
    startTime: Date.now(), migratedMcUsd: mcUsd,
    volumeUSD: 0, tradeCount: 0, firstPrice: null, lastPrice: null,
    timer: null, entered: false, pendingEntry: false,
    qualGate: false, qualStartPrice: null, qualMov2s: null,
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
  if (entry.pendingEntry) { entry.lastPrice = price; return; }
  entry.volumeUSD += solAmount * solPriceUSD;
  entry.tradeCount++;
  entry.lastPrice = price;
  if (!entry.firstPrice && price > 0) entry.firstPrice = price;
  const elapsed = Date.now() - entry.startTime;
  if (OBSERVER_MODE && entry.volumeUSD >= OBS_MIN_VOL && price > 0) {
    clearTimeout(entry.timer); entry.entered = true; state.migWatching.delete(mint);
    obsStartRecording(entry, price, elapsed); return;
  }
  if (elapsed < MIG_FAST_WINDOW_MS && entry.volumeUSD >= MIG_MIN_VOL_FAST) {
    clearTimeout(entry.timer); entry.pendingEntry = true;
    const precioA = entry.lastPrice;
    addLog(`⚡ MIG RÁPIDA: ${entry.symbol} | $${Math.round(entry.volumeUSD)} en ${(elapsed/1000).toFixed(1)}s — confirmando 3s`, "accept");
    broadcast({ event: "stats", data: state.stats });
    setTimeout(() => {
      const precioB = entry.lastPrice;
      if (precioB < precioA * (1 - MIG_MAX_CAIDA_DELAY)) {
        const caida = ((precioB / precioA - 1) * 100).toFixed(1);
        addLog(`🚫 MIG ENTRADA ABORTADA: ${entry.symbol} cayó ${caida}%`, "filter");
        state.stats.mig_rejected++; state.migWatching.delete(mint); unsubscribeToken(mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      entry.entered = false; entry.pendingEntry = true;
      addLog(`⚡ MIG RÁPIDA confirmada: ${entry.symbol} @ MC ${formatMC(precioB * 1_000_000_000)}`, "accept");
      migQualityGateThenOpen(entry, precioB);
    }, MIG_ENTRY_DELAY_MS); return;
  }
  broadcast({ event: "migWatchUpdate", data: {
    mint, symbol: entry.symbol, volumeUSD: entry.volumeUSD, tradeCount: entry.tradeCount,
    needed: elapsed < MIG_FAST_WINDOW_MS ? MIG_MIN_VOL_FAST : MIG_MIN_VOL_SLOW,
    timeLeft: Math.max(0, MIG_WINDOW_MS - elapsed), mc: price * 1_000_000_000,
  }});
}

function migQualityGateThenOpen(entry, entryPriceB) {
  if (!MIG_QUAL_GATE) {
    entry.entered = true; state.stats.mig_entered++;
    state.migWatching.delete(entry.mint); entry.firstPrice = entryPriceB;
    addLog(`✅ MIG ENTRADA: ${entry.symbol} @ MC ${formatMC(entryPriceB * 1_000_000_000)}`, "accept");
    migOpenTrades(entry); return;
  }
  entry.qualGate = true; entry.qualStartPrice = entryPriceB; entry.qualMov2s = null;
  addLog(`🔍 MIG CALIDAD: ${entry.symbol} — evaluando 15s (mov2s>3% Y pendiente15s>0)`, "filter");
  entry.qualTimer2s = setTimeout(() => {
    if (entry.qualStartPrice > 0 && entry.lastPrice > 0)
      entry.qualMov2s = (entry.lastPrice / entry.qualStartPrice - 1) * 100;
  }, 2_000);
  setTimeout(() => {
    const priceNow = entry.lastPrice;
    const pend15 = (entry.qualStartPrice > 0 && priceNow > 0) ? (priceNow / entry.qualStartPrice - 1) * 100 : -999;
    const mov2 = entry.qualMov2s == null ? -999 : entry.qualMov2s;
    entry.qualGate = false;
    if (mov2 > 3 && pend15 > 0) {
      entry.entered = true; state.stats.mig_entered++;
      state.migWatching.delete(entry.mint); entry.firstPrice = priceNow;
      addLog(`✅ MIG ENTRADA (calidad ✓): ${entry.symbol} | mov2s ${mov2>=0?"+":""}${mov2.toFixed(1)}% · pend15s ${pend15>=0?"+":""}${pend15.toFixed(1)}% @ MC ${formatMC(priceNow * 1_000_000_000)}`, "accept");
      migOpenTrades(entry);
    } else {
      addLog(`🚫 MIG FILTRO CALIDAD: ${entry.symbol} descartada | mov2s ${mov2<=-999?"n/a":(mov2>=0?"+":"")+mov2.toFixed(1)+"%"} · pend15s ${pend15<=-999?"n/a":(pend15>=0?"+":"")+pend15.toFixed(1)+"%"}`, "filter");
      state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
      broadcast({ event: "stats", data: state.stats });
    }
  }, MIG_QUAL_WINDOW_MS);
}

function migEvaluate(mint) {
  const entry = state.migWatching.get(mint);
  if (!entry || entry.entered || entry.pendingEntry) return;
  const elapsed = ((Date.now() - entry.startTime) / 1000).toFixed(1);
  if (entry.volumeUSD >= MIG_MIN_VOL_SLOW && entry.lastPrice) {
    entry.pendingEntry = true;
    const precioA = entry.lastPrice;
    addLog(`✅ MIG LENTA: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol | ${elapsed}s — confirmando 3s`, "accept");
    broadcast({ event: "stats", data: state.stats });
    setTimeout(() => {
      const precioB = entry.lastPrice;
      if (precioB < precioA * (1 - MIG_MAX_CAIDA_DELAY)) {
        const caida = ((precioB / precioA - 1) * 100).toFixed(1);
        addLog(`🚫 MIG ENTRADA ABORTADA: ${entry.symbol} cayó ${caida}%`, "filter");
        state.stats.mig_rejected++; state.migWatching.delete(mint); unsubscribeToken(mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      entry.pendingEntry = true;
      addLog(`✅ MIG LENTA confirmada: ${entry.symbol} @ MC ${formatMC(precioB * 1_000_000_000)}`, "accept");
      migQualityGateThenOpen(entry, precioB);
    }, MIG_ENTRY_DELAY_MS);
  } else {
    state.migWatching.delete(mint); unsubscribeToken(mint);
    addLog(`❌ MIG RECHAZADO: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol en ${elapsed}s`, "filter");
    state.stats.mig_rejected++; broadcast({ event: "stats", data: state.stats });
  }
}

function obsStartRecording(entry, entryPrice, velMs) {
  const rec = { mint: entry.mint, symbol: entry.symbol, vel: +(velMs/1000).toFixed(1),
    mc: entry.migratedMcUsd || (entryPrice*1_000_000_000), vol: Math.round(entry.volumeUSD),
    t0: Date.now(), entryPrice, puntos: [{t:0,p:0}], lastSample: Date.now(), mov2s: null, finished: false };
  state.obsRecordings.set(entry.mint, rec);
  state.stats.mig_entered++;
  addLog(`🔬 OBS GRABANDO: ${entry.symbol} | vel=${rec.vel}s MC=${formatMC(rec.mc)} — ${OBS_RECORD_MS/60000}min`, "accept");
  rec.timer = setTimeout(() => obsFinishRecording(entry.mint), OBS_RECORD_MS);
}

function obsSample(mint, price) {
  const rec = state.obsRecordings.get(mint);
  if (!rec || rec.finished) return;
  const dt = Date.now() - rec.t0;
  const interval = dt <= OBS_T1_MS ? OBS_T1_INTERVAL : dt <= OBS_T2_MS ? OBS_T2_INTERVAL : OBS_T3_INTERVAL;
  if (Date.now() - rec.lastSample < interval) return;
  rec.lastSample = Date.now();
  const pct = +((price - rec.entryPrice) / rec.entryPrice * 100).toFixed(2);
  rec.puntos.push({ t: Math.round(dt/1000), p: pct });
  if (rec.mov2s === null && dt >= 2000) rec.mov2s = pct;
}

function obsFinishRecording(mint) {
  const rec = state.obsRecordings.get(mint);
  if (!rec || rec.finished) return;
  rec.finished = true; state.obsRecordings.delete(mint); unsubscribeToken(mint);
  const pts = rec.puntos;
  let min = pts[0], max = pts[0];
  for (const pt of pts) { if (pt.p < min.p) min = pt; if (pt.p > max.p) max = pt; }
  const orden = min.t <= max.t ? "lava-antes" : "lava-despues";
  const cruces = [50, 70, 100].map(u => { let c=0; for (let i=1;i<pts.length;i++) if (pts[i-1].p<u&&pts[i].p>=u) c++; return c; });
  const cierreReal = obsSimulaGestionActual(pts);
  const mov2s = rec.mov2s === null ? "n/a" : `${rec.mov2s>=0?"+":""}${rec.mov2s}%`;
  const ptsRaw = pts.map(p=>`${p.t}:${p.p}`).join(",");
  addLog(`[REC] sym=${rec.symbol} vel=${rec.vel}s MC=${formatMC(rec.mc)} vol=${rec.vol} mov2s=${mov2s} MIN=${min.p}%@${min.t}s MAX=${max.p}%@${max.t}s orden=${orden} cruces[50,70,100]=${cruces[0]},${cruces[1]},${cruces[2]} cierre_real=${cierreReal>=0?"+":""}${cierreReal}% pts=${ptsRaw}`, "rec");
}

function obsSimulaGestionActual(pts) {
  const STEP_TRIGGER=70, STEP_FLOOR=13, TOP_FLOOR_TRIGGER=100, TOP_FLOOR=65;
  let armed=false, topFloor=false, maxSeen=0, sl=-20;
  for (const pt of pts) {
    maxSeen = Math.max(maxSeen, pt.p);
    if (!armed && maxSeen >= STEP_TRIGGER) armed = true;
    if (!topFloor && maxSeen >= TOP_FLOOR_TRIGGER) topFloor = true;
    if (armed) { const trail = maxSeen>=100?5:maxSeen>=60?8:maxSeen>=40?12:15; sl = Math.max(sl, maxSeen-trail, STEP_FLOOR); }
    if (topFloor) sl = Math.max(sl, TOP_FLOOR);
    if (pt.p <= sl) return +sl.toFixed(1);
  }
  return +pts[pts.length-1].p.toFixed(1);
}

function liveRecStart(entry, entryPrice) {
  if (!LIVE_RECORD || entryPrice <= 0) return;
  const velMs = Date.now() - entry.startTime;
  const rec = { mint: entry.mint, symbol: entry.symbol, vel: +(velMs/1000).toFixed(1),
    mc: entry.migratedMcUsd || (entryPrice*1_000_000_000), vol: Math.round(entry.volumeUSD||0),
    t0: Date.now(), entryPrice, puntos: [{t:0,p:0}], lastSample: Date.now(), mov2s: null, finished: false };
  state.liveRecordings.set(entry.mint, rec);
}

function liveRecSample(mint, price) {
  if (!LIVE_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished || price <= 0) return;
  const dt = Date.now() - rec.t0;
  const interval = dt <= LIVE_REC_DENSE_MS ? LIVE_REC_DENSE_INTERVAL : LIVE_REC_NORMAL_INTERVAL;
  if (Date.now() - rec.lastSample < interval) return;
  rec.lastSample = Date.now();
  const pct = +((price - rec.entryPrice) / rec.entryPrice * 100).toFixed(2);
  rec.puntos.push({ t: Math.round(dt/1000), p: pct });
  if (rec.mov2s === null && dt >= 2000) rec.mov2s = pct;
}

function liveRecFinish(mint, cierreRealPct) {
  if (!LIVE_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished) return;
  rec.finished = true; state.liveRecordings.delete(mint);
  const pts = rec.puntos;
  if (pts.length < 2) return;
  let min=pts[0], max=pts[0];
  for (const pt of pts) { if (pt.p<min.p) min=pt; if (pt.p>max.p) max=pt; }
  const orden = min.t<=max.t ? "lava-antes" : "lava-despues";
  const cruces = [10,15,20].map(u=>{let c=0;for(let i=1;i<pts.length;i++) if(pts[i-1].p<u&&pts[i].p>=u)c++;return c;});
  const mov2s = rec.mov2s===null?"n/a":`${rec.mov2s>=0?"+":""}${rec.mov2s}%`;
  const cr = +(+cierreRealPct).toFixed(1);
  const ptsRaw = pts.map(p=>`${p.t}:${p.p}`).join(",");
  addLog(`[MIGREC] sym=${rec.symbol} vel=${rec.vel}s MC=${formatMC(rec.mc)} vol=${rec.vol} mov2s=${mov2s} MIN=${min.p}%@${min.t}s MAX=${max.p}%@${max.t}s orden=${orden} cruces[10,15,20]=${cruces[0]},${cruces[1]},${cruces[2]} cierre_real=${cr>=0?"+":""}${cr}% pts=${ptsRaw}`, "rec");
}

function migOpenTrades(entry) {
  const price = entry.firstPrice;
  if (!price || price <= 0) return;
  liveRecStart(entry, price);
  const signal = {
    id: `mig-${entry.mint}-${Date.now()}`, strategy: "migration",
    mint: entry.mint, name: entry.name, symbol: entry.symbol,
    price, tp: +(price*MIG_TP).toFixed(12), sl: +(price*MIG_SL).toFixed(12),
    mcUsd: price*1_000_000_000, volumeUSD: entry.volumeUSD, time: Date.now(),
  };
  state.signals.unshift(signal);
  if (state.signals.length > 100) state.signals.pop();
  broadcast({ event: "newSignal", data: signal });
  if (!state.migMonitored.has(entry.mint)) {
    state.migMonitored.set(entry.mint, {
      mint: entry.mint, name: entry.name, symbol: entry.symbol,
      price, mc: price*1_000_000_000, priceHigh: price, priceLow: price,
      tradeCount: entry.tradeCount, volumeUSD: entry.volumeUSD,
      detectedAt: entry.startTime, lastUpdate: Date.now(),
    });
    broadcast({ event: "newMigToken", data: state.migMonitored.get(entry.mint) });
  }
  openDemoTrade(signal);
  openRealTrade(signal);
}

function migCleanup(mint, symbol) {
  unsubscribeToken(mint); state.migMonitored.delete(mint);
  broadcast({ event: "removeToken", data: { mint } });
  addLog(`🗑️ ${symbol} eliminado`, "info");
}

function migUpdatePrice(mint, price, solAmount) {
  const entry = state.migWatching.get(mint);
  if (entry) { migUpdateWatching(mint, price, solAmount, entry); return; }
  const token = state.migMonitored.get(mint);
  if (!token) return;
  if (!isPriceValid(price, token.price)) return;
  token.price = price; token.mc = price*1_000_000_000;
  token.priceHigh = Math.max(token.priceHigh, price);
  token.priceLow = Math.min(token.priceLow, price);
  token.tradeCount++; token.volumeUSD += solAmount*solPriceUSD;
  token.lastUpdate = Date.now();
  liveRecSample(mint, price);
  updateDemoTrades(mint, price, "migration");
  updateRealTrades(mint, price, "migration");
  broadcast({ event: "migTokenUpdate", data: token });
}

// ════════════════════════════════════════════════════════════════
// ESTRATEGIA 2: MOMENTUM  (DEMO — seguimiento SOLO con Birdeye)
// ════════════════════════════════════════════════════════════════

async function momentumScan() {
  seenMomPools.clear();
  let totalScanned = 0, totalSignals = 0;
  try {
    const url = `${BIRDEYE_TOKEN_LIST}?sort_by=volume_1h_usd&sort_type=desc`
      + `&min_liquidity=${MOM_MIN_LIQUIDITY}`
      + `&min_market_cap=${MOM_MIN_MC}&max_market_cap=${MOM_MAX_MC}`
      + `&offset=0&limit=50`;
    const res = await fetch(url, {
      headers: { "accept": "application/json", "x-chain": "solana", "X-API-KEY": BIRDEYE_API_KEY },
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) { addLog(`❌ Birdeye scan HTTP ${res.status}`, "error"); return; }
    const json = await res.json();
    const tokens = json?.data?.items || json?.data?.tokens || [];
    for (const tok of tokens) {
      const mint = tok.address || "";
      if (!mint || mint.length < 32) continue;
      const mc = parseFloat(tok.market_cap || tok.fdv || 0);
      if (mc < MOM_MIN_MC || mc > MOM_MAX_MC) continue;
      const vol1h = parseFloat(tok.volume_1h_usd || 0);
      const pct1h = parseFloat(tok.price_change_1h_percent || 0);
      const bePrice = parseFloat(tok.price || 0);
      if (bePrice <= 0) continue;
      const liquidity = parseFloat(tok.liquidity || 0);
      totalScanned++;
      // CAMBIO: si ya la monitoreamos, actualizamos su precio con el del SCAN (Birdeye),
      // que es la misma fuente que la entrada. Coherente. Antes esto venía mezclado.
      if (state.momMonitored.has(mint)) {
        const token = state.momMonitored.get(mint);
        token.vol1h = vol1h; token.pct1h = pct1h;
        if (bePrice > 0) momUpdatePrice(mint, bePrice);
        seenMomPools.add(mint); continue;
      }
      if (seenMomPools.has(mint)) continue;
      if (vol1h < MOM_MIN_VOL_1H) continue;
      if (!liquidity || liquidity < MOM_MIN_LIQUIDITY) {
        addLog(`⛔ MOM liquidez baja/desconocida (${formatMC(liquidity)}): ${tok.symbol||mint.slice(0,8)}`, "filter");
        state.stats.mom_disc_liquidity++; continue;
      }
      if (pct1h < MOM_MIN_PCT_1H || pct1h > MOM_MAX_PCT_1H) continue;
      const muteAt = momMuteCooldown.get(mint) || 0;
      if (Date.now() - muteAt < MOM_MUTE_COOLDOWN_MS) continue;
      const lastSig = momSignalCooldown.get(mint) || 0;
      if (Date.now() - lastSig < MOM_SIGNAL_COOLDOWN_MS) continue;
      seenMomPools.add(mint);
      state.stats.mom_scanned++;
      const symbol = tok.symbol || mint.slice(0, 8);
      momSignalCooldown.set(mint, Date.now());
      state.stats.mom_signals++; totalSignals++;
      state.momPending.set(mint, { mint, symbol, name: symbol, geckoPrice: bePrice, mc, vol1h, pct1h, pendingSince: Date.now() });
      state.stats.mom_pending = state.momPending.size;
      addLog(`⚡ MOMENTUM: ${symbol} | +${pct1h.toFixed(1)}% 1h | Vol ${formatMC(vol1h)} | MC ${formatMC(mc)}`, "signal");
      setTimeout(async () => {
        if (!state.momPending.has(mint)) return;
        const pending = state.momPending.get(mint);
        const freshPrice = await birdeyeFreshPrice(mint);
        if (!freshPrice) {
          addLog(`⛔ MOM sin precio fresco: ${pending.symbol}`, "filter");
          state.stats.mom_disc_noprice++;
          state.momPending.delete(mint); state.stats.mom_pending = state.momPending.size; return;
        }
        const drift = Math.abs(freshPrice - pending.geckoPrice) / pending.geckoPrice;
        if (drift > MOM_MAX_ENTRY_DRIFT) {
          addLog(`⛔ MOM drift ${(drift*100).toFixed(1)}% (${pending.symbol}) — NO entra`, "filter");
          state.stats.mom_disc_drift++;
          state.momPending.delete(mint); state.stats.mom_pending = state.momPending.size; return;
        }
        await new Promise(r => setTimeout(r, MOM_MUTE_CHECK_MS));
        if (!state.momPending.has(mint)) return;
        const secondPrice = await birdeyeFreshPrice(mint);
        if (!secondPrice) {
          addLog(`⛔ MOM 2ª lectura sin precio: ${pending.symbol}`, "filter");
          state.stats.mom_disc_noprice++;
          state.momPending.delete(mint); state.stats.mom_pending = state.momPending.size; return;
        }
        const move = Math.abs(secondPrice - freshPrice) / freshPrice;
        if (move < MOM_MUTE_MIN_MOVE) {
          addLog(`🔇 MOM MUDO en entrada: ${pending.symbol} — movió ${(move*100).toFixed(2)}% en ${MOM_MUTE_CHECK_MS/1000}s`, "filter");
          state.stats.mom_disc_mute++;
          momMuteCooldown.set(mint, Date.now());
          state.momPending.delete(mint); state.stats.mom_pending = state.momPending.size; return;
        }
        addLog(`⚡ ENTRADA [vivo]: ${pending.symbol} @ $${secondPrice.toFixed(8)} (movió ${(move*100).toFixed(2)}%)`, "accept");
        state.stats.mom_entered++;
        // entrada con precio Birdeye (secondPrice). El seguimiento también será Birdeye.
        momActivateFromPending(mint, secondPrice);
      }, MOM_PENDING_TIMEOUT_MS);
      broadcast({ event: "stats", data: state.stats });
    }
    addLog(`⚡ Scan: ${totalScanned} candidatos, ${totalSignals} señales nuevas`, "info");
    broadcast({ event: "stats", data: state.stats });
  } catch (e) { addLog(`❌ Momentum scan error: ${e.message}`, "error"); }
}

async function birdeyeFreshPrice(mint) {
  try {
    const res = await fetch(`${BIRDEYE_PRICE}?address=${mint}`, {
      headers: { "accept": "application/json", "x-chain": "solana", "X-API-KEY": BIRDEYE_API_KEY },
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) return null;
    const json = await res.json();
    const px = parseFloat(json?.data?.value || 0);
    return px > 0 ? px : null;
  } catch { return null; }
}

// CAMBIO (fix momentum): tracker dedicado. Cada MOM_TRACK_MS pide en UNA llamada
// batch (multi_price) el precio Birdeye de TODAS las posiciones de momentum abiertas
// y alimenta updateDemoTrades. Misma fuente que la entrada → sin saltos fantasma.
// Coste Birdeye: 1 batch por ciclo. Con N posiciones: ceil(N^0.8 * 3) CU. Despreciable.
async function momTrackTick() {
  const mints = Array.from(state.momMonitored.keys());
  if (mints.length === 0) return;
  try {
    const url = `${BIRDEYE_MULTI_PRICE}?list_address=${mints.join(",")}`;
    const res = await fetch(url, {
      headers: { "accept": "application/json", "x-chain": "solana", "X-API-KEY": BIRDEYE_API_KEY },
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) { addLog(`⚠️ MOM track HTTP ${res.status}`, "warn"); return; }
    const json = await res.json();
    const data = json?.data || {};
    for (const mint of mints) {
      const entry = data[mint];
      const px = entry ? parseFloat(entry.value || 0) : 0;
      if (px > 0) momUpdatePrice(mint, px);
    }
  } catch (e) { addLog(`⚠️ MOM track error: ${e.message}`, "warn"); }
}

function momActivateFromPending(mint, entryPrice) {
  const pending = state.momPending.get(mint);
  if (!pending) return;
  state.momPending.delete(mint); state.stats.mom_pending = state.momPending.size;
  const signal = {
    id: `mom-${mint}-${Date.now()}`, strategy: "momentum",
    mint, name: pending.name, symbol: pending.symbol,
    price: entryPrice, tp: +(entryPrice*MOM_TP).toFixed(12), sl: +(entryPrice*MOM_SL).toFixed(12),
    mcUsd: pending.mc, vol1h: pending.vol1h, pct1h: pending.pct1h, time: Date.now(),
  };
  addLog(`⚡ ENTRADA [birdeye]: ${pending.symbol} @ $${entryPrice.toFixed(8)} | TP +6% SL -3% | 45min`, "accept");
  state.signals.unshift(signal);
  if (state.signals.length > 100) state.signals.pop();
  broadcast({ event: "newSignal", data: signal });
  state.momMonitored.set(mint, {
    mint, symbol: pending.symbol, name: pending.name,
    mc: pending.mc, price: entryPrice, priceHigh: entryPrice, priceLow: entryPrice,
    pct1h: pending.pct1h, vol1h: pending.vol1h,
    tradeCount: 0, volumeUSD: 0,
    detectedAt: Date.now(), lastUpdate: Date.now(),
  });
  broadcast({ event: "newMomToken", data: state.momMonitored.get(mint) });
  momRecStart(mint, pending.symbol, entryPrice, { mc: pending.mc, vol: pending.vol1h, pct1h: pending.pct1h });
  openDemoTrade(signal);
  // v6.18.4: momentum REAL reactivado con límite estricto de 1 op (MAX_MOM_REAL) para medir el costo real on-chain. La demo sigue en paralelo para no perder el registro.
  openRealTrade(signal);
}

// CAMBIO: momUpdatePrice de momentum ya no recibe solAmount (no viene de PumpPortal).
// Solo precio de Birdeye, una sola escala. Sin volumen on-chain de PumpPortal.
function momUpdatePrice(mint, price) {
  const token = state.momMonitored.get(mint);
  if (!token) return;
  if (price <= 0) return;
  token.price = price; token.mc = price * 1_000_000_000;
  token.priceHigh = Math.max(token.priceHigh, price);
  token.priceLow = Math.min(token.priceLow, price);
  token.lastUpdate = Date.now();
  momRecSample(mint, price);
  updateDemoTrades(mint, price, "momentum");
  updateRealTrades(mint, price, "momentum");   // v6.18.5: FIX — las posiciones REALES de momentum también deben recibir el precio del tracker, o el SL/TP nunca se disparan (quedaban congeladas en 0% y desprotegidas).
  broadcast({ event: "momTokenUpdate", data: token });
}

function momCleanup(mint, symbol) {
  state.momMonitored.delete(mint);
  // ya no se suscribe momentum a PumpPortal, pero por si quedó algo de una versión previa:
  unsubscribeToken(mint);
  broadcast({ event: "removeToken", data: { mint } });
}

// ════════════════════════════════════════════════════════════════
// TRADING COMPARTIDO
// ════════════════════════════════════════════════════════════════

async function getSolDeltaFromTx(sig, retries = 6) {
  if (!wallet || !connection) return null;
  const me = wallet.publicKey.toString();
  for (let i = 0; i < retries; i++) {
    try {
      const tx = await connection.getParsedTransaction(sig, { maxSupportedTransactionVersion: 0, commitment: "confirmed" });
      if (tx?.meta && tx.transaction?.message?.accountKeys) {
        const keys = tx.transaction.message.accountKeys;
        let idx = -1;
        for (let k = 0; k < keys.length; k++) {
          const pk = keys[k]?.pubkey ? keys[k].pubkey.toString() : keys[k]?.toString?.();
          if (pk === me) { idx = k; break; }
        }
        if (idx >= 0 && tx.meta.preBalances && tx.meta.postBalances) {
          return +((tx.meta.postBalances[idx] - tx.meta.preBalances[idx]) / LAMPORTS_PER_SOL).toFixed(6);
        }
      }
    } catch (e) { /* reintentar */ }
    await new Promise(r => setTimeout(r, 1500));
  }
  addLog(`⚠️ No se pudo leer SOL movido de tx ${shortAddr(sig)}`, "warn");
  return null;
}

async function buyToken(mint, solAmount) {
  if (!wallet || !connection) return null;
  try {
    const response = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "buy", mint, denominatedInSol: "true", amount: solAmount, slippage: 15, priorityFee: 0.0005, pool: "auto" }),
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) { addLog(`❌ Compra error: ${response.status}`, "error"); return null; }
    const tx = VersionedTransaction.deserialize(new Uint8Array(await response.arrayBuffer()));
    tx.sign([wallet]);
    const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: "confirmed" });
    await connection.confirmTransaction(sig, "confirmed");
    const delta = await getSolDeltaFromTx(sig);
    const costSol = delta != null ? +(-delta).toFixed(6) : solAmount;
    addLog(`✅ COMPRA: ${shortAddr(mint)} | coste real ${costSol} SOL | ${sig}`, "real");
    return { sig, costSol };
  } catch (e) { addLog(`❌ Compra: ${e.message}`, "error"); return null; }
}

async function sellToken(mint) {
  if (!wallet || !connection) return null;
  try {
    const bal = await getTokenBalance(mint);
    if (bal <= 0) { addLog(`⚠️ Sin tokens: ${shortAddr(mint)}`, "warn"); return null; }
    const response = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "sell", mint, denominatedInSol: "false", amount: bal, slippage: 15, priorityFee: 0.0005, pool: "auto" }),
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) { addLog(`❌ Venta error: ${response.status}`, "error"); return null; }
    const tx = VersionedTransaction.deserialize(new Uint8Array(await response.arrayBuffer()));
    tx.sign([wallet]);
    const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: "confirmed" });
    await connection.confirmTransaction(sig, "confirmed");
    const delta = await getSolDeltaFromTx(sig);
    const proceedsSol = delta != null ? +delta.toFixed(6) : 0;
    addLog(`✅ VENTA: ${shortAddr(mint)} | recibido real ${proceedsSol} SOL | ${sig}`, "real");
    return { sig, proceedsSol };
  } catch (e) { addLog(`❌ Venta: ${e.message}`, "error"); return null; }
}

async function openRealTrade(signal) {
  if (!wallet) return;
  if (!REAL_STRATEGIES.includes(signal.strategy)) return;
  // CAMBIO (kill-switch): no abrir nuevas posiciones reales si la operativa está pausada.
  if (tradingHalted()) {
    const snap = riskSnapshot();
    addLog(`🛑 Entrada real BLOQUEADA por kill-switch: ${signal.symbol} | diario ${snap.dailyPnlSol.toFixed(3)} SOL · pausa ${(snap.pausedMsLeft/60000).toFixed(0)}min`, "warn");
    return;
  }
  const openReal = state.realTrades.filter(t => t.status === "OPEN");
  if (openReal.length >= MAX_REAL_TRADES) return;
  const stratOpen = openReal.filter(t => t.strategy === signal.strategy).length;
  const isMigStrat = signal.strategy === "migration";
  const maxForStrat = isMigStrat ? MAX_MIG_REAL : MAX_MOM_REAL;
  if (stratOpen >= maxForStrat) { addLog(`⚠️ Límite real [${signal.strategy}]: ${stratOpen}/${maxForStrat}`, "warn"); return; }
  const solAmount = isMigStrat ? SOL_PER_TRADE_MIG : SOL_PER_TRADE_MOM;
  const durationForStrat = isMigStrat ? MIG_DURATION_MS : MOM_DURATION_MS;
  const balance = await getWalletBalance();
  if (balance < solAmount + 0.01) { addLog(`⚠️ Balance insuficiente: ${balance.toFixed(3)} SOL (necesito ${(solAmount+0.01).toFixed(2)})`, "warn"); return; }
  const buy = await buyToken(signal.mint, solAmount);
  if (!buy) return;
  const trade = {
    id: `real-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    strategy: signal.strategy, mint: signal.mint, symbol: signal.symbol, name: signal.name,
    entryPrice: signal.price, tp: signal.tp, sl: signal.sl, solAmount,
    costSol: buy.costSol, buySignature: buy.sig, sellSignature: null,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, pnlSol: null,
    maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: "INITIAL", status: "OPEN",
    expiresAt: Date.now() + durationForStrat, sellRetries: 0,
  };
  state.realTrades.unshift(trade);
  if (state.realTrades.length > 200) state.realTrades.pop();
  state.stats.realOpen++;
  state.stats.walletBalance = await getWalletBalance(true);
  broadcast({ event: "newRealTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  addLog(`🔴 REAL [${signal.strategy}]: ${signal.symbol} | ${solAmount} SOL`, "real");
  saveState();
}

async function closeRealTrade(trade, price, reason) {
  if (trade.status !== "OPEN") return;
  trade.status = "CLOSING";
  const sell = await sellToken(trade.mint);
  if (!sell) {
    trade.sellRetries = (trade.sellRetries || 0) + 1;
    if (trade.sellRetries <= 3) { trade.status = "OPEN"; setTimeout(() => closeRealTrade(trade, price, reason), 15000); return; }
    trade.status = "SELL_FAILED"; broadcast({ event: "realTradeClosed", data: trade }); return;
  }
  const proceedsSol = sell.proceedsSol;
  const costSol = (trade.costSol != null && trade.costSol > 0) ? trade.costSol : trade.solAmount;
  const realPnlSol = +(proceedsSol - costSol).toFixed(4);
  const tickPnlSol = +(costSol * (price - trade.entryPrice) / trade.entryPrice).toFixed(4);
  const slipFeeSol = +(realPnlSol - tickPnlSol).toFixed(4);
  trade.sellSignature = sell.sig; trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
  const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
  trade.pnlPct = +pnlPct.toFixed(2); trade.pnlSol = realPnlSol; trade.slipFeeSol = slipFeeSol;
  addLog(`📊 PnL real: ${realPnlSol>=0?"+":""}${realPnlSol} SOL (coste ${costSol} → recibido ${proceedsSol}) | tick: ${tickPnlSol>=0?"+":""}${tickPnlSol} | slip+fee: ${slipFeeSol>=0?"+":""}${slipFeeSol}`, "real");
  // CAMBIO (kill-switch): registrar el resultado real en el control de riesgo.
  riskRecordClose(realPnlSol);
  const dur = Math.round((trade.closeTime - trade.openTime) / 1000);
  const prefix = stratPrefix(trade.strategy);
  const expWinPct = isMig(trade.strategy) ? MIG_EXPIRED_WIN_PCT : MOM_EXPIRED_WIN_PCT;
  if (reason === "TP" || reason === "STEP" || (reason === "SL" && trade.pnlPct >= 0)) {
    trade.result = "WIN"; state.stats[`${prefix}_realWins`]++;
    state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
    addLog(`✅ REAL WIN [${reason==="STEP"?"🪜 ESCALÓN":trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "realwin");
  } else if (reason === "SL") {
    trade.result = "LOSS"; state.stats[`${prefix}_realLosses`]++;
    state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
    addLog(`❌ REAL LOSS [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "realloss");
  } else {
    state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
    if (trade.pnlPct >= expWinPct) { trade.result = "WIN"; state.stats[`${prefix}_realWins`]++; addLog(`✅ REAL WIN [EXP+]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "realwin"); }
    else { trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS"; addLog(`⏱️ REAL EXP: ${trade.symbol} ${trade.pnlPct>0?"+":""}${trade.pnlPct}%`, "real"); }
  }
  state.stats.realOpen = Math.max(0, state.stats.realOpen - 1);
  state.stats.walletBalance = await getWalletBalance(true);
  if (isMig(trade.strategy)) migCleanup(trade.mint, trade.symbol);
  if (trade.strategy === "momentum") momCleanup(trade.mint, trade.symbol);
  broadcast({ event: "realTradeClosed", data: trade });
  broadcast({ event: "stats", data: state.stats });
  saveState();
}

function migTrailingPct(maxGainPct) {
  if (maxGainPct >= MIG_TRAIL_T3) return MIG_TRAIL_P4;
  if (maxGainPct >= MIG_TRAIL_T2) return MIG_TRAIL_P3;
  if (maxGainPct >= MIG_TRAIL_T1) return MIG_TRAIL_P2;
  return MIG_FOLLOW_PCT_STEP;
}

function isMig(strategy) { return strategy === "migration"; }

function veloDropTriggered(trade, price, strategy) {
  if (!isMig(strategy)) return false;
  const now = Date.now();
  const prevP = trade._veloPrice, prevT = trade._veloTime;
  trade._veloPrice = price; trade._veloTime = now;
  const bajoEntrada = price < trade.entryPrice;
  const escalonArmado = trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9;
  if (!bajoEntrada || escalonArmado) return false;
  if (prevP == null || prevT == null) return false;
  const dt = now - prevT;
  if (dt > MIG_VELO_MS) return false;
  return (prevP - price) / prevP >= MIG_VELO_DROP;
}

function stratPrefix(strategy) { return strategy === "migration" ? "mig" : "mom"; }

function updateRealTrades(mint, price, strategy) {
  const now = Date.now();
  const breakeven = isMig(strategy) ? MIG_BREAKEVEN_AT : MOM_BREAKEVEN_AT;
  const breakevenMargin = isMig(strategy) ? MIG_BREAKEVEN_MARGIN : 0;
  const lock = isMig(strategy) ? MIG_LOCK_AT : MOM_LOCK_AT;
  const follow = isMig(strategy) ? MIG_FOLLOW_PCT : MOM_FOLLOW_PCT;
  for (const trade of state.realTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    if (veloDropTriggered(trade, price, strategy)) {
      addLog(`⚡🛑 VELO-EXIT [${strategy} real]: ${trade.symbol} caída rápida @ ${currentPct.toFixed(1)}%`, "realloss");
      closeRealTrade(trade, price, "SL"); continue;
    }
    if (strategy === "momentum" && currentPct <= MOM_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [momentum real]: ${trade.symbol} ${currentPct.toFixed(1)}%`, "realloss");
      closeRealTrade(trade, price, "SL"); continue;
    }
    if (isMig(strategy) && currentPct <= MIG_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [${strategy} real]: ${trade.symbol} ${currentPct.toFixed(1)}%`, "realloss");
      closeRealTrade(trade, price, "SL"); continue;
    }
    const stepArmed = isMig(strategy) && trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9;
    const followEff = (isMig(strategy) && stepArmed) ? migTrailingPct(trade.maxGainPct) : follow;
    if (trade.trailingPhase === "FOLLOWING") {
      const newSl = price * (1 - followEff); if (newSl > trade.sl) trade.sl = +newSl.toFixed(12);
    } else if ((price - trade.entryPrice) / trade.entryPrice >= lock) {
      trade.trailingPhase = "FOLLOWING";
      trade.sl = +Math.max(trade.sl, price * (1 - followEff)).toFixed(12);
    } else if ((price - trade.entryPrice) / trade.entryPrice >= breakeven && trade.trailingPhase === "INITIAL") {
      trade.trailingPhase = "BREAKEVEN";
      trade.sl = +(trade.entryPrice * (1 - breakevenMargin)).toFixed(12);
    }
    const stepFloorPrice = trade.entryPrice * (1 + MIG_STEP_FLOOR);
    if (stepArmed && stepFloorPrice > trade.sl) trade.sl = +stepFloorPrice.toFixed(12);
    // suelo +65% solo para migración (no debe encadenarse con el cierre por SL)
    if (isMig(strategy) && trade.maxGainPct >= MIG_TOP_FLOOR_TRIGGER) {
      const topFloorPrice = trade.entryPrice * (1 + MIG_TOP_FLOOR);
      if (topFloorPrice > trade.sl) trade.sl = +topFloorPrice.toFixed(12);
    }
    // v6.18.6 FIX CRÍTICO: el cierre por TP NO existía en updateRealTrades.
    // Las operaciones reales nunca cerraban por Take Profit (solo por SL/trailing/exp).
    // Si el token subía directo al TP, la real no lo recogía. Ahora sí, igual que la demo.
    if (price >= trade.tp) { trade._slBelowCount = 0; closeRealTrade(trade, price, "TP"); }
    // v6.18.6 FIX: el cierre por SL/trailing ahora es independiente (antes colgaba de un
    // else-if del suelo de migración, y para momentum quedaba sin evaluarse bien → la real
    // no cerraba aunque el trailing hubiera saltado).
    else if (price <= trade.sl) {
      if (trade.sl >= trade.entryPrice) {
        closeRealTrade(trade, price, (stepArmed && Math.abs(trade.sl - stepFloorPrice) < 1e-9) ? "STEP" : "SL");
      } else {
        trade._slBelowCount = (trade._slBelowCount || 0) + 1;
        if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS) { closeRealTrade(trade, price, "SL"); }
        else { addLog(`⏳ SL sin confirmar [real] (${trade._slBelowCount}/${MIG_SL_CONFIRM_TICKS}): ${trade.symbol}`, "trail"); broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } }); }
      }
    } else {
      trade._slBelowCount = 0;
      if (now >= trade.expiresAt) closeRealTrade(trade, price, "EXPIRED");
      else broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
    }
  }
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.realTrades) {
    if (trade.status !== "OPEN") continue;
    // DEAD_FEED: sin ticks a los 30s → cerrar (v6.16.9)
    const sinceOpen = now - trade.openTime;
    if (isMig(trade.strategy) && sinceOpen >= 30_000 && trade.maxGainPct === 0 && trade.maxLossPct === 0) {
      addLog(`💀 FEED MUERTO [migration real]: ${trade.symbol} sin ticks en ${Math.round(sinceOpen/1000)}s`, "realloss");
      const token = state.migMonitored.get(trade.mint);
      closeRealTrade(trade, token?.price || trade.entryPrice, "DEAD_FEED"); continue;
    }
    // v6.18.5: feed mudo para momentum REAL (umbral 90s, coherente con el tracker Birdeye de 15s y con la demo de momentum). Sin esto, una posición real sin ticks quedaba desprotegida hasta los 45min.
    if (trade.strategy === "momentum" && sinceOpen >= MOM_MUTE_TIMEOUT_MS && trade.maxGainPct === 0 && trade.maxLossPct === 0) {
      addLog(`🔇 MOM FEED MUDO [real]: ${trade.symbol} sin ticks en ${Math.round(sinceOpen/1000)}s — cerrando`, "realloss");
      momMuteCooldown.set(trade.mint, Date.now());
      const token = state.momMonitored.get(trade.mint);
      closeRealTrade(trade, token?.price || trade.entryPrice, "EXPIRED"); continue;
    }
    if (now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint) || state.momMonitored.get(trade.mint);
    closeRealTrade(trade, token?.price || trade.entryPrice, "EXPIRED");
  }
}, 10_000);

function openDemoTrade(signal) {
  const duration = isMig(signal.strategy) ? MIG_DURATION_MS : MOM_DURATION_MS;
  const trade = {
    id: `demo-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    strategy: signal.strategy, mint: signal.mint, symbol: signal.symbol, name: signal.name,
    entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: "INITIAL", status: "OPEN",
    expiresAt: Date.now() + duration, mov1s: null, mov2s: null,
  };
  state.demoTrades.unshift(trade);
  if (state.demoTrades.length > 500) state.demoTrades.pop();
  state.stats.demoOpen++;
  broadcast({ event: "newDemoTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  const tpPct = isMig(signal.strategy) ? "+300%" : "+6%";
  const slPct = isMig(signal.strategy) ? "-20%" : "-3%";
  addLog(`📝 DEMO [${signal.strategy}]: ${signal.symbol} | TP ${tpPct} SL ${slPct}`, "demo");
}

function updateDemoTrades(mint, price, strategy) {
  const now = Date.now();
  const tp_pct = isMig(strategy) ? MIG_TP : MOM_TP;
  const breakeven = isMig(strategy) ? MIG_BREAKEVEN_AT : MOM_BREAKEVEN_AT;
  const breakevenMargin = isMig(strategy) ? MIG_BREAKEVEN_MARGIN : 0;
  const lock = isMig(strategy) ? MIG_LOCK_AT : MOM_LOCK_AT;
  const follow = isMig(strategy) ? MIG_FOLLOW_PCT : MOM_FOLLOW_PCT;
  for (const trade of state.demoTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    if (isMig(strategy)) {
      const sinceOpen = now - trade.openTime;
      if (trade.mov1s === null && sinceOpen >= 1000) trade.mov1s = +currentPct.toFixed(2);
      if (trade.mov2s === null && sinceOpen >= 2000) trade.mov2s = +currentPct.toFixed(2);
    }
    if (veloDropTriggered(trade, price, strategy)) {
      addLog(`⚡🛑 VELO-EXIT [${strategy}]: ${trade.symbol} caída rápida @ ${currentPct.toFixed(1)}%`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct); continue;
    }
    if (strategy === "momentum" && currentPct <= MOM_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [momentum]: ${trade.symbol} ${currentPct.toFixed(1)}%`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct); continue;
    }
    if (isMig(strategy) && currentPct <= MIG_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [${strategy}]: ${trade.symbol} ${currentPct.toFixed(1)}%`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct); continue;
    }
    const stepArmed = isMig(strategy) && trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9;
    const followEff = (isMig(strategy) && stepArmed) ? migTrailingPct(trade.maxGainPct) : follow;
    if (trade.trailingPhase === "FOLLOWING") {
      const newSl = price * (1 - followEff); if (newSl > trade.sl) trade.sl = +newSl.toFixed(12);
    } else if ((price - trade.entryPrice) / trade.entryPrice >= lock) {
      trade.trailingPhase = "FOLLOWING";
      trade.sl = +Math.max(trade.sl, price * (1 - followEff)).toFixed(12);
      addLog(`🔄 FOLLOWING [${strategy}]: ${trade.symbol}`, "trail");
    } else if ((price - trade.entryPrice) / trade.entryPrice >= breakeven && trade.trailingPhase === "INITIAL") {
      trade.trailingPhase = "BREAKEVEN";
      trade.sl = +(trade.entryPrice * (1 - breakevenMargin)).toFixed(12);
      addLog(`⚖️ BREAKEVEN [${strategy}]: ${trade.symbol}`, "trail");
    }
    const stepFloorPrice = trade.entryPrice * (1 + MIG_STEP_FLOOR);
    if (stepArmed && stepFloorPrice > trade.sl) {
      if (!trade._stepLogged) { trade._stepLogged = true; addLog(`🪜 ESCALÓN +13% suelo [${strategy}]: ${trade.symbol}`, "trail"); }
      trade.sl = +stepFloorPrice.toFixed(12);
    }
    if (isMig(strategy) && trade.maxGainPct >= MIG_TOP_FLOOR_TRIGGER) {
      const topFloorPrice = trade.entryPrice * (1 + MIG_TOP_FLOOR);
      if (topFloorPrice > trade.sl) {
        if (!trade._topFloorLogged) { trade._topFloorLogged = true; addLog(`🏔️ SUELO +65% [${strategy}]: ${trade.symbol}`, "trail"); }
        trade.sl = +topFloorPrice.toFixed(12);
      }
    }
    if (price >= trade.tp) { trade._slBelowCount = 0; closeDemoTrade(trade, price, "TP", tp_pct); }
    else if (price <= trade.sl) {
      if (trade.sl >= trade.entryPrice) {
        closeDemoTrade(trade, trade.sl, (stepArmed && Math.abs(trade.sl - stepFloorPrice) < 1e-9) ? "STEP" : "SL", tp_pct);
      } else {
        trade._slBelowCount = (trade._slBelowCount || 0) + 1;
        if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS) { closeDemoTrade(trade, price, "SL", tp_pct); }
        else { addLog(`⏳ SL sin confirmar (${trade._slBelowCount}/${MIG_SL_CONFIRM_TICKS}): ${trade.symbol} @ ${(trade.currentPct||0).toFixed(1)}%`, "trail"); broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } }); }
      }
    } else {
      trade._slBelowCount = 0;
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
  const prefix = stratPrefix(trade.strategy);
  const expWinPct = isMig(trade.strategy) ? MIG_EXPIRED_WIN_PCT : MOM_EXPIRED_WIN_PCT;
  if (reason === "TP") {
    trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++;
    state.stats[`${prefix}_demoPnL`] += (tp_pct - 1) * 100;
    addLog(`✅ WIN [TP][${trade.strategy}]: ${trade.symbol} +${((tp_pct-1)*100).toFixed(0)}% en ${dur}s`, "win");
  } else if (reason === "STEP") {
    trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++;
    state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
    addLog(`✅ WIN [🪜 ESCALÓN][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win");
  } else if (reason === "SL") {
    state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
    if (trade.pnlPct > 0) { trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++; addLog(`✅ WIN [${trade.trailingPhase}][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win"); }
    else { trade.result = "LOSS"; state.stats[`${prefix}_demoLosses`]++; addLog(`❌ LOSS [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "loss"); }
  } else {
    state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
    if (trade.pnlPct >= expWinPct) { trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++; addLog(`✅ WIN [EXP+][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win"); }
    else if (trade.pnlPct <= -expWinPct) { trade.result = "LOSS"; state.stats[`${prefix}_demoLosses`]++; addLog(`❌ LOSS [EXP-][${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "loss"); }
    else { trade.result = "EXPIRED"; state.stats[`${prefix}_demoExpired`]++; addLog(`⏱️ EXP [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct>0?"+":""}${trade.pnlPct}%`, "expire"); }
  }
  state.stats.demoOpen = Math.max(0, state.stats.demoOpen - 1);
  state.stats[`${prefix}_maxGainSum`] += trade.maxGainPct || 0;
  state.stats[`${prefix}_maxLossSum`] += Math.abs(trade.maxLossPct || 0);
  state.stats[`${prefix}_closedCount`]++;
  state.stats[`${prefix}_avgMaxGain`] = +(state.stats[`${prefix}_maxGainSum`] / state.stats[`${prefix}_closedCount`]).toFixed(1);
  state.stats[`${prefix}_avgMaxLoss`] = +(state.stats[`${prefix}_maxLossSum`] / state.stats[`${prefix}_closedCount`]).toFixed(1);
  if (isMig(trade.strategy) && trade.mov2s !== null && trade.result !== "EXPIRED") {
    const bucket = trade.mov2s > 1 ? "up" : (trade.mov2s < -1 ? "down" : "flat");
    state.stats[`mig_mov_${bucket}_${trade.result === "WIN" ? "win" : "loss"}`]++;
  }
  if (isMig(trade.strategy)) liveRecFinish(trade.mint, trade.pnlPct);
  if (trade.strategy === "momentum") momRecFinish(trade.mint, trade.pnlPct);
  if (isMig(trade.strategy)) migCleanup(trade.mint, trade.symbol);
  if (trade.strategy === "momentum") momCleanup(trade.mint, trade.symbol);
  broadcast({ event: "stats", data: state.stats });
  saveState();
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.status !== "OPEN") continue;
    if (trade.strategy === "momentum") {
      const aliveMs = now - trade.openTime;
      if (aliveMs >= MOM_MUTE_TIMEOUT_MS && trade.maxGainPct === 0 && trade.maxLossPct === 0) {
        const tk = state.momMonitored.get(trade.mint);
        addLog(`🔇 MOM FEED MUDO: ${trade.symbol} — sin trades en ${Math.round(aliveMs/1000)}s`, "warn");
        momMuteCooldown.set(trade.mint, Date.now());
        closeDemoTrade(trade, tk?.price || trade.entryPrice, "EXPIRED", MOM_TP); continue;
      }
    }
    if (now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint) || state.momMonitored.get(trade.mint);
    const tp_pct = isMig(trade.strategy) ? MIG_TP : MOM_TP;
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
    // CAMBIO: momentum YA NO se suscribe a PumpPortal (se sigue por Birdeye).
    // Solo migración usa el feed de PumpPortal.
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
        if (OBSERVER_MODE && state.obsRecordings.has(data.mint)) obsSample(data.mint, price);
        // CAMBIO: momentum eliminado de aquí. PumpPortal solo alimenta migración.
      }
    } catch (e) { console.log("PP:", e.message); }
  });
  pumpPortalWs.on("error", (err) => addLog(`❌ PumpPortal: ${err.message}`, "error"));
  pumpPortalWs.on("close", () => { addLog("🔄 PumpPortal reconectando...", "warn"); setTimeout(connectPumpPortal, 5000); });
}

function connectHelius() {
  addLog("ℹ️ Helius desactivado — precios via PumpPortal + Birdeye", "info");
}

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

// CAMBIO: endpoint para ver/operar el kill-switch desde el frontend.
app.get("/api/risk", (req, res) => res.json(riskSnapshot()));
// Reanudar manualmente la operativa real (limpia la pausa por racha; el tope
// diario sigue vigente hasta el cambio de día UTC).
app.post("/api/risk/resume", (req, res) => {
  riskState.pausedUntil = 0;
  riskState.consecutiveLosses = 0;
  addLog("▶️ Kill-switch: operativa real reanudada manualmente", "info");
  broadcast({ event: "risk", data: riskSnapshot() });
  saveState();
  res.json({ ok: true, risk: riskSnapshot() });
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
    risk: riskSnapshot(),
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
      risk: riskSnapshot(),
      wsStatus: "connected",
    }
  }));
  ws.on("close", () => frontendClients.delete(ws));
});

server.listen(PORT, async () => {
  console.log(`🚀 SolScanBot v6.18.6 — fix momentum real (precio+feed mudo) + límite 1 + TP +12% | momentum Birdeye + params v6.6 (lock +5%, mudo 0.03%) + reconciliación + kill-switch | OBSERVER ${OBSERVER_MODE ? "ACTIVO ⚠️" : "off"} | MAX_MIG_REAL: ${MAX_MIG_REAL} × ${SOL_PER_TRADE_MIG} SOL | scan mom ${MOM_SCAN_MS/1000}s | track mom ${MOM_TRACK_MS/1000}s | kill: -${RISK.maxDailyLossSol} SOL/día, ${RISK.maxConsecutiveLosses} losses`);
  // avisos de secretos faltantes
  if (!BIRDEYE_API_KEY) addLog("⚠️ Falta BIRDEYE_API_KEY en el entorno — el scan/track de momentum fallará", "warn");
  if (!HELIUS_API_KEY && !process.env.SOLANA_RPC) addLog("⚠️ Sin HELIUS_API_KEY ni SOLANA_RPC — usando RPC público (lento, puede limitar)", "warn");
  loadState();
  initWallet();
  connectPumpPortal();
  connectHelius();
  // CAMBIO: reconciliar estado contra la wallet ANTES de operar.
  await reconcileStateOnBoot();
  // scan de descubrimiento de momentum
  setTimeout(momentumScan, 5000);
  setInterval(momentumScan, MOM_SCAN_MS);
  // CAMBIO: tracker de precio de momentum (batch Birdeye), independiente del scan.
  setInterval(momTrackTick, MOM_TRACK_MS);
});
