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
// ── MODO DEMO ONLY ──
// true = solo opera en DEMO (papel), NO toca la wallet real. Para probar
// la nueva estrategia (filtro entrada + trailing +25%) sin arriesgar dinero.
const DEMO_ONLY = true;
const SOL_PER_TRADE_MIG = 0.5;
const MAX_REAL_TRADES = 6;
const MAX_MIG_REAL = 6;
const REAL_STRATEGIES = ["migration"];
// MISMO STATE_FILE que el server combinado: NO se pierde historial ni kill-switch.
const STATE_FILE = process.env.STATE_FILE
  || (fs.existsSync("/var/data") ? "/var/data/solscanbot_state.json" : "./solscanbot_state.json");

// ── CONFIG MIGRACIÓN ───────────────────────────────────────────
const MIG_TP = 10.00;                  // TP +1000% (era +300%)
const MIG_SL = 0.40;                   // SL -60% desde entrada (era -20%). Config agresiva/alta varianza.
const MIG_DURATION_MS = 30 * 60 * 1000; // red de seguridad 30min (antes 15). La posición se cierra por SL/escalón/estructura; esto solo evita quedarse colgado indefinidamente.
const MIG_WINDOW_MS = 60_000;
const MIG_MIN_VOL_FAST = 1_500;
const MIG_MIN_VOL_SLOW = 2_000;
const MIG_FAST_WINDOW_MS = 20_000;
const MIG_MIN_MC = 0;
const MIG_MAX_MC = 2_000_000;
// v6.20.2: tope de MC en el MOMENTO DE ENTRADA (primer tick real, no el evento).
// El evento de migración casi siempre llega con MC=? (mcSol:?), así que MIG_MAX_MC
// no filtra nada. Este sí: rechaza entrar en tokens ya inflados (MC alto = a menudo
// pump-para-rugear / honeypot). El 29-jun, 3 tokens de MC $228-326K dieron "venta a
// cero" (honeypots) y se comieron -1.48 SOL (98% de la pérdida del día). Las ganadoras
// validadas entran a $20-50K.
// v6.20.3: el MC NO discrimina rendimiento (análisis de 150 ops/7 días: WR ~27%
// en TODOS los rangos de MC). Este tope queda SOLO como cortafuegos anti-honeypot
// extremo (los rug de "venta a cero" del 29-jun eran de MC $228-326K). No filtra
// rendimiento, solo evita el riesgo de honeypot de MC muy alto.
const MIG_MAX_MC_ENTRY = 150_000;

// ── v6.20.3: CORTE POR NO-DESPEGUE ─────────────────────────────
// Hallazgo clave (150 ops/7 días): el predictor real del éxito NO es el MC sino
// el DESPEGUE rápido. Las ops que a los 15s no han tocado +10% tienen WR ~0%
// (64 de 64 perdieron). Las que despegan >+30% ganan el 91% de las veces.
// Regla: a los 15s de abrir, si el máximo alcanzado < +10%, salir ya.
// Backtest: corta 36/49 perdedoras sacrificando solo 2/20 ganadoras (ratio 18:1).
// Sube la media de +7.5%/op a ~+13.6%/op. Aplica en real y en demo.
const MIG_LAUNCH_CHECK = false;      // DESACTIVADO: mataba 14 ganadoras (ratio 1:1.4 malo). Muchos cohetes despegan a los 5-11min, el corte a 18s los ejecutaba antes.
const MIG_LAUNCH_CHECK_MS = 18_000;  // a los 18s de abrir (era 15s; subido a 18s tras ver que ganadoras que despegan justo en el límite —ej. +62% que a 15s iba +8%— se cortaban; 18s salva esas perdiendo solo 1 perdedora más)
const MIG_LAUNCH_MIN_PCT = 10;       // exige haber tocado +10% (maxGainPct)

// ── MODO OBSERVADOR / GRABACIÓN EN VIVO ────────────────────────
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

// ── MODO MC_OBSERVER ──
// ACTIVADO para grabar 20 min y captar la FIRMA DEL RUG que descubrimos:
//   - MC de NACIMIENTO (1er tick tras migrar) → los rugs nacen en ~2k, sanos en ~30k
//   - VELAS DE 1s del nacimiento (vela1, vela2, vela3...) con su % cada una
//   - La clave: tras un impulso fuerte, ¿CORRIGE (sano) o SIGUE INFLANDO (rug)?
//   - Resultado final a los 20 min (¿murió a cero o sostuvo?)
const MC_OBSERVER = false;
const MCO_RECORD_MS = 1_200_000;      // 20 min de grabación
const MCO_T1_MS = 60_000;             // primer MINUTO completo = alta resolución
const MCO_T1_INTERVAL = 1_000;        // muestreo cada 1s durante el primer minuto
const MCO_T2_INTERVAL = 5_000;        // después del minuto, cada 5s
const MCO_STRONG_REBOUND = 40;
// ── Captura de las VELAS DEL NACIMIENTO (lo que vimos con el ojo) ──
const MCO_BIRTH_CANDLES = 12;         // grabar el % de las primeras 12 velas de 1s
const MCO_BIRTH_WINDOW_MS = 1_000;    // cada "vela" = 1 segundo
const MCO_PUMP_CANDLE_PCT = 50;       // vela "vertical" si sube >+50% en 1s
const MCO_HEALTHY_CORRECTION = -3;    // corrección "sana" si una vela baja < -3%

const MIG_BREAKEVEN_AT = 0.20;        // breakeven al +20% (antes +99%): protege el suelo antes
const MIG_BREAKEVEN_MARGIN = 0.03;
const MIG_LOCK_AT = 0.25;             // trailing FOLLOWING se arma en +25% (antes +70%)
const MIG_FOLLOW_PCT = 0.20;
const MIG_MAX_PRICE_RATIO = 2.0;
const MIG_SL_CONFIRM_TICKS = 2;
const MIG_EXPIRED_WIN_PCT = 2;
const MIG_ENTRY_DELAY_MS = 3_000;
const MIG_QUAL_GATE = true;
const MIG_QUAL_MOV2S_MIN = 5;          // qual_gate: exige mov2s > +5% (era +3%)
const MIG_QUAL_WINDOW_MS = 15_000;
const MIG_MAX_CAIDA_DELAY = 0.35;      // aborta si cae más de -35% en la confirmación (era -25%)
const MIG_STEP_TRIGGER = 0.25;        // escalón (suelo +13%) se arma en +25% (antes +70%)
const MIG_STEP_FLOOR = 0.13;
const MIG_FOLLOW_PCT_STEP = 0.15;
const MIG_HARD_CAP_LOSS = -20;
const MIG_CAP_LOSS_ON = false;        // DESACTIVADO: en modo demo probamos "dejar respirar". Mataba ganadoras que caen<-20 y luego despegan. ALTA VARIANZA.
// ── TRAILING POR ESTRUCTURA (nuevo) ──
// En vez de seguir a distancia fija del máximo, sube el stop al último valle
// confirmado cuando el precio rompe el último máximo. Respeta la estructura del precio.
const MIG_STRUCT_ON = true;           // trailing por estructura activado
const MIG_STRUCT_ARM_PCT = 50;        // se arma cuando la posición supera +50%
const MIG_STRUCT_RETROCESO = 20;      // un valle cuenta si el retroceso desde el máximo es >= 20 puntos
// ── ESCALONES DE SL POR NIVEL (nuevo) ──
// Cuando la ganancia alcanza un nivel, sube el SL a un suelo garantizado.
// Protege la ganancia por tramos: si un cohete sube y se va a cero, te llevas un pedazo.
const MIG_ESCALONES_ON = true;
const MIG_ESCALONES = [               // [nivel_alcanzado%, sl_nuevo%]
  [123, 10],
  [200, 50],
];
const MIG_VELO_DROP = 0.10;
const MIG_VELO_ON = false;            // DESACTIVADO: en modo demo probamos "dejar respirar". El velo mataba 31 ganadoras que caían brusco y rebotaban. ALTA VARIANZA.
const MIG_VELO_MS = 2_000;
const MIG_TRAIL_T1 = 40;  const MIG_TRAIL_P1 = 0.15;
const MIG_TRAIL_T2 = 60;  const MIG_TRAIL_P2 = 0.12;
const MIG_TRAIL_T3 = 100; const MIG_TRAIL_P3 = 0.08;
const MIG_TRAIL_P4 = 0.05;
const MIG_TOP_FLOOR_TRIGGER = 100;
const MIG_TOP_FLOOR = 0.65;

// ── KILL-SWITCH DE PORTAFOLIO ──
const RISK = {
  maxDailyLossSol: 1.5,
  maxConsecutiveLosses: 12,
  cooldownAfterStreakMs: 60 * 60 * 1000,
};

// ── SECRETOS desde el entorno ──
const HELIUS_API_KEY = process.env.HELIUS_API_KEY || "";
const SOLANA_RPC = process.env.SOLANA_RPC
  || (HELIUS_API_KEY ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : "https://api.mainnet-beta.solana.com");
const PUMPPORTAL_API_KEY = process.env.PUMPPORTAL_API_KEY || "";
const PUMPPORTAL_WS = PUMPPORTAL_API_KEY
  ? `wss://pumpportal.fun/api/data?api-key=${PUMPPORTAL_API_KEY}`
  : "wss://pumpportal.fun/api/data";

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
  }
}

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
      riskState.consecutiveLosses = 0;
      addLog(`🛑 KILL-SWITCH: ${RISK.maxConsecutiveLosses} pérdidas reales seguidas — pausa 1h`, "error");
      broadcast({ event: "risk", data: riskSnapshot() });
    }
    return true;
  }
  return false;
}

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
  mcoRecordings: new Map(),
  liveRecordings: new Map(),
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

function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const saved = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    if (saved.demoTrades) state.demoTrades = saved.demoTrades;
    if (saved.realTrades) state.realTrades = saved.realTrades;
    if (saved.movements) state.movements = saved.movements;
    if (saved.stats) state.stats = { ...state.stats, ...saved.stats };
    if (saved.riskState) {
      riskState.dayKey = saved.riskState.dayKey ?? null;
      riskState.dailyPnlSol = saved.riskState.dailyPnlSol ?? 0;
      riskState.consecutiveLosses = saved.riskState.consecutiveLosses ?? 0;
      riskState.pausedUntil = saved.riskState.pausedUntil ?? 0;
      riskRolloverDay();
    }
    for (const trade of state.demoTrades) {
      if (trade.status === "OPEN" || trade.status === "CLOSING") {
        trade.status = "CLOSED"; trade.result = "EXPIRED";
        trade.pnlPct = trade.pnlPct || 0; trade.closeTime = Date.now();
      }
    }
    state.stats.demoOpen = 0;
    addLog(`✅ Estado cargado: ${state.demoTrades.length} demo, ${state.realTrades.length} real (real pendiente de reconciliar)`, "info");
  } catch (e) { addLog(`⚠️ Error cargando estado: ${e.message}`, "warn"); }
}

async function reconcileStateOnBoot() {
  if (!wallet || !connection) {
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

  let onChain = new Map();
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
      trade.status = "OPEN";
      const monitor = {
        mint: trade.mint, name: trade.name, symbol: trade.symbol,
        price: trade.entryPrice, mc: trade.entryPrice * 1_000_000_000,
        priceHigh: trade.entryPrice, priceLow: trade.entryPrice,
        tradeCount: 0, volumeUSD: 0,
        detectedAt: trade.openTime, lastUpdate: Date.now(),
      };
      state.migMonitored.set(trade.mint, monitor);
      resubscribePrice(trade.mint);
      resumed++;
      addLog(`♻️ Reanudada posición real: ${trade.symbol} (${shortAddr(trade.mint)}) — ${onChain.get(trade.mint)} tokens en wallet`, "real");
      onChain.delete(trade.mint);
    } else {
      trade.status = "CLOSED"; trade.result = "RECONCILED_GONE";
      trade.closeTime = Date.now();
      gone++;
      addLog(`🔁 Reconciliado (ya no en wallet): ${trade.symbol} → cerrado como RECONCILED_GONE`, "warn");
    }
  }

  for (const [mint, amt] of onChain.entries()) {
    addLog(`⚠️ HUÉRFANO en wallet: ${shortAddr(mint)} (${amt} tokens) — sin trade asociado. Revisar/liquidar manualmente.`, "warn");
  }

  state.stats.realOpen = state.realTrades.filter(t => t.status === "OPEN").length;
  addLog(`✅ Reconciliación: ${resumed} reanudadas, ${gone} cerradas (gone), ${onChain.size} huérfanos. Reales abiertas: ${state.stats.realOpen}`, "info");
  broadcast({ event: "stats", data: state.stats });
  saveState();
}

function resubscribePrice(mint) {
  if (pumpPortalWs?.readyState === WebSocket.OPEN) {
    pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
  }
}

const frontendClients = new Set();
const seenMigMints = new Set();

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
    const r = await fetch("https://lite-api.jup.ag/price/v3?ids=So11111111111111111111111111111111111111112", { signal: AbortSignal.timeout(8000) });
    const d = await r.json();
    const px = d?.["So11111111111111111111111111111111111111112"]?.usdPrice;
    if (px > 0) { solPriceUSD = px; solPriceReady = true; addLog(`ℹ️ SOL price vía Jupiter v3: $${px}`, "info"); return; }
  } catch (e) { addLog(`⚠️ SOL price Jupiter v3 falló: ${e.message}`, "warn"); }
  addLog(`⚠️ SOL price sin actualizar, sigo con $${solPriceUSD}`, "warn");
  if (!solPriceReady) { solPriceReady = true; addLog(`⚠️ Usando SOL price fallback $${solPriceUSD} — operativa desbloqueada`, "warn"); }
}
setInterval(updateSolPrice, 60_000);
updateSolPrice();

setInterval(async () => {
  if (wallet) { state.stats.walletBalance = await getWalletBalance(); broadcast({ event: "stats", data: state.stats }); }
}, 30_000);

// ════════════════════════════════════════════════════════════════
// MODO MC_OBSERVER (grabación del recorrido del MC desde la migración)
// ════════════════════════════════════════════════════════════════

function mcoStart(mint, symbol, mcMigUsd) {
  if (!MC_OBSERVER) return;
  if (state.mcoRecordings.has(mint)) return;
  if (pumpPortalWs?.readyState === WebSocket.OPEN) {
    pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
  }
  const rec = {
    mint, symbol,
    mcMig: mcMigUsd > 0 ? mcMigUsd : null,
    mcMigSource: mcMigUsd > 0 ? "evento" : "1er-tick",
    t0: Date.now(),
    puntos: [],
    lastSample: 0,
    minP: 0, minT: 0,
    maxP: 0, maxT: 0,
    finished: false,
    // ── captura de las velas de 1s del nacimiento ──
    birthCandles: [],        // [{idx, pctAcum, pctVela}] una por segundo
    lastCandleMc: null,      // MC al cierre de la última vela (para calcular % de la vela)
    lastCandleT: 0,          // timestamp de la última vela cerrada
    mcNacimiento: null,      // MC del primerísimo tick (el "nacimiento")
  };
  if (rec.mcMig != null) rec.puntos.push({ t: 0, p: 0 });
  state.mcoRecordings.set(mint, rec);
  addLog(`🔬 MCREC GRABANDO: ${symbol} | MC mig ${rec.mcMig != null ? formatMC(rec.mcMig) : "(esperando 1er tick)"} — ${MCO_RECORD_MS/60000}min`, "accept");
  rec.timer = setTimeout(() => mcoFinish(mint), MCO_RECORD_MS);
}

function mcoSample(mint, price) {
  const rec = state.mcoRecordings.get(mint);
  if (!rec || rec.finished || price <= 0) return;
  const mcNow = price * 1_000_000_000;
  if (rec.mcMig == null) {
    rec.mcMig = mcNow;
    rec.mcNacimiento = mcNow;
    rec.lastCandleMc = mcNow;
    rec.lastCandleT = Date.now();
    rec.t0 = Date.now();
    rec.lastSample = 0;
    rec.puntos.push({ t: 0, p: 0 });
    addLog(`🔬 MCREC ref fijada (1er tick): ${rec.symbol} | MC nacimiento ${formatMC(rec.mcMig)}`, "accept");
    return;
  }
  if (rec.mcMig <= 0) return;

  // ── VELAS DEL NACIMIENTO: cerrar una "vela" cada segundo durante las primeras N ──
  if (rec.birthCandles.length < MCO_BIRTH_CANDLES) {
    const dtCandle = Date.now() - rec.lastCandleT;
    if (dtCandle >= MCO_BIRTH_WINDOW_MS) {
      const pctVela = rec.lastCandleMc > 0 ? +((mcNow - rec.lastCandleMc) / rec.lastCandleMc * 100).toFixed(2) : 0;
      const pctAcum = +((mcNow - rec.mcNacimiento) / rec.mcNacimiento * 100).toFixed(2);
      rec.birthCandles.push({ idx: rec.birthCandles.length + 1, pctVela, pctAcum });
      rec.lastCandleMc = mcNow;
      rec.lastCandleT = Date.now();
    }
  }

  const dt = Date.now() - rec.t0;
  const interval = dt <= MCO_T1_MS ? MCO_T1_INTERVAL : MCO_T2_INTERVAL;
  if (rec.lastSample && Date.now() - rec.lastSample < interval) return;
  rec.lastSample = Date.now();
  const pct = +((mcNow - rec.mcMig) / rec.mcMig * 100).toFixed(2);
  const tSec = Math.round(dt / 1000);
  rec.puntos.push({ t: tSec, p: pct });
  if (pct < rec.minP) { rec.minP = pct; rec.minT = tSec; }
  if (pct > rec.maxP) { rec.maxP = pct; rec.maxT = tSec; }
}

function mcoFinish(mint) {
  const rec = state.mcoRecordings.get(mint);
  if (!rec || rec.finished) return;
  rec.finished = true;
  state.mcoRecordings.delete(mint);
  unsubscribeToken(mint);

  const pts = rec.puntos;
  if (rec.mcMig == null || pts.length < 2) {
    addLog(`[MCREC] sym=${rec.symbol} SIN DATOS (${pts.length} pts, MC ref ${rec.mcMig == null ? "nunca llegó" : formatMC(rec.mcMig)}) — token sin actividad tras migrar`, "rec");
    return;
  }

  const lastP = pts.length ? pts[pts.length - 1].p : 0;

  let maxAfterMin = rec.minP;
  let maxAfterMinT = rec.minT;
  for (const pt of pts) {
    if (pt.t > rec.minT && pt.p > maxAfterMin) { maxAfterMin = pt.p; maxAfterMinT = pt.t; }
  }
  const reboteDesdeSuelo = +(maxAfterMin - rec.minP).toFixed(2);
  const tiroFuerte = reboteDesdeSuelo >= MCO_STRONG_REBOUND ? "SI" : "no";

  // ── ANÁLISIS DE LA FIRMA DEL NACIMIENTO (lo que descubrimos con el ojo) ──
  const bc = rec.birthCandles;
  // ¿hubo vela "cohete" (subida vertical >+50% en 1s)?
  const idxCohete = bc.findIndex(c => c.pctVela >= MCO_PUMP_CANDLE_PCT);
  const huboCohete = idxCohete >= 0;
  // tras el cohete, ¿corrigió (vela negativa) o siguió inflando (otra vela muy +)?
  let firma = "normal";
  if (huboCohete) {
    const despues = bc.slice(idxCohete + 1);
    const corrige = despues.some(c => c.pctVela <= MCO_HEALTHY_CORRECTION);
    const sigueInflando = despues.length > 0 && despues.every(c => c.pctVela > MCO_HEALTHY_CORRECTION) &&
                          despues.some(c => c.pctVela >= 30);
    if (corrige) firma = "COHETE+corrige(sano?)";
    else if (sigueInflando) firma = "COHETE+sigue-inflando(RUG?)";
    else firma = "COHETE+plano";
  }
  // MC de nacimiento: ¿nació bajo (~2k) o normal (~30k)?
  const nacBajo = rec.mcNacimiento != null && rec.mcNacimiento < 5000 ? "BAJO" : "normal";
  // ¿murió? (cierre muy negativo respecto al techo)
  const murio = lastP < -50 || (rec.maxP > 20 && lastP < rec.maxP - 70) ? "MURIO" : "vivo";

  const velasStr = bc.map(c => `v${c.idx}:${c.pctVela>=0?"+":""}${c.pctVela}%`).join(" ");
  const ptsRaw = pts.map(p => `${p.t}:${p.p}`).join(",");
  addLog(
    `[MCREC] sym=${rec.symbol} nac=${rec.mcNacimiento != null ? formatMC(rec.mcNacimiento) : "?"}(${nacBajo}) ` +
    `velas[${velasStr}] firma=${firma} fin=${murio} ` +
    `suelo=${rec.minP}%@${rec.minT}s techo=${rec.maxP}%@${rec.maxT}s ` +
    `rebote_desde_suelo=${reboteDesdeSuelo}pts@${maxAfterMinT}s tiro_fuerte=${tiroFuerte} ` +
    `cierre=${lastP}% pts=${ptsRaw}`,
    "rec"
  );
}

// ════════════════════════════════════════════════════════════════
// ESTRATEGIA: SNIPER DE MIGRACIÓN
// ════════════════════════════════════════════════════════════════

function migStartWatching(coin) {
  if (seenMigMints.has(coin.mint)) return;
  if (!solPriceReady) { addLog("⏳ Esperando precio real de SOL antes de operar", "warn"); return; }
  seenMigMints.add(coin.mint);
  state.stats.mig_migrations++;
  if (MC_OBSERVER) {
    const mcMigUsd = (coin.marketCapSol || 0) * solPriceUSD;
    broadcast({ event: "stats", data: state.stats });
    mcoStart(coin.mint, coin.symbol || "???", mcMigUsd);
    return;
  }
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
  // Tope de MC de entrada: se aplica SIEMPRE, con o sin qual_gate.
  const mcEntryUsdPre = entryPriceB * 1_000_000_000;
  if (mcEntryUsdPre > MIG_MAX_MC_ENTRY) {
    addLog(`🛑 MIG MC ALTO: ${entry.symbol} descartada | MC ${formatMC(mcEntryUsdPre)} > tope ${formatMC(MIG_MAX_MC_ENTRY)} (riesgo honeypot/pump inflado)`, "filter");
    state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
    broadcast({ event: "stats", data: state.stats });
    return;
  }
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
    const mcEntryUsd = priceNow * 1_000_000_000;
    if (mov2 > MIG_QUAL_MOV2S_MIN && pend15 > 0 && mcEntryUsd > MIG_MAX_MC_ENTRY) {
      addLog(`🛑 MIG MC ALTO: ${entry.symbol} descartada | MC ${formatMC(mcEntryUsd)} > tope ${formatMC(MIG_MAX_MC_ENTRY)} (riesgo honeypot/pump inflado)`, "filter");
      state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
      broadcast({ event: "stats", data: state.stats });
      return;
    }
    if (mov2 > MIG_QUAL_MOV2S_MIN && pend15 > 0) {
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
// TRADING (solo migración — PumpPortal)
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

async function buyToken(mint, solAmount, slippage = 15) {
  if (!wallet || !connection) return null;
  try {
    const response = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "buy", mint, denominatedInSol: "true", amount: solAmount, slippage, priorityFee: 0.0005, pool: "auto" }),
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) {
      let motivo = "";
      try { motivo = (await response.text()).slice(0, 200); } catch {}
      addLog(`❌ Compra error: ${response.status}${motivo ? " — " + motivo : ""}`, "error");
      return null;
    }
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
  if (DEMO_ONLY) return;  // modo demo: no se abre ninguna operación real
  if (!wallet) return;
  if (!REAL_STRATEGIES.includes(signal.strategy)) return;
  if (tradingHalted()) {
    const snap = riskSnapshot();
    addLog(`🛑 Entrada real BLOQUEADA por kill-switch: ${signal.symbol} | diario ${snap.dailyPnlSol.toFixed(3)} SOL · pausa ${(snap.pausedMsLeft/60000).toFixed(0)}min`, "warn");
    return;
  }
  const openReal = state.realTrades.filter(t => t.status === "OPEN");
  if (openReal.length >= MAX_REAL_TRADES) return;
  const stratOpen = openReal.filter(t => t.strategy === signal.strategy).length;
  if (stratOpen >= MAX_MIG_REAL) { addLog(`⚠️ Límite real [migración]: ${stratOpen}/${MAX_MIG_REAL}`, "warn"); return; }
  const solAmount = SOL_PER_TRADE_MIG;
  const balance = await getWalletBalance();
  if (balance < solAmount + 0.01) { addLog(`⚠️ Balance insuficiente: ${balance.toFixed(3)} SOL (necesito ${(solAmount+0.01).toFixed(2)})`, "warn"); return; }
  const buy = await buyToken(signal.mint, solAmount, 15);
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
    expiresAt: Date.now() + MIG_DURATION_MS, sellRetries: 0,
  };
  state.realTrades.unshift(trade);
  if (state.realTrades.length > 200) state.realTrades.pop();
  state.stats.realOpen++;
  state.stats.walletBalance = await getWalletBalance(true);
  broadcast({ event: "newRealTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  addLog(`🔴 REAL [${signal.strategy}]: ${signal.symbol} | ${solAmount} SOL`, "real");
  saveState();
  scheduleLaunchCheck(trade, "real");
}

// v6.20.3: corte por no-despegue. A los MIG_LAUNCH_CHECK_MS de abrir, si el trade
// no ha tocado +MIG_LAUNCH_MIN_PCT%, se cierra (no despegó → WR histórico ~0%).
function scheduleLaunchCheck(trade, kind) {
  if (!MIG_LAUNCH_CHECK) return;
  if (trade.strategy !== "migration") return;
  setTimeout(() => {
    if (trade.status !== "OPEN") return;            // ya cerrado por TP/SL/etc
    if (trade.maxGainPct >= MIG_LAUNCH_MIN_PCT) return; // despegó, se mantiene
    const token = state.migMonitored.get(trade.mint);
    const price = token?.price || trade.entryPrice;
    addLog(`✂️ NO-DESPEGUE [${kind} ${trade.symbol}]: max ${trade.maxGainPct.toFixed(1)}% < ${MIG_LAUNCH_MIN_PCT}% a los ${MIG_LAUNCH_CHECK_MS/1000}s → salida`, kind === "real" ? "realloss" : "loss");
    if (kind === "real") closeRealTrade(trade, price, "NO_LAUNCH");
    else closeDemoTrade(trade, price, "NO_LAUNCH", MIG_TP);
  }, MIG_LAUNCH_CHECK_MS);
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
  riskRecordClose(realPnlSol);
  const dur = Math.round((trade.closeTime - trade.openTime) / 1000);
  const expWinPct = MIG_EXPIRED_WIN_PCT;
  if (reason === "TP" || reason === "STEP" || (reason === "SL" && trade.pnlPct >= 0)) {
    trade.result = "WIN"; state.stats.mig_realWins++;
    state.stats.mig_realPnL += trade.pnlPct; state.stats.mig_realPnLSol += trade.pnlSol;
    addLog(`✅ REAL WIN [${reason==="STEP"?"🪜 ESCALÓN":"migration"}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "realwin");
  } else if (reason === "SL") {
    trade.result = "LOSS"; state.stats.mig_realLosses++;
    state.stats.mig_realPnL += trade.pnlPct; state.stats.mig_realPnLSol += trade.pnlSol;
    addLog(`❌ REAL LOSS [migration]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "realloss");
  } else if (reason === "NO_LAUNCH") {
    trade.result = trade.pnlPct >= 0 ? "WIN" : "LOSS";
    if (trade.result === "WIN") state.stats.mig_realWins++; else state.stats.mig_realLosses++;
    state.stats.mig_realPnL += trade.pnlPct; state.stats.mig_realPnLSol += trade.pnlSol;
    addLog(`✂️ REAL NO-DESPEGUE: ${trade.symbol} ${trade.pnlPct>0?"+":""}${trade.pnlPct}% en ${dur}s`, trade.result === "WIN" ? "realwin" : "realloss");
  } else {
    state.stats.mig_realPnL += trade.pnlPct; state.stats.mig_realPnLSol += trade.pnlSol;
    if (trade.pnlPct >= expWinPct) { trade.result = "WIN"; state.stats.mig_realWins++; addLog(`✅ REAL WIN [EXP+]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "realwin"); }
    else { trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS"; addLog(`⏱️ REAL EXP: ${trade.symbol} ${trade.pnlPct>0?"+":""}${trade.pnlPct}%`, "real"); }
  }
  state.stats.realOpen = Math.max(0, state.stats.realOpen - 1);
  state.stats.walletBalance = await getWalletBalance(true);
  migCleanup(trade.mint, trade.symbol);
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

function updateRealTrades(mint, price, strategy) {
  const now = Date.now();
  const breakeven = MIG_BREAKEVEN_AT;
  const breakevenMargin = MIG_BREAKEVEN_MARGIN;
  const lock = MIG_LOCK_AT;
  const follow = MIG_FOLLOW_PCT;
  for (const trade of state.realTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    if (MIG_VELO_ON && veloDropTriggered(trade, price, strategy)) {
      addLog(`⚡🛑 VELO-EXIT [${strategy} real]: ${trade.symbol} caída rápida @ ${currentPct.toFixed(1)}%`, "realloss");
      closeRealTrade(trade, price, "SL"); continue;
    }
    if (MIG_CAP_LOSS_ON && currentPct <= MIG_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [${strategy} real]: ${trade.symbol} ${currentPct.toFixed(1)}%`, "realloss");
      closeRealTrade(trade, price, "SL"); continue;
    }
    const stepArmed = trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9;
    const followEff = stepArmed ? migTrailingPct(trade.maxGainPct) : follow;
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
    if (trade.maxGainPct >= MIG_TOP_FLOOR_TRIGGER) {
      const topFloorPrice = trade.entryPrice * (1 + MIG_TOP_FLOOR);
      if (topFloorPrice > trade.sl) trade.sl = +topFloorPrice.toFixed(12);
    }
    if (price >= trade.tp) { trade._slBelowCount = 0; closeRealTrade(trade, price, "TP"); }
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
    const sinceOpen = now - trade.openTime;
    if (sinceOpen >= 30_000 && trade.maxGainPct === 0 && trade.maxLossPct === 0) {
      addLog(`💀 FEED MUERTO [migration real]: ${trade.symbol} sin ticks en ${Math.round(sinceOpen/1000)}s`, "realloss");
      const token = state.migMonitored.get(trade.mint);
      closeRealTrade(trade, token?.price || trade.entryPrice, "DEAD_FEED"); continue;
    }
    if (now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint);
    closeRealTrade(trade, token?.price || trade.entryPrice, "EXPIRED");
  }
}, 10_000);

function openDemoTrade(signal) {
  const trade = {
    id: `demo-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    strategy: signal.strategy, mint: signal.mint, symbol: signal.symbol, name: signal.name,
    entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: "INITIAL", status: "OPEN",
    expiresAt: Date.now() + MIG_DURATION_MS, mov1s: null, mov2s: null,
  };
  state.demoTrades.unshift(trade);
  if (state.demoTrades.length > 500) state.demoTrades.pop();
  state.stats.demoOpen++;
  broadcast({ event: "newDemoTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  addLog(`📝 DEMO [${signal.strategy}]: ${signal.symbol} | TP +300% SL -20%`, "demo");
  scheduleLaunchCheck(trade, "demo");
}

// Gestiona el trailing por ESTRUCTURA y los ESCALONES sobre un trade.
// Actualiza trade.sl (precio) según los valles y los niveles alcanzados.
// Mantiene el estado de máximos/valles en el propio trade.
function aplicarEstructuraYEscalones(trade, price) {
  const pct = (price - trade.entryPrice) / trade.entryPrice * 100; // % desde entrada
  // inicializar estado la primera vez
  if (trade._structMax === undefined) {
    trade._structMax = pct;      // máximo corriente
    trade._structMin = pct;      // mínimo desde el último máximo
    trade._structArmed = false;
    trade._enRetroceso = false;
  }
  // ESCALONES: por cada nivel alcanzado (usando el máximo histórico), subir el SL
  if (MIG_ESCALONES_ON) {
    for (const [nivel, slNuevoPct] of MIG_ESCALONES) {
      if (trade.maxGainPct >= nivel) {
        const slPrice = trade.entryPrice * (1 + slNuevoPct / 100);
        if (slPrice > trade.sl) {
          trade.sl = +slPrice.toFixed(12);
          if (!trade._escalonesLog) trade._escalonesLog = {};
          if (!trade._escalonesLog[nivel]) {
            trade._escalonesLog[nivel] = true;
            addLog(`🪜 ESCALÓN +${nivel}% alcanzado → SL sube a +${slNuevoPct}% [${trade.symbol}]`, "trail");
          }
        }
      }
    }
  }
  // ESTRUCTURA: subir el stop al último valle cuando rompe el último máximo
  if (MIG_STRUCT_ON) {
    if (pct >= MIG_STRUCT_ARM_PCT) trade._structArmed = true;
    if (pct > trade._structMax) {
      // rompe el máximo: si veníamos de un retroceso suficiente, el mínimo es un valle confirmado
      if (trade._enRetroceso && (trade._structMax - trade._structMin) >= MIG_STRUCT_RETROCESO && trade._structArmed) {
        const vallePrice = trade.entryPrice * (1 + trade._structMin / 100);
        if (vallePrice > trade.sl) {
          trade.sl = +vallePrice.toFixed(12);
          addLog(`📐 ESTRUCTURA: ${trade.symbol} rompe máx, SL sube al valle +${trade._structMin.toFixed(0)}%`, "trail");
        }
      }
      trade._structMax = pct;
      trade._structMin = pct;
      trade._enRetroceso = false;
    } else if (pct < trade._structMin) {
      trade._structMin = pct;
      trade._enRetroceso = true;
    }
  }
}

function updateDemoTrades(mint, price, strategy) {
  const now = Date.now();
  const tp_pct = MIG_TP;
  for (const trade of state.demoTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    const sinceOpen = now - trade.openTime;
    if (trade.mov1s === null && sinceOpen >= 1000) trade.mov1s = +currentPct.toFixed(2);
    if (trade.mov2s === null && sinceOpen >= 2000) trade.mov2s = +currentPct.toFixed(2);
    if (MIG_VELO_ON && veloDropTriggered(trade, price, strategy)) {
      addLog(`⚡🛑 VELO-EXIT [${strategy}]: ${trade.symbol} caída rápida @ ${currentPct.toFixed(1)}%`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct); continue;
    }
    if (MIG_CAP_LOSS_ON && currentPct <= MIG_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [${strategy}]: ${trade.symbol} ${currentPct.toFixed(1)}%`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct); continue;
    }
    // Nueva lógica: estructura + escalones (sube trade.sl)
    aplicarEstructuraYEscalones(trade, price);
    // Cierre
    if (price >= trade.tp) { trade._slBelowCount = 0; closeDemoTrade(trade, price, "TP", tp_pct); }
    else if (price <= trade.sl) {
      const slPct = (trade.sl - trade.entryPrice) / trade.entryPrice * 100;
      const reason = slPct > 0 ? (trade._structArmed ? "STRUCT" : "STEP") : "SL";
      if (trade.sl >= trade.entryPrice) {
        closeDemoTrade(trade, trade.sl, reason, tp_pct);
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

function updateDemoTrades_OLD_UNUSED(mint, price, strategy) {
  const now = Date.now();
  const tp_pct = MIG_TP;
  const breakeven = MIG_BREAKEVEN_AT;
  const breakevenMargin = MIG_BREAKEVEN_MARGIN;
  const lock = MIG_LOCK_AT;
  const follow = MIG_FOLLOW_PCT;
  for (const trade of state.demoTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    const sinceOpen = now - trade.openTime;
    if (trade.mov1s === null && sinceOpen >= 1000) trade.mov1s = +currentPct.toFixed(2);
    if (trade.mov2s === null && sinceOpen >= 2000) trade.mov2s = +currentPct.toFixed(2);
    if (MIG_VELO_ON && veloDropTriggered(trade, price, strategy)) {
      addLog(`⚡🛑 VELO-EXIT [${strategy}]: ${trade.symbol} caída rápida @ ${currentPct.toFixed(1)}%`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct); continue;
    }
    if (MIG_CAP_LOSS_ON && currentPct <= MIG_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [${strategy}]: ${trade.symbol} ${currentPct.toFixed(1)}%`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct); continue;
    }
    const stepArmed = trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9;
    const followEff = stepArmed ? migTrailingPct(trade.maxGainPct) : follow;
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
    if (trade.maxGainPct >= MIG_TOP_FLOOR_TRIGGER) {
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
  const expWinPct = MIG_EXPIRED_WIN_PCT;
  if (reason === "TP") {
    trade.result = "WIN"; state.stats.mig_demoWins++;
    state.stats.mig_demoPnL += (tp_pct - 1) * 100;
    addLog(`✅ WIN [TP][${trade.strategy}]: ${trade.symbol} +${((tp_pct-1)*100).toFixed(0)}% en ${dur}s`, "win");
  } else if (reason === "STEP") {
    trade.result = "WIN"; state.stats.mig_demoWins++;
    state.stats.mig_demoPnL += trade.pnlPct;
    addLog(`✅ WIN [🪜 ESCALÓN][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win");
  } else if (reason === "SL") {
    state.stats.mig_demoPnL += trade.pnlPct;
    if (trade.pnlPct > 0) { trade.result = "WIN"; state.stats.mig_demoWins++; addLog(`✅ WIN [${trade.trailingPhase}][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win"); }
    else { trade.result = "LOSS"; state.stats.mig_demoLosses++; addLog(`❌ LOSS [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "loss"); }
  } else if (reason === "NO_LAUNCH") {
    state.stats.mig_demoPnL += trade.pnlPct;
    if (trade.pnlPct > 0) { trade.result = "WIN"; state.stats.mig_demoWins++; addLog(`✂️ NO-DESPEGUE [${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win"); }
    else { trade.result = "LOSS"; state.stats.mig_demoLosses++; addLog(`✂️ NO-DESPEGUE [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "loss"); }
  } else {
    state.stats.mig_demoPnL += trade.pnlPct;
    if (trade.pnlPct >= expWinPct) { trade.result = "WIN"; state.stats.mig_demoWins++; addLog(`✅ WIN [EXP+][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win"); }
    else if (trade.pnlPct <= -expWinPct) { trade.result = "LOSS"; state.stats.mig_demoLosses++; addLog(`❌ LOSS [EXP-][${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "loss"); }
    else { trade.result = "EXPIRED"; state.stats.mig_demoExpired++; addLog(`⏱️ EXP [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct>0?"+":""}${trade.pnlPct}%`, "expire"); }
  }
  state.stats.demoOpen = Math.max(0, state.stats.demoOpen - 1);
  state.stats.mig_maxGainSum += trade.maxGainPct || 0;
  state.stats.mig_maxLossSum += Math.abs(trade.maxLossPct || 0);
  state.stats.mig_closedCount++;
  state.stats.mig_avgMaxGain = +(state.stats.mig_maxGainSum / state.stats.mig_closedCount).toFixed(1);
  state.stats.mig_avgMaxLoss = +(state.stats.mig_maxLossSum / state.stats.mig_closedCount).toFixed(1);
  if (trade.mov2s !== null && trade.result !== "EXPIRED") {
    const bucket = trade.mov2s > 1 ? "up" : (trade.mov2s < -1 ? "down" : "flat");
    state.stats[`mig_mov_${bucket}_${trade.result === "WIN" ? "win" : "loss"}`]++;
  }
  liveRecFinish(trade.mint, trade.pnlPct);
  migCleanup(trade.mint, trade.symbol);
  broadcast({ event: "stats", data: state.stats });
  saveState();
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.status !== "OPEN") continue;
    if (now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint);
    closeDemoTrade(trade, token?.price || trade.entryPrice, "EXPIRED", MIG_TP);
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
    for (const [mint] of state.mcoRecordings.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
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
        if (MC_OBSERVER && state.mcoRecordings.has(data.mint)) mcoSample(data.mint, price);
      }
    } catch (e) { console.log("PP:", e.message); }
  });
  pumpPortalWs.on("error", (err) => addLog(`❌ PumpPortal: ${err.message}`, "error"));
  pumpPortalWs.on("close", () => { addLog("🔄 PumpPortal reconectando...", "warn"); setTimeout(connectPumpPortal, 5000); });
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

app.get("/api/risk", (req, res) => res.json(riskSnapshot()));
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
  if (MC_OBSERVER) {
    console.log(`🔬 SolScanBot — MODO OBSERVADOR PURO (NO OPERA) | graba ${MCO_RECORD_MS/60000}min por token | velas 1s primer minuto + ${MCO_BIRTH_CANDLES} velas nacimiento | detecta firma cohete+corrige(sano) vs cohete+sigue-inflando(rug)`);
    addLog(`🔬 MODO OBSERVADOR PURO ACTIVO — el bot NO opera, solo graba [MCREC]. Recoge datos y luego pon MC_OBSERVER=false para operar.`, "accept");
  } else if (DEMO_ONLY) {
    console.log(`📝 SolScanBot MIGRACIÓN — MODO DEMO (NO toca wallet real) | SL ${((1-MIG_SL)*100).toFixed(0)}% · TP +${(MIG_TP*100-100).toFixed(0)}% · estructura ${MIG_STRUCT_ON?`ON(arma+${MIG_STRUCT_ARM_PCT}%,valle${MIG_STRUCT_RETROCESO})`:"off"} · escalones ${MIG_ESCALONES_ON?MIG_ESCALONES.map(e=>`+${e[0]}→+${e[1]}`).join(" "):"off"} | qual_gate mov2s>+${MIG_QUAL_MOV2S_MIN}% | red ${MIG_DURATION_MS/60000}min | lote ${SOL_PER_TRADE_MIG} SOL`);
    addLog(`📝 MODO DEMO ACTIVO — estructura+escalones, SL -${((1-MIG_SL)*100).toFixed(0)}%, TP +${(MIG_TP*100-100).toFixed(0)}%. NO toca la wallet real. Config AGRESIVA (alta varianza).`, "accept");
  } else {
    console.log(`🚀 SolScanBot MIGRACIÓN v6.20.3 — REAL | trailing arma +${MIG_LOCK_AT*100}% · breakeven +${MIG_BREAKEVEN_AT*100}% · corte no-despegue ${MIG_LAUNCH_CHECK ? `ON (${MIG_LAUNCH_CHECK_MS/1000}s/+${MIG_LAUNCH_MIN_PCT}%)` : "off"} | tope MC $${(MIG_MAX_MC_ENTRY/1000)}K | MAX_MIG_REAL ${MAX_MIG_REAL} × ${SOL_PER_TRADE_MIG} SOL | kill -${RISK.maxDailyLossSol} SOL/día ${RISK.maxConsecutiveLosses}L`);
  }
  if (!HELIUS_API_KEY && !process.env.SOLANA_RPC) addLog("⚠️ Sin HELIUS_API_KEY ni SOLANA_RPC — usando RPC público (lento, puede limitar)", "warn");
  loadState();
  initWallet();
  connectPumpPortal();
  if (!MC_OBSERVER) await reconcileStateOnBoot();
});
