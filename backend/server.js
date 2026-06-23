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
const MAX_REAL_TRADES = 0;        // 0 = real apagado del todo. Subir a 2-3 para activar real.
// Estrategias que pueden operar en REAL. El resto sigue solo en demo aunque MAX_REAL_TRADES>0.
// Ej: ["migration"] = solo migración en real, momentum y re-migración se quedan en demo.
const REAL_STRATEGIES = ["migration"];
const STATE_FILE = "/tmp/solscanbot_state.json";

// ── CONFIG MIGRACIÓN ───────────────────────────────────────────
const MIG_TP = 4.00;              // v6.15: +300% (red de seguridad alta; imprescindible con el armado tardío, si no corta los pelotazos +300%+)
const MIG_SL = 0.80;              // v6.15: -20% (era -18%; aviso de la auditoría: bajar riesgo por op, el agregado apenas pierde)
const MIG_DURATION_MS = 15 * 60 * 1000; // 15 min — más tiempo para el movimiento
const MIG_WINDOW_MS = 60_000;
const MIG_MIN_VOL_FAST = 2_000;
const MIG_MIN_VOL_SLOW = 5_000;
const MIG_FAST_WINDOW_MS = 20_000;
const MIG_MIN_MC = 50_000;
const MIG_MAX_MC = 2_000_000;

// ── MODO OBSERVADOR (recogelotodo) ────────────────────────────────
// Día de recolección: migración entra en CASI TODO (umbral permisivo) y graba
// el recorrido de precio 4 min por token. Escribe una línea [REC] por migración.
// Ese día el P&L se ignora (se llena de operaciones malas a propósito). Apagar
// para volver a la operativa normal. NO toca momentum.
const OBSERVER_MODE = true;           // ⬅️ ACTIVO: día de recolección (v6.15.3)
// ── v6.15: GRABACIÓN EN VIVO (opera Y graba a la vez) ──────────────
// A diferencia de OBSERVER_MODE (que graba EN VEZ de operar), esto deja al bot
// operar normalmente en demo y, en paralelo, graba el recorrido de cada trade que
// ABRE de verdad. Escribe una línea [REC] al cerrar el trade, con cierre_real = el
// pnl REAL ejecutado por la gestión (no una simulación). Datos superiores: recorrido
// real del trade + la salida que de verdad hizo el bot. Solo migración.
const LIVE_RECORD = true;             // ⬅️ grabar las entradas reales de MIGRACIÓN mientras opera
const LIVE_REC_DENSE_MS = 60_000;     // primeros 60s: muestreo denso (donde el trailing trabaja)
const LIVE_REC_DENSE_INTERVAL = 2_000;// cada 2s en la fase densa
const LIVE_REC_NORMAL_INTERVAL = 5_000;// cada 5s después
// ── v6.15.4: GRABACIÓN DE MOMENTUM ([MOMREC]) ──────────────────────
// Igual idea que la de migración pero para momentum: graba el recorrido en % de
// cada trade que abre y escribe una línea [MOMREC] al cerrar, con cierre_real = el
// pnl REAL ejecutado. Sirve para decidir con datos el breakeven óptimo (grupo A vs B:
// las que mueren en breakeven, ¿siguieron cayendo o solo bachearon?). El feed de
// momentum es el scan cada 15s, así que la resolución es de ~15s (no tick a tick).
// Funciones momRec* SEPARADAS: no tocan nada de migración.
const MOM_RECORD = true;              // ⬅️ poner false para dejar de grabar momentum
const OBS_MIN_VOL = 2_000;            // umbral permisivo de volumen (vs 2K/5K normal)
const OBS_MIN_MC = 20_000;            // umbral permisivo de MC (vs 50K normal)
const OBS_RECORD_MS = 600_000;        // v6.15.3: grabar 10 min por token (era 4 min). Cubre el pico y la reversión de migración sin grabar la cola muerta del trade de 15 min
// v6.15.3: muestreo escalonado en 3 tramos. Más resolución donde el trailing/armado
// trabajan (subida y pico), menos en la cola. ~170 pts/token (los pts pesan nada).
const OBS_T1_MS = 60_000;             // tramo 1: primeros 60s (entrada + primer impulso, donde se decide mov2s)
const OBS_T1_INTERVAL = 2_000;        //   → cada 2s
const OBS_T2_MS = 300_000;            // tramo 2: 60s-300s (zona del pico y el armado, lo que se re-optimiza)
const OBS_T2_INTERVAL = 3_000;        //   → cada 3s
const OBS_T3_INTERVAL = 5_000;        // tramo 3: 300s-600s (cola: solo confirma si aguantó) → cada 5s
const MIG_BREAKEVEN_AT = 0.99;    // v6.15: DESACTIVADO en migración. Antes valía 0.22 y quedaba inactivo porque LOCK (+20%) < 0.22. Ahora LOCK es +70%, así que para mantener la rama breakeven inactiva (el armado tardío NO debe proteger a breakeven antes de +70%) lo subimos por encima del lock. La rama existe para momentum, que usa MOM_BREAKEVEN_AT. No tocar sin revisar ambas estrategias.
const MIG_BREAKEVEN_MARGIN = 0.03; // (solo aplicaría si el breakeven de migración estuviera activo, que no lo está)
const MIG_LOCK_AT = 0.70;         // v6.15: following a +70% (armado tardío; era +20%)
const MIG_FOLLOW_PCT = 0.20;      // trailing -20%
const MIG_MAX_PRICE_RATIO = 2.0;
const MIG_SL_CONFIRM_TICKS = 2;   // v6.7: nº de ticks consecutivos bajo el SL inicial necesarios para cerrar. Un tick basura aislado (DUR 0-2s, -47% imposible) no se confirma; una caída real sí. Solo aplica al SL bajo entrada, no al piso/trailing en positivo
const MIG_EXPIRED_WIN_PCT = 2;
const MIG_ENTRY_DELAY_MS = 3_000; // delay 3s antes de entrar (ahora ventana de confirmación, v6.9)
// ── v6.15: FILTRO DE CALIDAD con entrada retrasada a 15s ──────────
// Validado sobre observador: mov2s>0 Y pendiente15s>0 → WR 61%, +12.7/op (vs +8.9 global).
// El bot espera 15s desde que el token pasa el umbral de volumen, mide la dirección a 2s
// y la pendiente a 15s, y SOLO entra si ambas son positivas. La entrada es más cara (pierde
// los primeros 15s de subida) pero evita las que se desploman en esa ventana.
const MIG_QUAL_GATE = true;        // activar el filtro de calidad
const MIG_QUAL_WINDOW_MS = 15_000; // ventana de evaluación de calidad
const MIG_MAX_CAIDA_DELAY = 0.25; // v6.9: si el precio cae >25% durante la ventana de 3s, NO entrar (el token se desploma justo al abrir). Precio de entrada = precio REAL tras el delay, no el congelado
// ── NUEVO v6.2: escalón de beneficio (de v8.4) ─────────────────
const MIG_STEP_TRIGGER = 0.70;    // v6.15: armado tardío — al tocar +70% de beneficio...
const MIG_STEP_FLOOR = 0.13;      // ...asegurar piso de +13%
const MIG_FOLLOW_PCT_STEP = 0.15; // v6.5: trailing ceñido (-15%) cuando el escalón está armado (maxGain>=+20%). El trailing supera el piso +13% a partir de +15.3% (vs +41% con -20%), capturando las medianas-altas +25-40% que antes se cortaban en +13%
// ── v6.10: cap loss duro + trailing escalonado por tramos ──
const MIG_HARD_CAP_LOSS = -20;    // tope de pérdida duro (%). Cierre inmediato sin confirmación. Corta las caídas reales rápidas (-23/-30%) que la confirmación de 2 ticks deja ejecutar más profundo. Deja vivir los lavados de -16/-19% que se recuperan
const MIG_VELO_DROP = 0.10;       // v6.12 Palanca 1: si el precio cae >10%...
const MIG_VELO_MS = 2_000;        // ...en menos de 2s, vender YA (volcado vertical). Solo si está bajo entrada y sin escalón armado (no toca ganadoras)
// Trailing escalonado: se ciñe cuanto más alto el MAX. Tramos por maxGainPct (%).
const MIG_TRAIL_T1 = 40;  const MIG_TRAIL_P1 = 0.15;  // +20-40% → -15%
const MIG_TRAIL_T2 = 60;  const MIG_TRAIL_P2 = 0.12;  // +40-60% → -12% (matiz 2: suaviza el salto)
const MIG_TRAIL_T3 = 100; const MIG_TRAIL_P3 = 0.08;  // +60-100% → -8%
const MIG_TRAIL_P4 = 0.05;                            // +100%+ → -5%
const MIG_TOP_FLOOR_TRIGGER = 100; // al tocar +100%...
const MIG_TOP_FLOOR = 0.65;        // ...garantizar suelo +65% (matiz 3: protege el tramo -5% de caídas verticales que saltan ticks)

// ── CONFIG MOMENTUM ────────────────────────────────────────────
const MOM_TP = 1.06;
const MOM_SL = 0.97;
const MOM_DURATION_MS = 45 * 60 * 1000;
const MOM_MIN_PCT_1H = 10;
const MOM_MAX_PCT_1H = 30;
const MOM_MIN_VOL_1H = 100_000;
const MOM_MIN_MC = 100_000;
const MOM_MAX_MC = 1_000_000;
const MOM_SCAN_MS = 15_000;       // v6.15.2: bajado de 30s. El scan es el ÚNICO feed de los trades abiertos de momentum (momUpdatePrice se llama desde aquí); a 30s las medianas que suben +4 y se desinflan se gestionaban a ciegas. A 15s el trailing ve la bajada antes. Coste: x2 llamadas/min a Birdeye
const MOM_MIN_LIQUIDITY = 20_000;     // v6.13: bajado de 25K. Umbral bajo a propósito: el peso del filtrado lo lleva la Capa 2 (movimiento real de precio), porque la liquidez que reporta Birdeye no es fiable (WORLDCUP <$1 pasaba el 25K)
const MOM_MUTE_TIMEOUT_MS = 90_000;   // v6.4: 90s sin un solo movimiento => feed mudo, cancelar y liberar capital
const MOM_HARD_CAP_LOSS = -10;        // v6.8: tope de pérdida duro (%). Si currentPct <= esto, cerrar YA. Red de seguridad para caídas verticales en pools ilíquidos donde el SL -3% no se ejecuta porque el precio salta por encima del nivel entre ticks. Corta un -62% a ~-10%
const MOM_MAX_ENTRY_DRIFT = 0.04;     // v6.12: si el precio fresco se alejó >4% del de la señal, NO entrar (la vela ya se movió)
const MOM_MUTE_COOLDOWN_MS = 15 * 60_000; // v6.12: 15 min sin reentrar un token que expiró mudo
const MOM_MUTE_CHECK_MS = 5_000;      // v6.13 Capa 2: separación entre las dos lecturas de precio en la entrada
const MOM_MUTE_MIN_MOVE = 0.003;      // v6.13 Capa 2: <0.3% de cambio en esa ventana => mudo, no entrar
const BIRDEYE_PRICE = "https://public-api.birdeye.so/defi/price";
const MOM_BREAKEVEN_AT = 0.03;
const MOM_LOCK_AT = 0.05;
const MOM_FOLLOW_PCT = 0.02;
const MOM_PENDING_TIMEOUT_MS = 15_000;
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
  obsRecordings: new Map(),  // modo observador: mint → {symbol, vel, mc, vol, t0, entryPrice, puntos:[], mov2s, ...}
  liveRecordings: new Map(), // v6.15: grabación en vivo de trades reales (demo). mint → {symbol, t0, entryPrice, puntos:[], mov2s, vel, mc, vol}
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
    // v6.13: contadores de filtrado de entrada (para medir qué frena cada capa)
    mom_entered: 0,           // señales que pasaron TODO y abrieron operación
    mom_disc_liquidity: 0,    // Capa 1: descartadas en scan por liquidez baja/desconocida
    mom_disc_drift: 0,        // descartadas por drift (precio ya movido)
    mom_disc_mute: 0,         // Capa 2: descartadas por precio mudo en entrada
    mom_disc_noprice: 0,      // descartadas por no obtener precio fresco
    // v6.13 paso 2: cruce primer movimiento (2s) × resultado, solo migración.
    // Para validar si la dirección temprana predice el resultado (hallazgo del doc).
    mig_mov_up_win: 0,    mig_mov_up_loss: 0,    // mov2s > 0 (subió en 2s)
    mig_mov_flat_win: 0,  mig_mov_flat_loss: 0,  // mov2s ~0 (plano)
    mig_mov_down_win: 0,  mig_mov_down_loss: 0,  // mov2s < 0 (bajó en 2s)
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
const momMuteCooldown = new Map();  // v6.12: mint -> timestamp del último feed mudo (no reentrar 15 min)

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
  const mcMin = OBSERVER_MODE ? OBS_MIN_MC : MIG_MIN_MC;
  const mcMax = OBSERVER_MODE ? Infinity : MIG_MAX_MC;
  if (mcUsd > 0 && (mcUsd < mcMin || mcUsd > mcMax)) {
    addLog(`⛔ MIG MC fuera rango (${formatMC(mcUsd)}): ${coin.symbol}`, "filter");
    broadcast({ event: "stats", data: state.stats });
    return;
  }
  const entry = {
    mint: coin.mint, name: coin.name || "Unknown", symbol: coin.symbol || "???",
    startTime: Date.now(), migratedMcUsd: mcUsd,
    volumeUSD: 0, tradeCount: 0, firstPrice: null, lastPrice: null,
    timer: null, entered: false, pendingEntry: false,
    qualGate: false,
    qualStartPrice: null,
    qualMov2s: null,
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
  if (entry.pendingEntry) {
    entry.lastPrice = price;
    return;
  }
  entry.volumeUSD += solAmount * solPriceUSD;
  entry.tradeCount++;
  entry.lastPrice = price;
  if (!entry.firstPrice && price > 0) entry.firstPrice = price;
  const elapsed = Date.now() - entry.startTime;

  // ── MODO OBSERVADOR ──
  if (OBSERVER_MODE && entry.volumeUSD >= OBS_MIN_VOL && price > 0) {
    clearTimeout(entry.timer);
    entry.entered = true;
    state.migWatching.delete(mint);
    obsStartRecording(entry, price, elapsed);
    return;
  }

  // ── Entrada rápida: $2K en <20s ──
  if (elapsed < MIG_FAST_WINDOW_MS && entry.volumeUSD >= MIG_MIN_VOL_FAST) {
    clearTimeout(entry.timer);
    entry.pendingEntry = true;
    const precioA = entry.lastPrice;
    addLog(`⚡ MIG RÁPIDA: ${entry.symbol} | $${Math.round(entry.volumeUSD)} en ${(elapsed/1000).toFixed(1)}s — confirmando 3s`, "accept");
    broadcast({ event: "stats", data: state.stats });
    setTimeout(() => {
      const precioB = entry.lastPrice;
      if (precioB < precioA * (1 - MIG_MAX_CAIDA_DELAY)) {
        const caida = ((precioB / precioA - 1) * 100).toFixed(1);
        addLog(`🚫 MIG ENTRADA ABORTADA: ${entry.symbol} cayó ${caida}% en la ventana (precio fantasma evitado)`, "filter");
        state.stats.mig_rejected++;
        state.migWatching.delete(mint);
        unsubscribeToken(mint);
        broadcast({ event: "stats", data: state.stats });
        return;
      }
      entry.entered = false;
      entry.pendingEntry = true;
      addLog(`⚡ MIG RÁPIDA confirmada: ${entry.symbol} @ MC ${formatMC(precioB * 1_000_000_000)}`, "accept");
      migQualityGateThenOpen(entry, precioB);
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

// ── v6.15: gate de calidad ──
function migQualityGateThenOpen(entry, entryPriceB) {
  if (!MIG_QUAL_GATE) {
    entry.entered = true;
    state.stats.mig_entered++;
    state.migWatching.delete(entry.mint);
    entry.firstPrice = entryPriceB;
    addLog(`✅ MIG ENTRADA: ${entry.symbol} @ MC ${formatMC(entryPriceB * 1_000_000_000)}`, "accept");
    migOpenTrades(entry);
    return;
  }
  entry.qualGate = true;
  entry.qualStartPrice = entryPriceB;
  entry.qualMov2s = null;
  const t0 = Date.now();
  addLog(`🔍 MIG CALIDAD: ${entry.symbol} — evaluando 15s (mov2s>0 Y pendiente15s>0)`, "filter");
  entry.qualTimer2s = setTimeout(() => {
    if (entry.qualStartPrice > 0 && entry.lastPrice > 0) {
      entry.qualMov2s = (entry.lastPrice / entry.qualStartPrice - 1) * 100;
    }
  }, 2_000);
  setTimeout(() => {
    const priceNow = entry.lastPrice;
    const pend15 = (entry.qualStartPrice > 0 && priceNow > 0)
      ? (priceNow / entry.qualStartPrice - 1) * 100 : -999;
    const mov2 = entry.qualMov2s == null ? -999 : entry.qualMov2s;
    entry.qualGate = false;
    if (mov2 > 0 && pend15 > 0) {
      entry.entered = true;
      state.stats.mig_entered++;
      state.migWatching.delete(entry.mint);
      entry.firstPrice = priceNow;
      addLog(`✅ MIG ENTRADA (calidad ✓): ${entry.symbol} | mov2s ${mov2>=0?"+":""}${mov2.toFixed(1)}% · pend15s ${pend15>=0?"+":""}${pend15.toFixed(1)}% @ MC ${formatMC(priceNow * 1_000_000_000)}`, "accept");
      migOpenTrades(entry);
    } else {
      addLog(`🚫 MIG FILTRO CALIDAD: ${entry.symbol} descartada | mov2s ${mov2<=-999?"n/a":(mov2>=0?"+":"")+mov2.toFixed(1)+"%"} · pend15s ${pend15<=-999?"n/a":(pend15>=0?"+":"")+pend15.toFixed(1)+"%"}`, "filter");
      state.stats.mig_rejected++;
      state.migWatching.delete(entry.mint);
      unsubscribeToken(entry.mint);
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
        addLog(`🚫 MIG ENTRADA ABORTADA: ${entry.symbol} cayó ${caida}% en la ventana (precio fantasma evitado)`, "filter");
        state.stats.mig_rejected++;
        state.migWatching.delete(mint);
        unsubscribeToken(mint);
        broadcast({ event: "stats", data: state.stats });
        return;
      }
      entry.pendingEntry = true;
      addLog(`✅ MIG LENTA confirmada: ${entry.symbol} @ MC ${formatMC(precioB * 1_000_000_000)}`, "accept");
      migQualityGateThenOpen(entry, precioB);
    }, MIG_ENTRY_DELAY_MS);
  } else {
    state.migWatching.delete(mint);
    unsubscribeToken(mint);
    addLog(`❌ MIG RECHAZADO: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol en ${elapsed}s`, "filter");
    state.stats.mig_rejected++;
    broadcast({ event: "stats", data: state.stats });
  }
}

// ── MODO OBSERVADOR: grabación del recorrido ──
function obsStartRecording(entry, entryPrice, velMs) {
  const rec = {
    mint: entry.mint, symbol: entry.symbol,
    vel: +(velMs / 1000).toFixed(1),
    mc: entry.migratedMcUsd || (entryPrice * 1_000_000_000),
    vol: Math.round(entry.volumeUSD),
    t0: Date.now(),
    entryPrice,
    puntos: [{ t: 0, p: 0 }],
    lastSample: Date.now(),
    mov2s: null,
    finished: false,
  };
  state.obsRecordings.set(entry.mint, rec);
  state.stats.mig_entered++;
  addLog(`🔬 OBS GRABANDO: ${entry.symbol} | vel=${rec.vel}s MC=${formatMC(rec.mc)} vol=${rec.vol} — ${OBS_RECORD_MS/60000}min`, "accept");
  rec.timer = setTimeout(() => obsFinishRecording(entry.mint), OBS_RECORD_MS);
}

function obsSample(mint, price) {
  const rec = state.obsRecordings.get(mint);
  if (!rec || rec.finished) return;
  const dt = Date.now() - rec.t0;
  // v6.15.3: muestreo escalonado en 3 tramos (denso al principio, basto en la cola)
  const interval = dt <= OBS_T1_MS ? OBS_T1_INTERVAL
                 : dt <= OBS_T2_MS ? OBS_T2_INTERVAL
                 : OBS_T3_INTERVAL;
  if (Date.now() - rec.lastSample < interval) return;
  rec.lastSample = Date.now();
  const pct = +((price - rec.entryPrice) / rec.entryPrice * 100).toFixed(2);
  rec.puntos.push({ t: Math.round(dt / 1000), p: pct });
  if (rec.mov2s === null && dt >= 2000) rec.mov2s = pct;
}

function obsFinishRecording(mint) {
  const rec = state.obsRecordings.get(mint);
  if (!rec || rec.finished) return;
  rec.finished = true;
  state.obsRecordings.delete(mint);
  unsubscribeToken(mint);

  const pts = rec.puntos;
  let min = pts[0], max = pts[0];
  for (const pt of pts) {
    if (pt.p < min.p) min = pt;
    if (pt.p > max.p) max = pt;
  }
  const orden = min.t <= max.t ? "lava-antes" : "lava-despues";
  const cruces = [10, 15, 20].map(u => {
    let c = 0;
    for (let i = 1; i < pts.length; i++) {
      if (pts[i - 1].p < u && pts[i].p >= u) c++;
    }
    return c;
  });
  const cierreReal = obsSimulaGestionActual(pts);
  const mov2s = rec.mov2s === null ? "n/a" : `${rec.mov2s >= 0 ? "+" : ""}${rec.mov2s}%`;
  const ptsRaw = pts.map(p => `${p.t}:${p.p}`).join(",");
  addLog(
    `[REC] sym=${rec.symbol} vel=${rec.vel}s MC=${formatMC(rec.mc)} vol=${rec.vol} ` +
    `mov2s=${mov2s} MIN=${min.p}%@${min.t}s MAX=${max.p}%@${max.t}s orden=${orden} ` +
    `cruces[10,15,20]=${cruces[0]},${cruces[1]},${cruces[2]} cierre_real=${cierreReal>=0?"+":""}${cierreReal}% ` +
    `pts=${ptsRaw}`,
    "rec"
  );
}

function obsSimulaGestionActual(pts) {
  const STEP_TRIGGER = 20, STEP_FLOOR = 13;
  let armed = false, maxSeen = 0, sl = -18;
  for (const pt of pts) {
    maxSeen = Math.max(maxSeen, pt.p);
    if (!armed && maxSeen >= STEP_TRIGGER) armed = true;
    if (armed) {
      const trail = maxSeen >= 100 ? 5 : maxSeen >= 60 ? 8 : maxSeen >= 40 ? 12 : 15;
      sl = Math.max(sl, maxSeen - trail, STEP_FLOOR);
    }
    if (pt.p <= sl) return +sl.toFixed(1);
  }
  return +pts[pts.length - 1].p.toFixed(1);
}

// ── v6.15: GRABACIÓN EN VIVO ──
function liveRecStart(entry, entryPrice) {
  if (!LIVE_RECORD || entryPrice <= 0) return;
  const velMs = entry.qualStartPrice != null ? (Date.now() - entry.startTime) : (Date.now() - entry.startTime);
  const rec = {
    mint: entry.mint, symbol: entry.symbol,
    vel: +(velMs / 1000).toFixed(1),
    mc: entry.migratedMcUsd || (entryPrice * 1_000_000_000),
    vol: Math.round(entry.volumeUSD || 0),
    t0: Date.now(),
    entryPrice,
    puntos: [{ t: 0, p: 0 }],
    lastSample: Date.now(),
    mov2s: null,
    finished: false,
  };
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
  rec.puntos.push({ t: Math.round(dt / 1000), p: pct });
  if (rec.mov2s === null && dt >= 2000) rec.mov2s = pct;
}

function liveRecFinish(mint, cierreRealPct) {
  if (!LIVE_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished) return;
  rec.finished = true;
  state.liveRecordings.delete(mint);
  const pts = rec.puntos;
  if (pts.length < 2) return;
  let min = pts[0], max = pts[0];
  for (const pt of pts) { if (pt.p < min.p) min = pt; if (pt.p > max.p) max = pt; }
  const orden = min.t <= max.t ? "lava-antes" : "lava-despues";
  const cruces = [10, 15, 20].map(u => {
    let c = 0;
    for (let i = 1; i < pts.length; i++) if (pts[i - 1].p < u && pts[i].p >= u) c++;
    return c;
  });
  const mov2s = rec.mov2s === null ? "n/a" : `${rec.mov2s >= 0 ? "+" : ""}${rec.mov2s}%`;
  const cr = +(+cierreRealPct).toFixed(1);
  const ptsRaw = pts.map(p => `${p.t}:${p.p}`).join(",");
  addLog(
    `[MIGREC] sym=${rec.symbol} vel=${rec.vel}s MC=${formatMC(rec.mc)} vol=${rec.vol} ` +
    `mov2s=${mov2s} MIN=${min.p}%@${min.t}s MAX=${max.p}%@${max.t}s orden=${orden} ` +
    `cruces[10,15,20]=${cruces[0]},${cruces[1]},${cruces[2]} cierre_real=${cr>=0?"+":""}${cr}% ` +
    `pts=${ptsRaw}`,
    "rec"
  );
}

// ── v6.15.4: GRABACIÓN DE MOMENTUM (aislada, no toca migración) ──
// momMonitored guarda el token; arrancamos al activar el trade.
function momRecStart(mint, symbol, entryPrice, meta) {
  if (!MOM_RECORD || entryPrice <= 0) return;
  const rec = {
    mint, symbol,
    pct1h: meta?.pct1h ?? null,
    vol1h: meta?.vol1h ?? null,
    mc: meta?.mc ?? (entryPrice * 1_000_000_000),
    t0: Date.now(),
    entryPrice,
    puntos: [{ t: 0, p: 0 }],
    lastSample: Date.now(),
    finished: false,
  };
  state.liveRecordings.set(mint, rec);   // comparte el Map, pero clave distinta por mint
}

function momRecSample(mint, price) {
  if (!MOM_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished || price <= 0) return;
  // El feed de momentum es el scan (~15s); muestreamos cada vez que llega un precio,
  // sin sub-intervalo: la propia frecuencia del scan marca el ritmo.
  const dt = Date.now() - rec.t0;
  const pct = +((price - rec.entryPrice) / rec.entryPrice * 100).toFixed(2);
  const last = rec.puntos[rec.puntos.length - 1];
  if (last && last.t === Math.round(dt / 1000)) return;   // evita duplicar el mismo segundo
  rec.puntos.push({ t: Math.round(dt / 1000), p: pct });
}

function momRecFinish(mint, cierreRealPct) {
  if (!MOM_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished) return;
  rec.finished = true;
  state.liveRecordings.delete(mint);
  const pts = rec.puntos;
  if (pts.length < 2) return;   // sin recorrido útil
  let min = pts[0], max = pts[0];
  for (const pt of pts) { if (pt.p < min.p) min = pt; if (pt.p > max.p) max = pt; }
  const orden = min.t <= max.t ? "lava-antes" : "lava-despues";
  // Cruces en los umbrales relevantes de momentum: breakeven (+3), lock (+5), TP (+6)
  const cruces = [3, 5, 6].map(u => {
    let c = 0;
    for (let i = 1; i < pts.length; i++) if (pts[i - 1].p < u && pts[i].p >= u) c++;
    return c;
  });
  // Clave para decidir el breakeven: ¿cuánto cayó DESPUÉS de tocar su máximo?
  // (grupo A: siguió cayendo, el breakeven la salvó · grupo B: solo bacheó, la estranguló)
  let minTrasMax = max.p;
  for (const pt of pts) { if (pt.t > max.t && pt.p < minTrasMax) minTrasMax = pt.p; }
  const cr = +(+cierreRealPct).toFixed(1);
  const ptsRaw = pts.map(p => `${p.t}:${p.p}`).join(",");
  addLog(
    `[MOMREC] sym=${rec.symbol} pct1h=${rec.pct1h ?? "?"} MC=${formatMC(rec.mc)} ` +
    `MAX=${max.p}%@${max.t}s MIN=${min.p}%@${min.t}s minTrasMax=${minTrasMax}% orden=${orden} ` +
    `cruces[3,5,6]=${cruces[0]},${cruces[1]},${cruces[2]} cierre_real=${cr>=0?"+":""}${cr}% ` +
    `pts=${ptsRaw}`,
    "rec"
  );
}

function migOpenTrades(entry) {
  const price = entry.firstPrice;
  if (!price || price <= 0) return;
  liveRecStart(entry, price);
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
  liveRecSample(mint, price);
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
    const url = `${BIRDEYE_TOKEN_LIST}?sort_by=volume_1h_usd&sort_type=desc`
      + `&min_liquidity=${MOM_MIN_LIQUIDITY}`
      + `&min_market_cap=${MOM_MIN_MC}&max_market_cap=${MOM_MAX_MC}`
      + `&offset=0&limit=100`;   // v6.15.2: subido de 50. Misma 1 petición por scan (sin coste extra), pero mantiene en cobertura a más tokens abiertos antes de que se caigan del top por volumen y dejen de recibir precio
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
      const poolAddr = mint;
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
      // ── v6.13 Capa 1: filtro de liquidez ──
      if (!liquidity || liquidity < MOM_MIN_LIQUIDITY) {
        addLog(`⛔ MOM liquidez baja/desconocida (${formatMC(liquidity)}): ${tok.symbol || mint.slice(0,8)}`, "filter");
        state.stats.mom_disc_liquidity++;
        continue;
      }
      if (pct1h < MOM_MIN_PCT_1H) continue;
      if (pct1h > MOM_MAX_PCT_1H) continue;
      // ── v6.12 Fix B: saltar tokens que expiraron mudos hace poco ──
      const muteAt = momMuteCooldown.get(mint) || 0;
      if (Date.now() - muteAt < MOM_MUTE_COOLDOWN_MS) {
        continue;
      }
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
      setTimeout(async () => {
        if (!state.momPending.has(mint)) return;
        const pending = state.momPending.get(mint);
        // ── v6.12 Fix A: precio fresco + validación de drift ──
        const freshPrice = await birdeyeFreshPrice(mint);
        if (!freshPrice) {
          addLog(`⛔ MOM sin precio fresco: ${pending.symbol} — no entra`, "filter");
          state.stats.mom_disc_noprice++;
          state.momPending.delete(mint); state.stats.mom_pending = state.momPending.size;
          return;
        }
        const drift = Math.abs(freshPrice - pending.geckoPrice) / pending.geckoPrice;
        if (drift > MOM_MAX_ENTRY_DRIFT) {
          addLog(`⛔ MOM drift ${(drift*100).toFixed(1)}% (${pending.symbol}) — la vela ya se movió, NO entra`, "filter");
          state.stats.mom_disc_drift++;
          state.momPending.delete(mint); state.stats.mom_pending = state.momPending.size;
          return;
        }
        // ── v6.13 Capa 2: confirmar que el precio se MUEVE ──
        await new Promise(r => setTimeout(r, MOM_MUTE_CHECK_MS));
        if (!state.momPending.has(mint)) return;
        const secondPrice = await birdeyeFreshPrice(mint);
        if (!secondPrice) {
          addLog(`⛔ MOM 2ª lectura sin precio: ${pending.symbol} — no entra`, "filter");
          state.stats.mom_disc_noprice++;
          state.momPending.delete(mint); state.stats.mom_pending = state.momPending.size;
          return;
        }
        const move = Math.abs(secondPrice - freshPrice) / freshPrice;
        if (move < MOM_MUTE_MIN_MOVE) {
          addLog(`🔇 MOM MUDO en entrada: ${pending.symbol} — precio movió ${(move*100).toFixed(2)}% en ${MOM_MUTE_CHECK_MS/1000}s, NO entra`, "filter");
          state.stats.mom_disc_mute++;
          momMuteCooldown.set(mint, Date.now());
          state.momPending.delete(mint); state.stats.mom_pending = state.momPending.size;
          return;
        }
        addLog(`⚡ ENTRADA [vivo]: ${pending.symbol} @ $${secondPrice.toFixed(8)} (movió ${(move*100).toFixed(2)}%)`, "accept");
        state.stats.mom_entered++;
        momActivateFromPending(mint, secondPrice, 0);
      }, MOM_PENDING_TIMEOUT_MS);
      broadcast({ event: "stats", data: state.stats });
    }
    addLog(`⚡ Scan: ${totalScanned} candidatos, ${totalSignals} señales nuevas`, "info");
    broadcast({ event: "stats", data: state.stats });
  } catch (e) {
    addLog(`❌ Momentum scan error: ${e.message}`, "error");
  }
}

// v6.12: precio puntual fresco de Birdeye
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
  momRecStart(mint, pending.symbol, entryPrice, { pct1h: pending.pct1h, vol1h: pending.vol1h, mc: pending.mc }); // v6.15.4: grabar recorrido
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
  momRecSample(mint, price);   // v6.15.4: muestrear el recorrido del trade de momentum
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
  if (!REAL_STRATEGIES.includes(signal.strategy)) return;
  const openReal = state.realTrades.filter(t => t.status === "OPEN");
  const stratOpen = openReal.filter(t => t.strategy === signal.strategy).length;
  if (stratOpen >= 1) { addLog(`⚠️ Ya hay real abierta (${signal.strategy})`, "warn"); return; }
  if (openReal.length >= MAX_REAL_TRADES) return;
  const balance = await getWalletBalance();
  if (balance < SOL_PER_TRADE + 0.01) { addLog(`⚠️ Balance insuficiente: ${balance.toFixed(3)} SOL`, "warn"); return; }
  const sig = await buyToken(signal.mint, SOL_PER_TRADE);
  if (!sig) return;
  const duration = isMig(signal.strategy) ? MIG_DURATION_MS : MOM_DURATION_MS;
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
  const prefix = stratPrefix(trade.strategy);
  const expWinPct = isMig(trade.strategy) ? MIG_EXPIRED_WIN_PCT : MOM_EXPIRED_WIN_PCT;
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
  if (isMig(trade.strategy)) migCleanup(trade.mint, trade.symbol);
  if (trade.strategy === "momentum") momCleanup(trade.mint, trade.symbol);
  broadcast({ event: "realTradeClosed", data: trade });
  broadcast({ event: "stats", data: state.stats });
  saveState();
}

// v6.10: trailing escalonado para migración.
function migTrailingPct(maxGainPct) {
  if (maxGainPct >= MIG_TRAIL_T3) return MIG_TRAIL_P4;
  if (maxGainPct >= MIG_TRAIL_T2) return MIG_TRAIL_P3;
  if (maxGainPct >= MIG_TRAIL_T1) return MIG_TRAIL_P2;
  return MIG_FOLLOW_PCT_STEP;
}

function isMig(strategy) { return strategy === "migration"; }
// v6.12 Palanca 1: detecta volcado vertical.
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
  const drop = (prevP - price) / prevP;
  return drop >= MIG_VELO_DROP;
}
function stratPrefix(strategy) {
  if (strategy === "migration") return "mig";
  return "mom";
}

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
    // ── v6.12 Palanca 1: salida por velocidad ──
    if (veloDropTriggered(trade, price, strategy)) {
      addLog(`⚡🛑 VELO-EXIT [${strategy} real]: ${trade.symbol} caída rápida @ ${currentPct.toFixed(1)}% — vendiendo ya`, "realloss");
      closeRealTrade(trade, price, "SL");
      continue;
    }
    // ── v6.8: cap loss duro (solo Momentum) ──
    if (strategy === "momentum" && currentPct <= MOM_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [momentum real]: ${trade.symbol} ${currentPct.toFixed(1)}%`, "realloss");
      closeRealTrade(trade, price, "SL");
      continue;
    }
    // ── v6.10: cap loss duro (solo Migración, -20%) ──
    if (isMig(strategy) && currentPct <= MIG_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [${strategy} real]: ${trade.symbol} ${currentPct.toFixed(1)}%`, "realloss");
      closeRealTrade(trade, price, "SL");
      continue;
    }
    const gainPct = (price - trade.entryPrice) / trade.entryPrice;
    const stepArmed = isMig(strategy) && trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9;
    const followEff = (isMig(strategy) && stepArmed) ? migTrailingPct(trade.maxGainPct) : follow;
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
    const stepFloorPrice = trade.entryPrice * (1 + MIG_STEP_FLOOR);
    if (stepArmed && stepFloorPrice > trade.sl) {
      trade.sl = +stepFloorPrice.toFixed(12);
    }
    if (isMig(strategy) && trade.maxGainPct >= MIG_TOP_FLOOR_TRIGGER) {
      const topFloorPrice = trade.entryPrice * (1 + MIG_TOP_FLOOR);
      if (topFloorPrice > trade.sl) trade.sl = +topFloorPrice.toFixed(12);
    }
    else if (price <= trade.sl) {
      const stopProtegeGanancia = trade.sl >= trade.entryPrice;
      if (stopProtegeGanancia) {
        const reason = (stepArmed && Math.abs(trade.sl - stepFloorPrice) < 1e-9) ? "STEP" : "SL";
        closeRealTrade(trade, price, reason);
      } else {
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
    if (trade.strategy === "momentum") {
      const aliveMs = now - trade.openTime;
      const sinMovimiento = (trade.maxGainPct === 0 && trade.maxLossPct === 0);
      if (aliveMs >= MOM_MUTE_TIMEOUT_MS && sinMovimiento) {
        const tk = state.momMonitored.get(trade.mint);
        addLog(`🔇 MOM FEED MUDO [real]: ${trade.symbol} — sin trades en ${Math.round(aliveMs/1000)}s, cancelando`, "warn");
        momMuteCooldown.set(trade.mint, Date.now());
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
  const duration = isMig(signal.strategy) ? MIG_DURATION_MS : MOM_DURATION_MS;
  const trade = {
    id: `demo-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    strategy: signal.strategy, mint: signal.mint, symbol: signal.symbol, name: signal.name,
    entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: "INITIAL", status: "OPEN",
    expiresAt: Date.now() + duration,
    mov1s: null, mov2s: null,
  };
  state.demoTrades.unshift(trade);
  if (state.demoTrades.length > 500) state.demoTrades.pop();
  state.stats.demoOpen++;
  broadcast({ event: "newDemoTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  const tpPct = isMig(signal.strategy) ? "+150%" : "+6%";
  const slPct = isMig(signal.strategy) ? "-18%" : "-3%";
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
    // ── v6.13 paso 2: registrar primer movimiento a 1s y 2s (solo migración) ──
    if (isMig(strategy)) {
      const sinceOpen = now - trade.openTime;
      if (trade.mov1s === null && sinceOpen >= 1000) trade.mov1s = +currentPct.toFixed(2);
      if (trade.mov2s === null && sinceOpen >= 2000) trade.mov2s = +currentPct.toFixed(2);
    }
    // ── v6.12 Palanca 1: salida por velocidad ──
    if (veloDropTriggered(trade, price, strategy)) {
      addLog(`⚡🛑 VELO-EXIT [${strategy}]: ${trade.symbol} caída rápida @ ${currentPct.toFixed(1)}% — vendiendo ya`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct);
      continue;
    }
    // ── v6.8: cap loss duro (solo Momentum) ──
    if (strategy === "momentum" && currentPct <= MOM_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [momentum]: ${trade.symbol} ${currentPct.toFixed(1)}% (tope ${MOM_HARD_CAP_LOSS}%)`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct);
      continue;
    }
    // ── v6.10: cap loss duro (solo Migración, -20%) ──
    if (isMig(strategy) && currentPct <= MIG_HARD_CAP_LOSS) {
      addLog(`🛑 CAP LOSS [${strategy}]: ${trade.symbol} ${currentPct.toFixed(1)}% (tope ${MIG_HARD_CAP_LOSS}%)`, "loss");
      closeDemoTrade(trade, price, "SL", tp_pct);
      continue;
    }
    const gainPct = (price - trade.entryPrice) / trade.entryPrice;
    const stepArmed = isMig(strategy) && trade.maxGainPct >= MIG_STEP_TRIGGER * 100 - 1e-9;
    const followEff = (isMig(strategy) && stepArmed) ? migTrailingPct(trade.maxGainPct) : follow;
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
    const stepFloorPrice = trade.entryPrice * (1 + MIG_STEP_FLOOR);
    if (stepArmed && stepFloorPrice > trade.sl) {
      if (!trade._stepLogged) {
        trade._stepLogged = true;
        addLog(`🪜 ESCALÓN +13% suelo [${strategy}]: ${trade.symbol} (tocó +${trade.maxGainPct.toFixed(0)}%)`, "trail");
      }
      trade.sl = +stepFloorPrice.toFixed(12);
    }
    if (isMig(strategy) && trade.maxGainPct >= MIG_TOP_FLOOR_TRIGGER) {
      const topFloorPrice = trade.entryPrice * (1 + MIG_TOP_FLOOR);
      if (topFloorPrice > trade.sl) {
        if (!trade._topFloorLogged) {
          trade._topFloorLogged = true;
          addLog(`🏔️ SUELO +65% [${strategy}]: ${trade.symbol} (tocó +${trade.maxGainPct.toFixed(0)}%)`, "trail");
        }
        trade.sl = +topFloorPrice.toFixed(12);
      }
    }
    if (price >= trade.tp) { trade._slBelowCount = 0; closeDemoTrade(trade, price, "TP", tp_pct); }
    else if (price <= trade.sl) {
      const stopProtegeGanancia = trade.sl >= trade.entryPrice;
      if (stopProtegeGanancia) {
        const reason = (stepArmed && Math.abs(trade.sl - stepFloorPrice) < 1e-9) ? "STEP" : "SL";
        closeDemoTrade(trade, trade.sl, reason, tp_pct);
      } else {
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
    // FIX 0%=win (v6.15.1): el cierre plano en 0% ya NO cuenta como WIN. Antes era >= 0,
    // lo que inflaba el win rate (~12% de las "wins" eran ceros) y hacía decidir a ciegas.
    // El P&L no cambia (un 0% suma 0); solo el win rate pasa a ser honesto.
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
  // ── v6.13 paso 2: cruce primer movimiento (2s) × resultado, solo migración ──
  if (isMig(trade.strategy) && trade.mov2s !== null && trade.result !== "EXPIRED") {
    const bucket = trade.mov2s > 1 ? "up" : (trade.mov2s < -1 ? "down" : "flat");
    const wl = trade.result === "WIN" ? "win" : "loss";
    state.stats[`mig_mov_${bucket}_${wl}`]++;
  }
  if (isMig(trade.strategy)) liveRecFinish(trade.mint, trade.pnlPct);
  if (trade.strategy === "momentum") momRecFinish(trade.mint, trade.pnlPct);  // v6.15.4: [MOMREC] con cierre real
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
      const sinMovimiento = (trade.maxGainPct === 0 && trade.maxLossPct === 0);
      if (aliveMs >= MOM_MUTE_TIMEOUT_MS && sinMovimiento) {
        const tk = state.momMonitored.get(trade.mint);
        addLog(`🔇 MOM FEED MUDO: ${trade.symbol} — sin trades en ${Math.round(aliveMs/1000)}s, cancelando`, "warn");
        momMuteCooldown.set(trade.mint, Date.now());
        closeDemoTrade(trade, tk?.price || trade.entryPrice, "EXPIRED", MOM_TP);
        continue;
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
        if (OBSERVER_MODE && state.obsRecordings.has(data.mint)) obsSample(data.mint, price);
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
  console.log(`🚀 SolScanBot v6.15.4 — grabación momentum [MOMREC] ${MOM_RECORD ? "ON" : "off"} (recorrido + cierre real, para decidir breakeven con datos) | etiquetas: [MIGREC]/[MOMREC]/[REC] | OBSERVADOR ${OBSERVER_MODE ? "ACTIVO ⚠️" : "off"} | momentum scan ${MOM_SCAN_MS/1000}s`);
  loadState();
  initWallet();
  connectPumpPortal();
  connectHelius();
  setTimeout(momentumScan, 5000);
  setInterval(momentumScan, MOM_SCAN_MS);
});
