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

const PORT = process.env.PORT || 3001; // App Migración
const SOL_PER_TRADE_MIG = 0.15;   // App Migración: 0.15 SOL por posición
const SOL_PER_TRADE = 0.15;       // fallback
const MAX_REAL_TRADES = 6;         // máximo 6 simultáneas de migración
const MAX_MIG_REAL = 6;            // máximo 6 reales de migración
// Estrategias que pueden operar en REAL. El resto sigue solo en demo aunque MAX_REAL_TRADES>0.
// Ej: ["migration"] = solo migración en real, momentum y re-migración se quedan en demo.
const REAL_STRATEGIES = ["migration"];
const STATE_FILE = "/tmp/solscanbot_migracion_state.json";

// ── CONFIG MIGRACIÓN ───────────────────────────────────────────
const MIG_TP = 4.00;              // v6.15: +300% (red de seguridad alta; imprescindible con el armado tardío, si no corta los pelotazos +300%+)
const MIG_SL = 0.80;              // v6.15: -20% (era -18%; aviso de la auditoría: bajar riesgo por op, el agregado apenas pierde)
const MIG_DURATION_MS = 15 * 60 * 1000; // 15 min — más tiempo para el movimiento
const MIG_WINDOW_MS = 60_000;
const MIG_MIN_VOL_FAST = 1_500;
const MIG_MIN_VOL_SLOW = 2_000;
const MIG_FAST_WINDOW_MS = 20_000;
const MIG_MIN_MC = 0_000;
const MIG_MAX_MC = 2_000_000;

// ── MODO OBSERVADOR (recogelotodo) ────────────────────────────────
// Día de recolección: migración entra en CASI TODO (umbral permisivo) y graba
// el recorrido de precio 4 min por token. Escribe una línea [REC] por migración.
// Ese día el P&L se ignora (se llena de operaciones malas a propósito). Apagar
// para volver a la operativa normal. NO toca momentum.
const OBSERVER_MODE = false;          // ⬅️ APAGADO: opera en real (era true = solo grababa, no compraba)
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


const HELIUS_API_KEY = "86268796-07db-4bab-8e4f-abc4f697f64d";
const SOLANA_RPC = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data?api-key=e12mybvnahb5cx2uahup8y1rahn4ewbp99rn4j2u6h6mmy37f1c7cdakf5432kbkcctmmwkcdd37cgke718qey9ne96mpy1mdncmjmut6crkeeb5f5n7ac1gf137auudd56m4u1tcwyku6h130u3m9164cdad99rmuxjpd8b9qq4d3bddu76wu7ad270k2h7155gnbm5x0kuf8";
const GECKO_PUMPSWAP = "https://api.geckoterminal.com/api/v2/networks/solana/dexes/pumpswap/pools";

let wallet = null;
let connection = null;       // Helius Developer — envío de transacciones Y lectura de balance
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
const BALANCE_CACHE_MS = 30_000; // solo consultar RPC cada 30s

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

const state = {
  migWatching: new Map(),
  migMonitored: new Map(),
  obsRecordings: new Map(),  // modo observador: mint → {symbol, vel, mc, vol, t0, entryPrice, puntos:[], mov2s, ...}
  liveRecordings: new Map(), // v6.15: grabación en vivo de trades reales (demo). mint → {symbol, t0, entryPrice, puntos:[], mov2s, vel, mc, vol}

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
    // v6.13 paso 2: cruce primer movimiento (2s) × resultado, solo migración.
    mig_mov_up_win: 0,    mig_mov_up_loss: 0,
    mig_mov_flat_win: 0,  mig_mov_flat_loss: 0,
    mig_mov_down_win: 0,  mig_mov_down_loss: 0,
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
  if (!solPriceReady) {
    solPriceReady = true;
    addLog(`⚠️ Usando SOL price fallback $${solPriceUSD} — operativa desbloqueada`, "warn");
  }
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
  // v6.15.5: cruces actualizados a [50,70,100] (umbrales relevantes con armado+70)
  // +50: zona de aproximación al armado · +70: arma el trailing · +100: suelo +65%
  const cruces = [50, 70, 100].map(u => {
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
    `cruces[50,70,100]=${cruces[0]},${cruces[1]},${cruces[2]} cierre_real=${cierreReal>=0?"+":""}${cierreReal}% ` +
    `pts=${ptsRaw}`,
    "rec"
  );
}

function obsSimulaGestionActual(pts) {
  // v6.15.5: actualizado a la gestión REAL del server (era v6.10: armado+20, SL-18).
  // Parámetros actuales: armado+70 (LOCK/STEP_TRIGGER), SL-20, trailing -8%(60-100) / -5%(+100)
  // Suelo +13% (STEP_FLOOR) y suelo +65% (TOP_FLOOR al tocar +100) incluidos.
  // Los tramos -15% y -12% son código muerto con armado+70 (cuando arma, MAX ya >=70),
  // pero se incluyen por fidelidad al código real por si algún token arma justo en +70.
  const STEP_TRIGGER = 70, STEP_FLOOR = 13;
  const TOP_FLOOR_TRIGGER = 100, TOP_FLOOR = 65;
  let armed = false, topFloor = false, maxSeen = 0, sl = -20;
  for (const pt of pts) {
    maxSeen = Math.max(maxSeen, pt.p);
    if (!armed && maxSeen >= STEP_TRIGGER) armed = true;
    if (!topFloor && maxSeen >= TOP_FLOOR_TRIGGER) topFloor = true;
    if (armed) {
      // trailing escalonado idéntico al server (MIG_TRAIL_T1/T2/T3/P1/P2/P3/P4)
      const trail = maxSeen >= 100 ? 5 : maxSeen >= 60 ? 8 : maxSeen >= 40 ? 12 : 15;
      sl = Math.max(sl, maxSeen - trail, STEP_FLOOR);
    }
    if (topFloor) {
      sl = Math.max(sl, TOP_FLOOR);  // suelo +65% una vez tocado +100
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
// TRADING COMPARTIDO
// ════════════════════════════════════════════════════════════════

// Lee cuánto SOL movió EXACTAMENTE una transacción confirmada, mirando el cambio
// de saldo de nuestra wallet dentro de esa tx (pre/postBalances). Es el mismo número
// que muestra Solscan, ya neto de fee de red y slippage. No depende del timing de la
// caché de balance ni se contamina con operaciones simultáneas (lee SÓLO esta tx).
// Devuelve el delta en SOL: negativo si la wallet pagó (compra), positivo si recibió (venta).
async function getSolDeltaFromTx(sig, retries = 6) {
  if (!wallet || !connection) return null;
  const me = wallet.publicKey.toString();
  for (let i = 0; i < retries; i++) {
    try {
      const tx = await connection.getParsedTransaction(sig, {
        maxSupportedTransactionVersion: 0,
        commitment: "confirmed",
      });
      if (tx?.meta && tx.transaction?.message?.accountKeys) {
        const keys = tx.transaction.message.accountKeys;
        let idx = -1;
        for (let k = 0; k < keys.length; k++) {
          const pk = keys[k]?.pubkey ? keys[k].pubkey.toString() : keys[k]?.toString?.();
          if (pk === me) { idx = k; break; }
        }
        if (idx >= 0 && tx.meta.preBalances && tx.meta.postBalances) {
          const delta = (tx.meta.postBalances[idx] - tx.meta.preBalances[idx]) / LAMPORTS_PER_SOL;
          return +delta.toFixed(6);
        }
      }
    } catch (e) { /* reintentar */ }
    await new Promise(r => setTimeout(r, 1500)); // dar tiempo a que el RPC indexe la tx
  }
  addLog(`⚠️ No se pudo leer el SOL movido de la tx ${shortAddr(sig)} tras ${retries} intentos`, "warn");
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
    // Coste real = lo que bajó nuestra wallet en esta tx (token + fee + slippage)
    const delta = await getSolDeltaFromTx(sig);
    const costSol = delta != null ? +(-delta).toFixed(6) : solAmount; // fallback al nominal si falla la lectura
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
    // Recibido real = lo que subió nuestra wallet en esta tx (ya neto de fee y slippage)
    const delta = await getSolDeltaFromTx(sig);
    const proceedsSol = delta != null ? +delta.toFixed(6) : 0;
    addLog(`✅ VENTA: ${shortAddr(mint)} | recibido real ${proceedsSol} SOL | ${sig}`, "real");
    return { sig, proceedsSol };
  } catch (e) { addLog(`❌ Venta: ${e.message}`, "error"); return null; }
}

async function openRealTrade(signal) {
  if (!wallet) return;
  if (!REAL_STRATEGIES.includes(signal.strategy)) return;
  const openReal = state.realTrades.filter(t => t.status === "OPEN");
  if (openReal.length >= MAX_REAL_TRADES) return;
  const stratOpen = openReal.filter(t => t.strategy === signal.strategy).length;
  if (stratOpen >= MAX_MIG_REAL) { addLog(`⚠️ Límite real [migración]: ${stratOpen}/${MAX_MIG_REAL}`, "warn"); return; }
  const solAmount = SOL_PER_TRADE_MIG;
  const balance = await getWalletBalance();
  if (balance < solAmount + 0.01) { addLog(`⚠️ Balance insuficiente: ${balance.toFixed(3)} SOL (necesito ${(solAmount + 0.01).toFixed(2)})`, "warn"); return; }
  const sig = await buyToken(signal.mint, solAmount);
  if (!sig) return;
  const duration = MIG_DURATION_MS;
  const trade = {
    id: `real-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    strategy: signal.strategy, mint: signal.mint, symbol: signal.symbol, name: signal.name,
    entryPrice: signal.price, tp: signal.tp, sl: signal.sl, solAmount: solAmount,
    costSol: sig.costSol,
    buySignature: sig.sig, sellSignature: null,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, pnlSol: null,
    maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: "INITIAL", status: "OPEN",
    expiresAt: Date.now() + duration, sellRetries: 0,
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
  // P&L REAL del ciclo de este trade: lo que entró al vender menos lo que costó comprar.
  // costSol y proceedsSol se miden con balance fresco pegado a cada tx, así que no se
  // contaminan con compras/ventas simultáneas de otras migraciones.
  const proceedsSol = sell.proceedsSol;
  const costSol = (trade.costSol != null && trade.costSol > 0) ? trade.costSol : trade.solAmount;
  const realPnlSol = +(proceedsSol - costSol).toFixed(4);
  // tick = lo que "debería" haber dado el movimiento de precio sin fricción
  const tickPnlSol = +(costSol * (price - trade.entryPrice) / trade.entryPrice).toFixed(4);
  const slipFeeSol = +(realPnlSol - tickPnlSol).toFixed(4);
  trade.sellSignature = sell.sig; trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
  const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
  trade.pnlPct = +pnlPct.toFixed(2);
  trade.pnlSol = realPnlSol;
  trade.slipFeeSol = slipFeeSol;
  addLog(`📊 PnL real: ${realPnlSol >= 0 ? "+" : ""}${realPnlSol} SOL (coste ${costSol} → recibido ${proceedsSol}) | tick: ${tickPnlSol >= 0 ? "+" : ""}${tickPnlSol} | slip+fee: ${slipFeeSol >= 0 ? "+" : ""}${slipFeeSol}`, "real");
  const dur = Math.round((trade.closeTime - trade.openTime) / 1000);
  const prefix = stratPrefix(trade.strategy);
  const expWinPct = MIG_EXPIRED_WIN_PCT;
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
  state.stats.walletBalance = await getWalletBalance(true);
  if (isMig(trade.strategy)) migCleanup(trade.mint, trade.symbol);

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
    // ── v6.12 Palanca 1: salida por velocidad ──
    if (veloDropTriggered(trade, price, strategy)) {
      addLog(`⚡🛑 VELO-EXIT [${strategy} real]: ${trade.symbol} caída rápida @ ${currentPct.toFixed(1)}% — vendiendo ya`, "realloss");
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

    // ── DETECCIÓN DE FEED MUERTO (v6.16.9) ──
    // Si a los 30s de abrir el trade NUNCA se ha movido el % (maxGain y maxLoss
    // siguen exactamente en 0), es que no llega ningún tick de precio: el pool
    // está muerto (igual que las expiradas que cerraban en 0% tras 15 min).
    // Cerrar YA mientras quede algo de liquidez, en vez de esperar a expirar.
    // Un token vivo (aunque sea plano) recibe ticks y mueve el % del 0 enseguida.
    const sinceOpen = now - trade.openTime;
    if (sinceOpen >= 30_000 && trade.maxGainPct === 0 && trade.maxLossPct === 0) {
      addLog(`💀 FEED MUERTO [migration real]: ${trade.symbol} sin ticks en ${Math.round(sinceOpen/1000)}s — cerrando (pool sin liquidez)`, "realloss");
      const token = state.migMonitored.get(trade.mint);
      closeRealTrade(trade, token?.price || trade.entryPrice, "DEAD_FEED");
      continue;
    }

    if (now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint);
    closeRealTrade(trade, token?.price || trade.entryPrice, "EXPIRED");
  }
}, 10_000);

function openDemoTrade(signal) {
  const duration = MIG_DURATION_MS;
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
  addLog(`📝 DEMO [migración]: ${signal.symbol} | TP +300% SL -20%`, "demo");
}

function updateDemoTrades(mint, price, strategy) {
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
  const expWinPct = MIG_EXPIRED_WIN_PCT;
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

  if (isMig(trade.strategy)) migCleanup(trade.mint, trade.symbol);

  broadcast({ event: "stats", data: state.stats });
  saveState();
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.status !== "OPEN") continue;

    if (now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint);
    const tp_pct = MIG_TP;
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
  console.log(`🚀 SolScanBot-MIGRACION v6.16.9 — SNIPER migración pump.fun→PumpSwap | OBSERVER ${OBSERVER_MODE ? "ACTIVO ⚠️" : "off"} | RPC: Helius Developer | MAX_REAL: ${MAX_MIG_REAL} × ${SOL_PER_TRADE_MIG} SOL`);
  loadState();
  initWallet();
  connectPumpPortal();
  connectHelius();

});
