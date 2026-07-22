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
// ═══ EXPERIMENTO REAL (7-jul): lote micro 0.1 SOL × 2 días para MEDIR LA FRICCIÓN
// (slippage+fees reales vs tick). El demo sigue corriendo en paralelo con 0.5 para
// comparar op a op. Objetivo: saber si el edge (+2.8%/op en demo) sobrevive al peaje
// real. NO es para ganar dinero todavía. Requiere: keys ROTADAS + wallet dedicada.
const SOL_PER_TRADE_REAL = +(process.env.SOL_PER_TRADE_REAL || 0.25); // [v11.7f] fase de medición
const MIG_MAX_MC_REAL = 1_000_000; // [v11.7f] IDÉNTICO al demo (antes 200K: divergencia eliminada)
const SOL_PER_TRADE_MIG = 0.5;
const MAX_REAL_TRADES = 10;
const MAX_MIG_REAL = 10;
const REAL_STRATEGIES = ["migration", "reentry", "fuerza"]; // [v11.7f] real = demo

// ── [v11.7f] EJECUCIÓN REAL — obra de fricción ──────────────────────────────
// EXEC_MODE: "pp" = PumpPortal trade-local (actual, 0.5%/lado)
//            "hybrid" = compra por PP (velocidad en el seg 3) + VENTAS por Jupiter (0% router)
//            "jup" = todo por Jupiter (requiere que indexe el pool; puede fallar en entradas tempranas)
// Las ventas por Jupiter llevan fallback automático a PP si no hay ruta.
const EXEC_MODE = (process.env.EXEC_MODE || "pp").toLowerCase();
const JUP_BASE  = process.env.JUP_BASE || "https://quote-api.jup.ag/v6";
const WSOL_MINT = "So11111111111111111111111111111111111111112";
// Slippage por URGENCIA (%): la salida tranquila no debe pagar el peaje de la salida de pánico
const SLIP_ENTRY = +(process.env.SLIP_ENTRY || 15);   // entrada sniper: ancho, hay que entrar
const SLIP_PANIC = +(process.env.SLIP_PANIC || 30);   // SL / NO_LAUNCH / DEAD_FEED: salir como sea
const SLIP_CALM  = +(process.env.SLIP_CALM  || 6);    // STEP / RUNNER_END / TP / EXPIRED: sin prisa
// Priority fee por URGENCIA (SOL). El 0.0005 plano era barato para sniping: cada slot tarde ≈ 0.5-1% de precio
const PRIO_ENTRY = +(process.env.PRIO_ENTRY || 0.003);
const PRIO_PANIC = +(process.env.PRIO_PANIC || 0.004);
const PRIO_CALM  = +(process.env.PRIO_CALM  || 0.0005);
const execParams = (u) => u === "entry" ? { slip: SLIP_ENTRY, prio: PRIO_ENTRY }
                   : u === "panic" ? { slip: SLIP_PANIC, prio: PRIO_PANIC }
                   : { slip: SLIP_CALM, prio: PRIO_CALM };
const urgencyByReason = (r) => (r === "SL" || r === "NO_LAUNCH" || r === "DEAD_FEED") ? "panic" : "calm";
// [v11.7f] FRANJA HORARIA — SOLO MODO REAL. Tribunal 16-jul: la 20-21h ES pierde en ambas
// eras (-73/-64 mSOL/op), 9/10 días rojos, -70 mSOL/op con n=121. El demo SIGUE entrando
// a esa hora (el laboratorio no pierde los ojos y la señal se re-valida semanalmente).
const REAL_FRANJA_BLOCK = (process.env.REAL_FRANJA_BLOCK ?? "20").split(",").map(Number).filter(n => !isNaN(n));
const REAL_TZ_OFFSET = +(process.env.REAL_TZ_OFFSET || 2);  // España verano = UTC+2
const horaES = () => (new Date().getUTCHours() + REAL_TZ_OFFSET) % 24;
const franjaRealBloqueada = () => REAL_FRANJA_BLOCK.includes(horaES());
// [v11.7f] Veto de VELOCIDAD solo-real: señal fichada (vel>=4.7s pierde en ambas eras, 7/8 dias,
// ~+1.5 SOL/dia). APAGADO por defecto: activar con REAL_VEL_MAX=4.7 cuando el real arranque.
const REAL_VEL_MAX = +(process.env.REAL_VEL_MAX || 0);

// [v11.7f] STATE_FILE robusto: sonda de escritura al arrancar + fallback anunciado.
// Si el Volume de Railway no está montado/escribible, se ve EN EL LOG DEL BOT (antes moría en silencio).
const _stateCandidates = [
  process.env.STATE_FILE,
  "/data/solscanbot_state.json",
  "/var/data/solscanbot_state.json",
  "./solscanbot_state.json",
].filter(Boolean);
function _resolveStateFile() {
  for (const cand of _stateCandidates) {
    try {
      const dir = cand.includes("/") ? cand.slice(0, cand.lastIndexOf("/")) || "/" : ".";
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(cand + ".probe", "ok"); fs.unlinkSync(cand + ".probe");
      return { path: cand, persistent: cand.startsWith("/data") || !!process.env.STATE_FILE };
    } catch (e) { console.log(`💾 Ruta de estado NO escribible: ${cand} (${e.message})`); }
  }
  return { path: "./solscanbot_state.json", persistent: false };
}
const _stateInfo = _resolveStateFile();
const STATE_FILE = _stateInfo.path;

// ── CONFIG MIGRACIÓN ───────────────────────────────────────────
const MIG_TP = 21.00;                  // [CAMBIO 9-jul] TP simbólico +2000%: el trailing+runner son el techo natural; el TP solo queda como seguridad técnica
const MIG_SL = 0.61;   // [v11.7f] -39% (la del sofá)
// [CAMBIO 9-jul] Expiración 12min → 3 HORAS: el Excel de revisión demostró que los
// topes reales llegan a las 2h (ej. $33K→$416K a las 2h de entrar) y la expiración
// corta decapitaba a las corredoras. Se mantiene como red de seguridad anti-zombi.
const MIG_DURATION_MS = 60 * 60 * 1000; // 60 min: punto medio entre los 30 propuestos y las 2h de los topes del Excel
const MIG_WINDOW_MS = 60_000;
const MIG_MIN_VOL_FAST = 1_500;
const MIG_MIN_VOL_SLOW = 2_000;
const MIG_FAST_WINDOW_MS = 20_000;
// ── FILTRO DE VOLUMEN ──
// false = SIN filtro de volumen (entra en cuanto hay señal de precio, sin esperar $).
// PRUEBA en demo: ver qué hace sin volumen. AVISO: sin volumen entra en muchos
// tokens fantasma (suben por 1 compra mínima y se desploman). Alta basura.
const MIG_VOL_FILTER_ON = false;
const MIG_VOL_FAST_EFF = MIG_VOL_FILTER_ON ? MIG_MIN_VOL_FAST : 1;   // 1 dólar = prácticamente sin filtro
const MIG_VOL_SLOW_EFF = MIG_VOL_FILTER_ON ? MIG_MIN_VOL_SLOW : 1;
const MIG_MIN_MC = 0;
const MIG_MAX_MC = 2_000_000;
const MIG_MAX_MC_ENTRY = 1_000_000;
const MIG_MIN_MC_ENTRY = 2_500; // LAB: bajado de 5000 para ver todo el espectro
const MIG_EXCLUDE_BAND_ON = false; // LAB: off para grupo control
const MIG_EXCLUDE_BAND_LO = 30_000;
const MIG_EXCLUDE_BAND_HI = 40_000;

// ── [CAMBIO 9-jul] CORTE POR NO-DESPEGUE — REACTIVADO y por MÁXIMO ──
// Validado en backtest sobre 206 ops (8-9 jul, train/test cronológico): a los 30s,
// si el MÁXIMO alcanzado no ha tocado +10%, salir. Fue la palanca con mayor efecto
// marginal de toda la rejilla (+6.3 mSOL/op). La variante por máximo a 30s ganó
// tanto a la de 15s como a la R30 de "precio actual en rojo".
const MIG_LAUNCH_CHECK = false;  // [v11] OFF — validado: los 'cuchillazo y vuelo' se quedan dentro y el trailing ancho los cabalga (test +4.48 vs +0.21 con corte suave)
const MIG_LAUNCH_CHECK_MS = 30_000;  // a los 30 segundos de abrir
const MIG_LAUNCH_MIN_PCT = 10;       // exige que el MÁXIMO haya tocado +10%
const MIG_R30_ON = false;            // legacy, sustituida por el chequeo de máximo
const MIG_R30_THRESHOLD = 0;

// ── [CAMBIO 9-jul] FILTRO DE ENTRADA POR HOLDERS ──
// Validado (train +35.5 / test +20.3 mSOL/op): holders<20 en el PREMIG = veneno
// (cierre medio -48% sobre 10 ops de 8-9 jul). Fail-open: si Helius no ha
// respondido aún, se entra igualmente para no depender de su disponibilidad.
// [v11.7f] LISTA NEGRA DE QUOTES: PumpPortal puede emitir migraciones de pools
// cotizadas en USDC con el mint del QUOTE — el bot llegó a "operar" el propio USDC
// (fantasmas de -78/-81% en un activo de $1). Estos mints jamás se tocan.
const QUOTE_BLACKLIST = new Set([
  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", // USDT
  "So11111111111111111111111111111111111111112",  // wSOL
]);
// [v11.7f] CORDURA txs RETIRADA: el retro demostró que vetaba ~110 tokens/día legítimos
// (57% ganadores, incluido el +1274 del 13-jul con 2107 txs). Queda solo la lista negra de quotes.
const MIG_ABYSS_VETO = true;         // [v11.7f] ☠️ lista negra DE POR VIDA para creadores de pulls
const MIG_ABYSS_PNL  = -80;          // cierre demo <= -80% = retirada de liquidez -> su creador, vetado para siempre
const MIG_MIN_HOLDERS = 20;
const premigData = new Map();        // mint → { ageMin, total, holders, topPct, top5Pct, top10Pct, creator }
// [v10.1] MEMORIA DE CREADORES: wallet que acuñó cada token → resultados con nosotros.
// Fase 1 = solo medir (¿los rugs vienen de reincidentes?). El filtro llegará si los datos lo validan.
const creatorHist = new Map();       // wallet → { tokens, malas }
// [v11.7f] SEMILLA de la lista negra: 21 creadores cazados haciendo rug (cierre ≤-80%)
// hasta el 21-jul. Van en el código para que sobrevivan a cualquier reset del Volume.
const ABYSS_SEED = [
  "2fjnasKeqKS63myiKHqeVjaCKGEN75U4iPv6ZjsoJ8xa",
  "2i2nEBbt8E3h8CdokMy6n5DV7tQLCKowrWHX3wxZYrUp",
  "2xC3LT89o5FJLT7qSWBgqTFkobp7nC4Va1v5AoeQERQJ",
  "43wtwTcDSm5h7ecAaRvZfvkPRnzWCTYEBaH4knwZUUFm",
  "5Bg7YThAVCYs3VLibaGTu1fH6rK4bhFgHsHobZKMVRmS",
  "5dUXg97Ltrjid2KVTQAb6p6PVZXBneHcgV1Z8A9xzKgg",
  "5fjLhdEEhE22udkYc3sBZd3sByJ6eUcRyKzaUAj2VVjE",
  "5hJfeWQv6us5HcbacZSWwK5mBsztz1JKXn4Ti3EDfqZs",
  "5hZmf1xZ4KBJvAx9VFAtTZ5eCSYnv9B4KcksBW6cs7Ae",
  "67wkA3dkQYmGSAhrxWyhtV9XQ535MNVwcvFb9sg31W7h",
  "6JXvUAcTVT4DAenYsHRNNNp72oTEpV2vQ8AKsdo7dS82",
  "7eTU6LbY43xXyK9oCX7uMbTLmotRvud5TQjRqrBHrEL",
  "AMEd7bE5CYSEpVpQZK8r5rKMJZrhAdg6bnaJ7qum2ssV",
  "AYJSaJiDbxZjPjTKWdLjpFfoqMBvivp8uHEHqvabPFvb",
  "BTdjaPaor31yqs1mggv6eaRJbEox8sE3MbyBCMEjHPbq",
  "C7PNQWYuJJ9CvTRc5bUQGHaiRvs6jggfwGp95etL3r2w",
  "FA92cSxErMXVC8uZKkuNr8epmdjqrDZwVdzGM17QyRFV",
  "FTroG4aeMrXEVejLYgcwYHB88tbS8os3dDZCzh18MeRh",
  "FU3LdAH5iGwqHgt6KKdzr6qEUVPYNvP2JUEkMvb9vL5N",
  "Nprhp91TWBam85nZghGcvX7HwgDA7dzk9NuXhee4sPE",
  "ozDnyApycm95Zz9Y12PPoJ1Mp4Zx8pJiTLaFg2aB8QX"
];
const abyssCreators = new Set(ABYSS_SEED);     // [v11.7f] ☠️ creadores vetados de por vida (nos hicieron un ≤-80%)
// [v11.1] VETO DE FÁBRICA: no entrar en tokens de creadores con 2+ malas con nosotros.
// Quirúrgico: solo actúa sobre wallets que ya nos quemaron; un lanzador prolífico
// benigno (p.ej. 7 tokens / 0 malas) jamás se veta.
const MIG_CREATOR_VETO = true;
const MIG_CREATOR_VETO_MALAS = 2;
// [v11.1] SEMILLA a prueba de deploys: fábricas confirmadas por el censo de logs
// (30-jun → 13-jul). El bot añade nuevas solo; actualizar esta lista con cada versión.
const CREATOR_SEED = [
  ["8gM4gnxdLdkvifM9TCwkGAxrnNw4NiSiHbAdE1RqY96e", { tokens: 3, malas: 3 }],  // 3 rugs: -96/-99/-70 (noche 10-11 jul)
  ["niggerd597QYedtvjQDVHZTCCGyJrwHNm2i49dkm5zS",  { tokens: 5, malas: 3 }],  // fábrica activa (12-13 jul)
  ["GXRNpTLczwZZpAocDXRyKgLTrvxWG8fs1diKSQ99FWMy", { tokens: 2, malas: 2 }],
];
function seedCreators() {
  for (const [w, s] of CREATOR_SEED) {
    const h = creatorHist.get(w) || { tokens: 0, malas: 0 };
    h.tokens = Math.max(h.tokens, s.tokens);
    h.malas  = Math.max(h.malas,  s.malas);
    creatorHist.set(w, h);
  }
  addLog(`🏭 Semilla de creadores cargada: ${CREATOR_SEED.length} fábricas fichadas (veto a ${MIG_CREATOR_VETO_MALAS}+ malas: ${MIG_CREATOR_VETO ? "ON" : "off"})`, "info");
}

// ── [v10] FRENO DE RÉGIMEN (validado walk-forward 11 días: recupera 0.5-1.3 SOL
// en noches hostiles; 18/20 configs top de train también mejoran en test) ──
const MIG_BRAKE_ON = false; // [v11] OFF — calibrado para la distribución de cierres de la v9, a la v11 le cuesta -10 SOL (bloquea los cuchillazos-que-vuelan y mata las reentries); recalibraciones = agujas no fiables. El kill-switch real (riskState) sigue activo.
const MIG_BRAKE_N = 20;              // últimos 20 cierres demo de migración
const MIG_BRAKE_SUM = -150;          // si suman menos de -150% en total...
const MIG_BRAKE_PAUSE_MS = 30 * 60_000; // ...pausa de entradas 30 min (el LAB sigue grabando)
let brakeCloses = [];
let brakePausedUntil = 0;
function regimenPausado() { return MIG_BRAKE_ON && Date.now() < brakePausedUntil; }
function brakeRecordClose(pnlPct) {
  if (!MIG_BRAKE_ON) return;
  brakeCloses.push(pnlPct);
  if (brakeCloses.length > MIG_BRAKE_N) brakeCloses.shift();
  const suma = brakeCloses.reduce((a, b) => a + b, 0);
  if (brakeCloses.length === MIG_BRAKE_N && suma < MIG_BRAKE_SUM && Date.now() >= brakePausedUntil) {
    brakePausedUntil = Date.now() + MIG_BRAKE_PAUSE_MS;
    addLog(`🧊 MIG FRENO DE RÉGIMEN: últimos ${MIG_BRAKE_N} cierres suman ${suma.toFixed(0)}% — pausa de entradas ${MIG_BRAKE_PAUSE_MS/60000}min (mercado en modo rug; la grabación sigue)`, "warn");
  }
}

// ── [v10] TAMAÑO POR CALOR DEL MERCADO (validado: +2.5 SOL en 11 días a igual
// capital medio; señal monótona consistente train/test) ──
const MIG_HEAT_ON = false;  // [v11] OFF — 3/3 días invertido en la era v10.1 (lote 0.7 → media -3.3%); vuelve el lote fijo 0.5
const migFlowTimes = [];             // timestamps de migraciones detectadas
function calorMercado() {            // migraciones en los últimos 15 min
  const cutoff = Date.now() - 15 * 60_000;
  while (migFlowTimes.length && migFlowTimes[0] < cutoff) migFlowTimes.shift();
  return migFlowTimes.length;
}
function factorCalor() {
  if (!MIG_HEAT_ON) return 1.0;
  const c = calorMercado();
  return c <= 1 ? 0.6 : c <= 3 ? 1.0 : 1.4;   // frío 0.3 / normal 0.5 / caliente 0.7 SOL con lote base 0.5
}

// ── [v10] RE-ENTRADA EN RESUCITADOS (estrategia demo separada; validada
// walk-forward: +7.3 SOL aditivos en 11 días — negocio de cola: 1.4% de premios
// pagan los billetes; fricción real ~4.5% ya considerada en la validación) ──
const REENTRY_ON = true;
const REENTRY_MIN_T = 45;            // no antes del segundo 45 de la grabación
const REENTRY_DIP = -60;   // [v11.7f]
const REENTRY_JUMP = 45;   // [v11.7f]
const REENTRY_ZONE = -5;             // hasta al menos la zona de entrada (>= -5%)
const REENTRY_SL = -30;              // SL relativo a la re-entrada
const REENTRY_ARM = 50;   // [v11.7f]
const REENTRY_TRAIL = 0.55;   // [v11.7f]
const REENTRY_MAX_OPEN = 10;
const REENTRY_SIZE = 0.5;            // lote fijo (validado así)

// ── [v11.7f] RE-ENTRADA POR FUERZA ("la del sofá") ──
const FZ_ON = process.env.FZ_ON === "true";   // [v11.7f] APAGADA por defecto (encender con FZ_ON=true)
const FZ_MARGIN = 0.50;
const FZ_SL = -15;
const FZ_ARM = 40;
const FZ_TRAIL = 0.50;
const FZ_MAX_OPEN = 10;

// ── [v11.7f] RESCATE DE FEED: PumpPortal a veces calla en origen (5ª fuente: el volumen
// se muda de pool, o la suscripción cae en silencio). Si un mint con trades ABIERTOS lleva
// >45s sin ticks, se pide el precio a DexScreener (agrega TODOS los pools) y se alimenta
// SOLO la gestión de trades (nunca la grabación: el censo se queda puro PumpPortal). ──
const RESCUE_ON = process.env.RESCUE_ON !== "false";
const RESCUE_SILENCE_MS = 45_000;
const RESCUE_POLL_MS = 15_000;
const RESCUE_COOLDOWN_MS = 20_000;
const lastTickAt = new Map();
const lastRescueAt = new Map();

function mejorPrecioDex(j) {
  // [v11.7f] DexScreener da priceNative (precio en SOL) y priceUsd (en USD). El precio INTERNO
  // del bot está en USD/token (= priceEnSOL × solPriceUSD). Devolvemos en la MISMA escala USD,
  // preferimos priceUsd si viene, si no convertimos priceNative. El bug del -95% nacía de meter
  // priceNative (escala SOL, ~150× menor) como si fuera el precio interno.
  const pares = (j?.pairs || []).filter(p => p?.chainId === "solana" && (+p?.priceUsd > 0 || +p?.priceNative > 0));
  if (!pares.length) return null;
  pares.sort((a, b) => (b?.liquidity?.usd || 0) - (a?.liquidity?.usd || 0));
  const p = pares[0];
  if (+p.priceUsd > 0) return +p.priceUsd;
  return +p.priceNative * solPriceUSD;
}

// ── [CAMBIO 9-jul] MOON-BAG / RUNNER (solo DEMO por ahora) ──
// Cuando el trailing va a cerrar una posición cuyo máximo tocó +50% y va en verde:
// vende el 75% ahí y deja el 25% corriendo con trailing holgado del 30% y suelo
// en breakeven. Objetivo: dejar de ver los +500/+1000% desde el andén.
const MIG_RUNNER_ON = true;
const MIG_RUNNER_FRACTION = 0.25;    // fracción que se deja correr
const MIG_RUNNER_MIN_GAIN = 50;      // solo si el máximo tocó +50%
const MIG_RUNNER_TRAIL = 0.30;       // trailing holgado del runner (30% desde el precio)
const MIG_RUNNER_FLOOR = 0;          // el runner nunca cierra por debajo de breakeven

// ── MODO OBSERVADOR / GRABACIÓN EN VIVO ────────────────────────
const OBSERVER_MODE = false;
const LIVE_RECORD = true;
const LIVE_REC_DENSE_MS = 60_000;
const LIVE_REC_DENSE_INTERVAL = 1_000;   // [v11.7f] 1s el primer minuto: escalera s0-s10 exacta, gratis
const LIVE_REC_NORMAL_INTERVAL = 5_000;
const OBS_MIN_VOL = 2_000;
const OBS_MIN_MC = 20_000;
const OBS_RECORD_MS = 600_000;
const OBS_T1_MS = 60_000;
const OBS_T1_INTERVAL = 2_000;
const OBS_T2_MS = 300_000;
const OBS_T2_INTERVAL = 3_000;
const OBS_T3_INTERVAL = 5_000;

const MC_OBSERVER = false;  // OBSERVADOR OFF: el bot OPERA en demo. Ponlo en true para volver a grabar MCREC.
const MCO_RECORD_MS = 1_200_000;      // 20 min de grabación
const MCO_T1_MS = 60_000;             // primer MINUTO completo = alta resolución
const MCO_T1_INTERVAL = 1_000;        // muestreo cada 1s durante el primer minuto
const MCO_T2_INTERVAL = 5_000;        // después del minuto, cada 5s
const MCO_STRONG_REBOUND = 40;
const MCO_BIRTH_CANDLES = 12;         // grabar el % de las primeras 12 velas de 1s
const MCO_BIRTH_WINDOW_MS = 1_000;    // cada "vela" = 1 segundo
const MCO_VOL_SECONDS = 60;
const MCO_PUMP_CANDLE_PCT = 50;       // vela "vertical" si sube >+50% en 1s
const MCO_HEALTHY_CORRECTION = -3;    // corrección "sana" si una vela baja < -3%

const MIG_BREAKEVEN_AT = 0.20;        // breakeven al +20%: protege el suelo antes
const MIG_BREAKEVEN_MARGIN = 0.03;
const MIG_BE_ON = process.env.MIG_BE_ON === "true";   // [v11.7f] BE OFF por defecto (la del sofá)
const MIG_LOCK_AT = 0.25;             // trailing FOLLOWING se arma en +25%
const MIG_FOLLOW_PCT = 0.90;   // [v11.7f] x6.3 (cap 90%)  // [v11] x2.5 — config del usuario validada (train +36 / test +4.5, 11/13 días)
const MIG_MAX_PRICE_RATIO = 2.0;
const MIG_SL_CONFIRM_TICKS = 2;
const MIG_EXPIRED_WIN_PCT = 2;
const MIG_ENTRY_DELAY_MS = 3_000;
const MIG_QUAL_GATE = true;
const MIG_QUAL_MOV2S_MIN = 0.5; // LAB: casi todo entra (era 10)
const MIG_QUAL_MAX_WAIT_MS = 600_000;  // qual_gate CONTINUO: vigila hasta 10 min esperando la señal
const MIG_QUAL_PEND15_ON = false;
const MIG_QUAL_WINDOW_MS = 15_000;
const MIG_QUAL_DECIDE_MS = 2_500;
const MIG_MAX_CAIDA_DELAY = 0.35;      // aborta si cae más de -35% en la confirmación
const MIG_STEP_TRIGGER = 0.25;        // escalón (suelo +13%) se arma en +25%
const MIG_STEP_FLOOR = 0.13;
// [CAMBIO 9-jul] TIERS ANCHOS validados en backtest (192 configs, train/test):
// el trailing fino devolvía las corredoras (Excel: topes reales +682% cerrados a +10).
const MIG_FOLLOW_PCT_STEP = 0.90;   // [v11.7f] x6.3 (cap 90%)
const MIG_HARD_CAP_LOSS = -20;
const MIG_CAP_LOSS_ON = false;
// ── TRAILING POR ESTRUCTURA ──
const MIG_STRUCT_ON = false;
const MIG_STRUCT_ARM_PCT = 50;
const MIG_STRUCT_RETROCESO = 20;
// ── TRAILING POR TENDENCIA ──
// NOTA (auditoría 9-jul): aplicarEstructuraYEscalones NO se llama desde ningún sitio
// (código muerto). Se dejan los flags en false para que el banner no anuncie una
// estrategia que no corre. Si algún día se quiere activar, hay que invocarla desde
// updateDemoTrades/updateRealTrades.
const MIG_TREND_ON = false;           // [CAMBIO 9-jul] era true pero era código muerto
const MIG_TREND_ARM_PCT = 50;
const MIG_TREND_RETROCESO = 25;
// ── ESCALONES DE SL POR NIVEL ──
const MIG_ESCALONES_ON = false;       // [CAMBIO 9-jul] era true pero era código muerto
const MIG_ESCALONES = [
  [123, 10],
  [200, 50],
];
const MIG_VELO_DROP = 0.10;
const MIG_VELO_ON = false;
const MIG_VELO_MS = 2_000;
const MIG_TRAIL_T1 = 40;  const MIG_TRAIL_P1 = 0.15;
const MIG_TRAIL_T2 = 60;  const MIG_TRAIL_P2 = 0.90;    // [v11.7f] x6.3 (cap)  // [v11] era 0.15
const MIG_TRAIL_T3 = 100; const MIG_TRAIL_P3 = 0.756;   // [v11.7f] 0.12×6.3
const MIG_TRAIL_P4 = 0.504;   // [v11.7f] 0.08×6.3
const MIG_TOP_FLOOR_TRIGGER = 100;
const MIG_TOP_FLOOR = 0.65;

// ── KILL-SWITCH DE PORTAFOLIO ──
const RISK = {
  maxDailyLossSol: +(process.env.RISK_MAX_DAILY_LOSS || 3),   // [v11.7f] escalado al lote 0.25
  maxConsecutiveLosses: 12,
  maxWindowLossSol: 0.8,        // tope por VENTANA MÓVIL (independiente de medianoche UTC)
  windowHours: 6,              // ventana de las últimas 6 horas
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
  recentCloses: [],            // [{t, pnl}] para la ventana móvil
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

// FILTRO HORARIO (7-jul, validado sobre julio): evitar franjas malas consistentes.
const MIG_TIME_FILTER_ON = false; // LAB: off, se simulará después
const MIG_TZ_OFFSET = 2; // España respecto a UTC (verano). Ajustar a 1 en invierno.
function horaEspana() {
  const utcH = new Date().getUTCHours();
  const utcM = new Date().getUTCMinutes();
  return ((utcH + MIG_TZ_OFFSET + 24) % 24) + utcM / 60;
}
function franjaHorariaEvitada() {
  if (!MIG_TIME_FILTER_ON) return null;
  const h = horaEspana();
  // Madrugada 1:00 - 8:30
  if (h >= 1 && h < 8.5) return "madrugada (1:00-8:30 ES)";
  // Hora 20:00 - 21:00
  if (h >= 20 && h < 21) return "hora 20h ES";
  return null;
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
  // FRENO POR VENTANA MÓVIL (arregla el bug del reset a medianoche UTC): suma las últimas N horas
  const nowW = Date.now();
  const cutoffW = nowW - RISK.windowHours * 3600 * 1000;
  const windowPnl = riskState.recentCloses.filter(x => x.t >= cutoffW).reduce((s, x) => s + x.pnl, 0);
  if (windowPnl <= -RISK.maxWindowLossSol) {
    if (!riskState._windowLogged) {
      riskState._windowLogged = true;
      riskState.pausedUntil = nowW + 3 * 3600 * 1000; // pausa 3h tras tocar el tope de ventana
      addLog(`🛑 KILL-SWITCH VENTANA: pérdida ${windowPnl.toFixed(3)} SOL en ${RISK.windowHours}h ≥ tope ${RISK.maxWindowLossSol} — pausa 3h`, "error");
      broadcast({ event: "risk", data: riskSnapshot() });
    }
    return true;
  }
  if (windowPnl > -RISK.maxWindowLossSol) riskState._windowLogged = false;
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
  // ventana móvil: registrar y podar lo más viejo que la ventana
  const nowMs = Date.now();
  riskState.recentCloses.push({ t: nowMs, pnl: pnlSol });
  const cutoff = nowMs - RISK.windowHours * 3600 * 1000;
  riskState.recentCloses = riskState.recentCloses.filter(x => x.t >= cutoff);
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
  // ── REGISTRO DE PnL POR HORA ──
  horaActual: null,        // "2026-07-04 14" (año-mes-día hora)
  horaPnlSol: 0,           // PnL SOL acumulado de la hora en curso
  horaOps: 0,              // operaciones cerradas en la hora en curso
  horaWins: 0,             // ganadoras en la hora en curso
  diaPnlSol: 0,            // PnL SOL acumulado del día
  diaOps: 0,               // operaciones del día
  diaInicio: null,         // "2026-07-04" para detectar cambio de día
  historialHoras: [],      // [{hora, pnl, ops, wins}] para el resumen final
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
      shadow: state.shadow,
      fzJuicio: state.fzJuicio,
      creatorHist: [...creatorHist.entries()],
      abyssCreators: [...abyssCreators],
      riskState: {
        dayKey: riskState.dayKey,
        dailyPnlSol: riskState.dailyPnlSol,
        consecutiveLosses: riskState.consecutiveLosses,
        pausedUntil: riskState.pausedUntil,
      },
    }));
    fs.renameSync(tmp, STATE_FILE);
  } catch (e) {
    console.log("Error guardando estado:", e.message);
    if (!global._lastSaveErr || Date.now() - global._lastSaveErr > 600000) {
      global._lastSaveErr = Date.now();
      addLog(`🚨 ERROR GUARDANDO ESTADO en ${STATE_FILE}: ${e.message} — historial/lista negra en riesgo`, "error");
    }
  }
}

function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const saved = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    if (saved.demoTrades) state.demoTrades = saved.demoTrades;
    if (saved.realTrades) state.realTrades = saved.realTrades;
    if (saved.movements) state.movements = saved.movements;
    if (saved.stats) state.stats = { ...state.stats, ...saved.stats };
    if (saved.shadow && process.env.SHADOW_RESET !== "true") {
      state.shadow = saved.shadow;
      // [v11.7f] blindaje: nunca heredar un alta posterior al arranque (bug de redeploys encadenados)
      if (!state.shadow.alta || state.shadow.alta > Date.now()) state.shadow.alta = SHADOW_ALTA;
    }
    if (saved.fzJuicio) state.fzJuicio = saved.fzJuicio;
    if (saved.creatorHist) for (const [k, v] of saved.creatorHist) creatorHist.set(k, v);
    if (saved.abyssCreators) { for (const w of saved.abyssCreators) abyssCreators.add(w);
      if (abyssCreators.size) addLog(`☠️ Lista negra de por vida: ${abyssCreators.size} creador(es) (${ABYSS_SEED.length} de semilla + ${abyssCreators.size - ABYSS_SEED.length} cazados)`, "info"); }
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
        trade.pnlPct = trade.pnlPct ?? trade.currentPct ?? 0; trade.closeTime = Date.now();
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
        trade.pnlPct = trade.pnlPct ?? trade.currentPct ?? 0; trade.closeTime = Date.now(); n++;
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
  // GUARD anti-ceguera: no desuscribir si el mint sigue en uso por otra parte del sistema
  if (state.migMonitored.has(mint)) return;
  if (state.migWatching.has(mint)) return;
  if (state.liveRecordings.has(mint)) return;   // LAB: grabación extendida activa
  if (state.obsRecordings?.has?.(mint)) return;
  if (state.mcoRecordings?.has?.(mint)) return;   // [v11.7f]
  if (state.realTrades.some(t => t.mint === mint && (t.status === "OPEN" || t.status === "CLOSING"))) return;
  if (state.demoTrades.some(t => t.mint === mint && t.status === "OPEN")) return;   // [v11.7f] fuerza/reentry demo vivos
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

const MIG_PRICE_STALE_MS = 10_000;  // si el último precio válido tiene >10s, aceptar el nuevo aunque el salto sea grande (evita el congelamiento en rugs violentos)
function isPriceValid(newPrice, knownPrice, lastValidTs) {
  if (!knownPrice || knownPrice === 0) return newPrice > 0;
  if (lastValidTs && Date.now() - lastValidTs > MIG_PRICE_STALE_MS) return newPrice > 0;
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
    birthCandles: [],        // [{idx, pctAcum, pctVela}] una por segundo
    lastCandleMc: null,      // MC al cierre de la última vela (para calcular % de la vela)
    lastCandleT: 0,          // timestamp de la última vela cerrada
    mcNacimiento: null,      // MC del primerísimo tick (el "nacimiento")
    volPorSeg: [],           // [volAcumUSD] índice = segundo (0..VOL_SECONDS)
    volAcum: 0,              // volumen acumulado total (USD)
    lastVolSec: -1,          // último segundo registrado
  };
  if (rec.mcMig != null) rec.puntos.push({ t: 0, p: 0 });
  state.mcoRecordings.set(mint, rec);
  addLog(`🔬 MCREC GRABANDO: ${symbol} | MC mig ${rec.mcMig != null ? formatMC(rec.mcMig) : "(esperando 1er tick)"} — ${MCO_RECORD_MS/60000}min`, "accept");
  rec.timer = setTimeout(() => mcoFinish(mint), MCO_RECORD_MS);
}

function mcoSample(mint, price, volUSD = 0) {
  const rec = state.mcoRecordings.get(mint);
  if (!rec || rec.finished || price <= 0) return;
  const mcNow = price * 1_000_000_000;
  rec.volAcum = (rec.volAcum || 0) + volUSD;
  if (rec.t0) {
    const segNow = Math.floor((Date.now() - rec.t0) / 1000);
    if (segNow >= 0 && segNow <= MCO_VOL_SECONDS && segNow > rec.lastVolSec) {
      for (let s = rec.lastVolSec + 1; s <= segNow; s++) rec.volPorSeg[s] = +rec.volAcum.toFixed(0);
      rec.lastVolSec = segNow;
    }
  }
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

  const bc = rec.birthCandles;
  const idxCohete = bc.findIndex(c => c.pctVela >= MCO_PUMP_CANDLE_PCT);
  const huboCohete = idxCohete >= 0;
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
  const nacBajo = rec.mcNacimiento != null && rec.mcNacimiento < 5000 ? "BAJO" : "normal";
  const murio = lastP < -50 || (rec.maxP > 20 && lastP < rec.maxP - 70) ? "MURIO" : "vivo";

  const velasStr = bc.map(c => `v${c.idx}:${c.pctVela>=0?"+":""}${c.pctVela}%`).join(" ");
  const ptsRaw = pts.map(p => `${p.t}:${p.p}`).join(",");
  const volRaw = (rec.volPorSeg || []).map((v, s) => v != null ? `${s}:${v}` : null).filter(Boolean).join(",");
  addLog(
    `[MCREC] sym=${rec.symbol} nac=${rec.mcNacimiento != null ? formatMC(rec.mcNacimiento) : "?"}(${nacBajo}) ` +
    `velas[${velasStr}] firma=${firma} fin=${murio} ` +
    `suelo=${rec.minP}%@${rec.minT}s techo=${rec.maxP}%@${rec.maxT}s ` +
    `rebote_desde_suelo=${reboteDesdeSuelo}pts@${maxAfterMinT}s tiro_fuerte=${tiroFuerte} ` +
    `cierre=${lastP}% vol=${volRaw} pts=${ptsRaw}`,
    "rec"
  );
}

// ════════════════════════════════════════════════════════════════
// ESTRATEGIA: SNIPER DE MIGRACIÓN
// ════════════════════════════════════════════════════════════════


// ── REGISTRO DE EDAD PRE-MIGRACION via HELIUS (7-jul) ──
// [CAMBIO 9-jul] Ahora además: (a) guarda el resultado en premigData para que el
// filtro de entrada por holders pueda usarlo, y (b) registra top5Pct y top10Pct
// (la concentración del top-5/top-10 delata al deployer con el supply repartido
// en varias wallets; el topPct del top-1 solo demostró no separar nada).
async function registrarCalidadPremig(mint, symbol) {
  try {
    if (!connection) { addLog(`[PREMIG] sym=${symbol} mint=${mint} edad=sin-conexion`, "info"); return; }
    const pk = new PublicKey(mint);
    let before = undefined, oldest = null, total = 0, guard = 0;
    while (guard < 10) {
      guard++;
      const sigs = await connection.getSignaturesForAddress(pk, { limit: 1000, before }, "confirmed");
      if (!sigs || sigs.length === 0) break;
      total += sigs.length;
      oldest = sigs[sigs.length - 1];
      if (sigs.length < 1000) break;      // ya llegamos al final
      before = oldest.signature;
    }
    if (!oldest || !oldest.blockTime) {
      addLog(`[PREMIG] sym=${symbol} mint=${mint} edad=sin-blocktime txs=${total}`, "info");
      premigData.set(mint, { ageMin: null, total, holders: null, topPct: null, top5Pct: null, top10Pct: null });
      return;
    }
    const ageMin = Math.round((Date.now() / 1000 - oldest.blockTime) / 60);
    // [v10.1] la MARCA de acuñación: fee payer de la transacción más antigua = creador
    let creator = null;
    try {
      const tx0 = await connection.getParsedTransaction(oldest.signature, { maxSupportedTransactionVersion: 0, commitment: "confirmed" });
      const k0 = tx0?.transaction?.message?.accountKeys?.[0];
      creator = k0?.pubkey ? k0.pubkey.toString() : (k0?.toString?.() || null);
    } catch {}
    const hCre = creator ? creatorHist.get(creator) : null;
    const creStr = creator ? ` creator=${creator}${hCre ? ` creadorTokens=${hCre.tokens} creadorMalas=${hCre.malas}` : ""}` : "";
    let holdersStr = "";
    let holdersNum = null, topPctNum = null, top5Num = null, top10Num = null;
    try {
      const largest = await connection.getTokenLargestAccounts(new PublicKey(mint), "confirmed");
      const accounts = largest?.value || [];
      if (accounts.length > 0) {
        let totalSupply = 0;
        for (const a of accounts) totalSupply += Number(a.amount) || 0;
        const pctTop = (n) => totalSupply > 0
          ? (accounts.slice(0, n).reduce((s, a) => s + (Number(a.amount) || 0), 0) / totalSupply * 100)
          : -1;
        topPctNum = pctTop(1);
        top5Num = pctTop(5);
        top10Num = pctTop(10);
        holdersNum = accounts.filter(a => totalSupply > 0 && (Number(a.amount) / totalSupply) > 0.001).length;
        holdersStr = ` holders=${holdersNum} topPct=${topPctNum.toFixed(0)} top5Pct=${top5Num.toFixed(0)} top10Pct=${top10Num.toFixed(0)}`;
      }
    } catch (e) { holdersStr = " holders=err"; }
    // ── [v11.7f] PREMIG v2: calidad de las billeteras top-5 (identidad, no precio) ──
    let hqStr = "";
    try {
      const pack = (async () => {
        const largest = await connection.getTokenLargestAccounts(pk, "confirmed");
        const cuentas = (largest?.value || []).slice(1, 6);   // la 1ª suele ser el pool: fuera
        const owners = [];
        for (const c of cuentas) {
          const info = await connection.getParsedAccountInfo(c.address, "confirmed");
          const ow = info?.value?.data?.parsed?.info?.owner;
          if (ow) owners.push(ow);
        }
        const bals = [], births = []; let newW = 0;
        for (const ow of owners) {
          const opk = new PublicKey(ow);
          const [lam, sigs] = await Promise.all([
            connection.getBalance(opk, "confirmed"),
            connection.getSignaturesForAddress(opk, { limit: 25 }, "confirmed"),
          ]);
          bals.push(lam / 1e9);
          if ((sigs?.length || 0) <= 2) newW++;
          if (sigs && sigs.length > 0 && sigs.length < 25 && sigs[sigs.length - 1].blockTime)
            births.push(sigs[sigs.length - 1].blockTime);
        }
        if (!bals.length) return "";
        const sb = [...bals].sort((a, b) => a - b);
        const med = sb[Math.floor(sb.length / 2)].toFixed(2), mn = sb[0].toFixed(2);
        const spread = births.length >= 2 ? Math.round((Math.max(...births) - Math.min(...births)) / 60) + "m" : "n/a";
        return ` topBalMed=${med} topBalMin=${mn} newW=${newW}/${owners.length} fundSpread=${spread}`;
      })();
      hqStr = await Promise.race([pack, new Promise(r => setTimeout(() => r(""), 3500))]);
    } catch (e) { hqStr = ""; }
    premigData.set(mint, { ageMin, total, holders: holdersNum, topPct: topPctNum, top5Pct: top5Num, top10Pct: top10Num, creator, hq: hqStr.trim() });
    addLog(`[PREMIG] sym=${symbol} mint=${mint} edadMin=${ageMin} txsTotal=${total}${holdersStr}${creStr}${hqStr}`, "info");
    labStats.premigOk++;
  } catch (e) {
    addLog(`[PREMIG] sym=${symbol} mint=${mint} edad=error ${String(e).slice(0,50)}`, "info");
    labStats.premigErr++;
  }
}

function migStartWatching(coin) {
  if (seenMigMints.has(coin.mint)) return;
  if (!solPriceReady) { addLog("⏳ Esperando precio real de SOL antes de operar", "warn"); return; }
  if (QUOTE_BLACKLIST.has(coin.mint)) {
    addLog(`⚠️ MIG IGNORADA: el evento trae un QUOTE (${coin.symbol || coin.mint.slice(0,8)}…) como mint — pool no-SOL, no es un token pump`, "warn");
    return;
  }
  seenMigMints.add(coin.mint);
  state.stats.mig_migrations++;
  migFlowTimes.push(Date.now());   // [v10] termómetro del mercado
  registrarCalidadPremig(coin.mint, coin.symbol || "???"); // paralelo, no bloquea
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
  if (!isPriceValid(price, entry.lastPrice, entry.lastTickTs)) return;
  entry.lastTickTs = Date.now();
  // histórico corto de precios desde el 1er tick (para el mov2s del qual_gate continuo)
  if (!entry.priceHist) entry.priceHist = [];
  entry.priceHist.push([Date.now(), price]);
  while (entry.priceHist.length > 2 && Date.now() - entry.priceHist[0][0] > 5_000) entry.priceHist.shift();
  if (entry.pendingEntry) {
    entry.lastPrice = price;
    if (entry.qualGate) migQualTick(entry, price);  // qual_gate continuo: mira cada tick
    return;
  }
  entry.volumeUSD += solAmount * solPriceUSD;
  entry.tradeCount++;
  entry.lastPrice = price;
  if (!entry.firstPrice && price > 0) entry.firstPrice = price;
  const elapsed = Date.now() - entry.startTime;
  if (OBSERVER_MODE && entry.volumeUSD >= OBS_MIN_VOL && price > 0) {
    clearTimeout(entry.timer); entry.entered = true; state.migWatching.delete(mint);
    obsStartRecording(entry, price, elapsed); return;
  }
  if (elapsed < MIG_FAST_WINDOW_MS && entry.volumeUSD >= MIG_VOL_FAST_EFF) {
    clearTimeout(entry.timer); entry.pendingEntry = true;
    addLog(`⚡ MIG: ${entry.symbol} | $${Math.round(entry.volumeUSD)} en ${(elapsed/1000).toFixed(1)}s — armando qual_gate continuo`, "accept");
    broadcast({ event: "stats", data: state.stats });
    migQualityGateThenOpen(entry, price);
    return;
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
    const franjaEvit1 = franjaHorariaEvitada();
    if (franjaEvit1) {
      addLog(`🕐 MIG HORARIO: ${entry.symbol} descartada | franja evitada: ${franjaEvit1}`, "filter");
      state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
      broadcast({ event: "stats", data: state.stats }); return;
    }
    entry.entered = true; state.stats.mig_entered++;
    state.migWatching.delete(entry.mint); entry.firstPrice = entryPriceB;
    addLog(`✅ MIG ENTRADA: ${entry.symbol} @ MC ${formatMC(entryPriceB * 1_000_000_000)}`, "accept");
    migOpenTrades(entry); return;
  }
  entry.qualStartPrice = (entry.priceHist && entry.priceHist.length) ? entry.priceHist[0][1] : entryPriceB;
  entry.qualGate = true;
  addLog(`🔍 MIG CALIDAD: ${entry.symbol} — vigilando hasta ${(MIG_QUAL_MAX_WAIT_MS/60000).toFixed(0)}min, señal al 1er mov2s>+${MIG_QUAL_MOV2S_MIN}%`, "filter");
  entry.qualTimeout = setTimeout(() => {
    if (!entry.qualGate) return;
    entry.qualGate = false;
    addLog(`🚫 MIG FILTRO CALIDAD: ${entry.symbol} descartada | nunca dio mov2s>+${MIG_QUAL_MOV2S_MIN}% en ${(MIG_QUAL_MAX_WAIT_MS/60000).toFixed(0)}min`, "filter");
    state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
    broadcast({ event: "stats", data: state.stats });
  }, MIG_QUAL_MAX_WAIT_MS);
}

// Qual_gate CONTINUO: se llama en cada tick mientras entry.qualGate está activo.
function migQualTick(entry, price) {
  const now = Date.now();
  const hist = entry.priceHist || [];
  const prevPrice = hist.length >= 2 ? hist[hist.length - 2][1] : price;
  let p2s = null;
  for (let i = hist.length - 1; i >= 0; i--) {
    if (now - hist[i][0] >= 2_000) { p2s = hist[i][1]; break; }
  }
  if (p2s === null) p2s = hist.length ? hist[0][1] : null;
  if (p2s === null || p2s <= 0) return;
  const mov2 = (price / p2s - 1) * 100;
  if (mov2 > MIG_QUAL_MOV2S_MIN && price > prevPrice && price > entry.qualStartPrice) {
    entry.qualGate = false; clearTimeout(entry.qualTimeout);
    const precioSenal = price;
    const tSenal = ((now - entry.startTime) / 1000).toFixed(0);
    entry.sigMov2s = +mov2.toFixed(2); entry.sigT = +tSenal;   // [v11.7f] la señal causal, apuntada
    addLog(`🎯 MIG SEÑAL: ${entry.symbol} | mov2s +${mov2.toFixed(1)}% a los ${tSenal}s — confirmando ${(MIG_ENTRY_DELAY_MS/1000).toFixed(0)}s`, "accept");
    setTimeout(() => {
      const precioB = entry.lastPrice;
      if (precioB < precioSenal * (1 - MIG_MAX_CAIDA_DELAY)) {
        const caida = ((precioB / precioSenal - 1) * 100).toFixed(1);
        addLog(`🚫 MIG ENTRADA ABORTADA: ${entry.symbol} cayó ${caida}% en la confirmación`, "filter");
        state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      const mcEntryUsd = precioB * 1_000_000_000;
      if (mcEntryUsd < MIG_MIN_MC_ENTRY) {
        addLog(`🛑 MIG MC BASURA: ${entry.symbol} descartada | MC ${formatMC(mcEntryUsd)} < mínimo ${formatMC(MIG_MIN_MC_ENTRY)} (token glitch/muerto, inejecutable en real)`, "filter");
        state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      if (mcEntryUsd > MIG_MAX_MC_ENTRY) {
        addLog(`🛑 MIG MC ALTO: ${entry.symbol} descartada | MC ${formatMC(mcEntryUsd)} > tope ${formatMC(MIG_MAX_MC_ENTRY)}`, "filter");
        state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      if (MIG_EXCLUDE_BAND_ON && mcEntryUsd >= MIG_EXCLUDE_BAND_LO && mcEntryUsd < MIG_EXCLUDE_BAND_HI) {
        addLog(`🚫 MIG BANDA EXCLUIDA: ${entry.symbol} descartada | MC ${formatMC(mcEntryUsd)} en zona $30-40K (valle: WR 35%, -10.5 SOL histórico)`, "filter");
        state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      // [v10] FRENO DE RÉGIMEN: en pausa no se entra (la grabación sigue aparte)
      if (regimenPausado()) {
        addLog(`🧊 MIG FRENO: ${entry.symbol} descartada | régimen hostil (pausa ${Math.ceil((brakePausedUntil-Date.now())/60000)}min restantes)`, "filter");
        state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      // [v11.1] VETO DE FÁBRICA: creador con 2+ malas con nosotros → ni tocarlo
      if (MIG_CREATOR_VETO) {
        const preV = premigData.get(entry.mint);
        const hC = preV && preV.creator ? creatorHist.get(preV.creator) : null;
        if (hC && hC.malas >= MIG_CREATOR_VETO_MALAS) {
          addLog(`🏭 MIG VETO FÁBRICA: ${entry.symbol} descartada | creador ${preV.creator.slice(0,8)}… con ${hC.tokens} tokens / ${hC.malas} malas con nosotros`, "filter");
          state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
          broadcast({ event: "stats", data: state.stats }); return;
        }
      }
      // [v11.7f] ☠️ VETO ABISMO: si el creador está en la lista negra de por vida, ni tocarlo
      if (MIG_ABYSS_VETO) {
        const preA = premigData.get(entry.mint);
        if (preA && preA.creator && abyssCreators.has(preA.creator)) {
          addLog(`☠️ MIG VETO ABISMO: ${entry.symbol} descartada | creador ${preA.creator.slice(0,8)}… en lista negra de por vida (pull previo ≤${MIG_ABYSS_PNL}%)`, "filter");
          state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
          broadcast({ event: "stats", data: state.stats }); return;
        }
      }
      // [CAMBIO 9-jul] FILTRO HOLDERS (validado train +35.5 / test +20.3 mSOL/op).
      // Fail-open: si el PREMIG aún no respondió, se entra igualmente.
      const preD = premigData.get(entry.mint);
      if (preD && preD.holders !== null && preD.holders < MIG_MIN_HOLDERS) {
        addLog(`🚫 MIG HOLDERS: ${entry.symbol} descartada | holders=${preD.holders} < ${MIG_MIN_HOLDERS} (supply ultra-concentrado, perfil rug)`, "filter");
        state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      const franjaEvit2 = franjaHorariaEvitada();
      if (franjaEvit2) {
        addLog(`🕐 MIG HORARIO: ${entry.symbol} descartada | franja evitada: ${franjaEvit2}`, "filter");
        state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      entry.entered = true; state.stats.mig_entered++;
      state.migWatching.delete(entry.mint); entry.firstPrice = precioB;
      addLog(`✅ MIG ENTRADA (calidad ✓): ${entry.symbol} @ MC ${formatMC(mcEntryUsd)}`, "accept");
      migOpenTrades(entry);
    }, MIG_ENTRY_DELAY_MS);
  }
}

function migEvaluate(mint) {
  const entry = state.migWatching.get(mint);
  if (!entry || entry.entered || entry.pendingEntry) return;
  const elapsed = ((Date.now() - entry.startTime) / 1000).toFixed(1);
  if (entry.volumeUSD >= MIG_VOL_SLOW_EFF && entry.lastPrice) {
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
    t0: Date.now(), entryPrice, puntos: [{t:0,p:0}], lastSample: Date.now(), mov2s: null, sigMov2s: entry.sigMov2s ?? null, sigT: entry.sigT ?? null, finished: false,
    volSeg: [], lastVolSec: -1, minP: 0, reentered: false,
    wallets: new Map() };  // [v10.1] wallet → {buyUsd, sellUsd, buys, sells} en 0-60s
  state.liveRecordings.set(entry.mint, rec);
}

function liveRecSample(mint, price, volUSD = 0, trader = null, isBuy = false) {
  if (!LIVE_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished || price <= 0) return;
  rec.volPost = (rec.volPost || 0) + volUSD;  // LAB: volumen negociado DESPUÉS de migrar
  // [v10] volumen acumulado por segundo (0-60s) para la hipótesis del pump orquestado
  const segNow = Math.floor((Date.now() - rec.t0) / 1000);
  if (segNow >= 0 && segNow <= 60 && segNow > rec.lastVolSec) {
    for (let s = rec.lastVolSec + 1; s <= segNow; s++) rec.volSeg[s] = Math.round(rec.volPost);
    rec.lastVolSec = segNow;
  }
  // [v10.1] actores del primer minuto: quién compra y quién vende (cada tick)
  if (trader && segNow <= 60 && rec.wallets) {
    let w = rec.wallets.get(trader);
    if (!w) { w = { buyUsd: 0, sellUsd: 0, buys: 0, sells: 0 }; rec.wallets.set(trader, w); }
    if (isBuy) { w.buyUsd += volUSD; w.buys++; } else { w.sellUsd += volUSD; w.sells++; }
  }
  const dt = Date.now() - rec.t0;
  const interval = dt <= LIVE_REC_DENSE_MS ? LIVE_REC_DENSE_INTERVAL : LIVE_REC_NORMAL_INTERVAL;
  if (Date.now() - rec.lastSample < interval) return;
  rec.lastSample = Date.now();
  const pct = +((price - rec.entryPrice) / rec.entryPrice * 100).toFixed(2);
  rec.puntos.push({ t: Math.round(dt/1000), p: pct });
  // [v11.7f] FUERZA: el precio supera el máximo previo en el margen -> perseguir
  if (FZ_ON && rec.fzArmed && !rec.fzFired && pct >= rec.fzTrigPct) {
    rec.fzFired = true;
    fzOpenTrades(rec, price);
  }
  if (rec.mov2s === null && dt >= 2000) rec.mov2s = pct;
  if (pct < rec.minP) rec.minP = pct;
  maybeReentry(rec, price, pct, Math.round(dt/1000));  // [v10]
}

// ── [v10] RE-ENTRADA EN RESUCITADOS (demo puro) ──
function maybeReentry(rec, price, pct, tSec) {
  if (!REENTRY_ON || rec.reentered || MC_OBSERVER || OBSERVER_MODE) return;
  if (tSec < REENTRY_MIN_T) return;
  if (!(rec.minP <= REENTRY_DIP && pct >= rec.minP + REENTRY_JUMP && pct >= REENTRY_ZONE)) return;
  rec.reentered = true;  // una sola re-entrada por token
  const abiertos = state.demoTrades.filter(t => t.strategy === "reentry" && t.status === "OPEN").length;
  if (abiertos >= REENTRY_MAX_OPEN) { addLog(`🔄 REENTRY saltada (límite ${REENTRY_MAX_OPEN} abiertas): ${rec.symbol}`, "filter"); return; }
  const trade = {
    id: `reentry-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    strategy: "reentry", mint: rec.mint, symbol: rec.symbol, name: rec.symbol,
    entryPrice: price, tp: +(price * 21).toFixed(12),
    sl: +(price * (1 + REENTRY_SL/100)).toFixed(12),
    sizeSol: REENTRY_SIZE,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: "REENTRY", status: "OPEN",
    expiresAt: Date.now() + MIG_DURATION_MS, mov1s: null, mov2s: null,
  };
  state.demoTrades.unshift(trade);
  if (state.demoTrades.length > 500) state.demoTrades.pop();
  state.stats.demoOpen++;
  broadcast({ event: "newDemoTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  addLog(`🔄 REENTRY [demo]: ${rec.symbol} | resucitado (mín ${rec.minP.toFixed(0)}% → ahora ${pct.toFixed(0)}%) | SL ${REENTRY_SL}% trail ${REENTRY_TRAIL*100}% desde pico`, "accept");
  // [v11.7f] real = demo: la reentry también se opera en real (mismo disparo, mismo token)
  openRealTrade({ strategy: "reentry", mint: rec.mint, symbol: rec.symbol, name: rec.symbol,
    price, tp: +(price * 21).toFixed(12), sl: +(price * (1 + REENTRY_SL/100)).toFixed(12) });
}

function updateReentryTrades(mint, price) {
  if (!REENTRY_ON) return;
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== "reentry") continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    if (trade.maxGainPct >= REENTRY_ARM) {
      const peakPrice = trade.entryPrice * (1 + trade.maxGainPct/100);
      const cand = peakPrice * (1 - REENTRY_TRAIL);
      if (cand > trade.sl) trade.sl = +cand.toFixed(12);
    }
    if (price >= trade.tp) { closeDemoTrade(trade, price, "TP", 21); }
    else if (price <= trade.sl) {
      // [v10.1] confirmación de 2 ticks: el backtest se validó con muestras de 2-5s,
      // así que las mechas de un solo tick no deben ejecutar (igual que el SL de migración)
      trade._slBelowCount = (trade._slBelowCount || 0) + 1;
      if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS) { closeDemoTrade(trade, price, "SL", 21); }
    }
    else {
      trade._slBelowCount = 0;
      if (now >= trade.expiresAt) { closeDemoTrade(trade, price, "EXPIRED", 21); }
      else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
    }
  }
  // [v11.7f] mismas reglas para las reentries REALES
  for (const trade of state.realTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== "reentry") continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    if (trade.maxGainPct >= REENTRY_ARM) {
      const peakPrice = trade.entryPrice * (1 + trade.maxGainPct/100);
      const cand = peakPrice * (1 - REENTRY_TRAIL);
      if (cand > trade.sl) trade.sl = +cand.toFixed(12);
    }
    if (price >= trade.tp) { closeRealTrade(trade, price, "TP"); }
    else if (price <= trade.sl) {
      trade._slBelowCount = (trade._slBelowCount || 0) + 1;
      if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS) { closeRealTrade(trade, price, "SL"); }
    } else {
      trade._slBelowCount = 0;
      if (now >= trade.expiresAt) { closeRealTrade(trade, price, "EXPIRED"); }
      else broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
    }
  }
}

// ── [v11.7f] FUERZA: apertura (demo + real) y gestión de salidas ──
function fzOpenTrades(rec, price) {
  const abiertos = state.demoTrades.filter(t => t.strategy === "fuerza" && t.status === "OPEN").length;
  if (abiertos >= FZ_MAX_OPEN) { addLog(`⚡ FUERZA saltada (límite ${FZ_MAX_OPEN} abiertas): ${rec.symbol}`, "filter"); return; }
  const trade = {
    id: `fuerza-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    strategy: "fuerza", mint: rec.mint, symbol: rec.symbol, name: rec.symbol,
    entryPrice: price, tp: +(price * 21).toFixed(12),
    sl: +(price * (1 + FZ_SL/100)).toFixed(12),
    sizeSol: REENTRY_SIZE,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: "FUERZA", status: "OPEN",
    expiresAt: Date.now() + MIG_DURATION_MS, mov1s: null, mov2s: null,
  };
  state.demoTrades.unshift(trade);
  if (state.demoTrades.length > 500) state.demoTrades.pop();
  broadcast({ event: "demoTradeOpened", data: trade });
  addLog(`⚡ FUERZA [demo]: ${rec.symbol} | rompió su máximo previo (disparo a +${rec.fzTrigPct}%) | SL ${FZ_SL}% trail ${FZ_TRAIL*100}%@+${FZ_ARM}`, "accept");
  openRealTrade({ strategy: "fuerza", mint: rec.mint, symbol: rec.symbol, name: rec.symbol,
    price, tp: +(price * 21).toFixed(12), sl: +(price * (1 + FZ_SL/100)).toFixed(12) });
}

function updateFuerzaTrades(mint, price) {
  if (!FZ_ON) return;
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== "fuerza") continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    if (trade.maxGainPct >= FZ_ARM) {
      const peakPrice = trade.entryPrice * (1 + trade.maxGainPct/100);
      const cand = peakPrice * (1 - FZ_TRAIL);
      if (cand > trade.sl) trade.sl = +cand.toFixed(12);
    }
    if (price >= trade.tp) { closeDemoTrade(trade, price, "TP", 21); }
    else if (price <= trade.sl) {
      trade._slBelowCount = (trade._slBelowCount || 0) + 1;
      if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS) { closeDemoTrade(trade, price, "SL", 21); }
    } else {
      trade._slBelowCount = 0;
      if (now >= trade.expiresAt) { closeDemoTrade(trade, price, "EXPIRED", 21); }
      else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
    }
  }
  for (const trade of state.realTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== "fuerza") continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    if (trade.maxGainPct >= FZ_ARM) {
      const peakPrice = trade.entryPrice * (1 + trade.maxGainPct/100);
      const cand = peakPrice * (1 - FZ_TRAIL);
      if (cand > trade.sl) trade.sl = +cand.toFixed(12);
    }
    if (price >= trade.tp) { closeRealTrade(trade, price, "TP"); }
    else if (price <= trade.sl) {
      trade._slBelowCount = (trade._slBelowCount || 0) + 1;
      if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS) { closeRealTrade(trade, price, "SL"); }
    } else {
      trade._slBelowCount = 0;
      if (now >= trade.expiresAt) { closeRealTrade(trade, price, "EXPIRED"); }
      else broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
    }
  }
}

// [CAMBIO 9-jul] Grabación extendida 10 → 30 MINUTOS: necesitamos curvas de la
// región 10-30 min para backtestear el moon-bag y la ventana larga con datos
// reales (los topes del Excel demostraron que ahí vive el recorrido grande).
const LAB_EXTEND_MS = 30 * 60_000;
// LAB: contadores de salud del experimento (volcados cada hora)
const labStats = { premigOk: 0, premigErr: 0, migrecs: 0, inicio: Date.now() };
setInterval(() => {
  const h = ((Date.now() - labStats.inicio) / 3600000).toFixed(1);
  addLog(`[LAB-SALUD] ${h}h de lab | migraciones=${state.stats.mig_migrations} entradas=${state.stats.mig_entered} MIGRECs=${labStats.migrecs} PREMIG ok=${labStats.premigOk} err=${labStats.premigErr}${labStats.premigErr > labStats.premigOk ? " ⚠️ HELIUS FALLANDO" : ""}`, "info");
}, 3600_000);

// ═══════════════ [v11.7f] TORNEO DE SOMBRAS ═══════════════
// Cada grabación terminada se re-juega contra K configs con el motor de replay (clavado a los
// simuladores). Observador puro: no toca decisiones. Fuera-de-muestra de serie: cada config
// solo se juzga sobre ops nacidas tras su alta.
const SHADOW_ON = process.env.SHADOW_ON !== "false";
// [v11.7f] alta del torneo anclada al ARRANQUE del proceso (no a la 1ª grabación, que llega ~50min
// tarde). Reseteable con SHADOW_RESET=true si se quiere empezar el torneo de cero tras un cambio.
const SHADOW_ALTA = Date.now() - 90*60*1000;   // margen: cuenta lo grabado en la última hora y media
const SHADOW_FEE = 4.5, SHADOW_POS = 0.5;
const SH_SOFA = { sl:-39, mult:6.3, be:false, runTrig:50, runTr:0.30,
  re:{dip:-60,jump:45,zone:-5,sl:-30,arm:50,tr:0.55}, fz:{on:true,margin:0.50,sl:-15,arm:40,tr:0.50} };
const SH_STD  = { sl:-40, mult:5.0, be:true, runTrig:50, runTr:0.30,
  re:{dip:-45,jump:60,zone:-5,sl:-30,arm:40,tr:0.30}, fz:{on:false} };
const SHADOW_GRID = [
  { id:"sofa",        ref:true, cfg: SH_SOFA },
  { id:"x5",          cfg: SH_STD },
  { id:"x2.5",        cfg: { ...SH_STD, mult:2.5 } },
  { id:"sofa+BEon",   cfg: { ...SH_SOFA, be:true } },
  { id:"sofa-sinFZ",  cfg: { ...SH_SOFA, fz:{on:false} } },
  { id:"sofa+bala20", cfg: { ...SH_SOFA, dca:-20 } },
  // [v11.7f] vecinos de UN tornillo (exploración disciplinada, nunca combinatoria libre)
  { id:"sofa·SL-35",  cfg: { ...SH_SOFA, sl:-35 } },
  { id:"sofa·SL-42",  cfg: { ...SH_SOFA, sl:-42 } },
  { id:"sofa·x5.5",   cfg: { ...SH_SOFA, mult:5.5 } },
  { id:"sofa·x7",     cfg: { ...SH_SOFA, mult:7.0 } },
  { id:"sofa·reArm45",cfg: { ...SH_SOFA, re:{ ...SH_SOFA.re, arm:45 } } },
  { id:"sofa·reArm55",cfg: { ...SH_SOFA, re:{ ...SH_SOFA.re, arm:55 } } },
  // [v11.7f] FILTROS-SOMBRA: los tribunales pendientes como instrumentos permanentes
  { id:"sofa·vel<4.7",cfg: { ...SH_SOFA, fVelMax:4.7 } },
  { id:"sofa·tps<26", cfg: { ...SH_SOFA, fTpsMax:26 } },
  { id:"sofa·sig≥1",  cfg: { ...SH_SOFA, fSigMin:1.0 } },
  { id:"sofa·sin20h", cfg: { ...SH_SOFA, fSin20:true } },
];
function shFiltrada(rec, cfg){
  if (cfg.fVelMax && rec.vel != null && rec.vel >= cfg.fVelMax) return true;
  if (cfg.fSin20 && ((new Date(rec.t0).getUTCHours()+REAL_TZ_OFFSET)%24) === 20) return true;
  if (cfg.fSigMin != null && rec.sigMov2s != null && rec.sigMov2s < cfg.fSigMin) return true;
  if (cfg.fTpsMax){
    const pd = premigData.get(rec.mint);
    if (pd && pd.ageMin > 0 && (pd.total/(pd.ageMin*60)) >= cfg.fTpsMax) return true;
  }
  return false;
}
function shadowEvaluaPropuesta(S){
  const ref = S.libretas["sofa"];
  if (!ref || S.n < 300) return null;
  const dias = Object.keys(ref.porDia||{});
  if (dias.length < 7) return null;
  const refST = ref.neto - (ref.top||[]).reduce((a,b)=>a+b,0);
  let mejor = null;
  for (const [id, L] of Object.entries(S.libretas)){
    if (id === "sofa" || !L.n) continue;
    const delta = L.neto - ref.neto;
    if (delta < Math.max(1, 0.1*Math.abs(ref.neto))) continue;
    let w = 0;
    for (const d of dias) if ((L.porDia?.[d]||0) > (ref.porDia?.[d]||0) + 0.005) w++;
    if (w <= dias.length/2) continue;
    const st = (L.neto - (L.top||[]).reduce((a,b)=>a+b,0)) - refST;
    if (st <= 0) continue;
    if (!mejor || delta > mejor.delta) mejor = { id, delta:+delta.toFixed(2), dias:`${w}/${dias.length}`, sinTop3:+st.toFixed(2) };
  }
  return mejor;
}
function shMig(pts, cfg, delay=0){
  let i0=0;
  if (delay>0){ i0=pts.findIndex(p=>p.t>=delay); if(i0<0) return null; }
  const e=pts[i0].p; const reb=v=>((1+v/100)/(1+e/100)-1)*100;
  let sl=cfg.sl, maxg=0, be=false, run=false, partial=null, exitT=null, pnl=null;
  for(let j=i0+1;j<pts.length;j++){
    const v=reb(pts[j].p); if(v>maxg)maxg=v;
    if(v>=2000){pnl=2000;exitT=pts[j].t;break;}
    if(run){ sl=Math.max(sl,(1+v/100)*(1-cfg.runTr)*100-100,0); if(v<=sl){pnl=.75*partial+.25*v;exitT=pts[j].t;break;} continue; }
    if(cfg.be&&!be&&maxg>=20){be=true;sl=Math.max(sl,-3);}
    if(maxg>=25){const b=maxg>=100?.08:maxg>=60?.12:maxg>=40?.15:.20;const fe=Math.min(.9,b*cfg.mult);
      sl=Math.max(sl,(1+v/100)*(1-fe)*100-100,13); if(maxg>=100)sl=Math.max(sl,65);}
    if(v<=sl){ if(maxg>=cfg.runTrig&&v>0&&!run){run=true;partial=v;sl=Math.max(0,(1+v/100)*(1-cfg.runTr)*100-100);continue;} pnl=v;exitT=pts[j].t;break; }
  }
  if(pnl===null){ pnl = run? .75*partial+.25*reb(pts[pts.length-1].p) : reb(pts[pts.length-1].p); exitT=pts[pts.length-1].t; }
  if(cfg.dca!=null){
    const vE=pts[i0].p; const rebE=v=>((1+v/100)/(1+vE/100)-1)*100;
    let vf=null;
    for(let k=i0+1;k<pts.length;k++){ if(pts[k].t>=exitT)break; const vr=rebE(pts[k].p); if(vr<=cfg.dca){vf=vr;break;} }
    pnl = vf===null ? (pnl+SHADOW_FEE)/2 : (pnl+((1+pnl/100)/(1+vf/100)-1)*100)/2;
  }
  return {pnl, exitT};
}
function shRe(pts, r){ if(!r) return null; let minP=0,sig=-1;
  for(let i=0;i<pts.length;i++){const q=pts[i]; if(q.p<minP)minP=q.p;
    if(q.t>45&&minP<=r.dip&&q.p>=minP+r.jump&&q.p>=r.zone){sig=i;break;}}
  if(sig<0)return null; const v0=pts[sig].p; const reb=v=>((1+v/100)/(1+v0/100)-1)*100;
  let sl=r.sl,maxr=0;
  for(let i=sig+1;i<pts.length;i++){const rr=reb(pts[i].p); if(rr>maxr)maxr=rr;
    if(maxr>=2000)return 2000;
    if(maxr>=r.arm)sl=Math.max(sl,(1+maxr/100)*(1-r.tr)*100-100);
    if(rr<=sl)return rr;}
  return reb(pts[pts.length-1].p);
}
function shFz(pts, f, exitT){ if(!f||!f.on||exitT==null)return null;
  let maxAt=-1e9; for(const q of pts){ if(q.t<=exitT&&q.p>maxAt)maxAt=q.p; }
  const trig=((1+maxAt/100)*(1+f.margin)-1)*100; let sig=-1;
  for(let i=0;i<pts.length;i++){ if(pts[i].t>exitT&&pts[i].p>=trig){sig=i;break;} }
  if(sig<0)return null; const v0=pts[sig].p; const reb=v=>((1+v/100)/(1+v0/100)-1)*100;
  let sl=f.sl,maxr=0;
  for(let i=sig+1;i<pts.length;i++){const rr=reb(pts[i].p); if(rr>maxr)maxr=rr;
    if(maxr>=f.arm)sl=Math.max(sl,(1+maxr/100)*(1-f.tr)*100-100);
    if(rr<=sl)return rr;}
  return reb(pts[pts.length-1].p);
}
function shadowProcesa(rec){
  if(!SHADOW_ON) return;
  const pts = rec.puntos; if(!pts || pts.length < 2) return;   // [v11.7f] antes 5: descartaba baja liquidez
  if(!state.shadow) state.shadow = { alta: SHADOW_ALTA, libretas:{}, horas:{}, dias:{}, delays:{}, n:0 };
  const S = state.shadow;
  if(rec.t0 < S.alta) return;
  S.n++;
  const fecha = new Date(rec.t0 + REAL_TZ_OFFSET*3600e3).toISOString().slice(0,10);
  let refNeto = 0;
  for(const c of SHADOW_GRID){
    if (shFiltrada(rec, c.cfg)) {
      const Lf = S.libretas[c.id] || (S.libretas[c.id]={n:0,neto:0,w:0,porDia:{}});
      Lf.skip = (Lf.skip||0) + 1;
      continue;
    }
    const m = shMig(pts, c.cfg, 0);
    let neto = SHADOW_POS*(m.pnl-SHADOW_FEE)/100;
    const re = shRe(pts, c.cfg.re); if(re!==null) neto += SHADOW_POS*(re-SHADOW_FEE)/100;
    const fz = shFz(pts, c.cfg.fz, m.exitT); if(fz!==null) neto += SHADOW_POS*(fz-SHADOW_FEE)/100;
    const L = S.libretas[c.id] || (S.libretas[c.id]={n:0,neto:0,w:0,porDia:{}});
    L.n++; L.neto+=neto; if(neto>0)L.w++;
    L.porDia[fecha]=(L.porDia[fecha]||0)+neto;
    L.top=(L.top||[]); L.top.push(neto); L.top.sort((a,b)=>b-a); if(L.top.length>3)L.top.length=3;
    if(c.ref) refNeto=neto;
  }
  const hr = (new Date(rec.t0).getUTCHours()+REAL_TZ_OFFSET)%24;
  const dw = new Date(rec.t0 + REAL_TZ_OFFSET*3600e3).getUTCDay();
  const H = S.horas[hr] || (S.horas[hr]={n:0,neto:0,w:0}); H.n++; H.neto+=refNeto; if(refNeto>0)H.w++;
  const D = S.dias[dw] || (S.dias[dw]={n:0,neto:0,w:0}); D.n++; D.neto+=refNeto; if(refNeto>0)D.w++;
  const ref = SHADOW_GRID.find(c=>c.ref).cfg;
  for(let s=0;s<=10;s++){
    const m = shMig(pts, ref, s); if(!m) continue;
    const E = S.delays[s] || (S.delays[s]={n:0,neto:0}); E.n++; E.neto += SHADOW_POS*(m.pnl-SHADOW_FEE)/100;
  }
  broadcast({ event: "shadow", data: S });   // [v11.7f] el panel pinta el torneo en vivo
  if(S.n % 25 === 0){
    const tabla=Object.entries(S.libretas).map(([id,L])=>({id,neto:L.neto,n:L.n,wr:L.n?Math.round(100*L.w/L.n):0})).sort((a,b)=>b.neto-a.neto);
    addLog(`[SHADOW] n=${S.n} | `+tabla.slice(0,8).map(t=>`${t.id}:${t.neto>=0?"+":""}${t.neto.toFixed(2)}(${t.wr}%)`).join(" · ")+(tabla.length>8?" · …":""), "info");
    const prop = shadowEvaluaPropuesta(S);
    if (prop && S.propuesta?.id !== prop.id)
      addLog(`[PROPUESTA] 🏆 ${prop.id} supera a la sofá: +${prop.delta} SOL · días ${prop.dias} · sin-top3 +${prop.sinTop3} — LISTA PARA PROMOCIÓN (decisión humana)`, "warn");
    if (!prop && S.propuesta)
      addLog(`[PROPUESTA] retirada: ${S.propuesta.id} ya no pasa las puertas`, "warn");
    S.propuesta = prop;
    const hs=Object.entries(S.horas).filter(([,v])=>v.n>=3).sort((a,b)=>a[1].neto-b[1].neto);
    if(hs.length>=2) addLog(`[SHADOW-HORAS] peor ${hs[0][0]}h ${hs[0][1].neto.toFixed(2)} (n=${hs[0][1].n}) | mejor ${hs[hs.length-1][0]}h +${hs[hs.length-1][1].neto.toFixed(2)} (n=${hs[hs.length-1][1].n})`, "info");
    const dnom=["dom","lun","mar","mié","jue","vie","sáb"];
    const ds=Object.entries(S.dias).map(([d,v])=>`${dnom[d]}:${v.neto>=0?"+":""}${v.neto.toFixed(1)}(${v.n})`).join(" ");
    if(ds) addLog(`[SHADOW-DIAS] ${ds}`, "info");
    const es=Object.entries(S.delays).map(([s,E])=>`s${s}:${(E.neto/Math.max(1,E.n)*1000).toFixed(0)}`).join(" ");
    addLog(`[SHADOW-SEG] mSOL/op entrando en cada segundo → ${es}`, "info");
  }
}

function liveRecEmit(mint) {
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished) return;
  rec.finished = true; state.liveRecordings.delete(mint); unsubscribeToken(mint);
  const pts = rec.puntos;
  if (pts.length < 2) return;
  let min=pts[0], max=pts[0];
  for (const pt of pts) { if (pt.p<min.p) min=pt; if (pt.p>max.p) max=pt; }
  const orden = min.t<=max.t ? "lava-antes" : "lava-despues";
  const cruces = [10,15,20].map(u=>{let c=0;for(let i=1;i<pts.length;i++) if(pts[i-1].p<u&&pts[i].p>=u)c++;return c;});
  const mov2s = rec.mov2s===null?"n/a":`${rec.mov2s>=0?"+":""}${rec.mov2s}%`;
  const cd = rec.cierreDemo != null ? +(+rec.cierreDemo).toFixed(1) : null;
  const ptsRaw = pts.map(p=>`${p.t}:${p.p}`).join(",");
  const vol60 = (rec.volSeg && rec.volSeg.length) ? rec.volSeg.map((v,s)=>v!=null?`${s}:${v}`:null).filter(Boolean).join(",") : "";
  // [v10.1] firma de actores del primer minuto
  let wStr = "";
  if (rec.wallets && rec.wallets.size) {
    const ws = [...rec.wallets.values()];
    const buyers = ws.filter(w => w.buys > 0);
    const totalBuy = buyers.reduce((s, w) => s + w.buyUsd, 0);
    const topBuy = buyers.length ? Math.max(...buyers.map(w => w.buyUsd)) : 0;
    const wash = ws.filter(w => w.buys > 0 && w.sells > 0).length;
    wStr = ` buyers60=${buyers.length} topBuyer=${totalBuy > 0 ? (100*topBuy/totalBuy).toFixed(0) : 0}% wash60=${wash}`;
  }
  addLog(`[MIGREC] sym=${rec.symbol} mint=${rec.mint} vel=${rec.vel}s MC=${formatMC(rec.mc)} vol=${rec.vol} mov2s=${mov2s} sig=${rec.sigMov2s!=null?(rec.sigMov2s>=0?"+":"")+rec.sigMov2s+"%@"+rec.sigT+"s":"n/a"} MIN=${min.p}%@${min.t}s MAX=${max.p}%@${max.t}s orden=${orden} cruces[10,15,20]=${cruces[0]},${cruces[1]},${cruces[2]} cierre_real=${cd!=null?(cd>=0?"+":"")+cd:"n/a"}% dur_rec=${pts[pts.length-1].t}s volPost=${Math.round(rec.volPost||0)}${wStr}${vol60?` vol60=${vol60}`:""} pts=${ptsRaw}`, "rec");
  try { shadowProcesa(rec); } catch (e) { if (!state._shErr) { state._shErr = true; addLog(`⚠️ shadowProcesa error: ${e.message}`, "warn"); } }   // [v11.7f]
  labStats.migrecs++;
}

function liveRecFinish(mint, cierreRealPct) {
  if (!LIVE_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished) return;
  if (rec.cierreDemo == null) rec.cierreDemo = cierreRealPct;
  const restante = (rec.t0 + LAB_EXTEND_MS) - Date.now();
  if (restante <= 0) { liveRecEmit(mint); return; }
  if (!rec._emitTimer) rec._emitTimer = setTimeout(() => liveRecEmit(mint), restante);
}

// Registra el PnL de una operación cerrada en el acumulador por hora.
function registrarPnlHorario(pnlSol, esWin) {
  const ahora = new Date();
  const y = ahora.getFullYear(), mo = String(ahora.getMonth()+1).padStart(2,"0");
  const d = String(ahora.getDate()).padStart(2,"0"), h = String(ahora.getHours()).padStart(2,"0");
  const claveHora = `${y}-${mo}-${d} ${h}`;
  const claveDia = `${y}-${mo}-${d}`;

  if (state.horaActual !== null && state.horaActual !== claveHora) {
    const wr = state.horaOps > 0 ? Math.round(state.horaWins / state.horaOps * 100) : 0;
    addLog(`📊 RESUMEN HORA ${state.horaActual}h → ${state.horaOps} ops · ${state.horaWins}W/${state.horaOps-state.horaWins}L (WR ${wr}%) · PnL ${state.horaPnlSol>=0?"+":""}${state.horaPnlSol.toFixed(3)} SOL · acumulado día ${state.diaPnlSol>=0?"+":""}${state.diaPnlSol.toFixed(2)} SOL`, "accept");
    state.historialHoras.push({ hora: state.horaActual, pnl: +state.horaPnlSol.toFixed(3), ops: state.horaOps, wins: state.horaWins });
    state.horaPnlSol = 0; state.horaOps = 0; state.horaWins = 0;
  }

  if (state.diaInicio !== null && state.diaInicio !== claveDia) {
    const totalOps = state.diaOps, totalPnl = state.diaPnlSol;
    addLog(`🌙 RESUMEN DÍA ${state.diaInicio} → ${totalOps} ops · PnL TOTAL ${totalPnl>=0?"+":""}${totalPnl.toFixed(2)} SOL. Nuevo día empieza.`, "accept");
    state.diaPnlSol = 0; state.diaOps = 0; state.historialHoras = [];
  }

  state.horaActual = claveHora; state.diaInicio = claveDia;
  state.horaPnlSol += pnlSol; state.horaOps++; if (esWin) state.horaWins++;
  state.diaPnlSol += pnlSol; state.diaOps++;
}

function migOpenTrades(entry) {
  const price = entry.firstPrice;
  if (!price || price <= 0) return;
  const mcOpen = price * 1_000_000_000;
  if (MIG_EXCLUDE_BAND_ON && mcOpen >= MIG_EXCLUDE_BAND_LO && mcOpen < MIG_EXCLUDE_BAND_HI) {
    addLog(`🚫 MIG BANDA EXCLUIDA (cinturón apertura): ${entry.symbol} bloqueada | MC ${formatMC(mcOpen)} en zona $30-40K`, "filter");
    state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
    broadcast({ event: "stats", data: state.stats });
    return;
  }
  if (mcOpen > MIG_MAX_MC_ENTRY || mcOpen < MIG_MIN_MC_ENTRY) {
    addLog(`🛑 MIG MC FUERA DE RANGO (cinturón en apertura): ${entry.symbol} bloqueada | MC ${formatMC(mcOpen)} (rango válido ${formatMC(MIG_MIN_MC_ENTRY)}–${formatMC(MIG_MAX_MC_ENTRY)})`, "filter");
    state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
    broadcast({ event: "stats", data: state.stats });
    return;
  }
  liveRecStart(entry, price);
  const signal = {
    id: `mig-${entry.mint}-${Date.now()}`, strategy: "migration",
    mint: entry.mint, name: entry.name, symbol: entry.symbol,
    price, tp: +(price*MIG_TP).toFixed(12), sl: +(price*MIG_SL).toFixed(12),
    mcUsd: price*1_000_000_000, volumeUSD: entry.volumeUSD,
    vel: +(((Date.now()-entry.startTime)/1000).toFixed(1)),   // [v11.7f] para el veto de lentos
    time: Date.now(),
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

function migUpdatePrice(mint, price, solAmount, trader = null, isBuy = false) {
  const entry = state.migWatching.get(mint);
  if (entry) { migUpdateWatching(mint, price, solAmount, entry); return; }
  const token = state.migMonitored.get(mint);
  if (!token) {
    const rec = state.liveRecordings.get(mint);
    if (rec && !rec.finished && price > 0) {
      if (price < rec.entryPrice * 100 && price > rec.entryPrice / 100) {
        liveRecSample(mint, price, solAmount * solPriceUSD, trader, isBuy);
        // [v10] FIX ZOMBIE: si queda un demo OPEN de este mint (p.ej. el real cerró
        // antes y migCleanup borró el token del monitoreo), seguir gestionándolo.
        updateDemoTrades(mint, price, "migration");
        updateRealTrades(mint, price, "migration");   // [v11.7f] el real también respira aquí
        updateReentryTrades(mint, price);
        updateFuerzaTrades(mint, price);
      }
    } else if (price > 0) {
      // [v11.7f] POST-GRABACIÓN: la grabación terminó (o nunca existió) pero quedan
      // trades vivos de este mint (fuerza/reentry/migración larga). Sin esta rama,
      // los ticks llegaban y se tiraban -> congelación aunque la suscripción viva.
      const abierto = state.demoTrades.find(t => t.mint === mint && t.status === "OPEN")
                   || state.realTrades.find(t => t.mint === mint && t.status === "OPEN");
      if (abierto && price < abierto.entryPrice * 1000 && price > abierto.entryPrice / 1000) {
        updateDemoTrades(mint, price, "migration");
        updateRealTrades(mint, price, "migration");
        updateReentryTrades(mint, price);
        updateFuerzaTrades(mint, price);
      }
    }
    return;
  }
  if (!isPriceValid(price, token.price, token.lastUpdate)) {
    // [v11.7f] el salto es sospechoso para las MÉTRICAS del token (no las tocamos, ni grabamos),
    // pero los TRADES vivos no pueden quedarse ciegos 10s en pleno movimiento violento:
    // gestionar con sanidad propia (±1000x sobre su entrada) y salir.
    const ab = state.demoTrades.find(t => t.mint === mint && t.status === "OPEN")
            || state.realTrades.find(t => t.mint === mint && t.status === "OPEN");
    if (ab && price < ab.entryPrice * 1000 && price > ab.entryPrice / 1000) {
      updateDemoTrades(mint, price, "migration");
      updateRealTrades(mint, price, "migration");
      updateReentryTrades(mint, price);
      updateFuerzaTrades(mint, price);
    }
    return;
  }
  token.price = price; token.mc = price*1_000_000_000;
  token.priceHigh = Math.max(token.priceHigh, price);
  token.priceLow = Math.min(token.priceLow, price);
  token.tradeCount++; token.volumeUSD += solAmount*solPriceUSD;
  token.lastUpdate = Date.now();
  liveRecSample(mint, price, solAmount * solPriceUSD, trader, isBuy);
  updateDemoTrades(mint, price, "migration");
  updateRealTrades(mint, price, "migration");
  updateReentryTrades(mint, price);  // [v10]
  updateFuerzaTrades(mint, price);  // [v11.7f]
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

// [v11.7f] Swap vía Jupiter v6: quote → swap tx serializada → firmar y enviar con nuestro RPC.
// Sin comisión de router. prioritizationFeeLamports va dentro de la propia tx.
async function jupSwap(inputMint, outputMint, amountRaw, slipPct, prioSol) {
  const q = await fetch(`${JUP_BASE}/quote?inputMint=${inputMint}&outputMint=${outputMint}&amount=${amountRaw}&slippageBps=${Math.round(slipPct*100)}&onlyDirectRoutes=false`, { signal: AbortSignal.timeout(8000) });
  if (!q.ok) throw new Error(`quote ${q.status}`);
  const quote = await q.json();
  if (!quote || !quote.routePlan || !quote.routePlan.length) throw new Error("sin ruta");
  const s = await fetch(`${JUP_BASE}/swap`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ quoteResponse: quote, userPublicKey: wallet.publicKey.toString(),
      wrapAndUnwrapSol: true, dynamicComputeUnitLimit: true,
      prioritizationFeeLamports: Math.round(prioSol * 1e9) }),
    signal: AbortSignal.timeout(8000),
  });
  if (!s.ok) throw new Error(`swap ${s.status}`);
  const { swapTransaction } = await s.json();
  const tx = VersionedTransaction.deserialize(Buffer.from(swapTransaction, "base64"));
  tx.sign([wallet]);
  const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: "confirmed" });
  await connection.confirmTransaction(sig, "confirmed");
  return sig;
}

async function buyToken(mint, solAmount, urgency = "entry") {
  if (!wallet || !connection) return null;
  const P = execParams(urgency);
  if (EXEC_MODE === "jup") {
    try {
      const sig = await jupSwap(WSOL_MINT, mint, Math.round(solAmount * LAMPORTS_PER_SOL), P.slip, P.prio);
      const delta = await getSolDeltaFromTx(sig);
      const costSol = delta != null ? +(-delta).toFixed(6) : solAmount;
      addLog(`✅ COMPRA [jup]: ${shortAddr(mint)} | coste real ${costSol} SOL | ${sig}`, "real");
      return { sig, costSol };
    } catch (e) { addLog(`⚠️ Compra Jupiter falló (${e.message}) → PumpPortal`, "warn"); }
  }
  try {
    const response = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "buy", mint, denominatedInSol: "true", amount: solAmount, slippage: execParams(urgency).slip, priorityFee: execParams(urgency).prio, pool: "auto" }),
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

// [CAMBIO 9-jul] sellToken acepta fracción (para el moon-bag en real, cuando se
// active). fraction=1 = venta total (comportamiento idéntico al anterior).
async function sellToken(mint, fraction = 1, urgency = "calm") {
  if (!wallet || !connection) return null;
  try {
    const bal = await getTokenBalance(mint);
    if (bal <= 0) { addLog(`⚠️ Sin tokens: ${shortAddr(mint)}`, "warn"); return null; }
    const amount = fraction >= 1 ? bal : Math.floor(bal * fraction);
    if (amount <= 0) { addLog(`⚠️ Fracción demasiado pequeña: ${shortAddr(mint)}`, "warn"); return null; }
    const P = execParams(urgency);
    if (EXEC_MODE === "hybrid" || EXEC_MODE === "jup") {
      try {
        const sig = await jupSwap(mint, WSOL_MINT, amount, P.slip, P.prio);
        const delta = await getSolDeltaFromTx(sig);
        const proceedsSol = delta != null ? +delta.toFixed(6) : 0;
        addLog(`✅ VENTA [jup]${fraction < 1 ? ` (${Math.round(fraction*100)}%)` : ""} [${urgency}]: ${shortAddr(mint)} | recibido ${proceedsSol} SOL | ${sig}`, "real");
        return { sig, proceedsSol };
      } catch (e) { addLog(`⚠️ Venta Jupiter falló (${e.message}) → fallback PumpPortal`, "warn"); }
    }
    const response = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "sell", mint, denominatedInSol: "false", amount, slippage: execParams(urgency).slip, priorityFee: execParams(urgency).prio, pool: "auto" }),
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) { addLog(`❌ Venta error: ${response.status}`, "error"); return null; }
    const tx = VersionedTransaction.deserialize(new Uint8Array(await response.arrayBuffer()));
    tx.sign([wallet]);
    const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: "confirmed" });
    await connection.confirmTransaction(sig, "confirmed");
    const delta = await getSolDeltaFromTx(sig);
    const proceedsSol = delta != null ? +delta.toFixed(6) : 0;
    addLog(`✅ VENTA${fraction < 1 ? ` (${Math.round(fraction*100)}%)` : ""}: ${shortAddr(mint)} | recibido real ${proceedsSol} SOL | ${sig}`, "real");
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
  if (franjaRealBloqueada()) {
    addLog(`🕗 REAL saltada por franja horaria (${horaES()}h ES, bloqueadas: ${REAL_FRANJA_BLOCK.join(",")}h): ${signal.symbol} — el demo sí entra`, "filter");
    return;
  }
  if (REAL_VEL_MAX > 0 && signal.vel != null && signal.vel >= REAL_VEL_MAX) {
    addLog(`🐢 REAL saltada por señal lenta (vel ${signal.vel}s ≥ ${REAL_VEL_MAX}s): ${signal.symbol} — el demo sí entra`, "filter");
    return;
  }
  if (regimenPausado()) { addLog(`🧊 Entrada real BLOQUEADA por freno de régimen: ${signal.symbol}`, "warn"); return; }
  const openReal = state.realTrades.filter(t => t.status === "OPEN");
  if (openReal.length >= MAX_REAL_TRADES) return;
  const stratOpen = openReal.filter(t => t.strategy === signal.strategy).length;
  if (stratOpen >= MAX_MIG_REAL) { addLog(`⚠️ Límite real [migración]: ${stratOpen}/${MAX_MIG_REAL}`, "warn"); return; }
  const mcEntryReal = signal.price * 1_000_000_000;
  if (mcEntryReal > MIG_MAX_MC_REAL) {
    addLog(`🛑 REAL saltada (MC ${formatMC(mcEntryReal)} > tope real ${formatMC(MIG_MAX_MC_REAL)}) — solo demo`, "warn");
    return;
  }
  const solAmount = +(SOL_PER_TRADE_REAL * factorCalor()).toFixed(3);
  const balance = await getWalletBalance();
  if (balance < solAmount + 0.01) { addLog(`⚠️ Balance insuficiente: ${balance.toFixed(3)} SOL (necesito ${(solAmount+0.01).toFixed(2)})`, "warn"); return; }
  const buy = await buyToken(signal.mint, solAmount, "entry");
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

// [CAMBIO 9-jul] Corte por NO-DESPEGUE, versión por MÁXIMO (validada en backtest):
// a los 30s de abrir, si el MÁXIMO alcanzado no ha tocado +10%, salir ya.
// Fue la palanca con mayor efecto marginal de la rejilla (+6.3 mSOL/op).
function scheduleLaunchCheck(trade, kind) {
  if (!MIG_LAUNCH_CHECK) return;
  if (trade.strategy !== "migration") return;
  setTimeout(() => {
    if (trade.status !== "OPEN") return;            // ya cerrado por TP/SL/etc
    if (trade.maxGainPct >= MIG_LAUNCH_MIN_PCT) return;  // tocó +10% → se queda
    const token = state.migMonitored.get(trade.mint);
    const price = token?.price || trade.entryPrice;
    addLog(`✂️ NO-DESPEGUE 30s [${kind} ${trade.symbol}]: máx +${trade.maxGainPct.toFixed(1)}% → salida (no arrancó, WR histórico ~0%)`, kind === "real" ? "realloss" : "loss");
    if (kind === "real") closeRealTrade(trade, price, "NO_LAUNCH");
    else closeDemoTrade(trade, price, "NO_LAUNCH", MIG_TP);
  }, MIG_LAUNCH_CHECK_MS);
}

// [v11.7f] vende el 75% y deja el 25% corriendo (moon-bag real, espejo del demo)
async function realRunnerConvert(trade, price) {
  trade.runner = true; trade.trailingPhase = "RUNNER";
  trade.sl = +Math.max(trade.entryPrice, price * (1 - MIG_RUNNER_TRAIL)).toFixed(12);
  addLog(`🌙 MOON-BAG [real]: ${trade.symbol} vende 75% a +${trade.currentPct.toFixed(1)}% | 25% sigue (trail ${MIG_RUNNER_TRAIL*100}%, suelo BE)`, "real");
  const sell = await sellToken(trade.mint, 0.75, "calm");
  if (sell && sell.proceedsSol != null) { trade.partialProceedsSol = sell.proceedsSol; saveState(); }
  else {
    addLog(`⚠️ MOON-BAG real: venta parcial falló → cierro entera por seguridad: ${trade.symbol}`, "warn");
    trade.runner = false; closeRealTrade(trade, price, "SL");
  }
}

async function closeRealTrade(trade, price, reason) {
  if (trade.status !== "OPEN") return;
  trade.status = "CLOSING";
  const sell = await sellToken(trade.mint, 1, urgencyByReason(reason));
  if (!sell) {
    trade.sellRetries = (trade.sellRetries || 0) + 1;
    if (trade.sellRetries <= 3) { trade.status = "OPEN"; setTimeout(() => closeRealTrade(trade, price, reason), 15000); return; }
    trade.status = "SELL_FAILED"; broadcast({ event: "realTradeClosed", data: trade }); return;
  }
  const proceedsSol = +(((sell.proceedsSol || 0) + (trade.partialProceedsSol || 0))).toFixed(6); // [v11.7f] incluye moon-bag
  const costSol = (trade.costSol != null && trade.costSol > 0) ? trade.costSol : trade.solAmount;
  const realPnlSol = +(proceedsSol - costSol).toFixed(4);
  const tickPnlSol = +(costSol * (price - trade.entryPrice) / trade.entryPrice).toFixed(4);
  const slipFeeSol = +(realPnlSol - tickPnlSol).toFixed(4);
  trade.sellSignature = sell.sig; trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
  const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
  trade.pnlPct = +pnlPct.toFixed(2); trade.pnlSol = realPnlSol; trade.slipFeeSol = slipFeeSol;
  addLog(`📊 PnL real: ${realPnlSol>=0?"+":""}${realPnlSol} SOL (coste ${costSol} → recibido ${proceedsSol}) | tick: ${tickPnlSol>=0?"+":""}${tickPnlSol} | slip+fee: ${slipFeeSol>=0?"+":""}${slipFeeSol}`, "real");
  const slipPctLote = costSol > 0 ? +(slipFeeSol / costSol * 100).toFixed(2) : 0;
  addLog(`[REALREC] sym=${trade.symbol} reason=${reason} dur=${Math.round((trade.closeTime - trade.openTime)/1000)}s tickPct=${pnlPct.toFixed(1)}% cost=${costSol} recv=${proceedsSol} realSol=${realPnlSol} slipFee=${slipFeeSol} slipPct=${slipPctLote}%`, "real");
  riskRecordClose(realPnlSol);
  const dur = Math.round((trade.closeTime - trade.openTime) / 1000);
  const expWinPct = MIG_EXPIRED_WIN_PCT;
  if (reason === "TP" || reason === "STEP" || reason === "RUNNER_END" || (reason === "SL" && trade.pnlPct >= 0)) {
    trade.result = "WIN"; state.stats.mig_realWins++;
    state.stats.mig_realPnL += trade.pnlPct; state.stats.mig_realPnLSol += trade.pnlSol;
    addLog(`✅ REAL WIN [${reason==="STEP"?"🪜 ESCALÓN":reason==="RUNNER_END"?"🌙 RUNNER":trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "realwin");
  } else if (reason === "SL") {
    trade.result = "LOSS"; state.stats.mig_realLosses++;
    state.stats.mig_realPnL += trade.pnlPct; state.stats.mig_realPnLSol += trade.pnlSol;
    addLog(`❌ REAL LOSS [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "realloss");
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
  const breakeven = MIG_BE_ON ? MIG_BREAKEVEN_AT : Infinity;   // [v11.7f]
  const breakevenMargin = MIG_BREAKEVEN_MARGIN;
  const lock = MIG_LOCK_AT;
  const follow = MIG_FOLLOW_PCT;
  for (const trade of state.realTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    // [v11.7f] modo MOON-BAG real: el 25% restante corre con trailing propio y suelo breakeven
    if (trade.runner) {
      const cand = Math.max(price * (1 - MIG_RUNNER_TRAIL), trade.entryPrice);
      if (cand > trade.sl) trade.sl = +cand.toFixed(12);
      if (price <= trade.sl) { closeRealTrade(trade, price, "RUNNER_END"); }
      else if (now >= trade.expiresAt) { closeRealTrade(trade, price, "EXPIRED"); }
      else broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
      continue;
    }
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
      // [v11.7f] conversión a MOON-BAG (idéntico al demo): trailing salta con máx≥runner y en verde
      if (MIG_RUNNER_ON && !trade.runner && trade.maxGainPct >= MIG_RUNNER_MIN_GAIN && currentPct > 0 && strategy === "migration") {
        void realRunnerConvert(trade, price);
        continue;
      }
      if (trade.sl >= trade.entryPrice) {
        closeRealTrade(trade, price, (stepArmed && Math.abs(trade.sl - stepFloorPrice) < 1e-9) ? "STEP" : "SL");
      } else {
        trade._slBelowCount = (trade._slBelowCount || 0) + 1;
        if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS) { closeRealTrade(trade, price, "SL"); }
        else { addLog(`⏳ SL sin confirmar [real] (${trade._slBelowCount}/${MIG_SL_CONFIRM_TICKS}): ${trade.symbol}`, "trail"); broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } }); }
      }
    } else {
      trade._slBelowCount = 0;
      if (now >= trade.expiresAt) closeRealTrade(trade, price, "EXPIRED");
      else broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
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
      closeRealTrade(trade, token?.price ?? trade.entryPrice, "DEAD_FEED"); unsubscribeToken(trade.mint); continue;
    }
    if (now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint);
    const lastPx = trade.entryPrice * (1 + (trade.currentPct || 0) / 100);   // [v11.7f]
    if (!token?.price) addLog(`⚠️ cierre sin feed [${trade.strategy} real]: ${trade.symbol} — último precio conocido (${trade.currentPct>0?"+":""}${trade.currentPct||0}%)`, "warn");
    closeRealTrade(trade, token?.price ?? lastPx, "EXPIRED");
    unsubscribeToken(trade.mint);
  }
}, 10_000);

function openDemoTrade(signal) {
  const fCalor = factorCalor();
  const trade = {
    id: `demo-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    strategy: signal.strategy, mint: signal.mint, symbol: signal.symbol, name: signal.name,
    entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
    sizeSol: +(SOL_PER_TRADE_MIG * fCalor).toFixed(2),
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
  // [CAMBIO 9-jul] el log de apertura ahora refleja la config REAL (antes decía
  // "+300%/-20%" hardcodeado y no correspondía a las constantes: llevó a calibrar
  // una propuesta entera contra un baseline que no existía).
  addLog(`📝 DEMO [${signal.strategy}]: ${signal.symbol} | TP +${((MIG_TP-1)*100).toFixed(0)}% SL -${((1-MIG_SL)*100).toFixed(0)}% | runner ${MIG_RUNNER_ON ? Math.round(MIG_RUNNER_FRACTION*100)+"%" : "off"} | 🔥 calor=${calorMercado()} lote=${trade.sizeSol} SOL`, "demo");
  scheduleLaunchCheck(trade, "demo");
}

// (Auditoría 9-jul) aplicarEstructuraYEscalones era CÓDIGO MUERTO: nunca se llamaba
// desde updateDemoTrades/updateRealTrades aunque los flags estaban en true y el
// banner lo anunciaba. Se elimina la función y los flags quedan en false; si algún
// día quieres probar estructura/tendencia, recupérala del server anterior y llámala
// explícitamente desde la gestión.

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
    // ── [CAMBIO 9-jul] MODO RUNNER: tras la venta parcial del 75%, el 25% restante
    // solo se gestiona con su trailing holgado (30%) y el suelo de breakeven.
    // La gestión normal queda desactivada para el runner (sus tiers finos
    // machacarían el trailing ancho, porque el SL solo puede subir).
    if (trade.runner) {
      const candR = price * (1 - MIG_RUNNER_TRAIL);
      if (candR > trade.sl) trade.sl = +candR.toFixed(12);
      const floorR = trade.entryPrice * (1 + MIG_RUNNER_FLOOR / 100);
      if (floorR > trade.sl) trade.sl = +floorR.toFixed(12);
      if (price >= trade.tp) { closeDemoTrade(trade, price, "TP", tp_pct); continue; }
      if (price <= trade.sl) { closeDemoTrade(trade, price, "RUNNER_END", tp_pct); continue; }
      if (now >= trade.expiresAt) { closeDemoTrade(trade, price, "EXPIRED", tp_pct); continue; }
      broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
      continue;
    }
    // SINCRONIZADO CON EL REAL: misma lógica breakeven→lock→follow→floors.
    const breakeven = MIG_BE_ON ? MIG_BREAKEVEN_AT : Infinity;   // [v11.7f]
    const breakevenMargin = MIG_BREAKEVEN_MARGIN;
    const lock = MIG_LOCK_AT;
    const follow = MIG_FOLLOW_PCT;
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
    if (price >= trade.tp) { trade._slBelowCount = 0; closeDemoTrade(trade, price, "TP", tp_pct); }
    else if (price <= trade.sl) {
      // ── [CAMBIO 9-jul] MOON-BAG: si el trailing va a cerrar una posición cuyo
      // máximo tocó +50% y va en verde, vende el 75% aquí y deja el 25% corriendo.
      const canRunner = MIG_RUNNER_ON && !trade.runner && trade.maxGainPct >= MIG_RUNNER_MIN_GAIN && currentPct > 0;
      if (canRunner) {
        trade.runner = true;
        trade.partialPct = +currentPct.toFixed(2);   // ganancia asegurada del 75%
        trade._slBelowCount = 0;
        trade.trailingPhase = "RUNNER";
        const candR = Math.max(trade.entryPrice * (1 + MIG_RUNNER_FLOOR / 100), price * (1 - MIG_RUNNER_TRAIL));
        trade.sl = +candR.toFixed(12);
        addLog(`🌙 MOON-BAG [${trade.strategy}]: ${trade.symbol} vende ${Math.round((1-MIG_RUNNER_FRACTION)*100)}% @ +${trade.partialPct}% | runner ${Math.round(MIG_RUNNER_FRACTION*100)}% con trail ${MIG_RUNNER_TRAIL*100}% y suelo breakeven`, "trail");
        broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
      } else if (trade.sl >= trade.entryPrice) {
        closeDemoTrade(trade, price, (stepArmed && Math.abs(trade.sl - stepFloorPrice) < 1e-9) ? "STEP" : "SL", tp_pct);
      } else {
        trade._slBelowCount = (trade._slBelowCount || 0) + 1;
        if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS) { closeDemoTrade(trade, price, "SL", tp_pct); }
        else { addLog(`⏳ SL sin confirmar (${trade._slBelowCount}/${MIG_SL_CONFIRM_TICKS}): ${trade.symbol} @ ${(trade.currentPct||0).toFixed(1)}%`, "trail"); broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } }); }
      }
    } else {
      trade._slBelowCount = 0;
      if (now >= trade.expiresAt) closeDemoTrade(trade, price, "EXPIRED", tp_pct);
      else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
    }
  }
}

function closeDemoTrade(trade, price, reason, tp_pct) {
  trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
  const dur = Math.round((trade.closeTime - trade.openTime) / 1000);
  const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
  // [CAMBIO 9-jul] PnL COMBINADO del moon-bag: 75% cobrado en la parcial + 25% del runner
  if (trade.runner && trade.partialPct != null) {
    trade.pnlPct = +((1 - MIG_RUNNER_FRACTION) * trade.partialPct + MIG_RUNNER_FRACTION * pnlPct).toFixed(2);
    addLog(`🌙 CIERRE COMBINADO: ${trade.symbol} ${Math.round((1-MIG_RUNNER_FRACTION)*100)}% @ +${trade.partialPct}% + runner @ ${pnlPct.toFixed(1)}% → total ${trade.pnlPct}%`, "trail");
  } else {
    trade.pnlPct = +pnlPct.toFixed(2);
  }
  // [v11.7f] armar la caza por FUERZA sobre la grabación viva (una vez por token)
  if (FZ_ON && isMig(trade.strategy)) {
    const recFz = state.liveRecordings.get(trade.mint);
    if (recFz && !recFz.finished && !recFz.fzArmed) {
      let maxAt = 0;
      for (const p of recFz.puntos) if (p.p > maxAt) maxAt = p.p;
      recFz.fzArmed = true; recFz.fzFired = false;
      recFz.fzTrigPct = +(((1 + maxAt/100) * (1 + FZ_MARGIN) - 1) * 100).toFixed(2);
    }
  }
  const expWinPct = MIG_EXPIRED_WIN_PCT;
  if (reason === "TP") {
    trade.result = "WIN"; state.stats.mig_demoWins++;
    state.stats.mig_demoPnL += trade.pnlPct;
    addLog(`✅ WIN [TP][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win");
  } else if (reason === "STEP") {
    trade.result = "WIN"; state.stats.mig_demoWins++;
    state.stats.mig_demoPnL += trade.pnlPct;
    addLog(`✅ WIN [🪜 ESCALÓN][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win");
  } else if (reason === "RUNNER_END") {
    state.stats.mig_demoPnL += trade.pnlPct;
    if (trade.pnlPct > 0) { trade.result = "WIN"; state.stats.mig_demoWins++; addLog(`✅ WIN [🌙 RUNNER][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win"); }
    else { trade.result = "LOSS"; state.stats.mig_demoLosses++; addLog(`❌ LOSS [🌙 RUNNER][${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "loss"); }
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
  const loteOp = trade.sizeSol || SOL_PER_TRADE_MIG;
  const pnlSolOp = loteOp * trade.pnlPct / 100;
  registrarPnlHorario(pnlSolOp, trade.result === "WIN");
  if (trade.strategy === "migration") {
    brakeRecordClose(trade.pnlPct);  // [v10] alimenta el freno
    // [v10.1] memoria del creador: ¿este acuñador nos ha hecho daño antes?
    const preC = premigData.get(trade.mint);
    if (preC && preC.creator) {
      let h = creatorHist.get(preC.creator);
      if (!h) { h = { tokens: 0, malas: 0 }; creatorHist.set(preC.creator, h); }
      h.tokens++;
      const esMala = trade.pnlPct <= -30 || (trade.maxLossPct || 0) <= -60;
      if (esMala) h.malas++;
      addLog(`[CREATOR] wallet=${preC.creator} mint=${trade.mint} res=${esMala ? "MALA" : "ok"} pnl=${trade.pnlPct}% hist=${h.tokens}t/${h.malas}m${h.malas >= 2 ? " ⚠️ REINCIDENTE" : ""}`, "rec");
      // [v11.7f] ☠️ un cierre <= -80% = pull -> su creador entra en la lista negra DE POR VIDA
      if (MIG_ABYSS_VETO && trade.pnlPct <= MIG_ABYSS_PNL && !abyssCreators.has(preC.creator)) {
        abyssCreators.add(preC.creator);
        addLog(`☠️ LISTA NEGRA DE POR VIDA: creador ${preC.creator} vetado | su ${trade.symbol} cerró ${trade.pnlPct}% (retirada de liquidez)`, "filter");
        saveState();
      }
    }
  }
  state.stats.mig_maxGainSum += trade.maxGainPct || 0;
  state.stats.mig_maxLossSum += Math.abs(trade.maxLossPct || 0);
  state.stats.mig_closedCount++;
  state.stats.mig_avgMaxGain = +(state.stats.mig_maxGainSum / state.stats.mig_closedCount).toFixed(1);
  state.stats.mig_avgMaxLoss = +(state.stats.mig_maxLossSum / state.stats.mig_closedCount).toFixed(1);
  if (trade.mov2s !== null && trade.result !== "EXPIRED") {
    const bucket = trade.mov2s > 1 ? "up" : (trade.mov2s < -1 ? "down" : "flat");
    state.stats[`mig_mov_${bucket}_${trade.result === "WIN" ? "win" : "loss"}`]++;
  }
  // [CAMBIO 9-jul] línea parseable de cierre: con la ventana larga, el MIGREC puede
  // emitirse antes de que la op cierre (cierre_real=n/a). Esta línea permite al
  // analizador cruzar el cierre con su MIGREC por mint.
  addLog(`[MIGCLOSE] mint=${trade.mint} sym=${trade.symbol} strat=${trade.strategy} pnl=${trade.pnlPct>=0?"+":""}${trade.pnlPct}% reason=${reason} dur=${dur}s runner=${trade.runner?1:0} lote=${loteOp}`, "rec");
  unsubscribeToken(trade.mint);   // [v11.7f] no-op si la grabación u otro trade siguen vivos
  // [v11.7f] juicio pre-registrado de la FUERZA: n=100 con feed sano (desde el alta v11.7f)
  if (trade.strategy === "fuerza") {
    if (!state.fzJuicio) state.fzJuicio = { alta: Date.now(), n: 0, neto: 0, w: 0, dictado: false };
    const J = state.fzJuicio;
    J.n++; J.neto += (trade.sizeSol||0.5)*(((trade.pnlPct||0)-4.5)/100); if((trade.pnlPct||0)>0) J.w++;
    broadcast({ event: "fzJuicio", data: J });
    if (!J.dictado && J.n >= 100) {
      J.dictado = true;
      addLog(`[VEREDICTO] ⚡ FUERZA (pre-registrado): n=${J.n} · neto ${J.neto>=0?"+":""}${J.neto.toFixed(2)} SOL · WR ${Math.round(100*J.w/J.n)}% → ${J.neto>0?"ABSUELTA ✅ mantener FZ_ON":"CULPABLE ⚖️ recomendado FZ_ON=false (una variable, sin redeploy)"}`, "warn");
    }
  }
  if (trade.strategy === "migration") {
    liveRecFinish(trade.mint, trade.pnlPct);
    migCleanup(trade.mint, trade.symbol);
  } else {
    // reentry: la grabación y el monitoreo pertenecen a la operación original
    broadcast({ event: "demoTradeUpdate", data: { id: trade.id, status: trade.status, result: trade.result, pnlPct: trade.pnlPct } });
  }
  broadcast({ event: "stats", data: state.stats });
  saveState();
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.status !== "OPEN") continue;
    if (now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint);
    const lastPx = trade.entryPrice * (1 + (trade.currentPct || 0) / 100);   // [v11.7f] último precio visto
    if (!token?.price) addLog(`⚠️ cierre sin feed [${trade.strategy}]: ${trade.symbol} — usando último precio conocido (${trade.currentPct>0?"+":""}${trade.currentPct||0}%)`, "warn");
    closeDemoTrade(trade, token?.price ?? lastPx, "EXPIRED", MIG_TP);
    unsubscribeToken(trade.mint);
  }
}, 30_000);

// ── [v11.7f] EL RESCATADOR ──
setInterval(async () => {
  if (!RESCUE_ON) return;
  const now = Date.now();
  const mintsAbiertos = new Set(
    [...state.demoTrades, ...state.realTrades].filter(t => t.status === "OPEN").map(t => t.mint)
  );
  // poda del reloj (mints ya sin trades ni grabación)
  if (lastTickAt.size > 3000) for (const m of lastTickAt.keys()) if (!mintsAbiertos.has(m) && !state.liveRecordings.has(m)) lastTickAt.delete(m);
  for (const mint of mintsAbiertos) {
    const silencio = now - (lastTickAt.get(mint) || now);
    if (silencio < RESCUE_SILENCE_MS) continue;
    if (now - (lastRescueAt.get(mint) || 0) < RESCUE_COOLDOWN_MS) continue;
    lastRescueAt.set(mint, now);
    // re-suscripción por si la caída fue de la suscripción
    try { if (pumpPortalWs?.readyState === 1) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] })); } catch (e) {}
    try {
      const ctl = new AbortController();
      const to = setTimeout(() => ctl.abort(), 3000);
      const res = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${mint}`, { signal: ctl.signal });
      clearTimeout(to);
      if (!res.ok) continue;
      const px = mejorPrecioDex(await res.json());
      if (!px) continue;
      const ab = state.demoTrades.find(t => t.mint === mint && t.status === "OPEN")
              || state.realTrades.find(t => t.mint === mint && t.status === "OPEN");
      // [v11.7f] sanidad estrecha contra error de escala: el precio de rescate se compara con el
      // ÚLTIMO precio visto del trade (entryPrice × (1+currentPct/100)), no con la entrada a secas.
      // Un salto >20× sobre el último precio conocido es error de escala, no movimiento real -> se ignora.
      const ultimoPx = ab ? ab.entryPrice * (1 + (ab.currentPct || 0) / 100) : 0;
      const ratio = ultimoPx > 0 ? px / ultimoPx : 0;
      if (ab && ratio > 0.05 && ratio < 20) {
        const pct = ((px - ab.entryPrice) / ab.entryPrice * 100).toFixed(1);
        addLog(`🚑 precio de rescate [${mint.slice(0,6)}]: ${pct > 0 ? "+" : ""}${pct}% via DexScreener (feed en silencio ${Math.round(silencio/1000)}s)`, "warn");
        updateDemoTrades(mint, px, "migration");
        updateRealTrades(mint, px, "migration");
        updateReentryTrades(mint, px);
        updateFuerzaTrades(mint, px);
      } else if (ab) {
        addLog(`⚠️ rescate descartado [${mint.slice(0,6)}]: precio ${px} fuera de escala vs último ${ultimoPx.toFixed(8)} (ratio ${ratio.toFixed(2)}) — NO se toca el trade`, "warn");
      }
    } catch (e) {}
  }
}, RESCUE_POLL_MS);


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
    for (const [mint] of state.liveRecordings.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
    // [v11.7f] y los mints con trades abiertos (fuerza/reentry pueden sobrevivir a la grabación)
    const mintsAbiertos = new Set([...state.demoTrades, ...state.realTrades].filter(t => t.status === "OPEN").map(t => t.mint));
    for (const mint of mintsAbiertos) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
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
        lastTickAt.set(data.mint, Date.now());   // [v11.7f] reloj del feed por mint
        // [v11.7f] CUARTA PUERTA: sin esta condición, los ticks de mints con trades vivos
        // pero ya sin grabación/monitoreo morían aquí y nunca llegaban al enrutador.
        const hayTradeAbierto = state.demoTrades.some(t => t.mint === data.mint && t.status === "OPEN")
                             || state.realTrades.some(t => t.mint === data.mint && t.status === "OPEN");
        if (state.migWatching.has(data.mint) || state.migMonitored.has(data.mint) || state.liveRecordings.has(data.mint) || hayTradeAbierto) migUpdatePrice(data.mint, price, sol, data.traderPublicKey || null, data.txType === "buy");
        if (OBSERVER_MODE && state.obsRecordings.has(data.mint)) obsSample(data.mint, price);
        if (MC_OBSERVER && state.mcoRecordings.has(data.mint)) mcoSample(data.mint, price, sol * solPriceUSD);
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
// [v11.7f] exports de un toque: se acabó el ritual del zip
app.get("/export/shadow", (req, res) => res.json(state.shadow || {}));
// [v11.7f] ☠️ la lista negra visible: quién está vetado y por qué
app.get("/export/listanegra", (req, res) => {
  const filas = [...abyssCreators].map(w => {
    const h = creatorHist.get(w) || { tokens: 0, malas: 0 };
    return { wallet: w, tokens: h.tokens, malas: h.malas };
  });
  res.json({ total: filas.length, vetados: filas,
    reincidentes: [...creatorHist.entries()].filter(([w,h]) => h.malas >= 2 && !abyssCreators.has(w))
      .map(([w,h]) => ({ wallet: w, tokens: h.tokens, malas: h.malas })) });
});
app.get("/export/estado", (req, res) => res.json({
  version: "v11.7f", ts: Date.now(),
  shadow: state.shadow || null, fzJuicio: state.fzJuicio || null,
  listaNegra: [...abyssCreators],
  stats: state.stats, demoTrades: state.demoTrades.slice(0, 500), realTrades: state.realTrades.slice(0, 200),
}));
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
      shadow: state.shadow || null,
        fzJuicio: state.fzJuicio || null,
        stats: state.stats,
      risk: riskSnapshot(),
      wsStatus: "connected",
    }
  }));
  ws.on("close", () => frontendClients.delete(ws));
});

server.listen(PORT, async () => {
  if (MC_OBSERVER) {
    console.log(`🔬 SolScanBot — MODO OBSERVADOR PURO (NO OPERA) | graba ${MCO_RECORD_MS/60000}min por token`);
    addLog(`🔬 MODO OBSERVADOR PURO ACTIVO — el bot NO opera, solo graba [MCREC].`, "accept");
  } else if (DEMO_ONLY) {
    console.log(`📝 SolScanBot v11.7f — MODO DEMO | v9 intacta (SL -${((1-MIG_SL)*100).toFixed(0)} · tiers ${MIG_FOLLOW_PCT_STEP*100}/${MIG_TRAIL_P2*100}/${MIG_TRAIL_P3*100}/${MIG_TRAIL_P4*100} · 🌙 runner ${Math.round(MIG_RUNNER_FRACTION*100)}% · ✂️ 30s · 🚫 holders>=${MIG_MIN_HOLDERS}) + 🧊 freno(N=${MIG_BRAKE_N},${MIG_BRAKE_SUM}%,${MIG_BRAKE_PAUSE_MS/60000}m) + 🔥 lote por calor + 🔄 reentry(confirm ${MIG_SL_CONFIRM_TICKS} ticks) + 👛 buyers60/wash60 + 🏭 creator | ventana ${MIG_DURATION_MS/60000}min · grabación ${LAB_EXTEND_MS/60000}min`);
      if (_stateInfo.persistent) addLog(`💾 Estado persistente OK: ${STATE_FILE}`, "accept");
  else addLog(`🚨 VOLUMEN NO DISPONIBLE: guardando en ${STATE_FILE} (EFÍMERO — el historial y la lista negra SE PERDERÁN en cada redeploy). Revisa el Volume de Railway.`, "error");
  addLog(`📝 v11.7f DEMO — no-despegue OFF · trailing x2.5 · reentry estricta (-45/+60) · calor OFF · freno OFF · 🏭 veto de fábricas · 🚫 blacklist de quotes · ☠️ lista negra de por vida (cierre ≤${MIG_ABYSS_PNL}% → creador vetado) · cordura txs RETIRADA · ⚡ ejecución v11.7f: EXEC_MODE=${EXEC_MODE} slippage/prio por urgencia · REAL=DEMO (moon-bag+reentry, MC 1M, lote ${SOL_PER_TRADE_REAL}) · 🕗 franja real bloqueada: ${REAL_FRANJA_BLOCK.join(",")}h ES · 🐢 velMax ${REAL_VEL_MAX>0?REAL_VEL_MAX+"s":"OFF"} · 📡 sig+PREMIGv2 · 🛋️ LA DEL SOFÁ: SL-39 · x6.3 · BEoff · RE -60/+45/arm50/tr55 · ⚡FZ ${FZ_ON?"+50/sl-15/tr50":"APAGADA"} · 🔧 feed-guard+router+dispatcher (v11.7f) · 🛑 slPct vivo · 🚑 rescate DexScreener · 🏟️ SOMBRAS: 16 configs · horas · días · s0-s10 · 1s denso · 🗳️ propuestas+veredicto FZ · /export · 🔧 alta-fix · 🛡️ rescate-escala-fix · ☠️ /export/listanegra · 21 vetados de semilla (v11.7f). ${DEMO_ONLY?"NO toca wallet real.":"⚠️ MODO REAL ACTIVO"}`, "accept");
  } else {
    console.log(`🚀 SolScanBot v9.0-FUSION REAL+DEMO | mismos parámetros en ambos | runner solo en demo`);
  }
  if (!HELIUS_API_KEY && !process.env.SOLANA_RPC) addLog("⚠️ Sin HELIUS_API_KEY ni SOLANA_RPC — usando RPC público (lento, puede limitar)", "warn");
  loadState();
  seedCreators();  // [v11.1] la semilla se fusiona DESPUÉS del estado (máximo de ambos)
  initWallet();
  connectPumpPortal();
  if (!MC_OBSERVER) await reconcileStateOnBoot();
});
