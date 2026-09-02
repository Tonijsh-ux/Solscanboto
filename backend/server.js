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
const SOL_PER_TRADE_REAL = +(process.env.SOL_PER_TRADE_REAL || 0.3); // [v11.9] fase de medición
const MIG_MAX_MC_REAL = 1_000_000; // [v11.9] IDÉNTICO al demo (antes 200K: divergencia eliminada)
const SOL_PER_TRADE_MIG = 0.5;
const MAX_REAL_TRADES = +(process.env.MAX_REAL_TRADES || 2);   // [16-ago] prueba: 2 x 0.3 = 0.6 SOL, deja margen de gas sobre el balance de ~0.75 (era 10)
const MAX_MIG_REAL = 10;
const REAL_STRATEGIES = ["migration"];   // [27-ago] solo migración (reentry y fuerza eliminadas)

// ── [v11.9] EJECUCIÓN REAL — obra de fricción ──────────────────────────────
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
// [v11.9] FRANJA HORARIA — SOLO MODO REAL. Tribunal 16-jul: la 20-21h ES pierde en ambas
// eras (-73/-64 mSOL/op), 9/10 días rojos, -70 mSOL/op con n=121. El demo SIGUE entrando
// a esa hora (el laboratorio no pierde los ojos y la señal se re-valida semanalmente).
const REAL_FRANJA_BLOCK = (process.env.REAL_FRANJA_BLOCK ?? "20").split(",").map(Number).filter(n => !isNaN(n));
const REAL_TZ_OFFSET = +(process.env.REAL_TZ_OFFSET || 2);  // España verano = UTC+2
const horaES = () => (new Date().getUTCHours() + REAL_TZ_OFFSET) % 24;
const franjaRealBloqueada = () => REAL_FRANJA_BLOCK.includes(horaES());
// [v11.9] Veto de VELOCIDAD solo-real: señal fichada (vel>=4.7s pierde en ambas eras, 7/8 dias,
// ~+1.5 SOL/dia). APAGADO por defecto: activar con REAL_VEL_MAX=4.7 cuando el real arranque.
const REAL_VEL_MAX = +(process.env.REAL_VEL_MAX || 0);

// [v11.9] STATE_FILE robusto: sonda de escritura al arrancar + fallback anunciado.
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
const MIG_SL = 0.61;   // [v11.9] -39% (la del sofá)
// [CAMBIO 9-jul] Expiración 12min → 3 HORAS: el Excel de revisión demostró que los
// topes reales llegan a las 2h (ej. $33K→$416K a las 2h de entrar) y la expiración
// corta decapitaba a las corredoras. Se mantiene como red de seguridad anti-zombi.
const MIG_DURATION_MS = 0;   // [4-ago] 0 = SIN EXPIRACIÓN: la op vive hasta que el trailing la cierre.
                             // Antes 60min, pero cerraba cohetes vivos solo por el reloj.
const MIG_HARD_MAX_MS = 6 * 60 * 60 * 1000;   // salvavidas: 6h de tope absoluto
const MUERTO_PCT = 90;   // [16-ago] caída desde la entrada a partir de la cual damos el token por muerto
const MIG_WINDOW_MS = 60_000;
const MIG_FAST_WINDOW_MS = 20_000;
const MIG_VOL_FAST_EFF = 1;   // [27-ago] sin filtro de volumen (bandera eliminada)
const MIG_VOL_SLOW_EFF = 1;
const MIG_MIN_MC = 0;
const MIG_MAX_MC = 2_000_000;
const MIG_MAX_MC_ENTRY = 1_000_000;
const MIG_MIN_MC_ENTRY = 2_500; // LAB: bajado de 5000 para ver todo el espectro

// ── [CAMBIO 9-jul] CORTE POR NO-DESPEGUE — REACTIVADO y por MÁXIMO ──
// Validado en backtest sobre 206 ops (8-9 jul, train/test cronológico): a los 30s,
// si el MÁXIMO alcanzado no ha tocado +10%, salir. Fue la palanca con mayor efecto
// marginal de toda la rejilla (+6.3 mSOL/op). La variante por máximo a 30s ganó
// tanto a la de 15s como a la R30 de "precio actual en rojo".
const MIG_LAUNCH_CHECK = false;  // [v11] OFF — validado: los 'cuchillazo y vuelo' se quedan dentro y el trailing ancho los cabalga (test +4.48 vs +0.21 con corte suave)
const MIG_LAUNCH_CHECK_MS = 30_000;  // a los 30 segundos de abrir
const MIG_LAUNCH_MIN_PCT = 10;       // exige que el MÁXIMO haya tocado +10%

// ── [CAMBIO 9-jul] FILTRO DE ENTRADA POR HOLDERS ──
// Validado (train +35.5 / test +20.3 mSOL/op): holders<20 en el PREMIG = veneno
// (cierre medio -48% sobre 10 ops de 8-9 jul). Fail-open: si Helius no ha
// respondido aún, se entra igualmente para no depender de su disponibilidad.
// [v11.9] LISTA NEGRA DE QUOTES: PumpPortal puede emitir migraciones de pools
// cotizadas en USDC con el mint del QUOTE — el bot llegó a "operar" el propio USDC
// (fantasmas de -78/-81% en un activo de $1). Estos mints jamás se tocan.
const QUOTE_BLACKLIST = new Set([
  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", // USDT
  "So11111111111111111111111111111111111111112",  // wSOL
]);
// [v11.9] CORDURA txs RETIRADA: el retro demostró que vetaba ~110 tokens/día legítimos
// (57% ganadores, incluido el +1274 del 13-jul con 2107 txs). Queda solo la lista negra de quotes.
const MIG_ABYSS_VETO = true;         // [v11.9] ☠️ lista negra DE POR VIDA para creadores de pulls
const MIG_ABYSS_PNL  = -80;          // cierre demo <= -80% = retirada de liquidez -> su creador, vetado para siempre
const MIG_MIN_HOLDERS = 20;
// [FIX 26-jul] 💎 FILTRO topBal (mando del lab): si la MEDIANA de saldo de las 5 billeteras
// top es < 0.5 SOL, perfil rug — fuera. Fail-open: sin dato del PREMIG, la op entra igual.
// Poner MIG_MIN_TOPBAL=0 en el entorno para apagarlo.
const MIG_MIN_TOPBAL = +(process.env.MIG_MIN_TOPBAL ?? 0.5);
const MIG_DECIDE_ON = true;    // [4-ago] el bot decide EXACTAMENTE en los mismos instantes que graba
                               // (ver cadenciaMs): 1s los 3 primeros min, 2s hasta los 30, 5s después.
                               // Los ticks intermedios se descartan enteros: ni mueven el máximo ni
                               // disparan el stop. false = tick a tick (comportamiento antiguo).
// [5-ago] PUERTA DE REDES SOCIALES (fail-open: sin dato, pasa).
//   "tg"      → exige Telegram. Lo tiene el 13% de los tokens; en el lab separa +37 vs -31 mSOL/op (11 ops).
//   "tg_o_tw" → exige Telegram O Twitter. Twitter lo tiene el 90%, así que deja pasar casi todo:
//               en el lab 131 de 135 ops, resultado casi idéntico a no filtrar (-29 vs -31 mSOL/op).
//   "off"     → sin puerta de redes.
const MIG_RED_MODO = "tg_o_tw";
const premigData = new Map();        // mint → { ageMin, total, holders, topPct, top5Pct, top10Pct, creator }
// [v10.1] MEMORIA DE CREADORES: wallet que acuñó cada token → resultados con nosotros.
// Fase 1 = solo medir (¿los rugs vienen de reincidentes?). El filtro llegará si los datos lo validan.
const creatorHist = new Map();       // wallet → { tokens, malas }
// [v11.9] SEMILLA de la lista negra: 21 creadores cazados haciendo rug (cierre ≤-80%)
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
const abyssCreators = new Set(ABYSS_SEED);     // [v11.9] ☠️ creadores vetados de por vida (nos hicieron un ≤-80%)
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

const migFlowTimes = [];             // timestamps de migraciones detectadas

// ── [v10] RE-ENTRADA EN RESUCITADOS (estrategia demo separada; validada
// walk-forward: +7.3 SOL aditivos en 11 días — negocio de cola: 1.4% de premios
// pagan los billetes; fricción real ~4.5% ya considerada en la validación) ──

// ── [23-ago] 🤝 UNIDA (demo puro): pierna bot paramétrica + relevo mixta tras SL ──
// Motor clonado línea a línea de labSimUnida del analizador; decide en la MISMA cadencia en que
// graba la cámara (uniSample se llama desde liveRecSample), igual que el lab. Validada en el
// analizador 17-19 ago (111 ops): +25.9 SOL simple lote 0.5 (+72.1 compuesto banca 25/2%).
// OJO honesto: histórico completo depurado ≈ -2.3 SOL → estrategia SOMBRA en demo hasta que
// los días nuevos la confirmen. SIN espejo real a propósito.
const UNI_ON = true;
const UNI_SIZE = 0.5;              // lote demo por compra (pierna bot y cada relevo)
const UNI_MAX_OPEN = 12;           // tope global de TOKENS con unida abierta a la vez
const UNI_LATIDO_MS = 6_000;       // [28-ago] cada cuánto refresca el panel una posición abierta
const UNI_MAX_SOL = 25;            // [27-ago] tope de capital desplegado: no abre compras nuevas
                                   // por encima de esto (el pico real medido fue 17 SOL)
// pierna bot (tornillos del lab, captura 22-ago — INDEPENDIENTES de la Base de migración):
const UNI_BOT = { sl: -15, mult: 3, arm: 60, p1: 40, p2: 77, ndT: 10, ndM: 28, relNL: false };
// relevo mixta:
const UNI_GIRO = 10;               // % de giro de los pivotes (detector de soportes)
const UNI_TAU = 35;                // s de suavidad de la envolvente roja de máximos (venta)
const UNI_MD = 5;                  // margen del soporte: compra al cruzar soporte -5%
const UNI_MV = 20;                 // margen de la venta: vende al cruzar roja +20%
const UNI_MAXLOT = 8;              // compras acumulables simultáneas por paquete
const UNI_MAXVEND = 5;             // tope de lotes VENDIDOS por op (maxCiclos del lab)
const UNI_RUG = 50;                // guardarraíl: no comprar por debajo de -50%
const UNI_MUERTO_PCT = 60;         // [23-ago] token muerto: 60s seguidos por debajo de -60%
const UNI_MUERTO_S = 60;           //   → liquida el paquete a mercado y deja de operar ese token
                                   //   (validado: mejora 17-19 +35.5→+38.2 y el histórico -17.3→-13.9)

// ── [27-ago] DOS VARIANTES EN PARALELO ──────────────────────────────────────────
// A) "unida"  = la de siempre, la que lleva días midiéndose.
// B) "unida2" = la que salió de trastear el laboratorio: sin corte de no-despegue,
//    trailing más estrecho, soporte pegado y solo en tokens con poco wash.
//    AVISO honesto: en la ventana 18-19 esta variante perdió (-7.9 SOL) donde la A
//    ganaba (+10.4). Va en demo justamente para resolver eso con días nuevos.
const UNI2_ON = true;
const UNI2 = {
  id: "unida2",
  // [30-ago] config afinada en el laboratorio. Sobre las 292 ops del 17→28, con el wash
  // medido COMO LO VE EL BOT (dato parcial en el momento de comprar, no el del minuto entero):
  //   17-19: +58.98 · 23-26: +37.35 · 27-28: +25.08  →  TOTAL +121.4 SOL (la variante A: +47.5)
  // Último cambio: no-despegue de 10s/+29% a 20s/+35% (+10 SOL, todos del 17-19: la pierna
  // del bot aguanta el doble antes de rendirse y pilla los arranques lentos). Meseta entre
  // +30% y +40%; a 15s se cae a +102, así que el salto que importa es el del tiempo.
  // Gana en las tres ventanas. Ojo: 4 ops aportan la mayor parte; es una estrategia de pocos
  // aciertos grandes, así que un mes flojo puede dejarla plana.
  bot: { sl: -15, mult: 3, arm: 80, p1: -20, p2: 150, ndT: 20, ndM: 35, relNL: false },
  giro: 10, tau: 85, md: 2, mv: 20,
  maxlot: 8, maxvend: 5, rug: 50,
  muertoPct: 60, muertoS: 60,      // 💀 60s seguidos bajo -60% → liquida y abandona
                                   //    (+2.2 SOL, gana en las tres ventanas)
  washMax: 20,                     // el relevo solo entra si el wash del primer minuto ≤ 20
  tp: 500,                         // 🎯 cobra el paquete entero al llegar a +500% sobre su media
  rojaFrac: 0.5,                   // 🔴 en la roja vende solo la MITAD y deja correr el resto
  plazoMin: 35,                    // ⏳ deja de ABRIR compras nuevas pasado este minuto
                                   //    (meseta 25-45 min: +8.7 SOL, todo el efecto en 23-26)
};
const UNI1 = {
  id: "unida",
  bot: UNI_BOT, giro: UNI_GIRO, tau: UNI_TAU, md: UNI_MD, mv: UNI_MV,
  maxlot: UNI_MAXLOT, maxvend: UNI_MAXVEND, rug: UNI_RUG,
  muertoPct: UNI_MUERTO_PCT, muertoS: UNI_MUERTO_S, washMax: 0, rojaFrac: 1, plazoMin: 0,
  // [2-sep] único cambio en la A desde el 23-ago: objetivo de venta al +500%. Es lo más limpio
  // que hemos medido: 62.1 → 80.8 SOL sobre 371 ops, ganando en las CUATRO ventanas (incluida
  // la del 1-2 sep, que no se usó para ajustar nada) y con solo 3 disparos. No toca la entrada
  // ni el resto de salidas. Ojo: la media roja y el plazo, que van en la B, restaban en esa
  // ventana nueva, así que aquí NO se ponen.
  tp: 500,
};
const UNI_VARIANTES = () => UNI2_ON ? [UNI1, UNI2] : [UNI1];

// ── [v11.9] RE-ENTRADA POR FUERZA ("la del sofá") ──

// ── [v11.9] RESCATE DE FEED: PumpPortal a veces calla en origen (5ª fuente: el volumen
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
  // [v11.9] DexScreener da priceNative (precio en SOL) y priceUsd (en USD). El precio INTERNO
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
// ═══ [5-ago] GRABADOR DE RECHAZADOS ═══
// El bot solo graba las ops en las que ENTRA, así que no hay forma de saber si los filtros
// eligen bien: solo vemos supervivientes. Esto graba una MUESTRA de los tokens rechazados
// SIN operarlos, para poder comparar en el lab qué habrían hecho.
const LIVE_RECORD = true;
const LIVE_REC_DENSE_MS = 180_000;   // [FIX 27-jul] 1min→3min de muestreo a 1s: el 80% de las salidas se decide ahí y el lab divergía del bot justo en esa ventana (mediana real +3.6 vs simulada -5.0)
const LIVE_REC_DENSE_INTERVAL = 1_000;   // [v11.9] 1s el primer minuto: escalera s0-s10 exacta, gratis
const LIVE_REC_NORMAL_INTERVAL = 5_000;
// [4-ago] CADENCIA ÚNICA: la misma escalera gobierna la CÁMARA y las DECISIONES del bot.
// Así el lab reproduce exactamente lo que hizo el bot: si la curva tiene un punto por segundo,
// el bot decidió una vez por segundo; si tiene uno cada 5s, el bot también miró cada 5s.
// dt = milisegundos desde la entrada · nPts = puntos ya grabados (salvavidas de tamaño).
function cadenciaMs(dt, nPts) {
  let i;
  if (dt <= LIVE_REC_DENSE_MS) i = LIVE_REC_DENSE_INTERVAL;   // 0-3 min  → 1s
  else if (dt <= 1_800_000)    i = 2_000;                     // 3-30 min → 2s
  else                         i = LIVE_REC_NORMAL_INTERVAL;  // >30 min  → 5s
  if (nPts > 3000) i = Math.max(i, 10_000);
  if (nPts > 6000) i = Math.max(i, 30_000);
  return i;
}
const OBS_MIN_VOL = 2_000;
const OBS_MIN_MC = 20_000;
const OBS_RECORD_MS = 600_000;
const OBS_T1_MS = 60_000;
const OBS_T1_INTERVAL = 2_000;
const OBS_T2_MS = 300_000;
const OBS_T2_INTERVAL = 3_000;
const OBS_T3_INTERVAL = 5_000;

const MCO_T1_MS = 60_000;             // primer MINUTO completo = alta resolución
const MCO_T1_INTERVAL = 1_000;        // muestreo cada 1s durante el primer minuto
const MCO_T2_INTERVAL = 5_000;        // después del minuto, cada 5s

const MIG_BREAKEVEN_AT = 0.20;        // breakeven al +20%: protege el suelo antes
const MIG_BREAKEVEN_MARGIN = 0.03;
const MIG_BE_ON = process.env.MIG_BE_ON === "true";   // [v11.9] BE OFF por defecto (la del sofá)
const MIG_LOCK_AT = 0.25;             // trailing FOLLOWING se arma en +25%
const MIG_FOLLOW_PCT = 0.90;   // [v11.9] x6.3 (cap 90%)  // [v11] x2.5 — config del usuario validada (train +36 / test +4.5, 11/13 días)
const MIG_MAX_PRICE_RATIO = 2.0;
const MIG_SL_CONFIRM_TICKS = 1;   // [4-ago] 2→1: el lab cierra en la PRIMERA muestra bajo el stop.
                                  // Con la cadencia compartida, una mecha de milisegundos ya no llega
                                  // a muestrearse, así que la confirmación extra solo generaba divergencia.
// [FIX 27-jul] 🪂 PERFORACIÓN PROFUNDA: la confirmación de 2 ticks protege de mechas CERCA del
// stop, pero en un rug vertical cada tick llega cientos de puntos más abajo (caso 5nHepJY: stop
// +535, salida real +12). Si el precio ATRAVIESA el stop más de un 12%, se ejecuta al instante
// y no se convierte a runner (convertir en caída libre regala el 25% al abismo).
const MIG_SL_PANIC_BREACH = +(process.env.MIG_SL_PANIC_BREACH || 0.12);
const esPanico = (trade, price) => trade.sl > 0 && price <= trade.sl * (1 - MIG_SL_PANIC_BREACH);
// [31-jul CcYp2Y] GUARDIA ANTI-PINCHAZO EN LA SALIDA.
// Un tick muy por debajo de la mediana reciente casi siempre es un print fantasma, no mercado:
// en 2.370 ops solo el 1% de los pinchazos <-35% se recuperaba... pero de esos, 12 llegaban
// luego a +50% o mas (uno a +1367%). Y esperar 2s en los pinchazos de verdad cuesta 0,58
// puntos de media (189 peor / 178 mejor: moneda al aire). Conclusion: no vender en un tick
// solitario. El panico se mantiene, pero solo si la MEDIANA tambien esta rota.
const MIG_TICK_FANTASMA = 20;   // % por debajo de la mediana de 3s para considerar el tick sospechoso
function precioRefTrade(trade, price) {
  const h = trade._pHist || (trade._pHist = []);
  h.push([Date.now(), price]);
  while (h.length > 2 && Date.now() - h[0][0] > 3_000) h.shift();
  if (h.length < 3) return null;
  const v = h.map(x => x[1]).sort((a, b) => a - b);
  return v[v.length >> 1];
}
function tickFantasma(trade, price) {
  const ref = precioRefTrade(trade, price);
  return ref != null && price < ref * (1 - MIG_TICK_FANTASMA / 100);
}
const MIG_EXPIRED_WIN_PCT = 2;
const MIG_ENTRY_DELAY_MS = 3_000;
// [FIX 28-jul] 🎓 EXAMEN DE 2s: tras pasar todos los filtros, el bot NO compra aún — espera
// 2 segundos más y mide el movimiento desde ese instante. Si el token se está hundiendo más
// de un -5%, no entra: vuelve al portero (sigue vigilado dentro de sus 10 min por si da un
// tirón limpio después). Medido sobre 2.050 ops: -49.9 → -26.6 SOL conservando 40 de 47
// monstruos+godzillas, con peaje medio de +3.1% en las que entran.
const MIG_EX2S_ON = true;      // el examen SIEMPRE activo, forma parte de la estrategia
const MIG_EX2S_MIN = -5;       // suspende si cae más de -5% en los 2s del examen
const MIG_EX2S_MS = 2_000;
const MIG_QUAL_GATE = true;
const MIG_QUAL_MOV2S_MIN = 1.0; // [FIX 26-jul] mando del lab: señal solo si mov2s ≥ +1% (era 0.5). Backtest 1.696 ops: junto con topBal≥0.5 → cr medio +7.4, conserva 9/9 godzillas
const MIG_QUAL_ONE_STRIKE = 3.0; // [FIX 29-jul] REGLA A LA PRIMERA (lab, 2.332 ops): la 1ª señal es la que informa. Si el 1er cruce sale débil (<3%), token DESCARTADO para siempre — sin re-vigilancia. Umbral continuo a 3 ≈ −23 SOL (el 81% re-dispara a los ~16s y vuelve a perder); descarte a la primera ≈ −2 SOL vs −38.5 base.
const MIG_MAX_VEL_S = 10;   // [29-jul tarde] mando del lab: 11 → 10
const MIG_ENTRY_SPIKE_MAX = 30;   // [31-jul, relajado] ANTI-SPIKE DE ENTRADA: si el ultimo tick esta mas de un 15%
                                  // por encima de la mediana de los ultimos ~3s, es un print de lavado, no precio real.
                                  // Sin esto el bot 'compra' el pico y la op nace ya en -49%.

// ═══ [29-jul] MOTOR POR CLASES (portado de las tarjetas del lab · 2.332 ops) ═══
// El mov2s de los primeros 2s tras la entrada NO decide la entrada: a los 2s ya es
// pasado, y se usa solo para ELEGIR el motor de salida. Prioridad: godzilla >
// monstruo > cohete > mediana > Base. Condición con dato ausente = no encaja (Base),
// igual que el lab. t10/t5/txs/edad del PREMIG; vel y MC de la señal; mov2s a los 2s.
// La única diferencia con el lab: allí los tornillos de la clase aplican desde t=0;
// en vivo solo se conocen a t=2s, así que los 2 primeros segundos corren con la Base.
// [4-ago] Base = la configuración validada en el lab (275 ops, sig>=3+vel<=10+topBal+telegram):
// stop -40 · anchura x7 · escalón +30 · piso1 breakeven (0%) · piso2 +60 · SIN moon-bag · ND 90s/+25% · SIN take profit
const MIG_CFG_BASE = { nom:"base", sl:-40, mult:7.0, arm:30, piso1:0, piso2:60, pisoOn:true, moonOn:false, runTrig:75, runTr:50, ndT:90, ndMin:25, tp:1e9 };
// [4-ago] CLASES DESACTIVADAS. El lab valida la config Base sobre TODAS las ops; el server, en cambio,
// enrutaba a 4 clases cuyas ventanas de vel (<3.8, <5.4) ya no encajan con la población actual
// (mediana 5.8s), así que apenas se activaban y, cuando lo hacían, ejecutaban otra estrategia
// distinta de la validada. Para que bot y lab hagan lo mismo, todo va a la Base.
// [27-ago] clases eliminadas: todo corre con la config Base.
const MIG_CFGS = [
  { cond:d=> d.t10!=null&&d.t10>87 && d.mov!=null&&d.mov>1.12 && d.t5!=null&&d.t5>75,
    C:{ nom:"godzilla", arm:25, sl:-29, mult:6.0, piso1:24, piso2:80, pisoOn:false, runTrig:49, runTr:33, ndT:41, ndMin:58, tp:1e9 } },
  { cond:d=> d.tx!=null&&d.tx<1801 && d.vel!=null&&d.vel>3.4 && d.ed!=null&&d.ed<1,
    C:{ nom:"monstruo", arm:25, sl:-30, mult:6.0, piso1:11, piso2:65, pisoOn:false, runTrig:61, runTr:31, ndT:30, ndMin:20, tp:2000 } },
  { cond:d=> d.mc!=null&&d.mc>38100 && d.vel!=null&&d.vel<5.4 && d.mc<49000,
    C:{ nom:"cohete", arm:25, sl:-12, mult:6.3, piso1:30, piso2:60, pisoOn:true, runTrig:50, runTr:30, ndT:45, ndMin:30, tp:1000 } },
  { cond:d=> d.vel!=null&&d.vel<3.8 && d.mov!=null&&d.mov>-10.4 && d.mc!=null&&d.mc<82200,
    C:{ nom:"mediana", arm:25, sl:-30, mult:5.0, piso1:20, piso2:100, pisoOn:true, runTrig:45, runTr:30, ndT:40, ndMin:40, tp:1500 } },
];
function migCfgTrailPct(maxGainPct, mult) {   // mismos tiers del lab: b(máx) × anchura, tope 90%
  const b = maxGainPct >= 100 ? 0.08 : maxGainPct >= 60 ? 0.12 : maxGainPct >= 40 ? 0.15 : 0.20;
  return Math.min(0.90, b * mult);
}
function migRouteCfg(trade) {
  trade.cfg = MIG_CFG_BASE; return;   // [27-ago] siempre la Base (clases eliminadas)
  const pre = premigData.get(trade.mint) || {};
  const d = { t10: pre.top10Pct ?? null, t5: pre.top5Pct ?? null, tx: pre.total ?? null, ed: pre.ageMin ?? null,
              vel: trade.velSeg ?? null, mc: trade.mcOpenUsd ?? null, mov: trade.mov2s ?? null };
  let C = MIG_CFG_BASE;
  for (const c of MIG_CFGS) { if (c.cond(d)) { C = c.C; break; } }
  trade.cfg = C;
  const slCfg = trade.entryPrice * (1 + C.sl / 100);
  if (slCfg > trade.sl) setSL(trade, slCfg, "cfg-" + C.nom);
  trade.tp = +(trade.entryPrice * (1 + C.tp / 100)).toFixed(12);
  addLog(`🎛️ MOTOR ${C.nom.toUpperCase()}: ${trade.symbol} | mov2s ${trade.mov2s != null ? (trade.mov2s >= 0 ? "+" : "") + trade.mov2s.toFixed(1) + "%" : "n/a"} → SL ${C.sl}% · anchura x${C.mult} · pisos ${C.pisoOn ? "+" + C.piso1 + "/+" + C.piso2 : "off"} · moon +${C.runTrig}%/${C.runTr}% · ND ${C.ndT}s/+${C.ndMin}% · TP ${C.tp >= 1e8 ? "sin" : "+" + C.tp + "%"}`, "demo");
} // [FIX 29-jul] VETO DE LENTOS, mando del lab: vel = segundos migración→entrada (señal+confirmación+examen incluidos, la misma vel que graba el MIGREC). Si tarda más de 11s en entrar, fuera.
const MIG_QUAL_MAX_WAIT_MS = 600_000;  // qual_gate CONTINUO: vigila hasta 10 min esperando la señal
const MIG_QUAL_WINDOW_MS = 15_000;
const MIG_QUAL_DECIDE_MS = 2_500;
const MIG_MAX_CAIDA_DELAY = 0.10;      // [FIX 26-jul] aborta si cae más de -10% en la confirmación (antes 0.35: toleraba un -34% tras la señal y entraba en pleno cuchillo)
const MIG_STEP_TRIGGER = 0.25;        // escalón (suelo +13%) se arma en +25%
const MIG_STEP_FLOOR = 0.13;
// [CAMBIO 9-jul] TIERS ANCHOS validados en backtest (192 configs, train/test):
// el trailing fino devolvía las corredoras (Excel: topes reales +682% cerrados a +10).
const MIG_FOLLOW_PCT_STEP = 0.90;   // [v11.9] x6.3 (cap 90%)
const MIG_HARD_CAP_LOSS = -20;
const MIG_TRAIL_T1 = 40;  const MIG_TRAIL_P1 = 0.90;    // [FIX 27-jul] el ×6.3 de la v11.9 se aplicó a P2/P3/P4 pero NO a P1: quedaba un acantilado absurdo (máx 39→trailing 15%, máx 41→90%) que estranguló 232 de 236 ops de esa banda. Réplica sobre 1.227 ops: +1.9 SOL
const MIG_TRAIL_T2 = 60;  const MIG_TRAIL_P2 = 0.90;    // [v11.9] x6.3 (cap)  // [v11] era 0.15
const MIG_TRAIL_T3 = 100; const MIG_TRAIL_P3 = 0.756;   // [v11.9] 0.12×6.3
const MIG_TRAIL_P4 = 0.504;   // [v11.9] 0.08×6.3
const MIG_TOP_FLOOR_TRIGGER = 100;
const MIG_TOP_FLOOR = 0.65;

// ── KILL-SWITCH DE PORTAFOLIO ──
const RISK = {
  // [16-ago] PRUEBA EN REAL con lote 0.3 — límites deliberadamente estrechos.
  // Con esta estrategia (sin TP ni expiración) una op puede quedarse abierta horas,
  // así que el freno tiene que venir del portafolio, no del trade.
  maxDailyLossSol: +(process.env.RISK_MAX_DAILY_LOSS || 0.45),  // 1.5 lotes: con balance ~0.75 el tope debe morder ANTES que el saldo
  maxConsecutiveLosses: 5,
  maxWindowLossSol: 0.3,        // 1 lote completo en 6h
  windowHours: 6,
  cooldownAfterStreakMs: 60 * 60 * 1000,
};

// ── SECRETOS desde el entorno ──
const HELIUS_API_KEY = process.env.HELIUS_API_KEY || "";
const SOLANA_RPC = process.env.SOLANA_RPC
  || (HELIUS_API_KEY ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : "https://api.mainnet-beta.solana.com");
const PUMPPORTAL_API_KEY = process.env.PUMPPORTAL_API_KEY || "";

// ── [30-ago] ESPÍA DE HELIUS ─────────────────────────────────────────────────
// No opera ni decide nada: se suscribe a los MISMOS mints que PumpPortal y solo
// cuenta. Sirve para responder con datos a una pregunta concreta: cuando PumpPortal
// deja de mandar los trades de un token (153 grabaciones truncadas en 12 días),
// ¿los sigue viendo Helius? Si la respuesta es sí, la cámara debe cambiar de fuente.
const ESPIA_ON = process.env.ESPIA_ON !== "0" && !!HELIUS_API_KEY;
// [1-sep] el espía deja de ser solo observador: cuando PumpPortal lleva callado más de
// ESPIA_RELEVO_MS en un token, Helius alimenta la cámara con el precio de la piscina.
// Sin esto, 153 de 292 grabaciones se truncaban con el token vivo y los paquetes abiertos
// se liquidaban a mercado sin motivo. Si Helius falla, todo vuelve al comportamiento de antes.
const ESPIA_ALIMENTA = process.env.ESPIA_ALIMENTA !== "0";

// ── [1-sep] MAYHEM MODE de pump.fun ──────────────────────────────────────────────
// Tokens creados con esa opción reciben 1.000 millones de tokens EXTRA (2.000 en total)
// que un agente de IA usa para comprar y vender AL AZAR durante sus primeras 24 horas.
// Nos afecta de tres formas: (1) el MC nos salía a la mitad, porque lo calculábamos con
// 1.000 millones de supply; (2) el wash y los compradores se inflan con un bot; (3) los
// pivotes y la envolvente leen un paseo aleatorio, no un mercado.
// De momento solo se DETECTA y se apunta; el veto se enciende cuando los datos lo digan.
const MAYHEM_VETO = process.env.MAYHEM_VETO === "1";
const supplyCache = new Map();   // mint → { supply, mayhem }
// el MC se calculaba SIEMPRE con 1.000 millones de supply. En un token Mayhem el supply es
// el doble, así que el MC real es el doble del que veíamos y los filtros de MC decidían con
// la mitad del valor. Esta función devuelve el supply que toca en cada caso.
const supplyDe = (mint) => (supplyCache.get(mint)?.supply) || 1_000_000_000;
const ESPIA_RELEVO_MS = 25_000;
// [1-sep] HELIUS COMO FUENTE PRINCIPAL de la cámara. Con esto encendido, cada swap que llega
// de Helius alimenta la grabación con precio, cartera, dirección y volumen — todo lo que hasta
// ahora daba PumpPortal. Sus ticks siguen contándose para poder comparar, pero ya no graban.
// Motivo: el espía midió precio equivalente (1.8% de diferencia), 5-6% más de swaps vistos y
// tokens enteros donde PumpPortal enmudecía. Además subscribeTokenTrade se paga en SOL.
const HELIUS_PRIMARIO = process.env.HELIUS_PRIMARIO === "1";
const HELIUS_CAIDO_MS = 45_000;   // sin swaps de Helius durante este tiempo ⇒ vuelve PumpPortal
const HELIUS_WS = HELIUS_API_KEY ? `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : "";
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
// [2-sep] 30s → 5min. El saldo solo se muestra en el panel (estamos en demo), y con el
// WebSocket de Helius moviendo ~1,6 GB/día por la misma clave, las consultas sueltas
// rebotaban con 500. Menos llamadas = menos rechazos y menos ruido en el log.
const BALANCE_CACHE_MS = 300_000;
let balanceFallos = 0;

async function getWalletBalance(force = false) {
  if (!wallet || !connection) return cachedBalance;
  const now = Date.now();
  if (!force && now - lastBalanceFetch < BALANCE_CACHE_MS) return cachedBalance;
  for (let i = 0; i < 3; i++) {
    try {
      cachedBalance = (await connection.getBalance(wallet.publicKey)) / LAMPORTS_PER_SOL;
      lastBalanceFetch = Date.now();
      if (balanceFallos >= 5) addLog(`✅ el saldo vuelve a leerse (${cachedBalance.toFixed(4)} SOL)`, "info");
      balanceFallos = 0;
      return cachedBalance;
    }
    catch (e) {
      // espera creciente: 1s, 3s. Un 500 de Helius suele ser un bache de segundos.
      if (i < 2) { await new Promise(r => setTimeout(r, 1000 * (i + 1) * (i + 1))); }
      else {
        balanceFallos++;
        lastBalanceFetch = Date.now();   // no reintentar en bucle: espera el ciclo completo
        // el aviso, solo la primera vez y luego cada 10, para no llenar el log
        if (balanceFallos === 1 || balanceFallos % 10 === 0) {
          addLog(`⚠️ no se puede leer el saldo (${balanceFallos} veces): ${String(e.message).slice(0, 60)} — se sigue usando ${cachedBalance.toFixed(4)} SOL`, "warn");
        }
        return cachedBalance;
      }
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
const MIG_TZ_OFFSET = 2; // España respecto a UTC (verano). Ajustar a 1 en invierno.

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
      rechazadas: (state.rechazadas || []).slice(0, 300),   // [31-ago] con los veredictos manuales
      stats: state.stats,
      shadow: state.shadow,
      creatorHist: [...creatorHist.entries()],
      abyssCreators: [...abyssCreators],
      riskState: {
        dayKey: riskState.dayKey,
        dailyPnlSol: riskState.dailyPnlSol,
        consecutiveLosses: riskState.consecutiveLosses,
        pausedUntil: riskState.pausedUntil,
        recentCloses: riskState.recentCloses,   // [FIX 26-jul] la ventana móvil de 6h sobrevive al redeploy
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
    if (Array.isArray(saved.rechazadas)) state.rechazadas = saved.rechazadas;   // [31-ago] veredictos manuales
    if (saved.stats) state.stats = { ...state.stats, ...saved.stats };
    if (saved.shadow && process.env.SHADOW_RESET !== "true") {
      state.shadow = saved.shadow;
      // [v11.9] blindaje: nunca heredar un alta posterior al arranque (bug de redeploys encadenados)
      if (!state.shadow.alta || state.shadow.alta > Date.now()) state.shadow.alta = SHADOW_ALTA;
    }
    if (saved.creatorHist) for (const [k, v] of saved.creatorHist) creatorHist.set(k, v);
    if (saved.abyssCreators) { for (const w of saved.abyssCreators) abyssCreators.add(w);
      if (abyssCreators.size) addLog(`☠️ Lista negra de por vida: ${abyssCreators.size} creador(es) (${ABYSS_SEED.length} de semilla + ${abyssCreators.size - ABYSS_SEED.length} cazados)`, "info"); }
    if (saved.riskState) {
      riskState.dayKey = saved.riskState.dayKey ?? null;
      riskState.dailyPnlSol = saved.riskState.dailyPnlSol ?? 0;
      riskState.consecutiveLosses = saved.riskState.consecutiveLosses ?? 0;
      riskState.pausedUntil = saved.riskState.pausedUntil ?? 0;
      riskState.recentCloses = Array.isArray(saved.riskState.recentCloses) ? saved.riskState.recentCloses : [];   // [FIX 26-jul]
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
  if (state.liveRecordings.has(mint)) {   // LAB: grabación extendida activa
    const r0 = state.liveRecordings.get(mint);
    if (r0 && !r0._avisoDesus) { r0._avisoDesus = true;
      addLog(`🛡️ alguien intentó desuscribir ${r0.symbol} con la cámara viva — bloqueado (el fallo de las 292s)`, "info"); }
    return;
  }
  if (state.obsRecordings?.has?.(mint)) return;
  if (state.realTrades.some(t => t.mint === mint && (t.status === "OPEN" || t.status === "CLOSING"))) return;
  if (state.demoTrades.some(t => t.mint === mint && t.status === "OPEN")) return;   // [v11.9] demo vivos del mismo mint
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

// [2-sep] el panel refrescaba el saldo cada 30s aunque la caché dure 5min: eran llamadas
// inútiles que se comían el cupo y rebotaban con 500. Ahora va al mismo ritmo que la caché.
// En real, antes de comprar se pide con force=true, así que ahí el dato siempre es fresco.
setInterval(async () => {
  if (wallet) { state.stats.walletBalance = await getWalletBalance(); broadcast({ event: "stats", data: state.stats }); }
}, BALANCE_CACHE_MS);





// ════════════════════════════════════════════════════════════════
// ESTRATEGIA: SNIPER DE MIGRACIÓN
// ════════════════════════════════════════════════════════════════


// ── REGISTRO DE EDAD PRE-MIGRACION via HELIUS (7-jul) ──
// [CAMBIO 9-jul] Ahora además: (a) guarda el resultado en premigData para que el
// filtro de entrada por holders pueda usarlo, y (b) registra top5Pct y top10Pct
// (la concentración del top-5/top-10 delata al deployer con el supply repartido
// en varias wallets; el topPct del top-1 solo demostró no separar nada).
// [1-sep] supply real del token: 2.000 millones ⇒ Mayhem Mode. Una llamada por migración.
async function miraSupply(mint) {
  if (supplyCache.has(mint)) return supplyCache.get(mint);
  let r = { supply: null, mayhem: false };
  try {
    if (connection) {
      const s = await connection.getTokenSupply(new PublicKey(mint));
      const n = s?.value?.uiAmount;
      if (n > 0) r = { supply: n, mayhem: n > 1.5e9 };
    }
  } catch {}
  supplyCache.set(mint, r);
  if (supplyCache.size > 2000) supplyCache.delete(supplyCache.keys().next().value);
  return r;
}

async function registrarCalidadPremig(mint, symbol) {
  try {
    if (!connection) { addLog(`[PREMIG] sym=${symbol} mint=${mint} edad=sin-conexion`, "info"); return; }
    const pk = new PublicKey(mint);
    // [FIX 26-jul] 🌐 REDES SOCIALES del token (pump.fun): el estudio arXiv 2607.02823 (833K
    // lanzamientos) da Telegram ×9 en probabilidad de graduar. Se consulta EN PARALELO (no
    // retrasa nada) y alimenta solo los filtros-sombra: cero riesgo hasta que los datos hablen.
    const socP = (async () => {
      try {
        const r = await fetch(`https://frontend-api-v3.pump.fun/coins/${mint}`, {
          headers: { accept: "application/json" }, signal: AbortSignal.timeout(4000) });
        if (!r.ok) return null;
        const j = await r.json();
        if (!j) return null;
        const tg = !!j.telegram, tw = !!j.twitter, web = !!j.website;
        return { tg, tw, web, nSoc: (tg?1:0)+(tw?1:0)+(web?1:0) };
      } catch { return null; }
    })();
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
    // ── [v11.9] PREMIG v2: calidad de las billeteras top-5 (identidad, no precio) ──
    let hqStr = "";
    let tbMed = null, tbMin = null, newWn = null;   // [v11.9] guardados aparte, en número
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
        tbMed = +med; tbMin = +mn; newWn = newW;   // [v11.9]
        return ` topBalMed=${med} topBalMin=${mn} newW=${newW}/${owners.length} fundSpread=${spread}`;
      })();
      hqStr = await Promise.race([pack, new Promise(r => setTimeout(() => r(""), 10000))]);   // [FIX 26-jul] 3.5s→10s: con 3.5s el topBal llegaba en ~1/3 de las ops y el filtro no veía al resto; la entrada tarda 20-60s, así que hay margen de sobra
    } catch (e) { hqStr = ""; }
    const soc = await Promise.race([socP, new Promise(r => setTimeout(() => r(null), 100))]);   // [FIX 26-jul] ya debería estar resuelta (corrió en paralelo)
    // [FIX 27-jul] si pump.fun aún no respondió cuando el PREMIG acaba (tokens con PREMIG rápido),
    // no perder el dato: se parchea en premigData cuando llegue — las sombras lo leen 30 min después
    socP.then(s2 => { if (s2) { const pd0 = premigData.get(mint); if (pd0 && pd0.nSoc == null) { pd0.nSoc = s2.nSoc; pd0.tg = s2.tg; } } }).catch(() => {});
    const socStr = soc ? ` socials=${soc.nSoc}${soc.nSoc ? "(" + [soc.tg?"tg":null, soc.tw?"tw":null, soc.web?"web":null].filter(Boolean).join(",") + ")" : ""}` : "";
    premigData.set(mint, { ageMin, total, holders: holdersNum, topPct: topPctNum, top5Pct: top5Num, top10Pct: top10Num, creator, hq: hqStr.trim(),
      topBalMed: tbMed, topBalMin: tbMin, newW: newWn,
      nSoc: soc ? soc.nSoc : null, tg: soc ? soc.tg : null,
      tw: soc ? soc.tw : null, web: soc ? soc.web : null });   // [FIX 26-jul] redes · [5-ago] +tw/web para la puerta
    addLog(`[PREMIG] sym=${symbol} mint=${mint} edadMin=${ageMin} txsTotal=${total}${holdersStr}${creStr}${hqStr}${socStr}`, "info");
    labStats.premigOk++;
  } catch (e) {
    addLog(`[PREMIG] sym=${symbol} mint=${mint} edad=error ${String(e).slice(0,50)}`, "info");
    labStats.premigErr++;
  }
}

// [31-ago] aviso de RECHAZO al panel, con lo que se sabía del token al descartarlo
function migRechazada(entry, motivo) {
  try {
    if (!state.rechazadas) state.rechazadas = [];
    const data = {
      id: `rej-${entry.mint}-${Date.now()}`, mint: entry.mint, symbol: entry.symbol, name: entry.name,
      motivo, ts: Date.now(), dur: Math.round((Date.now() - entry.startTime) / 1000),
      mc: entry.migratedMcUsd || null, vol: Math.round(entry.volumeUSD || 0), trades: entry.tradeCount || 0,
      sig: entry.sigPct != null ? entry.sigPct : null, mov2s: entry.qualMov2s != null ? entry.qualMov2s : null,
      ultimo: (entry.firstPrice > 0 && entry.lastPrice > 0) ? +((entry.lastPrice / entry.firstPrice - 1) * 100).toFixed(1) : null,
      serie: entry.serie || [],
      veredicto: null, maxVisto: null,       // los rellena el humano desde el panel
    };
    state.rechazadas.unshift(data);
    if (state.rechazadas.length > 300) state.rechazadas.length = 300;
    broadcast({ event: "migRechazada", data });
  } catch {}
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
  miraSupply(coin.mint).then(s => {                        // [1-sep] ¿es un token Mayhem?
    const e = state.migWatching.get(coin.mint);
    if (!e) return;
    e.supply = s.supply; e.mayhem = s.mayhem;
    if (s.mayhem) addLog(`🤖 MAYHEM: ${e.symbol} tiene supply de ${Math.round(s.supply / 1e6)}M — el agente de pump.fun opera al azar 24h${MAYHEM_VETO ? " → DESCARTADA" : " (solo anotado)"}`, "warn");
  }).catch(() => {});
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
  // [31-ago] serie de precio de la vigilancia: sirve para ver en el panel qué hizo el token
  // desde que migró hasta que lo rechazamos (≤120 puntos, en % desde el primer tick)
  if (entry.firstPrice > 0) {
    if (!entry.serie) entry.serie = [];
    const tRel = Math.round((Date.now() - entry.startTime) / 1000);
    const pRel = +((price / entry.firstPrice - 1) * 100).toFixed(1);
    const ult = entry.serie[entry.serie.length - 1];
    if (!ult || ult[0] !== tRel) entry.serie.push([tRel, pRel]); else ult[1] = pRel;
    if (entry.serie.length > 120) entry.serie.splice(0, entry.serie.length - 120);
  }
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
    needed: elapsed < MIG_FAST_WINDOW_MS ? MIG_VOL_FAST_EFF : MIG_VOL_SLOW_EFF,   // [28-ago] usaban constantes borradas
    timeLeft: Math.max(0, MIG_WINDOW_MS - elapsed), mc: price * supplyDe(mint),
  }});
}

function migQualityGateThenOpen(entry, entryPriceB) {
  // Tope de MC de entrada: se aplica SIEMPRE, con o sin qual_gate.
  const mcEntryUsdPre = entryPriceB * supplyDe(entry.mint);   // [1-sep] supply real (Mayhem = 2.000M)
  if (mcEntryUsdPre > MIG_MAX_MC_ENTRY) {
    addLog(`🛑 MIG MC ALTO: ${entry.symbol} descartada | MC ${formatMC(mcEntryUsdPre)} > tope ${formatMC(MIG_MAX_MC_ENTRY)} (riesgo honeypot/pump inflado)`, "filter");
    migRechazada(entry, "MIG MC ALTO"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
    broadcast({ event: "stats", data: state.stats });
    return;
  }
  if (!MIG_QUAL_GATE) {
    migExamThenOpen(entry, entryPriceB); return;   // [FIX 28-jul]
  }
  entry.qualStartPrice = (entry.priceHist && entry.priceHist.length) ? entry.priceHist[0][1] : entryPriceB;
  entry.qualGate = true;
  addLog(`🔍 MIG CALIDAD: ${entry.symbol} — vigilando hasta ${(MIG_QUAL_MAX_WAIT_MS/60000).toFixed(0)}min, señal al 1er mov2s>+${MIG_QUAL_MOV2S_MIN}%`, "filter");
  entry.qualTimeout = setTimeout(() => {
    if (!entry.qualGate) return;
    entry.qualGate = false;
    addLog(`🚫 MIG FILTRO CALIDAD: ${entry.symbol} descartada | nunca dio mov2s>+${MIG_QUAL_MOV2S_MIN}% en ${(MIG_QUAL_MAX_WAIT_MS/60000).toFixed(0)}min`, "filter");
    migRechazada(entry, "MIG FILTRO CALIDAD"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
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
    // [FIX 29-jul] UNA-STRIKE: primera señal débil = fuera para siempre (no vuelve a observación)
    if (mov2 < MIG_QUAL_ONE_STRIKE) {
      addLog(`🚫 MIG UNA-STRIKE: ${entry.symbol} descartada | 1ª señal mov2s +${mov2.toFixed(1)}% < +${MIG_QUAL_ONE_STRIKE}% (débil = fuera, sin los 10 min de re-vigilancia)`, "filter");
      migRechazada(entry, "MIG UNA-STRIKE"); state.stats.mig_rejected++;
      state.migWatching.delete(entry.mint);
      unsubscribeToken(entry.mint);
      broadcast({ event: "stats", data: state.stats });
      return;
    }
    entry.sigPct = +mov2.toFixed(2);   // [31-jul] fuerza de la senal, para el MIGREC/MIGCLOSE
    const precioSenal = price;
    const tSenal = ((now - entry.startTime) / 1000).toFixed(0);
    entry.sigMov2s = +mov2.toFixed(2); entry.sigT = +tSenal;   // [v11.9] la señal causal, apuntada
    addLog(`🎯 MIG SEÑAL: ${entry.symbol} | mov2s +${mov2.toFixed(1)}% a los ${tSenal}s — confirmando ${(MIG_ENTRY_DELAY_MS/1000).toFixed(0)}s`, "accept");
    const confT0 = Date.now();
    setTimeout(() => {
      // [FIX 28-jul] frescura también en la confirmación: sin ticks en los 3s = feed mudo, no fiarse del precio congelado
      if (entry.entered) return;
      if (!entry.lastTickTs || entry.lastTickTs < confT0) {
        volverAlPortero(entry, entry.lastPrice || precioSenal, "🔇 confirmación sin ticks (feed mudo tras la señal)");
        return;
      }
      const precioB = entry.lastPrice;
      if (precioB < precioSenal * (1 - MIG_MAX_CAIDA_DELAY)) {
        const caida = ((precioB / precioSenal - 1) * 100).toFixed(1);
        addLog(`🚫 MIG ENTRADA ABORTADA: ${entry.symbol} cayó ${caida}% en la confirmación`, "filter");
        migRechazada(entry, "MIG ENTRADA ABORTADA"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      const mcEntryUsd = precioB * supplyDe(entry.mint);   // [1-sep] supply real (Mayhem = 2.000M)
      if (mcEntryUsd < MIG_MIN_MC_ENTRY) {
        addLog(`🛑 MIG MC BASURA: ${entry.symbol} descartada | MC ${formatMC(mcEntryUsd)} < mínimo ${formatMC(MIG_MIN_MC_ENTRY)} (token glitch/muerto, inejecutable en real)`, "filter");
        migRechazada(entry, "MIG MC BASURA"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      if (mcEntryUsd > MIG_MAX_MC_ENTRY) {
        addLog(`🛑 MIG MC ALTO: ${entry.symbol} descartada | MC ${formatMC(mcEntryUsd)} > tope ${formatMC(MIG_MAX_MC_ENTRY)}`, "filter");
        migRechazada(entry, "MIG MC ALTO"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      // [v11.1] VETO DE FÁBRICA: creador con 2+ malas con nosotros → ni tocarlo
      if (MIG_CREATOR_VETO) {
        const preV = premigData.get(entry.mint);
        const hC = preV && preV.creator ? creatorHist.get(preV.creator) : null;
        if (hC && hC.malas >= MIG_CREATOR_VETO_MALAS) {
          addLog(`🏭 MIG VETO FÁBRICA: ${entry.symbol} descartada | creador ${preV.creator.slice(0,8)}… con ${hC.tokens} tokens / ${hC.malas} malas con nosotros`, "filter");
          migRechazada(entry, "MIG VETO FÁBRICA"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
          broadcast({ event: "stats", data: state.stats }); return;
        }
      }
      // [v11.9] ☠️ VETO ABISMO: si el creador está en la lista negra de por vida, ni tocarlo
      if (MIG_ABYSS_VETO) {
        const preA = premigData.get(entry.mint);
        if (preA && preA.creator && abyssCreators.has(preA.creator)) {
          addLog(`☠️ MIG VETO ABISMO: ${entry.symbol} descartada | creador ${preA.creator.slice(0,8)}… en lista negra de por vida (pull previo ≤${MIG_ABYSS_PNL}%)`, "filter");
          migRechazada(entry, "MIG VETO ABISMO"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
          broadcast({ event: "stats", data: state.stats }); return;
        }
      }
      // [CAMBIO 9-jul] FILTRO HOLDERS (validado train +35.5 / test +20.3 mSOL/op).
      // Fail-open: si el PREMIG aún no respondió, se entra igualmente.
      const preD = premigData.get(entry.mint);
      if (preD && preD.holders !== null && preD.holders < MIG_MIN_HOLDERS) {
        addLog(`🚫 MIG HOLDERS: ${entry.symbol} descartada | holders=${preD.holders} < ${MIG_MIN_HOLDERS} (supply ultra-concentrado, perfil rug)`, "filter");
        migRechazada(entry, "MIG HOLDERS"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      // [FIX 26-jul] 💎 FILTRO topBal (mando del lab): billeteras top pobres = perfil rug
      if (MIG_MIN_TOPBAL > 0 && preD) {
        let tbF = preD.topBalMed;
        if (tbF == null && preD.hq) { const mT = preD.hq.match(/topBalMed=([\d.]+)/); if (mT) tbF = +mT[1]; }
        if (tbF != null && tbF < MIG_MIN_TOPBAL) {
          addLog(`💎 MIG topBal: ${entry.symbol} descartada | topBalMed=${tbF} SOL < ${MIG_MIN_TOPBAL} (billeteras top sin fondos, perfil rug)`, "filter");
          migRechazada(entry, "MIG topBal"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
          broadcast({ event: "stats", data: state.stats }); return;
        }
      }
      // [4-ago] ✈️ PUERTA DE TELEGRAM (mando del lab, fail-open: sin dato de redes, pasa)
      const redOK = MIG_RED_MODO === "off" || !preD || preD.tg == null
        || (MIG_RED_MODO === "tg" ? preD.tg === true : (preD.tg === true || preD.tw === true));
      if (!redOK) {
        addLog(`✈️ MIG SIN REDES [${MIG_RED_MODO}]: ${entry.symbol} descartada | tg=${preD.tg?1:0} tw=${preD.tw?1:0}`, "filter");
        migRechazada(entry, "MIG SIN REDES MIG_RED_MODO"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      migExamThenOpen(entry, precioB);   // [FIX 28-jul] el examen de 2s decide la entrada
    }, MIG_ENTRY_DELAY_MS);
  }
}

// [FIX 28-jul] devuelve un token al portero por lo que le quede de sus 10 minutos
function volverAlPortero(entry, precioRef, motivo) {
  const resto = MIG_QUAL_MAX_WAIT_MS - (Date.now() - entry.startTime);
  addLog(`↩️ ${entry.symbol}: ${motivo} — ${resto > 5000 ? "vuelve al portero (" + Math.round(resto/60000) + "min restantes)" : "sin tiempo, descartada"}`, "filter");
  if (resto <= 5000) {
    migRechazada(entry, "sin tiempo tras el examen: " + motivo); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
    broadcast({ event: "stats", data: state.stats });
    return;
  }
  entry.qualStartPrice = precioRef;
  entry.qualGate = true;
  entry.qualTimeout = setTimeout(() => {
    if (!entry.qualGate) return;
    entry.qualGate = false;
    addLog(`🚫 MIG FILTRO CALIDAD: ${entry.symbol} descartada | sin tirón limpio tras volver al portero`, "filter");
    migRechazada(entry, "MIG FILTRO CALIDAD"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
    broadcast({ event: "stats", data: state.stats });
  }, resto);
}

// [FIX 28-jul] 🎓 el examen de 2s antes de abrir. FAIL-CLOSED ante el silencio: si durante el
// examen no llegó NI UN tick fresco, no se entra (el precio congelado del pump es una mentira:
// caso 7DgDGg 28-jul, compró un precio que ya no existía y el 1er tick real fue -91.5%).
// [31-jul] precio de referencia: mediana de los ticks de los ultimos 3s (robusta a prints sueltos)
function migRefPrice(entry) {
  const h = entry.priceHist || [];
  const t0 = Date.now() - 3_000;
  const v = h.filter(x => x[0] >= t0).map(x => x[1]).sort((a, b) => a - b);
  if (v.length < 3) return null;
  return v[v.length >> 1];
}
function migExamThenOpen(entry, precioBase) {
  if (MAYHEM_VETO && entry.mayhem) {
    addLog(`🤖 MIG MAYHEM: ${entry.symbol} descartada | token con agente de IA operando al azar`, "filter");
    migRechazada(entry, "MIG MAYHEM"); state.stats.mig_rejected++;
    state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
    broadcast({ event: "stats", data: state.stats }); return;
  }
  if (!MIG_EX2S_ON) {
    entry.entered = true; state.stats.mig_entered++;
    state.migWatching.delete(entry.mint); entry.firstPrice = precioBase;
    addLog(`✅ MIG ENTRADA (calidad ✓): ${entry.symbol} @ MC ${formatMC(precioBase * 1_000_000_000)}`, "accept");
    migOpenTrades(entry);
    return;
  }
  const p0 = precioBase;
  const examT0 = Date.now();
  setTimeout(() => {
    if (entry.entered) return;
    // [FIX 28-jul] frescura obligatoria: al menos un tick DURANTE el examen
    if (!entry.lastTickTs || entry.lastTickTs < examT0) {
      volverAlPortero(entry, p0, "🔇 examen 2s sin ticks (feed mudo = rug o token muerto)");
      return;
    }
    const p2 = entry.lastPrice || p0;
    const mov = p0 > 0 ? (p2 / p0 - 1) * 100 : 0;
    entry.ex2s = +mov.toFixed(2);
    if (mov >= MIG_EX2S_MIN) {
      // [31-jul CcYp2Y] ANTI-SPIKE: no comprar un print de lavado. Vuelve al portero, no descarta.
      const ref = migRefPrice(entry);
      if (ref && p2 > ref * (1 + MIG_ENTRY_SPIKE_MAX / 100)) {
        volverAlPortero(entry, p2, `⚡ ENTRADA ANULADA (spike): tick ${(((p2 / ref) - 1) * 100).toFixed(0)}% sobre la mediana de 3s — print de lavado, no precio real`);
        return;
      }
      entry.entered = true; state.stats.mig_entered++;
      state.migWatching.delete(entry.mint); entry.firstPrice = p2;
      addLog(`✅ MIG ENTRADA (examen 2s ✓ ${mov >= 0 ? "+" : ""}${mov.toFixed(1)}%): ${entry.symbol} @ MC ${formatMC(p2 * 1_000_000_000)}`, "accept");
      migOpenTrades(entry);
      return;
    }
    // suspendido: NO se descarta — vuelve al portero por lo que quede de los 10 min
    volverAlPortero(entry, p2, `🎓 examen 2s suspendido: cayó ${mov.toFixed(1)}% (< ${MIG_EX2S_MIN}%)`);
  }, MIG_EX2S_MS);
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
        migRechazada(entry, "MIG ENTRADA ABORTADA"); state.stats.mig_rejected++; state.migWatching.delete(mint); unsubscribeToken(mint);
        broadcast({ event: "stats", data: state.stats }); return;
      }
      entry.pendingEntry = true;
      addLog(`✅ MIG LENTA confirmada: ${entry.symbol} @ MC ${formatMC(precioB * 1_000_000_000)}`, "accept");
      migQualityGateThenOpen(entry, precioB);
    }, MIG_ENTRY_DELAY_MS);
  } else {
    state.migWatching.delete(mint); unsubscribeToken(mint);
    addLog(`❌ MIG RECHAZADO: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol en ${elapsed}s`, "filter");
    migRechazada(entry, "MIG RECHAZADO"); state.stats.mig_rejected++; broadcast({ event: "stats", data: state.stats });
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
  const rec = { supply: entry.supply || null, mayhem: !!entry.mayhem, mint: entry.mint, symbol: entry.symbol, vel: +(velMs/1000).toFixed(1),
    mc: entry.migratedMcUsd || (entryPrice*1_000_000_000), vol: Math.round(entry.volumeUSD||0),
    t0: Date.now(), entryPrice, puntos: [{t:0,p:0}], lastSample: Date.now(), mov2s: null, sigMov2s: entry.sigMov2s ?? null, sigT: entry.sigT ?? null, ex2s: entry.ex2s ?? null, finished: false,
    volSeg: [], lastVolSec: -1, minP: 0, reentered: false,
    wallets: new Map() };  // [v10.1] wallet → {buyUsd, sellUsd, buys, sells} en 0-60s
  state.liveRecordings.set(entry.mint, rec);
  // [16-ago] TOPE DESDE EL MINUTO CERO. Antes el temporizador solo nacía en liveRecFinish (al cerrar
  // una op): si la op no cerraba nunca —y ya no hay expiración— la cámara grababa sin límite,
  // acumulando puntos y suscripción para siempre. Al cerrar se reprograma con la ventana correcta.
  rec._emitTimer = setTimeout(() => liveRecEmit(entry.mint, 'tope 6h'), MIG_HARD_MAX_MS);
  rec._ventanaMin = Math.round(MIG_HARD_MAX_MS / 60000);
  uniInit(rec, entryPrice);   // [23-ago] 🤝 UNIDA: pierna bot dentro desde la entrada
  espiaSuscribir(entry.mint);   // [30-ago] el espía mira el mismo token, sin operar
}

function liveRecSample(mint, price, volUSD = 0, trader = null, isBuy = false, fuente = "pp") {
  if (!LIVE_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished || price <= 0) return;
  if (fuente === "pp" && trader) { const c0 = espia.cuenta.get(mint); if (c0) { c0.portal++; c0.ultP = Date.now(); c0.sym = rec.symbol; } }   // solo ticks reales, no el rescate
  // [1-sep] con Helius de fuente principal, los ticks de PumpPortal se siguen contando (arriba)
  // para poder comparar, pero NO graban: si no, cada trade entraría dos veces en la curva.
  if (HELIUS_PRIMARIO && fuente === "pp") {
    // RED DE SEGURIDAD: si Helius lleva HELIUS_CAIDO_MS sin traer un solo swap de este token
    // (se cayó, perdió la suscripción, cambió la piscina...), PumpPortal vuelve a grabar. Sin
    // esto la cámara se quedaría congelada sin avisar y la unida gestionaría con precio viejo.
    const cH = espia.cuenta.get(mint);
    const ultimoH = cH ? (cH.ultH || 0) : 0;
    const heliusCaido = !ultimoH || Date.now() - ultimoH > HELIUS_CAIDO_MS;
    if (!heliusCaido) { rec.lastTickAt = Date.now(); return; }
    if (cH && !cH.avisoCaido) {
      cH.avisoCaido = true;
      addLog(`⚠️ HELIUS SIN DATOS en ${rec.symbol} (${Math.round((Date.now() - ultimoH) / 1000)}s) — la cámara vuelve a PumpPortal`, "warn");
    }
  } else if (fuente === "helius") {
    const cH = espia.cuenta.get(mint);
    if (cH && cH.avisoCaido) { cH.avisoCaido = false; addLog(`✅ Helius vuelve a dar datos en ${rec.symbol}`, "info"); }
  }
  rec.lastPrice = price; rec.lastTickAt = Date.now();   // [5-ago] último precio · [16-ago] y cuándo, para el vigilante
  // [16-ago] TOKEN MUERTO: por debajo de -90% desde la entrada ya no hay nada que observar.
  // Cerramos la cámara, liberamos la suscripción y dejamos de re-suscribir un cadáver.
  if (!rec.finished && rec.entryPrice > 0 && price <= rec.entryPrice * (1 - MUERTO_PCT / 100)) {
    const caida = ((price / rec.entryPrice - 1) * 100).toFixed(1);
    addLog(`⚰️ TOKEN MUERTO: ${rec.symbol} a ${caida}% de la entrada — cámara cerrada · precio=${price.toPrecision(4)} entrada=${rec.entryPrice.toPrecision(4)} fuente=${fuente}`, "warn");
    liveRecEmit(mint, 'muerto -90%');
    return;
  }
  // [5-ago] seguimiento post-cierre: cuánto se movió el token DESDE que vendimos
  if (rec.post && rec.post.salida > 0) {
    const p = rec.post, desde = (price / p.salida - 1) * 100;
    p.ultimo = +desde.toFixed(1);
    if (desde > p.maxDespues) p.maxDespues = +desde.toFixed(1);
    if (Date.now() - p.lastEmit > 15_000) {
      p.lastEmit = Date.now();
      broadcast({ event: "postCierre", data: { mint, tradeId: p.tradeId, symbol: p.symbol,
        desde: p.ultimo, max: p.maxDespues, mcAhora: price * 1_000_000_000,
        min: Math.round((Date.now() - p.t0) / 60000) } });
    }
  }
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
  // [4-ago] ESCALERA DE MUESTREO. El bot decide cada 1s (MIG_DECIDE_MS) durante TODA la vida de la op;
  // si la cámara graba cada 5s, el lab no puede reproducir esas decisiones y diverge justo donde ahora
  // vive la estrategia (sin expiración, las ops duran horas). Densificamos, con tope de puntos para
  // que la línea de log no se dispare.
  const interval = cadenciaMs(dt, rec.pts ? rec.pts.length : 0);
  if (Date.now() - rec.lastSample < interval) return;
  rec.lastSample = Date.now();
  rec.sampleTick = rec.lastSample;   // [4-ago] sello: en ESTE tick sí hay muestra → el motor decide aquí
  const pct = +((price - rec.entryPrice) / rec.entryPrice * 100).toFixed(2);
  rec.puntos.push({ t: Math.round(dt/1000), p: pct });
  if (rec.mov2s === null && dt >= 2000) rec.mov2s = pct;
  if (pct < rec.minP) rec.minP = pct;

  // [30-ago] FLUJO en tramos de 30s: volumen, compras, ventas y carteras distintas.
  // Con solo precio no se puede distinguir "plano porque no opera nadie" de "plano con
  // alguien acumulando", que es lo que hace falta para juzgar los suelos largos.
  {
    const seg = Math.round(dt / 1000), cubo = Math.floor(seg / 30) * 30;
    if (!rec.flujo) rec.flujo = [];
    let fx = rec.flujo[rec.flujo.length - 1];
    if (!fx || fx.t !== cubo) { fx = { t: cubo, v: 0, c: 0, s: 0, w: new Set() }; rec.flujo.push(fx); }
    // OJO: los precios que pone el relevo de Helius (o el rescate) llegan sin 'trader'. No son
    // trades: si los contáramos, cada uno sumaría una VENTA falsa y ensuciaría justo el dato
    // que sirve para detectar acumulación. Solo cuentan los ticks con cartera.
    if (trader) {
      fx.v += volUSD || 0;
      if (isBuy) fx.c++; else fx.s++;
      fx.w.add(trader);
    }
    if (rec.flujo.length > 480) rec.flujo.shift();   // tope: 4 horas
  }
  uniSample(rec, price, pct, Math.round(dt/1000));     // [23-ago] 🤝 UNIDA: decide en cadencia de muestra
}



// ── [23-ago] 🤝 UNIDA: motor (réplica 1:1 de labSimUnida). Estado por grabación en rec.uni. ──
// Las curvas del lab SON los puntos que muestrea liveRecSample, así que decidir aquí con los
// mismos pct redondeados garantiza que server y analizador vean la misma película.
const uniF = (v) => 1 + v / 100;   // % → ratio (el f() del lab)

function uniAbiertas(id) {   // [27-ago] cuenta TOKENS distintos, no trades (cada op abre hasta 2)
  const mints = new Set();
  for (const t of state.demoTrades) if (t.strategy === id && t.status === "OPEN") mints.add(t.mint);
  return mints.size;
}

function uniOpenTrade(rec, price, fase, id) {
  const trade = {
    id: `${id}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
    strategy: id, mint: rec.mint, symbol: rec.symbol, name: rec.symbol,
    entryPrice: price, tp: +(price * 1e9).toFixed(12),   // sin TP: cierra el motor
    sl: 0,                                               // informativo: el stop vive en rec.uni
    sizeSol: UNI_SIZE,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: fase, status: "OPEN",
    expiresAt: Date.now() + MIG_HARD_MAX_MS, mov1s: null, mov2s: rec.mov2s ?? null,
    velSeg: rec.vel ?? null,
    mcEntry: rec.mc && rec.entryPrice ? +(rec.mc * (price / rec.entryPrice)).toFixed(0) : null,   // [23-ago] MC real al precio de esta entrada
  };
  state.demoTrades.unshift(trade);
  if (state.demoTrades.length > 500) state.demoTrades.pop();
  state.stats.demoOpen++;
  broadcast({ event: "newDemoTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  return trade;
}

function uniCierra(rec, trade, price, reason) {
  trade.mov2s = rec.mov2s ?? trade.mov2s;   // para la línea MIGCLOSE (strat=unida, parseable como las demás)
  trade.closeReason = reason;
  trade.spark = curvaMini(rec.mint);        // [27-ago] curva para el minigráfico de la tarjeta
  trade.calidad = calidadEntrada(rec.mint); // wash / compradores / concentración               // [23-ago] el panel muestra el motivo en la línea de tiempo
  closeDemoTrade(trade, price, reason, 21);
}

function uniInit(rec, entryPrice) {
  if (!UNI_ON || OBSERVER_MODE) return;
  rec.uni = [];
  for (const cfg of UNI_VARIANTES()) {
    if (uniAbiertas(cfg.id) >= UNI_MAX_OPEN) { addLog(`🤝 ${cfg.id} saltada (límite ${UNI_MAX_OPEN} abiertas): ${rec.symbol}`, "filter"); continue; }
    // estado = labSimUnida con el primer punto (t=0, p=0) ya consumido:
    rec.uni.push({
      cfg,
      dir: 0, ref: { t: 0, p: 0 }, prev: 0, mins: [],   // pivotes causales (los máximos no se usan para comprar)
      envLog: 0, envT: 0,                                // log-envolvente de máximos (labEnvUp): arranca en log(f(0))=0
      bSl: uniF(cfg.bot.sl), bFol: false, bMax: 0, bAbierta: true, mixOn: false,
      lotes: [], sumInv: 0, tUlt: 0, vendidos: 0, muertoIni: null, vetado: false,
      tradeBot: uniOpenTrade(rec, entryPrice, "UNI_BOT", cfg.id),
      tradePack: null,
    });
  }
  if (!rec.uni.length) { rec.uni = null; return; }
  const b = UNI1.bot;
  addLog(`🤝 UNIDA [demo]: ${rec.symbol} | ${rec.uni.length} variante(s) · pierna bot dentro (stop ${b.sl}% · ×${b.mult} · arma +${b.arm} · ND ${b.ndT ? b.ndT + "s/+" + b.ndM + "%" : "off"})`, "accept");
}

function uniSample(rec, price, pct, tSec) {
  if (!rec.uni || !rec.uni.length) return;
  for (const u of rec.uni) uniSampleUno(rec, u, price, pct, tSec);
  if (!rec.uni.some(u => u)) rec.uni = null;
}

function uniSampleUno(rec, u, price, pct, tSec) {
  if (!u || u.muerto) return;
  const C = u.cfg;
  const t = tSec, v = pct;
  // 1) envolvente roja: el valor que DECIDE este tick es el PREVIO a integrar el punto
  //    (labEnvUp asigna A[q]=exp(U) antes de actualizar U con el punto q)
  const upRatio = Math.exp(u.envLog);
  const dtE = Math.max(0.001, t - u.envT); u.envT = t;
  const lp = Math.log(uniF(v));
  if (lp > u.envLog) u.envLog = lp; else u.envLog += (1 - Math.exp(-dtE / C.tau)) * (lp - u.envLog);
  // 2) pivotes causales (siempre mirando, también con la pierna bot dentro — igual que el lab)
  const cg = (uniF(v) / uniF(u.ref.p) - 1) * 100;
  if (u.dir >= 0 && cg <= -C.giro) { u.dir = -1; u.ref = { t, p: v }; }
  else if (u.dir <= 0 && cg >= C.giro) { u.mins.push({ ...u.ref }); u.dir = 1; u.ref = { t, p: v }; }
  else if ((u.dir >= 0 && v > u.ref.p) || (u.dir < 0 && v < u.ref.p)) u.ref = { t, p: v };
  // [23-ago] cronómetro del token muerto: cuenta desde CUALQUIER tick bajo el umbral
  if (C.muertoPct > 0) { if (v <= -C.muertoPct) { if (u.muertoIni == null) u.muertoIni = t; } else u.muertoIni = null; }
  // 3) pierna del bot (tornillos UNI_BOT, independientes de la migración Base)
  if (u.bAbierta) {
    const tb = u.tradeBot;
    tb.currentPct = +v.toFixed(2);
    if (v > tb.maxGainPct) tb.maxGainPct = v;
    if (v < tb.maxLossPct) tb.maxLossPct = v;
    const ra = uniF(v);
    if (v > u.bMax) u.bMax = v;
    if (!u.bFol && ra >= 1 + C.bot.arm / 100) u.bFol = true;
    if (u.bFol) {
      const b2 = u.bMax >= 100 ? 0.08 : u.bMax >= 60 ? 0.12 : u.bMax >= 40 ? 0.15 : 0.20;
      const anch = Math.min(0.90, b2 * C.bot.mult);
      const cand = ra * (1 - anch); if (cand > u.bSl) u.bSl = cand;
      if (u.bMax >= C.bot.arm && 1 + C.bot.p1 / 100 > u.bSl) u.bSl = 1 + C.bot.p1 / 100;
      if (u.bMax >= 100 && 1 + C.bot.p2 / 100 > u.bSl) u.bSl = 1 + C.bot.p2 / 100;
    }
    if (C.bot.ndT && t >= C.bot.ndT && u.bMax < C.bot.ndM) {
      u.bAbierta = false;
      uniCierra(rec, tb, price, "NO_LAUNCH");
      if (C.bot.relNL) u.mixOn = true;   // apagado en la config decidida: tras NO_LAUNCH no hay relevo
    } else if (ra <= u.bSl) {
      u.bAbierta = false; u.mixOn = true;
      uniCierra(rec, tb, price, "SL");
      addLog(`🤝 UNIDA: ${rec.symbol} pierna bot fuera por SL a ${v.toFixed(0)}% → relevo armado (soporte giro ${C.giro}% -${C.md}% · roja ${C.tau}s +${C.mv}%)`, "info");
    } else {
      const ahoraB = Date.now();
      const conCurva = !u.ultLatidoBot || ahoraB - u.ultLatidoBot >= UNI_LATIDO_MS;
      if (conCurva) u.ultLatidoBot = ahoraB;
      broadcast({ event: "demoTradeUpdate", data: { id: tb.id, currentPct: tb.currentPct, maxGainPct: tb.maxGainPct,
        maxLossPct: tb.maxLossPct, sl: 0, slPct: +((u.bSl - 1) * 100).toFixed(1), trailingPhase: tb.trailingPhase,
        ...(conCurva ? { spark: curvaMini(rec.mint) } : {}) } });
    }
    u.prev = v; return;   // un solo turno: mientras el bot está dentro, el relevo no compra (igual que el lab)
  }
  // 4) [23-ago] token muerto confirmado: M segundos seguidos bajo el umbral → liquidar y abandonar
  if (C.muertoPct > 0 && u.muertoIni != null && t - u.muertoIni >= C.muertoS) {
    if (u.tradePack && u.tradePack.status === "OPEN") {
      addLog(`🤝 UNIDA abandona token muerto: ${rec.symbol} lleva ${t - u.muertoIni}s bajo -${C.muertoPct}% → liquida paquete a ${v.toFixed(0)}%`, "info");
      uniCierra(rec, u.tradePack, price, "MUERTO");
    }
    u.muerto = true;   // esta variante abandona el token (la otra sigue)
    return;
  }
  // 5) relevo mixta (solo si mixOn: la pierna bot salió por SL)
  if (u.mixOn) {
    let hecho = false;
    const solEnJuego = state.demoTrades.reduce((s, t) => s + (t.strategy === "unida" && t.status === "OPEN" ? (t.sizeSol || 0) : 0), 0);
    const dentroDePlazo = !C.plazoMin || t <= C.plazoMin * 60;   // [29-ago] ⏳ deja de comprar tarde
    if (u.lotes.length < C.maxlot && u.vendidos < C.maxvend && u.mins.length >= 2 && dentroDePlazo && solEnJuego + UNI_SIZE <= UNI_MAX_SOL) {
      const A = u.mins[u.mins.length - 2], B = u.mins[u.mins.length - 1];
      const dtN = B.t - A.t;
      const soporte = dtN ? A.p + (B.p - A.p) * (t - A.t) / dtN : B.p;   // niv() del lab
      const nn = (uniF(soporte) * uniF(-C.md) - 1) * 100;              // aj() del lab
      if (u.prev > nn && v <= nn && v > -C.rug) {
        u.lotes.push(v); u.sumInv += 1 / price; u.tUlt = t; hecho = true;
        if (!u.tradePack && C.washMax > 0) {
          // [27-ago] la variante con filtro solo despliega el relevo en tokens con poco
          // wash del primer minuto (si aún no ha pasado 1 min, se usa lo acumulado).
          const q = calidadEntrada(rec.mint);
          // [1-sep] anotar el wash que se veía EN ESE INSTANTE. El laboratorio solo tenía el del
          // minuto completo y lo estimaba como wash60×(t/60); con tokens muy sucios esa estimación
          // se separaba del recuento real y el cuadre fallaba (9zgcyrfS: real bajo, estimado 119).
          if (q && rec.washCompra == null) { rec.washCompra = q.wash; rec.washCompraT = t; }
          if (q && q.wash > C.washMax) {
            u.vetado = true; u.mixOn = false;
            addLog(`🧼 ${C.id}: ${rec.symbol} descartada por wash ${q.wash} > ${C.washMax}`, "filter");
            u.prev = v; return;
          }
        }
        if (!u.tradePack) {
          u.tradePack = uniOpenTrade(rec, price, "UNI_RELEVO", C.id);
          u.tradePack.compras = [{ t, p: +v.toFixed(1) }];   // [23-ago] desglose para el detalle del panel
          addLog(`🤝 UNIDA compra 1/${C.maxlot}: ${rec.symbol} a ${v.toFixed(0)}% (soporte ${nn.toFixed(0)}%)`, "accept");
        } else {
          const pk = u.tradePack;
          const oldE = pk.entryPrice;
          pk.entryPrice = u.lotes.length / u.sumInv;   // media ARMÓNICA: lotes de SOL iguales → mismo total que el lab
          pk.sizeSol = +(UNI_SIZE * u.lotes.length).toFixed(2);
          pk.trailingPhase = `UNI_RELEVO×${u.lotes.length}`;
          pk.compras.push({ t, p: +v.toFixed(1) });   // [23-ago] desglose para el detalle del panel
          // [23-ago] la media cambió → re-basar TODO sobre la nueva media (mismos precios, otra referencia):
          const reb = oldE / pk.entryPrice;
          pk.maxGainPct = +(((1 + pk.maxGainPct / 100) * reb - 1) * 100).toFixed(2);
          pk.maxLossPct = +(((1 + pk.maxLossPct / 100) * reb - 1) * 100).toFixed(2);
          pk.currentPct = +((price - pk.entryPrice) / pk.entryPrice * 100).toFixed(2);
          if (pk.currentPct < pk.maxLossPct) pk.maxLossPct = pk.currentPct;
          pk.mcEntry = rec.mc && rec.entryPrice ? +(rec.mc * (pk.entryPrice / rec.entryPrice)).toFixed(0) : pk.mcEntry;
          broadcast({ event: "demoTradeUpdate", data: { id: pk.id, currentPct: pk.currentPct, maxGainPct: pk.maxGainPct, maxLossPct: pk.maxLossPct, sl: 0, slPct: 0, trailingPhase: pk.trailingPhase, sizeSol: pk.sizeSol, entryPrice: pk.entryPrice, mcEntry: pk.mcEntry, compras: pk.compras, spark: curvaMini(rec.mint) } });
          addLog(`🤝 UNIDA compra ${u.lotes.length}/${C.maxlot}: ${rec.symbol} a ${v.toFixed(0)}% (paquete promediado)`, "accept");
        }
      }
    }
    if (!hecho && u.lotes.length && t > u.tUlt) {
      const up = (upRatio * uniF(C.mv) - 1) * 100;
      // [29-ago] 🎯 objetivo: si el paquete alcanza +tp% sobre SU media, cobra entero.
      // Validado en 3 ventanas con las dos configuraciones (mejora en todas).
      const mediaR = u.lotes.length / u.sumInv;
      const ganPk = (price / mediaR - 1) * 100;
      const porTP = C.tp > 0 && ganPk >= C.tp;
      if (porTP || (u.prev < up && v >= up)) {
        // [29-ago] en la roja se puede vender solo una PARTE del paquete y dejar correr el resto:
        // medido sobre 292 ops, el 31% de las ventas en la roja se dejaban +100% por delante.
        // El objetivo (TP) sí cobra siempre el paquete entero.
        const frac = porTP ? 1 : (C.rojaFrac > 0 && C.rojaFrac < 1 ? C.rojaFrac : 1);
        const nVende = frac >= 1 ? u.lotes.length : Math.max(1, Math.round(u.lotes.length * frac));
        const parcial = nVende < u.lotes.length;
        u.vendidos += nVende;
        addLog(porTP
          ? `🎯 ${C.id}: ${rec.symbol} OBJETIVO +${C.tp}% alcanzado (paquete a +${ganPk.toFixed(0)}%) → cobra ×${nVende}`
          : `🔴 ${C.id} vende ${parcial ? "MEDIO paquete" : "paquete"} ×${nVende}: ${rec.symbol} a ${v.toFixed(0)}% (roja ${up.toFixed(0)}%)${parcial ? ` · siguen ${u.lotes.length - nVende} lotes` : ""} | vendidos ${u.vendidos}/${C.maxvend}`, porTP ? "accept" : "info");
        if (!parcial) {
          uniCierra(rec, u.tradePack, price, porTP ? "TP" : "ROJA");
          u.tradePack = null; u.lotes = []; u.sumInv = 0;
        } else {
          // cobra media posición: cierra un trade por la parte vendida y deja el paquete vivo
          // OJO: sumInv acumula precios ABSOLUTOS (1/price), no ratios. Mantener las mismas
          // unidades aquí es imprescindible: con ratios la media quedaría dividida por el
          // precio de entrada del token y el objetivo saltaría cuando no debe.
          const abs = (pct) => rec.entryPrice * uniF(pct);
          const compradas = u.tradePack.compras || [];
          const vendidosLotes = u.lotes.slice(0, nVende);
          const quedan = u.lotes.slice(nVende);
          let invV = 0; for (const l of vendidosLotes) invV += 1 / abs(l);
          const pkV = u.tradePack;
          pkV.entryPrice = vendidosLotes.length / invV;
          pkV.sizeSol = +(UNI_SIZE * nVende).toFixed(2);
          pkV.compras = compradas.slice(0, nVende);
          uniCierra(rec, pkV, price, "ROJA_MEDIA");
          // el resto sigue en un paquete nuevo, con su media re-basada
          u.lotes = quedan;
          u.sumInv = 0; for (const l of quedan) u.sumInv += 1 / abs(l);
          const mediaQ = quedan.length / u.sumInv;
          u.tradePack = uniOpenTrade(rec, mediaQ, `UNI_RELEVO×${quedan.length}`, C.id);
          u.tradePack.compras = compradas.slice(nVende);
          u.tradePack.sizeSol = +(UNI_SIZE * quedan.length).toFixed(2);
          u.tradePack.mcEntry = rec.mc && rec.entryPrice ? +(rec.mc * (mediaQ / rec.entryPrice)).toFixed(0) : null;
        }
      }
    }
    if (u.tradePack) {
      const pk = u.tradePack;
      pk.currentPct = +((price - pk.entryPrice) / pk.entryPrice * 100).toFixed(2);
      if (pk.currentPct > pk.maxGainPct) pk.maxGainPct = pk.currentPct;
      if (pk.currentPct < pk.maxLossPct) pk.maxLossPct = pk.currentPct;
      // [28-ago] BUG: el paquete solo informaba al comprar, así que en el panel se quedaba
      // congelado (curva parada y "+0.0%") mientras no hubiera lotes nuevos. Ahora late.
      const ahora = Date.now();
      if (!u.ultLatido || ahora - u.ultLatido >= UNI_LATIDO_MS) {
        u.ultLatido = ahora;
        broadcast({ event: "demoTradeUpdate", data: { id: pk.id, currentPct: pk.currentPct,
          maxGainPct: pk.maxGainPct, maxLossPct: pk.maxLossPct, sl: 0, slPct: 0,
          trailingPhase: pk.trailingPhase, sizeSol: pk.sizeSol, entryPrice: pk.entryPrice,
          mcEntry: pk.mcEntry, compras: pk.compras, spark: curvaMini(rec.mint) } });
      }
    }
  }
  u.prev = v;
}

function uniFinish(rec) {
  if (!rec.uni || !rec.uni.length) return;
  for (const u of rec.uni) uniFinishUno(rec, u);
  rec.uni = null;
}

function uniFinishUno(rec, u) {
  if (!u) return;
  const last = rec.puntos.length ? rec.puntos[rec.puntos.length - 1].p : 0;
  const price = rec.entryPrice * uniF(last);
  if (u.bAbierta && u.tradeBot && u.tradeBot.status === "OPEN") { u.bAbierta = false; uniCierra(rec, u.tradeBot, price, "EXPIRED"); }
  if (u.tradePack && u.tradePack.status === "OPEN") uniCierra(rec, u.tradePack, price, "EXPIRED");
}



// [CAMBIO 9-jul] Grabación extendida 10 → 30 MINUTOS: necesitamos curvas de la
// región 10-30 min para backtestear el moon-bag y la ventana larga con datos
// reales (los topes del Excel demostraron que ahí vive el recorrido grande).
const LAB_EXTEND_MS = 120 * 60_000;   // [16-ago] 60→120min: sin expiración las ops duran más y los cohetes despegan tarde   // [FIX 27-jul] 30→60 min: la cámara ahora cubre la vida completa de la op (MIG_DURATION=60min); un 19% de las ops seguían vivas al apagarse la grabación y su final quedaba fuera de plano para el lab y el torneo
// LAB: contadores de salud del experimento (volcados cada hora)
const labStats = { premigOk: 0, premigErr: 0, migrecs: 0, inicio: Date.now() };
setInterval(() => {
  const h = ((Date.now() - labStats.inicio) / 3600000).toFixed(1);
  addLog(`[LAB-SALUD] ${h}h de lab | migraciones=${state.stats.mig_migrations} entradas=${state.stats.mig_entered} MIGRECs=${labStats.migrecs} PREMIG ok=${labStats.premigOk} err=${labStats.premigErr}${labStats.premigErr > labStats.premigOk ? " ⚠️ HELIUS FALLANDO" : ""} | feed: ${Math.round((Date.now()-feedLastMigAt)/60000)}min sin migracion, reconexiones=${feedReconexiones} | subs=${state.liveRecordings.size + state.migWatching.size + state.migMonitored.size} camaras=${state.liveRecordings.size} mudas=${[...state.liveRecordings.values()].filter(r => !r.finished && Date.now() - (r.lastTickAt || r.t0) > 90000).length}`, "info");
}, 3600_000);

// ═══════════════ [v11.9] TORNEO DE SOMBRAS ═══════════════
// Cada grabación terminada se re-juega contra K configs con el motor de replay (clavado a los
// simuladores). Observador puro: no toca decisiones. Fuera-de-muestra de serie: cada config
// solo se juzga sobre ops nacidas tras su alta.
// [16-ago] TORNEO DE SOMBRAS APAGADO por decisión propia.
// Evaluaba ~25 configuraciones alternativas sobre cada grabación. Se apaga porque (a) competía
// con el motor real usando el sofá viejo como patrón, así que su ranking ya no representaba lo
// que corre, y (b) con dinero real conviene que el server haga una sola cosa y la haga bien.
// El código se conserva intacto: poner SHADOW_ON=true en el entorno lo reactiva sin tocar nada.
const SHADOW_ON = process.env.SHADOW_ON === "true";
// [v11.9] alta del torneo anclada al ARRANQUE del proceso (no a la 1ª grabación, que llega ~50min
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
  // [v11.9] vecinos de UN tornillo (exploración disciplinada, nunca combinatoria libre)
  { id:"sofa·SL-35",  cfg: { ...SH_SOFA, sl:-35 } },
  { id:"sofa·SL-42",  cfg: { ...SH_SOFA, sl:-42 } },
  { id:"sofa·x5.5",   cfg: { ...SH_SOFA, mult:5.5 } },
  { id:"sofa·x7",     cfg: { ...SH_SOFA, mult:7.0 } },
  { id:"sofa·reArm45",cfg: { ...SH_SOFA, re:{ ...SH_SOFA.re, arm:45 } } },
  { id:"sofa·reArm55",cfg: { ...SH_SOFA, re:{ ...SH_SOFA.re, arm:55 } } },
  // [v11.9] FILTROS-SOMBRA: los tribunales pendientes como instrumentos permanentes
  { id:"sofa·vel<4.7",cfg: { ...SH_SOFA, fVelMax:4.7 } },
  { id:"sofa·tps<26", cfg: { ...SH_SOFA, fTpsMax:26 } },
  { id:"sofa·sig≥1",  cfg: { ...SH_SOFA, fSigMin:1.0 } },
  { id:"sofa·sin20h", cfg: { ...SH_SOFA, fSin20:true } },
  // [v11.9] CORTACIRCUITOS: pausa tras racha de rugs. Nace del 22-jul (40% rugs, 6 sigmas
  // sobre lo normal). El retro daba +35 SOL ese día y -2.85 en un día normal: asimetría
  // prometedora pero SIN validar (solo 4 días de cierres). Aquí compite sin arriesgar nada.
  // [v11.9] 💎 el hallazgo del 23-jul: el saldo de las billeteras top separa rugs de cohetes
  { id:"sofa·topBal≥1", cfg: { ...SH_SOFA, fTopBal:1 } },
  { id:"sofa·topBal≥5", cfg: { ...SH_SOFA, fTopBal:5 } },
  { id:"sofa·topBal≥2", cfg: { ...SH_SOFA, fTopBal:2 } },
  // [FIX 26-jul] 🌐 redes: ¿"tiene socials" separa rugs de cohetes en NUESTROS datos? (arXiv: Telegram ×9 en graduación)
  { id:"sofa·redes≥1",  cfg: { ...SH_SOFA, fSocMin:1 } },
  { id:"sofa·tgON",     cfg: { ...SH_SOFA, fTg:true } },
  { id:"sofa+CB5/60", cfg: { ...SH_SOFA, cb:{ n:5, win:20, pausa:60 } } },
  { id:"sofa+CB5/30", cfg: { ...SH_SOFA, cb:{ n:5, win:20, pausa:30 } } },
  { id:"sofa+CB7/60", cfg: { ...SH_SOFA, cb:{ n:7, win:20, pausa:60 } } },
];
function shFiltrada(rec, cfg){
  // [v11.9] 💎 topBal: el saldo mediano de las 5 billeteras top. Tribunal del 23-jul sobre
  // 367 ops: train -5→+24 mSOL/op, 3/3 días mejoran, p=0.000 contra 2000 muestras al azar,
  // y aguanta sin las 3 mejores. Fail-open: si el PREMIG no respondió, la op pasa.
  if (cfg.fTopBal != null){
    const pd = premigData.get(rec.mint);
    let tb = pd ? pd.topBalMed : null;
    if (tb == null && pd && pd.hq){ const mm = pd.hq.match(/topBalMed=([\d.]+)/); if (mm) tb = +mm[1]; }
    if (tb != null && tb < cfg.fTopBal) return true;
  }
  if (cfg.fVelMax && rec.vel != null && rec.vel >= cfg.fVelMax) return true;
  // [FIX 26-jul] 🌐 filtros de redes: fail-open (sin dato = pasa), igual que el resto
  if (cfg.fSocMin != null){
    const pd = premigData.get(rec.mint);
    if (pd && pd.nSoc != null && pd.nSoc < cfg.fSocMin) return true;
  }
  if (cfg.fTg){
    const pd = premigData.get(rec.mint);
    if (pd && pd.tg === false) return true;
  }
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
  const pts = rec.puntos; if(!pts || pts.length < 2) return;   // [v11.9] antes 5: descartaba baja liquidez
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
    // [v11.9] CORTACIRCUITOS: si esta sombra está en pausa por racha de rugs, se salta la op
    if (c.cfg.cb){
      const Lc = S.libretas[c.id] || (S.libretas[c.id]={n:0,neto:0,w:0,porDia:{}});
      if (Lc.cbHasta && rec.t0 < Lc.cbHasta) { Lc.skip = (Lc.skip||0)+1; continue; }
    }
    const m = shMig(pts, c.cfg, 0);
    let neto = SHADOW_POS*(m.pnl-SHADOW_FEE)/100;
    const re = shRe(pts, c.cfg.re); if(re!==null) neto += SHADOW_POS*(re-SHADOW_FEE)/100;
    const fz = shFz(pts, c.cfg.fz, m.exitT); if(fz!==null) neto += SHADOW_POS*(fz-SHADOW_FEE)/100;
    const L = S.libretas[c.id] || (S.libretas[c.id]={n:0,neto:0,w:0,porDia:{}});
    L.n++; L.neto+=neto; if(neto>0)L.w++;
    L.porDia[fecha]=(L.porDia[fecha]||0)+neto;
    L.top=(L.top||[]); L.top.push(neto); L.top.sort((a,b)=>b-a); if(L.top.length>3)L.top.length=3;
    // [v11.9] ventana del cortacircuitos: mira SU propio historial reciente
    if (c.cfg.cb){
      L.cbWin = L.cbWin || [];
      L.cbWin.push(+m.pnl.toFixed(1));
      if (L.cbWin.length > c.cfg.cb.win) L.cbWin.shift();
      if (L.cbWin.length >= c.cfg.cb.win && L.cbWin.filter(x=>x<=-80).length >= c.cfg.cb.n){
        L.cbHasta = rec.t0 + c.cfg.cb.pausa*60000;
        L.cbWin = [];
        L.cbTrig = (L.cbTrig||0)+1;
      }
    }
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
  broadcast({ event: "shadow", data: S });   // [v11.9] el panel pinta el torneo en vivo
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

// [16-ago] VIGILANTE DE SUSCRIPCIÓN POR TOKEN.
// Caso real (5629xdj, 15-ago): la cámara corrió sus 60min pero los ticks se cortaron a los 215s
// y justo después el token hizo +363% sin que lo viéramos. El socket global seguía vivo (llegaban
// otras migraciones), así que el vigilante de feed no saltaba: era ESE mint el que dejó de emitir.
// Cada minuto revisamos las grabaciones vivas y re-suscribimos las que llevan >90s mudas.
setInterval(() => {
  if (!LIVE_RECORD || pumpPortalWs?.readyState !== WebSocket.OPEN) return;
  const mudos = [];
  for (const [mint, rec] of state.liveRecordings) {
    if (rec.finished) continue;
    // [16-ago] no re-suscribir cadáveres: si el último precio visto ya está por debajo del
    // umbral de muerte, o llevamos muchos reintentos sin éxito, se cierra la cámara.
    if (rec.lastPrice && rec.entryPrice > 0 && rec.lastPrice <= rec.entryPrice * (1 - MUERTO_PCT / 100)) {
      addLog(`⚰️ TOKEN MUERTO (mudo): ${rec.symbol} — cámara cerrada, no se re-suscribe`, "info");
      liveRecEmit(mint, 'muerto mudo'); continue;
    }
    const ultimo = rec.lastTickAt || rec.t0;
    if (Date.now() - ultimo > 90_000) {
      if ((rec.resubs || 0) >= 5) {   // 5 intentos sin recuperar ticks: darlo por perdido
        addLog(`🔇 SIN TICKS: ${rec.symbol} tras ${rec.resubs} re-suscripciones — cámara cerrada`, "warn");
        liveRecEmit(mint, 'sin ticks'); continue;
      }
      mudos.push(mint); rec.resubs = (rec.resubs || 0) + 1; rec.lastTickAt = Date.now();
    }
  }
  if (mudos.length) {
    pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: mudos }));
    addLog(`🔌 RE-SUSCRIPCIÓN: ${mudos.length} token(s) llevaban 90s sin ticks con la cámara viva — reenviado subscribeTokenTrade`, "warn");
  }
}, 60_000);

// [5-ago] LATIDO DEL SEGUIMIENTO: los tokens muertos (rug) dejan de emitir ticks, así que sin esto
// su tarjeta nunca mostraría el post-cierre. Cada 15s emitimos el estado de todas las ops vigiladas
// con el último precio conocido, así la franja aparece y se mantiene viva en las 60 ops de la hora.
setInterval(() => {
  if (!LIVE_RECORD) return;
  for (const [mint, rec] of state.liveRecordings) {
    const p = rec.post;
    if (!p || !p.salida || rec.finished) continue;
    // [16-ago] el seguimiento vive lo que viva la cámara (hasta 2h), no 60 min
    const px = rec.lastPrice || p.salida;
    const desde = +((px / p.salida - 1) * 100).toFixed(1);
    p.ultimo = desde;
    if (desde > p.maxDespues) p.maxDespues = desde;
    broadcast({ event: "postCierre", data: { mint, tradeId: p.tradeId, symbol: p.symbol,
      desde, max: p.maxDespues, mcAhora: px * 1_000_000_000,
      min: Math.round((Date.now() - p.t0) / 60000), vivo: !!rec.lastPrice } });
  }
}, 15_000);

// [5-ago] veredicto: ¿fue buen momento de vender? Se emite al apagarse la cámara.
function emitirVeredicto(mint, rec) {
  if (!rec || !rec.post || !rec.post.salida) return;
  const p = rec.post;
  // [16-ago] VEREDICTO JUSTO. Antes solo miraba el pico: un token que repuntaba +75% y luego
  // moría a -95% salía como "vendiste pronto", cuando vender fue justo lo correcto. Ahora el
  // pico solo condena si el token TERMINA arriba; si repuntó y se hundió, el cierre fue bueno.
  const subioMucho = p.maxDespues >= 50, sigueArriba = p.ultimo >= 15;
  const veredicto = (subioMucho && sigueArriba) ? "pronto"
    : (subioMucho && !sigueArriba) ? "repunte"
    : p.maxDespues >= 15 && sigueArriba ? "justo" : "bien";
  const datos = { mint, tradeId: p.tradeId, symbol: p.symbol, desde: p.ultimo, max: p.maxDespues,
    mcAhora: (rec.lastPrice || p.salida) * 1_000_000_000,
    min: Math.round((Date.now() - p.t0) / 60000), final: true, veredicto };
  // [16-ago] lo guardamos EN EL TRADE: así sobrevive a recargas del panel y a reinicios del
  // server (va en saveState), y viaja con la op a cualquier dispositivo.
  const tr = state.demoTrades.find(t => t.id === p.tradeId) || state.realTrades.find(t => t.id === p.tradeId);
  if (tr) { tr.post = datos; saveState(); }
  broadcast({ event: "postCierre", data: datos });
  addLog(`🔭 POST-CIERRE ${rec.symbol}: tras vender el token hizo ${p.maxDespues >= 0 ? "+" : ""}${p.maxDespues}% como máximo `
    + `(ahora ${p.ultimo >= 0 ? "+" : ""}${p.ultimo}%) → ${veredicto === "pronto" ? "❌ vendimos PRONTO" : veredicto === "repunte" ? "✅ buen cierre (repuntó y murió)" : veredicto === "justo" ? "🟡 justo" : "✅ buen cierre"}`, "rec");
}

function liveRecEmit(mint, motivo = "?") {
  const rec = state.liveRecordings.get(mint);
  if (rec && rec.post) emitirVeredicto(mint, rec);   // [5-ago]
  if (!rec || rec.finished) return;
  // [2-sep] TRAZA: 14 cámaras se apagaron con el token vivo y los ticks llegando cada 2-3s,
  // sin dejar ni una línea en el log. Hasta saber quién las cierra no se puede arreglar.
  {
    const dur = rec.puntos && rec.puntos.length ? rec.puntos[rec.puntos.length - 1].t : 0;
    const pct = (rec.lastPrice && rec.entryPrice) ? ((rec.lastPrice / rec.entryPrice - 1) * 100).toFixed(1) : "?";
    const mudez = Math.round((Date.now() - (rec.lastTickAt || rec.t0)) / 1000);
    const ab = state.demoTrades.filter(t => t.mint === mint && t.status === "OPEN");
    const abiertas = ab.length;
    const porEstr = ab.reduce((a, t) => { a[t.strategy] = (a[t.strategy] || 0) + 1; return a; }, {});
    addLog(`🎬 CÁMARA CERRADA (${motivo}): ${rec.symbol} · dur=${dur}s · precio ${pct}% · ${mudez}s desde el último tick · ${abiertas} abiertas ${JSON.stringify(porEstr)} · resubs=${rec.resubs || 0} · cierreDemo=${rec.cierreDemo != null ? "sí" : "no"} · ventana=${rec._ventanaMin || "-"}min`, "warn");
  }
  uniFinish(rec);   // [23-ago] 🤝 UNIDA: cerrar posiciones vivas antes de apagar la cámara
  rec.finished = true; state.liveRecordings.delete(mint); unsubscribeToken(mint);
  { const c0 = espia.cuenta.get(mint);   // [30-ago] veredicto del espía al cerrar la cámara
    if (c0) addLog(`[ESPIA-FIN] ${rec.symbol} dur=${rec.puntos && rec.puntos.length ? rec.puntos[rec.puntos.length-1].t : 0}s · portal=${c0.portal} helius-swaps=${c0.swaps || 0} (${c0.helius}tx)`
      + (c0.difN ? ` · precio dif media ${(c0.difSum / c0.difN).toFixed(2)}% peor ${(c0.difMax || 0).toFixed(2)}%` : "")
      + ((c0.swaps || 0) > c0.portal * 1.2 ? " ⚠️ HELIUS VIO MÁS" : "")
      + (c0.relevo ? ` · 🩺 ${c0.relevo} precios puestos por Helius` : "")
      + (c0.precioH && rec.lastPrice ? ` · último precio H=${c0.precioH.toPrecision(4)} P=${rec.lastPrice.toPrecision(4)}` : ""), "info"); }
  espiaDesuscribir(mint);
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
  addLog(`[MIGREC] sym=${rec.symbol} mint=${rec.mint} vel=${rec.vel}s MC=${formatMC(rec.mc)} vol=${rec.vol} mov2s=${mov2s} sig=${rec.sigMov2s!=null?(rec.sigMov2s>=0?"+":"")+rec.sigMov2s+"%@"+rec.sigT+"s":"n/a"} ex2s=${rec.ex2s!=null?(rec.ex2s>=0?"+":"")+rec.ex2s+"%":"n/a"} MIN=${min.p}%@${min.t}s MAX=${max.p}%@${max.t}s orden=${orden} cruces[10,15,20]=${cruces[0]},${cruces[1]},${cruces[2]} cierre_real=${cd!=null?(cd>=0?"+":"")+cd:"n/a"}% dur_rec=${pts[pts.length-1].t}s volPost=${Math.round(rec.volPost||0)}${wStr}${vol60?` vol60=${vol60}`:""}${rec.washCompra!=null?` washCompra=${rec.washCompra}@${rec.washCompraT}s`:""}${rec.supply?` supply=${Math.round(rec.supply/1e6)}M mayhem=${rec.mayhem?1:0}`:""}${rec.flujo&&rec.flujo.length?` flujo=${rec.flujo.map(x=>`${x.t}:${Math.round(x.v)}:${x.c}:${x.s}:${x.w.size}`).join(",")}`:""} pts=${ptsRaw}`, "rec");
  try { shadowProcesa(rec); } catch (e) { if (!state._shErr) { state._shErr = true; addLog(`⚠️ shadowProcesa error: ${e.message}`, "warn"); } }   // [v11.9]
  labStats.migrecs++;
}

// [27-ago] firma de actores del primer minuto, consultable en cualquier momento.
// Sirve para etiquetar cada cierre con su wash/compradores y poder evaluar
// la "estrategia con filtro de wash" sin duplicar el motor: son las MISMAS
// decisiones sobre un subconjunto de ops, así que basta con saber a cuál pertenece.
// [27-ago] curva reducida (≤40 puntos) para el minigráfico del panel
function curvaMini(mint, n = 40) {
  const rec = state.liveRecordings.get(mint);
  if (!rec || !rec.puntos || rec.puntos.length < 3) return null;
  const P = rec.puntos, paso = Math.max(1, Math.floor(P.length / n));
  const out = [];
  for (let i = 0; i < P.length; i += paso) out.push(+P[i].p.toFixed(1));
  const ult = +P[P.length - 1].p.toFixed(1);
  if (out[out.length - 1] !== ult) out.push(ult);
  return out;
}

function calidadEntrada(mint) {
  const rec = state.liveRecordings.get(mint);
  if (!rec || !rec.wallets || !rec.wallets.size) return null;
  const ws = [...rec.wallets.values()];
  const buyers = ws.filter(w => w.buys > 0);
  const totalBuy = buyers.reduce((s, w) => s + w.buyUsd, 0);
  const topBuy = buyers.length ? Math.max(...buyers.map(w => w.buyUsd)) : 0;
  return {
    buyers: buyers.length,
    top: totalBuy > 0 ? Math.round(100 * topBuy / totalBuy) : 0,
    wash: ws.filter(w => w.buys > 0 && w.sells > 0).length,
  };
}

function liveRecFinish(mint, cierreRealPct) {
  if (!LIVE_RECORD) return;
  const rec = state.liveRecordings.get(mint);
  if (!rec || rec.finished) return;
  if (rec.cierreDemo == null) rec.cierreDemo = cierreRealPct;
  // [4-ago] sin expiración, la cámara acompaña a la op mientras siga abierta (si no, el lab quedaría ciego)
  const sigueAbierta = state.demoTrades.some(t => t.mint === mint && t.status === "OPEN")
                    || state.realTrades.some(t => t.mint === mint && (t.status === "OPEN" || t.status === "CLOSING"));
  // [16-ago] sin expiración, una op puede durar horas: la cámara la acompaña hasta el tope duro.
  // Si ya cerró, seguimos grabando LAB_EXTEND (2h) para ver qué hizo el token después de vender.
  const ventana = sigueAbierta ? MIG_HARD_MAX_MS : LAB_EXTEND_MS;
  const restante = (rec.t0 + ventana) - Date.now();
  if (restante <= 0) { liveRecEmit(mint, 'ventana agotada'); return; }
  // [16-ago] REPROGRAMAR SIEMPRE. Antes se creaba el timer una sola vez (`if (!rec._emitTimer)`),
  // así que si la op cerraba con la re-caza aún abierta se fijaba la ventana de 6h y ya no se
  // recortaba al cerrar esta: la cámara seguía horas sobre un token muerto, gastando suscripción.
  // Y al revés, si la ventana debía ampliarse (re-caza que entra después), tampoco se ampliaba.
  if (rec._emitTimer) clearTimeout(rec._emitTimer);
  rec._emitTimer = setTimeout(() => liveRecEmit(mint, 'fin de ventana'), restante);
  rec._ventanaMin = Math.round(ventana / 60000);
}

// Registra el PnL de una operación cerrada en el acumulador por hora.
function registrarPnlHorario(pnlSol, esWin) {
  // [FIX 26-jul] hora ESPAÑOLA (UTC + REAL_TZ_OFFSET), no la hora local del servidor
  // (Railway corre en UTC): así el RESUMEN HORA/DÍA cuadra con la franja bloqueada,
  // con [SHADOW-HORAS] y con lo que ves tú en el reloj.
  const ahora = new Date(Date.now() + REAL_TZ_OFFSET * 3600 * 1000);
  const y = ahora.getUTCFullYear(), mo = String(ahora.getUTCMonth()+1).padStart(2,"0");
  const d = String(ahora.getUTCDate()).padStart(2,"0"), h = String(ahora.getUTCHours()).padStart(2,"0");
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
  if (mcOpen > MIG_MAX_MC_ENTRY || mcOpen < MIG_MIN_MC_ENTRY) {
    addLog(`🛑 MIG MC FUERA DE RANGO (cinturón en apertura): ${entry.symbol} bloqueada | MC ${formatMC(mcOpen)} (rango válido ${formatMC(MIG_MIN_MC_ENTRY)}–${formatMC(MIG_MAX_MC_ENTRY)})`, "filter");
    migRechazada(entry, "MIG MC FUERA DE RANGO"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
    broadcast({ event: "stats", data: state.stats });
    return;
  }
  // [FIX 29-jul] VETO DE LENTOS (mando del lab): la vel del MIGREC se mide aquí mismo.
  // Si desde la migración hasta este punto (señal+confirmación+examen) pasaron >11s, fuera.
  const velSeg = +(((Date.now() - entry.startTime) / 1000).toFixed(1));
  if (velSeg > MIG_MAX_VEL_S) {
    addLog(`🐢 MIG VETO LENTOS: ${entry.symbol} descartada | vel=${velSeg}s > ${MIG_MAX_VEL_S}s (migración→entrada demasiado lenta)`, "filter");
    migRechazada(entry, "MIG VETO LENTOS"); state.stats.mig_rejected++; state.migWatching.delete(entry.mint); unsubscribeToken(entry.mint);
    broadcast({ event: "stats", data: state.stats });
    return;
  }
  liveRecStart(entry, price);
  const signal = {
    id: `mig-${entry.mint}-${Date.now()}`, strategy: "migration",
    mint: entry.mint, name: entry.name, symbol: entry.symbol,
    price, tp: +(price*MIG_TP).toFixed(12), sl: +(price*MIG_SL).toFixed(12),
    mcUsd: price*1_000_000_000, volumeUSD: entry.volumeUSD,
    sigPct: entry.sigPct ?? null,   // [31-jul]
    vel: velSeg,   // [v11.9] para el veto de lentos (ya aplicado arriba)
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
  // [29-jul H78WEN] la cámara rodaba 60min (LAB_EXTEND) pero este unsubscribe la dejaba sin
  // ticks al cerrar el último trade (dur_rec colapsaba, ej. 292s) y de paso cegaba a la
  // re-caza — ese token resucitó a +4500% sin nadie mirando. Mientras la grabación siga
  // viva, el feed no se toca; liveRecEmit desuscribe al terminar.
  const rec = state.liveRecordings.get(mint);
  if (LIVE_RECORD && rec && !rec.finished) {
    state.migMonitored.delete(mint);
    broadcast({ event: "removeToken", data: { mint } });
    addLog(`🎥 ${symbol} fuera del panel — cámara y re-caza siguen ${Math.max(1, Math.round((rec.t0 + LAB_EXTEND_MS - Date.now()) / 60000))}min más`, "info");
    return;
  }
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
      // [16-ago] El guardarraíl ×100/÷100 protegía contra precios corruptos, pero descartaba
      // el TICK ENTERO: con la cámara larga, todo lo que pasa tras cerrar la op entra por aquí,
      // así que un solo precio fuera de banda dejaba de alimentar la curva Y de gestionar los
      // trades vivos (sin actualizar máximo ni poder disparar el stop). Ahora el filtro solo
      // decide si se GRABA la muestra; la gestión de trades se ejecuta siempre.
      const cuerdo = price < rec.entryPrice * 100 && price > rec.entryPrice / 100;
      if (cuerdo) {
        liveRecSample(mint, price, solAmount * solPriceUSD, trader, isBuy);
      } else {
        rec.lastTickAt = Date.now();   // hay vida: que el vigilante no lo dé por mudo
        if (!rec._avisoBanda) { rec._avisoBanda = true;
          addLog(`⚠️ precio fuera de banda en ${shortAddr(mint)}: ${price.toPrecision(4)} vs entrada ${rec.entryPrice.toPrecision(4)} — no se graba, pero la op se sigue gestionando`, "warn"); }
      }
      {
        // [v10] FIX ZOMBIE: si queda un demo OPEN de este mint (p.ej. el real cerró
        // antes y migCleanup borró el token del monitoreo), seguir gestionándolo.
        updateDemoTrades(mint, price, "migration");
        updateRealTrades(mint, price, "migration");   // [v11.9] el real también respira aquí
      }
    } else if (price > 0) {
      // [v11.9] POST-GRABACIÓN: la grabación terminó (o nunca existió) pero quedan
      // trades vivos de este mint (migración larga o unida). Sin esta rama,
      // los ticks llegaban y se tiraban -> congelación aunque la suscripción viva.
      const abierto = state.demoTrades.find(t => t.mint === mint && t.status === "OPEN")
                   || state.realTrades.find(t => t.mint === mint && t.status === "OPEN");
      if (abierto && price < abierto.entryPrice * 1000 && price > abierto.entryPrice / 1000) {
        updateDemoTrades(mint, price, "migration");
        updateRealTrades(mint, price, "migration");
      }
    }
    return;
  }
  if (!isPriceValid(price, token.price, token.lastUpdate)) {
    // [v11.9] el salto es sospechoso para las MÉTRICAS del token (no las tocamos, ni grabamos),
    // pero los TRADES vivos no pueden quedarse ciegos 10s en pleno movimiento violento:
    // gestionar con sanidad propia (±1000x sobre su entrada) y salir.
    const ab = state.demoTrades.find(t => t.mint === mint && t.status === "OPEN")
            || state.realTrades.find(t => t.mint === mint && t.status === "OPEN");
    if (ab && price < ab.entryPrice * 1000 && price > ab.entryPrice / 1000) {
      updateDemoTrades(mint, price, "migration");
      updateRealTrades(mint, price, "migration");
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

// [v11.9] Swap vía Jupiter v6: quote → swap tx serializada → firmar y enviar con nuestro RPC.
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
  if (DEMO_ONLY) return;
  if (OBSERVER_MODE) return;
  if (!wallet || !connection) return;
  if (!REAL_STRATEGIES.includes(signal.strategy)) return;
  if (tradingHalted()) { addLog(`🛑 REAL bloqueado por kill-switch: ${signal.symbol}`, "warn"); return; }
  if (franjaRealBloqueada()) { addLog(`🕐 REAL bloqueado por franja (${horaES()}h ES): ${signal.symbol} — el demo sigue`, "filter"); return; }
  if (REAL_VEL_MAX > 0 && signal.strategy === "migration" && signal.vel != null && signal.vel >= REAL_VEL_MAX) {
    addLog(`🐌 REAL bloqueado por velocidad (${signal.vel}s ≥ ${REAL_VEL_MAX}): ${signal.symbol} — el demo sigue`, "filter"); return;
  }
  const openReal = state.realTrades.filter(t => t.status === "OPEN").length;
  if (openReal >= MAX_REAL_TRADES) return;
  const migOpen = state.realTrades.filter(t => t.status === "OPEN" && REAL_STRATEGIES.includes(t.strategy)).length;
  if (migOpen >= MAX_MIG_REAL) return;
  if (signal.strategy === "migration" && signal.mcUsd > MIG_MAX_MC_REAL) {
    addLog(`⚠️ REAL: MC ${formatMC(signal.mcUsd)} > ${formatMC(MIG_MAX_MC_REAL)}, skip`, "warn"); return;
  }
  const balance = await getWalletBalance(true);   // [2-sep] antes de gastar, saldo fresco
  const solIn = SOL_PER_TRADE_REAL;
  if (balance < solIn + 0.01) { addLog(`⚠️ Balance bajo: ${balance.toFixed(3)}`, "warn"); return; }

  const buyRes = await buyToken(signal.mint, solIn, "entry");
  if (!buyRes) return;

  const trade = {
    id: `real-${signal.strategy}-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, strategy: signal.strategy,   // [FIX 27-jul] idem
    mint: signal.mint, symbol: signal.symbol, name: signal.name,
    entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
    solIn: buyRes.costSol ?? solIn, txBuy: buyRes.sig,
    openTime: Date.now(), closeTime: null,
    status: "OPEN", result: null, pnlPct: null, pnlSol: null,
    maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: signal.strategy === "migration" ? "INITIAL" : signal.strategy.toUpperCase(),
    expiresAt: Date.now() + (MIG_DURATION_MS || MIG_HARD_MAX_MS),
  };
  state.realTrades.unshift(trade);
  if (state.realTrades.length > 200) state.realTrades.pop();
  state.stats.realOpen = state.realTrades.filter(t => t.status === "OPEN").length;
  broadcast({ event: "newRealTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  addLog(`💰 REAL [${signal.strategy}]: ${signal.symbol} | ${trade.solIn} SOL @ ${formatMC(signal.price * 1_000_000_000)}`, "real");
  if (signal.strategy === "migration" && MIG_LAUNCH_CHECK) scheduleLaunchCheck(trade, "real");
  saveState();
}

function scheduleLaunchCheck(trade, kind) {
  setTimeout(() => {
    try {
      const t = kind === "real"
        ? state.realTrades.find(x => x.id === trade.id)
        : state.demoTrades.find(x => x.id === trade.id);
      if (!t || t.status !== "OPEN") return;
      if ((t.maxGainPct || 0) < MIG_LAUNCH_MIN_PCT) {
        addLog(`🪂 NO-DESPEGUE (${kind}): ${t.symbol} máx +${(t.maxGainPct||0).toFixed(1)}% < +${MIG_LAUNCH_MIN_PCT}% a los ${MIG_LAUNCH_CHECK_MS/1000}s — fuera`, "warn");
        const px = t.entryPrice * (1 + (t.currentPct || 0)/100);
        if (kind === "real") closeRealTrade(t, px, "NO_LAUNCH");
        else closeDemoTrade(t, px, "NO_LAUNCH", MIG_TP);
      }
    } catch (e) { addLog(`⚠️ launchCheck error: ${e.message}`, "warn"); }
  }, MIG_LAUNCH_CHECK_MS);
}

// [CAMBIO 9-jul] Convierte una posición real en runner: vende el 75% y deja correr el resto.
async function realRunnerConvert(trade, price) {
  trade.status = "CLOSING";
  const sellRes = await sellToken(trade.mint, 1 - MIG_RUNNER_FRACTION, "calm");
  if (!sellRes) { trade.status = "OPEN"; return false; }
  trade.runnerActive = true;
  trade.runnerPartialSol = sellRes.proceedsSol;
  trade.runnerPartialPct = (price - trade.entryPrice) / trade.entryPrice * 100;
  trade.trailingPhase = "RUNNER";
  setSL(trade, Math.max(trade.entryPrice, price * (1 - MIG_RUNNER_TRAIL)), "runner-conv");
  trade.status = "OPEN";
  addLog(`🚀 RUNNER (real): ${trade.symbol} vendido ${Math.round((1-MIG_RUNNER_FRACTION)*100)}% a +${trade.runnerPartialPct.toFixed(1)}% (${sellRes.proceedsSol} SOL) — 25% corre con trailing ${MIG_RUNNER_TRAIL*100}%`, "real");
  broadcast({ event: "realTradeUpdate", data: { id: trade.id, trailingPhase: "RUNNER", sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1) } });   // [FIX 27-jul] idem
  return true;
}

async function closeRealTrade(trade, price, reason) {
  if (trade.status !== "OPEN") return;
  trade.status = "CLOSING";
  const sellRes = await sellToken(trade.mint, 1, urgencyByReason(reason));
  if (!sellRes) {
    trade.status = "OPEN";
    trade._failedSells = (trade._failedSells || 0) + 1;
    if (trade._failedSells >= 8) {
      trade.status = "CLOSED"; trade.result = "SELL_FAILED";
      trade.pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
      trade.pnlSol = -trade.solIn;
      trade.closeTime = Date.now();
      state.stats.mig_realLosses++;
      state.stats.mig_realPnLSol += trade.pnlSol;
      riskRecordClose(trade.pnlSol);
      registrarPnlHorario(trade.pnlSol, false);
      addLog(`💀 VENTA IMPOSIBLE tras 8 intentos: ${trade.symbol} — marcado SELL_FAILED (honeypot/LP retirada). Pérdida contable ${trade.pnlSol} SOL`, "error");
      state.stats.realOpen = state.realTrades.filter(t => t.status === "OPEN").length;
      broadcast({ event: "realTradeClosed", data: trade });
      broadcast({ event: "stats", data: state.stats });
      saveState();
      const rec0 = state.liveRecordings.get(trade.mint);
      if (rec0 && rec0.cierreDemo == null) liveRecFinish(trade.mint, trade.pnlPct);
      if (!state.demoTrades.some(t => t.mint === trade.mint && t.status === "OPEN")) migCleanup(trade.mint, trade.symbol);
      return;
    }
    addLog(`⚠️ Venta fallida (${trade._failedSells}/8): ${trade.symbol} — reintento en próximo tick`, "warn");
    return;
  }

  const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
  const solOut = sellRes.proceedsSol + (trade.runnerPartialSol || 0);
  const pnlSol = +(solOut - trade.solIn).toFixed(6);

  trade.status = "CLOSED";
  trade.closeTime = Date.now();
  trade.closePrice = price;
  trade.txSell = sellRes.sig;
  trade.solOut = solOut;
  trade.pnlPct = +pnlPct.toFixed(2);
  trade.pnlSol = pnlSol;
  trade.result = pnlSol >= 0 ? "WIN" : "LOSS";
  if (reason === "EXPIRED") trade.result = pnlSol >= 0 ? "WIN" : "EXPIRED";

  if (trade.result === "WIN") state.stats.mig_realWins++;
  else state.stats.mig_realLosses++;
  state.stats.mig_realPnLSol = +(state.stats.mig_realPnLSol + pnlSol).toFixed(6);
  riskRecordClose(pnlSol);
  registrarPnlHorario(pnlSol, trade.result === "WIN");

  state.stats.realOpen = state.realTrades.filter(t => t.status === "OPEN").length;
  const emoji = pnlSol >= 0 ? "🟢" : "🔴";
  addLog(`${emoji} REAL CERRADA [${reason}]: ${trade.symbol} | ${pnlPct >= 0 ? "+" : ""}${pnlPct.toFixed(1)}% | ${pnlSol >= 0 ? "+" : ""}${pnlSol} SOL (in ${trade.solIn} → out ${solOut.toFixed(4)})`, pnlSol >= 0 ? "real" : "error");
  broadcast({ event: "realTradeClosed", data: trade });
  broadcast({ event: "stats", data: state.stats });
  saveState();
  const rec = state.liveRecordings.get(trade.mint);
  if (rec && rec.cierreDemo == null) liveRecFinish(trade.mint, trade.pnlPct);
  if (!state.demoTrades.some(t => t.mint === trade.mint && t.status === "OPEN")) migCleanup(trade.mint, trade.symbol);
}


// [FIX 27-jul] 📼 CAJA NEGRA DEL SL: cada cambio queda apuntado (hora, valor, motivo) en
// trade.slHist (últimos 60). Si el SL BAJA fuera de la conversión a runner, salta un ⚠️ en el
// log — el tripwire que responde de una vez si "el SL baja solo" es bug o es el moon-bag.
function setSL(trade, v, tag) {
  const old = trade.sl;
  if (!(v !== old)) return;
  trade.slHist = trade.slHist || [];
  trade.slHist.push({ t: Date.now(), sl: +(+v).toFixed(12), pct: +(((v - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), tag });
  if (trade.slHist.length > 60) trade.slHist.shift();
  if (v < old && tag !== "runner-conv") {
    addLog(`⚠️ [SLTRACE] SL BAJÓ fuera del runner: ${trade.symbol} [${tag}] ${(((old - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1)}% → ${(((v - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1)}%`, "warn");
  }
  trade.sl = v;
}

function migTrailingPct(maxGainPct) {
  if (maxGainPct >= MIG_TRAIL_T3) return MIG_TRAIL_P4;
  if (maxGainPct >= MIG_TRAIL_T2) return MIG_TRAIL_P3;
  if (maxGainPct >= MIG_TRAIL_T1) return MIG_TRAIL_P2;
  return MIG_TRAIL_P1;
}

const isMig = (t) => t.strategy === "migration";


function updateRealTrades(mint, price, strategy) {
  const nowR = Date.now();
  for (const trade of state.realTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
    // [16-ago] cadencia por tiempo, igual que el demo (ver nota arriba: no atarla a la cámara)
    if (MIG_DECIDE_ON && isMig(trade)) {
      const cad = cadenciaMs(nowR - trade.openTime, 0);
      if (trade._lastEval && nowR - trade._lastEval < cad) continue;
      trade._lastEval = nowR;
    }
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    if (trade.mcEntry) trade.mcMax = Math.max(trade.mcMax || 0, trade.mcEntry * (1 + trade.maxGainPct / 100));   // [5-ago]

    if (isMig(trade)) {
      const gainRatio = price / trade.entryPrice;
      if (MIG_BE_ON && trade.trailingPhase === "INITIAL" && gainRatio >= 1 + MIG_BREAKEVEN_AT) {
        trade.trailingPhase = "BREAKEVEN";
        setSL(trade, trade.entryPrice * (1 + MIG_BREAKEVEN_MARGIN), "breakeven");
        addLog(`🔒 BE (real): ${trade.symbol}`, "real");
      }
      if ((trade.trailingPhase === "INITIAL" || trade.trailingPhase === "BREAKEVEN") && gainRatio >= 1 + (((trade.cfg || MIG_CFG_BASE).arm ?? 25) / 100)) {   // [29-jul] escalón por clase
        trade.trailingPhase = "FOLLOWING";
      }
      if (trade.trailingPhase === "FOLLOWING") {
        const trailPct = Math.min(0.90, migTrailingPct(trade.maxGainPct) );
        const newSL = price * (1 - trailPct);
        if (newSL > trade.sl) setSL(trade, newSL, "trail");
        if (trade.maxGainPct >= MIG_STEP_TRIGGER * 100) {
          const stepFloor = trade.entryPrice * (1 + MIG_STEP_FLOOR);
          if (stepFloor > trade.sl) setSL(trade, stepFloor, "step13");
        }
        if (trade.maxGainPct >= MIG_TOP_FLOOR_TRIGGER) {
          const floor65 = trade.entryPrice * (1 + MIG_TOP_FLOOR);   // [FIX 27-jul] limpiada la línea muerta con la fórmula enrevesada (calculaba lo mismo por casualidad)
          if (floor65 > trade.sl) setSL(trade, floor65, "suelo65");
        }
      }
    }

    const expired = Date.now() >= trade.expiresAt;
    let reason = null;
    if (price >= trade.tp) reason = "TP";
    else if (price <= trade.sl) {
      trade._slBelowCount = (trade._slBelowCount || 0) + 1;
      trade._slPanic = esPanico(trade, price) && !tickFantasma(trade, price);   // [FIX 27-jul · 31-jul anti-pinchazo]
      if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS || trade._slPanic) reason = "SL";
    } else { trade._slBelowCount = 0; trade._slPanic = false; }
    if (!reason && expired) {
      if (isMig(trade) && currentPct >= MIG_EXPIRED_WIN_PCT) reason = "TP_EXPIRED";
      else reason = "EXPIRED";
    }

    if (reason === "SL" && isMig(trade) && MIG_RUNNER_ON && !trade.runnerActive
        && !trade._slPanic && trade.maxGainPct >= MIG_RUNNER_MIN_GAIN && currentPct > 0) {   // [FIX 27-jul] en perforación profunda se cierra TODO
      realRunnerConvert(trade, price);
      continue;
    }
    if (reason) closeRealTrade(trade, price, reason);
    else broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
  }
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.realTrades) {
    if (trade.status !== "OPEN" || now < trade.expiresAt) continue;
    const price = trade.entryPrice * (1 + (trade.currentPct || 0) / 100);
    const reason = isMig(trade) && (trade.currentPct || 0) >= MIG_EXPIRED_WIN_PCT ? "TP_EXPIRED" : "EXPIRED";
    addLog(`⏱️ REAL expirada sin ticks: ${trade.symbol} (${trade.currentPct?.toFixed(1) ?? "?"}%) — cerrando por ${reason}`, "warn");
    closeRealTrade(trade, price, reason);
  }
}, 30_000);

function openDemoTrade(signal) {
  if (OBSERVER_MODE) return;
  const openCount = state.demoTrades.filter(t => t.status === "OPEN").length;
  if (openCount >= 50) return;
  const sizeSol = +(SOL_PER_TRADE_MIG ).toFixed(2);   // [v10] tamaño por calor
  const trade = {
    id: `demo-${signal.strategy}-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, strategy: signal.strategy,   // [FIX 27-jul] sufijo aleatorio: dos aperturas en el mismo ms compartían id y el panel mezclaba sus tarjetas
    mint: signal.mint, symbol: signal.symbol, name: signal.name,
    entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
    sizeSol,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: "INITIAL", status: "OPEN",
    // [5-ago] datos para el desglose de la tarjeta del panel
    mcEntry: signal.mcUsd ?? null,          // market cap al entrar
    mcMax: signal.mcUsd ?? null,            // se irá actualizando con el máximo
    velSeg: signal.vel ?? null, sigPct: signal.sigPct ?? null,
    lote: null,                             // se rellena abajo con sizeSol
    expiresAt: Date.now() + (MIG_DURATION_MS || MIG_HARD_MAX_MS),
    mov1s: null, mov2s: null,
  };
  if (signal.strategy === "migration") {   // [29-jul] motor por clases: abre con la Base y a los 2s se enruta
    trade.cfg = MIG_CFG_BASE;
    trade.velSeg = signal.vel ?? null;
    trade.sigPct = signal.sigPct ?? null;   // [31-jul] la senal que abrio la puerta, para el MIGCLOSE
    trade.mcOpenUsd = signal.mcUsd ?? null;
    trade.sl = +(trade.entryPrice * (1 + MIG_CFG_BASE.sl / 100)).toFixed(12);
    trade.tp = +(trade.entryPrice * (1 + MIG_CFG_BASE.tp / 100)).toFixed(12);
  }
  setTimeout(() => { if (trade.status === "OPEN") trade.mov1s = trade.currentPct; }, 1000);
  setTimeout(() => { if (trade.status === "OPEN") { trade.mov2s = trade.currentPct; if (isMig(trade)) migRouteCfg(trade); } }, 2000);
  state.demoTrades.unshift(trade);
  if (state.demoTrades.length > 500) state.demoTrades.pop();
  state.stats.demoOpen++;
  broadcast({ event: "newDemoTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  addLog(`🧪 DEMO [${signal.strategy}]: ${signal.symbol} (${sizeSol} SOL${""})`, "demo");
  if (signal.strategy === "migration" && MIG_LAUNCH_CHECK) scheduleLaunchCheck(trade, "demo");
}

function updateDemoTrades(mint, price, strategy) {
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
    // [16-ago] CADENCIA POR TIEMPO, no atada al sello de la cámara.
    // Antes se comparaba con rec.sampleTick: si la cámara terminaba (2h) o el tick llegaba por
    // una ruta que no la alimenta, sampleTick se congelaba y el trade DEJABA DE EVALUARSE —
    // con el stop loss sin poder dispararse. Inaceptable con dinero real.
    if (MIG_DECIDE_ON && isMig(trade)) {
      const cad = cadenciaMs(now - trade.openTime, 0);
      if (trade._lastEval && now - trade._lastEval < cad) continue;
      trade._lastEval = now;
    }
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);

    if (isMig(trade)) {
      const gainRatio = price / trade.entryPrice;
      if (MIG_BE_ON && trade.trailingPhase === "INITIAL" && gainRatio >= 1 + MIG_BREAKEVEN_AT) {
        trade.trailingPhase = "BREAKEVEN";
        setSL(trade, trade.entryPrice * (1 + MIG_BREAKEVEN_MARGIN), "breakeven");
      }
      if ((trade.trailingPhase === "INITIAL" || trade.trailingPhase === "BREAKEVEN") && gainRatio >= 1 + (((trade.cfg || MIG_CFG_BASE).arm ?? 25) / 100)) {   // [29-jul] escalón por clase
        trade.trailingPhase = "FOLLOWING";
        addLog(`📈 TRAILING: ${trade.symbol} +${((gainRatio-1)*100).toFixed(0)}%`, "demo");
      }
      if (trade.trailingPhase === "FOLLOWING" || trade.trailingPhase === "RUNNER") {
        const cfgM = trade.cfg || MIG_CFG_BASE;   // [29-jul] tornillos de la clase
        if (trade.trailingPhase === "RUNNER") {
          const cand = price * (1 - cfgM.runTr / 100);
          if (cand > trade.sl) setSL(trade, cand, "runner-trail");
          const floorBE = trade.entryPrice * (1 + MIG_RUNNER_FLOOR / 100);
          if (floorBE > trade.sl) setSL(trade, floorBE, "runner-floor");
        } else {
          const trailPct = migCfgTrailPct(trade.maxGainPct, cfgM.mult);
          const newSL = price * (1 - trailPct);
          if (newSL > trade.sl) setSL(trade, newSL, "trail");
          if (cfgM.pisoOn !== false) {
            if (trade.maxGainPct >= (cfgM.arm ?? 25)) {   // [29-jul] escalón por clase
              const stepFloor = trade.entryPrice * (1 + cfgM.piso1 / 100);
              if (stepFloor > trade.sl) setSL(trade, stepFloor, "piso1");
            }
            if (trade.maxGainPct >= MIG_TOP_FLOOR_TRIGGER) {
              const floor2 = trade.entryPrice * (1 + cfgM.piso2 / 100);
              if (floor2 > trade.sl) setSL(trade, floor2, "piso2");
            }
          }
        }
      }
    }

    const expired = now >= trade.expiresAt;
    let reason = null;
    // [29-jul] no-despegue por clase (ndT/ndMin de la config, como en el lab)
    if (isMig(trade) && !trade.runnerActive) {
      const cfgND = trade.cfg || MIG_CFG_BASE;
      if (cfgND.ndT && (now - trade.openTime) / 1000 >= cfgND.ndT && trade.maxGainPct < cfgND.ndMin) reason = "NO_LAUNCH";
    }
    if (!reason && price >= trade.tp) reason = "TP";
    else if (!reason && price <= trade.sl) {
      trade._slBelowCount = (trade._slBelowCount || 0) + 1;
      trade._slPanic = esPanico(trade, price) && !tickFantasma(trade, price);   // [FIX 27-jul · 31-jul anti-pinchazo]
      if (trade._slBelowCount >= MIG_SL_CONFIRM_TICKS || trade._slPanic) reason = "SL";
    } else { trade._slBelowCount = 0; trade._slPanic = false; }
    if (!reason && expired) {
      if (isMig(trade) && currentPct >= MIG_EXPIRED_WIN_PCT) reason = "TP_EXPIRED";
      else reason = "EXPIRED";
    }

    // [CAMBIO 9-jul] MOON-BAG: si el trailing dispara y el máximo tocó +50% en verde,
    // en vez de cerrar todo → vender 75% y dejar el 25% corriendo (solo demo).
    if (reason === "SL" && isMig(trade) && MIG_RUNNER_ON && !trade.runnerActive
        && !trade._slPanic && ((trade.cfg || MIG_CFG_BASE).moonOn !== false) && trade.maxGainPct >= ((trade.cfg || MIG_CFG_BASE).runTrig) && currentPct > 0) {   // [4-ago] moon-bag por clase   // [FIX 27-jul] perforación profunda cierra TODO · [29-jul] gatillo por clase
      trade.runnerActive = true;
      trade.runnerPartialPct = currentPct;
      trade.trailingPhase = "RUNNER";
      const slAntes = trade.sl;
      setSL(trade, Math.max(trade.entryPrice, price * (1 - ((trade.cfg || MIG_CFG_BASE).runTr) / 100)), "runner-conv");   // [29-jul] anchura por clase
      addLog(`🚀 RUNNER (demo): ${trade.symbol} asegura 75% a +${currentPct.toFixed(1)}% — 25% corre · SL ${((slAntes-trade.entryPrice)/trade.entryPrice*100).toFixed(0)}%→${((trade.sl-trade.entryPrice)/trade.entryPrice*100).toFixed(0)}% (trail ${MIG_RUNNER_TRAIL*100}%, suelo BE)`, "demo");
      broadcast({ event: "demoTradeUpdate", data: { id: trade.id, trailingPhase: "RUNNER", sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1) } });   // [FIX 27-jul] slPct incluido: el panel mostraba el viejo hasta el siguiente tick
      continue;
    }

    if (reason) closeDemoTrade(trade, price, reason, MIG_TP);
    else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, slPct: +(((trade.sl - trade.entryPrice) / trade.entryPrice) * 100).toFixed(1), trailingPhase: trade.trailingPhase } });
  }
}

function closeDemoTrade(trade, price, reason, tpMult) {
  if (trade.status !== "OPEN") return;
  trade.status = "CLOSED";
  trade.closeTime = Date.now();
  trade.closePrice = price;
  let pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
  // Moon-bag: PnL ponderado (75% asegurado + 25% al precio de cierre del runner)
  if (trade.runnerActive && trade.runnerPartialPct != null) {
    pnlPct = 0.75 * trade.runnerPartialPct + 0.25 * pnlPct;
  }
  trade.pnlPct = +pnlPct.toFixed(2);
  trade.result = reason === "TP" || reason === "TP_EXPIRED" ? "WIN"
    : reason === "EXPIRED" ? (pnlPct >= MIG_EXPIRED_WIN_PCT ? "WIN" : "EXPIRED")
    : pnlPct >= 0 ? "WIN" : "LOSS";

  const sizeSol = trade.sizeSol ?? SOL_PER_TRADE_MIG;
  const pnlSolOp = +(sizeSol * pnlPct / 100).toFixed(4);

  if (isMig(trade)) {
    if (trade.result === "WIN") state.stats.mig_demoWins++;
    else if (trade.result === "EXPIRED") state.stats.mig_demoExpired++;
    else state.stats.mig_demoLosses++;
    state.stats.mig_demoPnL = +(state.stats.mig_demoPnL + pnlPct).toFixed(2);
    state.stats.mig_closedCount++;
    state.stats.mig_maxGainSum += trade.maxGainPct;
    state.stats.mig_maxLossSum += trade.maxLossPct;
    state.stats.mig_avgMaxGain = +(state.stats.mig_maxGainSum / state.stats.mig_closedCount).toFixed(1);
    state.stats.mig_avgMaxLoss = +(state.stats.mig_maxLossSum / state.stats.mig_closedCount).toFixed(1);
    const mv = trade.mov2s;
    const win = trade.result === "WIN";
    if (mv !== null && mv !== undefined) {
      if (mv > 1) win ? state.stats.mig_mov_up_win++ : state.stats.mig_mov_up_loss++;
      else if (mv >= -1) win ? state.stats.mig_mov_flat_win++ : state.stats.mig_mov_flat_loss++;
      else win ? state.stats.mig_mov_down_win++ : state.stats.mig_mov_down_loss++;
    }
    // [v10.1] historial del creador: cierre malo (<= -50%) = mala para su wallet
    // [v11.9] y si fue un pull de verdad (<= MIG_ABYSS_PNL), su creador entra en la lista negra DE POR VIDA
    const preC = premigData.get(trade.mint);
    if (preC && preC.creator) {
      const hc = creatorHist.get(preC.creator) || { tokens: 0, malas: 0 };
      hc.tokens++;
      if (trade.pnlPct <= -50) hc.malas++;
      creatorHist.set(preC.creator, hc);
      if (MIG_ABYSS_VETO && trade.pnlPct <= MIG_ABYSS_PNL && !abyssCreators.has(preC.creator)) {
        abyssCreators.add(preC.creator);
        addLog(`☠️ LISTA NEGRA: creador ${preC.creator.slice(0,8)}… vetado DE POR VIDA (${trade.symbol} cerró ${trade.pnlPct}%)`, "warn");
      }
    }
  }

  state.stats.demoOpen = Math.max(0, state.stats.demoOpen - 1);
  registrarPnlHorario(pnlSolOp, trade.result === "WIN");

  const emoji = trade.result === "WIN" ? "🟢" : trade.result === "EXPIRED" ? "⚪" : "🔴";
  addLog(`${emoji} DEMO CERRADA [${reason}]${trade.runnerActive ? " +RUNNER" : ""}: ${trade.symbol} | ${pnlPct >= 0 ? "+" : ""}${pnlPct.toFixed(1)}% (${pnlSolOp >= 0 ? "+" : ""}${pnlSolOp} SOL de ${sizeSol})${trade.strategy !== "migration" ? ` [${trade.strategy}]` : ""}`, trade.result === "WIN" ? "demo" : "error");
  broadcast({ event: "demoTradeClosed", data: trade });
  broadcast({ event: "stats", data: state.stats });
  saveState();
  // [31-jul] MIGCLOSE de vuelta: linea parseable de cierre para cuadrar el lab con lo que hace el bot.
  // El MIGREC se emite al acabar la camara (60min), asi que su cierre_real puede ir en n/a; esta linea
  // permite cruzar por mint el resultado real de CADA cierre (migracion y unida).
  // [5-ago] desglose para la tarjeta del panel: MC de entrada, pico y salida + neto en SOL
  trade.mcClose = trade.mcEntry ? trade.mcEntry * (1 + pnlPct / 100) : null;
  trade.mcMin = trade.mcEntry ? trade.mcEntry * (1 + trade.maxLossPct / 100) : null;
  trade.mcMax = trade.mcEntry ? trade.mcEntry * (1 + trade.maxGainPct / 100) : trade.mcMax;   // [5-ago] refrescar al cerrar
  // [5-ago] SEGUIMIENTO POST-CIERRE: la cámara sigue grabando, así que marcamos el punto de salida
  // para poder decir después si vender fue acierto o precipitación.
  const recPost = state.liveRecordings.get(trade.mint);
  if (recPost && !recPost.finished) {
    recPost.post = { salida: trade.closePrice || null, pnlPct,
      mcSalida: trade.mcClose, t0: Date.now(), maxDespues: 0, ultimo: 0,
      tradeId: trade.id, symbol: trade.symbol, lastEmit: 0 };
  }
  trade.brutoSol = pnlSolOp;                                  // ya calculado arriba con el lote real
  trade.netoSol = +(pnlSolOp - (trade.sizeSol ?? SOL_PER_TRADE_MIG) * 0.045).toFixed(4);   // fricción 4.5%
  trade.dejadoPts = +(trade.maxGainPct - pnlPct).toFixed(1);
  trade.lote = trade.sizeSol ?? SOL_PER_TRADE_MIG;
  addLog(`[MIGCLOSE] mint=${trade.mint} sym=${trade.symbol} strat=${trade.strategy} pnl=${pnlPct >= 0 ? "+" : ""}${pnlPct.toFixed(2)}% reason=${reason} dur=${Math.round((Date.now() - trade.openTime) / 1000)}s runner=${trade.runnerActive ? 1 : 0} cfg=${(trade.cfg || {}).nom || "-"} sig=${trade.sigPct != null ? trade.sigPct : "n/a"} vel=${trade.velSeg != null ? trade.velSeg : "n/a"} mov2s=${trade.mov2s != null ? trade.mov2s.toFixed(2) : "n/a"} lote=${trade.sizeSol != null ? trade.sizeSol : "n/a"}${(q => q ? ` buyers=${q.buyers} topBuyer=${q.top}% wash=${q.wash}` : "")(calidadEntrada(trade.mint))}`, "rec");
  if (isMig(trade)) {
    liveRecFinish(trade.mint, trade.pnlPct);
    if (!state.realTrades.some(t => t.mint === trade.mint && (t.status === "OPEN" || t.status === "CLOSING"))
        && !state.demoTrades.some(t => t.mint === trade.mint && t.status === "OPEN")) {
      migCleanup(trade.mint, trade.symbol);
    }
  } else {
    if (!state.realTrades.some(t => t.mint === trade.mint && (t.status === "OPEN" || t.status === "CLOSING"))
        && !state.demoTrades.some(t => t.mint === trade.mint && t.status === "OPEN")
        && !state.liveRecordings.has(trade.mint)) {
      unsubscribeToken(trade.mint);
    }
  }
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.status !== "OPEN" || now < trade.expiresAt) continue;
    const price = trade.entryPrice * (1 + (trade.currentPct || 0) / 100);
    const reason = isMig(trade) && (trade.currentPct || 0) >= MIG_EXPIRED_WIN_PCT ? "TP_EXPIRED" : "EXPIRED";
    closeDemoTrade(trade, price, reason, MIG_TP);
  }
}, 30_000);

// ── [v11.9] RESCATADOR DE FEED: precio alternativo para trades huérfanos de ticks ──
setInterval(async () => {
  if (!RESCUE_ON) return;
  const now = Date.now();
  const mints = new Set();
  for (const t of state.demoTrades) if (t.status === "OPEN") mints.add(t.mint);
  for (const t of state.realTrades) if (t.status === "OPEN") mints.add(t.mint);
  for (const mint of mints) {
    // [1-sep] el rescate debe mirar a la fuente que de verdad alimenta la cámara. Con Helius
    // de principal, los ticks de PumpPortal ya no graban: si nos guiáramos por ellos, el
    // rescate no saltaría nunca aunque Helius estuviera mudo, y saltaría de más cuando el
    // que calla es PumpPortal (con Helius dando precios perfectamente).
    let last = lastTickAt.get(mint) || 0;
    if (HELIUS_PRIMARIO) {
      const cH = espia.cuenta.get(mint);
      last = cH ? (cH.ultH || 0) : 0;
    }
    if (now - last < RESCUE_SILENCE_MS) continue;
    if (now - (lastRescueAt.get(mint) || 0) < RESCUE_COOLDOWN_MS) continue;
    lastRescueAt.set(mint, now);
    try {
      const r = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${mint}`, { signal: AbortSignal.timeout(6000) });
      if (!r.ok) continue;
      const px = mejorPrecioDex(await r.json());
      if (!px || px <= 0) continue;
      const secs = Math.round((now - last) / 1000);
      addLog(`🚑 RESCATE FEED: ${shortAddr(mint)} sin ticks ${secs}s — precio DexScreener $${px.toPrecision(4)} alimenta la gestión`, "warn");
      updateDemoTrades(mint, px, "migration");
      liveRecSample(mint, px);   // [27-ago] BUG ARREGLADO: sin esto, un paquete de la unida
                                 // se congelaba mientras el token estuviera mudo (no vendía
                                 // en la roja, no contaba el 💀 ni el reloj del muerto).
      updateRealTrades(mint, px, "migration");
    } catch {}
  }
}, RESCUE_POLL_MS);

// ════════════════════════════════════════════════════════════════
// PUMPPORTAL WEBSOCKET
// ════════════════════════════════════════════════════════════════

// ═══ [30-ago] ESPÍA: Helius en paralelo, solo mide ═══
const espia = {
  ws: null, subs: new Map(),        // mint → id de suscripción
  porSub: new Map(),                // id de suscripción → mint (varios ids pueden apuntar al mismo)
  pend: new Map(),                  // id de petición → mint
  cuenta: new Map(),                // mint → { helius, portal, t0, ultH, ultP }
  reconex: 0, credEst: 0,
};
function espiaConectar() {
  if (!ESPIA_ON || !HELIUS_WS) return;
  try { if (espia.ws) espia.ws.terminate(); } catch {}
  espia.ws = new WebSocket(HELIUS_WS);
  espia.ws.on("open", () => {
    addLog("🕵️ ESPÍA Helius conectado", "info");
    espia.subs.clear(); espia.porSub.clear();
    for (const c1 of espia.cuenta.values()) c1.atada = false;   // al reconectar hay que volver a atar
    for (const mint of state.liveRecordings.keys()) espiaSuscribir(mint);
  });
  espia.ws.on("message", (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== "object") return;
    espia.credEst += raw.length;
    if (m.id && espia.pend.has(m.id)) {            // respuesta a una suscripción
      const mintP = espia.pend.get(m.id); espia.pend.delete(m.id);
      if (m.error) {                                // p.ej. método no disponible en el plan
        if (!espia.errAvisado) { espia.errAvisado = true;
          addLog(`🕵️ ESPÍA: Helius rechazó la suscripción (${m.error.message || JSON.stringify(m.error)}) — el espía no medirá nada`, "warn"); }
        return;
      }
      espia.subs.set(mintP, m.result);
      espia.porSub.set(m.result, mintP);
      return;
    }
    if (m.method !== "transactionNotification" && m.method !== "logsNotification") return;
    // [1-sep] búsqueda por id: al atar un token a su piscina se crea una suscripción nueva y
    // se da de baja la vieja. Si solo mirásemos el mapa mint→id, los mensajes que llegan en ese
    // hueco se perderían. Con el mapa inverso, los dos ids siguen apuntando al mismo token.
    const sub = m.params?.subscription;
    const mint = espia.porSub.get(sub);
    if (!mint) return;
    const c0 = espia.cuenta.get(mint);
    if (!c0) return;
    // [31-ago] con commitment "processed" la misma transacción llega varias veces (re-procesos y
    // bifurcaciones). Sin esto salían ratios de 10-20× que no eran trades de más, eran repes.
    const firma = m.params?.result?.signature;
    if (firma) {
      if (!c0.vistas) c0.vistas = new Set();
      if (c0.vistas.has(firma)) { c0.repes = (c0.repes || 0) + 1; return; }
      c0.vistas.add(firma);
      if (c0.vistas.size > 4000) c0.vistas = new Set([...c0.vistas].slice(-2000));
    }
    c0.helius++; c0.ultH = Date.now();
    // precio = reservas de WSOL / reservas del token en la piscina (las cuentas con más saldo)
    const meta = m.params?.result?.transaction?.meta;
    const post = meta?.postTokenBalances;
    if (!Array.isArray(post) || !post.length) {
      espia.sinSaldos = (espia.sinSaldos || 0) + 1;
      if (espia.sinSaldos === 50 && !espia.conSaldos) addLog("🕵️ ESPÍA: 50 mensajes sin postTokenBalances — probar transactionDetails: \"full\"", "warn");
      return;
    }
    espia.conSaldos = (espia.conSaldos || 0) + 1;
    // [31-ago] FIJAR LA PISCINA. Llegan TODAS las transacciones que tocan el mint (transferencias,
    // bots, agregadores...), y en la mayoría no está la piscina: coger "la cuenta con más tokens"
    // daba precios de ballenas (dif media 70%, pico 1178%). La piscina es el único owner que
    // tiene a la vez una cuenta del token y otra de WSOL; la identificamos una vez y la exigimos.
    const porOwner = new Map();
    for (const b of post) {
      const amt = +(b.uiTokenAmount?.uiAmountString || b.uiTokenAmount?.uiAmount || 0);
      if (!b.owner) continue;
      const o = porOwner.get(b.owner) || { tok: 0, sol: 0 };
      if (b.mint === mint && amt > o.tok) o.tok = amt;
      else if (b.mint === WSOL_MINT && amt > o.sol) o.sol = amt;
      porOwner.set(b.owner, o);
    }
    let tok = 0, sol = 0;
    if (c0.pool) {                                  // piscina ya conocida: solo ella vale
      const o = porOwner.get(c0.pool);
      if (o && o.tok > 0 && o.sol > 0) { tok = o.tok; sol = o.sol; }
    } else {                                        // aún no: el owner con token+WSOL y más tokens
      let mejor = null;
      for (const [owner, o] of porOwner) if (o.tok > 0 && o.sol > 0 && (!mejor || o.tok > mejor.o.tok)) mejor = { owner, o };
      if (mejor) { c0.pool = mejor.owner; tok = mejor.o.tok; sol = mejor.o.sol; espiaAtarAPiscina(mint, c0.pool); }
    }
    if (!(tok > 0 && sol > 0)) return;              // no es una transacción de la piscina
    // ¿han cambiado de verdad las reservas? si no, la transacción tocó la piscina pero no operó
    const clave = tok.toFixed(6) + "/" + sol.toFixed(9);
    if (c0.ultReservas === clave) return;
    c0.ultReservas = clave;
    c0.swaps = (c0.swaps || 0) + 1;                 // esto sí es comparable con un tick de PumpPortal
    // [1-sep] lo que hasta ahora solo daba PumpPortal, sacado de la propia transacción:
    //  · dirección: si la reserva de WSOL de la piscina SUBE, alguien metió SOL → fue una COMPRA
    //  · volumen: cuánto SOL se movió, pasado a dólares
    //  · cartera: el firmante de la transacción
    let hVol = 0, hCompra = false, hTrader = null;
    {
      let solAntes = null;
      const pre = meta?.preTokenBalances;
      if (Array.isArray(pre)) {
        for (const b of pre) if (b.mint === WSOL_MINT && b.owner === c0.pool) {
          solAntes = +(b.uiTokenAmount?.uiAmountString || b.uiTokenAmount?.uiAmount || 0);
        }
      }
      if (solAntes == null && c0.ultSol != null) solAntes = c0.ultSol;
      if (solAntes != null) {
        const d = sol - solAntes;
        hCompra = d > 0;
        hVol = Math.abs(d) * (solPriceUSD || 0);
      }
      c0.ultSol = sol;
      // [1-sep] OJO con la ruta: con transactionDetails:"accounts" las cuentas NO cuelgan de
      // message, sino directamente de transaction.accountKeys. Buscarlas donde no están dejaba
      // hTrader a null y con Helius de fuente principal el wash y los compradores salían a CERO
      // (o sea: el filtro de wash dejaba pasar todo). Probamos las dos formas.
      const tr = m.params?.result?.transaction?.transaction;
      const keys = tr?.accountKeys || tr?.message?.accountKeys;
      if (Array.isArray(keys) && keys.length) {
        const firm = keys.find(k => k && typeof k === "object" && k.signer);
        hTrader = firm ? (firm.pubkey || null)
                : (typeof keys[0] === "string" ? keys[0] : (keys[0]?.pubkey || null));
      }
      if (!hTrader && !espia.avisoSinCartera) {
        espia.avisoSinCartera = true;
        addLog("⚠️ ESPÍA: no encuentro la cartera en los mensajes de Helius — el wash y los compradores quedarían a cero", "warn");
      }
    }
    {
      // la piscina da SOL por token; PumpPortal manda DÓLARES por token (de ahí el 99% de "diferencia"
      // que salía en los primeros informes: era el cambio SOL→USD, no un desacuerdo de precios).
      c0.precioH = (sol / tok) * (solPriceUSD || 0);
      c0.tPrecioH = Date.now();
      const rec0 = state.liveRecordings.get(mint);
      if (rec0 && rec0.lastPrice > 0 && Date.now() - (rec0.lastTickAt || 0) < 15_000) {
        const dif = Math.abs(c0.precioH / rec0.lastPrice - 1) * 100;
        c0.difSum = (c0.difSum || 0) + dif; c0.difN = (c0.difN || 0) + 1;
        if (dif > (c0.difMax || 0)) c0.difMax = dif;
      }
      // La cámara come de Helius: siempre si es la fuente principal; si no, solo cuando
      // PumpPortal lleva callado (relevo). Con cartera y dirección, el wash y el flujo salen igual.
      if (rec0 && !rec0.finished && c0.precioH > 0) {
        // [2-sep] el mudez se mide con el ÚLTIMO TICK DE PUMPPORTAL (c0.ultP), no con
        // rec.lastTickAt: ese lo actualiza el propio relevo al grabar, así que se estrangulaba
        // solo y dejaba una muestra cada 25s exactos. Medido en 7Vwj: 404 puntos en 2 horas,
        // uno cada 25s, y por eso se perdió el pico del token (+331% grabado, ~1M en pump.fun).
        const mudo = Date.now() - (c0.ultP || rec0.t0 || 0) > ESPIA_RELEVO_MS;
        if (HELIUS_PRIMARIO || (ESPIA_ALIMENTA && mudo)) {
          if (mudo && !HELIUS_PRIMARIO) {
            const primera = !c0.relevo;
            c0.relevo = (c0.relevo || 0) + 1;
            if (primera) addLog(`🩺 RELEVO HELIUS: ${rec0.symbol} — PumpPortal lleva ${Math.round((Date.now() - (rec0.lastTickAt || 0)) / 1000)}s mudo, la cámara sigue con Helius`, "warn");
          }
          try { liveRecSample(mint, c0.precioH, hVol, hTrader, hCompra, "helius"); } catch {}
        }
      }
    }
  });
  espia.ws.on("close", () => { espia.reconex++; setTimeout(espiaConectar, 5000); });
  espia.ws.on("error", () => {});
}
let espiaId = 1000;
function espiaSuscribir(mint) {
  if (!ESPIA_ON || espia.ws?.readyState !== WebSocket.OPEN || espia.subs.has(mint)) return;
  const id = ++espiaId;
  espia.pend.set(id, mint);
  // transactionSubscribe (plan Developer) trae los saldos pre/post de cada transacción:
  // con eso se calcula el precio de la piscina sin decodificar el binario del programa.
  espia.ws.send(JSON.stringify({ jsonrpc: "2.0", id, method: "transactionSubscribe",
    params: [{ accountInclude: [mint], failed: false, vote: false },
             { commitment: "processed", encoding: "jsonParsed", transactionDetails: "accounts", maxSupportedTransactionVersion: 0 }] }));
  if (!espia.cuenta.has(mint)) espia.cuenta.set(mint, { helius: 0, portal: 0, t0: Date.now(), ultH: 0, ultP: 0, sym: "" });
}
// [1-sep] al conocer la piscina cambiamos la suscripción del MINT a la PISCINA: solo llegan
// las transacciones que la tocan (los swaps), no transferencias ni bots. Medido en los logs:
// de 44.300 transacciones solo 6.790 eran swaps → alrededor del 85% menos de datos.
function espiaAtarAPiscina(mint, pool) {
  if (!ESPIA_ON || !pool || espia.ws?.readyState !== WebSocket.OPEN) return;
  const c0 = espia.cuenta.get(mint);
  if (!c0 || c0.atada) return;
  c0.atada = true;
  const viejo = espia.subs.get(mint);
  if (viejo) espia.ws.send(JSON.stringify({ jsonrpc: "2.0", id: ++espiaId, method: "transactionUnsubscribe", params: [viejo] }));
  espia.subs.delete(mint);   // (el mapa por id se mantiene: el viejo sigue resolviendo al token)
  const id = ++espiaId;
  espia.pend.set(id, mint);
  espia.ws.send(JSON.stringify({ jsonrpc: "2.0", id, method: "transactionSubscribe",
    params: [{ accountInclude: [pool], failed: false, vote: false },
             { commitment: "processed", encoding: "jsonParsed", transactionDetails: "accounts", maxSupportedTransactionVersion: 0 }] }));
  addLog(`🕵️ ESPÍA: ${shortAddr(mint)} atado a su piscina ${shortAddr(pool)} (menos tráfico)`, "info");
}

function espiaDesuscribir(mint) {
  if (!ESPIA_ON) return;
  const sub = espia.subs.get(mint);
  if (sub && espia.ws?.readyState === WebSocket.OPEN) {
    espia.ws.send(JSON.stringify({ jsonrpc: "2.0", id: ++espiaId, method: "transactionUnsubscribe", params: [sub] }));
  }
  espia.subs.delete(mint);
  for (const [id, mi] of espia.porSub) if (mi === mint) espia.porSub.delete(id);
}
// ping cada minuto: Helius cierra los sockets ociosos a los 10
setInterval(() => { try { if (espia.ws?.readyState === WebSocket.OPEN) espia.ws.ping(); } catch {} }, 60_000);

// informe cada 10 minutos: solo los tokens donde las dos fuentes discrepan
setInterval(() => {
  if (!ESPIA_ON || !espia.cuenta.size) return;
  const filas = [];
  for (const [mint, c0] of espia.cuenta) {
    const vivo = state.liveRecordings.has(mint);
    const mudoP = c0.ultP && Date.now() - c0.ultP > 90_000;
    const mudoH = c0.ultH && Date.now() - c0.ultH > 90_000;
    const dm = c0.difN ? (c0.difSum / c0.difN) : null;
    filas.push(`${shortAddr(mint)}:P${c0.portal}/H${c0.swaps || 0}(${c0.helius}tx)${dm != null ? `·Δ${dm.toFixed(2)}%` : ""}${mudoP && !mudoH ? "⚠️PORTAL-MUDO" : ""}${mudoH && !mudoP ? "·helius-mudo" : ""}${vivo ? "" : "·cerrada"}`);
    if (!vivo && Date.now() - c0.t0 > 30 * 60_000) espia.cuenta.delete(mint);
  }
  const tot = [...espia.cuenta.values()].reduce((a, x) => ({ p: a.p + x.portal, h: a.h + (x.swaps || 0), tx: a.tx + x.helius }), { p: 0, h: 0, tx: 0 });
  const dTot = [...espia.cuenta.values()].filter(x => x.difN);
  const difMedia = dTot.length ? (dTot.reduce((a, x) => a + x.difSum / x.difN, 0) / dTot.length) : null;
  const difPeor = dTot.length ? Math.max(...dTot.map(x => x.difMax || 0)) : null;
  addLog(`[ESPIA] portal=${tot.p} helius-swaps=${tot.h} (de ${tot.tx} tx) · ratio=${tot.p ? (tot.h / tot.p).toFixed(2) : "-"}`
    + (difMedia != null ? ` · PRECIO: dif media ${difMedia.toFixed(2)}% · peor ${difPeor.toFixed(2)}% (${dTot.length} tokens)` : " · PRECIO: sin comparaciones aún")
    + ` · subs=${espia.subs.size} · reconex=${espia.reconex} · ~${Math.round(espia.credEst / 1024)}KB · relevos=${[...espia.cuenta.values()].reduce((a, x) => a + (x.relevo || 0), 0)} | ${filas.slice(0, 10).join(" ")}`, "info");
}, 10 * 60_000);

if (ESPIA_ON) setTimeout(espiaConectar, 8000);

// ═══ [1-ago] VIGILANTE DEL FEED ═══
// Sintoma real (31-jul): el WS quedo zombi a las 6:00 — conectado, sin cerrarse y sin recibir
// nada. El bot siguio "vivo" 9h con migraciones=257 congelado y ~35 entradas perdidas.
// Aqui lo detectamos y forzamos reconexion. Dos relojes: mensajes (cualquier trade) y
// migraciones (mas lentas: a ~1.150/dia, 20min sin ninguna ya es anomalo).
let feedLastMsgAt = Date.now();
let feedLastMigAt = Date.now();
let feedReconexiones = 0;
const FEED_MSG_TIMEOUT_MS = 3 * 60_000;    // 3 min sin ningun mensaje = feed muerto
const FEED_MIG_TIMEOUT_MS = 20 * 60_000;   // 20 min sin ninguna migracion = sospechoso
function feedWatchdog() {
  const ahora = Date.now();
  const sinMsg = ahora - feedLastMsgAt, sinMig = ahora - feedLastMigAt;
  const estado = pumpPortalWs ? pumpPortalWs.readyState : -1;
  let motivo = null;
  if (sinMsg > FEED_MSG_TIMEOUT_MS) motivo = `${Math.round(sinMsg / 60000)}min sin NINGUN mensaje`;
  else if (sinMig > FEED_MIG_TIMEOUT_MS) motivo = `${Math.round(sinMig / 60000)}min sin ninguna migracion`;
  else if (estado !== 1 && estado !== 0) motivo = `socket en estado ${estado}`;
  if (!motivo) return;
  feedReconexiones++;
  addLog(`🚨 FEED ZOMBI: ${motivo} — forzando reconexion (#${feedReconexiones})`, "error");
  feedLastMsgAt = ahora; feedLastMigAt = ahora;   // margen para que la nueva conexion respire
  try { if (pumpPortalWs) { pumpPortalWs.removeAllListeners(); pumpPortalWs.terminate(); } } catch {}
  pumpPortalWs = null;
  setTimeout(connectPumpPortal, 1000);
}
setInterval(feedWatchdog, 60_000);

function connectPumpPortal() {
  addLog("🔌 Conectando PumpPortal…", "info");
  pumpPortalWs = new WebSocket(PUMPPORTAL_WS);

  pumpPortalWs.on("open", () => {
    addLog("✅ PumpPortal conectado", "info");
    feedLastMsgAt = Date.now(); feedLastMigAt = Date.now();   // [1-ago] relojes a cero
    pumpPortalWs.send(JSON.stringify({ method: "subscribeMigration" }));
    const activeMints = [
      ...new Set([
        ...state.migMonitored.keys(),
        ...state.migWatching.keys(),
        ...state.liveRecordings.keys(),
        ...state.realTrades.filter(t => t.status === "OPEN" || t.status === "CLOSING").map(t => t.mint),
        ...state.demoTrades.filter(t => t.status === "OPEN").map(t => t.mint),
      ]),
    ];
    if (activeMints.length) {
      pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: activeMints }));
      addLog(`🔁 Re-suscrito a ${activeMints.length} mints activos tras reconexión`, "info");
    }
  });

  pumpPortalWs.on("message", (raw) => {
    feedLastMsgAt = Date.now();   // [1-ago] pulso del vigilante: cualquier mensaje cuenta
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.txType === "migrate" || msg.txType === "migration") {
        feedLastMigAt = Date.now();   // [1-ago] pulso especifico de migraciones
        migStartWatching({ mint: msg.mint, name: msg.name, symbol: msg.symbol, marketCapSol: msg.marketCapSol });
        return;
      }
      if ((msg.txType === "buy" || msg.txType === "sell") && msg.mint) {
        const price = calcPrice(msg);
        if (price > 0) {
          lastTickAt.set(msg.mint, Date.now());   // [v11.9] pulso del feed para el rescatador
          if (OBSERVER_MODE && state.obsRecordings.has(msg.mint)) { obsSample(msg.mint, price); return; }
          migUpdatePrice(msg.mint, price, msg.solAmount || 0, msg.traderPublicKey || null, msg.txType === "buy");
        }
      }
    } catch {}
  });

  pumpPortalWs.on("close", () => { addLog("🔌 PumpPortal cerrado, reconectando en 5s…", "warn"); setTimeout(connectPumpPortal, 5000); });
  pumpPortalWs.on("error", (e) => addLog(`❌ PumpPortal: ${e.message}`, "error"));
}

// ════════════════════════════════════════════════════════════════
// EXPRESS + WEBSOCKET SERVER
// ════════════════════════════════════════════════════════════════

const app = express();
app.use(cors());
app.use(express.json());

app.get("/health", (req, res) => res.json({ ok: true, uptime: process.uptime() }));

app.get("/api/state", (req, res) => {
  res.json({
    demoTrades: state.demoTrades.slice(0, 100),
    realTrades: state.realTrades.slice(0, 100),
    signals: state.signals.slice(0, 50),
    migMonitored: Array.from(state.migMonitored.values()),
    migWatching: serializeMigWatching(),
    log: state.log.slice(0, 100),
    stats: state.stats,
    risk: riskSnapshot(),
    shadow: state.shadow || null,
    movements: state.movements.slice(0, 200),
    historialHoras: state.historialHoras,
    horaActual: state.horaActual, horaPnlSol: state.horaPnlSol, horaOps: state.horaOps,
    diaPnlSol: state.diaPnlSol, diaOps: state.diaOps, diaInicio: state.diaInicio,
    demoOnly: DEMO_ONLY,
    observerMode: OBSERVER_MODE,
  });
});

app.get("/api/risk", (req, res) => res.json(riskSnapshot()));

app.post("/api/risk/resume", (req, res) => {
  riskState.pausedUntil = 0;
  riskState._dailyLogged = false;
  riskState._windowLogged = false;
  riskState.dailyPnlSol = Math.max(riskState.dailyPnlSol, -RISK.maxDailyLossSol + 0.001);
  addLog("▶️ Kill-switch rearmado manualmente vía API", "warn");
  broadcast({ event: "risk", data: riskSnapshot() });
  res.json({ ok: true, risk: riskSnapshot() });
});

app.get("/api/movement", (req, res) => res.json(state.movements.slice(0, 200)));

// [FIX 26-jul] el calendario del panel crea y borra movimientos (retiros/depósitos)
app.post("/api/movement", (req, res) => {
  const { date, amount, type, note } = req.body || {};
  if (!date || !amount || isNaN(+amount) || !["deposit", "withdrawal"].includes(type)) {
    return res.status(400).json({ error: "date, amount y type (deposit|withdrawal) son obligatorios" });
  }
  const mov = { id: `mov-${Date.now()}-${Math.random().toString(36).slice(2,6)}`, date, amount: +amount, type, note: note || "" };
  state.movements.push(mov);
  broadcast({ event: "newMovement", data: mov });
  saveState();
  res.json({ ok: true, movement: mov });
});

// [31-ago] VEREDICTO MANUAL sobre una rechazada: el humano la mira en pump.fun y dice si el
// filtro acertó ("bien") o se equivocó ("mal"), con el máximo que vio. Queda en el estado y en
// el log ([REJ-VEREDICTO]) para poder medir la tasa de acierto de cada filtro.
app.post("/api/rechazada/veredicto", (req, res) => {
  const { id, veredicto, maxVisto } = req.body || {};
  if (!state.rechazadas) state.rechazadas = [];
  let r = state.rechazadas.find(x => x.id === id);
  if (!r) {
    // [31-ago] si el server se reinició, la tarjeta sigue en el panel pero no en memoria:
    // en vez de fallar en silencio, la damos de alta con lo que manda el propio panel.
    const t = req.body || {};
    if (!id) return res.status(400).json({ error: "falta id" });
    r = { id, mint: t.mint || "", symbol: t.symbol || "???", motivo: t.motivo || "?", ts: t.ts || Date.now(),
          dur: t.dur ?? null, mc: t.mc ?? null, vol: t.vol ?? null, trades: t.trades ?? null,
          sig: t.sig ?? null, mov2s: t.mov2s ?? null, ultimo: t.ultimo ?? null, serie: [], veredicto: null, maxVisto: null };
    state.rechazadas.unshift(r);
    if (state.rechazadas.length > 300) state.rechazadas.length = 300;
  }
  if (!["bien", "mal", null].includes(veredicto)) return res.status(400).json({ error: "veredicto: bien | mal | null" });
  r.veredicto = veredicto;
  r.maxVisto = (maxVisto === "" || maxVisto == null || isNaN(+maxVisto)) ? null : +maxVisto;
  addLog(`[REJ-VEREDICTO] mint=${r.mint} sym=${r.symbol} motivo=${r.motivo} veredicto=${veredicto} maxVisto=${r.maxVisto ?? "n/a"} ultimo=${r.ultimo ?? "n/a"} dur=${r.dur}s mc=${r.mc ?? "n/a"}`, "info");
  broadcast({ event: "migRechazadaVeredicto", data: { id, veredicto: r.veredicto, maxVisto: r.maxVisto } });
  saveState();
  res.json({ ok: true });
});

app.delete("/api/movement/:id", (req, res) => {
  const idx = state.movements.findIndex(m => m.id === req.params.id);
  if (idx < 0) return res.status(404).json({ error: "movimiento no encontrado" });
  const [mov] = state.movements.splice(idx, 1);
  broadcast({ event: "movementDeleted", data: { id: mov.id } });
  saveState();
  res.json({ ok: true });
});

app.get("/export/estado", (req, res) => {
  res.setHeader("Content-Type", "application/json");
  res.setHeader("Content-Disposition", `attachment; filename="estado_${new Date().toISOString().slice(0,10)}.json"`);
  res.send(JSON.stringify({
    fecha: new Date().toISOString(),
    stats: state.stats,
    risk: riskSnapshot(),
    shadow: state.shadow || null,
    demoTrades: state.demoTrades,
    realTrades: state.realTrades,
    historialHoras: state.historialHoras,
  }, null, 2));
});

app.get("/export/shadow", (req, res) => {
  res.setHeader("Content-Type", "application/json");
  res.send(JSON.stringify(state.shadow || {}, null, 2));
});

app.get("/export/listanegra", (req, res) => {
  res.setHeader("Content-Type", "application/json");
  res.send(JSON.stringify({
    abyssCreators: [...abyssCreators],
    creatorHist: [...creatorHist.entries()].map(([w, h]) => ({ wallet: w, ...h })),
  }, null, 2));
});

const server = createServer(app);
const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  frontendClients.add(ws);
  ws.send(JSON.stringify({ event: "fullState", data: {
    rechazadas: (state.rechazadas || []).slice(0, 150),   // [FIX 26-jul] el frontend escucha "fullState", no "init"
    wsStatus: "connected",
    demoTrades: state.demoTrades.slice(0, 100),
    realTrades: state.realTrades.slice(0, 100),
    signals: state.signals.slice(0, 50),
    migMonitored: Array.from(state.migMonitored.values()),
    migWatching: serializeMigWatching(),
    log: state.log.slice(0, 100),
    stats: state.stats,
    risk: riskSnapshot(),
    shadow: state.shadow || null,
    movements: state.movements.slice(0, 200),
    historialHoras: state.historialHoras,
    horaActual: state.horaActual, horaPnlSol: state.horaPnlSol, horaOps: state.horaOps,
    diaPnlSol: state.diaPnlSol, diaOps: state.diaOps, diaInicio: state.diaInicio,
    demoOnly: DEMO_ONLY,
    observerMode: OBSERVER_MODE,
  }}));
  ws.on("close", () => frontendClients.delete(ws));
});

server.listen(PORT, () => {
  addLog(`🚀 Server v11.9 [FIX 26-jul] en puerto ${PORT}`, "info");
  addLog(`💾 Estado en ${STATE_FILE}${_stateInfo.persistent ? " (persistente)" : " ⚠️ NO persistente: se pierde en cada deploy — monta un Volume en /data"}`, _stateInfo.persistent ? "info" : "warn");
  addLog(`🔧 [FIX 26-jul] Cambios: (1) confirmación aborta a -10% · (2) FUERZA cuenta/emite bien · (3) RESUMEN en hora ES · (4) kill-switch ventana persiste · (5) MANDOS: topBal≥${MIG_MIN_TOPBAL} SOL + señal mov2s≥+${MIG_QUAL_MOV2S_MIN}%`, "info");
  if (DEMO_ONLY) addLog("🧪 MODO DEMO ONLY: la wallet real no se toca", "warn");
  if (SHADOW_ON) addLog(`👥 TORNEO DE SOMBRAS ON: ${SHADOW_GRID.length} configs compitiendo`, "info");
  if (MIG_EX2S_ON) addLog(`🎓 EXAMEN 2s ON: tras los filtros espera ${MIG_EX2S_MS/1000}s y solo entra si no cae más de ${MIG_EX2S_MIN}% (suspenso → vuelve al portero)`, "info");
  loadState();
  seedCreators();
  initWallet();
  reconcileStateOnBoot();
  connectPumpPortal();
});

setInterval(saveState, 60_000);
