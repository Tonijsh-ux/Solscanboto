// ═══════════════════════════════════════════════════════════════════════════════════════════
//  EL SERVER DE SOLANA, LISTO PARA REAL · fase 3 · 5-oct-2026
//  = el server en papel + real.js (las órdenes de verdad: Jupiter y PumpPortal, cola, cuadres, protecciones).
//  Sin SOLANA_PRIVATE_KEY es exactamente el server en papel. Con ella, ARRANCA DESARMADO: se arma desde el panel (PANEL_CLAVE).
//  Variables nuevas: SOLANA_PRIVATE_KEY · PANEL_CLAVE · LOTE_SOL · MAX_ABIERTAS (8) · MAX_EXPUESTO_SOL (1) · PERDIDA_DIA_SOL (0.5)
//    · RESERVA_SOL (0.05) · PRIO_ENTRADA / PRIO_SALIDA (0.0005) / PRIO_PANICO (0.001) · SLIP_ENTRADA (15) · TOL_VENTA (10,20,35,50)
//    · FALLOS_SEGUIDOS (4) · REAL_SIMULADO=1 (prueba sin tocar la cadena)
// ─────────────────────────────────────────────────────────────────────────────────────────
//  EL SERVER DE SOLANA EN PAPEL · fase 2 de "Pons en Solana" · 5-oct-2026
//  = el grabador (PumpPortal avisa, Helius lo hace todo) + EL MOTOR DEL SERVER DE PONS (el mismo del artefacto: motor.js)
//    con ⭐⭐ (v3 a los 300 s al 60 % · roja 25 · 1 venta · 1ª v3 a los 10 min al 60 % · soporte 35 %) y el VETO DE BALLENAS
//    (migra en ≤5 s con los 5 mayores ≥80 %). NO OPERA: lleva las cuentas en papel y en real estimado, igual que el artefacto.
//  Variables nuevas: LOTE_SOL (0.1) · NAC_ESPERA_S (10: sin nacimiento a tiempo, no entra) · ESTRATEGIA (JSON de mandos; de serie ⭐⭐)
//    · VETO_BALLENA (1) · ESTADO_FILE (/data/solana_papel.json)
// ─────────────────────────────────────────────────────────────────────────────────────────
//  (lo de abajo es el grabador de siempre)
//  EL GRABADOR DE SOLANA · fase 1 de "Pons en Solana" · 3-oct-2026
//
//  Qué hace: SOLO GRABA. No compra ni vende nada. Para cada token que migra de pump.fun a PumpSwap:
//   1. PumpPortal (gratis) avisa de la migración: mint + firma de la transacción.
//   2. Con esa firma, Helius (RPC) lee la transacción de la migración: de ahí salen la PISCINA y el
//      precio inicial (reservas de SOL / reservas del token) = el "precio al graduar" de Pons.
//   3. Helius (WebSocket mejorado, transactionSubscribe sobre la piscina) manda cada swap: con los
//      saldos de después se saca el precio y se graba un punto de la curva (segundo:% sobre el inicial).
//   4. En paralelo, el "nacimiento": la API de pump.fun da el símbolo, el creador, cuándo nació y
//      todas las compras de la curva de bonos ANTES de migrar → compradores, tiempo hasta migrar,
//      lo que tienen los 5 mayores, la mayor compra, lo del creador y lo revendido (como el detector
//      de nacimiento de Pons; mismo orden de campos en nac=).
//   5. Al cabo de CAMARA_MIN minutos (o antes si el token muere), escribe UNA línea [SOLREC] con la curva
//      entera: la lee herramientas/mete_registro_solana.py y a partir de ahí todo es igual que en Pons.
//
//  Variables: HELIUS_API_KEY (obligatoria) · PUMPPORTAL_API_KEY (opcional; sin ella también funciona)
//   · CAMARA_MIN (90) · MAX_SUBS_WS (40, suscripciones por conexión de Helius) · WS_MAX (4 conexiones)
//   · PORT (8080) · LATIDO_S (15, un punto aunque no haya swaps) · MUERTO_MIN (10, cierra antes si lleva
//     ese tiempo a −95 % sin swaps) · SOLO_LOG=1 (menos detalle)
// ═══════════════════════════════════════════════════════════════════════════════════════════
import http from "http";
import WebSocket from "ws";

const HELIUS_API_KEY = (process.env.HELIUS_API_KEY || "").trim();
const PUMPPORTAL_API_KEY = (process.env.PUMPPORTAL_API_KEY || "").trim();
const CAMARA_MIN = +(process.env.CAMARA_MIN || 60);
// [6-oct] LA CÁMARA LARGA: si al cumplir los 60 min el token sigue ≥ LARGA_SI % (como EGO), se sigue grabando (y el bot llevándolo) hasta CAMARA_LARGA_MIN
const LARGA_SI = +(process.env.LARGA_SI || 300), CAMARA_LARGA_MIN = +(process.env.CAMARA_LARGA_MIN || 240);
// y si a las 4 h sigue ≥ MUY_LARGA_SI % (como CRAWL, que a las 14 h hizo otro tramo hasta ×180), hasta CAMARA_MUY_LARGA_MIN (24 h)
const MUY_LARGA_SI = +(process.env.MUY_LARGA_SI || 1000), CAMARA_MUY_LARGA_MIN = +(process.env.CAMARA_MUY_LARGA_MIN || 1440);
const GRABA_MAYHEM = process.env.GRABA_MAYHEM === "1";   // de serie, los Mayhem (supply > 1,5e9) no se graban: ahorran créditos y no se operarían
const MAX_SUBS_WS = +(process.env.MAX_SUBS_WS || 40);
const WS_MAX = +(process.env.WS_MAX || 4);
const PORT = +(process.env.PORT || 8080);
const LATIDO_S = +(process.env.LATIDO_S || 15);
const MUERTO_MIN = +(process.env.MUERTO_MIN || 10);
const SOLO_LOG = process.env.SOLO_LOG === "1";
const POLL_S = +(process.env.POLL_S || 3);           // [4-oct] cada cuántos segundos se leen los saldos de la piscina (como la cámara de Pons)
const MODO_WS = process.env.MODO_WS !== "0";          // [5-oct] de serie el WebSocket swap a swap: el motor ve los MISMOS puntos que las curvas con que se simula (MODO_WS=0 = sondeo cada 3 s)
const WSOL = "So11111111111111111111111111111111111111112";
const RPC = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const HELIUS_WS = `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const PP_WS = PUMPPORTAL_API_KEY ? `wss://pumpportal.fun/api/data?api-key=${PUMPPORTAL_API_KEY}` : "wss://pumpportal.fun/api/data";

if (!HELIUS_API_KEY) { console.log("❌ falta HELIUS_API_KEY: sin ella no hay precios. Me paro."); process.exit(1); }
// ═══ [5-oct] EL MOTOR (el del server de Pons, en motor.js: el mismo texto que corre el artefacto) ═══
import fs from "fs"; import path from "path"; import { fileURLToPath } from "url"; import * as REAL from "./real.js";
const DIR = path.dirname(fileURLToPath(import.meta.url));
const W = new Function(fs.readFileSync(path.join(DIR, "motor.js"), "utf-8") + ";return {construyeMotor, netoReal};")();
const ESTRELLA2 = { v3Corte2: 60, mv: 25, maxvend: 1, v3Seg: 600, v3Corte: 60, soporteSigue: 35 };
let ESTRATEGIA = ESTRELLA2; try { if (process.env.ESTRATEGIA) ESTRATEGIA = JSON.parse(process.env.ESTRATEGIA); } catch (e) { console.log("⚠️ ESTRATEGIA no es un JSON válido: uso ⭐⭐"); }
// ═══ [7-oct] EL PLAN POR REGLAS (como en Pons): la primera regla que encaja decide; acc −1 = no entrar; cada cfg distinta tiene su motor ═══
//   de serie: «robusto + stop + vetos v2» (7-oct). PLAN_JSON (JSON con {reglas:[…]}) para otro; PLAN=0 → una sola estrategia (ESTRATEGIA, de serie ⭐⭐)
const PLAN_DE_SERIE = {"nombre": "limpio 8-oct", "reglas": [{"ten": [0, 5], "seg": null, "top5": null, "horas": [], "acc": -1, "cfg": {}, "nota": "5 o menos tenedores (creador): no entrar"}, {"ten": null, "seg": null, "top5": null, "horas": [20, 21, 22, 23], "acc": -1, "cfg": {}, "nota": "de 20 a 24 h: no entrar"}, {"ten": null, "seg": [6, 60], "top5": null, "horas": [], "acc": -1, "cfg": {}, "nota": "migra en 6-60 s: no entrar"}, {"ten": null, "seg": null, "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 40, "maxlot": 8, "soporteSigue": 30, "escN": 20}, "nota": "todos: v3 a los 5 min 40, 8 lotes, seguir la curva 30, escalera 20"}]};   // [8-oct] el plan LIMPIO: elegido con unos días y examinado en otros (validación cruzada); la v4 (83 reglas) estaba sobreajustada
let PLAN = process.env.PLAN === "0" ? null : PLAN_DE_SERIE; try { if (process.env.PLAN_JSON) PLAN = { nombre: "PLAN_JSON", ...JSON.parse(process.env.PLAN_JSON) }; } catch (e) { console.log("⚠️ PLAN_JSON no es un JSON válido: uso el plan de serie"); }
const MOTORES = new Map(); const EXTRAS = { encola: (tipo, rec, datos) => REAL.encola(tipo, rec, datos) };
function motorDe(cfg) { const k = JSON.stringify(cfg); if (!MOTORES.has(k)) { const m = W.construyeMotor({ cazDesde: 1e9, ...cfg }, EXTRAS); if (!m) { console.log("❌ el motor no arranca. Me paro."); process.exit(1); } MOTORES.set(k, m); } return MOTORES.get(k); }
const MOTOR = motorDe(ESTRATEGIA);   // el de ESTRATEGIA (⭐⭐): para cuando no hay plan o ninguna regla encaja
const horaEsp = (ms) => +new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", hour: "2-digit", hourCycle: "h23" }).format(new Date(ms));
function reglaDe(c) { if (!PLAN) return null; const n = c.nac || {}; const seg = n.seg, ten = n.compradores, top5 = n.top5, hg = horaEsp(c.t0);
  for (let i = 0; i < PLAN.reglas.length; i++) { const r = PLAN.reglas[i]; if (r.ten && !(ten != null && ten >= r.ten[0] && ten <= r.ten[1])) continue; if (r.seg && !(seg != null && seg >= r.seg[0] && seg <= r.seg[1])) continue;
    if (r.top5 && !(top5 != null && top5 >= r.top5[0] && top5 <= r.top5[1])) continue; if (r.horas && r.horas.length && !r.horas.includes(hg)) continue; return { i, r }; } return null; }
const LOTE_SOL = +(process.env.LOTE_SOL || 0.1), K = LOTE_SOL / 0.01;   // el motor cuenta en lotes de 0,01: se escala
const NAC_ESPERA_S = +(process.env.NAC_ESPERA_S || 10), VETO_BALLENA = process.env.VETO_BALLENA !== "0";
const ESTADO_FILE = process.env.ESTADO_FILE || (fs.existsSync("/data") ? "/data/solana_papel.json" : "/tmp/solana_papel.json");
const P = { papel: 0, real: 0, cerradas: 0, ganadas: 0, abiertas: 0, entradas: 0, vetadas: 0, sinNac: 0, ultimas: [], desde: Date.now(), porMotivo: {}, maxExpuesto: 0 };
// ═══ [7-oct] EL DIARIO DE TOKENS PARA EL PANEL: cada token con su regla o su veto, dónde entró y salió, y lo que hizo después (36 h, en memoria) ═══
const DIARIO = new Map(), DIARIO_H = 36;
function apunta(c, extra = {}) { let d = DIARIO.get(c.mint); if (!d) { const n = c.nac || {}; d = { mint: c.mint, sym: c.sym, t0: c.t0, ten: n.compradores ?? null, seg: n.seg ?? null, top5: n.top5 ?? null, regla: null, nota: null, veto: null, ops: [], max: null, fin: null, puntos: null }; DIARIO.set(c.mint, d); } Object.assign(d, extra); if (c.sym && d.sym !== c.sym) d.sym = c.sym; return d; }
function resumenCurva(P0) { const n = P0.length; if (!n) return []; const paso = Math.max(1, Math.ceil(n / 240)); const out = []; for (let i = 0; i < n; i += paso) out.push([Math.round(P0[i].t), +P0[i].p.toFixed(1)]); const u = P0[n - 1]; out.push([Math.round(u.t), +u.p.toFixed(1)]); return out; }
setInterval(() => { const lim = Date.now() - DIARIO_H * 3.6e6; for (const [k, d] of DIARIO) if (d.t0 < lim) DIARIO.delete(k); }, 10 * 60e3);
// [7-oct] las cuentas en papel van con el PLAN: si el guardado era de otro plan (o del server viejo), se empieza de cero (lo de antes queda en P.anterior)
const PLAN_ID = PLAN ? "plan:" + PLAN.nombre : "estrategia:" + JSON.stringify(ESTRATEGIA); P.plan = PLAN_ID;
try { const g = JSON.parse(fs.readFileSync(ESTADO_FILE, "utf-8"));
  if (g.plan === PLAN_ID) Object.assign(P, g);
  else { P.anterior = { plan: g.plan || "el server viejo (⭐⭐)", desde: g.desde, hasta: Date.now(), cerradas: g.cerradas, real: g.real, papel: g.papel }; console.log(`📊 cuentas en papel NUEVAS para «${PLAN ? PLAN.nombre : "ESTRATEGIA"}» · las de antes (${P.anterior.plan}: ${g.cerradas || 0} cerradas, real ${(g.real || 0).toFixed(2)} SOL, con las operaciones falsas del server viejo) quedan aparte`); } } catch {}
const guardaP = () => { try { fs.writeFileSync(ESTADO_FILE, JSON.stringify({ ...P, ultimas: P.ultimas.slice(0, 300) })); } catch (e) { } };
setInterval(guardaP, 60e3);

// ── el estado ──
const S = { creadores: new Map(), arranque: Date.now(), solUsd: 0, solUsdT: 0, migs: 0, ultimaMig: 0, curvas: 0, swaps: 0, bytes: 0, errores: 0,
  camaras: new Map(),        // mint → la cámara
  vistas: new Set(),         // mints ya vistos (para no repetir)
  pp: { ws: null, ok: false, reconexiones: 0 }, hel: [], nacHechos: 0, nacFallos: 0, ultimas: [] };
const log = (m) => console.log(m);
const corto = (s) => s ? s.slice(0, 4) + "…" + s.slice(-4) : "?";
const f1 = (x) => Math.round(x * 10) / 10, f2 = (x) => Math.round(x * 100) / 100;

// ── el precio de SOL (CoinGecko; si falla, Jupiter) ──
async function precioSol() {
  try { const r = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd", { signal: AbortSignal.timeout(6000) });
    const j = await r.json(); const p = +j?.solana?.usd; if (p > 0) { S.solUsd = p; S.solUsdT = Date.now(); return; } } catch {}
  try { const r = await fetch(`https://lite-api.jup.ag/price/v2?ids=${WSOL}`, { signal: AbortSignal.timeout(6000) });
    const j = await r.json(); const p = +j?.data?.[WSOL]?.price; if (p > 0) { S.solUsd = p; S.solUsdT = Date.now(); } } catch {}
}

// ── RPC de Helius (sin librerías: JSON-RPC a pelo) ──
async function rpc(method, params, ms = 8000) { S.llamadas = (S.llamadas || 0) + 1; S.porMetodo = S.porMetodo || {}; S.porMetodo[method] = (S.porMetodo[method] || 0) + 1;
  const r = await fetch(RPC, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }), signal: AbortSignal.timeout(ms) });
  const j = await r.json(); if (j.error) throw new Error(j.error.message || JSON.stringify(j.error)); return j.result;
}

// las reservas de la piscina en una transacción: el único owner que tiene a la vez una cuenta del token y otra de WSOL
function reservasDe(post, mint, poolConocida) {
  const porOwner = new Map();
  for (const b of post || []) { if (!b.owner) continue; const amt = +(b.uiTokenAmount?.uiAmountString || b.uiTokenAmount?.uiAmount || 0);
    const o = porOwner.get(b.owner) || { tok: 0, sol: 0 }; if (b.mint === mint && amt > o.tok) o.tok = amt; else if (b.mint === WSOL && amt > o.sol) o.sol = amt; porOwner.set(b.owner, o); }
  if (poolConocida) { const o = porOwner.get(poolConocida); return (o && o.tok > 0 && o.sol > 0) ? { pool: poolConocida, ...o } : null; }
  let mejor = null; for (const [owner, o] of porOwner) if (o.tok > 0 && o.sol > 0 && (!mejor || o.tok > mejor.tok)) mejor = { pool: owner, ...o };
  return mejor;
}

// las dos cuentas de la piscina (la del token y la de WSOL): con ellas basta un accountSubscribe por cuenta, mucho más ligero
function vaultsDe(tx, mint, pool) { const post = tx?.meta?.postTokenBalances || []; const keys = tx?.transaction?.message?.accountKeys || tx?.transaction?.accountKeys || [];
  const pk = (i) => { const k = keys[i]; return typeof k === "string" ? k : (k?.pubkey || null); }; let base = null, quote = null;
  // [5-oct] la cuenta de cada moneda CON MÁS SALDO de la piscina (antes cogía la última: si la piscina tiene otra cuenta pequeña del token, el precio salía disparado)
  let mb = -1, mq = -1; for (const b of post) { if (b.owner !== pool) continue; const amt = +(b.uiTokenAmount?.uiAmountString || 0); if (b.mint === mint && amt > mb) { mb = amt; base = pk(b.accountIndex); } else if (b.mint === WSOL && amt > mq) { mq = amt; quote = pk(b.accountIndex); } }
  return (base && quote) ? { base, quote } : null; }
async function vaultsPorDueno(pool, mint) { const una = async (m) => { const r = await rpc("getTokenAccountsByOwner", [pool, { mint: m }, { encoding: "jsonParsed", commitment: "confirmed" }]);
    let mejor = null; for (const x of r?.value || []) { const a = +(x.account?.data?.parsed?.info?.tokenAmount?.uiAmountString || 0); if (!mejor || a > mejor.a) mejor = { k: x.pubkey, a }; } return mejor ? mejor.k : null; };
  const [base, quote] = await Promise.all([una(mint), una(WSOL)]); return (base && quote) ? { base, quote } : null; }
// la transacción de la migración: la piscina y el precio inicial (con reintentos, porque PumpPortal avisa en cuanto la ve)
async function leeMigracion(firma, mint) {
  for (let i = 0; i < 6; i++) {
    try { const tx = await rpc("getTransaction", [firma, { encoding: "jsonParsed", commitment: "confirmed", maxSupportedTransactionVersion: 0 }]);
      if (tx) { const r = reservasDe(tx.meta?.postTokenBalances, mint, null); if (r) { r.vaults = vaultsDe(tx, mint, r.pool); return r; } return null; } } catch (e) { if (i === 5) throw e; }
    await new Promise(r => setTimeout(r, 1500));
  }
  return null;
}

// ── las conexiones con Helius (varias, con un tope de suscripciones cada una) ──
// [5-oct] LAS MIGRACIONES DIRECTAS DE LA CADENA: Helius manda la transacción de la migración en cuanto la ve (el programa de migración
// de pump.fun), con la piscina y las reservas dentro → no hay que esperar al aviso de PumpPortal ni leer la transacción después.
// PumpPortal sigue de respaldo: la que llegue primero abre la cámara, y se apunta cuánto se adelantó una a la otra.
const MIG_PROGRAM = "39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg";
const MIG_DIRECTA = process.env.MIG_DIRECTA !== "0";
const MIGF = { helius: 0, pumpportal: 0, adelanto: [], soloHelius: 0, soloPP: 0 };   // quién llegó primero y por cuánto
const llegadas = new Map();   // mint → { fuente, t }
function apuntaLlegada(mint, fuente) { const a = llegadas.get(mint); const ahora = Date.now();
  if (!a) { llegadas.set(mint, { fuente, t: ahora }); MIGF[fuente]++; if (llegadas.size > 3000) llegadas.delete(llegadas.keys().next().value); return true; }
  if (a.fuente !== fuente && !a.segunda) { a.segunda = true; const ms = ahora - a.t; MIGF.adelanto.push(a.fuente === "helius" ? ms : -ms); if (MIGF.adelanto.length > 500) MIGF.adelanto.shift(); }
  return false; }
function heliusNueva() {
  const H = { ws: null, subs: new Map(), porSub: new Map(), pend: new Map(), nId: 0, ok: false, reconexiones: 0 };
  const abre = () => {
    const ws = new WebSocket(HELIUS_WS); H.ws = ws;
    ws.on("open", () => { H.ok = true; log(`🔌 Helius: conexión ${S.hel.indexOf(H) + 1} abierta`);
      if (MIG_DIRECTA && S.hel.indexOf(H) === 0) { const id = ++H.nId; H.pendMig = id; ws.send(JSON.stringify({ jsonrpc: "2.0", id, method: "transactionSubscribe", params: [{ accountInclude: [MIG_PROGRAM], failed: false, vote: false }, { commitment: "processed", encoding: "jsonParsed", transactionDetails: "accounts", maxSupportedTransactionVersion: 0 }] })); } for (const c of S.camaras.values()) if (c.hel === H && !c.fin) { H.subs.delete(c.mint); suscribe(H, c); } });   // al reconectar, las cámaras vivas se vuelven a suscribir
    ws.on("message", (raw) => { S.bytes += raw.length; let m; try { m = JSON.parse(raw); } catch { return; } mensajeHelius(H, m); });
    ws.on("close", () => { H.ok = false; H.reconexiones++; H.subs.clear(); H.porSub.clear(); H.pend.clear(); setTimeout(abre, Math.min(30000, 2000 * H.reconexiones)); });
    ws.on("error", (e) => { S.errores++; log(`⚠️ Helius: ${e.message}`); });
  };
  abre(); S.hel.push(H); return H;
}
function heliusConHueco() { for (const H of S.hel) if (H.ok && H.subs.size < MAX_SUBS_WS) return H; if (S.hel.length < WS_MAX) return heliusNueva(); return S.hel.reduce((a, b) => (a.subs.size <= b.subs.size ? a : b)); }
function suscribe(H, c) { if (!H.ok || H.ws.readyState !== WebSocket.OPEN) return;
  if (c.vaults) {   // ligero: solo los saldos de las dos cuentas de la piscina
    for (const cual of ["base", "quote"]) { const id = ++H.nId; H.pend.set(id, { mint: c.mint, cual, tipo: "cuenta" });
      H.ws.send(JSON.stringify({ jsonrpc: "2.0", id, method: "accountSubscribe", params: [c.vaults[cual], { encoding: "jsonParsed", commitment: "processed" }] })); }
  } else {          // sin piscina aún: las transacciones del mint, solo hasta el 1er swap
    const id = ++H.nId; H.pend.set(id, { mint: c.mint, cual: "tx", tipo: "tx" });
    H.ws.send(JSON.stringify({ jsonrpc: "2.0", id, method: "transactionSubscribe", params: [{ accountInclude: [c.mint], failed: false, vote: false }, { commitment: "processed", encoding: "jsonParsed", transactionDetails: "accounts", maxSupportedTransactionVersion: 0 }] })); } }
function desuscribe(H, c) { const L = H.subs.get(c.mint); if (!L) return; H.subs.delete(c.mint);
  for (const { id, tipo } of L) { H.porSub.delete(id); if (H.ok && H.ws.readyState === WebSocket.OPEN) H.ws.send(JSON.stringify({ jsonrpc: "2.0", id: ++H.nId, method: tipo === "cuenta" ? "accountUnsubscribe" : "transactionUnsubscribe", params: [id] })); } }

function mensajeHelius(H, m) {
  if (m.id && m.id === H.pendMig) { H.pendMig = null; if (m.error) log(`⚠️ Helius no deja escuchar las migraciones (${m.error.message || JSON.stringify(m.error)}): solo PumpPortal`); else { H.subMig = m.result; log(`📡 escuchando las migraciones directamente en la cadena (Helius) · PumpPortal de respaldo`); } return; }
  if (m.method === "transactionNotification" && H.subMig != null && m.params?.subscription === H.subMig) { migracionDirecta(m.params.result); return; }
  if (m.id && H.pend.has(m.id)) { const { mint, cual, tipo } = H.pend.get(m.id); H.pend.delete(m.id);
    if (m.error) { S.errores++; log(`⚠️ Helius rechazó la suscripción de ${corto(mint)} (${cual}): ${m.error.message || JSON.stringify(m.error)}`); return; }
    const L = H.subs.get(mint) || []; L.push({ id: m.result, cual, tipo }); H.subs.set(mint, L); H.porSub.set(m.result, { mint, cual }); return; }
  if (m.method === "accountNotification") {   // un saldo de una de las dos cuentas de la piscina
    const q = H.porSub.get(m.params?.subscription); if (!q) return; const c = S.camaras.get(q.mint); if (!c || c.fin) return;
    const amt = +(m.params?.result?.value?.data?.parsed?.info?.tokenAmount?.uiAmountString || m.params?.result?.value?.data?.parsed?.info?.tokenAmount?.uiAmount || 0); if (!(amt >= 0)) return;
    if (q.cual === "base") c.resBase = amt; else c.resQuote = amt;
    clearTimeout(c.timerPrecio); c.timerPrecio = setTimeout(() => precioDesdeReservas(c), 150);   // las dos cuentas cambian a la vez: se espera a tener las dos
    return; }
  if (m.method !== "transactionNotification") return;
  const q0 = H.porSub.get(m.params?.subscription); const mint = q0 && q0.mint; if (!mint) return; const c = S.camaras.get(mint); if (!c || c.fin) return;
  const firma = m.params?.result?.signature; if (firma) { if (c.vistas.has(firma)) return; c.vistas.add(firma); if (c.vistas.size > 4000) c.vistas = new Set([...c.vistas].slice(-2000)); }
  const meta = m.params?.result?.transaction?.meta; const r = reservasDe(meta?.postTokenBalances, mint, c.pool); if (!r) return;
  if (!c.pool) { c.pool = r.pool; const tx = m.params?.result?.transaction; c.vaults = vaultsDe({ meta, transaction: tx?.transaction || tx }, mint, c.pool);
    log(`🏊 ${c.sym}: piscina ${corto(c.pool)} descubierta en el 1er swap${c.vaults ? " · paso a vigilar solo sus dos cuentas" : ""}`); desuscribe(H, c); suscribe(H, c); }
  const clave = r.tok.toFixed(6) + "/" + r.sol.toFixed(9); if (c.ultReservas === clave) return; c.ultReservas = clave;
  const precio = r.sol / r.tok; if (!(precio > 0)) return;
  if (!c.precioIni) { c.precioIni = precio; c.ref = "1er swap"; }
  // dirección y cartera (para los primeros minutos)
  let compra = null; const pre = meta?.preTokenBalances; let solAntes = null; if (Array.isArray(pre)) for (const b of pre) if (b.mint === WSOL && b.owner === c.pool) solAntes = +(b.uiTokenAmount?.uiAmountString || b.uiTokenAmount?.uiAmount || 0);
  if (solAntes != null) compra = r.sol > solAntes; const tr = m.params?.result?.transaction?.transaction; const keys = tr?.accountKeys || tr?.message?.accountKeys;
  const firmante = Array.isArray(keys) ? (keys.find(k => k && typeof k === "object" && k.signer)?.pubkey || (typeof keys[0] === "string" ? keys[0] : keys[0]?.pubkey) || null) : null;
  c.swaps++; S.swaps++; c.ultSwap = Date.now();
  const t = (Date.now() - c.t0) / 1000;
  if (t <= 60 && firmante) { c.min1.swaps++; if (compra === true) { c.min1.compradores.add(firmante); c.min1.solIn += (r.sol - (solAntes ?? r.sol)); } else if (compra === false) { c.min1.vendedores.add(firmante); c.min1.solOut += ((solAntes ?? r.sol) - r.sol); } }
  punto(c, t, precio);
}
function precioDesdeReservas(c) { if (c.fin || !(c.resBase > 0) || !(c.resQuote > 0)) return; const precio = c.resQuote / c.resBase; if (!(precio > 0)) return;
  // [8-oct] base × SOL de la piscina NO es fijo en PumpSwap: va bajando con las operaciones y salta cuando alguien pone o quita liquidez.
  //   El filtro del 5-oct (no usar el precio si base×SOL se alejaba más de ×0,5-×5 del de la migración) dejaba CONGELADA la curva de 8 de cada 10 tokens
  //   (581 de 718 del 7 al 8-oct): el server dejaba de ver el precio. Ahora solo se aparta una lectura que salta DE GOLPE (más de ×3 frente a la anterior)
  //   hasta que la siguiente la confirma: si era un cambio de liquidez de verdad, la siguiente sale parecida y se acepta; si era un error de lectura, se pierde solo esa.
  { const k = c.resBase * c.resQuote; if (!c.kUlt) { const r0 = c.reservasIni; c.kUlt = r0 && r0.tok > 0 && r0.sol > 0 ? r0.tok * r0.sol : k; }
    const salto = k / c.kUlt;
    if (salto > 3 || salto < 1 / 3) {
      if (c.kPend && Math.abs(k / c.kPend - 1) < 0.3) { c.kPend = null; c.liquidez = (c.liquidez || 0) + 1; if (c.liquidez <= 3) log(`💧 ${c.sym}: cambio de liquidez en la piscina (base×SOL ×${salto.toFixed(2)}) · confirmado, sigo con el precio nuevo`); }
      else { c.kPend = k; c.raros = (c.raros || 0) + 1; if (c.raros === 1 || c.raros % 50 === 0) log(`⚠️ ${c.sym}: lectura rara (base×SOL ×${salto.toFixed(2)} de golpe) · espero a la siguiente${c.raros === 1 ? ' · reviso las cuentas de la piscina' : ''}`);
        if (c.raros === 1 && c.pool && typeof vaultsPorDueno === 'function') vaultsPorDueno(c.pool, c.mint).then(v => { if (v && (v.base !== c.vaults?.base || v.quote !== c.vaults?.quote)) { log(`🔧 ${c.sym}: cuentas de la piscina corregidas`); c.vaults = v; c.resBase = 0; c.resQuote = 0; c.kUlt = 0; c.kPend = null; if (c.hel) { desuscribe(c.hel, c); suscribe(c.hel, c); } } }).catch(() => {});
        return; } }
    else c.kPend = null;
    c.kUlt = k; }
  const clave = c.resBase.toFixed(6) + "/" + c.resQuote.toFixed(9); if (c.ultReservas === clave) return;
  const subeSol = c.ultQuote != null ? c.resQuote > c.ultQuote : null; const dSol = c.ultQuote != null ? Math.abs(c.resQuote - c.ultQuote) : 0; c.ultQuote = c.resQuote; c.ultReservas = clave;
  if (!c.precioIni) { c.precioIni = precio; c.ref = "1er swap"; }
  c.swaps++; S.swaps++; c.ultSwap = Date.now(); const t = (Date.now() - c.t0) / 1000;
  if (t <= 60) { c.min1.swaps++; if (subeSol === true) c.min1.solIn += dSol; else if (subeSol === false) c.min1.solOut += dSol; }
  punto(c, t, precio); }
function punto(c, t, precio) { const p = (precio / c.precioIni - 1) * 100; const ult = c.puntos[c.puntos.length - 1];
  if (ult && t - ult.t < 1) { ult.p = p; return; }            // como mucho un punto por segundo
  c.puntos.push({ t, p }); alMotor(c, c.puntos.length - 1); /* [5-oct] los anteriores ya no cambian: al motor */ if (p > c.max.p) c.max = { t, p }; if (p < c.min.p) c.min = { t, p }; c.ultPrecio = precio; }

// el veto: la ballena (migra en ≤5 s con los 5 mayores ≥80 %) no se opera; el resto, al motor
function decide(c) { if (c.fin || c.rec || c.veto) return; clearTimeout(c.timerNac); const n = c.nac || {};
  if (VETO_BALLENA && n.seg != null && n.top5 != null && n.seg <= 5 && n.top5 >= 80) { c.veto = "ballena"; P.vetadas++; log(`🐋 ${c.sym}: ballena (${n.seg} s hasta migrar, los 5 mayores ${n.top5} %) · no se opera`); return; }
  const rg = reglaDe(c); if (rg && rg.r.acc < 0) { c.veto = "regla " + (rg.i + 1); c.regla = rg.i + 1; P.vetadas++; apunta(c, { regla: rg.i + 1, nota: rg.r.nota || "no entrar", veto: rg.r.nota || "no entrar" }); log(`⛔ ${c.sym}: regla ${rg.i + 1} (${rg.r.nota || "no entrar"}) · ${n.seg ?? "?"} s · ${n.compradores ?? "?"} tenedores · 5 mayores ${n.top5 ?? "?"} % · ${horaEsp(c.t0)}h · no se opera`); return; }
  c.motor = rg ? motorDe(rg.r.cfg) : MOTOR; c.regla = rg ? rg.i + 1 : 0; apunta(c, { regla: c.regla, nota: rg ? rg.r.nota || "" : "⭐⭐" });
  arrancaMotor(c); log(`🎛️ ${c.sym}: al motor (${rg ? "regla " + (rg.i + 1) + ": " + (rg.r.nota || "") : "⭐⭐"}) · ${n.seg != null ? n.seg + " s hasta migrar" : "?"} · ${n.compradores ?? "?"} tenedores · los 5 mayores ${n.top5 ?? "?"} % · ${horaEsp(c.t0)}h`); }
// [5-oct] lo de papel de un token que aún no se ha cuadrado con lo real (para el cuadre de real.js)
const CERRADAS = new Map();
function papelDe(mint) { const c = S.camaras.get(mint) || CERRADAS.get(mint); if (!c) return null; const d = { papel: (c.papel || 0) - (c.pC || 0), real: (c.real || 0) - (c.rC || 0) }; c.pC = c.papel || 0; c.rC = c.real || 0; return d; }
// ═══ [5-oct] EL MOTOR CON CADA TOKEN ═══
// c.rec = el token para el motor (como en el server de Pons) · c.mi = cuántos puntos ha visto · se le dan los puntos ya definitivos
function arrancaMotor(c) { if (c.rec || c.fin || c.veto) return;
  c.rec = { poolId: c.mint, token: c.mint, pool: c.pool, pumpswap: c.esPumpSwap, symbol: c.sym, feePct: 0.25, precioIni: 1, supply: c.supply || 1e9, puntos: [], est: null, desliz: 0.58, gradMs: c.t0, dsV3: 0.58, v3: null };
  c.motor = c.motor || MOTOR; c.mi = 0; P.entradas++; try { if (c.puntos.length) { c.rec.puntos.push({ t: 0, p: 0 }); c.motor.estrategiaInit(c.rec, 1); c.mi = 1; } } catch (e) { log(`⚠️ motor (inicio) ${c.sym}: ${e.message}`); }
  alMotor(c, c.puntos.length - 1); }
function alMotor(c, hasta) { if (!c.rec || c.fin && hasta < c.puntos.length) { if (!c.rec) return; }
  if (!c.mi && c.puntos.length) { c.rec.puntos.push({ t: 0, p: 0 }); try { (c.motor || MOTOR).estrategiaInit(c.rec, 1); } catch (e) {} c.mi = 1; }
  for (; c.mi < hasta; c.mi++) { const q = c.puntos[c.mi]; const pr = +q.p.toFixed(2), tr = Math.round(q.t); c.rec.puntos.push({ t: tr, p: pr });   // [5-oct] igual que en la línea [SOLREC]
    try { (c.motor || MOTOR).estrategiaTick(c.rec, 1 + pr / 100, pr, tr); } catch (e) { S.errores++; if (S.errores < 20) log(`⚠️ motor (tick) ${c.sym}: ${e.message}`); } }
  cuentas(c); }
// lo que ha cerrado el motor: papel (lo del motor) y real estimado (como el artefacto), en SOL
function cuentas(c) { const L = (c.motor || MOTOR).sb.state.posiciones; let abiertas = 0, expuesto = 0; for (const [, m] of MOTORES) if (m !== (c.motor || MOTOR)) for (const p of m.sb.state.posiciones) if (p.estado === "ABIERTA") { abiertas++; expuesto += p.tamaño * K; }
  for (const p of L) { if (p.estado === "ABIERTA") { abiertas++; expuesto += p.tamaño * K; continue; } if (p._contada || p.token !== c.mint && p.poolId !== c.mint && p.rec !== c.rec && !(p.poolId === undefined && p.token === undefined)) { if (p._contada) continue; }
    if (p._contada) continue; const suyo = (p.poolId === c.mint || p.token === c.mint); if (!suyo) continue; p._contada = true;
    const papel = (p.neto || 0) * K, real = W.netoReal({ fee: p.feePct, tam: p.tamaño, compras: p.compras || [], entrada: p.entrada, cierre: p.precioCierre }, { _P: c.rec.puntos, _ds: 0.58 }) * K;
    P.papel += papel; P.real += real; P.cerradas++; if (real > 0) P.ganadas++; P.porMotivo[p.motivo] = (P.porMotivo[p.motivo] || 0) + real;
    const lotes = (p.compras || []).length || 1; c.papel = (c.papel || 0) + papel; c.real = (c.real || 0) + real; c.ops = (c.ops || 0) + 1;
    log(`[SOLPOS] sym=${c.sym} mint=${c.mint} motivo=${p.motivo} lotes=${lotes} pnl=${(p.pnlPct ?? 0).toFixed(1)}% papel=${papel.toFixed(4)}SOL real=${real.toFixed(4)}SOL acum_real=${P.real.toFixed(3)}SOL`);
    { const comp = p.compras || []; apunta(c).ops.push({ entraT: comp.length ? Math.round(c.t0 + 1000 * comp[0].t) : null, entraPct: comp.length ? +((comp[0].precio - 1) * 100).toFixed(1) : null, saleT: Date.now(), salePct: +(((p.precioCierre || 1) - 1) * 100).toFixed(1), motivo: p.motivo, lotes, real: +real.toFixed(4) }); }
    P.ultimas.unshift({ sym: c.sym, mint: c.mint, motivo: p.motivo, lotes, pnl: +(p.pnlPct ?? 0).toFixed(1), papel: +papel.toFixed(4), real: +real.toFixed(4), t: Date.now() }); if (P.ultimas.length > 300) P.ultimas.length = 300; }
  P.abiertas = abiertas; if (expuesto > P.maxExpuesto) P.maxExpuesto = +expuesto.toFixed(3); P.expuesto = +expuesto.toFixed(3); }

// ── PumpPortal: las migraciones ──
function abrePumpPortal() {
  const ws = new WebSocket(PP_WS); S.pp.ws = ws;
  ws.on("open", () => { S.pp.ok = true; ws.send(JSON.stringify({ method: "subscribeMigration" })); log(`🔌 PumpPortal: escuchando las migraciones${PUMPPORTAL_API_KEY ? " (con clave)" : " (sin clave)"}`); });
  ws.on("message", (raw) => { let m; try { m = JSON.parse(raw); } catch { return; } if (m && m.mint && (m.txType === "migrate" || m.txType === "migration" || m.signature) && apuntaLlegada(m.mint, "pumpportal")) migracion({ ...m, fuente: "pumpportal" }).catch(e => { S.errores++; log(`⚠️ migración ${corto(m.mint)}: ${e.message}`); }); });
  ws.on("close", () => { S.pp.ok = false; S.pp.reconexiones++; setTimeout(abrePumpPortal, Math.min(30000, 2000 * S.pp.reconexiones)); });
  ws.on("error", (e) => { S.errores++; log(`⚠️ PumpPortal: ${e.message}`); });
}

// una migración vista en la cadena: la piscina, el token y las reservas salen de la propia transacción
function migracionDirecta(res) { try { const meta = res?.transaction?.meta; const firma = res?.signature; const post = meta?.postTokenBalances || [];
    const porOwner = new Map(); for (const b of post) { if (!b.owner) continue; const o = porOwner.get(b.owner) || { mint: null, tok: 0, sol: 0 }; const amt = +(b.uiTokenAmount?.uiAmountString || 0);
      if (b.mint === WSOL) { if (amt > o.sol) o.sol = amt; } else if (amt > o.tok) { o.tok = amt; o.mint = b.mint; } porOwner.set(b.owner, o); }
    let mejor = null; for (const [owner, o] of porOwner) if (o.mint && o.tok > 0 && o.sol > 0 && (!mejor || o.sol > mejor.sol)) mejor = { pool: owner, ...o };
    if (!mejor) return;   // no era una migración (o no trae la piscina)
    const tx = res?.transaction?.transaction; const vaults = vaultsDe({ meta, transaction: tx }, mejor.mint, mejor.pool);
    if (!apuntaLlegada(mejor.mint, "helius")) return;
    migracion({ mint: mejor.mint, signature: firma, fuente: "helius", reservas: { pool: mejor.pool, tok: mejor.tok, sol: mejor.sol, vaults } }).catch(e => { S.errores++; log(`⚠️ migración directa ${corto(mejor.mint)}: ${e.message}`); });
  } catch (e) { S.errores++; } }
async function migracion(m) {
  const mint = m.mint; if (S.vistas.has(mint)) return; S.vistas.add(mint); if (S.vistas.size > 20000) S.vistas = new Set([...S.vistas].slice(-10000));
  S.migs++; S.ultimaMig = Date.now();
  const c = { mint, sym: m.symbol || m.name || corto(mint), firma: m.signature || null, t0: Date.now(), pool: null, precioIni: 0, ref: "migración", puntos: [], max: { t: 0, p: 0 }, min: { t: 0, p: 0 },
    swaps: 0, vistas: new Set(), ultReservas: null, ultSwap: 0, supply: null, mayhem: false, nac: null, nacTxt: null, fin: false, hel: null, min1: { swaps: 0, compradores: new Set(), vendedores: new Set(), solIn: 0, solOut: 0 } };
  S.camaras.set(mint, c);
  // 1) la piscina y el precio inicial, de la transacción de la migración
  c.fuente = m.fuente || "pumpportal";
  if (m.reservas && m.reservas.tok > 0 && m.reservas.sol > 0) { const r = m.reservas; c.pool = r.pool; c.precioIni = r.sol / r.tok; c.reservasIni = r; c.vaults = r.vaults || null; c.ultPrecio = c.precioIni; c.puntos.push({ t: 0, p: 0 }); }
  else if (c.firma) { try { const r = await leeMigracion(c.firma, mint); if (r) { c.pool = r.pool; c.precioIni = r.sol / r.tok; c.reservasIni = r; c.ultPrecio = c.precioIni; c.puntos.push({ t: 0, p: 0 }); } } catch (e) { log(`⚠️ ${c.sym}: no pude leer la migración (${e.message}); el precio inicial será el del 1er swap`); } }
  if (c.fin) return;
  // [5-oct] ¿la piscina es de PumpSwap? (PumpPortal también manda migraciones de otras plataformas, p. ej. a Raydium)
  // [8-oct] la cuenta de la piscina recién creada muchas veces aún no se ve (salía «NO es de PumpSwap: ?» en el 96 %): se reintenta, y si no se sabe queda en null («sin saber»), nunca en false
  const miraPrograma = async () => { if (!c.pool) return; try { const ai = await rpc("getAccountInfo", [c.pool, { encoding: "base64", dataSlice: { offset: 0, length: 0 }, commitment: "confirmed" }]); const o = ai?.value?.owner || null;
    if (o) { c.programa = o; c.esPumpSwap = o === "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"; if (c.rec) c.rec.pumpswap = c.esPumpSwap; } } catch {} };
  c.esPumpSwap = null; await miraPrograma(); if (c.esPumpSwap == null) for (const ms of [3000, 10000, 30000]) setTimeout(() => { if (c.esPumpSwap == null && !c.fin) miraPrograma(); }, ms);
  // 2) a grabar: las dos cuentas de la piscina (si no salieron de la transacción, se piden por el dueño)
  if (c.pool && !c.vaults) { try { c.vaults = await vaultsPorDueno(c.pool, mint); } catch (e) { log(`⚠️ ${c.sym}: no encuentro las cuentas de la piscina (${e.message})`); } }
  if (c.fin) return;
  if (MODO_WS || !c.vaults) { c.hel = heliusConHueco(); suscribe(c.hel, c); S.porWs = (S.porWs || 0) + 1; } else S.porSondeo = (S.porSondeo || 0) + 1;
  c.timerFin = setTimeout(() => { const va = c.ultPrecio && c.precioIni ? (c.ultPrecio / c.precioIni - 1) * 100 : 0;
    if (!c.fin && CAMARA_LARGA_MIN > CAMARA_MIN && va >= LARGA_SI) { c.larga = true; log(`🔭 ${c.sym} sigue a +${va.toFixed(0)} % a los ${CAMARA_MIN} min: lo sigo grabando (y el bot llevándolo) hasta las ${(CAMARA_LARGA_MIN / 60).toFixed(0)} h`); c.timerFin = setTimeout(() => { const va2 = c.ultPrecio && c.precioIni ? (c.ultPrecio / c.precioIni - 1) * 100 : 0;
      if (!c.fin && CAMARA_MUY_LARGA_MIN > CAMARA_LARGA_MIN && va2 >= MUY_LARGA_SI) { c.muyLarga = true; log(`🔭🔭 ${c.sym} sigue a +${va2.toFixed(0)} % a las ${(CAMARA_LARGA_MIN / 60).toFixed(0)} h: lo sigo hasta las ${(CAMARA_MUY_LARGA_MIN / 60).toFixed(0)} h`); c.timerFin = setTimeout(() => cierra(c, "fin de la cámara muy larga"), (CAMARA_MUY_LARGA_MIN - CAMARA_LARGA_MIN) * 60e3); return; }
      cierra(c, "fin de la cámara larga"); }, (CAMARA_LARGA_MIN - CAMARA_MIN) * 60e3); return; }
    cierra(c, "fin de la cámara"); }, CAMARA_MIN * 60e3);
  log(`🐣 MIGRACIÓN${c.esPumpSwap === false ? " (NO es de PumpSwap: " + (c.programa || "?").slice(0, 8) + ")" : c.esPumpSwap == null ? " (piscina sin confirmar aún)" : ""} ${c.sym} (${corto(mint)})${c.pool ? ` · piscina ${corto(c.pool)} · precio inicial ${c.precioIni.toExponential(3)} SOL` : " · piscina aún no (se verá en el 1er swap)"} · grabando ${CAMARA_MIN} min`);
  // 3) supply (¿Mayhem?) y el nacimiento, en paralelo
  try { const sp = await rpc("getTokenSupply", [mint]); const n = +sp?.value?.uiAmount; if (n > 0) { c.supply = n; c.mayhem = n > 1.5e9; } } catch {}
  if (c.mayhem && !GRABA_MAYHEM) { S.mayhemFuera = (S.mayhemFuera || 0) + 1; cierra(c, "Mayhem: no se graba"); return; }
  c.timerNac = setTimeout(() => { if (!c.nac && !c.fin && !c.rec) { c.veto = "sin nacimiento"; P.sinNac++; log(`⏱️ ${c.sym}: el nacimiento no llegó en ${NAC_ESPERA_S} s · no se opera (se graba)`); } }, NAC_ESPERA_S * 1000);
  nacimiento(c).then(() => decide(c)).catch(e => { S.nacFallos++; log(`⚠️ nacimiento de ${c.sym}: ${e.message}`); });
}

// ── el nacimiento: lo que pasó en la curva de bonos ANTES de migrar (API de pump.fun) ──
const CAB = { accept: "application/json", "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36", origin: "https://pump.fun", referer: "https://pump.fun/" };
async function pumpApi(ruta) { const r = await fetch(`https://frontend-api-v3.pump.fun${ruta}`, { headers: CAB, signal: AbortSignal.timeout(8000) }); if (!r.ok) throw new Error(`pump.fun ${r.status}`); return r.json(); }
let avisoPump = 0;
// ── el nacimiento por Helius (cuando la API de pump.fun no contesta): símbolo (getAsset), tenedores y concentración (cuentas del token),
//    y edad (la firma más antigua que menciona el mint). "compradores" = tenedores al migrar (los que compraron y aún tienen).
const TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
async function nacimientoHelius(c) {
  const n = { seg: null, compradores: 0, compras: 0, ventas: 0, exentas: 0, top1: 0, top5: 0, top10: 0, mayorTxPct: 0, mayorTxDest: 1, prim3Pct: 0, creadorPct: 0, vendidoPct: 0, snipeSol: 0, tradesLeidos: 0, fuente: "helius" };
  try { const a = await rpc("getAsset", { id: c.mint }); const sym = a?.content?.metadata?.symbol; if (sym) c.sym = String(sym).slice(0, 14); } catch {}
  try { let antes = undefined, viejo = null; for (let i = 0; i < 5; i++) { const sigs = await rpc("getSignaturesForAddress", [c.mint, { limit: 1000, before: antes, commitment: "confirmed" }]); if (!sigs || !sigs.length) break; viejo = sigs[sigs.length - 1]; if (sigs.length < 1000) break; antes = viejo.signature; }
    if (viejo && viejo.blockTime) { n.seg = Math.max(0, Math.round(c.t0 / 1000 - viejo.blockTime)); c.creado = viejo.blockTime * 1000; }
    if (viejo && viejo.signature) { const tx = await rpc("getTransaction", [viejo.signature, { encoding: "jsonParsed", commitment: "confirmed", maxSupportedTransactionVersion: 0 }]);
      const k0 = tx?.transaction?.message?.accountKeys?.[0]; const firmante = typeof k0 === "string" ? k0 : k0?.pubkey; if (firmante) c.creador = firmante; } } catch {}
  // el creador repetido (en lo que lleva encendido el grabador)
  if (c.creador) { const L = S.creadores.get(c.creador) || []; L.push(c.sym); S.creadores.set(c.creador, L); n.creadorTokens = L.length;
    if (L.length >= 2) log(`🔁 CREADOR REPETIDO ${corto(c.creador)}: ${L.length} tokens (${L.slice(-6).join(", ")})`); }
  try { const supply = c.supply || 1e9; const saldos = []; let cursor = null;
    for (let pag = 0; pag < 5; pag++) { const r = await rpc("getTokenAccounts", { mint: c.mint, limit: 1000, cursor: cursor || undefined, options: { showZeroBalance: false } }, 15000);
      for (const x of r?.token_accounts || []) { const amt = Number(x.amount || 0) / 1e6; if (amt > 0 && x.owner !== c.pool) saldos.push({ owner: x.owner, amt }); }
      cursor = r?.cursor; if (!cursor || !(r?.token_accounts || []).length) break; }
    if (!S.avisoTen) { S.avisoTen = true; log(`ℹ️ tenedores por Helius (getTokenAccounts): ${c.sym} → ${saldos.length} cuentas con saldo`); }
    saldos.sort((a, b) => b.amt - a.amt); const pct = (x) => f2(100 * x / supply);
    n.compradores = saldos.length; n.top1 = pct(saldos[0]?.amt || 0); n.top5 = pct(saldos.slice(0, 5).reduce((s, v) => s + v.amt, 0)); n.top10 = pct(saldos.slice(0, 10).reduce((s, v) => s + v.amt, 0));
    if (c.creador) { const cr = saldos.find(v => v.owner === c.creador); n.creadorPct = pct(cr ? cr.amt : 0); } } catch (e) { log(`⚠️ ${c.sym}: tenedores por Helius: ${e.message}`); }
  return n;
}
// base58 sin librerías (para los owners de las cuentas)
const B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
function bs58(buf) { const d = [0]; for (const b of buf) { let c = b; for (let j = 0; j < d.length; j++) { c += d[j] << 8; d[j] = c % 58; c = (c / 58) | 0; } while (c > 0) { d.push(c % 58); c = (c / 58) | 0; } }
  let s = ""; for (const b of buf) { if (b !== 0) break; s += "1"; } for (let j = d.length - 1; j >= 0; j--) s += B58[d[j]]; return s; }
const PUMP_API = process.env.PUMP_API === "1";   // [3-oct] de serie, el nacimiento lo hace HELIUS; PUMP_API=1 añade lo de pump.fun (compras/ventas/revendido) si contesta
async function nacimiento(c) {
  if (!PUMP_API) {   // ── de serie: todo por Helius ──
    c.nac = await nacimientoHelius(c); const n = c.nac;
    c.nacTxt = [n.seg ?? "-", n.compradores, 0, n.top5, n.top1, n.mayorTxPct, 1, n.prim3Pct, n.creadorPct, n.vendidoPct, c.creador || "-", n.creadorTokens || 1].join("/"); S.nacHechos++;
    log(`👶 NACIMIENTO ${c.sym}: ${n.seg != null ? (n.seg < 120 ? n.seg + " s" : Math.round(n.seg / 60) + " min") : "?"} hasta migrar · ${n.compradores} tenedores al migrar · los 5 mayores ${n.top5} % · el mayor ${n.top1} % · el creador ${corto(c.creador)} tiene ${n.creadorPct} %${(n.creadorTokens || 1) > 1 ? ` · 🔁 su token nº ${n.creadorTokens}` : ""}${c.mayhem ? " · ⚠️ MAYHEM" : ""}`); return; }
  let fallo = null; const coin = await pumpApi(`/coins/${c.mint}`).catch(e => { fallo = e.message; return null; });
  if (coin) { if (coin.symbol) c.sym = String(coin.symbol).slice(0, 14); c.creador = coin.creator || null; c.creado = coin.created_timestamp ? +coin.created_timestamp : null; c.redes = { tg: !!coin.telegram, tw: !!coin.twitter, web: !!coin.website };
    if (!c.supply && coin.total_supply) c.supply = +coin.total_supply / 1e6; }
  const trades = []; if (coin) for (let off = 0; off < 4000; off += 200) { const L = await pumpApi(`/trades/all/${c.mint}?limit=200&offset=${off}&minimumSize=0`).catch(e => { fallo = e.message; return null; }); if (!Array.isArray(L) || !L.length) break; trades.push(...L); if (L.length < 200) break; }
  if (!coin || !trades.length) {   // la API de pump.fun no contesta (o no da trades): lo mismo por Helius
    if (avisoPump < 3) { avisoPump++; log(`ℹ️ la API de pump.fun no contestó para ${c.sym} (${fallo || "sin trades"}) → nacimiento por Helius (tenedores al migrar y edad del mint)`); }
    c.nac = await nacimientoHelius(c); const n = c.nac;
    c.nacTxt = [n.seg ?? "-", n.compradores, 0, n.top5, n.top1, n.mayorTxPct, 1, n.prim3Pct, n.creadorPct, n.vendidoPct, (c.creador || "-").slice(0, 10)].join("/"); S.nacHechos++;
    log(`👶 NACIMIENTO ${c.sym} (por Helius): ${n.seg != null ? (n.seg < 120 ? n.seg + " s" : Math.round(n.seg / 60) + " min") : "?"} hasta migrar · ${n.compradores} tenedores · los 5 mayores ${n.top5} % · el mayor ${n.top1} % · el creador ${n.creadorPct} %${c.mayhem ? " · ⚠️ MAYHEM" : ""}`); return; }
  const antes = trades.filter(t => !t.timestamp || t.timestamp * 1000 <= c.t0 + 2000).sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0) || (a.slot || 0) - (b.slot || 0));
  const supply = c.supply || 1e9; const tok = (t) => (+t.token_amount || 0) / 1e6; const sol = (t) => (+t.sol_amount || 0) / 1e9;
  const compradores = new Set(), neto = new Map(); let compras = 0, ventas = 0, comprado = 0, vendido = 0, mayor = 0, solPrim3 = 0, tokPrim3 = 0;
  antes.forEach((t, i) => { const u = t.user || "?"; const k = tok(t); if (t.is_buy) { compras++; comprado += k; compradores.add(u); neto.set(u, (neto.get(u) || 0) + k); if (k > mayor) mayor = k; if (i < 3) { solPrim3 += sol(t); tokPrim3 += k; } } else { ventas++; vendido += k; neto.set(u, (neto.get(u) || 0) - k); } });
  const tenencias = [...neto.values()].filter(v => v > 0).sort((a, b) => b - a);
  const pct = (x) => f2(100 * x / supply);
  const seg = c.creado ? Math.max(0, Math.round((c.t0 - c.creado) / 1000)) : null;
  c.nac = { seg, compradores: compradores.size, compras, ventas, exentas: 0, top1: pct(tenencias[0] || 0), top5: pct(tenencias.slice(0, 5).reduce((s, v) => s + v, 0)), top10: pct(tenencias.slice(0, 10).reduce((s, v) => s + v, 0)),
    mayorTxPct: pct(mayor), mayorTxDest: 1, prim3Pct: pct(tokPrim3), creadorPct: pct(c.creador ? (neto.get(c.creador) || 0) : 0), vendidoPct: comprado > 0 ? f2(100 * vendido / comprado) : 0, snipeSol: f2(solPrim3), tradesLeidos: antes.length };
  // mismo orden que el nac= de Pons: seg/compradores/exentas/top5/top1/mayorTx/sus carteras/3 primeros/creador/revendido/creador(10)
  c.nacTxt = [seg ?? "-", c.nac.compradores, 0, c.nac.top5, c.nac.top1, c.nac.mayorTxPct, 1, c.nac.prim3Pct, c.nac.creadorPct, c.nac.vendidoPct, (c.creador || "-").slice(0, 10)].join("/");
  S.nacHechos++;
  log(`👶 NACIMIENTO ${c.sym}: ${seg != null ? (seg < 120 ? seg + " s" : Math.round(seg / 60) + " min") : "?"} hasta migrar · ${c.nac.compradores} compradores (${compras} compras, ${ventas} ventas) · los 5 mayores ${c.nac.top5} % · la mayor compra ${c.nac.mayorTxPct} % · el creador ${c.nac.creadorPct} % · revendido ${c.nac.vendidoPct} %${c.mayhem ? " · ⚠️ MAYHEM" : ""}`);
}

// ── el cierre de una cámara: la línea [SOLREC] ──
function cierra(c, motivo) {
  if (c.fin) return;
  if (c.rec) { const m = c.motor || MOTOR; alMotor(c, c.puntos.length); try { m.estrategiaFin(c.rec); } catch (e) { log(`⚠️ motor (fin) ${c.sym}: ${e.message}`); } cuentas(c);
    m.sb.state.posiciones = m.sb.state.posiciones.filter(p => p.estado === "ABIERTA"); }   // lo cerrado ya está contado
  { const d = DIARIO.get(c.mint); if (d) { d.puntos = resumenCurva(c.puntos); d.max = c.max ? { t: Math.round(c.max.t), p: +c.max.p.toFixed(1) } : null; d.fin = c.puntos.length ? +c.puntos[c.puntos.length - 1].p.toFixed(1) : null; } }
  clearTimeout(c.timerNac); c.fin = true; if (c.rec) { CERRADAS.set(c.mint, c); setTimeout(() => CERRADAS.delete(c.mint), 30 * 60e3); } clearTimeout(c.timerFin); if (c.hel) desuscribe(c.hel, c); S.camaras.delete(c.mint);
  if (motivo.startsWith("Mayhem")) { if (!SOLO_LOG) log(`🚫 ${c.sym}: Mayhem (supply ${Math.round((c.supply || 0) / 1e6)}M) · no se graba`); return; }
  if (c.puntos.length < 2 || !c.precioIni) { log(`🗑️ ${c.sym}: sin curva (${c.swaps} swaps, ${c.puntos.length} puntos) · ${motivo}`); return; }
  S.curvas++;
  const mc = c.precioIni * (c.supply || 1e9) * (S.solUsd || 0); const pts = c.puntos.map(q => `${Math.round(q.t)}:${f2(q.p)}`).join(",");
  const m1 = c.min1; const conducta = `60:${m1.compradores.size}/${m1.vendedores.size}/${f2(m1.solIn)}/${f2(m1.solOut)}/${m1.swaps}`;
  const linea = `[SOLREC] sym=${c.sym} mint=${c.mint} pool=${c.pool || "-"} firma=${c.firma || "-"} MC=$${(mc / 1000).toFixed(1)}K supply=${Math.round((c.supply || 1e9) / 1e6)}M MIN=${f2(c.min.p)}%@${Math.round(c.min.t)}s MAX=${f2(c.max.p)}%@${Math.round(c.max.t)}s par=SOL precioIni=${c.precioIni.toExponential(4)} ref=${c.ref} swaps=${c.swaps} puntos=${c.puntos.length} mayhem=${c.mayhem ? 1 : 0} prog=${c.esPumpSwap === true ? "pumpswap" : c.programa ? c.programa.slice(0, 8) : "-"} veto=${c.veto ? c.veto.replace(" ", "_") : "-"} regla=${c.regla ?? "-"} bot=${c.ops || 0}/${(c.papel || 0).toFixed(4)}/${(c.real || 0).toFixed(4)} liq=${c.liquidez || 0}/${c.raros || 0} conducta=${conducta} ${c.nacTxt ? `nac=${c.nacTxt} ` : ""}pts=${pts}`;
  log(linea);
  if (!SOLO_LOG) log(`📼 ${c.sym}: ${c.puntos.length} puntos en ${Math.round(c.puntos[c.puntos.length - 1].t / 60)} min · máx ${f1(c.max.p)} % a los ${Math.round(c.max.t)} s · mín ${f1(c.min.p)} % · ${c.swaps} swaps · ${motivo}`);
  S.ultimas.unshift({ sym: c.sym, mint: c.mint, max: f1(c.max.p), min: f1(c.min.p), puntos: c.puntos.length, swaps: c.swaps, nac: c.nac, t: Date.now() }); S.ultimas = S.ultimas.slice(0, 50);
}

// ── el sondeo: cada POLL_S s, los saldos de las cuentas de todas las piscinas vivas (getMultipleAccounts, de 100 en 100) ──
let sondeando = false;
setInterval(async () => { if (sondeando) return; sondeando = true;
  try { const vivas = [...S.camaras.values()].filter(c => !c.fin && c.vaults && !c.hel); const cuentas = []; for (const c of vivas) cuentas.push([c, "base", c.vaults.base], [c, "quote", c.vaults.quote]);
    for (let i = 0; i < cuentas.length; i += 100) { const lote = cuentas.slice(i, i + 100);
      let r; try { r = await rpc("getMultipleAccounts", [lote.map(x => x[2]), { encoding: "jsonParsed", commitment: "processed" }], 6000); } catch (e) { S.errores++; continue; }
      (r?.value || []).forEach((acc, j) => { const [c, cual] = lote[j]; const amt = +(acc?.data?.parsed?.info?.tokenAmount?.uiAmountString ?? NaN); if (!(amt >= 0)) return; if (cual === "base") c.resBase = amt; else c.resQuote = amt; });
      for (const c of new Set(lote.map(x => x[0]))) precioDesdeReservas(c); } } finally { sondeando = false; } }, POLL_S * 1000);

// ── el latido: un punto aunque no haya swaps, y cerrar a los muertos ──
setInterval(() => { const ahora = Date.now();
  for (const c of S.camaras.values()) { if (c.fin || !c.precioIni) continue; const t = (ahora - c.t0) / 1000; const ult = c.puntos[c.puntos.length - 1];
    if (!ult || t - ult.t >= LATIDO_S) punto(c, t, c.ultPrecio || c.precioIni);
    if (MUERTO_MIN > 0 && ult && ult.p <= -95 && c.ultSwap && ahora - c.ultSwap > MUERTO_MIN * 60e3) cierra(c, `muerto (${MUERTO_MIN} min a −95 % sin swaps)`); } }, LATIDO_S * 1000);

// ── la salud, cada 10 minutos ──
setInterval(() => { const h = ((Date.now() - S.arranque) / 3600e3).toFixed(1); const subs = S.hel.reduce((s, H) => s + H.subs.size, 0);
  log(`[SALUD] ${h}h · migraciones=${S.migs} (última hace ${S.ultimaMig ? Math.round((Date.now() - S.ultimaMig) / 60e3) + " min" : "—"}) · grabando=${S.camaras.size} · curvas=${S.curvas} · swaps=${S.swaps} · nacimientos=${S.nacHechos}/${S.nacHechos + S.nacFallos} · 📡 migraciones: Helius primero ${MIGF.helius} · PumpPortal primero ${MIGF.pumpportal}${MIGF.adelanto.length ? ` · Helius se adelanta ${Math.round(MIGF.adelanto.slice().sort((a, b) => a - b)[Math.floor(MIGF.adelanto.length / 2)])} ms (mediana)` : ""} · 💰 REAL: ${(() => { const r = REAL.resumen(); return !r.listo ? "sin cartera" : `${r.armado ? "ARMADO" : "desarmado"} · saldo ${r.saldo != null ? r.saldo.toFixed(3) : "?"} SOL · dentro ${r.dentro} · cuadres ${r.totales.cuadres} · REAL ${r.totales.real >= 0 ? "+" : ""}${r.totales.real.toFixed(4)} SOL (hoy ${r.hoy.real >= 0 ? "+" : ""}${(r.hoy.real || 0).toFixed(4)}) · fallos ${r.totales.fallos}`; })()} · 📊 PAPEL (desde ${new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" }).format(new Date(P.desde))}): entradas ${P.entradas} · vetadas ${P.vetadas} · sin nacimiento ${P.sinNac} · cerradas ${P.cerradas} (${P.ganadas} ganadas) · abiertas ${P.abiertas} (${(P.expuesto || 0).toFixed(2)} SOL; máx ${P.maxExpuesto} SOL) · papel ${P.papel >= 0 ? "+" : ""}${P.papel.toFixed(3)} SOL · REAL ${P.real >= 0 ? "+" : ""}${P.real.toFixed(3)} SOL (${(P.real * S.solUsd >= 0 ? "+" : "−")}$${Math.abs(P.real * S.solUsd).toFixed(0)}) · Mayhem fuera=${S.mayhemFuera || 0} · creadores repetidos=${[...S.creadores.values()].filter(L => L.length >= 2).length} · por sondeo ${S.porSondeo || 0} / por WebSocket ${S.porWs || 0} · Helius: ${S.llamadas || 0} llamadas (${Math.round((S.llamadas || 0) / Math.max(0.01, +h) / 60)}/min; ${Object.entries(S.porMetodo || {}).sort((a, b) => b[1] - a[1]).slice(0, 4).map(([k, v]) => k + " " + v).join(", ")}) · WS ${S.hel.filter(H => H.ok).length}/${S.hel.length} con ${subs} tokens, ≈${(S.bytes / 1e6).toFixed(0)} MB · PumpPortal ${S.pp.ok ? "ok" : "CAÍDO"} · SOL $${S.solUsd.toFixed(0)} · errores ${S.errores}`); }, 10 * 60e3);

// ── un HTTP mínimo para ver que vive ──
let PANEL_HTML = null; try { PANEL_HTML = fs.readFileSync(path.join(DIR, "panel.html"), "utf-8"); } catch { console.log("ℹ️ no hay panel.html junto al server: uso el panel sencillo"); }
// [8-oct] estado(): lo usaban /estado y el panel viejo, pero NO existía → «ReferenceError: estado is not defined» tumbaba el server
//   (pasaba con cualquier visita: el navegador pide /favicon.ico, que caía en el panel viejo). Ahora existe, y ningún error de una ruta tumba el server.
function estado() { return { arranque: S.arranque, migraciones: S.migs, grabando: S.camaras.size, curvas: S.curvas, swaps: S.swaps, errores: S.errores, nacimientos: S.nacHechos, nacFallos: S.nacFallos, solUsd: S.solUsd,
  plan: PLAN ? PLAN.nombre : null, papel: { desde: P.desde, real: P.real, papel: P.papel, cerradas: P.cerradas, ganadas: P.ganadas, abiertas: P.abiertas, expuesto: P.expuesto || 0, entradas: P.entradas, vetadas: P.vetadas } }; }
const atiende = async (req, res) => {
  if (req.url.startsWith("/favicon")) { res.statusCode = 204; res.end(); return; }
  if (req.method === "POST") { let cuerpo = ""; for await (const ch of req) { cuerpo += ch; if (cuerpo.length > 2000) break; } const q = new URLSearchParams(cuerpo); const u = req.url.split("?")[0];
    const ok = REAL.CLAVE && q.get("clave") === REAL.CLAVE; let msg = !REAL.CLAVE ? "Falta la variable PANEL_CLAVE en Railway: ponle la clave que quieras y vuelve a probar" : "Clave incorrecta: tiene que ser la misma que la variable PANEL_CLAVE de Railway";
    if (ok && u === "/papel-a-cero") { P.anterior = { plan: P.plan, desde: P.desde, hasta: Date.now(), cerradas: P.cerradas, real: P.real, papel: P.papel };   // [8-oct] poner a cero las cuentas en papel desde el panel (lo de antes queda en P.anterior)
      Object.assign(P, { papel: 0, real: 0, cerradas: 0, ganadas: 0, entradas: 0, vetadas: 0, sinNac: 0, ultimas: [], porMotivo: {}, maxExpuesto: 0, desde: Date.now() }); guardaP(); log("📊 cuentas en papel a cero desde el panel"); msg = "Cuentas en papel a cero: empiezan a contar desde ahora (las posiciones abiertas siguen y contarán al cerrar)"; }
    else if (ok && u === "/armar") msg = REAL.arma(); else if (ok && u === "/desarmar") msg = REAL.desarma("a mano desde el panel"); else if (ok && u === "/vender" && q.get("mint")) msg = await REAL.vendeAMano(q.get("mint"));
    if (req.headers["x-panel"]) { res.setHeader("content-type", "application/json"); res.end(JSON.stringify({ ok, msg })); return; }
    res.setHeader("content-type", "text/html; charset=utf-8"); res.end(`<meta http-equiv="refresh" content="2;url=/"><p style="font:16px system-ui;padding:20px">${msg}</p>`); return; }
  const json = (o) => { res.setHeader("content-type", "application/json"); res.setHeader("cache-control", "no-store"); res.end(JSON.stringify(o)); };
  if (req.url.startsWith("/datos")) { const r = REAL.resumen(); const serie = P.ultimas.filter(o => o.t >= P.desde).slice().reverse().map(o => [o.t, o.real]);
    const med = MIGF.adelanto.length ? Math.round(MIGF.adelanto.slice().sort((a, b) => a - b)[Math.floor(MIGF.adelanto.length / 2)]) : null;
    return json({ ahora: Date.now(), hora: horaEsp(Date.now()), plan: PLAN ? { nombre: PLAN.nombre, reglas: PLAN.reglas.length, vetos: PLAN.reglas.filter(x => x.acc < 0).length } : null, lote: LOTE_SOL, solUsd: S.solUsd || 0,
      papel: { desde: P.desde, real: +P.real.toFixed(4), papel: +P.papel.toFixed(4), cerradas: P.cerradas, ganadas: P.ganadas, abiertas: P.abiertas, expuesto: P.expuesto || 0, entradas: P.entradas, vetadas: P.vetadas, sinNac: P.sinNac, porMotivo: P.porMotivo }, serie,
      real: { listo: r.listo, armado: r.armado, motivo: r.motivo, simulado: r.simulado, saldo: r.saldo, totales: r.totales, hoy: r.hoy, topes: r.topes, pendientes: r.pendientes, huerfanos: Object.entries(r.huerfanos || {}).map(([m, h]) => ({ mint: m, sym: h.sym })) },
      salud: { arranque: S.arranque || null, migraciones: S.migs, grabando: S.camaras.size, curvas: S.curvas, nacimientos: S.nacHechos, nacFallos: S.nacFallos, errores: S.errores, heliusPrimero: MIGF.helius, pumpportalPrimero: MIGF.pumpportal, adelantoMs: med } }); }
  if (req.url.startsWith("/tokens")) { const out = []; for (const d of DIARIO.values()) { const c = S.camaras.get(d.mint); const pts = c ? c.puntos : (d.puntos || []).map(([t, p]) => ({ t, p })); const ult = pts.length ? pts[pts.length - 1] : null;
      const abiertas = c && c.motor ? c.motor.sb.state.posiciones.filter(q => q.estado === "ABIERTA" && (q.token === d.mint || q.poolId === d.mint)) : [];
      let abierta = null; if (abiertas.length && ult) { const comp = abiertas.flatMap(q => q.compras || []); const media = comp.length ? comp.reduce((a, q) => a + q.precio, 0) / comp.length : 1; abierta = { lotes: comp.length, entraT: comp.length ? Math.round(d.t0 + 1000 * Math.min(...comp.map(q => q.t))) : null, va: +(((1 + ult.p / 100) / media - 1) * 100).toFixed(1) }; }
      const sale = d.ops.length ? d.ops[d.ops.length - 1].saleT : null; let despues = null; if (sale) for (const q of pts) if (d.t0 + q.t * 1000 > sale && (!despues || q.p > despues.p)) despues = { t: Math.round(q.t), p: +(+q.p).toFixed(1) };
      const mx = c ? c.max : d.max; out.push({ mint: d.mint, sym: d.sym, t0: d.t0, ten: d.ten, seg: d.seg, top5: d.top5, regla: d.regla, nota: d.nota, veto: d.veto, ops: d.ops, abierta, grabando: !!c, max: mx ? { t: Math.round(mx.t), p: +(+mx.p).toFixed(1) } : null, fin: ult ? +(+ult.p).toFixed(1) : d.fin, despues }); }
    out.sort((a, b) => b.t0 - a.t0); return json(out.slice(0, 500)); }
  if (req.url.startsWith("/curva")) { const m = new URL(req.url, "http://x").searchParams.get("mint"); const d = DIARIO.get(m); const c = S.camaras.get(m); if (!d) return json({ error: "no está en el diario" });
    return json({ t0: d.t0, puntos: c ? resumenCurva(c.puntos) : d.puntos || [], ops: d.ops }); }
  if (req.url.startsWith("/mapa")) { const GR = [["≤5", 3], ["6-25", 15], ["26-100", 60], ["101-150", 125], ["151-300", 225], ["301-500", 400], [">500", 750]]; const tH = {}; for (let k = 0; k < 24; k++) { const t = Date.now() + k * 3.6e6; tH[horaEsp(t)] = t; }
    return json({ hora: horaEsp(Date.now()), grupos: GR.map(([n, ten]) => ({ n, horas: [...Array(24).keys()].map(h => { const rg = reglaDe({ nac: { compradores: ten, seg: 300, top5: 20 }, t0: tH[h] }); return rg ? (rg.r.acc < 0 ? 0 : 1) : 1; }) })) }); }
  if (req.url.startsWith("/real")) { res.setHeader("content-type", "application/json"); res.end(JSON.stringify(REAL.resumen())); return; }
  if (req.url.startsWith("/estado")) { res.setHeader("content-type", "application/json"); res.end(JSON.stringify(estado())); return; }
  if (req.url.startsWith("/ultimas")) { res.setHeader("content-type", "application/json"); res.end(JSON.stringify(P.ultimas.slice(0, 100))); return; }
  if ((req.url === "/" || req.url.startsWith("/?")) && PANEL_HTML) { res.setHeader("content-type", "text/html; charset=utf-8"); res.setHeader("cache-control", "no-store"); res.end(PANEL_HTML); return; }
  // el panel viejo (se refresca solo cada 15 s): en "/viejo", o en "/" si falta panel.html
  const e = estado(), u = S.solUsd || 0, d = (x) => (x >= 0 ? "+" : "−") + Math.abs(x).toFixed(3) + " SOL" + (u ? ` <small>(${x >= 0 ? "+" : "−"}$${Math.abs(x * u).toFixed(0)})</small>` : "");
  const filas = P.ultimas.slice(0, 40).map(o => `<tr><td>${new Date(o.t).toLocaleTimeString("es-ES", { timeZone: "Europe/Madrid" })}</td><td><a href="https://dexscreener.com/solana/${o.mint}" target="_blank">${o.sym}</a></td><td>${o.motivo}</td><td>${o.lotes}</td><td>${o.pnl}%</td><td class="${o.real >= 0 ? "v" : "r"}">${o.real >= 0 ? "+" : ""}${o.real.toFixed(4)}</td></tr>`).join("");
  res.setHeader("content-type", "text/html; charset=utf-8");
  res.end(`<!DOCTYPE html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta http-equiv="refresh" content="15"><title>🟣 Solana · papel</title>
<style>body{font:15px -apple-system,system-ui,sans-serif;margin:0;padding:12px;background:#f7f6f2;color:#1d1d1b}@media (prefers-color-scheme:dark){body{background:#161615;color:#ecebe6}.c{background:#1f1f1d!important;border-color:#34332f!important}}
.c{background:#fff;border:1px solid #e6e3da;border-radius:14px;padding:12px;margin:10px 0}.k{display:grid;grid-template-columns:1fr 1fr;gap:8px}.k div{padding:6px 0}.k b{display:block;font-size:20px}.v{color:#1f7a4b}.r{color:#b3322e}
table{width:100%;border-collapse:collapse;font-size:13px}td,th{padding:4px;border-bottom:1px solid #e6e3da;text-align:right}td:nth-child(2),td:nth-child(3),th:nth-child(2),th:nth-child(3){text-align:left}small{color:#888}</style></head><body>
<h2 style="margin:4px 0">🟣 Solana · papel ${REAL.resumen().listo ? "y REAL" : ""}</h2><div><small>motor de Pons · ${JSON.stringify(ESTRATEGIA) === JSON.stringify(ESTRELLA2) ? "⭐⭐" : "estrategia propia"} · veto de ballenas ${VETO_BALLENA ? "sí" : "no"} · lote ${LOTE_SOL} SOL · lleva ${e.horas} h · no opera: son cuentas</small></div>
<div class="c k"><div>REAL estimado<b class="${P.real >= 0 ? "v" : "r"}">${d(P.real)}</b></div><div>papel<b>${d(P.papel)}</b></div>
<div>operaciones<b>${P.cerradas}</b><small>${P.ganadas} ganadas (${P.cerradas ? Math.round(100 * P.ganadas / P.cerradas) : 0} %)</small></div><div>abiertas ahora<b>${P.abiertas}</b><small>${(P.expuesto || 0).toFixed(2)} SOL · máx ${P.maxExpuesto} SOL</small></div>
<div>tokens al motor<b>${P.entradas}</b></div><div>vetados<b>${P.vetadas}</b><small>🐋 ballenas · ${P.sinNac} sin nacimiento</small></div></div>
${(() => { const r = REAL.resumen(); if (!r.listo) return `<div class="c"><b>💰 Real</b><br><small>sin cartera: el server solo hace papel. Para operar de verdad, pon SOLANA_PRIVATE_KEY y PANEL_CLAVE en Railway.</small></div>`;
  const cu = r.cuadres.slice(0, 15).map(c => `<tr><td>${new Date(c.t).toLocaleTimeString("es-ES", { timeZone: "Europe/Madrid" })}</td><td>${c.sym}</td><td>${c.compras}</td><td class="${c.real >= 0 ? "v" : "r"}">${c.real >= 0 ? "+" : ""}${c.real.toFixed(4)}</td><td>${c.papel >= 0 ? "+" : ""}${c.papel.toFixed(4)}</td><td>${c.estimado >= 0 ? "+" : ""}${c.estimado.toFixed(4)}</td></tr>`).join("");
  const hu = Object.entries(r.huerfanos).map(([m, h]) => `<form method="post" action="/vender" style="margin:4px 0"><input type="hidden" name="mint" value="${m}"><input name="clave" type="password" placeholder="clave" style="width:90px"> <button>vender ${h.sym}</button> <small>${h.motivo}</small></form>`).join("");
  return `<div class="c" style="border:2px solid ${r.armado ? "#1f7a4b" : "#b3322e"}"><b>💰 Real ${r.simulado ? "(SIMULADO)" : ""}: ${r.armado ? "🟢 ARMADO" : "🔴 desarmado"}</b>${r.motivo ? ` <small>(${r.motivo})</small>` : ""}<br><small>cartera ${String(r.cartera).slice(0, 4)}…${String(r.cartera).slice(-4)} · saldo ${r.saldo != null ? r.saldo.toFixed(4) : "?"} SOL · lote ${r.lote} SOL · dentro ${r.dentro} · en cola ${r.cola}</small>
  <div class="k"><div>REAL de verdad<b class="${r.totales.real >= 0 ? "v" : "r"}">${d(r.totales.real)}</b><small>hoy ${r.hoy.real >= 0 ? "+" : ""}${(r.hoy.real || 0).toFixed(4)} SOL</small></div><div>cuadres<b>${r.totales.cuadres}</b><small>papel ${r.totales.papel >= 0 ? "+" : ""}${r.totales.papel.toFixed(3)} · estimado ${r.totales.estimado >= 0 ? "+" : ""}${r.totales.estimado.toFixed(3)}</small></div></div>
  <form method="post" action="/armar" style="display:inline"><input name="clave" type="password" placeholder="clave" style="width:110px"> <button>🟢 armar</button></form> <form method="post" action="/desarmar" style="display:inline"><input name="clave" type="password" placeholder="clave" style="width:110px"> <button>🔴 desarmar</button></form>
  ${r.pendientes.length ? `<p><b>⏳ ventas pendientes:</b> ${r.pendientes.map(p => `${p.sym} (${p.error})`).join(" · ")}</p>` : ""}${hu ? `<p><b>👻 huérfanos</b> (tokens en la cartera sin operación)</p>${hu}` : ""}
  <div style="overflow-x:auto"><table><tr><th>hora</th><th>token</th><th>compras</th><th>REAL</th><th>papel</th><th>estimado</th></tr>${cu || "<tr><td colspan=6>aún ningún cuadre</td></tr>"}</table></div>
  <p><small>rutas: ${Object.entries(r.rutas).map(([k, v]) => `${k} ${v.ordenes} órdenes${v.msMedio != null ? " (" + v.msMedio + " ms de media)" : ""}${v.fallos ? " · " + v.fallos + " fallos" : ""}`).join(" · ") || "aún ninguna orden"} · envío ${r.envio.RUTAS.join(" → ")} por ${r.envio.SENDER === "no" ? "RPC" : "Sender " + r.envio.SENDER}</small></p>
  <small>topes: ${r.topes.MAX_ABIERTAS} tokens · ${r.topes.MAX_EXPUESTO} SOL dentro · pérdida máx del día ${r.topes.PERDIDA_DIA} SOL · reserva ${r.topes.RESERVA} SOL · prioridad ${r.topes.PRIO_ENTRADA}/${r.topes.PRIO_SALIDA} SOL</small></div>`; })()}
<div class="c"><b>Las últimas</b> <small>(papel: real estimado, SOL)</small><div style="overflow-x:auto"><table><tr><th>hora</th><th>token</th><th>salida</th><th>lotes</th><th>%</th><th>real</th></tr>${filas || "<tr><td colspan=6>aún ninguna</td></tr>"}</table></div></div>
<div class="c"><small>grabador: ${S.migs} migraciones · grabando ${S.camaras.size} · ${S.curvas} curvas · nacimientos ${S.nacHechos}/${S.nacHechos + S.nacFallos} · SOL $${u.toFixed(0)} · errores ${S.errores} · <a href="/estado">/estado</a> · <a href="/ultimas">/ultimas</a></small></div></body></html>`);
};
http.createServer((req, res) => { atiende(req, res).catch((e) => { S.errores++; log(`⚠️ error en el panel (${req.url}): ${e && e.message} · el server sigue`); try { if (!res.headersSent) res.statusCode = 500; res.end("error"); } catch {} }); })
  .listen(PORT, () => log(`🌐 panel en :${PORT} (/ , /viejo , /datos , /tokens , /estado)`));
// [8-oct] y por si acaso: un error suelto en una promesa se apunta, no tumba el server (perdería las cámaras y las posiciones en papel en curso)
process.on("unhandledRejection", (e) => { S.errores++; log(`⚠️ error sin atrapar: ${e && (e.stack || e.message || e)}`.slice(0, 400)); });

// ── arranque ──
log(`🟣 SOLANA EN PAPEL · ${PLAN ? "PLAN «" + PLAN.nombre + "»: " + PLAN.reglas.length + " reglas (" + PLAN.reglas.filter(r => r.acc < 0).length + " de no entrar) · si ninguna encaja" : "sin plan"}, motor de Pons con ${JSON.stringify(ESTRATEGIA) === JSON.stringify(ESTRELLA2) ? "⭐⭐" : "ESTRATEGIA " + JSON.stringify(ESTRATEGIA)} · veto de ballenas ${VETO_BALLENA ? "sí" : "NO"} · lote ${LOTE_SOL} SOL · espera del nacimiento ${NAC_ESPERA_S} s · cuentas en ${ESTADO_FILE}${P.cerradas ? ` (sigue: ${P.cerradas} cerradas, real ${P.real.toFixed(3)} SOL)` : ""}`);
log(`🟣 GRABADOR DE SOLANA · solo graba (no opera) · cámara ${CAMARA_MIN} min · latido ${LATIDO_S} s · muerto a los ${MUERTO_MIN} min · hasta ${WS_MAX} conexiones de Helius con ${MAX_SUBS_WS} suscripciones cada una · nacimiento por ${PUMP_API ? "pump.fun (y Helius si falla)" : "HELIUS (tenedores al migrar, concentración y edad)"} · precio ${MODO_WS ? "por WebSocket, swap a swap" : `leyendo la piscina cada ${POLL_S} s (getMultipleAccounts)`}`);
await REAL.init({ log, papelDe });
await precioSol(); setInterval(precioSol, 5 * 60e3); log(`💱 SOL a $${S.solUsd.toFixed(2)}`);
if (MODO_WS) heliusNueva(); abrePumpPortal();
process.on("SIGTERM", () => { for (const c of [...S.camaras.values()]) cierra(c, "apagado"); guardaP(); setTimeout(() => process.exit(0), 500); });
