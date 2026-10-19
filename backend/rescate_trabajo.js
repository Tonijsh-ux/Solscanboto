// ═══════════════════════════════════════════════════════════════════════════════════════════
//  EL RESCATE, DENTRO DEL SERVER · el trabajador (va en un hilo aparte) · 12-oct-2026
//  Lo mismo que hacían en el PC rescate_helius.js y rescate_rasgos.js, pero solo, token a token, al cerrar cada cámara:
//   (A) la curva de bonos de pump.fun operación a operación, con cartera (desde la creación hasta la migración)
//   (B) los swaps de la piscina de PumpSwap uno a uno, con cartera (toda la cámara)
//   (C) si el server se apagó a mitad de la cámara: esos mismos swaps en rotos/, con la curva rehecha
//   (D) la migración exacta en la cadena, el creador y los metadatos
//  Deja los MISMOS ficheros que el rescate del PC (curva/, swaps/, rotos/, extras/: los leen rescate_rasgos.js y prepara_swaps.js)
//  y devuelve los rasgos de cada token (bundle, creador, primeros compradores, concentración, quién vende en la piscina, bots…).
//  Va en un hilo aparte para que leer miles de transacciones no frene la grabación ni el motor.
//  La clave de Helius la coge de la variable de entorno HELIUS_API_KEY (la misma del server). Nunca va en el código.
//  Créditos (Helius, oct-2026): getTransactionsForAddress completo 10 por cada 100 transacciones (mínimo 10); getTransaction 1;
//  getSignaturesForAddress 1; getAsset 10 (si el server ya lo pidió al nacer el token, se reutiliza y no cuesta nada).
// ═══════════════════════════════════════════════════════════════════════════════════════════
import { parentPort, workerData } from "worker_threads";
import fs from "fs"; import path from "path"; import zlib from "zlib";

const WD = workerData || {};
const DIR = WD.dir;
const RPS = Math.max(0.5, +WD.rps || 4), MIN_ENTRE = 1000 / RPS;   // llamadas por segundo (las del server van aparte y son pocas)
const HILOS = Math.max(1, +WD.hilos || 1);
const KEY = (process.env.HELIUS_API_KEY || "").trim();
const URL_RPC = WD.rpcUrl || process.env.RESCATE_RPC_URL || `https://mainnet.helius-rpc.com/?api-key=${KEY}`;   // RESCATE_RPC_URL solo para las pruebas (un Helius de mentira)
const WSOL = "So11111111111111111111111111111111111111112";
const ES_BOT = new Set(WD.bots || []);
const BOT_A = "AgmLJBMDCqWynYnQiPCuj9ewsNNsBJXyzoUhD9LJzN51", BOT_F = "FHpcNSe6tb2n15bAdq4BkeYWGyZKFD7yLYrH92ng7wCT";
const TOPE_TX_CURVA = +WD.topeTxCurva || 60000, TOPE_TX_PISCINA = +WD.topeTxPiscina || 150000;
for (const d of ["curva", "swaps", "rotos", "extras", "parcial"]) fs.mkdirSync(path.join(DIR, d), { recursive: true });
const manda = (m) => parentPort.postMessage(m);
const duerme = (ms) => new Promise(r => setTimeout(r, ms));

// ── la llamada a Helius, con reintentos y el gasto apuntado ──
class Corte extends Error { constructor(m, tipo) { super(m); this.tipo = tipo; } }   // tipo: «creditos» (Helius se quedó sin créditos) · «clave» (la clave no vale) · «disco» (lleno) · «tope»
let ultimo = 0;
const SIN_CREDITOS = /max usage|out of credits|insufficient credits|credits? (are |have been )?(exhausted|used up)|credit limit|usage limit|exceeded (your|the) (monthly )?(credit|usage)/i;
async function rpc(job, method, params, fase, cuesta) {
  if (job.creditos >= job.tope + 300) throw new Corte(`tope del trabajo: ${job.creditos} de ${job.tope} créditos`, "tope");
  let ultErr = "", avisosCreditos = 0;
  for (let intento = 0; intento < 7; intento++) {
    const espera = ultimo + MIN_ENTRE - Date.now(); if (espera > 0) await duerme(espera); ultimo = Date.now();
    let res, txt = "", j = null;
    try { res = await fetch(URL_RPC, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }), signal: AbortSignal.timeout(120000) }); txt = await res.text(); }
    catch (e) { ultErr = e.message; job.errores++; await duerme(2000 * Math.pow(2, intento)); continue; }
    if (res.status === 401) throw new Corte(`Helius contesta 401: la clave no vale`, "clave");
    try { j = JSON.parse(txt); } catch { j = null; }
    const msg = (j && j.error && (j.error.message || JSON.stringify(j.error))) || (res.ok ? "" : `HTTP ${res.status} ${txt.slice(0, 160)}`);
    if (msg && SIN_CREDITOS.test(msg)) { if (++avisosCreditos >= 2) throw new Corte(`Helius sin créditos: ${msg.slice(0, 160)}`, "creditos"); job.errores++; await duerme(30000); continue; }
    if (res.status === 429 || res.status >= 500 || (j && j.error && (j.error.code === -32005 || j.error.code === -32429 || /rate|too many/i.test(msg)))) { ultErr = msg; job.errores++; await duerme(2000 * Math.pow(2, intento)); continue; }
    if (!j) { ultErr = `respuesta rara (${res.status}): ${txt.slice(0, 80)}`; job.errores++; await duerme(2000 * Math.pow(2, intento)); continue; }
    if (j.error) throw new Error(`${method}: ${msg.slice(0, 200)}`);
    const r = j.result; const c = cuesta(r); job.creditos += c; job.porFase[fase] = (job.porFase[fase] || 0) + c; manda({ tipo: "gasto", id: job.id, n: c, fase });
    return r;
  }
  throw new Error(`${method}: 7 intentos fallidos (${String(ultErr).slice(0, 140)})`);
}
const costeHist = (r) => { const n = Array.isArray(r?.data) ? r.data.length : 0; return Math.max(10, Math.ceil(n / 100) * 10); };

// ── el historial completo de una dirección, página a página (cada página se decodifica y se suelta: 1.000 transacciones completas pesan ~6-20 MB) ──
async function historial(job, direccion, desde, hasta, fase, topeTx, extraFiltros, porPagina) {
  let token = null, n = 0, cortado = null; let filtros = { status: "succeeded", blockTime: { gte: Math.floor(desde), lte: Math.ceil(hasta) }, ...(extraFiltros || {}) };
  for (let pag = 0; pag < 400; pag++) {
    if (job.creditos + 100 > job.tope) { cortado = job.topeMax ? "tope" : "presupuesto"; break; }   // una página llena cuesta 100: no se pasa del tope del trabajo (se repite otro día; si ya tenía el tope más alto posible, se guarda lo que hay)
    const opts = { transactionDetails: "full", encoding: "jsonParsed", maxSupportedTransactionVersion: 1, limit: 1000, sortOrder: "asc", commitment: "finalized", filters: filtros };
    if (token) opts.paginationToken = token;
    let r; try { r = await rpc(job, "getTransactionsForAddress", [direccion, opts], fase, costeHist); }
    catch (e) { if (!(e instanceof Corte) && extraFiltros && pag === 0 && /filter|invalid|param/i.test(e.message)) { filtros = { status: "succeeded", blockTime: filtros.blockTime }; extraFiltros = null; pag--; continue; } throw e; }
    const data = r?.data || []; n += data.length; if (data.length) porPagina(data, pag);
    token = r?.paginationToken; if (!token || !data.length) break;
    if (topeTx && n >= topeTx) { cortado = "tx"; break; }   // demasiadas transacciones: se guarda lo que hay, marcado como cortado
  }
  return { n, cortado };
}

// ── decodificar una transacción: la cartera, el cambio de tokens y de SOL en el «sitio» (la curva de bonos o la piscina) — igual que en el PC ──
function claves(tx) { const k = (tx.transaction?.message?.accountKeys || []).map(x => typeof x === "string" ? x : x.pubkey); const la = tx.meta?.loadedAddresses; if (la) { for (const x of la.writable || []) k.push(x); for (const x of la.readonly || []) k.push(x); } return k; }
function firmante(tx) { const ks = tx.transaction?.message?.accountKeys || []; const s = ks.find(x => typeof x === "object" && x.signer); return s ? s.pubkey : (typeof ks[0] === "string" ? ks[0] : ks[0]?.pubkey) || null; }
function saldoTok(bal, mint, owner) { let s = null; for (const b of bal || []) if (b.mint === mint && (!owner || b.owner === owner)) { const v = +(b.uiTokenAmount?.uiAmountString ?? b.uiTokenAmount?.uiAmount ?? 0); if (s == null || v > s) s = v; } return s; }
function decodifica(tx, mint, sitio, modo) {
  // modo «curva»: sitio = la cuenta de la curva de bonos (el SOL en lamports de esa cuenta; los tokens en su cuenta del token, owner = sitio)
  // modo «pool»: sitio = la piscina de PumpSwap (los tokens y el WSOL en cuentas cuyo owner es la piscina)
  const ks = claves(tx); const m = tx.meta || {}; const w = firmante(tx);
  const tokPre = saldoTok(m.preTokenBalances, mint, sitio), tokPost = saldoTok(m.postTokenBalances, mint, sitio);
  let solPre = null, solPost = null;
  if (modo === "pool") { solPre = saldoTok(m.preTokenBalances, WSOL, sitio); solPost = saldoTok(m.postTokenBalances, WSOL, sitio); }
  else { const i = ks.indexOf(sitio); if (i >= 0 && m.preBalances && m.postBalances) { solPre = m.preBalances[i] / 1e9; solPost = m.postBalances[i] / 1e9; } }
  const dTok = tokPre != null && tokPost != null ? tokPost - tokPre : null, dSol = solPre != null && solPost != null ? solPost - solPre : null;
  let lado = 0; if (dTok != null && Math.abs(dTok) > 1e-9) lado = dTok < 0 ? 1 : -1;   // el sitio suelta tokens = compra; recibe tokens = venta
  else if (dSol != null && Math.abs(dSol) > 1e-9) lado = dSol > 0 ? 1 : -1;
  // [19-oct] en la curva, soltar tokens Y SOL a la vez no es una compra: es la retirada al migrar (~85 SOL y 206,9 M de tokens a la piscina).
  // Venía como compra (lado 1) cuando su firma no era la de la migración que vio el server: 7-10 % de las curvas, ~85 SOL de más en
  // sol_comprado (y en el churn, mayor_compra, top1…). FRANCEDAY (19-oct): 171,8 SOL «comprados» en vez de 85,9.
  if (modo !== "pool" && dTok != null && dSol != null && dTok < -1e-9 && dSol < -1e-6) lado = 2;
  return { sig: tx.transaction?.signatures?.[0] || null, slot: tx.slot, idx: tx.transactionIndex, bt: tx.blockTime, w, lado, sol: dSol != null ? +Math.abs(dSol).toFixed(6) : null, tok: dTok != null ? +Math.abs(dTok).toFixed(2) : null, rt: tokPost != null ? +tokPost.toFixed(2) : null, rs: solPost != null ? +solPost.toFixed(6) : null, fee: m.fee != null ? m.fee / 1e9 : null, nk: ks.length };
}
// la cuenta de la curva de bonos: en las primeras operaciones, el dueño (que no es quien firma) con más tokens del mint
function sitioCurva(txs, mint) { const cuenta = new Map(); for (const tx of txs.slice(0, 30)) { const w = firmante(tx); for (const b of tx.meta?.postTokenBalances || []) { if (b.mint !== mint || !b.owner || b.owner === w) continue; const v = +(b.uiTokenAmount?.uiAmountString ?? b.uiTokenAmount?.uiAmount ?? 0); cuenta.set(b.owner, Math.max(cuenta.get(b.owner) || 0, v)); } } let mejor = null; for (const [o, v] of cuenta) if (!mejor || v > mejor[1]) mejor = [o, v]; return mejor ? mejor[0] : null; }
// el flujo por minuto («swaps/SOL entra/SOL sale», como el flujo= del server) y el detalle por segundo («n|SOL neto|máx|mín», como el ptx=)
function flujoDe(rows, tRef, pctDe) { const F = [], S = new Map(); let mxMin = -1;
  for (const r of rows) { if (r.lado !== 1 && r.lado !== -1) continue; const t = r.bt - tRef; if (t < 0) continue; const m = Math.floor(t / 60); if (m > mxMin) mxMin = m; const f = F[m] || (F[m] = { s: 0, i: 0, o: 0 }); f.s++; if (r.lado === 1) f.i += r.sol || 0; else f.o += r.sol || 0;
    const sg = Math.floor(t); const q = S.get(sg) || { n: 0, s: 0, h: -Infinity, l: Infinity }; q.n++; q.s += r.lado === 1 ? (r.sol || 0) : -(r.sol || 0); const p = pctDe(r); if (p != null) { if (p > q.h) q.h = p; if (p < q.l) q.l = p; } S.set(sg, q); }
  const flujo = []; for (let m = 0; m <= mxMin; m++) { const f = F[m]; flujo.push(f ? `${f.s}/${f.i.toFixed(1)}/${f.o.toFixed(1)}` : "0/0/0"); }
  const ptx = [...S.entries()].sort((a, b) => a[0] - b[0]).map(([sg, q]) => [sg, q.n, +q.s.toFixed(2), isFinite(q.h) ? +q.h.toFixed(2) : null, isFinite(q.l) ? +q.l.toFixed(2) : null]);
  return { flujo: flujo.join(","), ptx }; }

// ── los ficheros (escritos de golpe: primero a .tmp y luego se renombran, para que nadie baje uno a medias) ──
const rel = (carpeta, mint, ext = ".json.gz") => `${carpeta}/${mint}${ext}`;
function escribe(job, r, buf, sinApuntar) { const f = path.join(DIR, r); const tmp = f + ".tmp";
  try { fs.writeFileSync(tmp, buf); fs.renameSync(tmp, f); } catch (e) { try { fs.unlinkSync(tmp); } catch {} if (e && (e.code === "ENOSPC" || e.code === "EDQUOT")) throw new Corte("el disco está lleno", "disco"); throw e; }
  if (!sinApuntar) { const st = fs.statSync(f); job.ficheros.push([r, st.size, Math.round(st.mtimeMs)]); } }
const gz = (job, r, obj) => escribe(job, r, zlib.gzipSync(JSON.stringify(obj)));
const lee = (r) => { try { return JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(DIR, r)))); } catch { return null; } };
// lo que se quedó a medias por el presupuesto del día: se guarda en parcial/ y la próxima vez sigue desde la última hora bajada (sin pagar dos veces)
const relParcial = (carpeta, mint) => `parcial/${carpeta}_${mint}.json.gz`;
const borraParcial = (carpeta, mint) => { try { fs.unlinkSync(path.join(DIR, relParcial(carpeta, mint))); } catch {} };
function guardaParcial(job, carpeta, mint, o) { escribe(job, relParcial(carpeta, mint), zlib.gzipSync(JSON.stringify(o)), true); }
// sigue el historial desde donde se quedó: mismas filas (sin repetir las del último segundo), y se apunta qué falta
async function historialSeguido(job, carpeta, mint, direccion, desde, hasta, fase, topeTx, extraFiltros, alPrincipio, decodificaTx) {
  const P = lee(relParcial(carpeta, mint)); const rows = P && Array.isArray(P.rows) ? P.rows : []; const extra = P ? (P.extra || {}) : {};
  const desde2 = rows.length ? rows[rows.length - 1].bt : desde; const vistas = new Set(rows.filter(x => x.bt >= desde2).map(x => x.sig));
  const h = await historial(job, direccion, desde2, hasta, fase, Math.max(1000, topeTx - rows.length), extraFiltros, (data, pag) => { if (pag === 0 && !rows.length) alPrincipio(data, extra); for (const tx of data) { const sig = tx.transaction?.signatures?.[0]; if (sig && vistas.has(sig)) continue; rows.push(decodificaTx(tx, extra)); } });
  if (h.cortado === "presupuesto") { guardaParcial(job, carpeta, mint, { rows, extra, hasta: rows.length ? rows[rows.length - 1].bt : null }); return { rows, extra, cortado: "presupuesto", n: rows.length }; }
  return { rows, extra, cortado: h.cortado, n: rows.length, seguido: !!P }; }

// ── la migración exacta (bloque y hora en la cadena): el ancla de las ventanas (nuestro t0 es la hora del aviso) ──
async function migracionDe(job, t) {
  if (t.firma) { try { const tx = await rpc(job, "getTransaction", [t.firma, { encoding: "json", commitment: "finalized", maxSupportedTransactionVersion: 0 }], "extras", () => 1); if (tx && tx.blockTime) return { bt: tx.blockTime, slot: tx.slot, deFirma: true }; } catch (e) { if (e instanceof Corte) throw e; } }
  return { bt: Math.round(t.t0 / 1000), slot: null, deFirma: false };
}
// [16-oct] lo que tardó el aviso: solo si nuestro t0 es el de verdad (en los atrasados es aproximado: va al minuto)
const latencia = (t, mig) => (mig && mig.deFirma && !t.t0Aprox && t.t0 > 0 ? t.t0 - mig.bt * 1000 : null);
// ── E · [16-oct] el creador de los que el server no supo quién creó: quien firma la 1ª transacción del mint (la que lo crea; lo mismo que hace
//    el server al nacer el token, pero aquí con una sola llamada: el historial del mint en orden, 1 transacción, 10 créditos) ──
async function faseCreador(job, t, mig) {
  if (t.creador) return { estado: "ya estaba", creador: t.creador };
  const hasta = mig && mig.bt ? Math.ceil(mig.bt + 2) : Math.ceil(Date.now() / 1000);
  const r = await rpc(job, "getTransactionsForAddress", [t.mint, { transactionDetails: "full", encoding: "jsonParsed", maxSupportedTransactionVersion: 1, limit: 1, sortOrder: "asc", commitment: "finalized", filters: { status: "succeeded", blockTime: { lte: hasta } } }], "creador", costeHist);
  const tx = r?.data?.[0]; const w = tx ? firmante(tx) : null;
  return { estado: w ? "ok" : "vacía", creador: w || null, creacionBt: tx?.blockTime ?? null };
}
// ── A · la curva de bonos ──
async function faseCurva(job, t, mig) {
  const r = rel("curva", t.mint);
  if (fs.existsSync(path.join(DIR, r))) { const c = lee(r); if (c && !c.vacio && Array.isArray(c.rows)) return { estado: "ya estaba", c }; }
  const tMig = mig.bt;
  // desde la creación (la edad sale del nacimiento, con 10 min de margen; si no se sabe o el mint tenía más de 5.000 firmas, 3 días antes) hasta la migración + 2 s
  const desde = (t.seg != null && !t.txsCurvaTope) ? tMig - t.seg - 600 : tMig - 3 * 86400;
  const h = await historialSeguido(job, "curva", t.mint, t.mint, desde, tMig + 2, "curva", TOPE_TX_CURVA, null,
    (data, ex) => { ex.sitio = sitioCurva(data, t.mint); ex.creacionBt = data[0].blockTime; }, (tx, ex) => decodifica(tx, t.mint, ex.sitio, "curva"));
  if (h.cortado === "presupuesto") return { estado: "sin presupuesto", c: null };   // a medias por el presupuesto del día: sigue otro día donde lo dejó
  borraParcial("curva", t.mint);
  const rows = h.rows, sitio = h.extra.sitio || null, creacionBt = h.extra.creacionBt ?? (rows[0] ? rows[0].bt : null);
  if (!rows.length) { gz(job, r, { mint: t.mint, sym: t.sym, vacio: true, migBt: tMig }); return { estado: "vacía", c: null }; }
  for (const x of rows) if (x.sig === t.firma || (x.lado === -1 && x.sol > 20 && x.rs != null && x.rs < 1)) x.lado = 2;   // 2 = la migración: la curva se queda sin SOL
  let pc0 = null; const pctC = (x) => { if (!(x.rt > 0 && x.rs > 0)) return null; const pr = x.rs / x.rt; if (pc0 == null) pc0 = pr; return (pr / pc0 - 1) * 100; };
  const FL = flujoDe(rows, creacionBt, pctC);
  const c = { mint: t.mint, sym: t.sym, grupo: t.grupo, t0: t.t0, migBt: tMig, migSlot: mig.slot, curvaPda: sitio, creacionBt, flujoDesdeCreacion: FL.flujo, ptxDesdeCreacion: FL.ptx, rows, ...(h.cortado ? { cortado: true } : {}) };
  gz(job, r, c);
  return { estado: h.cortado ? "cortada" : "ok", c };
}
// ── B y C · los swaps de la piscina (y la curva rehecha) ──
async function faseSwaps(job, t, mig, carpeta) {
  const r = rel(carpeta, t.mint);
  if (fs.existsSync(path.join(DIR, r))) { const p = lee(r); if (p && Array.isArray(p.rows) && !(t.durPiscina == null && p.ventanaS != null && p.ventanaS < Math.max(600, +t.dur || 3600))) return { estado: "ya estaba", p }; }   // (si había solo 10 min y ahora toca entera, se rehace)
  if (!t.pool) return { estado: "sin piscina", p: null };
  const tMig = mig.bt; const dur = t.durPiscina ? +t.durPiscina : Math.max(600, +t.dur || 3600);   // durPiscina: los no operados, solo los primeros minutos
  // solo las transacciones con transferencia del token (los swaps): sin el filtro, 1 de cada 3 era ruido de bots que tocan la piscina sin operar
  const h = await historialSeguido(job, carpeta, t.mint, t.pool, tMig - 2, Math.max(tMig, t.t0 / 1000) + dur + 20, carpeta, TOPE_TX_PISCINA, { tokenTransfer: { mint: t.mint } }, () => {}, (tx) => decodifica(tx, t.mint, t.pool, "pool"));
  if (h.cortado === "presupuesto") return { estado: "sin presupuesto", p: null };
  borraParcial(carpeta, t.mint);
  const rows = h.rows;
  for (const x of rows) if (x.sig === t.firma || (x.lado === -1 && x.tok > 1e8)) x.lado = 2;   // la migración (que crea la piscina y le mete los tokens) no es un swap
  // la curva rehecha: % respecto al precio inicial de la piscina (las reservas antes del 1er swap), un punto por segundo (el último del segundo), t relativo a nuestro t0
  let p0 = null; const curva = [];
  for (const x of rows) { if (x.lado !== 2 && x.rt > 0 && x.rs > 0) { const pr = x.rs / x.rt; if (p0 == null) { const rsPre = x.rs - x.lado * (x.sol || 0), rtPre = x.rt + x.lado * (x.tok || 0); p0 = (rsPre > 0 && rtPre > 0) ? rsPre / rtPre : pr; } const tt = Math.round(x.bt - t.t0 / 1000); const p = +((pr / p0 - 1) * 100).toFixed(2); if (curva.length && curva[curva.length - 1][0] === tt) curva[curva.length - 1][1] = p; else curva.push([tt, p]); } }
  const FL = flujoDe(rows, tMig, (x) => (p0 && x.rt > 0 && x.rs > 0) ? ((x.rs / x.rt) / p0 - 1) * 100 : null);
  const p = { mint: t.mint, sym: t.sym, grupo: t.grupo, t0: t.t0, migBt: tMig, migSlot: mig.slot, pool: t.pool, precioIniHelius: p0, precioIniServer: t.precioIni, flujo: FL.flujo, ptx: FL.ptx, rows, curva, ventanaS: dur, ...(h.cortado ? { cortado: true } : {}) };
  gz(job, r, p);
  return { estado: h.cortado ? "cortada" : (rows.length ? "ok" : "vacía"), p };
}
// ── D · extras (el creador, los metadatos) ──
async function faseExtras(job, t, mig) {
  const o = { mint: t.mint, sym: t.sym };
  if (mig.deFirma) { o.migSlot = mig.slot; o.migBlockTime = mig.bt; o.latenciaDeteccionMs = latencia(t, mig); }
  if (t.asset) Object.assign(o, t.asset, { assetDelServer: true });   // el server ya lo pidió al nacer el token: gratis
  else if (t.sinAsset) {   // [16-oct] los atrasados que solo necesitan el creador: el nombre y los enlaces ya los tenemos (rescate del PC); si el server ya tenía sus extras, se conservan
    o.sinAsset = true; try { const ant = JSON.parse(fs.readFileSync(path.join(DIR, rel("extras", t.mint, ".json")), "utf-8")); for (const k of ["nombre", "uri", "links", "descripcion", "mutable", "autoridades", "assetDelServer"]) if (ant[k] !== undefined) o[k] = ant[k]; } catch {} }
  else { try { const a = await rpc(job, "getAsset", { id: t.mint }, "extras", () => 10); o.nombre = a?.content?.metadata?.name; o.uri = a?.content?.json_uri; o.links = a?.content?.links || null; o.descripcion = (a?.content?.metadata?.description || "").slice(0, 300); o.mutable = a?.mutable; o.autoridades = (a?.authorities || []).map(x => x.address); } catch (e) { if (e instanceof Corte) throw e; o.errAsset = e.message; } }
  if (t.creador) { try { let n = 0, viejo = null, antes = undefined; for (let i = 0; i < 3; i++) { const sigs = await rpc(job, "getSignaturesForAddress", [t.creador, { limit: 1000, before: antes, commitment: "confirmed" }], "extras", () => 1); if (!sigs || !sigs.length) break; n += sigs.length; viejo = sigs[sigs.length - 1]; if (sigs.length < 1000) break; antes = viejo.signature; }
    o.creador = t.creador; o.creadorDe = t.creadorDe || "server"; o.creadorTx = n + (n >= 3000 ? "+" : ""); o.creadorDesde = viejo ? viejo.blockTime : null; } catch (e) { if (e instanceof Corte) throw e; o.errCreador = e.message; } }
  o.creditos = job.porFase.extras || 0;
  escribe(job, rel("extras", t.mint, ".json"), JSON.stringify(o));
  return { estado: "ok", o };
}

// ═══ LOS RASGOS (los mismos que rescate_rasgos.js en el PC) ═══
const r2 = (x) => x == null || !isFinite(x) ? null : Math.round(x * 100) / 100;
const maxSol = (L) => { let m = 0; for (const r of L) { const v = r.sol || 0; if (v > m) m = v; } return m; };   // = Math.max(0, …), sin reventar la pila con 100.000 swaps
function rasgosCurva(c, t, mig) {
  const SUPPLY = t.supply || 1e9;
  const R = (c.rows || []).filter(r => r.bt != null); if (!R.length) return null;
  const migBt = mig && mig.deFirma ? mig.bt : (c.migBt || t.t0 / 1000); const cre = c.creacionBt || R[0].bt;
  const slot0 = R[0].slot; const o = { creador: t.creador || null, creacion_utc: new Date(cre * 1000).toISOString(), migracion_utc: new Date(migBt * 1000).toISOString(), seg_curva: Math.max(0, migBt - cre), tx: R.length };
  const compras = R.filter(r => r.lado === 1), ventas = R.filter(r => r.lado === -1);
  o.compras = compras.length; o.ventas = ventas.length; o.sol_comprado = r2(compras.reduce((a, r) => a + (r.sol || 0), 0)); o.sol_vendido = r2(ventas.reduce((a, r) => a + (r.sol || 0), 0));
  // por cartera: neto de tokens, comprado, vendido, primera compra
  const W = new Map(); const orden = [];
  for (const r of R) { if (r.lado !== 1 && r.lado !== -1) continue; let w = W.get(r.w); if (!w) { w = { tokC: 0, tokV: 0, solC: 0, solV: 0, t1: r.bt, slot1: r.slot, n: 0 }; W.set(r.w, w); orden.push(r.w); } w.n++; if (r.lado === 1) { w.tokC += r.tok || 0; w.solC += r.sol || 0; } else { w.tokV += r.tok || 0; w.solV += r.sol || 0; } }
  o.carteras = W.size; o.compradores = [...W.values()].filter(w => w.tokC > 0).length; o.vendedores = [...W.values()].filter(w => w.tokV > 0).length;
  const neto = (w) => Math.max(0, w.tokC - w.tokV);
  // el creador
  const cr = t.creador && W.get(t.creador); o.creador_compro_sol = cr ? r2(cr.solC) : null; o.creador_pct_supply = cr ? r2(100 * cr.tokC / SUPPLY) : null; o.creador_vendio_pct = cr && cr.tokC > 0 ? r2(100 * Math.min(1, cr.tokV / cr.tokC)) : null; o.creador_conserva_pct = cr ? r2(100 * neto(cr) / SUPPLY) : null;
  // el bundle: compras en el mismo bloque que la creación, y en los primeros 5 s
  const enSlot0 = compras.filter(r => r.slot === slot0), en5s = compras.filter(r => r.bt - cre <= 5);
  o.bundle_slot0_compras = enSlot0.length; o.bundle_slot0_carteras = new Set(enSlot0.map(r => r.w)).size; o.bundle_slot0_pct = r2(100 * enSlot0.reduce((a, r) => a + (r.tok || 0), 0) / SUPPLY); o.bundle_slot0_sol = r2(enSlot0.reduce((a, r) => a + (r.sol || 0), 0));
  o.primeros5s_compras = en5s.length; o.primeros5s_carteras = new Set(en5s.map(r => r.w)).size; o.primeros5s_pct = r2(100 * en5s.reduce((a, r) => a + (r.tok || 0), 0) / SUPPLY);
  // los primeros 5/10/20 compradores: qué parte de lo comprado conservan al migrar
  const comp = orden.filter(w => W.get(w).tokC > 0); const ret = (n) => { const ws = comp.slice(0, n); if (!ws.length) return [null, null]; let cc = 0, nt = 0; for (const w of ws) { const x = W.get(w); cc += x.tokC; nt += neto(x); } return [r2(100 * nt / Math.max(1, cc)), r2(100 * nt / SUPPLY)]; };
  [o.primeros5_conservan_pct, o.primeros5_pct_supply] = ret(5); [o.primeros10_conservan_pct, o.primeros10_pct_supply] = ret(10); [o.primeros20_conservan_pct, o.primeros20_pct_supply] = ret(20);
  // concentración al migrar (por lo comprado en la curva y no vendido)
  const netos = [...W.values()].map(neto).filter(x => x > 0).sort((a, b) => b - a);
  o.top1_pct = r2(100 * (netos[0] || 0) / SUPPLY); o.top5_pct = r2(100 * netos.slice(0, 5).reduce((a, b) => a + b, 0) / SUPPLY); o.top10_pct = r2(100 * netos.slice(0, 10).reduce((a, b) => a + b, 0) / SUPPLY); o.tenedores_curva = netos.length; o.tenedores_mas1pct = netos.filter(x => x > SUPPLY / 100).length;
  // los que lo soltaron todo antes de migrar
  const fuera = [...W.values()].filter(w => w.tokC > 0 && w.tokV >= 0.9 * w.tokC); o.salieron_antes = fuera.length; o.salieron_antes_sol = r2(fuera.reduce((a, w) => a + w.solV, 0));
  // el llenado: SOL comprado en cada quinto del tiempo, y en los últimos 60 s
  const dur = Math.max(1, migBt - cre); const q = [0, 0, 0, 0, 0]; for (const r of compras) { const k = Math.min(4, Math.max(0, Math.floor(5 * (r.bt - cre) / dur))); q[k] += r.sol || 0; } const qs = q.reduce((a, b) => a + b, 0) || 1; for (let k = 0; k < 5; k++) o["llenado_q" + (k + 1) + "_pct"] = r2(100 * q[k] / qs);
  o.ultimos60s_sol = r2(compras.filter(r => migBt - r.bt <= 60).reduce((a, r) => a + (r.sol || 0), 0)); o.ultimos60s_compradores = new Set(compras.filter(r => migBt - r.bt <= 60).map(r => r.w)).size;
  o.primeros60s_compradores = new Set(compras.filter(r => r.bt - cre <= 60).map(r => r.w)).size; o.primeros300s_compradores = new Set(compras.filter(r => r.bt - cre <= 300).map(r => r.w)).size;
  // el precio en la curva: máximo, mayor retroceso, mayor venta, ventas grandes
  let p0 = null, mx = 0, dd = 0, pk = 0; for (const r of R) { if (!(r.rt > 0 && r.rs >= 0.5)) continue; const p = r.rs / r.rt; if (p0 == null) p0 = p; /* p0: el primer precio con ≥0,5 SOL en la curva (la creación sola no vale) */ const v = p / p0; if (v > pk) pk = v; if (v > mx) mx = v; const d = 1 - v / pk; if (d > dd) dd = d; }
  o.curva_max_x = r2(mx); o.curva_mayor_retroceso_pct = r2(100 * dd); o.mayor_venta_sol = r2(maxSol(ventas)); o.ventas_mas1sol = ventas.filter(r => (r.sol || 0) >= 1).length; o.mayor_compra_sol = r2(maxSol(compras));
  o.compra_mediana_sol = r2(compras.length ? compras.map(r => r.sol || 0).sort((a, b) => a - b)[Math.floor(compras.length / 2)] : null);
  // ritmo: swaps en el pico (ventana de 5 s) y el mayor hueco
  const bts = R.map(r => r.bt).sort((a, b) => a - b); let pico = 0, hueco = 0; for (let i = 0, j = 0; i < bts.length; i++) { while (bts[i] - bts[j] > 5) j++; pico = Math.max(pico, i - j + 1); if (i) hueco = Math.max(hueco, bts[i] - bts[i - 1]); } o.pico_swaps_5s = pico; o.mayor_hueco_s = hueco;
  // los bots en la curva: cuántos entran, cuánto SOL meten y sacan, qué parte del supply conservan al migrar; los 2 grandes aparte
  { let n = 0, sc = 0, sv = 0, net = 0; for (const [w, x] of W) { if (!ES_BOT.has(w)) continue; n++; sc += x.solC; sv += x.solV; net += neto(x); }
    o.bots_curva_n = n; o.bots_curva_sol_c = r2(sc); o.bots_curva_sol_v = r2(sv); o.bots_curva_pct_supply = r2(100 * net / SUPPLY);
    const a = W.get(BOT_A), f = W.get(BOT_F); o.botA_curva_sol_c = a ? r2(a.solC) : null; o.botA_curva_pct_supply = a ? r2(100 * neto(a) / SUPPLY) : null; o.botA_curva_t1 = a ? Math.round(a.t1 - cre) : null; o.botF_curva_sol_c = f ? r2(f.solC) : null; o.botF_curva_pct_supply = f ? r2(100 * neto(f) / SUPPLY) : null; o.botF_curva_t1 = f ? Math.round(f.t1 - cre) : null; }
  if (c.cortado) o.cortada = 1;
  return o;
}
function rasgosPiscina(p, t, curvaW, mig) {
  const R = (p.rows || []).filter(r => r.bt != null && (r.lado === 1 || r.lado === -1)); if (!R.length) return null;
  const migBt = mig && mig.deFirma ? mig.bt : (p.migBt || t.t0 / 1000);
  const o = { swaps: R.length, carteras: new Set(R.map(r => r.w)).size };
  const en = (a, b) => R.filter(r => r.bt - migBt >= a && r.bt - migBt < b);
  for (const [nm, a, b] of [["m1", 0, 60], ["m2", 60, 120], ["m3", 120, 180], ["m4_5", 180, 300], ["m6_10", 300, 600]]) { const S = en(a, b); const c = S.filter(r => r.lado === 1), v = S.filter(r => r.lado === -1);
    o[nm + "_compradores"] = new Set(c.map(r => r.w)).size; o[nm + "_vendedores"] = new Set(v.map(r => r.w)).size; o[nm + "_sol_in"] = r2(c.reduce((x, r) => x + (r.sol || 0), 0)); o[nm + "_sol_out"] = r2(v.reduce((x, r) => x + (r.sol || 0), 0)); o[nm + "_mayor_venta_sol"] = r2(maxSol(v)); o[nm + "_mayor_compra_sol"] = r2(maxSol(c)); }
  // los de la curva (bundle, creador, primeros) vendiendo en la piscina: cuándo y cuánto en los primeros 5 min
  const primeros5min = en(0, 300).filter(r => r.lado === -1);
  const deCurva = primeros5min.filter(r => curvaW && curvaW.has(r.w)); o.ventas5min_de_la_curva_sol = r2(deCurva.reduce((x, r) => x + (r.sol || 0), 0)); o.ventas5min_de_la_curva_n = deCurva.length;
  const creV = t.creador ? R.filter(r => r.lado === -1 && r.w === t.creador) : []; o.creador_vende_en_s = creV.length ? Math.round(creV[0].bt - migBt) : null; o.creador_vende_sol = r2(creV.reduce((x, r) => x + (r.sol || 0), 0));
  const bund = curvaW && curvaW.bundle ? R.filter(r => r.lado === -1 && curvaW.bundle.has(r.w)) : []; o.bundle_vende_en_s = bund.length ? Math.round(bund[0].bt - migBt) : null; o.bundle_vende_sol_5min = r2(bund.filter(r => r.bt - migBt < 300).reduce((x, r) => x + (r.sol || 0), 0));
  // los bots en la piscina: SOL que meten y sacan por ventana, cuándo venden por primera vez, cuántos distintos; los 2 grandes aparte
  { const B = R.filter(r => ES_BOT.has(r.w)); o.bots_piscina_n = new Set(B.map(r => r.w)).size;
    for (const [nm, a, b] of [["m1", 0, 60], ["m2", 60, 120], ["m3", 120, 180], ["m4_5", 180, 300], ["m6_10", 300, 600]]) { const S = B.filter(r => r.bt - migBt >= a && r.bt - migBt < b); o["bots_" + nm + "_sol_in"] = r2(S.filter(r => r.lado === 1).reduce((x, r) => x + (r.sol || 0), 0)); o["bots_" + nm + "_sol_out"] = r2(S.filter(r => r.lado === -1).reduce((x, r) => x + (r.sol || 0), 0)); }
    const bv = B.filter(r => r.lado === -1); o.bots_primera_venta_s = bv.length ? Math.round(bv[0].bt - migBt) : null; o.bots_sol_in_total = r2(B.filter(r => r.lado === 1).reduce((x, r) => x + (r.sol || 0), 0)); o.bots_sol_out_total = r2(bv.reduce((x, r) => x + (r.sol || 0), 0));
    for (const [nm, w] of [["botA", BOT_A], ["botF", BOT_F]]) { const S = R.filter(r => r.w === w); const c5 = S.filter(r => r.lado === 1 && r.bt - migBt < 300), v5 = S.filter(r => r.lado === -1 && r.bt - migBt < 300); o[nm + "_n"] = S.length; o[nm + "_sol_in_5min"] = r2(c5.reduce((x, r) => x + (r.sol || 0), 0)); o[nm + "_sol_out_5min"] = r2(v5.reduce((x, r) => x + (r.sol || 0), 0)); o[nm + "_sol_in"] = r2(S.filter(r => r.lado === 1).reduce((x, r) => x + (r.sol || 0), 0)); o[nm + "_sol_out"] = r2(S.filter(r => r.lado === -1).reduce((x, r) => x + (r.sol || 0), 0)); const pv = S.find(r => r.lado === -1), pc = S.find(r => r.lado === 1); o[nm + "_primera_compra_s"] = pc ? Math.round(pc.bt - migBt) : null; o[nm + "_primera_venta_s"] = pv ? Math.round(pv.bt - migBt) : null; } }
  // la mayor venta de toda la grabación y cuándo; y cuántas carteras nuevas (que no estaban en la curva) compran en el 1er minuto
  let mv = null; for (const r of R) if (r.lado === -1 && (!mv || (r.sol || 0) > (mv.sol || 0))) mv = r; o.mayor_venta_sol = mv ? r2(mv.sol) : null; o.mayor_venta_en_s = mv ? Math.round(mv.bt - migBt) : null;
  const nuevos1 = new Set(en(0, 60).filter(r => r.lado === 1 && !(curvaW && curvaW.has(r.w))).map(r => r.w)); o.m1_compradores_nuevos = nuevos1.size;
  o.flujo = p.flujo || null; o.precioIni_helius = p.precioIniHelius; o.precioIni_server = p.precioIniServer; o.ventana_s = p.ventanaS ?? null;
  if (p.cortado) o.cortada = 1;
  return o;
}

// ═══ UN TOKEN ENTERO ═══
async function trabaja(m) {
  const t = m.t, F = m.fases || {}; const job = { id: m.id, tope: Math.max(0, +m.tope || 0), topeMax: !!m.topeMax, creditos: 0, porFase: {}, errores: 0, ficheros: [] };
  const estado = {}; let corte = null, mig = null, C = null, Pz = null, X = null;
  try {
    mig = await migracionDe(job, t);
    if (F.curva) { try { const r = await faseCurva(job, t, mig); estado.curva = r.estado; C = r.c; } catch (e) { if (e instanceof Corte) throw e; estado.curva = "error: " + String(e.message).slice(0, 140); } }
    // si la curva ya estaba bajada (de antes de un reinicio) y ahora solo toca la piscina, se lee para saber quién compró en la curva
    if (!C && F.piscina && !F.curva) { const c = lee(rel("curva", t.mint)); if (c && !c.vacio) C = c; }
    // [16-oct] el creador que el server no supo: quien firma la 1ª transacción de la curva de bonos (la que crea el mint), o la fase del creador
    if (!t.creador && C && Array.isArray(C.rows) && C.rows.length && C.rows[0].w) { t.creador = C.rows[0].w; t.creadorDe = "curva"; }
    if (F.creador) { try { const r = await faseCreador(job, t, mig); estado.creador = r.estado; if (r.creador && !t.creador) { t.creador = r.creador; t.creadorDe = "1ª transacción"; } } catch (e) { if (e instanceof Corte) throw e; estado.creador = "error: " + String(e.message).slice(0, 140); } }
    if (F.piscina) { try { const r = await faseSwaps(job, t, mig, t.roto ? "rotos" : "swaps"); estado.piscina = r.estado; Pz = r.p; } catch (e) { if (e instanceof Corte) throw e; estado.piscina = "error: " + String(e.message).slice(0, 140); } }
    if (F.extras) { try { const r = await faseExtras(job, t, mig); estado.extras = r.estado; X = r.o; } catch (e) { if (e instanceof Corte) throw e; estado.extras = "error: " + String(e.message).slice(0, 140); } }
  } catch (e) { corte = e.tipo || "error"; estado.corte = String(e.message).slice(0, 200); }
  let curvaW = null; if (C && Array.isArray(C.rows) && C.rows.length) { curvaW = new Set(C.rows.filter(r => r.lado === 1).map(r => r.w)); curvaW.bundle = new Set(C.rows.filter(r => r.lado === 1 && r.slot === C.rows[0].slot).map(r => r.w)); }
  let fc = null, fp = null; try { fc = C ? rasgosCurva(C, t, mig) : null; } catch (e) { estado.rasgos_curva = "error: " + e.message; } try { fp = Pz ? rasgosPiscina(Pz, t, curvaW, mig) : null; } catch (e) { estado.rasgos_piscina = "error: " + e.message; }
  const fx = X ? { nombre: X.nombre ?? null, links: X.links ? Object.keys(X.links).join("|") : null, descripcion_len: (X.descripcion || "").length, mutable: X.mutable ?? null, creador: X.creador || null, creador_de: X.creadorDe ?? null, creador_tx: X.creadorTx ?? null, creador_desde_utc: X.creadorDesde ? new Date(X.creadorDesde * 1000).toISOString() : null, autoridades_n: Array.isArray(X.autoridades) ? X.autoridades.length : null } : null;
  const ficha = { v: 1, mint: t.mint, sym: t.sym, grupo: t.grupo, roto: t.roto ? 1 : 0, mig: mig ? { bt: mig.bt, slot: mig.slot, deFirma: mig.deFirma } : null, latencia_ms: latencia(t, mig),
    fases: estado, creditos: job.creditos, porFase: job.porFase, curva: fc, piscina: fp, extras: fx };
  manda({ tipo: "hecho", id: job.id, mint: t.mint, ficha, creditos: job.creditos, porFase: job.porFase, ficheros: job.ficheros, corte, errores: job.errores });
}

// ── el reparto: el server manda los trabajos de uno en uno; aquí van como mucho HILOS a la vez ──
const cola = []; let activos = 0;
function bombea() { while (activos < HILOS && cola.length) { const m = cola.shift(); activos++;
  trabaja(m).catch(e => manda({ tipo: "hecho", id: m.id, mint: m.t && m.t.mint, ficha: null, creditos: 0, porFase: {}, ficheros: [], corte: "error", error: String(e && e.message || e).slice(0, 200) })).finally(() => { activos--; bombea(); }); } }
parentPort.on("message", (m) => { if (m && m.tipo === "trabajo") { cola.push(m); bombea(); } });
manda({ tipo: "listo", hilos: HILOS, rps: RPS });
