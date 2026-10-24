// ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
//  LA CURVA DE BONOS, LEÍDA AL MIGRAR · 24-oct-2026 (Tony: «quiero poner la de los 310 SOL pero con el lote reinvirtiendo»)
//  Para el 2º lote: lee de Helius la curva de bonos del token entera (getTransactionsForAddress del mint, desde que se creó hasta la migración),
//  decodificada con EL MISMO decodifica() que la fase A del rescate (rescate_trabajo.js: con esas curvas se eligió la regla), y saca lo que mira el veto:
//   · la 🏭 (fábrica de volumen: «sube en un segundo a 27-30K, lateral, migra, tontea y pum»): SOL comprado en el bloque de creación / 85 ≥ 0,40 ·
//     de 6 a 900 s hasta migrar · churn ((SOL comprado − el del bloque) + SOL vendido) / (85 − el del bloque) ≥ 9,7
//   · las recompras: carteras que compran 2 o más veces en la curva / carteras que compran.
//  En el 3-23 oct (NUEVO/fam2/regla_recompran.py, 9 SOL de verdad): la regla ANCHA sin 🏭 +243,62 SOL (847 ops); quitando además «recompran > 0,22»
//  +310,27 (552 ops), y quita perdedoras en los tres trozos (3-8 · 9-13 · 14-23).
//  Créditos de Helius: 10 por cada 100 transacciones de una página (mínimo 10). Una curva tiene de mediana ~500 transacciones → ~50-100 por token.
//  Las transacciones de la curva tienen que estar «finalized» (~13 s después): se lee a partir de los LOTE2_CURVA_T s (20) de migrar.
// ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

const WSOL = "So11111111111111111111111111111111111111112";

// ── decodificar una transacción: la cartera, el cambio de tokens y de SOL en la curva — copiado TAL CUAL de rescate_trabajo.js (no tocar por separado) ──
function claves(tx) { const k = (tx.transaction?.message?.accountKeys || []).map(x => typeof x === "string" ? x : x.pubkey); const la = tx.meta?.loadedAddresses; if (la) { for (const x of la.writable || []) k.push(x); for (const x of la.readonly || []) k.push(x); } return k; }
function firmante(tx) { const ks = tx.transaction?.message?.accountKeys || []; const s = ks.find(x => typeof x === "object" && x.signer); return s ? s.pubkey : (typeof ks[0] === "string" ? ks[0] : ks[0]?.pubkey) || null; }
function saldoTok(bal, mint, owner) { let s = null; for (const b of bal || []) if (b.mint === mint && (!owner || b.owner === owner)) { const v = +(b.uiTokenAmount?.uiAmountString ?? b.uiTokenAmount?.uiAmount ?? 0); if (s == null || v > s) s = v; } return s; }
export function decodifica(tx, mint, sitio, modo) {
  const ks = claves(tx); const m = tx.meta || {}; const w = firmante(tx);
  const tokPre = saldoTok(m.preTokenBalances, mint, sitio), tokPost = saldoTok(m.postTokenBalances, mint, sitio);
  let solPre = null, solPost = null;
  if (modo === "pool") { solPre = saldoTok(m.preTokenBalances, WSOL, sitio); solPost = saldoTok(m.postTokenBalances, WSOL, sitio); }
  else { const i = ks.indexOf(sitio); if (i >= 0 && m.preBalances && m.postBalances) { solPre = m.preBalances[i] / 1e9; solPost = m.postBalances[i] / 1e9; } }
  const dTok = tokPre != null && tokPost != null ? tokPost - tokPre : null, dSol = solPre != null && solPost != null ? solPost - solPre : null;
  let lado = 0; if (dTok != null && Math.abs(dTok) > 1e-9) lado = dTok < 0 ? 1 : -1;   // el sitio suelta tokens = compra; recibe tokens = venta
  else if (dSol != null && Math.abs(dSol) > 1e-9) lado = dSol > 0 ? 1 : -1;
  if (modo !== "pool" && dTok != null && dSol != null && dTok < -1e-9 && dSol < -1e-6) lado = 2;   // soltar tokens Y SOL a la vez = la retirada al migrar
  return { sig: tx.transaction?.signatures?.[0] || null, slot: tx.slot, idx: tx.transactionIndex, bt: tx.blockTime, w, lado, sol: dSol != null ? +Math.abs(dSol).toFixed(6) : null, tok: dTok != null ? +Math.abs(dTok).toFixed(2) : null, rt: tokPost != null ? +tokPost.toFixed(2) : null, rs: solPost != null ? +solPost.toFixed(6) : null, fee: m.fee != null ? m.fee / 1e9 : null, nk: ks.length };
}
// la cuenta de la curva de bonos: en las primeras operaciones, el dueño (que no es quien firma) con más tokens del mint (igual que el rescate)
export function sitioCurva(txs, mint) { const cuenta = new Map(); for (const tx of txs.slice(0, 30)) { const w = firmante(tx); for (const b of tx.meta?.postTokenBalances || []) { if (b.mint !== mint || !b.owner || b.owner === w) continue; const v = +(b.uiTokenAmount?.uiAmountString ?? b.uiTokenAmount?.uiAmount ?? 0); cuenta.set(b.owner, Math.max(cuenta.get(b.owner) || 0, v)); } } let mejor = null; for (const [o, v] of cuenta) if (!mejor || v > mejor[1]) mejor = [o, v]; return mejor ? mejor[0] : null; }

// ── los rasgos que mira el veto, de las filas de la curva (lo mismo que NUEVO/fam2/carteras_curva.py y la 🏭 de malos.py) ──
export const FAB = { vela: 0.40, segMin: 6, segMax: 900, churn: 9.7 };
export function rasgos(filas, creacionBt, migBt, creador) {
  const rows = filas.filter(r => r.lado === 1 || r.lado === -1).sort((a, b) => (a.slot - b.slot) || ((a.idx || 0) - (b.idx || 0)));
  if (!rows.length) return null;
  const compras = new Map(), vendedoras = new Set(); let solc = 0, solv = 0, ncompras = 0, nventas = 0, creadorSol = 0;
  for (const r of rows) { const s = +r.sol || 0; if (r.lado === 1) { compras.set(r.w, (compras.get(r.w) || 0) + 1); solc += s; ncompras++; if (creador && r.w === creador) creadorSol += s; } else { solv += s; nventas++; if ((+r.tok || 0) > 0) vendedoras.add(r.w); } }
  let recompran = 0; for (const n of compras.values()) if (n >= 2) recompran++;
  const s0 = rows[0].slot; let b0 = 0; for (const r of rows) { if (r.slot !== s0) break; if (r.lado === 1) b0 += +r.sol || 0; }
  const cre = creacionBt != null ? creacionBt : Math.min(...rows.map(r => r.bt).filter(x => x != null));
  const seg = migBt && cre != null && isFinite(cre) ? migBt - cre : null;
  const vela = b0 / 85, churn = ((solc - b0) + solv) / Math.max(85 - b0, 1);
  const fab = vela >= FAB.vela && seg != null && seg >= FAB.segMin && seg <= FAB.segMax && churn >= FAB.churn;
  // [25-oct] para F1 (propia/techo.py): vendedores = carteras que vendieron tokens en la curva (como rasgosCurva del rescate: tokV > 0); creadorSol = SOL que compró el creador (null si no se sabe quién es)
  return { compradores: compras.size, compras: ncompras, ventas: nventas, recompran: compras.size ? recompran / compras.size : 0, solc: +solc.toFixed(3), solv: +solv.toFixed(3),
    b0: +b0.toFixed(3), vela: +vela.toFixed(4), seg, churn: +churn.toFixed(3), fab, vendedores: vendedoras.size, creadorSol: creador ? +creadorSol.toFixed(4) : null };
}

// ── el lector: una cola pequeña (como mucho LECTORES a la vez), páginas de POR_PAGINA transacciones, tope de transacciones y de tiempo ──
export function crea(op = {}) {
  const log = op.log || console.log; const RPC = op.rpcUrl; const TOPE_TX = +(op.topeTx || 5000), POR_PAGINA = Math.min(1000, Math.max(100, +(op.porPagina || 500)));
  const LECTORES = Math.max(1, +(op.lectores || 3)), MAX_MS = +(op.maxMs || 35000);
  const S = { pedidas: 0, ok: 0, fallos: 0, cortadas: 0, creditos: 0, ms: [], ultErr: null, enCola: 0, activas: 0 };
  const cola = [];
  const coste = (n) => Math.max(10, Math.ceil(n / 100) * 10);
  async function llamada(params, hasta) {
    let ultErr = "";
    for (let intento = 0; intento < 4; intento++) {
      const queda = hasta - Date.now(); if (queda < 1500) break;
      try { const r = await fetch(RPC, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getTransactionsForAddress", params }), signal: AbortSignal.timeout(Math.min(20000, queda)) });
        const txt = await r.text(); let j = null; try { j = JSON.parse(txt); } catch {}
        const msg = (j && j.error && (j.error.message || JSON.stringify(j.error))) || (r.ok ? "" : `HTTP ${r.status} ${txt.slice(0, 120)}`);
        if (r.status === 429 || r.status >= 500 || (j && j.error && (j.error.code === -32005 || j.error.code === -32429 || /rate|too many/i.test(msg)))) { ultErr = msg || `HTTP ${r.status}`; await new Promise(z => setTimeout(z, 700 * (intento + 1))); continue; }
        if (!j) throw new Error(`respuesta rara (${r.status}): ${txt.slice(0, 80)}`);
        if (j.error) throw new Error(`getTransactionsForAddress: ${msg.slice(0, 160)}`);
        return j.result; }
      catch (e) { if (/getTransactionsForAddress:|respuesta rara/.test(e.message)) throw e; ultErr = e.message; await new Promise(z => setTimeout(z, 700 * (intento + 1))); }
    }
    throw new Error(`sin respuesta de Helius (${String(ultErr).slice(0, 100)})`);
  }
  // { tMig (s), seg (s desde la creación, del nacimiento; null si no se sabe), tope (el nacimiento llegó al tope de firmas), firma (la de la migración), creador (la cartera que creó el token, del nacimiento) }
  async function leeYa(mint, d) {
    const t0 = Date.now(), hasta = t0 + MAX_MS; const tMig = Math.round(d.tMig);
    const desde = (d.seg != null && !d.tope) ? tMig - d.seg - 600 : tMig - 3 * 86400;
    let token = null, sitio = null, creacionBt = null, n = 0, creditos = 0, cortada = false; const filas = [];
    for (let pag = 0; pag < 100; pag++) {
      const opts = { transactionDetails: "full", encoding: "jsonParsed", maxSupportedTransactionVersion: 1, limit: POR_PAGINA, sortOrder: "asc", commitment: "finalized", filters: { status: "succeeded", blockTime: { gte: Math.floor(desde), lte: Math.ceil(tMig + 2) } } };
      if (token) opts.paginationToken = token;
      const r = await llamada([mint, opts], hasta); const data = (r && r.data) || []; creditos += coste(data.length); S.creditos += coste(data.length);
      if (pag === 0) { if (!data.length) return { estado: "vacía", n: 0, creditos, ms: Date.now() - t0 }; sitio = sitioCurva(data, mint); creacionBt = data[0].blockTime ?? null; }
      for (const tx of data) filas.push(decodifica(tx, mint, sitio, "curva"));
      n += data.length; token = r && r.paginationToken;
      if (!token || !data.length) break;
      if (n >= TOPE_TX) { cortada = true; break; }
      if (Date.now() > hasta - 1500) { cortada = true; break; }
    }
    for (const x of filas) if ((d.firma && x.sig === d.firma) || (x.lado === -1 && x.sol > 20 && x.rs != null && x.rs < 1)) x.lado = 2;   // 2 = la migración (igual que el rescate)
    const ras = rasgos(filas, creacionBt, tMig, d.creador || null);
    if (!ras) return { estado: "sin compras", n, creditos, ms: Date.now() - t0, sitio };
    return { estado: "ok", rasgos: ras, n, creditos, ms: Date.now() - t0, cortada, sitio, creacionBt };
  }
  function bombea() {
    while (S.activas < LECTORES && cola.length) { const it = cola.shift(); S.enCola = cola.length; S.activas++;
      leeYa(it.mint, it.d).then(r => { if (r.estado === "ok") { S.ok++; if (r.cortada) S.cortadas++; } else S.fallos++; S.ms.push(r.ms); if (S.ms.length > 300) S.ms.shift(); it.ok(r); })
        .catch(e => { S.fallos++; S.ultErr = (e && e.message || String(e)).slice(0, 140); it.ok({ estado: "error", err: S.ultErr }); })
        .finally(() => { S.activas--; bombea(); }); }
  }
  function lee(mint, d) { S.pedidas++; return new Promise(ok => { cola.push({ mint, d, ok }); S.enCola = cola.length; bombea(); }); }
  function resumen() { const ms = S.ms.slice().sort((a, b) => a - b); return { pedidas: S.pedidas, ok: S.ok, fallos: S.fallos, cortadas: S.cortadas, creditos: S.creditos, msMed: ms.length ? ms[Math.floor(ms.length / 2)] : null, msP90: ms.length ? ms[Math.min(ms.length - 1, Math.floor(ms.length * 0.9))] : null, enCola: S.enCola, activas: S.activas, ultErr: S.ultErr }; }
  return { lee, resumen };
}
