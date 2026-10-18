// ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
//  MIRA UNA CARTERA · 18-oct-2026 · (Tony: «hazlo tú, te doy permiso») ¿de dónde le llegan los tokens a una cartera que solo vende?
//  Al arrancar, si está MIRA_CARTERA=<dirección>, pide a Helius las últimas MIRA_N (300) transacciones de esa cartera (getTransactionsForAddress,
//  10 créditos por cada 100) y las lee por los balances: para cada transacción, lo que cambia en los tokens de la cartera (por mint) y en su SOL, y
//  quién está al otro lado (la cuenta del mismo mint que cambia al revés). Clasifica: ENTRA (le llegan tokens sin pagar SOL: una transferencia →
//  de qué cartera), SALE (manda tokens), VENDE (tokens fuera, SOL dentro), COMPRA (SOL fuera, tokens dentro). Saca líneas [CARTERA] con el
//  resumen (quién le manda, cuántas veces, cuántos mints distintos; qué programas aparecen; quién firma) y guarda el detalle en
//  /data/cartera_<dirección>.json. Una vez por arranque; no toca nada más. MIRA_N (300, tope 1000).
// ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import fs from "fs";
const WSOL = "So11111111111111111111111111111111111111112";
const PROGRAMAS = { "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "pump.fun (curva)", pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA: "PumpSwap", JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4: "Jupiter v6", TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA: "Token", TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb: "Token-2022", "11111111111111111111111111111111": "System", ComputeBudget111111111111111111111111111111: "ComputeBudget", ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL: "ATA", DF1ow4mD9UVQdHsHSoXUhzKXLj2bwPi2k7AJmbzBVYMo: "DFlow" };
const corto = (s) => s ? String(s).slice(0, 4) + "…" + String(s).slice(-4) : "-";
export function lee(tx, A) {
  const m = tx.meta || {}; const msg = tx.transaction?.message || {}; const keys = (msg.accountKeys || []).map(k => typeof k === "string" ? { pubkey: k } : k);
  const firmantes = keys.filter(k => k.signer).map(k => k.pubkey); const iA = keys.findIndex(k => k.pubkey === A);
  const solA = iA >= 0 && m.preBalances && m.postBalances ? (m.postBalances[iA] - m.preBalances[iA]) / 1e9 : 0;
  const pre = new Map(), post = new Map();   // "owner|mint" → cantidad
  for (const b of m.preTokenBalances || []) pre.set(`${b.owner}|${b.mint}`, +(b.uiTokenAmount?.uiAmount || 0));
  for (const b of m.postTokenBalances || []) post.set(`${b.owner}|${b.mint}`, +(b.uiTokenAmount?.uiAmount || 0));
  const delta = new Map(); for (const k of new Set([...pre.keys(), ...post.keys()])) { const d = (post.get(k) || 0) - (pre.get(k) || 0); if (Math.abs(d) > 1e-9) delta.set(k, d); }
  const mios = [...delta].filter(([k]) => k.startsWith(A + "|")).map(([k, d]) => ({ mint: k.split("|")[1], d }));
  const wsolA = mios.find(x => x.mint === WSOL)?.d || 0; const toks = mios.filter(x => x.mint !== WSOL);
  const progs = new Set(); const memos = []; const MEMO = /^Memo/;
  const mira = (ins) => { if (!ins.programId) return; const nom = PROGRAMAS[ins.programId] || corto(ins.programId); progs.add(nom); if ((ins.program === "spl-memo" || /^Memo/.test(ins.programId)) && ins.parsed) memos.push(String(typeof ins.parsed === "string" ? ins.parsed : JSON.stringify(ins.parsed)).slice(0, 80)); };
  for (const ins of msg.instructions || []) mira(ins);
  for (const inner of m.innerInstructions || []) for (const ins of inner.instructions || []) mira(ins);
  // todos los cambios de tokens de la transacción (para los ejemplos): owner|mint → delta
  const todos = [...delta].map(([k, d]) => `${corto(k.split("|")[0])}|${corto(k.split("|")[1])}:${d > 0 ? "+" : ""}${Math.abs(d) >= 1000 ? d.toFixed(0) : d.toFixed(3)}`);
  const out = { sig: tx.transaction?.signatures?.[0] || tx.signature, t: tx.blockTime, firma: firmantes.includes(A) ? "ella" : corto(firmantes[0]), firmantes: firmantes.length, progs: [...progs].filter(p => p !== "ComputeBudget" && p !== "System" && p !== "ATA"), memos, todos, sol: +(solA + wsolA).toFixed(4), toks: [] };
  for (const x of toks) {
    // el otro lado: cuentas del mismo mint que cambian al revés
    const otros = [...delta].filter(([k, d]) => k.endsWith("|" + x.mint) && !k.startsWith(A + "|") && Math.sign(d) === -Math.sign(x.d)).map(([k, d]) => ({ owner: k.split("|")[0], d })).sort((a, b) => Math.abs(b.d) - Math.abs(a.d));
    const paga = (solA + wsolA) < -0.0005, cobra = (solA + wsolA) > 0.0005;
    const que = x.d > 0 ? (paga ? "COMPRA" : "ENTRA") : (cobra ? "VENDE" : "SALE");
    out.toks.push({ mint: x.mint, d: x.d, que, otro: otros[0]?.owner || null, otros: otros.length });
  }
  return out;
}
export async function mira(A, { rpc, log, n = 300, dir = "/data" }) {
  const t0 = Date.now(); let token = null; const txs = [];
  try {
    while (txs.length < n) {
      const opts = { transactionDetails: "full", encoding: "jsonParsed", maxSupportedTransactionVersion: 1, limit: Math.min(100, n - txs.length), sortOrder: "desc", commitment: "finalized", filters: { status: "succeeded" } };
      if (token) opts.paginationToken = token;
      const r = await rpc("getTransactionsForAddress", [A, opts], 30000); const data = r?.data || []; txs.push(...data); token = r?.paginationToken; if (!token || !data.length) break;
    }
  } catch (e) { log(`[CARTERA] ${corto(A)}: Helius no contesta (${String(e && e.message || e).slice(0, 120)}) · leídas ${txs.length}`); if (!txs.length) return; }
  const L = txs.map(tx => { try { return lee(tx, A); } catch (e) { return null; } }).filter(Boolean);
  const cuenta = (arr) => { const c = new Map(); for (const x of arr) c.set(x, (c.get(x) || 0) + 1); return [...c].sort((a, b) => b[1] - a[1]); };
  const tipos = cuenta(L.flatMap(x => x.toks.map(t => t.que))); const sinTok = L.filter(x => !x.toks.length).length;
  const entran = L.flatMap(x => x.toks.filter(t => t.que === "ENTRA").map(t => ({ ...t, firma: x.firma, progs: x.progs, t: x.t })));
  const deQuien = cuenta(entran.map(t => t.otro || "?")); const mintsDe = new Map(); for (const t of entran) { if (!mintsDe.has(t.otro)) mintsDe.set(t.otro, new Set()); mintsDe.get(t.otro).add(t.mint); }
  const desde = L.length ? new Date(Math.min(...L.map(x => x.t * 1000))).toISOString().slice(0, 16) : "-", hasta = L.length ? new Date(Math.max(...L.map(x => x.t * 1000))).toISOString().slice(0, 16) : "-";
  log(`[CARTERA] ${A} · ${L.length} transacciones leídas (${desde} → ${hasta}Z) · ${Math.round((Date.now() - t0) / 1000)} s · movimientos de tokens: ${tipos.map(([k, v]) => `${k} ${v}`).join(" · ") || "ninguno"} · sin tokens ${sinTok} · firma ella misma ${L.filter(x => x.firma === "ella").length}, firma otro ${L.filter(x => x.firma !== "ella").length}`);
  log(`[CARTERA] programas: ${cuenta(L.flatMap(x => x.progs)).slice(0, 8).map(([k, v]) => `${k} ${v}`).join(" · ")}`);
  if (entran.length) {
    log(`[CARTERA] le ENTRAN tokens ${entran.length} veces, de ${deQuien.length} carteras distintas; las que más: ${deQuien.slice(0, 8).map(([k, v]) => `${k} ×${v} (${mintsDe.get(k === "?" ? null : k)?.size || 0} mints; firma ${cuenta(entran.filter(t => (t.otro || "?") === k).map(t => t.firma))[0]?.[0]})`).join(" · ")} · mints que entran: ${cuenta(entran.map(t => corto(t.mint))).slice(0, 5).map(([k, v]) => `${k} ×${v}`).join(" · ")}`);
    const ej = entran.slice(0, 5).map(t => `${corto(t.mint)} ${t.d.toFixed(0)} tokens de ${t.otro || "?"} vía ${t.progs.join("+") || "transferencia"}`); log(`[CARTERA] ejemplos de entradas: ${ej.join(" | ")}`);
  }
  const vende = L.flatMap(x => x.toks.filter(t => t.que === "VENDE").map(t => ({ ...t, sol: x.sol, progs: x.progs })));
  if (vende.length) log(`[CARTERA] VENDE ${vende.length} veces · SOL que cobra: mediana ${vende.map(v => v.sol).sort((a, b) => a - b)[Math.floor(vende.length / 2)].toFixed(3)}, total ${vende.reduce((s, v) => s + v.sol, 0).toFixed(1)} · por: ${cuenta(vende.flatMap(v => v.progs)).slice(0, 4).map(([k, v]) => `${k} ${v}`).join(" · ")}`);
  const sale = L.flatMap(x => x.toks.filter(t => t.que === "SALE")); if (sale.length) log(`[CARTERA] SALEN tokens ${sale.length} veces hacia ${cuenta(sale.map(t => t.otro || "?")).slice(0, 5).map(([k, v]) => `${k} ×${v}`).join(" · ")}`);
  const solNeto = L.reduce((s, x) => s + x.sol, 0); log(`[CARTERA] SOL neto en estas ${L.length} transacciones: ${solNeto >= 0 ? "+" : ""}${solNeto.toFixed(2)}`);
  const memos = cuenta(L.flatMap(x => x.memos)); if (memos.length) log(`[CARTERA] memos (${L.filter(x => x.memos.length).length} transacciones con memo): ${memos.slice(0, 6).map(([k, v]) => `«${k}» ×${v}`).join(" · ")}`);
  for (const que of ["ENTRA", "COMPRA", "VENDE", "SALE"]) for (const x of L.filter(x => x.toks.some(t => t.que === que)).slice(0, 2)) log(`[CARTERA] ejemplo ${que}: ${x.sig} · firma ${x.firma} (${x.firmantes}) · SOL ${x.sol >= 0 ? "+" : ""}${x.sol} · ${x.progs.join("+")}${x.memos.length ? ` · memo «${x.memos[0]}»` : ""} · cambios: ${x.todos.slice(0, 8).join(" ")}`);
  try { const f = `${fs.existsSync(dir) ? dir : "/tmp"}/cartera_${A.slice(0, 8)}.json`; fs.writeFileSync(f, JSON.stringify({ A, leidas: L.length, desde, hasta, L })); log(`[CARTERA] detalle en ${f}`); } catch (e) {}
}

// ── [18-oct] VARIAS DE UNA VEZ (MIRA_CARTERAS=a,b,c; MIRA_N2 tx cada una, 100 de serie = 10 créditos): una línea [CARTERA62] por cartera con lo que
//    decide si es un bot de verdad o una app que firma por otros: en qué parte de sus transacciones cambian SUS tokens (propias), en cuántas solo cambian
//    los de otros (firma por otros), si le entra USDC pequeño y constante (comisión), memos, programas, SOL neto, y a qué ritmo opera. ──
export async function miraVarias(lista, { rpc, log, n = 100 }) {
  for (const A of lista) {
    try {
      const r = await rpc("getTransactionsForAddress", [A, { transactionDetails: "full", encoding: "jsonParsed", maxSupportedTransactionVersion: 1, limit: Math.min(100, n), sortOrder: "desc", commitment: "finalized", filters: { status: "succeeded" } }], 30000);
      const txs = r?.data || []; const L = txs.map(tx => { try { return lee(tx, A); } catch (e) { return null; } }).filter(Boolean);
      if (!L.length) { log(`[CARTERA62] ${A} · sin transacciones`); continue; }
      const propias = L.filter(x => x.toks.some(t => t.mint !== "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")).length;   // cambian sus tokens (sin contar USDC)
      const otros = L.filter(x => !x.toks.length && x.todos.length).length;   // solo cambian los tokens de otros
      const usdc = L.filter(x => x.toks.some(t => t.mint === "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" && t.d > 0 && t.d < 5)).length;
      const firma = L.filter(x => x.firma === "ella").length; const memos = L.filter(x => x.memos.length).length; const sol = L.reduce((s, x) => s + x.sol, 0);
      const t0 = Math.min(...L.map(x => x.t)), t1 = Math.max(...L.map(x => x.t)); const min = Math.max(1, (t1 - t0) / 60);
      const cuenta = (arr) => { const c = new Map(); for (const x of arr) c.set(x, (c.get(x) || 0) + 1); return [...c].sort((a, b) => b[1] - a[1]); };
      const tipos = cuenta(L.flatMap(x => x.toks.filter(t => t.mint !== "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").map(t => t.que)));
      const progs = cuenta(L.flatMap(x => x.progs)).slice(0, 5).map(([k, v]) => `${k} ${v}`).join(", ");
      const memoTxt = cuenta(L.flatMap(x => x.memos.map(m => m.replace(/[0-9A-Za-z]{30,}/g, "…")))).slice(0, 2).map(([k, v]) => `«${k}» ${v}`).join(", ");
      log(`[CARTERA62] ${A} · ${L.length} tx en ${min.toFixed(0)} min (${(L.length / min).toFixed(1)}/min) · firma ${Math.round(100 * firma / L.length)} % · SUS tokens cambian en ${Math.round(100 * propias / L.length)} % (${tipos.map(([k, v]) => `${k} ${v}`).join(" ") || "—"}) · solo los de OTROS en ${Math.round(100 * otros / L.length)} % · le entra USDC pequeño en ${Math.round(100 * usdc / L.length)} % · memos ${Math.round(100 * memos / L.length)} %${memoTxt ? ` (${memoTxt})` : ""} · SOL neto ${sol >= 0 ? "+" : ""}${sol.toFixed(2)} · ${progs}`);
    } catch (e) { log(`[CARTERA62] ${A} · Helius: ${String(e && e.message || e).slice(0, 100)}`); }
    await new Promise(r => setTimeout(r, 700));
  }
  log(`[CARTERA62] fin: ${lista.length} carteras`);
}
