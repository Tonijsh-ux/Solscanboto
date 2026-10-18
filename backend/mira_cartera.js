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
  const progs = new Set(); for (const ins of msg.instructions || []) if (ins.programId) progs.add(PROGRAMAS[ins.programId] || corto(ins.programId));
  for (const inner of m.innerInstructions || []) for (const ins of inner.instructions || []) if (ins.programId) progs.add(PROGRAMAS[ins.programId] || corto(ins.programId));
  const out = { sig: tx.transaction?.signatures?.[0] || tx.signature, t: tx.blockTime, firma: firmantes.includes(A) ? "ella" : corto(firmantes[0]), progs: [...progs].filter(p => p !== "ComputeBudget" && p !== "System" && p !== "ATA"), sol: +(solA + wsolA).toFixed(4), toks: [] };
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
    log(`[CARTERA] le ENTRAN tokens ${entran.length} veces, de ${deQuien.length} carteras distintas; las que más: ${deQuien.slice(0, 8).map(([k, v]) => `${k} ×${v} (${mintsDe.get(k)?.size || 0} mints; firma ${cuenta(entran.filter(t => t.otro === k).map(t => t.firma))[0]?.[0]})`).join(" · ")}`);
    const ej = entran.slice(0, 5).map(t => `${corto(t.mint)} ${t.d.toFixed(0)} tokens de ${t.otro || "?"} vía ${t.progs.join("+") || "transferencia"}`); log(`[CARTERA] ejemplos de entradas: ${ej.join(" | ")}`);
  }
  const vende = L.flatMap(x => x.toks.filter(t => t.que === "VENDE").map(t => ({ ...t, sol: x.sol, progs: x.progs })));
  if (vende.length) log(`[CARTERA] VENDE ${vende.length} veces · SOL que cobra: mediana ${vende.map(v => v.sol).sort((a, b) => a - b)[Math.floor(vende.length / 2)].toFixed(3)}, total ${vende.reduce((s, v) => s + v.sol, 0).toFixed(1)} · por: ${cuenta(vende.flatMap(v => v.progs)).slice(0, 4).map(([k, v]) => `${k} ${v}`).join(" · ")}`);
  const sale = L.flatMap(x => x.toks.filter(t => t.que === "SALE")); if (sale.length) log(`[CARTERA] SALEN tokens ${sale.length} veces hacia ${cuenta(sale.map(t => t.otro || "?")).slice(0, 5).map(([k, v]) => `${k} ×${v}`).join(" · ")}`);
  const solNeto = L.reduce((s, x) => s + x.sol, 0); log(`[CARTERA] SOL neto en estas ${L.length} transacciones: ${solNeto >= 0 ? "+" : ""}${solNeto.toFixed(2)}`);
  try { const f = `${fs.existsSync(dir) ? dir : "/tmp"}/cartera_${A.slice(0, 8)}.json`; fs.writeFileSync(f, JSON.stringify({ A, leidas: L.length, desde, hasta, L })); log(`[CARTERA] detalle en ${f}`); } catch (e) {}
}
