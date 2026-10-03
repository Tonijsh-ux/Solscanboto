// ═══════════════════════════════════════════════════════════════════════════════════════════
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
const CAMARA_MIN = +(process.env.CAMARA_MIN || 90);
const MAX_SUBS_WS = +(process.env.MAX_SUBS_WS || 40);
const WS_MAX = +(process.env.WS_MAX || 4);
const PORT = +(process.env.PORT || 8080);
const LATIDO_S = +(process.env.LATIDO_S || 15);
const MUERTO_MIN = +(process.env.MUERTO_MIN || 10);
const SOLO_LOG = process.env.SOLO_LOG === "1";
const WSOL = "So11111111111111111111111111111111111111112";
const RPC = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const HELIUS_WS = `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const PP_WS = PUMPPORTAL_API_KEY ? `wss://pumpportal.fun/api/data?api-key=${PUMPPORTAL_API_KEY}` : "wss://pumpportal.fun/api/data";

if (!HELIUS_API_KEY) { console.log("❌ falta HELIUS_API_KEY: sin ella no hay precios. Me paro."); process.exit(1); }

// ── el estado ──
const S = { arranque: Date.now(), solUsd: 0, solUsdT: 0, migs: 0, ultimaMig: 0, curvas: 0, swaps: 0, bytes: 0, errores: 0,
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
async function rpc(method, params, ms = 8000) {
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

// la transacción de la migración: la piscina y el precio inicial (con reintentos, porque PumpPortal avisa en cuanto la ve)
async function leeMigracion(firma, mint) {
  for (let i = 0; i < 6; i++) {
    try { const tx = await rpc("getTransaction", [firma, { encoding: "jsonParsed", commitment: "confirmed", maxSupportedTransactionVersion: 0 }]);
      if (tx) { const r = reservasDe(tx.meta?.postTokenBalances, mint, null); if (r) return r; return null; } } catch (e) { if (i === 5) throw e; }
    await new Promise(r => setTimeout(r, 1500));
  }
  return null;
}

// ── las conexiones con Helius (varias, con un tope de suscripciones cada una) ──
function heliusNueva() {
  const H = { ws: null, subs: new Map(), porSub: new Map(), pend: new Map(), nId: 0, ok: false, reconexiones: 0 };
  const abre = () => {
    const ws = new WebSocket(HELIUS_WS); H.ws = ws;
    ws.on("open", () => { H.ok = true; log(`🔌 Helius: conexión ${S.hel.indexOf(H) + 1} abierta`); for (const c of S.camaras.values()) if (c.hel === H && !c.fin) { H.subs.delete(c.mint); suscribe(H, c); } });   // al reconectar, las cámaras vivas se vuelven a suscribir
    ws.on("message", (raw) => { S.bytes += raw.length; let m; try { m = JSON.parse(raw); } catch { return; } mensajeHelius(H, m); });
    ws.on("close", () => { H.ok = false; H.reconexiones++; H.subs.clear(); H.porSub.clear(); H.pend.clear(); setTimeout(abre, Math.min(30000, 2000 * H.reconexiones)); });
    ws.on("error", (e) => { S.errores++; log(`⚠️ Helius: ${e.message}`); });
  };
  abre(); S.hel.push(H); return H;
}
function heliusConHueco() { for (const H of S.hel) if (H.ok && H.subs.size < MAX_SUBS_WS) return H; if (S.hel.length < WS_MAX) return heliusNueva(); return S.hel.reduce((a, b) => (a.subs.size <= b.subs.size ? a : b)); }
function suscribe(H, c) { if (!H.ok || H.ws.readyState !== WebSocket.OPEN) return; const id = ++H.nId; H.pend.set(id, c.mint);
  H.ws.send(JSON.stringify({ jsonrpc: "2.0", id, method: "transactionSubscribe", params: [{ accountInclude: [c.pool || c.mint], failed: false, vote: false },
    { commitment: "processed", encoding: "jsonParsed", transactionDetails: "accounts", maxSupportedTransactionVersion: 0 }] })); }
function desuscribe(H, c) { const id = H.subs.get(c.mint); if (id == null) return; H.subs.delete(c.mint); H.porSub.delete(id); if (H.ok && H.ws.readyState === WebSocket.OPEN) H.ws.send(JSON.stringify({ jsonrpc: "2.0", id: ++H.nId, method: "transactionUnsubscribe", params: [id] })); }

function mensajeHelius(H, m) {
  if (m.id && H.pend.has(m.id)) { const mint = H.pend.get(m.id); H.pend.delete(m.id);
    if (m.error) { S.errores++; log(`⚠️ Helius rechazó la suscripción de ${corto(mint)}: ${m.error.message || JSON.stringify(m.error)}`); return; }
    H.subs.set(mint, m.result); H.porSub.set(m.result, mint); return; }
  if (m.method !== "transactionNotification") return;
  const mint = H.porSub.get(m.params?.subscription); if (!mint) return; const c = S.camaras.get(mint); if (!c || c.fin) return;
  const firma = m.params?.result?.signature; if (firma) { if (c.vistas.has(firma)) return; c.vistas.add(firma); if (c.vistas.size > 4000) c.vistas = new Set([...c.vistas].slice(-2000)); }
  const meta = m.params?.result?.transaction?.meta; const r = reservasDe(meta?.postTokenBalances, mint, c.pool); if (!r) return;
  if (!c.pool) { c.pool = r.pool; log(`🏊 ${c.sym}: piscina ${corto(c.pool)} descubierta en el 1er swap`); desuscribe(H, c); suscribe(H, c); }   // y a partir de ahora solo las transacciones de la piscina (menos tráfico)
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
function punto(c, t, precio) { const p = (precio / c.precioIni - 1) * 100; const ult = c.puntos[c.puntos.length - 1];
  if (ult && t - ult.t < 1) { ult.p = p; return; }            // como mucho un punto por segundo
  c.puntos.push({ t, p }); if (p > c.max.p) c.max = { t, p }; if (p < c.min.p) c.min = { t, p }; c.ultPrecio = precio; }

// ── PumpPortal: las migraciones ──
function abrePumpPortal() {
  const ws = new WebSocket(PP_WS); S.pp.ws = ws;
  ws.on("open", () => { S.pp.ok = true; ws.send(JSON.stringify({ method: "subscribeMigration" })); log(`🔌 PumpPortal: escuchando las migraciones${PUMPPORTAL_API_KEY ? " (con clave)" : " (sin clave)"}`); });
  ws.on("message", (raw) => { let m; try { m = JSON.parse(raw); } catch { return; } if (m && m.mint && (m.txType === "migrate" || m.txType === "migration" || m.signature)) migracion(m).catch(e => { S.errores++; log(`⚠️ migración ${corto(m.mint)}: ${e.message}`); }); });
  ws.on("close", () => { S.pp.ok = false; S.pp.reconexiones++; setTimeout(abrePumpPortal, Math.min(30000, 2000 * S.pp.reconexiones)); });
  ws.on("error", (e) => { S.errores++; log(`⚠️ PumpPortal: ${e.message}`); });
}

async function migracion(m) {
  const mint = m.mint; if (S.vistas.has(mint)) return; S.vistas.add(mint); if (S.vistas.size > 20000) S.vistas = new Set([...S.vistas].slice(-10000));
  S.migs++; S.ultimaMig = Date.now();
  const c = { mint, sym: m.symbol || m.name || corto(mint), firma: m.signature || null, t0: Date.now(), pool: null, precioIni: 0, ref: "migración", puntos: [], max: { t: 0, p: 0 }, min: { t: 0, p: 0 },
    swaps: 0, vistas: new Set(), ultReservas: null, ultSwap: 0, supply: null, mayhem: false, nac: null, nacTxt: null, fin: false, hel: null, min1: { swaps: 0, compradores: new Set(), vendedores: new Set(), solIn: 0, solOut: 0 } };
  S.camaras.set(mint, c);
  // 1) la piscina y el precio inicial, de la transacción de la migración
  if (c.firma) { try { const r = await leeMigracion(c.firma, mint); if (r) { c.pool = r.pool; c.precioIni = r.sol / r.tok; c.reservasIni = r; c.ultPrecio = c.precioIni; c.puntos.push({ t: 0, p: 0 }); } } catch (e) { log(`⚠️ ${c.sym}: no pude leer la migración (${e.message}); el precio inicial será el del 1er swap`); } }
  if (c.fin) return;
  // 2) Helius: a grabar
  c.hel = heliusConHueco(); suscribe(c.hel, c);
  c.timerFin = setTimeout(() => cierra(c, "fin de la cámara"), CAMARA_MIN * 60e3);
  log(`🐣 MIGRACIÓN ${c.sym} (${corto(mint)})${c.pool ? ` · piscina ${corto(c.pool)} · precio inicial ${c.precioIni.toExponential(3)} SOL` : " · piscina aún no (se verá en el 1er swap)"} · grabando ${CAMARA_MIN} min`);
  // 3) supply (¿Mayhem?) y el nacimiento, en paralelo
  rpc("getTokenSupply", [mint]).then(s => { const n = +s?.value?.uiAmount; if (n > 0) { c.supply = n; c.mayhem = n > 1.5e9; } }).catch(() => {});
  nacimiento(c).catch(e => { S.nacFallos++; log(`⚠️ nacimiento de ${c.sym}: ${e.message}`); });
}

// ── el nacimiento: lo que pasó en la curva de bonos ANTES de migrar (API de pump.fun) ──
async function pumpApi(ruta) { const r = await fetch(`https://frontend-api-v3.pump.fun${ruta}`, { headers: { accept: "application/json" }, signal: AbortSignal.timeout(8000) }); if (!r.ok) throw new Error(`pump.fun ${r.status}`); return r.json(); }
async function nacimiento(c) {
  const coin = await pumpApi(`/coins/${c.mint}`).catch(() => null);
  if (coin) { if (coin.symbol) c.sym = String(coin.symbol).slice(0, 14); c.creador = coin.creator || null; c.creado = coin.created_timestamp ? +coin.created_timestamp : null; c.redes = { tg: !!coin.telegram, tw: !!coin.twitter, web: !!coin.website };
    if (!c.supply && coin.total_supply) c.supply = +coin.total_supply / 1e6; }
  const trades = []; for (let off = 0; off < 4000; off += 200) { const L = await pumpApi(`/trades/all/${c.mint}?limit=200&offset=${off}&minimumSize=0`).catch(() => null); if (!Array.isArray(L) || !L.length) break; trades.push(...L); if (L.length < 200) break; }
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
  if (c.fin) return; c.fin = true; clearTimeout(c.timerFin); if (c.hel) desuscribe(c.hel, c); S.camaras.delete(c.mint);
  if (c.puntos.length < 2 || !c.precioIni) { log(`🗑️ ${c.sym}: sin curva (${c.swaps} swaps, ${c.puntos.length} puntos) · ${motivo}`); return; }
  S.curvas++;
  const mc = c.precioIni * (c.supply || 1e9) * (S.solUsd || 0); const pts = c.puntos.map(q => `${Math.round(q.t)}:${f2(q.p)}`).join(",");
  const m1 = c.min1; const conducta = `60:${m1.compradores.size}/${m1.vendedores.size}/${f2(m1.solIn)}/${f2(m1.solOut)}/${m1.swaps}`;
  const linea = `[SOLREC] sym=${c.sym} mint=${c.mint} pool=${c.pool || "-"} firma=${c.firma || "-"} MC=$${(mc / 1000).toFixed(1)}K supply=${Math.round((c.supply || 1e9) / 1e6)}M MIN=${f2(c.min.p)}%@${Math.round(c.min.t)}s MAX=${f2(c.max.p)}%@${Math.round(c.max.t)}s par=SOL precioIni=${c.precioIni.toExponential(4)} ref=${c.ref} swaps=${c.swaps} puntos=${c.puntos.length} mayhem=${c.mayhem ? 1 : 0} conducta=${conducta} ${c.nacTxt ? `nac=${c.nacTxt} ` : ""}pts=${pts}`;
  log(linea);
  if (!SOLO_LOG) log(`📼 ${c.sym}: ${c.puntos.length} puntos en ${Math.round(c.puntos[c.puntos.length - 1].t / 60)} min · máx ${f1(c.max.p)} % a los ${Math.round(c.max.t)} s · mín ${f1(c.min.p)} % · ${c.swaps} swaps · ${motivo}`);
  S.ultimas.unshift({ sym: c.sym, mint: c.mint, max: f1(c.max.p), min: f1(c.min.p), puntos: c.puntos.length, swaps: c.swaps, nac: c.nac, t: Date.now() }); S.ultimas = S.ultimas.slice(0, 50);
}

// ── el latido: un punto aunque no haya swaps, y cerrar a los muertos ──
setInterval(() => { const ahora = Date.now();
  for (const c of S.camaras.values()) { if (c.fin || !c.precioIni) continue; const t = (ahora - c.t0) / 1000; const ult = c.puntos[c.puntos.length - 1];
    if (!ult || t - ult.t >= LATIDO_S) punto(c, t, c.ultPrecio || c.precioIni);
    if (MUERTO_MIN > 0 && ult && ult.p <= -95 && c.ultSwap && ahora - c.ultSwap > MUERTO_MIN * 60e3) cierra(c, `muerto (${MUERTO_MIN} min a −95 % sin swaps)`); } }, LATIDO_S * 1000);

// ── la salud, cada 10 minutos ──
setInterval(() => { const h = ((Date.now() - S.arranque) / 3600e3).toFixed(1); const subs = S.hel.reduce((s, H) => s + H.subs.size, 0);
  log(`[SALUD] ${h}h · migraciones=${S.migs} (última hace ${S.ultimaMig ? Math.round((Date.now() - S.ultimaMig) / 60e3) + " min" : "—"}) · grabando=${S.camaras.size} · curvas=${S.curvas} · swaps=${S.swaps} · nacimientos=${S.nacHechos}/${S.nacHechos + S.nacFallos} · Helius ${S.hel.filter(H => H.ok).length}/${S.hel.length} conexiones, ${subs} suscripciones, ≈${(S.bytes / 1e6).toFixed(0)} MB · PumpPortal ${S.pp.ok ? "ok" : "CAÍDO"} · SOL $${S.solUsd.toFixed(0)} · errores ${S.errores}`); }, 10 * 60e3);

// ── un HTTP mínimo para ver que vive ──
http.createServer((req, res) => { if (req.url.startsWith("/ultimas")) { res.setHeader("content-type", "application/json"); res.end(JSON.stringify(S.ultimas.map(u => ({ ...u, nac: u.nac ? { seg: u.nac.seg, compradores: u.nac.compradores, top5: u.nac.top5 } : null })))); return; }
  res.setHeader("content-type", "application/json"); res.end(JSON.stringify({ horas: +((Date.now() - S.arranque) / 3600e3).toFixed(2), migraciones: S.migs, grabando: S.camaras.size, curvas: S.curvas, swaps: S.swaps, nacimientos: S.nacHechos, fallosNac: S.nacFallos, helius: S.hel.map(H => ({ ok: H.ok, subs: H.subs.size })), pumpportal: S.pp.ok, solUsd: S.solUsd, errores: S.errores, mb: +(S.bytes / 1e6).toFixed(1) })); }).listen(PORT, () => log(`🌐 escuchando en :${PORT}`));

// ── arranque ──
log(`🟣 GRABADOR DE SOLANA · solo graba (no opera) · cámara ${CAMARA_MIN} min · latido ${LATIDO_S} s · muerto a los ${MUERTO_MIN} min · hasta ${WS_MAX} conexiones de Helius con ${MAX_SUBS_WS} suscripciones cada una`);
await precioSol(); setInterval(precioSol, 5 * 60e3); log(`💱 SOL a $${S.solUsd.toFixed(2)}`);
heliusNueva(); abrePumpPortal();
process.on("SIGTERM", () => { for (const c of [...S.camaras.values()]) cierra(c, "apagado"); setTimeout(() => process.exit(0), 500); });
