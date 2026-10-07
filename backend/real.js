// ═══════════════════════════════════════════════════════════════════════════════════════════
//  LAS ÓRDENES REALES DE SOLANA (fase 3) · 5-oct-2026
//  El motor pide las órdenes con encola("compra"|"venta", rec), como en Pons. Aquí se ejecutan de verdad:
//   · [5-oct tarde] RUTAS (RUTA="directo,jupiter"): 1º DIRECTO a la piscina de PumpSwap con el SDK oficial de pump.fun
//     (@pump-fun/pump-swap-sdk: la orden se construye aquí, sin preguntar a nadie), enviada por HELIUS SENDER (SWQOS, propina
//     PROPINA_SOL 0,00001) con la prioridad que estima Helius en cada momento (PRIO_NIVEL, tope PRIO_MAX_SOL); 2º Jupiter (sin
//     comisión) por el RPC de Helius. PumpPortal (0,5 % por operación) solo si se pone en RUTA.
//   · COMPRA: LOTE_SOL de SOL → token.
//   · VENTA: TODO el saldo del token → SOL, con 4 intentos de margen creciente (10 → 20 → 35 → 50 %), y si no sale,
//     se reintenta sola más tarde (20 s, 1, 2, 5, 10 min); dos ventas atascadas a la vez → se desarma (las ventas siguen).
//   · CUADRE: al cerrar cada token, lo de papel, lo real estimado y lo REAL (SOL que salió y que volvió, con todas las comisiones).
//  PROTECCIONES: arranca SIEMPRE desarmado (se arma desde el panel, con PANEL_CLAVE); no compra si el saldo no deja RESERVA_SOL;
//   como mucho MAX_ABIERTAS tokens y MAX_EXPUESTO_SOL dentro; se desarma solo con FALLOS_SEGUIDOS compras fallidas seguidas,
//   con dos ventas atascadas o si la pérdida REAL del día llega a PERDIDA_DIA_SOL. Desarmado no compra, pero SÍ vende lo que tiene.
//  REAL_SIMULADO=1: no toca la cadena; simula las órdenes con el precio de la curva (para probar la cola y el cuadre).
// ═══════════════════════════════════════════════════════════════════════════════════════════
import fs from "fs";
const E = process.env;
const RPC = (E.SOLANA_RPC || "").trim() || `https://mainnet.helius-rpc.com/?api-key=${(E.HELIUS_API_KEY || "").trim()}`;
const JUP = (E.JUP_BASE || "https://lite-api.jup.ag/swap/v1").replace(/\/$/, "");
const WSOL = "So11111111111111111111111111111111111111112";
const LOTE = +(E.LOTE_SOL || 0.1);
const SLIP_ENTRADA = +(E.SLIP_ENTRADA || 15);
const TOL_VENTA = (E.TOL_VENTA || "10,20,35,50").split(",").map(Number).filter(x => x > 0);
const PRIO_ENTRADA = +(E.PRIO_ENTRADA || 0.0005), PRIO_SALIDA = +(E.PRIO_SALIDA || 0.0005), PRIO_PANICO = +(E.PRIO_PANICO || 0.001);
const MAX_ABIERTAS = +(E.MAX_ABIERTAS || 8), MAX_EXPUESTO = +(E.MAX_EXPUESTO_SOL || 1), PERDIDA_DIA = +(E.PERDIDA_DIA_SOL || 0.5);
const RESERVA = +(E.RESERVA_SOL || 0.05), FALLOS_SEGUIDOS = +(E.FALLOS_SEGUIDOS || 4);
const REINTENTOS_S = (E.REINTENTOS_VENTA || "20,60,120,300,600").split(",").map(Number).filter(x => x > 0);
const SIMULADO = E.REAL_SIMULADO === "1";
const SOLO_PUMPSWAP = E.SOLO_PUMPSWAP !== "0";   // [5-oct] en real, solo tokens cuya piscina es de PumpSwap (los demás, solo papel)
const avisadosNoPS = new Set();
const RUTAS = (E.RUTA || "directo,jupiter").split(",").map(x => x.trim().toLowerCase()).filter(Boolean);
const SENDER = (E.SENDER || "swqos").toLowerCase();   // swqos (barato) · max (propina ≥ 0,001) · no (RPC normal)
const SENDER_URL = SENDER === "max" ? "https://sender.helius-rpc.com/fast" : "https://sender.helius-rpc.com/fast?swqos_only=true";
const PROPINA = +(E.PROPINA_SOL || (SENDER === "max" ? 0.001 : 0.00001));
const PROPINA_CUENTAS = ["4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE", "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ", "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta", "5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn", "2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD", "2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ", "wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF", "3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT", "4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey", "4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or"];
const PRIO_NIVEL = E.PRIO_NIVEL || "Medium", PRIO_MAX = +(E.PRIO_MAX_SOL || 0.0005), CU = +(E.CU_LIMIT || 250000);
const SLIP_SDK = (E.SLIP_UNIDAD || "pct") === "frac" ? (x) => x / 100 : (x) => x;   // el SDK de pump.fun: el deslizamiento en % (si no, SLIP_UNIDAD=frac)
const PUMPSWAP = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
export const CLAVE = (E.PANEL_CLAVE || "").trim();
const FILE = E.REAL_FILE || (fs.existsSync("/data") ? "/data/solana_real.json" : "/tmp/solana_real.json");

export const R = { listo: false, armado: false, motivo: "arranca desarmado", pub: null, saldo: null, saldoT: 0,
  tokens: {},            // mint → { sym, solIn, solOut, compras, ventas, abierto, t0 }
  totales: { real: 0, compras: 0, ventas: 0, fallos: 0, comisionesPrio: 0, cuadres: 0, papel: 0, estimado: 0 },
  dia: { fecha: null, real: 0 }, cuadres: [], pendientes: {}, huerfanos: {}, ultima: null, cola: 0 };
let spl = null, pss = null, BN = null, sdkOnline = null;
const RUTA_STATS = {};   // ruta → { n, ms, fallos }
const marcaRuta = (ruta, ms, ok) => { const r = RUTA_STATS[ruta] = RUTA_STATS[ruta] || { n: 0, ms: 0, fallos: 0 }; if (ok) { r.n++; r.ms += ms; } else r.fallos++; };
let web3 = null, bs58 = null, wallet = null, conn = null, log = console.log, papelDe = () => null, cola = [], girando = false, fallosSeguidos = 0;
const fechaEsp = () => new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date());
const s4 = (x) => (x >= 0 ? "+" : "") + (+x).toFixed(4);
const guarda = () => { try { fs.writeFileSync(FILE, JSON.stringify({ tokens: R.tokens, totales: R.totales, dia: R.dia, cuadres: R.cuadres.slice(0, 200), huerfanos: R.huerfanos })); } catch {} };
setInterval(guarda, 60e3);

// ── el arranque: la cartera (si hay clave) y lo guardado ──
export async function init(opts = {}) {
  log = opts.log || log; papelDe = opts.papelDe || papelDe;
  try { const g = JSON.parse(fs.readFileSync(FILE, "utf-8")); Object.assign(R.tokens, g.tokens || {}); Object.assign(R.totales, g.totales || {}); R.dia = g.dia || R.dia; R.cuadres = g.cuadres || []; R.huerfanos = g.huerfanos || {}; } catch {}
  if (SIMULADO) { R.listo = true; R.pub = "SIMULADO"; R.saldo = 10; log(`🧪 ÓRDENES REALES EN MODO SIMULADO (REAL_SIMULADO=1): no se toca la cadena`); return; }
  const clave = (E.SOLANA_PRIVATE_KEY || E.WALLET_PRIVATE_KEY || "").trim();
  if (!clave) { log(`🔒 sin SOLANA_PRIVATE_KEY: el server solo hace papel (no puede armarse)`); return; }
  try { web3 = await import("@solana/web3.js"); bs58 = (await import("bs58")).default; try { spl = await import("@solana/spl-token"); } catch { log("ℹ️ sin @solana/spl-token: no se cerrarán las cuentas vacías (se pierde ~0,002 SOL de alquiler por token)"); }
    let bytes; try { bytes = bs58.decode(clave); } catch { bytes = Uint8Array.from(JSON.parse(clave)); }   // base58 o el array [12,34,…]
    wallet = web3.Keypair.fromSecretKey(bytes); conn = new web3.Connection(RPC, "confirmed"); R.pub = wallet.publicKey.toString(); R.listo = true;
    if (RUTAS.includes("directo")) { try { pss = await import("@pump-fun/pump-swap-sdk"); BN = (await import("bn.js")).default; sdkOnline = new pss.OnlinePumpAmmSdk(conn); log(`⚡ ruta directa a PumpSwap lista (SDK de pump.fun) · envío ${SENDER === "no" ? "por el RPC" : "por Helius Sender (" + SENDER + ", propina " + PROPINA + " SOL)"} · prioridad ${PRIO_NIVEL} (tope ${PRIO_MAX} SOL)`); }
      catch (e) { pss = null; log(`⚠️ sin el SDK de pump.fun (${e.message.slice(0, 80)}): se usará Jupiter`); } }
    await saldo(true); log(`🔑 cartera ${R.pub.slice(0, 4)}…${R.pub.slice(-4)} · saldo ${R.saldo?.toFixed(4)} SOL · lote ${LOTE} SOL · DESARMADO (se arma desde el panel)`);
    buscaHuerfanos().catch(() => {}); setInterval(() => buscaHuerfanos().catch(() => {}), 10 * 60e3);
  } catch (e) { log(`❌ la cartera no carga (${e.message}): el server solo hace papel`); }
}
async function saldo(forzar) { if (SIMULADO) return R.saldo; if (!conn) return null; if (!forzar && Date.now() - R.saldoT < 60e3 && R.saldo != null) return R.saldo;
  try { R.saldo = (await conn.getBalance(wallet.publicKey, "confirmed")) / 1e9; R.saldoT = Date.now(); } catch {} return R.saldo; }
async function saldoToken(mint) { if (SIMULADO) return R.tokens[mint]?.simTok || 0n;
  const r = await conn.getParsedTokenAccountsByOwner(wallet.publicKey, { mint: new web3.PublicKey(mint) }, "confirmed"); let s = 0n;
  for (const a of r.value || []) s += BigInt(a.account.data.parsed.info.tokenAmount.amount || "0"); return s; }

// ── la prioridad que estima Helius en cada momento (micro-lamports por unidad de cómputo), con tope ──
let prioCache = { t: 0, micro: 0 }, bhCache = { t: 0, bh: null };
async function prioridadMicro() { if (Date.now() - prioCache.t < 10000 && prioCache.micro) return prioCache.micro; let micro = 50000;
  try { const r = await fetch(RPC, { method: "POST", headers: { "content-type": "application/json" }, signal: AbortSignal.timeout(3000), body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getPriorityFeeEstimate", params: [{ accountKeys: [PUMPSWAP], options: { priorityLevel: PRIO_NIVEL } }] }) });
    const j = await r.json(); const v = +j?.result?.priorityFeeEstimate; if (v >= 0) micro = Math.round(v); } catch {}
  const tope = Math.floor(PRIO_MAX * 1e9 * 1e6 / CU), minimo = Math.ceil(5000 * 1e6 / CU);   // Sender pide al menos 5.000 lamports de prioridad
  prioCache = { t: Date.now(), micro: Math.max(minimo, Math.min(tope, micro)) }; return prioCache.micro; }
const prioLamports = (micro) => Math.round(micro * CU / 1e6);
async function blockhash() { if (Date.now() - bhCache.t < 15000 && bhCache.bh) return bhCache.bh; const { blockhash: bh } = await conn.getLatestBlockhash("confirmed"); bhCache = { t: Date.now(), bh }; return bh; }
// enviar por Helius Sender (y si no lo acepta, por el RPC normal)
async function envia(tx) { const crudo = Buffer.from(tx.serialize());
  if (SENDER !== "no") { try { const r = await fetch(SENDER_URL, { method: "POST", headers: { "content-type": "application/json" }, signal: AbortSignal.timeout(5000), body: JSON.stringify({ jsonrpc: "2.0", id: "1", method: "sendTransaction", params: [crudo.toString("base64"), { encoding: "base64", skipPreflight: true, maxRetries: 0 }] }) });
      const j = await r.json(); if (j.result) return j.result; log(`ℹ️ Sender no la aceptó (${JSON.stringify(j.error || j).slice(0, 100)}) → RPC normal`); } catch (e) { log(`ℹ️ Sender falló (${e.message.slice(0, 60)}) → RPC normal`); } }
  return conn.sendRawTransaction(crudo, { skipPreflight: true, maxRetries: 3 }); }
// la RUTA DIRECTA: la orden a la piscina de PumpSwap, construida aquí con el SDK oficial
async function directo(tipo, rec, cantidad, slipPct) { if (!pss || !rec.pool) throw new Error(!rec.pool ? "sin piscina" : "sin SDK");
  const st = await sdkOnline.swapSolanaState(new web3.PublicKey(rec.pool), wallet.publicKey);
  const ins = tipo === "compra" ? await pss.PUMP_AMM_SDK.buyQuoteInput(st, new BN(String(cantidad)), SLIP_SDK(slipPct)) : await pss.PUMP_AMM_SDK.sellBaseInput(st, new BN(String(cantidad)), SLIP_SDK(slipPct));
  const micro = await prioridadMicro(); const lista = [web3.ComputeBudgetProgram.setComputeUnitLimit({ units: CU }), web3.ComputeBudgetProgram.setComputeUnitPrice({ microLamports: micro }), ...ins];
  if (SENDER !== "no") lista.push(web3.SystemProgram.transfer({ fromPubkey: wallet.publicKey, toPubkey: new web3.PublicKey(PROPINA_CUENTAS[Math.floor(Math.random() * PROPINA_CUENTAS.length)]), lamports: Math.round(PROPINA * 1e9) }));
  const msg = new web3.TransactionMessage({ payerKey: wallet.publicKey, recentBlockhash: await blockhash(), instructions: lista }).compileToV0Message();
  const tx = new web3.VersionedTransaction(msg); tx.sign([wallet]);
  // las primeras órdenes directas de cada arranque se SIMULAN antes de enviarlas: si algo del SDK no cuadra, no se gasta nada y se usa Jupiter
  if (directasProbadas < 3) { const sim = await conn.simulateTransaction(tx, { sigVerify: false, replaceRecentBlockhash: true, commitment: "processed" });
    if (sim.value.err) { const ult = (sim.value.logs || []).slice(-3).join(" | "); throw new Error(`la simulación de la orden directa falla: ${JSON.stringify(sim.value.err).slice(0, 80)} · ${ult.slice(0, 160)}`); }
    directasProbadas++; log(`✅ orden directa simulada bien (${tipo} ${rec.symbol}: ${sim.value.unitsConsumed || "?"} unidades de cómputo de ${CU})`); }
  const sig = await envia(tx); await confirma(sig);
  R.totales.comisionesPrio += prioLamports(micro) / 1e9 + (SENDER !== "no" ? PROPINA : 0); return sig; }
let directasProbadas = 0;
// las rutas en orden: la primera que salga (y se apunta cuánto tarda cada una)
async function porRutas(tipo, rec, cantidadRaw, cantidadSol, slipPct, prioSolJup) { let ultimo = null;
  for (const ruta of RUTAS) { const t0 = Date.now();
    try { let sig;
      if (ruta === "directo") sig = await directo(tipo, rec, cantidadRaw, slipPct);
      else if (ruta === "jupiter") sig = tipo === "compra" ? await jupiter(WSOL, rec.token, cantidadRaw, slipPct, prioSolJup) : await jupiter(rec.token, WSOL, cantidadRaw, slipPct, prioSolJup);
      else if (ruta === "pumpportal") sig = tipo === "compra" ? await pumpportal("buy", rec.token, cantidadSol, true, slipPct, prioSolJup) : await pumpportal("sell", rec.token, cantidadRaw, false, slipPct, prioSolJup);
      else continue;
      marcaRuta(ruta, Date.now() - t0, true); return { sig, ruta, ms: Date.now() - t0 }; }
    catch (e) { marcaRuta(ruta, 0, false); ultimo = e; if (e.colgada) throw e; log(`↻ ${rec.symbol}: ${tipo} por ${ruta} no salió (${(e.message || "").slice(0, 90)})${RUTAS.indexOf(ruta) < RUTAS.length - 1 ? " → siguiente ruta" : ""}`); } }
  throw ultimo || new Error("ninguna ruta"); }

// ── las órdenes en la cadena (lo de tu bot viejo: Jupiter, y PumpPortal de respaldo) ──
async function confirma(sig, ms = 45000) { const t0 = Date.now();
  while (Date.now() - t0 < ms) { try { const st = (await conn.getSignatureStatuses([sig])).value[0]; if (st) { if (st.err) throw new Error("la transacción falló en la cadena: " + JSON.stringify(st.err).slice(0, 80)); if (st.confirmationStatus === "confirmed" || st.confirmationStatus === "finalized") return true; } } catch (e) { if (/falló en la cadena/.test(e.message)) throw e; }
    await new Promise(r => setTimeout(r, 1200)); }
  const e = new Error("sin confirmar en 45 s"); e.colgada = true; throw e; }
async function firmaYEnvia(bytes) { const tx = web3.VersionedTransaction.deserialize(bytes); tx.sign([wallet]);
  const sig = await conn.sendRawTransaction(tx.serialize(), { skipPreflight: true, maxRetries: 3 }); await confirma(sig); return sig; }
async function jupiter(entra, sale, cantidad, slipPct, prioSol) {
  const q = await fetch(`${JUP}/quote?inputMint=${entra}&outputMint=${sale}&amount=${cantidad}&slippageBps=${Math.round(slipPct * 100)}`, { signal: AbortSignal.timeout(6000) });
  if (!q.ok) throw new Error(`Jupiter cotización ${q.status}`); const quote = await q.json(); if (!quote || !quote.routePlan || !quote.routePlan.length) throw new Error("Jupiter: sin ruta");
  const s = await fetch(`${JUP}/swap`, { method: "POST", headers: { "Content-Type": "application/json" }, signal: AbortSignal.timeout(8000),
    body: JSON.stringify({ quoteResponse: quote, userPublicKey: R.pub, wrapAndUnwrapSol: true, dynamicComputeUnitLimit: true, prioritizationFeeLamports: Math.round(prioSol * 1e9) }) });
  if (!s.ok) throw new Error(`Jupiter swap ${s.status}`); const { swapTransaction } = await s.json(); return firmaYEnvia(Buffer.from(swapTransaction, "base64")); }
async function pumpportal(accion, mint, cantidad, enSol, slipPct, prioSol) {
  const r = await fetch("https://pumpportal.fun/api/trade-local", { method: "POST", headers: { "Content-Type": "application/json" }, signal: AbortSignal.timeout(10000),
    body: JSON.stringify({ publicKey: R.pub, action: accion, mint, denominatedInSol: enSol ? "true" : "false", amount: cantidad, slippage: slipPct, priorityFee: prioSol, pool: "auto" }) });
  if (!r.ok) throw new Error(`PumpPortal ${r.status}: ${(await r.text().catch(() => "")).slice(0, 120)}`); return firmaYEnvia(new Uint8Array(await r.arrayBuffer())); }
async function deltaSol(sig) { for (let i = 0; i < 6; i++) { try { const tx = await conn.getParsedTransaction(sig, { maxSupportedTransactionVersion: 0, commitment: "confirmed" });
      if (tx?.meta) { const keys = tx.transaction.message.accountKeys; const k = keys.findIndex(x => (x.pubkey ? x.pubkey.toString() : String(x)) === R.pub); if (k >= 0) return (tx.meta.postBalances[k] - tx.meta.preBalances[k]) / 1e9; } } catch {}
    await new Promise(r => setTimeout(r, 1500)); } return null; }
const precioDe = (rec) => { const P = rec.puntos || []; const q = P[P.length - 1]; return q ? 1 + q.p / 100 : 1; };   // (solo para el modo simulado)

async function compra(rec) { const t = R.tokens[rec.token] = R.tokens[rec.token] || { sym: rec.symbol, solIn: 0, solOut: 0, compras: 0, ventas: 0, abierto: false, t0: Date.now() };
  if (SIMULADO) { const pr = precioDe(rec) * 1.01; t.simTok = (t.simTok || 0n) + BigInt(Math.floor(LOTE * 0.99 / pr * 1e6)); t.solIn += LOTE + PRIO_ENTRADA; t.compras++; t.abierto = true; R.saldo -= LOTE + PRIO_ENTRADA; return { coste: LOTE + PRIO_ENTRADA, sig: "sim" }; }
  const micro = await prioridadMicro(); const r = await porRutas("compra", rec, Math.round(LOTE * 1e9), LOTE, SLIP_ENTRADA, Math.max(PRIO_ENTRADA === 0.0005 ? 0 : PRIO_ENTRADA, prioLamports(micro) / 1e9));
  const d = await deltaSol(r.sig); const coste = d != null ? -d : LOTE; t.solIn += coste; t.compras++; t.abierto = true; R.saldoT = 0; return { coste, sig: r.sig, ruta: r.ruta, ms: r.ms }; }
async function vende(rec, tol, prio) { const t = R.tokens[rec.token]; const bal = await saldoToken(rec.token); if (bal <= 0n) return { nada: true };
  if (SIMULADO) { const pr = precioDe(rec) * 0.99; const sol = Number(bal) / 1e6 * pr * 0.99 - prio; t.simTok = 0n; R.saldo += sol; return { recibido: sol, sig: "sim" }; }
  const micro = await prioridadMicro(); const r = await porRutas("venta", rec, bal.toString(), null, tol, Math.max(prioLamports(micro) / 1e9, prio === PRIO_PANICO ? PRIO_PANICO : 0));
  const d = await deltaSol(r.sig); R.saldoT = 0; return { recibido: d != null ? d : 0, sig: r.sig, ruta: r.ruta, ms: r.ms }; }

// al venderlo todo, se cierra la cuenta del token: así vuelve el alquiler que se pagó al comprar (~0,002 SOL)
async function cierraCuenta(mint) { if (SIMULADO || !spl || !conn) return 0;
  try { const r = await conn.getParsedTokenAccountsByOwner(wallet.publicKey, { mint: new web3.PublicKey(mint) }, "confirmed"); const ins = [];
    for (const a of r.value || []) { if (BigInt(a.account.data.parsed.info.tokenAmount.amount || "0") > 0n) continue; ins.push(spl.createCloseAccountInstruction(a.pubkey, wallet.publicKey, wallet.publicKey, [], a.account.owner)); }
    if (!ins.length) return 0; const { blockhash } = await conn.getLatestBlockhash("confirmed");
    const msg = new web3.TransactionMessage({ payerKey: wallet.publicKey, recentBlockhash: blockhash, instructions: [web3.ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 20000 }), ...ins] }).compileToV0Message();
    const tx = new web3.VersionedTransaction(msg); tx.sign([wallet]); const sig = await conn.sendRawTransaction(tx.serialize(), { skipPreflight: true, maxRetries: 3 }); await confirma(sig, 30000);
    const d = await deltaSol(sig); return d || 0; } catch (e) { log(`ℹ️ no pude cerrar la cuenta vacía de ${mint.slice(0, 6)}: ${e.message.slice(0, 80)}`); return 0; } }

// ── la cola: el motor pide, aquí se hace en orden ──
export function encola(tipo, rec, datos = {}) {
  if (!R.listo || !rec || !rec.token) return;
  if (tipo === "compra") { if (!R.armado) return; if (cola.length >= 20) return;
    if (SOLO_PUMPSWAP && rec.pumpswap === false) { if (!avisadosNoPS.has(rec.token)) { avisadosNoPS.add(rec.token); log(`⛔ ${rec.symbol}: su piscina no es de PumpSwap · en real no se opera (en papel sí)`); } return; } }
  else { const t = R.tokens[rec.token]; if (!(t && t.abierto) && !cola.some(x => x.tipo === "compra" && x.rec.token === rec.token)) return; if (cola.some(x => x.tipo === "venta" && x.rec.token === rec.token)) return; }
  cola.push({ tipo, rec, puesta: Date.now() }); R.cola = cola.length; gira();
}
async function gira() { if (girando) return; girando = true;
  try { while (cola.length) { const o = cola.shift(); R.cola = cola.length; const rec = o.rec;
      if (o.tipo === "compra") { if (!R.armado) continue;
        const abiertos = Object.values(R.tokens).filter(t => t.abierto); const dentro = abiertos.reduce((s, t) => s + Math.max(0, t.solIn - t.solOut), 0);
        if (!(R.tokens[rec.token]?.abierto) && abiertos.length >= MAX_ABIERTAS) { log(`⛔ ${rec.symbol}: no compro, ya hay ${abiertos.length} tokens dentro (máx ${MAX_ABIERTAS})`); continue; }
        if (dentro + LOTE > MAX_EXPUESTO) { log(`⛔ ${rec.symbol}: no compro, habría ${(dentro + LOTE).toFixed(2)} SOL dentro (máx ${MAX_EXPUESTO})`); continue; }
        const sal = await saldo(); if (sal != null && sal < LOTE + RESERVA) { log(`⛔ ${rec.symbol}: no compro, el saldo (${sal.toFixed(3)} SOL) no deja la reserva de ${RESERVA} SOL`); continue; }
        try { const r = await compra(rec); fallosSeguidos = 0; R.totales.compras++; R.ultima = `compra ${rec.symbol} ${r.coste.toFixed(4)} SOL`;
          log(`🟢 COMPRA REAL ${rec.symbol} · ${LOTE} SOL · coste real ${r.coste.toFixed(4)} SOL · ${r.ruta || "sim"}${r.ms != null ? " " + r.ms + " ms" : ""} · ${r.sig}`); }
        catch (e) { R.totales.fallos++; fallosSeguidos++; log(`❌ compra real de ${rec.symbol} no salió: ${(e.message || String(e)).slice(0, 140)}${e.colgada ? " (sin confirmar: se mirará el saldo al vender)" : ""}`);
          if (e.colgada) { const t = R.tokens[rec.token] = R.tokens[rec.token] || { sym: rec.symbol, solIn: 0, solOut: 0, compras: 0, ventas: 0, abierto: false, t0: Date.now() }; t.abierto = true; t.solIn += LOTE; }
          if (fallosSeguidos >= FALLOS_SEGUIDOS) desarma(`${fallosSeguidos} compras fallidas seguidas`); }
      } else {
        const urgente = /FRENO|MUERTO|NO_DESPEGA|STOP|BESTIA/.test(String((rec.est && rec.est.posPack && rec.est.posPack.motivo) || "")) ; let hecho = null, err = null;
        for (let i = 0; i < TOL_VENTA.length && !hecho; i++) { try { hecho = await vende(rec, TOL_VENTA[i], i >= 2 ? PRIO_PANICO : PRIO_SALIDA); } catch (e) { err = e; if (e.colgada) break; await new Promise(r => setTimeout(r, 800)); } }
        if (!hecho) { pendiente(rec, err); continue; }
        delete R.pendientes[rec.token]; const t = R.tokens[rec.token];
        if (hecho.nada) { if (t) t.abierto = false; cierraCuadre(rec, "no había tokens que vender"); continue; }
        t.solOut += hecho.recibido; t.ventas++; t.abierto = false; R.totales.ventas++; R.ultima = `venta ${rec.symbol} ${hecho.recibido.toFixed(4)} SOL`;
        const alquiler = await cierraCuenta(rec.token); t.solOut += alquiler;
        log(`🔴 VENTA REAL ${rec.symbol} · todo · recibido ${hecho.recibido.toFixed(4)} SOL${alquiler > 0 ? ` (+${alquiler.toFixed(4)} del alquiler de la cuenta)` : ""} · ${hecho.ruta || "sim"}${hecho.ms != null ? " " + hecho.ms + " ms" : ""} · ${hecho.sig}`); cierraCuadre(rec); } } }
  finally { girando = false; } }
function pendiente(rec, e) { R.totales.fallos++; const p = R.pendientes[rec.token] = R.pendientes[rec.token] || { sym: rec.symbol, ronda: 0 }; p.error = (e && e.message || String(e)).slice(0, 140);
  if (p.ronda >= REINTENTOS_S.length) { delete R.pendientes[rec.token]; R.huerfanos[rec.token] = { sym: rec.symbol, motivo: "la venta no sale: véndela desde el panel" }; log(`❌ ${rec.symbol}: la venta no sale ni reintentando · queda en huérfanos (véndela desde el panel) · ${p.error}`); return; }
  const s = REINTENTOS_S[p.ronda++]; p.proximo = Date.now() + s * 1000; p.rec = rec; log(`⏳ ${rec.symbol}: la venta no salió (${p.error}) · se reintenta en ${s < 60 ? s + " s" : Math.round(s / 60) + " min"}`);
  if (Object.keys(R.pendientes).length >= 2) desarma(`${Object.keys(R.pendientes).length} ventas atascadas a la vez`); }
setInterval(() => { const ahora = Date.now(); for (const [m, p] of Object.entries(R.pendientes)) if (p.proximo <= ahora && p.rec && !cola.some(x => x.tipo === "venta" && x.rec.token === m)) { p.proximo = Infinity; cola.push({ tipo: "venta", rec: p.rec, puesta: ahora }); } if (cola.length) gira(); }, 5000);

// ── el cuadre de cada token: papel (el motor) · real estimado (el cálculo) · REAL (lo que se movió de verdad) ──
function cierraCuadre(rec, nota) { const t = R.tokens[rec.token]; if (!t) return; const real = t.solOut - t.solIn; const p = papelDe(rec.token) || {};
  R.totales.real += real; R.totales.cuadres++; R.totales.papel += p.papel || 0; R.totales.estimado += p.real || 0;
  const hoy = fechaEsp(); if (R.dia.fecha !== hoy) R.dia = { fecha: hoy, real: 0 }; R.dia.real += real;
  const c = { t: Date.now(), sym: t.sym, mint: rec.token, compras: t.compras, ventas: t.ventas, solIn: +t.solIn.toFixed(5), solOut: +t.solOut.toFixed(5), real: +real.toFixed(5), papel: +(p.papel || 0).toFixed(5), estimado: +(p.real || 0).toFixed(5), nota: nota || null };
  R.cuadres.unshift(c); if (R.cuadres.length > 200) R.cuadres.length = 200; delete R.tokens[rec.token];
  log(`📊 CUADRE ${t.sym}: ${t.compras} compra${t.compras === 1 ? "" : "s"} · REAL ${s4(real)} SOL · papel ${s4(c.papel)} · real estimado ${s4(c.estimado)}${nota ? " · " + nota : ""} · acumulado REAL ${s4(R.totales.real)} SOL (hoy ${s4(R.dia.real)})`);
  if (R.armado && PERDIDA_DIA > 0 && R.dia.real <= -PERDIDA_DIA) desarma(`la pérdida REAL de hoy llega a ${R.dia.real.toFixed(3)} SOL (tope ${PERDIDA_DIA})`); guarda(); }

// ── armar, desarmar, huérfanos y vender a mano ──
export function arma() { if (!R.listo) return "no hay cartera: pon SOLANA_PRIVATE_KEY"; if (R.armado) return "ya estaba armado"; R.armado = true; R.motivo = null; fallosSeguidos = 0;
  log(`🟢 BOT ARMADO · lote ${LOTE} SOL · máx ${MAX_ABIERTAS} tokens y ${MAX_EXPUESTO} SOL dentro · pérdida máx del día ${PERDIDA_DIA} SOL · reserva ${RESERVA} SOL${SIMULADO ? " · (SIMULADO)" : ""}`); return "armado"; }
export function desarma(motivo) { if (!R.armado) return "ya estaba desarmado"; R.armado = false; R.motivo = motivo;
  const quedan = Object.values(R.tokens).filter(t => t.abierto).length; log(`🛑 BOT DESARMADO: ${motivo} · ya no compra${quedan ? ` · lo que tiene (${quedan} token${quedan > 1 ? "s" : ""}) lo sigue vendiendo cuando toque` : ""}`); return "desarmado"; }
async function buscaHuerfanos() { if (SIMULADO || !conn) return; const r = await conn.getParsedTokenAccountsByOwner(wallet.publicKey, { programId: new web3.PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA") }, "confirmed");
  for (const a of r.value || []) { const inf = a.account.data.parsed.info; const amt = BigInt(inf.tokenAmount.amount || "0"); if (amt <= 0n || inf.mint === WSOL) continue;
    if (!(R.tokens[inf.mint]?.abierto) && !R.huerfanos[inf.mint]) R.huerfanos[inf.mint] = { sym: inf.mint.slice(0, 6), motivo: "en la cartera sin operación abierta" }; } }
export async function vendeAMano(mint) { if (!R.listo) return "no hay cartera"; const rec = { token: mint, symbol: (R.huerfanos[mint] || R.tokens[mint] || {}).sym || mint.slice(0, 6), puntos: [] };
  try { let h = null, err = null; for (const tol of TOL_VENTA) { try { h = await vende(rec, tol, PRIO_PANICO); break; } catch (e) { err = e; } }
    if (!h) return "no salió: " + (err && err.message || "").slice(0, 100); delete R.huerfanos[mint];
    if (h.nada) return "no había tokens"; log(`🔴 VENTA A MANO ${rec.symbol} · recibido ${h.recibido.toFixed(4)} SOL · ${h.sig}`); return `vendido: ${h.recibido.toFixed(4)} SOL`; } catch (e) { return "error: " + e.message; } }
export function resumen() { return { listo: R.listo, armado: R.armado, motivo: R.motivo, cartera: R.pub, saldo: R.saldo, lote: LOTE, simulado: SIMULADO, dentro: Object.values(R.tokens).filter(t => t.abierto).length, cola: R.cola,
  totales: R.totales, hoy: R.dia, pendientes: Object.values(R.pendientes).map(p => ({ sym: p.sym, error: p.error, ronda: p.ronda })), huerfanos: R.huerfanos, cuadres: R.cuadres.slice(0, 40),
  rutas: Object.fromEntries(Object.entries(RUTA_STATS).map(([k, v]) => [k, { ordenes: v.n, msMedio: v.n ? Math.round(v.ms / v.n) : null, fallos: v.fallos }])), envio: { RUTAS, SENDER, PROPINA, PRIO_NIVEL, PRIO_MAX },
  topes: { MAX_ABIERTAS, MAX_EXPUESTO, PERDIDA_DIA, RESERVA, FALLOS_SEGUIDOS, PRIO_ENTRADA, PRIO_SALIDA, SLIP_ENTRADA, TOL_VENTA } }; }
