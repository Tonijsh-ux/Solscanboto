// ═══════════════════════════════════════════════════════════════════════════════════════════
//  LAS ÓRDENES REALES DE SOLANA (fase 3) · 5-oct-2026 · [18-oct] la «sombra real» que mide la latencia
//  [24-oct] + EL 2º LOTE DE VERDAD, REINVIRTIENDO (Tony, 23:01: «la de los 310 SOL pero con el lote reinvirtiendo»): el server (REAL_COPIA=lote2, de serie)
//   manda aquí la compra y la venta del 2º lote con datos.lote2; cada compra va con la CARTERA / REAL_LOTE2_DIV (10; con 5,5 SOL → 0,55), tope
//   REAL_LOTE2_MAX (9) y mínimo REAL_LOTE2_MIN (0,05); REAL_LOTE2_SOL > 0 = lote fijo. Cartera = el saldo + lo que hay dentro a precio de compra.
//   Los topes van en proporción: dentro como mucho REAL_EXPUESTO_FRAC (0,5) de la cartera (o MAX_EXPUESTO_SOL si es más) y la pérdida del día
//   como mucho PERDIDA_DIA_FRAC (0,25) de la cartera con que empezó el día (o PERDIDA_DIA_SOL si es más): si se llega, se desarma sola.
//   En el 3-23 oct con 5,5 SOL (NUEVO/fam2/sim_capital.py, sim_dia_pct.py): acaba en ~160-166 SOL; peor día −26 % con el tope del 25 %.
//  El motor pide las órdenes con encola("compra"|"venta", rec), como en Pons. Aquí se ejecutan de verdad:
//   · [5-oct tarde] RUTAS (RUTA="directo,jupiter"): 1º DIRECTO a la piscina de PumpSwap con el SDK oficial de pump.fun
//     (@pump-fun/pump-swap-sdk: la orden se construye aquí, sin preguntar a nadie), enviada por HELIUS SENDER (SWQOS, propina
//     PROPINA_SOL 0,00001) con la prioridad que estima Helius en cada momento (PRIO_NIVEL, tope PRIO_MAX_SOL); 2º Jupiter (sin
//     comisión) por el RPC de Helius. PumpPortal (0,5 % por operación) solo si se pone en RUTA.
//   · COMPRA: el lote REAL de SOL → token.
//   · VENTA: TODO el saldo del token → SOL, con 4 intentos de margen creciente (10 → 20 → 35 → 50 %), y si no sale,
//     se reintenta sola más tarde (20 s, 1, 2, 5, 10 min); dos ventas atascadas a la vez → se desarma (las ventas siguen).
//   · CUADRE: al cerrar cada token, lo de papel, lo real estimado y lo REAL (SOL que salió y que volvió, con todas las comisiones).
//  PROTECCIONES: arranca SIEMPRE desarmado (se arma desde el panel, con PANEL_CLAVE); no compra si el saldo no deja RESERVA_SOL;
//   como mucho MAX_ABIERTAS tokens y MAX_EXPUESTO_SOL dentro; se desarma solo con FALLOS_SEGUIDOS compras fallidas seguidas,
//   con dos ventas atascadas o si la pérdida REAL del día llega a PERDIDA_DIA_SOL. Desarmado no compra, pero SÍ vende lo que tiene.
//  REAL_SIMULADO=1: no toca la cadena; simula las órdenes con el precio de la curva (para probar la cola y el cuadre).
//  [18-oct] LA SOMBRA REAL (Tony: «¿cómo sabemos lo que tenemos de latencia de verdad?» → «prepáralo todo»):
//   · REAL_LOTE_SOL separa el lote REAL del de papel (LOTE_SOL): con REAL_LOTE_SOL=0.01 el papel sigue con 0,1 y cada compra y venta
//     del 1er lote se manda también de verdad con 0,01 SOL. Lo que importa no es lo que gana, sino lo que tarda y a qué precio llena.
//   · Una línea [LATENCIA] por orden (y por intento de venta fallido): t_dec (cuándo decidió el motor, ms de reloj) · t_tok (segundo del
//     token en ese momento) · slot_dec (el último slot que el server había visto de ese token) · espera (ms en la cola) · envio (ms desde
//     que empieza la orden hasta que la cadena la acepta: estado de la piscina + construir + firmar + enviar) · conf (ms hasta confirmada)
//     · total (desde la decisión) · slot_fill y bt (slot y hora de bloque del llenado; slots = slot_fill − slot_dec, ≈ 0,4 s cada uno)
//     · sol y tok (lo que se movió de verdad en la cartera: el precio de llenado se compara fuera con el pts= de [SOLREC]) · fee · prio
//     (micro-lamports) · p_dec (precio relativo del último punto al decidir) · intentos por ruta · err si falló o se colgó.
//   · La cola ya no es una sola: UNA POR TOKEN (una compra no espera a que se confirme la venta de otro token: eso era latencia nuestra).
//   · El CUADRE escala lo REAL al lote de papel (×LOTE_SOL/REAL_LOTE_SOL) para que las dos cuentas se puedan comparar.
//   · prepara(): el server la llama al ver cada migración; refresca la prioridad y el blockhash si están viejos (2 llamadas a Helius por
//     migración, no un sondeo continuo), para que la orden de los 4 s no gaste tiempo en eso.
//   · [19-oct] lo que quedó dentro de antes de un reinicio se vende al arrancar (a los REAL_VENDE_ARRANQUE_S, 40 s, tras releer el fichero por si el server
//     anterior lo vendió y guardó al apagarse; con la piscina guardada en la compra)
//     y ocupado() para que el server espere a las órdenes en marcha al apagarse.
//   · [18-oct, noche] SOLANA_PRIVATE_KEY en cualquier formato de cartera (base58 de 64 bytes o semilla de 32, array [..], hex) y SOLANA_DIRECCION
//     (la dirección PÚBLICA, opcional): si está y la clave no es de esa dirección, la cartera no se carga (la clave de Ethereum de MetaMask, otra cuenta…).
// ═══════════════════════════════════════════════════════════════════════════════════════════
import fs from "fs";
const E = process.env;
const RPC = (E.SOLANA_RPC || "").trim() || `https://mainnet.helius-rpc.com/?api-key=${(E.HELIUS_API_KEY || "").trim()}`;
const JUP = (E.JUP_BASE || "https://lite-api.jup.ag/swap/v1").replace(/\/$/, "");
const WSOL = "So11111111111111111111111111111111111111112";
const LOTE_PAPEL = +(E.LOTE_SOL || 0.1);                       // el lote del papel (el del motor)
const LOTE = +(E.REAL_LOTE_SOL || E.LOTE_SOL || 0.1);          // [18-oct] el lote REAL: REAL_LOTE_SOL si está (la «sombra», 0,01); si no, el del papel
const SLIP_ENTRADA = +(E.SLIP_ENTRADA || 15);
const TOL_VENTA = (E.TOL_VENTA || "10,20,35,50").split(",").map(Number).filter(x => x > 0);
const PRIO_ENTRADA = +(E.PRIO_ENTRADA || 0.0005), PRIO_SALIDA = +(E.PRIO_SALIDA || 0.0005), PRIO_PANICO = +(E.PRIO_PANICO || 0.001);
const MAX_ABIERTAS = +(E.MAX_ABIERTAS || 8), MAX_EXPUESTO = +(E.MAX_EXPUESTO_SOL || 1), PERDIDA_DIA = +(E.PERDIDA_DIA_SOL || 0.5);
const RESERVA = +(E.RESERVA_SOL || 0.05), FALLOS_SEGUIDOS = +(E.FALLOS_SEGUIDOS || 4);
// [24-oct] el 2º lote reinvirtiendo (ver arriba)
const numE = (k, def) => { const v = String(E[k] ?? "").trim().replace(",", "."); return v !== "" && Number.isFinite(+v) ? +v : def; };
const L2_DIV = Math.max(1, numE("REAL_LOTE2_DIV", 10)), L2_MAX = numE("REAL_LOTE2_MAX", 9), L2_MIN = numE("REAL_LOTE2_MIN", 0.05), L2_FIJO = numE("REAL_LOTE2_SOL", 0);
const EXP_FRAC = numE("REAL_EXPUESTO_FRAC", 0.5), PERDIDA_FRAC = numE("PERDIDA_DIA_FRAC", 0.25);
let MODO_L2 = false;   // el server dice al arrancar si la cartera copia el 2º lote (entonces los topes van en proporción a la cartera)
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
// [18-oct] la latencia medida (para /real y [SALUD]): las últimas 1.000 órdenes
const LAT = { n: 0, ok: 0, fallos: 0, colgadas: 0, espera: [], envio: [], conf: [], total: [], slots: [], visto: [] };
const apuntaLat = (k, v) => { if (v == null || !(v >= 0)) return; const a = LAT[k]; a.push(v); if (a.length > 1000) a.splice(0, a.length - 1000); };
const pct = (a, q) => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(q * s.length))]; };
// [24-oct] la cartera (saldo + lo de dentro a precio de compra), el lote del 2º lote ahora mismo y los topes en proporción
const dentroSol = () => Object.values(R.tokens).filter(t => t.abierto).reduce((s, t) => s + Math.max(0, t.solIn - t.solOut), 0);
const capital = () => (R.saldo || 0) + dentroSol();
const lote2Ahora = () => L2_FIJO > 0 ? L2_FIJO : Math.min(L2_MAX, capital() / L2_DIV);
const loteDe = (d) => (d && d.lote2) ? lote2Ahora() : LOTE;
const maxExpuesto = (es2) => es2 ? Math.max(MAX_EXPUESTO, EXP_FRAC * capital()) : MAX_EXPUESTO;
const perdidaDia = () => Math.max(PERDIDA_DIA, MODO_L2 && PERDIDA_FRAC > 0 && R.dia && R.dia.base > 0 ? PERDIDA_FRAC * R.dia.base : 0);
const nuevoDia = (hoy) => { R.dia = { fecha: hoy, real: 0, base: +capital().toFixed(4) }; };
const lote2Txt = () => L2_FIJO > 0 ? `con lote fijo de ${L2_FIJO} SOL` : `reinvirtiendo: la cartera (${capital().toFixed(3)} SOL) / ${L2_DIV} = ${lote2Ahora().toFixed(4)} SOL por compra (tope ${L2_MAX})`;
let web3 = null, bs58 = null, wallet = null, conn = null, log = console.log, papelDe = () => null, papel2De = () => null, fallosSeguidos = 0;
const fechaEsp = () => new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date());
const s4 = (x) => (x >= 0 ? "+" : "") + (+x).toFixed(4);
const espera = (ms) => new Promise(r => setTimeout(r, ms));
const guarda = () => { try { const tmp = FILE + ".tmp"; fs.writeFileSync(tmp, JSON.stringify({ tokens: R.tokens, totales: R.totales, dia: R.dia, cuadres: R.cuadres.slice(0, 200), huerfanos: R.huerfanos }, (k, v) => typeof v === "bigint" ? v.toString() : v)); fs.renameSync(tmp, FILE); } catch {} };   // [12-oct] primero a .tmp y luego se renombra: con el disco lleno no se queda a medias
setInterval(guarda, 60e3);

// ── [18-oct, noche] la clave privada en cualquiera de los formatos de las carteras (Tony: «¿cómo saco la clave de Solana de MetaMask?»):
//   base58 (Phantom, Solflare, MetaMask: los 64 bytes; o la semilla de 32) · el array [12,34,…] (solana-keygen: 64; o 32) · hex (64 o 128 caracteres, con o sin 0x).
//   Se quitan espacios, saltos de línea y comillas que se cuelan al copiar. Los errores NUNCA repiten nada de la clave (solo el formato y el tamaño).
function leeClave(txt) {
  if (/^\s*(?:[a-z]+\s+){11,23}[a-z]+\s*$/.test(String(txt))) throw new Error("SOLANA_PRIVATE_KEY parece la FRASE DE RECUPERACIÓN (12-24 palabras): no la pongas nunca en Railway; bórrala de ahí y pon la clave privada de la cuenta");
  const s = String(txt).replace(/\s+/g, "").replace(/^["'`]+|["'`]+$/g, ""); const hex = s.replace(/^0x/i, ""); let bytes = null, formato = null;
  if (/^[0-9a-fA-F]+$/.test(hex) && (hex.length === 64 || hex.length === 128)) { bytes = Uint8Array.from(Buffer.from(hex, "hex")); formato = "hex"; }
  else if (s.startsWith("[")) { try { const a = JSON.parse(s); if (Array.isArray(a) && a.every(x => Number.isInteger(x) && x >= 0 && x <= 255)) { bytes = Uint8Array.from(a); formato = "array"; } } catch {} }
  else { try { bytes = bs58.decode(s); formato = "base58"; } catch {} }
  if (!bytes) throw new Error(`SOLANA_PRIVATE_KEY no tiene un formato que reconozca (${s.length} caracteres): tiene que ser base58 como la de Phantom/MetaMask, el array [12,34,…] o hex`);
  if (bytes.length === 64) { let kp; try { kp = web3.Keypair.fromSecretKey(bytes); } catch { throw new Error(`SOLANA_PRIVATE_KEY (${formato}, 64 bytes) no es una clave de Solana válida: sus dos mitades no casan`); } return { kp, formato: `${formato}, 64 bytes` }; }
  if (bytes.length === 32) return { kp: web3.Keypair.fromSeed(bytes), formato: `${formato}, semilla de 32 bytes` };
  throw new Error(`SOLANA_PRIVATE_KEY (${formato}) tiene ${bytes.length} bytes: una clave de Solana tiene 64 (o 32 si es la semilla)`); }

// ── el arranque: la cartera (si hay clave) y lo guardado ──
export async function init(opts = {}) {
  log = opts.log || log; papelDe = opts.papelDe || papelDe; papel2De = opts.papel2De || papel2De; MODO_L2 = !!opts.lote2;
  try { const g = JSON.parse(fs.readFileSync(FILE, "utf-8")); Object.assign(R.tokens, g.tokens || {}); Object.assign(R.totales, g.totales || {}); R.dia = g.dia || R.dia; R.cuadres = g.cuadres || []; R.huerfanos = g.huerfanos || {}; } catch {}
  CARGADO_MS = Date.now();
  for (const t of Object.values(R.tokens)) { if (t.simTok != null) t.simTok = BigInt(t.simTok); t.comprando = false; }   // (lo guardado en simulado vuelve como texto)
  const ARRANQUE_S = +(E.REAL_VENDE_ARRANQUE_S || 40);   // [19-oct] 40 s: el server anterior sigue vivo unos segundos (SIGTERM ~5 s después de arrancar este) y espera hasta 15 s a sus ventas
  if (SIMULADO) { R.listo = true; R.pub = "SIMULADO"; R.saldo = 10; log(`🧪 ÓRDENES REALES EN MODO SIMULADO (REAL_SIMULADO=1): no se toca la cadena · lote REAL ${LOTE} SOL (papel ${LOTE_PAPEL})`); setTimeout(vendeLoDeAntes, ARRANQUE_S * 1000); return; }
  const clave = (E.SOLANA_PRIVATE_KEY || E.WALLET_PRIVATE_KEY || "").trim();
  if (!clave) { log(`🔒 sin SOLANA_PRIVATE_KEY: el server solo hace papel (no puede armarse)`); return; }
  try { web3 = await import("@solana/web3.js"); bs58 = (await import("bs58")).default; try { spl = await import("@solana/spl-token"); } catch { log("ℹ️ sin @solana/spl-token: no se cerrarán las cuentas vacías (se pierde ~0,002 SOL de alquiler por token)"); }
    const { kp, formato } = leeClave(clave); const pub = kp.publicKey.toString(); const corta = (d) => `${d.slice(0, 4)}…${d.slice(-4)}`;
    // [18-oct, noche] SOLANA_DIRECCION (la dirección PÚBLICA, opcional): si está y la clave no es de esa dirección, no se carga (p. ej. si se copió la clave de Ethereum de MetaMask)
    const DIR = (E.SOLANA_DIRECCION || "").trim();
    if (DIR && pub !== DIR) throw new Error(`la clave no es de la dirección de SOLANA_DIRECCION (${corta(DIR)}): sale ${corta(pub)} · ¿se copió la clave de Ethereum o la de otra cuenta?`);
    wallet = kp; conn = new web3.Connection(RPC, "confirmed"); R.pub = pub; R.listo = true;
    log(`🔑 clave leída (${formato}) · dirección ${corta(pub)}${DIR ? " · coincide con SOLANA_DIRECCION ✅" : " · comprueba que es tu dirección de Solana (si no, se copió otra clave)"}`);
    if (RUTAS.includes("directo")) { try { pss = await import("@pump-fun/pump-swap-sdk"); BN = (await import("bn.js")).default; sdkOnline = new pss.OnlinePumpAmmSdk(conn); log(`⚡ ruta directa a PumpSwap lista (SDK de pump.fun) · envío ${SENDER === "no" ? "por el RPC" : "por Helius Sender (" + SENDER + ", propina " + PROPINA + " SOL)"} · prioridad ${PRIO_NIVEL} (tope ${PRIO_MAX} SOL)`); }
      catch (e) { pss = null; log(`⚠️ sin el SDK de pump.fun (${e.message.slice(0, 80)}): se usará Jupiter`); } }
    await saldo(true); log(`🔑 cartera ${R.pub.slice(0, 4)}…${R.pub.slice(-4)} · saldo ${R.saldo != null ? R.saldo.toFixed(4) : "? (no pude leerlo)"} SOL · ${MODO_L2 ? `copia el 2º lote ${lote2Txt()}` : `lote REAL ${LOTE} SOL${LOTE !== LOTE_PAPEL ? ` (SOMBRA del papel, que va con ${LOTE_PAPEL})` : ""}`} · DESARMADO (se arma desde el panel)`);
    setTimeout(vendeLoDeAntes, ARRANQUE_S * 1000); buscaHuerfanos().catch(() => {}); setInterval(() => buscaHuerfanos().catch(() => {}), 10 * 60e3);
  } catch (e) { log(`❌ la cartera no carga (${e.message}): el server solo hace papel`); }
}
async function saldo(forzar) { if (SIMULADO) return R.saldo; if (!conn) return null; if (!forzar && Date.now() - R.saldoT < 60e3 && R.saldo != null) return R.saldo;
  try { R.saldo = (await conn.getBalance(wallet.publicKey, "confirmed")) / 1e9; R.saldoT = Date.now(); } catch {} return R.saldo; }
// [19-oct] el saldo del panel y de [SALUD] era el leído ANTES de la última compra (solo se leía para comprar): tras vender, el panel decía 5,057 con la
//   cartera en 5,126 (Tony). Ahora se vuelve a leer 1 s después de cada compra y de cada venta (1 llamada getBalance, sin esperar a nada)
function refrescaSaldo() { if (SIMULADO || !conn) return; setTimeout(() => { saldo(true).catch(() => {}); }, 1000); }
// [19-oct] y antes de comprar ya NO se espera a leerlo (MEM, 08:31Z: un getBalance lento retrasó una compra 10 s y las dos de detrás 3-4 s en la cola,
//   y llenaron un 18-26 % más caro): se usa el último leído y, si tiene más de 60 s, se pide otro sin esperar. Solo se espera si aún no hay ninguno.
function saldoYa() { if (SIMULADO || R.saldo == null) return saldo(); if (Date.now() - R.saldoT > 60e3) saldo(true).catch(() => {}); return Promise.resolve(R.saldo); }
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
// [18-oct] el server la llama al ver una migración: la prioridad y el blockhash ya calientes para la orden de los 4 s (si está armado y hay cartera)
export function prepara() { if (SIMULADO || !conn || !R.armado) return; prioridadMicro().catch(() => {}); blockhash().catch(() => {}); }
// el contexto de una orden: cuándo la aceptó la cadena, cuándo se confirmó, la prioridad y los intentos por ruta
const nuevoCtx = (precioAhora) => ({ t0: Date.now(), tEnv: null, tVisto: null, pVisto: null, tConf: null, micro: null, intentos: [], precioAhora });   // [19-oct] tVisto/pVisto: cuándo la vio la cadena y el precio en ese momento
// enviar por Helius Sender (y si no lo acepta, por el RPC normal)
async function envia(tx, ctx) { const crudo = Buffer.from(tx.serialize());
  if (SENDER !== "no") { try { const r = await fetch(SENDER_URL, { method: "POST", headers: { "content-type": "application/json" }, signal: AbortSignal.timeout(5000), body: JSON.stringify({ jsonrpc: "2.0", id: "1", method: "sendTransaction", params: [crudo.toString("base64"), { encoding: "base64", skipPreflight: true, maxRetries: 0 }] }) });
      const j = await r.json(); if (j.result) { if (ctx) ctx.tEnv = Date.now(); return j.result; } log(`ℹ️ Sender no la aceptó (${JSON.stringify(j.error || j).slice(0, 100)}) → RPC normal`); } catch (e) { log(`ℹ️ Sender falló (${e.message.slice(0, 60)}) → RPC normal`); } }
  const sig = await conn.sendRawTransaction(crudo, { skipPreflight: true, maxRetries: 3 }); if (ctx) ctx.tEnv = Date.now(); return sig; }
// la RUTA DIRECTA: la orden a la piscina de PumpSwap, construida aquí con el SDK oficial
async function directo(tipo, rec, cantidad, slipPct, ctx) { if (!pss || !rec.pool) throw new Error(!rec.pool ? "sin piscina" : "sin SDK");
  const st = await sdkOnline.swapSolanaState(new web3.PublicKey(rec.pool), wallet.publicKey);
  const ins = tipo === "compra" ? await pss.PUMP_AMM_SDK.buyQuoteInput(st, new BN(String(cantidad)), SLIP_SDK(slipPct)) : await pss.PUMP_AMM_SDK.sellBaseInput(st, new BN(String(cantidad)), SLIP_SDK(slipPct));
  const micro = await prioridadMicro(); if (ctx) ctx.micro = micro; const lista = [web3.ComputeBudgetProgram.setComputeUnitLimit({ units: CU }), web3.ComputeBudgetProgram.setComputeUnitPrice({ microLamports: micro }), ...ins];
  if (SENDER !== "no") lista.push(web3.SystemProgram.transfer({ fromPubkey: wallet.publicKey, toPubkey: new web3.PublicKey(PROPINA_CUENTAS[Math.floor(Math.random() * PROPINA_CUENTAS.length)]), lamports: Math.round(PROPINA * 1e9) }));
  const msg = new web3.TransactionMessage({ payerKey: wallet.publicKey, recentBlockhash: await blockhash(), instructions: lista }).compileToV0Message();
  const tx = new web3.VersionedTransaction(msg); tx.sign([wallet]);
  // las primeras órdenes directas de cada arranque se SIMULAN antes de enviarlas: si algo del SDK no cuadra, no se gasta nada y se usa Jupiter
  if (directasProbadas < 3) { const sim = await conn.simulateTransaction(tx, { sigVerify: false, replaceRecentBlockhash: true, commitment: "processed" });
    if (sim.value.err) { const ult = (sim.value.logs || []).slice(-3).join(" | "); throw new Error(`la simulación de la orden directa falla: ${JSON.stringify(sim.value.err).slice(0, 80)} · ${ult.slice(0, 160)}`); }
    directasProbadas++; log(`✅ orden directa simulada bien (${tipo} ${rec.symbol}: ${sim.value.unitsConsumed || "?"} unidades de cómputo de ${CU})`); }
  const sig = await envia(tx, ctx); await confirma(sig, 45000, ctx);
  R.totales.comisionesPrio += prioLamports(micro) / 1e9 + (SENDER !== "no" ? PROPINA : 0); return sig; }
let directasProbadas = 0;
// las rutas en orden: la primera que salga (y se apunta cuánto tarda cada una)
async function porRutas(tipo, rec, cantidadRaw, cantidadSol, slipPct, prioSolJup, ctx) { let ultimo = null; ctx = ctx || nuevoCtx();
  for (const ruta of RUTAS) { const t0 = Date.now(); ctx.tEnv = null; ctx.tConf = null;
    try { let sig;
      if (ruta === "directo") sig = await directo(tipo, rec, cantidadRaw, slipPct, ctx);
      else if (ruta === "jupiter") sig = tipo === "compra" ? await jupiter(WSOL, rec.token, cantidadRaw, slipPct, prioSolJup, ctx) : await jupiter(rec.token, WSOL, cantidadRaw, slipPct, prioSolJup, ctx);
      else if (ruta === "pumpportal") sig = tipo === "compra" ? await pumpportal("buy", rec.token, cantidadSol, true, slipPct, prioSolJup, ctx) : await pumpportal("sell", rec.token, cantidadRaw, false, slipPct, prioSolJup, ctx);
      else continue;
      const ms = Date.now() - t0; marcaRuta(ruta, ms, true); ctx.intentos.push({ ruta, ms, ok: true });
      return { sig, ruta, ms, envioMs: ctx.tEnv ? ctx.tEnv - t0 : null, vistoMs: ctx.tEnv && ctx.tVisto ? ctx.tVisto - ctx.tEnv : null, confMs: ctx.tEnv && ctx.tConf ? ctx.tConf - ctx.tEnv : null, ctx }; }
    catch (e) { marcaRuta(ruta, 0, false); ultimo = e; ctx.intentos.push({ ruta, ms: Date.now() - t0, ok: false, err: (e.message || "").slice(0, 60) }); e.ctx = ctx; if (e.colgada) throw e; log(`↻ ${rec.symbol}: ${tipo} por ${ruta} no salió (${(e.message || "").slice(0, 90)})${RUTAS.indexOf(ruta) < RUTAS.length - 1 ? " → siguiente ruta" : ""}`); } }
  const e = ultimo || new Error("ninguna ruta"); e.ctx = ctx; throw e; }

// ── las órdenes en la cadena (lo de tu bot viejo: Jupiter, y PumpPortal de respaldo) ──
async function confirma(sig, ms = 45000, ctx) { const t0 = Date.now();   // [19-oct] mira cada 250 ms los primeros 4 s y luego cada 1,2 s (antes siempre 1,2 s: «conf» medía sobre todo esa espera)
  while (Date.now() - t0 < ms) { try { const st = (await conn.getSignatureStatuses([sig])).value[0];
      if (st) { if (st.err) throw new Error("la transacción falló en la cadena: " + JSON.stringify(st.err).slice(0, 80));
        if (ctx && ctx.tVisto == null) { ctx.tVisto = Date.now(); try { ctx.pVisto = ctx.precioAhora ? ctx.precioAhora() : null; } catch {} }   // la primera vez que aparece (processed o más)
        if (st.confirmationStatus === "confirmed" || st.confirmationStatus === "finalized") { if (ctx) ctx.tConf = Date.now(); return true; } } } catch (e) { if (/falló en la cadena/.test(e.message)) throw e; }
    await espera(Date.now() - t0 < 4000 ? 250 : 1200); }
  const e = new Error(`sin confirmar en ${Math.round(ms / 1000)} s`); e.colgada = true; throw e; }
async function firmaYEnvia(bytes, ctx) { const tx = web3.VersionedTransaction.deserialize(bytes); tx.sign([wallet]);
  const sig = await conn.sendRawTransaction(tx.serialize(), { skipPreflight: true, maxRetries: 3 }); if (ctx) ctx.tEnv = Date.now(); await confirma(sig, 45000, ctx); return sig; }
async function jupiter(entra, sale, cantidad, slipPct, prioSol, ctx) {
  const q = await fetch(`${JUP}/quote?inputMint=${entra}&outputMint=${sale}&amount=${cantidad}&slippageBps=${Math.round(slipPct * 100)}`, { signal: AbortSignal.timeout(6000) });
  if (!q.ok) throw new Error(`Jupiter cotización ${q.status}`); const quote = await q.json(); if (!quote || !quote.routePlan || !quote.routePlan.length) throw new Error("Jupiter: sin ruta");
  const s = await fetch(`${JUP}/swap`, { method: "POST", headers: { "Content-Type": "application/json" }, signal: AbortSignal.timeout(8000),
    body: JSON.stringify({ quoteResponse: quote, userPublicKey: R.pub, wrapAndUnwrapSol: true, dynamicComputeUnitLimit: true, prioritizationFeeLamports: Math.round(prioSol * 1e9) }) });
  if (!s.ok) throw new Error(`Jupiter swap ${s.status}`); const { swapTransaction } = await s.json(); return firmaYEnvia(Buffer.from(swapTransaction, "base64"), ctx); }
async function pumpportal(accion, mint, cantidad, enSol, slipPct, prioSol, ctx) {
  const r = await fetch("https://pumpportal.fun/api/trade-local", { method: "POST", headers: { "Content-Type": "application/json" }, signal: AbortSignal.timeout(10000),
    body: JSON.stringify({ publicKey: R.pub, action: accion, mint, denominatedInSol: enSol ? "true" : "false", amount: cantidad, slippage: slipPct, priorityFee: prioSol, pool: "auto" }) });
  if (!r.ok) throw new Error(`PumpPortal ${r.status}: ${(await r.text().catch(() => "")).slice(0, 120)}`); return firmaYEnvia(new Uint8Array(await r.arrayBuffer()), ctx); }
// lo que se movió de verdad en la transacción: SOL y tokens de la cartera, el slot, la hora de bloque y la comisión base
async function detalleTx(sig, mint) { for (let i = 0; i < 6; i++) { try { const tx = await conn.getParsedTransaction(sig, { maxSupportedTransactionVersion: 0, commitment: "confirmed" });
      if (tx?.meta) { const keys = tx.transaction.message.accountKeys; const k = keys.findIndex(x => (x.pubkey ? x.pubkey.toString() : String(x)) === R.pub);
        const dSol = k >= 0 ? (tx.meta.postBalances[k] - tx.meta.preBalances[k]) / 1e9 : null;
        const tok = (L) => { let s = 0n; for (const b of L || []) if (b.mint === mint && b.owner === R.pub) s += BigInt(b.uiTokenAmount?.amount || "0"); return s; };
        const dTok = mint ? tok(tx.meta.postTokenBalances) - tok(tx.meta.preTokenBalances) : null;
        return { dSol, dTok, slot: tx.slot ?? null, bt: tx.blockTime ?? null, fee: tx.meta.fee != null ? tx.meta.fee / 1e9 : null }; } } catch {}
    await espera(1500); } return null; }
const precioDe = (rec) => { const P = rec.puntos || []; const q = P[P.length - 1]; return q ? 1 + q.p / 100 : 1; };   // el precio relativo del último punto (1 = el de migrar)
const ultimoT = (rec) => { const P = rec.puntos || []; return P.length ? P[P.length - 1].t : null; };

async function compra(rec, d = {}) { const L = d.lote || LOTE;   // [24-oct] el lote de ESTA orden (el 2º lote, reinvirtiendo; el 1er lote, LOTE)
  const t = R.tokens[rec.token] = R.tokens[rec.token] || { sym: rec.symbol, solIn: 0, solOut: 0, compras: 0, ventas: 0, abierto: false, t0: Date.now() }; const ctx = nuevoCtx(d.precioAhora); if (rec.pool) t.pool = rec.pool; t.lote = L; t.lote2 = !!d.lote2; t.comprando = true;   // [24-oct] «comprando»: una venta que llegue mientras tanto espera en la cola del token
  if (SIMULADO) { const pr = precioDe(rec) * 1.01; const tok = BigInt(Math.floor(L * 0.99 / pr * 1e6)); t.simTok = (t.simTok || 0n) + tok; t.solIn += L + PRIO_ENTRADA; t.compras++; t.abierto = true; R.saldo -= L + PRIO_ENTRADA;
    ctx.intentos.push({ ruta: "sim", ms: 0, ok: true }); return { coste: L + PRIO_ENTRADA, sig: "sim", ruta: "sim", ms: 0, envioMs: 0, vistoMs: 0, confMs: 0, det: { dSol: -(L + PRIO_ENTRADA), dTok: tok, slot: null, bt: null, fee: null }, ctx }; }
  const micro = await prioridadMicro(); const r = await porRutas("compra", rec, Math.round(L * 1e9), L, SLIP_ENTRADA, Math.max(PRIO_ENTRADA === 0.0005 ? 0 : PRIO_ENTRADA, prioLamports(micro) / 1e9), ctx);
  const det = await detalleTx(r.sig, rec.token); const coste = det && det.dSol != null ? -det.dSol : L; t.solIn += coste; t.compras++; t.abierto = true; return { ...r, coste, det }; }
async function vende(rec, tol, prio, ctx) { const t = R.tokens[rec.token]; const bal = await saldoToken(rec.token); if (bal <= 0n) return { nada: true }; ctx = ctx || nuevoCtx();
  if (SIMULADO) { const pr = precioDe(rec) * 0.99; const sol = Number(bal) / 1e6 * pr * 0.99 - prio; t.simTok = 0n; R.saldo += sol; ctx.intentos.push({ ruta: "sim", ms: 0, ok: true });
    return { recibido: sol, sig: "sim", ruta: "sim", ms: 0, envioMs: 0, vistoMs: 0, confMs: 0, tol, det: { dSol: sol, dTok: -bal, slot: null, bt: null, fee: null }, ctx }; }
  const micro = await prioridadMicro(); const r = await porRutas("venta", rec, bal.toString(), null, tol, Math.max(prioLamports(micro) / 1e9, prio === PRIO_PANICO ? PRIO_PANICO : 0), ctx);
  const det = await detalleTx(r.sig, rec.token); return { ...r, recibido: det && det.dSol != null ? det.dSol : 0, tol, det }; }

// al venderlo todo, se cierra la cuenta del token: así vuelve el alquiler que se pagó al comprar (~0,002 SOL)
async function cierraCuenta(mint) { if (SIMULADO || !spl || !conn) return 0;
  try { const r = await conn.getParsedTokenAccountsByOwner(wallet.publicKey, { mint: new web3.PublicKey(mint) }, "confirmed"); const ins = [];
    for (const a of r.value || []) { if (BigInt(a.account.data.parsed.info.tokenAmount.amount || "0") > 0n) continue; ins.push(spl.createCloseAccountInstruction(a.pubkey, wallet.publicKey, wallet.publicKey, [], a.account.owner)); }
    if (!ins.length) return 0; const { blockhash } = await conn.getLatestBlockhash("confirmed");
    const msg = new web3.TransactionMessage({ payerKey: wallet.publicKey, recentBlockhash: blockhash, instructions: [web3.ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 20000 }), ...ins] }).compileToV0Message();
    const tx = new web3.VersionedTransaction(msg); tx.sign([wallet]); const sig = await conn.sendRawTransaction(tx.serialize(), { skipPreflight: true, maxRetries: 3 }); await confirma(sig, 30000);
    const d = await detalleTx(sig, null); return (d && d.dSol) || 0; } catch (e) { log(`ℹ️ no pude cerrar la cuenta vacía de ${mint.slice(0, 6)}: ${e.message.slice(0, 80)}`); return 0; } }

// ── [18-oct] la línea [LATENCIA]: una por orden (y por intento de venta que falla) ──
function latencia(o, tipo, ok, r, extra = {}) {
  const rec = o.rec, d = o.datos || {}; const ahora = Date.now(); const det = (r && r.det) || {}; const ctx = (r && r.ctx) || extra.ctx || {};
  const esp = (o.inicio || ahora) - o.puesta, total = ahora - o.puesta; const slots = det.slot != null && d.slot != null ? det.slot - d.slot : null;
  const slotsRed = det.slot != null && d.slotAhora != null ? det.slot - d.slotAhora : null;   // [19-oct] slots desde el de la red al decidir (más justo que desde el último visto del token)
  const vistoTotal = ctx.tVisto ? ctx.tVisto - o.puesta : null; let pAhora = null; try { pAhora = d.precioAhora ? d.precioAhora() : null; } catch {} if (pAhora == null) pAhora = precioDe(rec);
  LAT.n++; if (ok) { LAT.ok++; apuntaLat("espera", esp); apuntaLat("envio", r && r.envioMs); apuntaLat("conf", r && r.confMs); apuntaLat("total", total); apuntaLat("slots", slotsRed ?? slots); apuntaLat("visto", vistoTotal); } else if (extra.colgada) LAT.colgadas++; else LAT.fallos++;
  const f = (x, n = 0) => x == null || Number.isNaN(+x) ? "-" : (+x).toFixed(n);
  const intentos = (ctx.intentos || []).map(i => `${i.ruta}:${i.ok ? "ok" : "fallo"}/${i.ms}ms`).join("|") || "-";
  log(`[LATENCIA] tipo=${tipo} sym=${rec.symbol} mint=${rec.token} ok=${ok ? 1 : 0}${extra.colgada ? " colgada=1" : ""} ruta=${(r && r.ruta) || "-"} lote=${d.lote ?? (R.tokens[rec.token] || {}).lote ?? LOTE} cual=${d.lote2 || (R.tokens[rec.token] || {}).lote2 ? 2 : 1} t_dec=${o.puesta} t_tok=${f(d.tTok, 1)} slot_dec=${d.slot ?? "-"} espera=${esp} envio=${f(r && r.envioMs)} visto=${f(r && r.vistoMs)} conf=${f(r && r.confMs)} total=${total} decide_a_visto=${f(vistoTotal)} slot_fill=${det.slot ?? "-"} slots=${slots ?? "-"} slot_red=${d.slotAhora ?? "-"} slots_red=${slotsRed ?? "-"} bt=${det.bt ?? "-"} sol=${det.dSol != null ? (+det.dSol).toFixed(6) : "-"} tok=${det.dTok != null ? String(det.dTok) : "-"} fee=${det.fee != null ? (+det.fee).toFixed(6) : "-"} prio=${ctx.micro ?? "-"} p_dec=${f(d.pDec, 4)} p_visto=${ctx.pVisto != null ? (+ctx.pVisto).toFixed(4) : "-"} p_ahora=${pAhora != null ? (+pAhora).toFixed(4) : "-"} precioIni=${d.precioIni != null ? (+d.precioIni).toExponential(4) : "-"} intentos=${intentos}${extra.tol != null ? ` tol=${extra.tol}` : ""}${extra.intento != null ? ` intento=${extra.intento}` : ""}${d.reintento != null ? ` reintento=${d.reintento}` : ""}${extra.err ? ` err=${String(extra.err).replace(/\s+/g, "_").slice(0, 120)}` : ""}`); }

// ── la cola: el motor pide, aquí se hace en orden · [18-oct] UNA COLA POR TOKEN (dentro de un token, en orden; entre tokens, a la vez) ──
const COLAS = new Map();   // token → { items: [], girando: false }
const nCola = () => { let n = 0; for (const q of COLAS.values()) n += q.items.length; return n; };
const colaDe = (token) => { let q = COLAS.get(token); if (!q) { q = { items: [], girando: false }; COLAS.set(token, q); } return q; };
export function encola(tipo, rec, datos = {}) {
  if (!R.listo || !rec || !rec.token) return; const q0 = COLAS.get(rec.token);
  if (tipo === "compra") { if (!R.armado) return; if (nCola() >= 20) return;
    if (SOLO_PUMPSWAP && rec.pumpswap === false) { if (!avisadosNoPS.has(rec.token)) { avisadosNoPS.add(rec.token); log(`⛔ ${rec.symbol}: su piscina no es de PumpSwap · en real no se opera (en papel sí)`); } return; } }
  else { const t = R.tokens[rec.token]; if (!(t && (t.abierto || t.comprando)) && !(q0 && (q0.enCompra || q0.items.some(x => x.tipo === "compra")))) return; if (q0 && q0.items.some(x => x.tipo === "venta")) return; }
  const it = { tipo, rec, datos: { ...datos, pDec: datos.pDec ?? precioDe(rec), tTok: datos.tTok ?? ultimoT(rec) }, puesta: Date.now() };
  if (!SIMULADO && conn) conn.getSlot("processed").then(sl => { it.datos.slotAhora = sl; }).catch(() => {});   // [19-oct] el slot de la red al decidir (no el último visto del token)
  const q = colaDe(rec.token); q.items.push(it); R.cola = nCola(); gira(rec.token);
}
async function gira(token) { const q = COLAS.get(token); if (!q || q.girando) return; q.girando = true;
  try { while (q.items.length) { const o = q.items.shift(); R.cola = nCola(); const rec = o.rec; o.inicio = Date.now();
      if (o.tipo === "compra") { q.enCompra = true; try { if (!R.armado) continue;   // [24-oct] enCompra: una venta que llegue mientras se compra espera en la cola
        const es2 = !!(o.datos && o.datos.lote2); const sal = await saldoYa(); const L = loteDe(o.datos); o.datos.lote = L;   // [24-oct] el lote de esta compra (el 2º lote: la cartera / REAL_LOTE2_DIV)
        if (es2 && !(L >= L2_MIN)) { log(`⛔ ${rec.symbol}: no compro el 2º lote, sale de ${L.toFixed(4)} SOL (mínimo ${L2_MIN}; cartera ${capital().toFixed(3)} SOL)`); continue; }
        const abiertos = Object.values(R.tokens).filter(t => t.abierto); const dentro = abiertos.reduce((s, t) => s + Math.max(0, t.solIn - t.solOut), 0);
        if (!(R.tokens[rec.token]?.abierto) && abiertos.length >= MAX_ABIERTAS) { log(`⛔ ${rec.symbol}: no compro, ya hay ${abiertos.length} tokens dentro (máx ${MAX_ABIERTAS})`); continue; }
        const maxE = maxExpuesto(es2); if (dentro + L > maxE) { log(`⛔ ${rec.symbol}: no compro, habría ${(dentro + L).toFixed(2)} SOL dentro (máx ${maxE.toFixed(2)})`); continue; }
        if (sal != null && sal < L + RESERVA) { log(`⛔ ${rec.symbol}: no compro, el saldo (${sal.toFixed(3)} SOL) no deja la reserva de ${RESERVA} SOL con un lote de ${L.toFixed(4)}`); continue; }
        try { const r = await compra(rec, o.datos); fallosSeguidos = 0; R.totales.compras++; R.ultima = `compra ${rec.symbol} ${r.coste.toFixed(4)} SOL`;
          log(`🟢 COMPRA REAL ${rec.symbol}${es2 ? " (2º lote)" : ""} · ${+L.toFixed(4)} SOL${es2 && !(L2_FIJO > 0) ? ` (la cartera / ${L2_DIV})` : ""} · coste real ${r.coste.toFixed(4)} SOL · ${r.ruta || "sim"}${r.ms != null ? " " + r.ms + " ms" : ""} · ${r.sig}`); latencia(o, "compra", true, r); refrescaSaldo(); }
        catch (e) { R.totales.fallos++; fallosSeguidos++; log(`❌ compra real de ${rec.symbol} no salió: ${(e.message || String(e)).slice(0, 140)}${e.colgada ? " (sin confirmar: se mirará el saldo al vender)" : ""}`); latencia(o, "compra", false, null, { err: e.message || String(e), colgada: !!e.colgada, ctx: e.ctx });
          if (e.colgada) { const t = R.tokens[rec.token] = R.tokens[rec.token] || { sym: rec.symbol, solIn: 0, solOut: 0, compras: 0, ventas: 0, abierto: false, t0: Date.now() }; t.abierto = true; t.solIn += L; t.lote = L; t.lote2 = es2; }
          if (fallosSeguidos >= FALLOS_SEGUIDOS) desarma(`${fallosSeguidos} compras fallidas seguidas`); }
        finally { const tt = R.tokens[rec.token]; if (tt) { tt.comprando = false; if (!tt.abierto && !(tt.solIn > 0)) delete R.tokens[rec.token]; } }   // la compra que no salió no deja rastro
      } finally { q.enCompra = false; } } else {
        let hecho = null, err = null, i = 0;
        for (i = 0; i < TOL_VENTA.length && !hecho; i++) { const ctx = nuevoCtx(o.datos && o.datos.precioAhora); try { hecho = await vende(rec, TOL_VENTA[i], i >= 2 ? PRIO_PANICO : PRIO_SALIDA, ctx); } catch (e) { err = e; latencia(o, "venta", false, null, { err: e.message || String(e), colgada: !!e.colgada, ctx: e.ctx || ctx, tol: TOL_VENTA[i], intento: i + 1 }); if (e.colgada) break; await espera(800); } }
        if (!hecho) { pendiente(rec, err); continue; }
        delete R.pendientes[rec.token]; const t = R.tokens[rec.token];
        if (hecho.nada) { if (t) t.abierto = false; cierraCuadre(rec, "no había tokens que vender"); continue; }
        if (!t) { log(`🔴 VENTA REAL ${rec.symbol} sin compra apuntada · recibido ${hecho.recibido.toFixed(4)} SOL · ${hecho.sig}`); refrescaSaldo(); continue; }   // [24-oct] no se cae la cola
        t.solOut += hecho.recibido; t.ventas++; t.abierto = false; R.totales.ventas++; R.ultima = `venta ${rec.symbol} ${hecho.recibido.toFixed(4)} SOL`;
        latencia(o, "venta", true, hecho, { tol: hecho.tol, intento: i });   // [19-oct] antes de cerrar la cuenta (si no, el «total» de la venta llevaba también esa otra transacción)
        const alquiler = await cierraCuenta(rec.token); t.solOut += alquiler;
        log(`🔴 VENTA REAL ${rec.symbol} · todo · recibido ${hecho.recibido.toFixed(4)} SOL${alquiler > 0 ? ` (+${alquiler.toFixed(4)} del alquiler de la cuenta)` : ""} · ${hecho.ruta || "sim"}${hecho.ms != null ? " " + hecho.ms + " ms" : ""} · ${hecho.sig}`); cierraCuadre(rec); refrescaSaldo(); } } }
  finally { q.girando = false; if (!q.items.length) COLAS.delete(token); else gira(token); } }
function pendiente(rec, e) { R.totales.fallos++; const p = R.pendientes[rec.token] = R.pendientes[rec.token] || { sym: rec.symbol, ronda: 0 }; p.error = (e && e.message || String(e)).slice(0, 140);
  if (p.ronda >= REINTENTOS_S.length) { delete R.pendientes[rec.token]; R.huerfanos[rec.token] = { sym: rec.symbol, motivo: "la venta no sale: véndela desde el panel" }; log(`❌ ${rec.symbol}: la venta no sale ni reintentando · queda en huérfanos (véndela desde el panel) · ${p.error}`); return; }
  const s = REINTENTOS_S[p.ronda++]; p.proximo = Date.now() + s * 1000; p.rec = rec; log(`⏳ ${rec.symbol}: la venta no salió (${p.error}) · se reintenta en ${s < 60 ? s + " s" : Math.round(s / 60) + " min"}`);
  if (Object.keys(R.pendientes).length >= 2) desarma(`${Object.keys(R.pendientes).length} ventas atascadas a la vez`); }
setInterval(() => { const ahora = Date.now();
  for (const [m, p] of Object.entries(R.pendientes)) { if (!(p.proximo <= ahora) || !p.rec) continue; const q = colaDe(m); if (q.items.some(x => x.tipo === "venta")) continue; p.proximo = Infinity; q.items.push({ tipo: "venta", rec: p.rec, datos: { pDec: precioDe(p.rec), tTok: ultimoT(p.rec), reintento: p.ronda }, puesta: ahora }); R.cola = nCola(); gira(m); }
  for (const [m, q] of COLAS) if (q.items.length && !q.girando) gira(m); }, 5000);

// ── el cuadre de cada token: papel (el motor) · real estimado (el cálculo) · REAL (lo que se movió de verdad) ──
function cierraCuadre(rec, nota) { const t = R.tokens[rec.token]; if (!t) return; const real = t.solOut - t.solIn;
  // [24-oct] el 2º lote: el papel es el del 2º lote (a LOTE2_SOL) y el «estimado», el modelo AL MISMO LOTE que el real (para ver si lo real sigue al modelo)
  const es2 = !!t.lote2, lote = t.lote || LOTE; const p = (es2 ? papel2De(rec.token, lote) : papelDe(rec.token)) || {};
  const lotePapel = es2 ? (p.lotePapel || 0) : LOTE_PAPEL; const esc = lote > 0 && lotePapel > 0 ? lotePapel / lote : 1;
  const papel = p.papel || 0, estimado = (es2 ? p.estimado : p.real) || 0;
  R.totales.real += real; R.totales.cuadres++; R.totales.papel += papel; R.totales.estimado += estimado;
  if (es2) { const T2 = R.totales.lote2 || (R.totales.lote2 = { n: 0, gan: 0, real: 0, modelo: 0, metido: 0 }); T2.n++; if (real > 0) T2.gan++; T2.real += real; T2.modelo += estimado; T2.metido += lote; }
  const hoy = fechaEsp(); if (R.dia.fecha !== hoy) nuevoDia(hoy); R.dia.real += real;
  const c = { t: Date.now(), sym: t.sym, mint: rec.token, cual: es2 ? 2 : 1, compras: t.compras, ventas: t.ventas, solIn: +t.solIn.toFixed(5), solOut: +t.solOut.toFixed(5), real: +real.toFixed(5), realEscalado: +(real * esc).toFixed(5), lote: +lote.toFixed(4), lotePapel, papel: +(+papel).toFixed(5), estimado: +(+estimado).toFixed(5), motivo: es2 ? (p.motivo || null) : null, nota: nota || null };
  R.cuadres.unshift(c); if (R.cuadres.length > 200) R.cuadres.length = 200; delete R.tokens[rec.token];
  log(`📊 CUADRE ${t.sym}${es2 ? " (2º lote)" : ""}: ${t.compras} compra${t.compras === 1 ? "" : "s"} · REAL ${s4(real)} SOL (lote ${+lote.toFixed(4)}${esc !== 1 ? `; a lote de papel ${lotePapel}: ${s4(real * esc)}` : ""}) · papel ${s4(c.papel)} · ${es2 ? `el modelo al mismo lote ${s4(c.estimado)}${p.motivo ? " (sale por " + p.motivo + ")" : ""}` : `real estimado ${s4(c.estimado)}`}${nota ? " · " + nota : ""} · acumulado REAL ${s4(R.totales.real)} SOL (hoy ${s4(R.dia.real)})${es2 ? ` · cartera ≈ ${capital().toFixed(3)} SOL` : ""}`);
  const tope = perdidaDia(); if (R.armado && tope > 0 && R.dia.real <= -tope) desarma(`la pérdida REAL de hoy llega a ${R.dia.real.toFixed(3)} SOL (tope ${tope.toFixed(3)}${MODO_L2 && PERDIDA_FRAC > 0 ? ` = el ${Math.round(100 * PERDIDA_FRAC)} % de la cartera con que empezó el día` : ""})`); guarda(); }

// ── armar, desarmar, huérfanos y vender a mano ──
export function arma() { if (!R.listo) return "no hay cartera: pon SOLANA_PRIVATE_KEY"; if (R.armado) return "ya estaba armado"; R.armado = true; R.motivo = null; fallosSeguidos = 0; prepara();
  const hoy = fechaEsp(); if (R.dia.fecha !== hoy || !(R.dia.base > 0)) { const real = R.dia.fecha === hoy ? R.dia.real : 0; nuevoDia(hoy); R.dia.real = real; }   // [24-oct] la cartera con que empieza el día (para el tope en proporción)
  if (MODO_L2) log(`🟢 BOT ARMADO · copia el 2º lote ${lote2Txt()} · máx ${MAX_ABIERTAS} tokens · dentro como mucho ${maxExpuesto(true).toFixed(2)} SOL (el ${Math.round(100 * EXP_FRAC)} % de la cartera) · pérdida máx del día ${perdidaDia().toFixed(3)} SOL (el ${Math.round(100 * PERDIDA_FRAC)} % de ${(+R.dia.base || 0).toFixed(3)}) · reserva ${RESERVA} SOL${SIMULADO ? " · (SIMULADO)" : ""}`);
  else log(`🟢 BOT ARMADO · lote REAL ${LOTE} SOL${LOTE !== LOTE_PAPEL ? ` (SOMBRA: el papel va con ${LOTE_PAPEL}; lo real mide la latencia)` : ""} · máx ${MAX_ABIERTAS} tokens y ${MAX_EXPUESTO} SOL dentro · pérdida máx del día ${PERDIDA_DIA} SOL · reserva ${RESERVA} SOL${SIMULADO ? " · (SIMULADO)" : ""}`); guarda(); return "armado"; }
export function desarma(motivo) { if (!R.armado) return "ya estaba desarmado"; R.armado = false; R.motivo = motivo;
  const quedan = Object.values(R.tokens).filter(t => t.abierto).length; log(`🛑 BOT DESARMADO: ${motivo} · ya no compra${quedan ? ` · lo que tiene (${quedan} token${quedan > 1 ? "s" : ""}) lo sigue vendiendo cuando toque` : ""}`); return "desarmado"; }
async function buscaHuerfanos() { if (SIMULADO || !conn) return; const r = await conn.getParsedTokenAccountsByOwner(wallet.publicKey, { programId: new web3.PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA") }, "confirmed");
  for (const a of r.value || []) { const inf = a.account.data.parsed.info; const amt = BigInt(inf.tokenAmount.amount || "0"); if (amt <= 0n || inf.mint === WSOL) continue;
    if (!(R.tokens[inf.mint]?.abierto) && !R.huerfanos[inf.mint]) R.huerfanos[inf.mint] = { sym: inf.mint.slice(0, 6), motivo: "en la cartera sin operación abierta" }; } }
export async function vendeAMano(mint) { if (!R.listo) return "no hay cartera"; const rec = { token: mint, symbol: (R.huerfanos[mint] || R.tokens[mint] || {}).sym || mint.slice(0, 6), pool: (R.tokens[mint] || {}).pool, puntos: [] };
  try { let h = null, err = null; for (const tol of TOL_VENTA) { try { h = await vende(rec, tol, PRIO_PANICO); break; } catch (e) { err = e; } }
    if (!h) return "no salió: " + (err && err.message || "").slice(0, 100); delete R.huerfanos[mint];
    if (h.nada) return "no había tokens"; log(`🔴 VENTA A MANO ${rec.symbol} · recibido ${h.recibido.toFixed(4)} SOL · ${h.sig}`); return `vendido: ${h.recibido.toFixed(4)} SOL`; } catch (e) { return "error: " + e.message; } }
// [19-oct] ¿hay órdenes en marcha? (el server espera a que acaben al apagarse, hasta 15 s)
export function ocupado() { for (const q of COLAS.values()) if (q.girando || q.items.length) return true; return false; }
// [19-oct] lo que quedó DENTRO de antes de un reinicio: el motor que tenía que venderlo ya no existe, así que se vende al arrancar (si no, se queda atascado)
let CARGADO_MS = 0;
// [19-oct] el server anterior sigue vivo unos segundos después de arrancar este y al apagarse vende y GUARDA: si el fichero cambió después de leerlo, se vuelve a leer
//   (este aún no ha operado: arranca desarmado y sin motor con posiciones), para no vender otra vez lo ya vendido ni apuntar un cuadre a medias
function recargaSiCambio() { try { const st = fs.statSync(FILE); if (!(st.mtimeMs > CARGADO_MS)) return; const g = JSON.parse(fs.readFileSync(FILE, "utf-8"));
    R.tokens = g.tokens || {}; for (const t of Object.values(R.tokens)) if (t.simTok != null) t.simTok = BigInt(t.simTok); Object.assign(R.totales, g.totales || {}); R.dia = g.dia || R.dia; R.cuadres = g.cuadres || []; R.huerfanos = g.huerfanos || {};
    CARGADO_MS = Date.now(); log(`ℹ️ sombra real: el server anterior guardó al apagarse → estado recargado (${Object.values(R.tokens).filter(t => t.abierto).length} dentro · ${R.totales.cuadres} cuadres)`); } catch {} }
function vendeLoDeAntes() { recargaSiCambio(); const ab = Object.entries(R.tokens).filter(([, t]) => t.abierto); if (!ab.length || !R.listo) return;
  log(`🧹 ${ab.length} token${ab.length > 1 ? "s" : ""} dentro de antes del reinicio: se vende${ab.length > 1 ? "n" : ""} ya (${ab.map(([, t]) => t.sym).join(", ")})`);
  for (const [mint, t] of ab) encola("venta", { token: mint, symbol: t.sym, pool: t.pool, pumpswap: true, puntos: [] }, { reintento: "arranque" }); }
export function latenciaResumen() { return { ordenes: LAT.n, ok: LAT.ok, fallos: LAT.fallos, colgadas: LAT.colgadas, esperaMed: pct(LAT.espera, .5), envioMed: pct(LAT.envio, .5), envioP90: pct(LAT.envio, .9), confMed: pct(LAT.conf, .5), confP90: pct(LAT.conf, .9), totalMed: pct(LAT.total, .5), totalP90: pct(LAT.total, .9), vistoMed: pct(LAT.visto, .5), vistoP90: pct(LAT.visto, .9), slotsMed: pct(LAT.slots, .5), slotsP90: pct(LAT.slots, .9) }; }
export function resumen() { return { listo: R.listo, armado: R.armado, motivo: R.motivo, cartera: R.pub, saldo: R.saldo, lote: LOTE, lotePapel: LOTE_PAPEL, sombra: LOTE !== LOTE_PAPEL, simulado: SIMULADO, dentro: Object.values(R.tokens).filter(t => t.abierto).length, cola: R.cola,
  lote2: { modo: MODO_L2, div: L2_DIV, max: L2_MAX, min: L2_MIN, fijo: L2_FIJO, ahora: +lote2Ahora().toFixed(4), capital: +capital().toFixed(4), dentroSol: +dentroSol().toFixed(4), totales: R.totales.lote2 || null },   // [24-oct]
  totales: R.totales, hoy: R.dia, pendientes: Object.values(R.pendientes).map(p => ({ sym: p.sym, error: p.error, ronda: p.ronda })), huerfanos: R.huerfanos, cuadres: R.cuadres.slice(0, 40), latencia: latenciaResumen(),
  rutas: Object.fromEntries(Object.entries(RUTA_STATS).map(([k, v]) => [k, { ordenes: v.n, msMedio: v.n ? Math.round(v.ms / v.n) : null, fallos: v.fallos }])), envio: { RUTAS, SENDER, PROPINA, PRIO_NIVEL, PRIO_MAX },
  topes: { MAX_ABIERTAS, MAX_EXPUESTO, PERDIDA_DIA, RESERVA, FALLOS_SEGUIDOS, PRIO_ENTRADA, PRIO_SALIDA, SLIP_ENTRADA, TOL_VENTA, EXP_FRAC, PERDIDA_FRAC, expuestoAhora: +maxExpuesto(MODO_L2).toFixed(4), perdidaDiaAhora: +perdidaDia().toFixed(4) } }; }
