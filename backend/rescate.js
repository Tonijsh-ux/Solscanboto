// ═══════════════════════════════════════════════════════════════════════════════════════════
//  EL RESCATE, DENTRO DEL SERVER · el que manda · 12-oct-2026
//  1. EL REGISTRO EN EL DISCO: cada línea que escribe el server (las [SOLREC], [SOLPOS], [SOLVER], [SOLFICHA], [SALUD] y todo lo demás)
//     se guarda también en el volumen, un fichero por día (registro/AAAA-MM-DD.log, y comprimido .log.gz al acabar el día), en el mismo
//     formato que los registros que se bajan de Railway ({"timestamp","message"} por línea). Así no se pierde nada aunque cambie el
//     despliegue (cada despliegue de Railway tiene sus propios registros: el hueco del 10-11 oct fue eso). Las claves salen tapadas (***).
//  2. EL RESCATE DE CADA TOKEN: al cerrar su cámara, el token entra en una cola; un trabajador en otro hilo (rescate_trabajo.js) baja de
//     Helius su curva de bonos, los swaps de su piscina, la migración exacta y los extras, y deja los ficheros y una línea [SOLFICHA]
//     con sus rasgos. En este orden (lo más útil por crédito primero):
//       1º la curva de bonos y los extras de TODOS (≈140 créditos por token) · y la piscina entera de los que se cortaron al reiniciar (rotos/)
//       2º la piscina entera (toda la cámara) de los que el bot operó (≈600 por token)
//       3º la piscina de los primeros RESCATE_PISCINA_MIN minutos del resto, si RESCATE_PISCINA=todos (≈200 por token)
//     Lo que no cabe hoy espera a mañana (y lo que se quedó a medias sigue donde lo dejó, sin pagar dos veces); lo que lleva más de
//     RESCATE_COLA_DIAS días, a pendientes.
//  EL PRESUPUESTO: Helius cobra también el WebSocket (2 créditos por cada 0,1 MB), y el server en vivo ya gasta ~6-7 M al mes. El rescate usa solo
//  lo que sobra para que TODO el server se quede por debajo de HELIUS_CREDITOS_MES (9,5 M de serie, con un plan de 10 M): cada día, como mucho
//  HELIUS_CREDITOS_MES/30,4 − lo que gasta el server en vivo (medido), y nunca más de RESCATE_CREDITOS_DIA.
//  3. LAS RUTAS para bajarlo todo al PC: /rescate (cómo va), /rescate/indice?desde=ms (qué ficheros hay), /rescate/f/<carpeta>/<fichero>,
//     /rescate/pendientes. Piden la clave del panel (PANEL_CLAVE; o RESCATE_CLAVE si la pones; RESCATE_CLAVE=no las deja abiertas).
//  EL DISCO: si se llena, se borra primero lo que ya se bajó al PC (lo más viejo antes), y lo que no se ha bajado solo si no queda otra.
//  Variables (todas opcionales): RESCATE (1) · HELIUS_CREDITOS_MES (9500000) · RESCATE_CREDITOS_DIA (400000) · RESCATE_CURVA (todos|motor|operados|no; todos)
//   · RESCATE_PISCINA (operados|motor|todos|no; operados) · RESCATE_PISCINA_MIN (10: minutos de piscina de los no operados) · RESCATE_EXTRAS (1)
//   · RESCATE_ROTOS (1) · RESCATE_ESPERA_S (90) · RESCATE_HILOS (1) · RESCATE_RPS (4) · RESCATE_TOPE_TOKEN (20000) · RESCATE_MAX_GB (3)
//   · RESCATE_COLA_DIAS (7) · RESCATE_MEM_MB (768) · RESCATE_DIR (/data/rescate) · REGISTRO (1) · REGISTRO_DIAS (120) · RESCATE_CLAVE
// ═══════════════════════════════════════════════════════════════════════════════════════════
import fs from "fs"; import path from "path"; import zlib from "zlib"; import { Worker } from "worker_threads"; import { pipeline } from "stream/promises"; import { fileURLToPath } from "url";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const CARPETAS = ["curva", "swaps", "rotos", "extras", "registro", "fichas"];
const REL_OK = /^(curva|swaps|rotos|extras|registro|fichas)\/[A-Za-z0-9._-]{1,120}$/;
const dia = (ms = Date.now()) => new Date(ms).toISOString().slice(0, 10);   // el día en UTC (el presupuesto cuenta desde las 00:00 UTC = 2:00 en Madrid)
const fmt = (n) => Math.round(n).toLocaleString("es-ES");
const FIN = /^(ok|cortada|ya estaba|vacía|sin piscina)/;   // fases acabadas (no se repiten)

// un rescate que no hace nada (si el de verdad no puede arrancar, el server sigue igual)
export function nulo() { const n = () => {}; return { activo: false, dir: null, linea: n, encola: () => null, atiende: async () => false, resumen: () => ({ activo: false }), saludTxt: () => "🧾 rescate no disponible", arranqueTxt: () => "🧾 RESCATE no disponible", gastoHoy: () => 0, cierra: n, despacha: n, _estado: () => ({}) }; }

export function crea(op = {}) {
  const E_ = op.env || process.env; const log = op.log || ((m) => console.log(m));
  const num = (k, d) => { const v = E_[k]; return v != null && v !== "" && !Number.isNaN(+v) ? +v : d; };
  const txt = (k, d) => (E_[k] != null && String(E_[k]).trim() !== "" ? String(E_[k]).trim().toLowerCase() : d);
  const ON = txt("RESCATE", "1") !== "0" && !!(E_.HELIUS_API_KEY || E_.RESCATE_RPC_URL);
  const REG_ON = txt("REGISTRO", "1") !== "0";
  const DIR = op.dir || E_.RESCATE_DIR || path.join(fs.existsSync("/data") ? "/data" : "/tmp", "rescate");
  const TOPE_DIA = num("RESCATE_CREDITOS_DIA", 400000), TOPE_TOKEN = num("RESCATE_TOPE_TOKEN", 20000), TOPE_MES = num("HELIUS_CREDITOS_MES", 9500000);
  const A_CURVA = txt("RESCATE_CURVA", "todos"), A_PISCINA = txt("RESCATE_PISCINA", "operados");
  const EXTRAS = txt("RESCATE_EXTRAS", "1") !== "0", ROTOS = txt("RESCATE_ROTOS", "1") !== "0", PISCINA_MIN = num("RESCATE_PISCINA_MIN", 10);
  const ESPERA_S = num("RESCATE_ESPERA_S", 90), HILOS = Math.max(1, num("RESCATE_HILOS", 1)), RPS = num("RESCATE_RPS", 4);
  const MAX_GB = num("RESCATE_MAX_GB", 3), COLA_DIAS = num("RESCATE_COLA_DIAS", 7), REG_DIAS = num("REGISTRO_DIAS", 120), MEM_MB = num("RESCATE_MEM_MB", 768);
  const CLAVE_R = (E_.RESCATE_CLAVE || "").trim(); const CLAVE = /^(no|0|abierto)$/i.test(CLAVE_R) ? "" : (CLAVE_R || (E_.PANEL_CLAVE || "").trim());
  for (const c of [...CARPETAS, "parcial"]) fs.mkdirSync(path.join(DIR, c), { recursive: true });
  const VOLATIL = !DIR.startsWith("/data") && !E_.RESCATE_DIR && !op.dir;

  // ── lo que no debe salir nunca en el registro: las claves (tapadas con ***), y la ruta de los RPC de pago (la clave va dentro de la URL) ──
  const SECRETOS = []; for (const [k, v0] of Object.entries(E_)) { const v = String(v0 || "").trim(); if (v.length < 8 || !/KEY|CLAVE|SECRET|TOKEN|PRIVATE|PASS|RPC|WEBHOOK/i.test(k)) continue;
    SECRETOS.push(v); if (/^https?:\/\//i.test(v)) { try { const u = new URL(v); if (u.pathname.length > 8) SECRETOS.push(u.pathname.replace(/\/+$/, "")); for (const [, q] of u.searchParams) if (q.length >= 8) SECRETOS.push(q); } catch {} } }
  SECRETOS.sort((a, b) => b.length - a.length);
  const limpia = (s) => { let x = String(s); for (const k of SECRETOS) if (x.includes(k)) x = x.split(k).join("***"); return x.replace(/(api[-_]?key|token|secret)=[A-Za-z0-9_\-.]+/gi, "$1=***"); };

  // ═══ 1 · EL REGISTRO (se escribe en bloques cada medio segundo, sin frenar al server) ═══
  const REG = { dia: null, buf: [], escribiendo: false, lineas: 0, fallos: 0 };
  const regRel = (d) => `registro/${d}.log`;
  const linea = (texto) => {
    if (!REG_ON) return;
    try { const ahora = new Date(); const d = ahora.toISOString().slice(0, 10);
      if (d !== REG.dia) { if (REG.dia && REG.buf.length) vuelca(true); REG.dia = d; }
      REG.buf.push(JSON.stringify({ timestamp: ahora.toISOString(), message: limpia(texto) }) + "\n"); REG.lineas++;
      if (REG.buf.length > 20000) REG.buf.splice(0, REG.buf.length - 20000);   // si el disco no deja escribir, no se acumula sin fin
    } catch {}
  };
  function vuelca(sync = false) {   // lo pendiente del registro al disco
    if (!REG.buf.length || !REG.dia || (REG.escribiendo && !sync)) return; const f = path.join(DIR, regRel(REG.dia)); const datos = REG.buf.join(""); REG.buf = [];
    if (sync) { try { fs.appendFileSync(f, datos); apuntaFichero(regRel(REG.dia)); } catch { REG.fallos++; } return; }
    REG.escribiendo = true; const d = REG.dia;
    fs.appendFile(f, datos, (e) => { REG.escribiendo = false; if (e) { REG.fallos++; if (REG.fallos === 1 || REG.fallos % 500 === 0) console.error(`⚠️ registro en el disco: no puedo escribir (${e.code || e.message})`); } else apuntaFichero(regRel(d)); });
  }
  async function comprime(d, carpeta = "registro", ext = ".log") {   // el registro (o las fichas) de un día acabado → .gz (y fuera el sin comprimir)
    const f = path.join(DIR, carpeta, d + ext); if (!fs.existsSync(f)) return;
    const g = f + ".gz", tmp = g + ".tmp";
    try { await pipeline(fs.createReadStream(f), zlib.createGzip(), fs.createWriteStream(tmp));
      if (fs.existsSync(g)) { fs.writeFileSync(g, Buffer.concat([fs.readFileSync(g), fs.readFileSync(tmp)])); fs.unlinkSync(tmp); }   // ya había uno: se añade detrás (un .gz puede tener varios trozos)
      else fs.renameSync(tmp, g);
      fs.unlinkSync(f); apuntaFichero(`${carpeta}/${d}${ext}.gz`); IDX.delete(`${carpeta}/${d}${ext}`);
    } catch { try { fs.unlinkSync(tmp); } catch {} }
  }

  // ═══ 2 · LOS FICHEROS: el índice (qué hay, cuánto pesa, cuándo se escribió), lo que ya se bajó al PC, y la limpieza ═══
  const IDX = new Map();   // rel → [bytes, ms]
  function apuntaFichero(rel) { try { const st = fs.statSync(path.join(DIR, rel)); IDX.set(rel, [st.size, Math.round(st.mtimeMs)]); } catch { IDX.delete(rel); } }
  function indexa() { IDX.clear(); for (const c of CARPETAS) { let L = []; try { L = fs.readdirSync(path.join(DIR, c)); } catch {} for (const f of L) { if (f.endsWith(".tmp")) { try { fs.unlinkSync(path.join(DIR, c, f)); } catch {} continue; } apuntaFichero(c + "/" + f); } } }
  const F_BAJ = path.join(DIR, "bajados.txt"); const BAJADOS = new Set();   // lo que el PC ya se ha bajado (si hace falta sitio, se borra antes)
  try { for (const l of fs.readFileSync(F_BAJ, "utf-8").split("\n")) if (l) BAJADOS.add(l); } catch {}
  const apuntaBajado = (rel) => { const k = rel + "|" + (IDX.get(rel) || [0])[0]; if (BAJADOS.has(k)) return; BAJADOS.add(k); fs.appendFile(F_BAJ, k + "\n", () => {}); };
  const bajado = (rel) => { const v = IDX.get(rel); return !!v && BAJADOS.has(rel + "|" + v[0]); };
  const usado = () => { let b = 0; for (const [, v] of IDX) b += v[0]; return b; };
  function disco() { try { const s = fs.statfsSync(DIR); return { libre: s.bavail * s.bsize, tam: s.blocks * s.bsize }; } catch { return { libre: null, tam: null }; } }
  const umbrales = (tam) => tam ? { critico: Math.min(300e6, tam * 0.08), objetivo: Math.min(600e6, tam * 0.15) } : { critico: 300e6, objetivo: 600e6 };
  function discoJusto() { const D = disco(); return D.libre != null && D.libre < umbrales(D.tam).critico; }
  function limpiaDisco() {
    const ahora = Date.now(); let borrados = 0, bytes = 0, sinBajar = 0; const D = disco(); const U = umbrales(D.tam);
    const borra = (rel, v) => { try { fs.unlinkSync(path.join(DIR, rel)); } catch {} IDX.delete(rel); borrados++; bytes += v[0]; if (D.libre != null) D.libre += v[0]; };
    // el registro y las fichas de hace más de REGISTRO_DIAS días
    for (const [rel, v] of [...IDX]) if (/^(registro|fichas)\//.test(rel) && ahora - v[1] > REG_DIAS * 864e5) borra(rel, v);
    const limite = Math.min(MAX_GB * 1e9, D.tam ? D.tam * 0.7 : Infinity); let total = usado();
    const falta = () => total > 0.9 * limite || (D.libre != null && D.libre < U.objetivo);
    if (!(total > limite || (D.libre != null && D.libre < U.critico))) { if (borrados) log(`🧹 rescate: borro ${borrados} ficheros del registro de hace más de ${REG_DIAS} días`); return; }
    const hoyR = REG.dia ? regRel(REG.dia) : null; const esTok = (rel) => /^(curva|swaps|rotos|extras)\//.test(rel);
    const L = [...IDX].filter(([rel]) => rel !== hoyR).sort((a, b) => a[1][1] - b[1][1]);
    // 1º lo de los tokens ya bajado al PC; 2º el registro y las fichas ya bajados (de hace más de 7 días); 3º (si no queda otra) lo de los tokens sin bajar
    for (const [rel, v] of L) { if (!falta()) break; if (esTok(rel) && bajado(rel)) { total -= v[0]; borra(rel, v); } }
    for (const [rel, v] of L) { if (!falta()) break; if (!esTok(rel) && IDX.has(rel) && bajado(rel) && ahora - v[1] > 7 * 864e5) { total -= v[0]; borra(rel, v); } }
    if ((D.libre != null && D.libre < U.critico) || total > limite) for (const [rel, v] of L) { if (!falta()) break; if (esTok(rel) && IDX.has(rel)) { total -= v[0]; borra(rel, v); sinBajar++; } }
    if (borrados) log(`🧹 rescate: borro ${borrados} ficheros viejos (${(bytes / 1e6).toFixed(0)} MB) para no llenar el disco${sinBajar ? ` · ⚠️ ${sinBajar} NO se habían bajado al PC: lanza baja_del_server.bat más a menudo` : " (ya estaban en el PC)"}`);
  }

  // ═══ 3 · LA COLA Y EL PRESUPUESTO ═══
  const F_EST = path.join(DIR, "estado.json"), F_COLA = path.join(DIR, "cola.json"), F_PEND = path.join(DIR, "pendientes.jsonl");
  const leeJSON = (f, d, ok) => { try { const x = JSON.parse(fs.readFileSync(f, "utf-8")); if (ok(x)) return x; fs.renameSync(f, f + ".malo"); } catch {} return d; };
  const E = leeJSON(F_EST, { dias: {}, total: { creditos: 0, fichas: 0 }, pausaHasta: 0, pausaMotivo: null }, (x) => x && typeof x === "object" && x.dias && typeof x.dias === "object" && x.total);
  let cola = leeJSON(F_COLA, [], (x) => Array.isArray(x)).filter(j => j && j.mint && j.t && j.fases);
  let sucioE = false, sucioC = false, nId = cola.reduce((m, j) => Math.max(m, +j.id || 0), 0);
  const enCurso = new Map();   // id → { j, fases, tope, gastado }
  const hoyE = () => { const d = dia(); return E.dias[d] || (E.dias[d] = { creditos: 0, fichas: 0, curva: 0, piscina: 0, extras: 0, errores: 0, porFase: {} }); };
  const gastoHoy = () => (E.dias[dia()] || {}).creditos || 0;
  // lo que gasta al día el server en vivo (WebSocket + llamadas), medido; con menos de 2 h encendido, lo último que se midió (o 230.000)
  const vivo = () => { try { const v = op.gastoVivoDia && op.gastoVivoDia(); if (v && v.horas >= 2 && v.dia > 0) { if (Math.abs((E.vivoDia || 0) - v.dia) > 2000) { E.vivoDia = Math.round(v.dia); sucioE = true; } return v.dia; } } catch {} return E.vivoDia || 230000; };
  const presupuestoHoy = () => Math.max(0, Math.min(TOPE_DIA, Math.round(TOPE_MES / 30.4 - vivo())));
  let guardando = false;
  const guarda = (sync = false) => {   // el estado (pequeño) y la cola (con los trabajos en marcha dentro, por si el server se cae a medias)
    try { const dd = Object.keys(E.dias).sort(); for (const d of dd.slice(0, Math.max(0, dd.length - 90))) delete E.dias[d]; const tmp = F_EST + ".tmp"; fs.writeFileSync(tmp, JSON.stringify(E)); fs.renameSync(tmp, F_EST); sucioE = false; } catch {}
    if (!sucioC && !sync) return; if (guardando && !sync) return;
    const todo = [...cola, ...[...enCurso.values()].map(x => ({ ...x.j, fases: x.fases, listo: Math.min(x.j.listo || 0, Date.now()) }))]; sucioC = false;
    const txt = JSON.stringify(todo), tc = F_COLA + ".tmp";
    if (sync) { try { fs.writeFileSync(tc, txt); fs.renameSync(tc, F_COLA); } catch {} return; }
    guardando = true; fs.writeFile(tc, txt, (e) => { if (e) { guardando = false; sucioC = true; return; } fs.rename(tc, F_COLA, () => { guardando = false; }); });
  };
  const quiere = (alcance, grupo) => alcance === "todos" || (alcance === "motor" && (grupo === "operado" || grupo === "motor")) || (alcance === "operados" && grupo === "operado");
  function estima(j) {   // créditos que costará (para no pasarse del presupuesto del día): sale de lo que el server ya sabe del token
    const t = j.t, F = j.fases;
    const curva = F.curva ? (t.txsCurva != null && !t.txsCurvaTope ? Math.ceil(t.txsCurva / 100) * 10 + 10 : t.txsCurvaTope ? 800 : 160) : 0;
    const sw = t.durPiscina && t.swapsVentana != null ? t.swapsVentana : (t.swaps || 0);
    let piscina = F.piscina ? Math.ceil(2.1 * sw / 100) * 10 + 20 : 0; if (F.piscina && t.roto) piscina = Math.max(piscina, 600);   // Helius ve ~2× los swaps que cuenta el server
    const extras = F.extras ? (t.asset ? 4 : 14) : 0;
    return { total: curva + piscina + extras + 1, curva, piscina, extras };
  }
  const mete = (t, fases, prio) => {
    // se espera a que la cadena lo dé por definitivo (~15 s) con margen; si la cámara se cortó, a que pase su hora entera
    const listo = Math.max(Date.now() + ESPERA_S * 1000, t.roto && fases.piscina ? t.t0 + ((+t.dur || 3600) + 90) * 1000 : 0);
    const clave = JSON.stringify(fases); cola = cola.filter(x => !(x.mint === t.mint && JSON.stringify(x.fases) === clave));   // sin repetidos
    const j = { id: ++nId, mint: t.mint, sym: t.sym, t, fases, prio, listo, enc: Date.now(), intentos: 0 }; cola.push(j); sucioC = true; return j;
  };
  const encola = (t) => {
    if (!ON || !t || !t.mint) return null;
    const grupo = t.grupo || ((t.ops || 0) > 0 ? "operado" : t.motor ? "motor" : t.veto ? "vetado" : "otro"); t.grupo = grupo;
    const out = [];
    // 1º la curva de bonos y los extras (baratos: de todos los que pida RESCATE_CURVA)
    const fc = { curva: quiere(A_CURVA, grupo), piscina: false, extras: EXTRAS }; if (fc.curva || fc.extras) out.push(mete(t, fc, 0));
    // la piscina: entera si el bot lo operó (o si la cámara se cortó al reiniciar); de los demás, solo los primeros RESCATE_PISCINA_MIN minutos
    const roto = !!t.roto && ROTOS, pisc = quiere(A_PISCINA, grupo) || roto;
    if (pisc) { const entera = grupo === "operado" || roto || PISCINA_MIN <= 0; const tp = entera ? t : { ...t, durPiscina: PISCINA_MIN * 60 };
      out.push(mete(tp, { curva: false, piscina: true, extras: false }, roto ? 0 : grupo === "operado" ? 1 : 2)); }
    return out;
  };
  function elige() {
    const pres = presupuestoHoy(); let reservado = 0; for (const [, x] of enCurso) reservado += Math.max(0, x.tope - x.gastado);   // lo que aún pueden gastar los que van en marcha
    const queda = pres - gastoHoy() - reservado; if (queda < 30) return null; const ahora = Date.now();
    // la piscina de un token espera a que su curva de bonos esté bajada (para saber quién compró en la curva)
    const conCurva = new Set(); for (const o of cola) if (o.fases.curva) conCurva.add(o.mint); for (const [, o] of enCurso) conCurva.add(o.j.mint);
    cola.sort((a, b) => a.prio - b.prio || a.listo - b.listo);
    for (let i = 0; i < cola.length; i++) { const j = cola[i]; if (j.listo > ahora) continue;
      if (j.fases.piscina && !j.fases.curva && conCurva.has(j.mint)) continue;
      const e = estima(j); const tope = Math.min(TOPE_TOKEN, queda);
      const ultimo = ahora - (j.enc || ahora) > Math.max(1, COLA_DIAS - 1) * 864e5;   // el último día que le queda en la cola: lo que haya se guarda, aunque no esté entero
      if (e.total <= queda) { cola.splice(i, 1); sucioC = true; return { j, fases: j.fases, tope, topeMax: ultimo }; }
      // no cabe entero: lo demás ya, y la piscina espera su turno (mañana, si hoy no llega)
      if (j.fases.piscina && (j.fases.curva || j.fases.extras) && e.total - e.piscina <= queda) { cola[i] = { ...j, id: ++nId, fases: { curva: false, piscina: true, extras: false }, partida: true }; sucioC = true; return { j, fases: { curva: j.fases.curva, piscina: false, extras: j.fases.extras }, tope, topeMax: ultimo }; }
    }
    return null;
  }
  const pausado = () => E.pausaHasta && Date.now() < E.pausaHasta;
  const pendiente = (o) => { try { fs.appendFileSync(F_PEND, JSON.stringify({ ...o, hora: new Date().toISOString() }) + "\n"); const st = fs.statSync(F_PEND); if (st.size > 8e6) { const L = fs.readFileSync(F_PEND, "utf-8").split("\n"); fs.writeFileSync(F_PEND, L.slice(Math.floor(L.length / 2)).join("\n")); } } catch {} };
  function caduca() {   // lo que lleva más de RESCATE_COLA_DIAS días esperando sale de la cola a pendientes.jsonl (se puede rescatar desde el PC)
    const lim = Date.now() - COLA_DIAS * 864e5; const fuera = cola.filter(j => (j.enc || 0) < lim); if (!fuera.length && cola.length <= 30000) return;
    cola = cola.filter(j => (j.enc || 0) >= lim); if (cola.length > 30000) { cola.sort((a, b) => a.prio - b.prio || b.enc - a.enc); fuera.push(...cola.splice(30000)); }
    for (const j of fuera) { pendiente({ mint: j.mint, sym: j.sym, fases: j.fases, grupo: j.t.grupo, t: { ...j.t, asset: undefined }, porque: "caducado en la cola (sin presupuesto)" }); for (const k of ["curva", "swaps", "rotos"]) try { fs.unlinkSync(path.join(DIR, "parcial", `${k}_${j.mint}.json.gz`)); } catch {} }
    sucioC = true; log(`🧾 rescate: ${fuera.length} tokens llevaban más de ${COLA_DIAS} días en la cola (sin presupuesto): a pendientes.jsonl, se pueden rescatar desde el PC`);
  }

  // ═══ 4 · EL TRABAJADOR (otro hilo) ═══
  let W = null, wListo = false, caidas = 0, caidasSeguidas = 0, wDesde = 0, parando = false;
  const BOTS = (() => { for (const f of [path.join(AQUI, "bots.json"), path.join(DIR, "bots.json")]) { try { return JSON.parse(fs.readFileSync(f, "utf-8")).map(b => b.c || b); } catch {} } return []; })();
  function arranca() {
    if (!ON || parando) return;
    try { W = new Worker(new URL("./rescate_trabajo.js", import.meta.url), { workerData: { dir: DIR, rps: RPS, hilos: HILOS, bots: BOTS, rpcUrl: E_.RESCATE_RPC_URL || null }, resourceLimits: { maxOldGenerationSizeMb: MEM_MB } }); wDesde = Date.now(); }
    catch (e) { log(`⚠️ rescate: no arranca el trabajador (${e.message})`); W = null; return; }
    W.on("message", (m) => { try { alMensaje(m); } catch (e) { log(`⚠️ rescate: ${e && e.message}`); } });
    W.on("error", (e) => { log(`⚠️ rescate: el trabajador falló (${String(e && e.message || e).slice(0, 200)})`); });
    W.on("exit", (code) => { try { wListo = false; W = null; caidas++; caidasSeguidas = Date.now() - wDesde > 10 * 60e3 ? 1 : caidasSeguidas + 1;
      for (const [, x] of enCurso) { const j = x.j; const intentos = (j.intentos || 0) + 1;
        if (intentos >= 3) pendiente({ mint: j.mint, sym: j.sym, fases: x.fases, grupo: j.t.grupo, t: { ...j.t, asset: undefined }, porque: "el trabajador se cayó 3 veces con este token (¿demasiado grande?)" });
        else cola.push({ ...j, fases: x.fases, intentos, listo: Date.now() + 60e3 }); }
      enCurso.clear(); sucioC = true; guarda(true);
      if (!parando) { const ms = Math.min(15 * 60e3, 15000 * Math.pow(2, Math.max(0, caidasSeguidas - 1))); log(`⚠️ rescate: el trabajador se paró (código ${code}); lo vuelvo a arrancar en ${Math.round(ms / 1000)} s`); setTimeout(arranca, ms); } } catch {} });
  }
  function alMensaje(m) {
    if (!m) return;
    if (m.tipo === "listo") { wListo = true; return; }
    if (m.tipo === "gasto") { const h = hoyE(); h.creditos += m.n; h.porFase[m.fase] = (h.porFase[m.fase] || 0) + m.n; E.total.creditos += m.n; const x = enCurso.get(m.id); if (x) x.gastado += m.n; sucioE = true; return; }
    if (m.tipo === "log") { log(m.m); return; }
    if (m.tipo !== "hecho") return;
    const x = enCurso.get(m.id); enCurso.delete(m.id); sucioC = true; const j = x ? x.j : null; const h = hoyE();
    for (const [rel] of m.ficheros || []) apuntaFichero(rel);
    const f = m.ficha;
    if (f) { h.fichas++; E.total.fichas++; const fs_ = f.fases || {};
      if (/^(ok|cortada|ya estaba)/.test(fs_.curva || "")) h.curva++; if (/^(ok|cortada|ya estaba|vacía)/.test(fs_.piscina || "")) h.piscina++; if (fs_.extras === "ok") h.extras++;
      const fc = f.curva || {}, fp = f.piscina || {};
      if (f.curva || f.piscina || f.extras) {
        log(`[SOLFICHA] sym=${String(f.sym || "?").replace(/\s+/g, "_")} mint=${f.mint} grupo=${f.grupo} creditos=${m.creditos} curva=${fs_.curva || "-"} piscina=${fs_.piscina || "-"} extras=${fs_.extras || "-"} · ${fc.tx != null ? `curva de bonos ${fc.tx} tx, ${fc.compradores} compradores, bundle ${fc.bundle_slot0_pct ?? 0} %, top5 ${fc.top5_pct ?? "?"} %, creador conserva ${fc.creador_conserva_pct ?? "?"} %` : "sin curva de bonos"} · ${fp.swaps != null ? `piscina ${fp.swaps} swaps, ${fp.carteras} carteras` : "sin piscina"} json=${JSON.stringify(f)}`);
        try { const rel = `fichas/${dia()}.jsonl`; fs.appendFileSync(path.join(DIR, rel), JSON.stringify({ hecho: new Date().toISOString(), t: j ? { ...j.t, asset: undefined } : { mint: m.mint }, ficha: f }) + "\n"); apuntaFichero(rel); } catch {} }
    } else { h.errores++; log(`⚠️ rescate de ${j ? j.sym : m.mint}: ${m.error || "sin ficha"}`); }
    // lo que quedó sin hacer vuelve a la cola:
    //   · por el presupuesto del día → mañana, y sigue donde lo dejó (no se paga dos veces)
    //   · Helius sin créditos, la clave mal o el disco lleno → se para el rescate un rato (la grabación y el bot siguen igual), sin contar como intento
    //   · un error suelto → otra vez dentro de 30 min, como mucho 3 intentos
    const pausa = m.corte === "creditos" || m.corte === "clave" || m.corte === "disco";
    if (pausa) { const horas = m.corte === "clave" ? 6 : 1; E.pausaHasta = Date.now() + horas * 3600e3; E.pausaMotivo = m.corte === "clave" ? "Helius no acepta la clave" : m.corte === "disco" ? "el disco del server está lleno" : "Helius dice que no quedan créditos"; sucioE = true;
      log(`⏸️ rescate: ${E.pausaMotivo} → lo paro ${horas} h (la grabación y el bot siguen igual)`); }
    if (j && x) { const re = {}; let porError = false;
      for (const k of ["curva", "piscina", "extras"]) { if (!x.fases[k]) continue; const st = (f && f.fases && f.fases[k]) || ""; if (FIN.test(st)) continue;
        if (st !== "sin presupuesto") porError = true; re[k] = true; }
      if (Object.keys(re).length) { const intentos = (j.intentos || 0) + (porError && !pausa ? 1 : 0);
        if (intentos >= 3) pendiente({ mint: j.mint, sym: j.sym, fases: re, grupo: j.t.grupo, t: { ...j.t, asset: undefined }, porque: "falló 3 veces: " + JSON.stringify(f && f.fases || m.error || "") });
        else { const listo = pausa ? Math.max(E.pausaHasta || 0, Date.now() + 3600e3) : porError ? Date.now() + 30 * 60e3 : Date.parse(dia(Date.now() + 864e5) + "T00:05:00Z");
          cola.push({ ...j, id: ++nId, fases: { curva: !!re.curva, piscina: !!re.piscina, extras: !!re.extras }, listo, intentos }); } } }
    despacha();
  }
  let avisoDisco = 0;
  function despacha() {
    if (!ON || parando || !W || !wListo || pausado()) return;
    if (discoJusto()) { if (Date.now() - avisoDisco > 3600e3) { avisoDisco = Date.now(); log(`⚠️ rescate: el disco del server va muy justo: no bajo nada nuevo hasta que haya sitio (lanza baja_del_server.bat; se borra lo ya bajado)`); limpiaDisco(); } return; }
    while (enCurso.size < HILOS) { const s = elige(); if (!s) break; enCurso.set(s.j.id, { j: s.j, fases: s.fases, tope: s.tope, gastado: 0, desde: Date.now() }); sucioC = true; W.postMessage({ tipo: "trabajo", id: s.j.id, t: s.j.t, fases: s.fases, tope: s.tope, topeMax: s.topeMax }); }
  }

  // ═══ 5 · LAS RUTAS ═══
  const atiende = async (req, res) => {
    const u = new URL(req.url, "http://x"); if (!u.pathname.startsWith("/rescate")) return false;
    const json = (o) => { res.setHeader("content-type", "application/json"); res.setHeader("cache-control", "no-store"); res.end(JSON.stringify(o)); };
    if (CLAVE && u.searchParams.get("clave") !== CLAVE && req.headers["x-clave"] !== CLAVE) { res.statusCode = 403; res.end("falta la clave del panel (PANEL_CLAVE, o RESCATE_CLAVE si la pusiste)"); return true; }
    if (u.pathname === "/rescate" || u.pathname === "/rescate/") { json(resumen()); return true; }
    if (u.pathname === "/rescate/indice") { const desde = +(u.searchParams.get("desde") || 0); vuelca(true);
      for (const rel of [...IDX.keys()].filter(r => /^(registro\/.*\.log|fichas\/.*\.jsonl)$/.test(r))) apuntaFichero(rel);   // los de hoy crecen: se miran al momento
      json({ ahora: Date.now(), ficheros: [...IDX].filter(([, v]) => v[1] > desde).map(([rel, v]) => [rel, v[0], v[1]]) }); return true; }
    if (u.pathname === "/rescate/pendientes") { let pend = []; try { pend = (await fs.promises.readFile(F_PEND, "utf-8")).split("\n").filter(Boolean).slice(-5000).map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean); } catch {}
      json({ cola: cola.map(j => ({ mint: j.mint, sym: j.sym, grupo: j.t.grupo, fases: j.fases, prio: j.prio, listo: j.listo, estimado: estima(j).total })), caducados: pend.map(p => ({ mint: p.mint, sym: p.sym, grupo: p.grupo, fases: p.fases, porque: p.porque, t: p.t })) }); return true; }
    if (u.pathname.startsWith("/rescate/f/")) { const rel = decodeURIComponent(u.pathname.slice("/rescate/f/".length));
      if (!REL_OK.test(rel) || rel.includes("..")) { res.statusCode = 400; res.end("ruta no válida"); return true; }
      const f = path.join(DIR, rel); let st; try { st = fs.statSync(f); } catch { res.statusCode = 404; res.end("no está"); return true; }
      res.setHeader("content-type", rel.endsWith(".gz") ? "application/gzip" : rel.endsWith(".json") ? "application/json" : "text/plain; charset=utf-8"); res.setHeader("content-length", st.size); res.setHeader("x-mtime", Math.round(st.mtimeMs));
      if (!st.size) { res.end(); return true; }
      let entero = true; await pipeline(fs.createReadStream(f, { start: 0, end: st.size - 1 }), res).catch(() => { entero = false; });   // hasta el tamaño anunciado (el registro de hoy sigue creciendo)
      if (entero) { if (!IDX.has(rel)) apuntaFichero(rel); if (IDX.has(rel) && IDX.get(rel)[0] === st.size) apuntaBajado(rel); } return true; }
    res.statusCode = 404; res.end("rutas: /rescate · /rescate/indice?desde=ms · /rescate/f/<carpeta>/<fichero> · /rescate/pendientes"); return true;
  };

  // ═══ 6 · CÓMO VA ═══
  function resumen() { const h = E.dias[dia()] || {}; const D = disco(); let sinBajar = 0, bSinBajar = 0; for (const [rel, v] of IDX) if (/^(curva|swaps|rotos|extras)\//.test(rel) && !bajado(rel)) { sinBajar++; bSinBajar += v[0]; }
    return { activo: ON, registro: REG_ON, dir: DIR, volatil: VOLATIL, conClave: !!CLAVE, alcance: { curva: A_CURVA, piscina: A_PISCINA, piscinaMinNoOperados: PISCINA_MIN, extras: EXTRAS, rotos: ROTOS }, topeDia: presupuestoHoy(), topeDiaFijo: TOPE_DIA, topeMes: TOPE_MES, vivoDia: Math.round(vivo()),
      hoy: { dia: dia(), ...h }, total: E.total, cola: cola.length, colaListos: cola.filter(j => j.listo <= Date.now()).length, enCurso: enCurso.size, trabajador: W ? (wListo ? "listo" : "arrancando") : "parado", caidas,
      pausa: pausado() ? { hasta: E.pausaHasta, motivo: E.pausaMotivo } : null, disco: { usadoMB: Math.round(usado() / 1e6), libreMB: D.libre != null ? Math.round(D.libre / 1e6) : null, ficheros: IDX.size, sinBajar, sinBajarMB: Math.round(bSinBajar / 1e6), topeGB: MAX_GB }, lineasRegistro: REG.lineas, fallosRegistro: REG.fallos,
      ultimos7: Object.entries(E.dias).sort().slice(-7).map(([d, x]) => ({ dia: d, creditos: x.creditos, fichas: x.fichas, curva: x.curva, piscina: x.piscina })) }; }
  function saludTxt() { if (!ON) return `🧾 rescate apagado${REG_ON ? " · registro en el disco sí" : ""}`; const h = E.dias[dia()] || {};
    return `🧾 rescate: hoy ${h.fichas || 0} fichas (curva ${h.curva || 0} · piscina ${h.piscina || 0}) · ${fmt(h.creditos || 0)} de ${fmt(presupuestoHoy())} créditos (lo que sobra de ${fmt(TOPE_MES)} al mes con el server gastando ≈${fmt(vivo())}/día) · cola ${cola.length}${enCurso.size ? ` (${enCurso.size} en marcha)` : ""} · ${(usado() / 1e9).toFixed(2)} GB${pausado() ? ` · ⏸️ PAUSADO: ${E.pausaMotivo}` : ""}${VOLATIL ? " · ⚠️ sin volumen /data: se pierde al reiniciar" : ""}`; }
  const arranqueTxt = () => !ON ? `🧾 RESCATE apagado (RESCATE=0 o sin clave de Helius)${REG_ON ? ` · el registro sí se guarda en ${DIR}/registro` : ""}`
    : `🧾 RESCATE dentro del server: curva de bonos y extras de ${A_CURVA === "todos" ? "todos" : A_CURVA} · piscina con cartera de ${A_PISCINA === "operados" ? "los operados (entera)" : A_PISCINA === "todos" ? `los operados (entera) y del resto los primeros ${PISCINA_MIN} min` : A_PISCINA}${ROTOS ? " · las cámaras cortadas se rehacen" : ""} · con lo que sobre de ${fmt(TOPE_MES)} créditos al mes para todo el server (hoy ≈${fmt(presupuestoHoy())}; tope ${fmt(TOPE_DIA)}/día) · ${HILOS} a la vez, ${RPS} llamadas/s · en ${DIR}${VOLATIL ? " (⚠️ NO es un volumen: se borra al reiniciar)" : ""} · cola ${cola.length} · registro ${REG_ON ? "en el disco" : "no"} · rutas /rescate ${CLAVE ? "con la clave del panel" : "SIN clave"}`;

  // ── arranque ──
  indexa();
  const comprimeViejos = () => { const hoy = dia(); for (const rel of [...IDX.keys()]) { const m = rel.match(/^(registro|fichas)\/(\d{4}-\d{2}-\d{2})(\.log|\.jsonl)$/); if (m && m[2] < hoy && !(m[1] === "registro" && m[2] === REG.dia && REG.buf.length)) comprime(m[2], m[1], m[3]).catch(() => {}); } };
  const seguro = (fn) => () => { try { fn(); } catch (e) { try { log(`⚠️ rescate: ${e && e.message}`); } catch {} } };
  seguro(comprimeViejos)(); seguro(limpiaDisco)();
  const t0 = setInterval(seguro(() => vuelca()), 500); t0.unref?.();
  const t1 = setInterval(seguro(() => { despacha(); if (sucioE || sucioC) guarda(); }), 3000); t1.unref?.();
  const t2 = setInterval(seguro(() => { caduca(); limpiaDisco(); }), 20 * 60e3); t2.unref?.();
  const t3 = setInterval(seguro(comprimeViejos), 10 * 60e3); t3.unref?.();
  arranca();
  // al apagar: lo que estaba a medias vuelve a la cola (lo ya escrito no se pide otra vez) y se guarda todo
  const cierra = () => { if (parando) return; parando = true; for (const [, x] of enCurso) cola.push({ ...x.j, fases: x.fases, listo: Date.now() }); enCurso.clear(); sucioC = true; guarda(true); vuelca(true); for (const t of [t0, t1, t2, t3]) clearInterval(t); try { W && W.terminate(); } catch {} };
  return { activo: ON, dir: DIR, linea, encola, atiende, resumen, saludTxt, arranqueTxt, gastoHoy, cierra, despacha, _estado: () => ({ cola, enCurso, E, IDX, BAJADOS }) };
}
