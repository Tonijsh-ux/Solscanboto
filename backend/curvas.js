// ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
//  LA MUESTRA DE CURVAS · 18-oct-2026 · ¿cuántas de las curvas de pump.fun que llegan a X % migran?
//  Lo que tenemos (las curvas del rescate) son solo las que MIGRARON: comprar en la curva al 50 % y vender en la piscina da +1,5 SOL por SOL si
//  migra, pero no sabemos qué parte de las que llegan al 50 % se muere a medio llenar (y ese número es el que decide si compensa: al 50 %
//  hacen falta un 25-39 % de migraciones para empatar, al 75 % un 47-64 %, al 90 % un 67-80 %; herramientas/solana/curva/llenado.py).
//  Esto lo mide: PumpPortal (el WebSocket gratis que ya usamos para las migraciones) avisa de cada token que se crea (subscribeNewToken); se
//  apunta 1 de cada CURVAS_UNO_DE (6) con su hora, símbolo y la compra inicial del creador. CURVAS_ESPERA_H (24) horas después se pregunta a la
//  API de pump.fun por cada uno (frontend-api-v3.pump.fun/coins-v2/<mint>, la única ruta que contesta: la lista /coins da 403 hasta desde el PC):
//  si migró (complete), su capitalización de ahora y la máxima que tuvo (ath_market_cap, en dólares; market_cap va en SOL), de la que sale hasta
//  dónde se llenó la curva: capitalización en SOL = (30 + SOL reales)² / 32,214 (27,9 SOL al nacer, 410,5 al migrar).
//  Se guarda en CURVAS_FILE (/data/curvas_muestra.json) y cada hora sale una línea [CURVAS] con lo acumulado: cuántos tokens se crean por hora y,
//  de los que llegaron a ≥ 10 / 25 / 50 / 75 / 90 %, cuántos migraron. Sin créditos de Helius; la API es gratis y a veces no contesta: con 3 fallos
//  seguidos se para media hora, y un 404 cuenta como «muerta sin llenar». Ruta GET /curvas (resumen) y /curvas/todo (el fichero), con la clave
//  del panel (?clave=).  Variables: CURVAS_MUESTRA (1; 0 apaga) · CURVAS_UNO_DE (6) · CURVAS_ESPERA_H (24) · CURVAS_FILE.
// ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import fs from "fs";
export function nulo() { return { activo: false, nueva: () => {}, atiende: async () => false, resumen: () => ({ activo: false }), saludTxt: () => "", arranqueTxt: () => "🧪 muestra de curvas apagada", cierra: () => {} }; }
export function crea(op = {}) {
  const log = op.log || console.log; const pumpApi = op.pumpApi; const solUsd = op.solUsd || (() => 0); const CLAVE = op.clave || "";
  const ON = process.env.CURVAS_MUESTRA !== "0" && typeof pumpApi === "function";
  const UNO_DE = Math.max(1, Math.round(+(process.env.CURVAS_UNO_DE || 6))), ESPERA_H = +(process.env.CURVAS_ESPERA_H || 24);
  const FILE = process.env.CURVAS_FILE || (fs.existsSync("/data") ? "/data/curvas_muestra.json" : "/tmp/curvas_muestra.json");
  const K_MC = 32.214;   // capitalización en SOL = (30 + s)² / K_MC  (K = 30 SOL × 1.073,8 M tokens virtuales, supply 1.000 M)
  const llenado = (mcSol) => mcSol > 0 ? Math.max(0, Math.min(1.2, (Math.sqrt(mcSol * K_MC) - 30) / 85)) : 0;
  let D = { vistas: {}, hechas: [], err: 0, errSeguidos: 0, errTxt: "", pausaHasta: 0, creadas: 0, apuntadas: 0, ultimaCreada: 0, ultimaResuelta: 0, desde: Date.now() };
  try { if (fs.existsSync(FILE)) { const j = JSON.parse(fs.readFileSync(FILE, "utf-8")); if (j && j.vistas && Array.isArray(j.hechas)) D = { ...D, ...j, errSeguidos: 0, pausaHasta: 0 }; } } catch (e) { log(`⚠️ muestra de curvas: no leo ${FILE} (${e.message}); empiezo de cero`); }
  const guarda = () => { try { const tmp = FILE + ".tmp"; fs.writeFileSync(tmp, JSON.stringify(D)); fs.renameSync(tmp, FILE); } catch (e) {} };
  const falla = (e, que) => { D.err++; D.errSeguidos++; D.errTxt = `${que}: ${String(e && e.message || e).slice(0, 80)}`; if (D.errSeguidos >= 3) { D.pausaHasta = Date.now() + 30 * 60e3; D.errSeguidos = 0; } };
  const bien = () => { D.errSeguidos = 0; };
  const HECHAS = new Set(D.hechas.map(h => h.m));
  // ── cada token nuevo que avisa PumpPortal (txType create): se apunta 1 de cada UNO_DE ──
  function nueva(m) {
    if (!ON || !m || !m.mint) return; D.creadas++; D.ultimaCreada = Date.now();
    if (D.creadas % UNO_DE !== 0 || D.vistas[m.mint] || HECHAS.has(m.mint)) return;
    if (Object.keys(D.vistas).length >= 4000) return;   // si la API no da abasto, no se acumulan más
    D.vistas[m.mint] = { c: Date.now(), s: String(m.symbol || m.name || "").slice(0, 12), dev: +(+m.solAmount || 0).toFixed(3), mc0: +(+m.marketCapSol || 0).toFixed(1) }; D.apuntadas++;
    if (D.apuntadas % 500 === 0) guarda();
  }
  // ── la resolución: ESPERA_H horas después, qué fue de cada token ──
  let resolviendo = false;
  async function resuelve() {
    if (!ON || resolviendo || Date.now() < D.pausaHasta) return; resolviendo = true;
    try {
      const lim = Date.now() - ESPERA_H * 3600e3; const pend = Object.entries(D.vistas).filter(([, v]) => v.c <= lim).sort((a, b) => a[1].c - b[1].c).slice(0, 80);
      for (const [m, v] of pend) {
        if (Date.now() < D.pausaHasta) break;
        try {
          const c = await pumpApi(`/coins-v2/${m}`); const ahora = Date.now();
          D.hechas.push({ m, s: v.s, c: v.c, dev: v.dev, mc0: v.mc0, t: ahora, mig: !!(c && c.complete), mc: c && c.market_cap != null ? +(+c.market_cap).toFixed(2) : null, usd: c && c.usd_market_cap != null ? Math.round(+c.usd_market_cap) : null,
            ath: c && c.ath_market_cap != null ? Math.round(+c.ath_market_cap) : null, athT: c && c.ath_market_cap_timestamp || null, ult: c && c.last_trade_timestamp || null, sol: +(+solUsd() || 0).toFixed(2), pool: c && c.pump_swap_pool ? 1 : 0 });
          HECHAS.add(m); delete D.vistas[m]; D.ultimaResuelta = ahora; bien();
        } catch (e) {
          const txt = String(e && e.message || e);
          if (/pump\.fun 404/.test(txt)) { D.hechas.push({ m, s: v.s, c: v.c, dev: v.dev, mc0: v.mc0, t: Date.now(), mig: false, mc: null, usd: null, ath: null, athT: null, ult: null, sol: 0, pool: 0, nf: 1 }); HECHAS.add(m); delete D.vistas[m]; bien(); }   // ya no existe para la API: muerta sin llenar
          else { falla(e, "token"); if (D.err <= 3 || D.err % 50 === 0) log(`⚠️ muestra de curvas (token): ${txt.slice(0, 120)} · fallos ${D.err}`); }
        }
        await new Promise(r => setTimeout(r, 1500));
      }
      if (D.hechas.length > 60000) D.hechas = D.hechas.slice(-60000);
      guarda();
    } finally { resolviendo = false; }
  }
  // ── el resumen: de las que llegaron a ≥ X %, cuántas migraron (el máximo de la capitalización en USD pasado a SOL con el precio de ese momento) ──
  function resumen() {
    const h = Math.max(0.05, (Date.now() - (D.desde || Date.now())) / 3600e3);
    const R = { activo: ON, creadas: D.creadas, porHora: Math.round(D.creadas / h), apuntadas: D.apuntadas, unoDe: UNO_DE, esperando: Object.keys(D.vistas).length, hechas: D.hechas.length, migran: 0, sinDato: 0, niveles: {}, err: D.err, errTxt: D.errTxt, pausa: D.pausaHasta > Date.now(), ultimaCreada: D.ultimaCreada };
    const niveles = [0.1, 0.25, 0.5, 0.75, 0.9]; for (const x of niveles) R.niveles[x] = { llegan: 0, migran: 0 };
    for (const h of D.hechas) {
      if (h.mig) R.migran++;
      let f = h.mig ? 1 : null;
      if (f == null) { if (h.ath != null && h.sol > 0) f = llenado(h.ath / h.sol); else if (h.nf) f = 0; else { R.sinDato++; continue; } }
      for (const x of niveles) if (f >= x) { R.niveles[x].llegan++; if (h.mig) R.niveles[x].migran++; }
    }
    return R;
  }
  const pct = (a, b) => b ? Math.round(100 * a / b) + " %" : "—";
  const saludTxt = () => { if (!ON) return ""; const R = resumen(); return `[CURVAS] pump.fun crea ≈ ${R.porHora} tokens/h (${R.creadas} vistos${R.ultimaCreada ? `, el último hace ${Math.round((Date.now() - R.ultimaCreada) / 60e3)} min` : ""}) · apuntados 1 de cada ${R.unoDe}: ${R.apuntadas} · esperando ${R.esperando} · resueltas ${R.hechas} · migran ${R.migran} (${pct(R.migran, R.hechas)}) · llegan a ≥10 % ${R.niveles[0.1].llegan} → migran ${pct(R.niveles[0.1].migran, R.niveles[0.1].llegan)} · ≥25 % ${R.niveles[0.25].llegan} → ${pct(R.niveles[0.25].migran, R.niveles[0.25].llegan)} · ≥50 % ${R.niveles[0.5].llegan} → ${pct(R.niveles[0.5].migran, R.niveles[0.5].llegan)} · ≥75 % ${R.niveles[0.75].llegan} → ${pct(R.niveles[0.75].migran, R.niveles[0.75].llegan)} · ≥90 % ${R.niveles[0.9].llegan} → ${pct(R.niveles[0.9].migran, R.niveles[0.9].llegan)} · sin dato ${R.sinDato} · fallos ${R.err}${R.errTxt ? ` (último ${R.errTxt})` : ""}${R.pausa ? " · en pausa" : ""}`; };
  const arranqueTxt = () => ON ? `🧪 MUESTRA DE CURVAS: 1 de cada ${UNO_DE} tokens que crea pump.fun (PumpPortal), y a las ${ESPERA_H} h si migraron y hasta dónde se llenaron (coins-v2) · ${Object.keys(D.vistas).length} esperando, ${D.hechas.length} resueltas · en ${FILE} · GET /curvas con la clave` : `🧪 muestra de curvas apagada (CURVAS_MUESTRA=0${typeof pumpApi === "function" ? "" : " o sin pumpApi"})`;
  const atiende = async (req, res) => {
    if (!req.url.startsWith("/curvas")) return false;
    const u = new URL(req.url, "http://x"); if (CLAVE && u.searchParams.get("clave") !== CLAVE && req.headers["x-clave"] !== CLAVE) { res.statusCode = 403; res.end("falta la clave del panel (PANEL_CLAVE)"); return true; }
    res.setHeader("content-type", "application/json; charset=utf-8");
    if (u.pathname === "/curvas/todo") { res.end(JSON.stringify(D)); return true; }
    res.end(JSON.stringify(resumen())); return true;
  };
  let T2 = null, T3 = null, T4 = null;
  if (ON) { T2 = setInterval(resuelve, 20 * 60e3); T3 = setInterval(() => log(saludTxt()), 60 * 60e3); T4 = setInterval(guarda, 10 * 60e3); setTimeout(resuelve, 90e3); }
  const cierra = () => { for (const t of [T2, T3, T4]) if (t) clearInterval(t); guarda(); };
  return { activo: ON, nueva, atiende, resumen, saludTxt, arranqueTxt, cierra, _estado: () => D, _llenado: llenado, _resuelve: resuelve };
}
