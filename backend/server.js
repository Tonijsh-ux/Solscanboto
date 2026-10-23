// ═══════════════════════════════════════════════════════════════════════════════════════════
//  EL SERVER DE SOLANA, LISTO PARA REAL · fase 3 · 5-oct-2026
//  [23-oct] + «HOY + CAZADOR» (Tony: «pon en el server la de hoy + cazador y lánzalo, pon el panel a cero»; «quita las ballenas de siempre de los contadores
//    de la sección de tokens»): plan «hoy + cazador 23-oct» = el 1er lote con UNA sola compra (antes hasta 8; con lo grabado desde el 14-oct: −15,5 SOL
//    tal cual, +2,2 con una) y, al cambiar de plan, las cuentas en papel empiezan de cero · el 2º lote (y el cazador) también en los tokens que el 1er
//    lote no compra por el precio a los 3 s («nosube», «cae»), como «Hoy» en las pruebas · EL CAZADOR (papel, bloque CAZA) · /tokens sin las ballenas.
//  [13-oct] + LAS BALLENAS QUE SE COLABAN, FUERA: desde el 7-oct el nacimiento no traía los segundos hasta migrar en ~30 % de los tokens (justo los que
//    se crean y migran de golpe: Helius aún no tiene sus firmas cuando se le preguntan) y la regla de siempre los necesitaba. Ahora se reintenta, y a los
//    3 s de migrar se veta: la ballena de siempre, sin dato de cuándo se creó, salto ×2 de verdad en los 3 primeros segundos, una cartera con toda la curva
//    o ≤5 tenedores, y los bundles (migra en ≤5 s sin salto). Variables: VETO_BALLENA (1) · VETO_BUNDLE (1) · VETO_SALTO (2) · NAC_REINTENTO_MS (2000).
//    Y NO SE GRABAN (Tony: «si no las voy a operar, que no las grabe», para gastar menos créditos de Helius: eran el ~66 % de los swaps que llegan por el
//    WebSocket): al vetarlas se cierra su cámara y sale una línea corta [SOLVETO] (mint, motivo, nacimiento y los primeros segundos), sin rescate.
//    Se graba entera 1 de cada GRABA_MUESTRA (20), para poder comprobar que el veto sigue acertando. GRABA_VETADAS=1 = grabarlas todas (como antes).
//  [13-oct, 07:30] + NO COMPRA SI YA CAE (Tony: «si desde que migra hasta que lanza la compra ha bajado un X %, que cancele»): a los 3,5 s, si el precio
//    está un NO_COMPRA_SI_CAE % (2) o más por debajo del de migrar, no se compra (se graba igual, para comprobarlo). NO_COMPRA_SI_CAE=no = comprar siempre.
//    [13-oct, 15:40] mira el PRECIO QUE GRABA (wSOL / tokens de la piscina, el del papel y el de la página «Solana desde cero»), no el de verdad: así el corte
//    que se elige en la página es el mismo que aplica el server. En los 2.661 limpios del 3 al 12-oct, EN PAPEL (el limpio de serie, con el plan, lote 0,1):
//    compra 1.021 de 1.768 y pasa de +42,1 a +45,5 SOL (replay_cae.js = la página). (Con el precio y los costes de verdad, el exigente pasaba de −23,5 a +7,6.)
//  [16-oct] + NO COMPRA SI NO SUBE (la criba de «Todas» v42, 📈): a los 3,5 s, si el precio que graba no ha subido al menos un NO_COMPRA_SI_NO_SUBE %
//    sobre el de migrar, no se compra (se graba igual). De serie apagado. En 3-16 oct (la página = el server, de verdad, lote 0,1), con 4 %: +7,5 → +22,0 SOL,
//    mejora 11 de 14 días; en los cierres reales desde el reloj (14-oct 13:38Z → 16-oct 14:37Z), los de −2…+4 % a los 3 s dieron −4,5 SOL en 79 tokens.
//  [18-oct] + EL 2º LOTE A LOS 60 s, EN PAPEL (Tony: «sí»): si el token fue al motor y en su 1er minuto el server ha visto entrar ≥ LOTE2_ENTRA SOL
//    con ≤ LOTE2_SWAPS swaps, un 2º lote de LOTE2_SOL SOL (de serie apagado: 0) que sale al ×LOTE2_OBJ o a los LOTE2_RELOJ s, contado con la piscina
//    de verdad como el banco de «Todas» ([SOLLOTE2], «2º lote» en [SALUD]). En la página (Helius, 4-17 oct): 136 ops, +284 SOL con 9, 11 días de 12.
//    El motor y el 1er lote no cambian; se apunta también lo que habría hecho el 1er lote saliendo con el 2º (verdad1con2=).
//  [18-oct] + MIRA_CARTERA=<dirección> (mira_cartera.js): al arrancar lee sus últimas MIRA_N transacciones por Helius y dice de dónde le llegan los tokens ([CARTERA]).
//  [18-oct] + LA MUESTRA DE CURVAS (curvas.js): 1 de cada 6 tokens que crea pump.fun (PumpPortal subscribeNewToken) y a las 24 h si migraron y hasta dónde
//    se llenó la curva (coins-v2, ath_market_cap): la parte que migra desde cada nivel de llenado, lo que falta para saber si compensa comprar en la curva
//    (CURVAS_MUESTRA=0 apaga; CURVAS_UNO_DE; GET /curvas).
//  [18-oct, noche] + LA SOMBRA REAL (real.js; Tony: «¿cómo sabemos lo que tenemos de latencia?» → «prepáralo todo»): con SOLANA_PRIVATE_KEY y
//    REAL_LOTE_SOL=0.01, cada compra y venta del 1er lote se manda también de verdad con 0,01 SOL (el papel sigue con LOTE_SOL) y se apunta
//    una línea [LATENCIA] por orden: espera en cola, ms hasta que la cadena la acepta, ms hasta confirmada, slots de retraso (slot_dec = el
//    último slot visto del token al decidir), SOL y tokens movidos de verdad (precio de llenado frente al del modelo, con pts= de [SOLREC]).
//    Cola por token (una compra no espera la venta de otro), prepara() al ver la migración, ⏱️ en [SALUD] y el panel. Sin clave no cambia nada.
//  [18-oct, noche] + EL 3er LOTE AL MOMENTUM, EN PAPEL (bloque LOTE3 tras lote2Valora): en [10-120] s, cuando el SOL neto que el server ve entrar en los
//    últimos 10 s llega a un umbral (varios a la vez: LOTE3_UMBRALES 12,16,20, cada uno su cuenta), un lote de LOTE3_SOL (de serie 0) que sale al ×1,5 o a
//    los 270 s ([SOLLOTE3], «3er lote» en [SALUD]). En la página (Helius, carteras normales, ≥ 20 SOL): 334 ops +19,4 SOL con 0,5, 11 días de 14.
//  [19-oct] + LÍNEAS LARGAS EN TROZOS ([PARTE …]: Railway corta a 81.920 caracteres; mete_registro_solana.py las junta) y, una vez, las que ya cortó, desde el
//    registro del disco · APAGADO DE VERDAD (railway.toml: node server.js + 20 s de drainingSeconds; antes cada despliegue perdía las cámaras abiertas)
//    · la sombra real vende al arrancar lo que quedó dentro de antes del reinicio.
//  [12-oct, noche] + EL RESCATE DENTRO DEL SERVER (rescate.js + rescate_trabajo.js): al cerrar cada cámara baja de Helius la curva de bonos,
//    los swaps de la piscina con cartera, la migración exacta y los extras (con presupuesto de créditos por día) → ficheros en el volumen
//    y una línea [SOLFICHA] con los rasgos; y TODO el registro se guarda también en el volumen, un fichero por día (no más huecos).
//    Se baja al PC con baja_del_server.bat. Rutas: /rescate, /rescate/indice, /rescate/f/… · En [SALUD]: 🧾 el rescate y 💳 el gasto de Helius.
//  [12-oct] + LAS CUENTAS DE VERDAD: precio con los 17,585 SOL que la migración deja aparte, comisión de PumpSwap por tramo (1,2-1,25 %),
//    impacto, orden 1 s tarde y 0,000515 SOL por transacción → [SOLVER], verdad= en [SOLREC], «DE VERDAD» en [SALUD] y en el panel.
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
import fs from "fs"; import path from "path"; import zlib from "zlib"; import readline from "readline"; import { fileURLToPath } from "url"; import * as REAL from "./real.js"; import * as RESCATE from "./rescate.js"; import * as CURVASM from "./curvas.js"; import * as MIRA from "./mira_cartera.js"; import { piscinaCanonica } from "./piscina.js";
const DIR = path.dirname(fileURLToPath(import.meta.url));
// ═══ [12-oct] EL RESCATE DENTRO DEL SERVER y EL REGISTRO EN EL DISCO (rescate.js): todo lo que sale por consola se guarda también en el volumen ═══
let RESC; try { RESC = RESCATE.crea({ log: (m) => console.log(m), gastoVivoDia: () => vivoDia() }); }   // el rescate usa lo que sobre del presupuesto de Helius
catch (e) { console.log(`⚠️ el rescate no arranca (${e && e.message}): el server sigue sin él`); RESC = RESCATE.nulo(); }   // pase lo que pase con el disco, el server arranca
// [19-oct] Railway CORTA cada línea a 81.920 caracteres: las [SOLREC] de los tokens más movidos perdían pts= (va al final) y no entraban en los datos.
//   Ahora una línea de más de LINEA_MAX caracteres sale por la consola en trozos «[PARTE <id> i/n de <ETIQUETA>( ts=<hora original>)] …» y
//   herramientas/mete_registro_solana.py los vuelve a juntar. En el disco del server (el registro de rescate.js) la línea sigue entera.
const LINEA_MAX = Math.max(200, +(process.env.LINEA_MAX || 75000)); let PARTE_N = 0;
function trozosDe(txt, ts) { const et = (String(txt).match(/^\s*\[([A-Za-z0-9_]+)\]/) || [])[1] || "LINEA"; const id = Date.now().toString(36) + (PARTE_N++ % 1296).toString(36).padStart(2, "0");
  const T = []; for (let i = 0; i < txt.length;) { let j = Math.min(txt.length, i + LINEA_MAX); if (j < txt.length) { const cc = txt.charCodeAt(j - 1); if (cc >= 0xD800 && cc <= 0xDBFF) j--; } T.push(txt.slice(i, j)); i = j; }   // sin partir un emoji por la mitad
  return T.map((t, i) => `[PARTE ${id} ${i + 1}/${T.length} de ${et}${ts ? " ts=" + ts : ""}] ${t}`); }
function consolaPartes(escribe, txt, ts) { const T = trozosDe(txt, ts); for (const t of T) escribe(t); return T.length; }
const ORIG = { log: console.log.bind(console), error: console.error.bind(console), warn: console.warn.bind(console) };
{ const orig = ORIG;
  for (const k of ["log", "error", "warn"]) console[k] = (...a) => { let txt = null; try { txt = a.map(x => typeof x === "string" ? x : x instanceof Error ? (x.stack || x.message) : (() => { try { return JSON.stringify(x); } catch { return String(x); } })()).join(" "); } catch {}
    if (txt != null && txt.length > LINEA_MAX) consolaPartes(orig[k], txt); else orig[k](...a); try { if (txt != null) RESC.linea(txt); } catch {} }; }
const W = new Function(fs.readFileSync(path.join(DIR, "motor.js"), "utf-8") + ";return {construyeMotor, netoReal};")();
const ESTRELLA2 = { v3Corte2: 60, mv: 25, maxvend: 1, v3Seg: 600, v3Corte: 60, soporteSigue: 35 };
let ESTRATEGIA = ESTRELLA2; try { if (process.env.ESTRATEGIA) ESTRATEGIA = JSON.parse(process.env.ESTRATEGIA); } catch (e) { console.log("⚠️ ESTRATEGIA no es un JSON válido: uso ⭐⭐"); }
// ═══ [7-oct] EL PLAN POR REGLAS (como en Pons): la primera regla que encaja decide; acc −1 = no entrar; cada cfg distinta tiene su motor ═══
//   de serie: PLAN_DE_SERIE (desde el 14-oct, «reloj 270 14-oct»; antes v4, v3, limpio, robusto + stop + vetos v2…). PLAN_JSON (JSON con {reglas:[…]}) para otro; PLAN=0 → una sola estrategia (ESTRATEGIA, de serie ⭐⭐)
const PLAN_DE_SERIE = {"nombre": "hoy + cazador 23-oct", "reglas": [{"ten": [0, 5], "seg": null, "top5": null, "horas": [], "acc": -1, "cfg": {}, "nota": "5 o menos tenedores (ballenas): no entrar"}, {"ten": null, "seg": null, "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 0, "maxlot": 1, "soporteSigue": 30, "escN": 20, "v3Corte": 0, "reloj": 270}, "nota": "todos: el limpio sin la bestia, UNA sola compra (hasta el 23-oct, 8) y fuera a los 270 s"}]};   // [23-oct] «hoy + cazador 23-oct": maxlot 8 → 1   // [14-oct] «reloj 270 14-oct» (Tony: «vender todo a los 270 s… como quieras», en papel)
//   [14-oct] EL RELOJ (mando «reloj», más abajo en motorDe): el limpio SIN la bestia y todo fuera a los 270 s desde la migración, sin volver a comprar.
//   En papel, en los 1.867 limpios que pasan el corte (3-14 oct, lote 0,1): +91,1 SOL frente a +58,3 del limpio con la bestia (3-10 oct +71,5 / +46,9;
//   11-14 oct, días que no se usaron para nada, +19,6 / +11,4; al precio de los 5 s +61,6 / +29,7; sin los 3 mejores +79,8 / +47,2). Eligiendo el segundo
//   con 10 días y mirando en los otros 2 (6 veces): sale 270 las 6 y gana las 6. Meseta de 200 a 330 s, salvo justo a los 300 s (+62,5: los bots que venden
//   a los 5 min); de 340 s en adelante se hunde. Gana en los monstruos (+66,6) y pierde algo en los rugs (−13,2). El v4 con el reloj: +88,9 / +15,9;
//   el v4 tal cual +107,2 / +8,2 (en los días nuevos, peor que el limpio). Este código, con su motor, da lo mismo que el laboratorio token a token (+91,09).
//   Para volver al v4: PLAN_JSON = datos/solana_plan_v4_PLAN_JSON.txt (está también en el LEEME). El v4 («v4 13-oct», del 13-oct 23:40 al 14-oct):
//   PLAN_V4_13 = {"nombre": "v4 13-oct", "reglas": [{"ten": [0, 5], "seg": null, "top5": null, "horas": [], "acc": -1, "cfg": {}, "nota": "5 o menos tenedores (ballenas): no entrar"}, {"ten": [301, 1000000000], "seg": null, "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 60, "mv": 25, "maxvend": 1, "v3Seg": 600, "v3Corte": 60, "soporteSigue": 30, "maxlot": 12, "addCae": 25, "tau": 25, "v3Salvo": 200, "slGana": 50, "mvSalvo": 60}, "nota": "más de 300 tenedores → v2 >300 + maxlot 12 + tau 25"}, {"ten": [151, 300], "seg": null, "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 40, "maxlot": 10, "soporteSigue": 30, "escN": 20, "v3Seg2": 180}, "nota": "151-300 tenedores → limpio · v3Seg2 180 + maxlot 10"}, {"ten": null, "seg": null, "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 60, "mv": 25, "maxvend": 1, "v3Seg": 125, "v3Corte": 60, "soporteSigue": 35, "maxlot": 6, "addCae": 25, "tau": 17, "v3Salvo": 200, "slGana": 50, "mvSalvo": 60, "tp": 1000, "obsSeg": 60}, "nota": "el resto (150 tenedores o menos) → ⭐⭐+stop · tp 1000 + v3Seg 125 + obsSeg 60"}]};
//   14 maneras de ajustar el plan, cada una examinada igual: elegir con 8 días y mirar en los otros 2 (cinco veces) y elegir con una mitad y mirar en la otra.
//   Gana «3 grupos por tenedores (>300, 151-300, el resto) y en cada uno el cambio que mejora el total Y al menos 6 de cada 10 días»: examinado +78,4 SOL
//   (honesto +55,5), el limpio +53,6 (+30,3) y el v3 +55,1. Con los 10 días: +114,3 (+80,6 | +33,7), honesto +89,6; el v3 daba +128,0 con esos mismos días,
//   pero examinado no ganaba al limpio (se ajustaba a los días con que se hizo). OJO: la ventaja del v4 viene sobre todo del 3-4 oct (el día de los monstruos:
//   aguanta más los de >300 tenedores); en los otros 8 días, +6,8 sobre el limpio, y en el 5-6 oct perdió con él (+6,7 frente a +11,0).
//   El veto de ≤5 tenedores no cambia nada en estos datos (las ballenas se vetan antes): va de seguro, como en el v3.
//   Para volver al v3: PLAN_JSON = datos/solana_plan_v3_PLAN_JSON.txt. El v3 («v3 13-oct», de 21:50 a 23:40):
//   PLAN_V3_13 = {"nombre": "v3 13-oct", "reglas": [{"ten": [0, 5], "seg": null, "top5": null, "horas": [], "acc": -1, "cfg": {}, "nota": "5 o menos tenedores (ballenas): no entrar"}, {"ten": null, "seg": null, "top5": null, "horas": [20, 21, 22, 23], "acc": 0, "cfg": {"v3Corte2": 40, "maxlot": 8, "soporteSigue": 30, "escN": 20, "v3Seg2": 180}, "nota": "de 20 a 24 h → limpio · v3Seg2 180"}, {"ten": [151, 300], "seg": null, "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 40, "maxlot": 12, "soporteSigue": 30, "escN": 20, "tau": 8}, "nota": "151-300 tenedores → limpio · maxlot 12 + tau 8"}, {"ten": [0, 25], "seg": null, "top5": null, "horas": [16, 17, 18, 19], "acc": 0, "cfg": {"v3Corte2": 40, "maxlot": 8, "soporteSigue": 30, "escN": 20, "md": 0}, "nota": "≤25 tenedores de 16 a 20 h → limpio · md 0"}, {"ten": [301, 1000000000], "seg": null, "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 60, "mv": 25, "maxvend": 1, "v3Seg": 600, "v3Corte": 60, "soporteSigue": 25, "maxlot": 12, "addCae": 25, "tau": 17, "v3Salvo": 200, "slGana": 50, "mvSalvo": 60}, "nota": "más de 300 tenedores → v2 >300 + maxlot 12 + soporteSigue 25"}, {"ten": [6, 25], "seg": null, "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 60, "mv": 25, "maxvend": 1, "v3Seg": 60, "v3Corte": 60, "soporteSigue": 35, "maxlot": 6, "addCae": 25, "tau": 3, "v3Salvo": 200, "slGana": 50, "mvSalvo": 60}, "nota": "6-25 tenedores → v2 1-10 min"}, {"ten": null, "seg": [61, 600], "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 40, "maxlot": 8, "soporteSigue": 30, "escN": 20, "v3Seg": 125, "ndSeg": 120, "ndPct": -10, "addCae": 25}, "nota": "migra en 1-10 min → limpio · v3Seg 125 + ndSeg 120 ndPct -10 + addCae 25"}, {"ten": null, "seg": [601, 3600], "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 40, "maxlot": 8, "soporteSigue": 30, "escN": 20, "v3Seg2": 600}, "nota": "migra en 10-60 min → limpio · v3Seg2 600"}, {"ten": null, "seg": [3601, 1000000000000000], "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 60, "mv": 25, "maxvend": 1, "v3Seg": 600, "v3Corte": 60, "soporteSigue": 35, "maxlot": 6, "addCae": 25, "tau": 17, "v3Salvo": 200, "slGana": 50, "mvSalvo": 60, "obsSeg": 60, "escN": 20}, "nota": "migra en más de 1 h → ⭐⭐+stop · obsSeg 60 + escN 20"}, {"ten": null, "seg": null, "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 40, "maxlot": 8, "soporteSigue": 30, "escN": 20, "mv": 20, "addSube": 100}, "nota": "el resto → limpio · mv 20 + addSube 100"}]};
//   El v2 del 7-oct rehecho con los 2.661 limpios del 3 al 12-oct (sin ballenas ni bundles; el corte de −2 % va antes): en cada grupo del v2, veto o la configuración
//   (de 171 probadas + hasta 2 mandos más) que mejora las DOS mitades (3-7 y 8-12 oct). En papel, con el corte: +128,0 SOL (+89,0 | +38,9), el limpio +53,6.
//   OJO: eso es con los mismos días con que se eligió. Examinado (elegido con 8 días y mirado en los otros 2, cinco veces): +55,1, el limpio +53,6 → como el limpio.
//   Para volver al limpio: PLAN_JSON = datos/solana_plan_limpio13_PLAN_JSON.txt. Antes («limpio 13-oct»):
//   PLAN_LIMPIO_13 = {"nombre": "limpio 13-oct", "reglas": [{"ten": [0, 5], "seg": null, "top5": null, "horas": [], "acc": -1, "cfg": {}, "nota": "5 o menos tenedores (creador): no entrar"}, {"ten": null, "seg": null, "top5": null, "horas": [], "acc": 0, "cfg": {"v3Corte2": 40, "maxlot": 8, "soporteSigue": 30, "escN": 20}, "nota": "todos: v3 a los 5 min 40, 8 lotes, seguir la curva 30, escalera 20"}]};
//   [13-oct, 18:45] SIN LOS VETOS DE 20 A 24 H Y DE MIGRAR EN 6-60 S (Tony: «sí», a quitarlos): en papel, después del corte, quitaban 550 limpios que daban +8,1 SOL
//   (los de 20 a 24 h, +9,2, y en las dos mitades). El «limpio 8-oct» los llevaba: PLAN_JSON con sus 4 reglas para volver a ponerlos.
let PLAN = process.env.PLAN === "0" ? null : PLAN_DE_SERIE; try { if (process.env.PLAN_JSON) PLAN = { nombre: "PLAN_JSON", ...JSON.parse(process.env.PLAN_JSON) }; } catch (e) { console.log("⚠️ PLAN_JSON no es un JSON válido: uso el plan de serie"); }
const MOTORES = new Map();
// [18-oct] al pedir una orden real se le pasa lo que el server sabe en ese instante: el último slot visto del token (slot_dec de [LATENCIA]), el precio absoluto de migrar y el segundo del token
const EXTRAS = { encola: (tipo, rec, datos) => { const c = rec && (S.camaras.get(rec.token) || CERRADAS.get(rec.token));
  const pc = () => { const q = c && c.puntos && c.puntos.length ? c.puntos[c.puntos.length - 1] : null; return q ? 1 + q.p / 100 : null; };   // [19-oct] el precio de la CÁMARA (el del motor iba un punto por detrás y en la 1ª compra era siempre 1,0)
  REAL.encola(tipo, rec, { ...(datos || {}), slot: c ? c.ultSlot ?? null : null, precioIni: c ? c.precioIni : null, tTok: c ? (Date.now() - c.t0) / 1000 : (rec && rec.puntos && rec.puntos.length ? rec.puntos[rec.puntos.length - 1].t : null), pDec: pc(), precioAhora: pc }); } };
// ═══ [14-oct] EL RELOJ · mando «reloj» en el cfg de una regla (o en ESTRATEGIA): en el primer punto en o después de «reloj» segundos desde la migración se vende
//   todo y el token se da por muerto, y desde ese segundo no se compra nada más (aunque no hubiera nada dentro). Es el «cortaEn(T)» del laboratorio
//   (herramientas/solana/nuevo/pruebas/flujo/reloj270.js) por los ganchos POLITICA del motor (veta = no comprar · sale 2 = vender todo y no volver · ajusta = cada
//   punto), pero con la marca en el token (u.relojHecho) y no en una variable suelta, porque aquí un mismo motor lleva muchos tokens a la vez.
//   Sin «reloj» (o 0) el motor es exactamente el de antes (sin POLITICA). El motor apunta esas ventas como "NEURONA": en las cuentas salen como "RELOJ".
function politicaReloj(T) {
  return {
    veta: (rec, u, t) => !!u.relojHecho || t >= T,
    sale: (rec, u, t) => { if (!u.relojHecho && t >= T) { u.relojHecho = true; return 2; } return 0; },
    ajusta: (rec, u, t) => { if (!u.relojHecho && t >= T) u.relojHecho = true; },
  };
}
function motorDe(cfg) { const k = JSON.stringify(cfg); if (!MOTORES.has(k)) { const { reloj, ...resto } = cfg || {}; const T = +reloj > 0 ? +reloj : 0;
  const m = W.construyeMotor({ cazDesde: 1e9, ...resto }, T ? { ...EXTRAS, POLITICA: politicaReloj(T) } : EXTRAS); if (!m) { console.log("❌ el motor no arranca. Me paro."); process.exit(1); }
  m.reloj = T; if (T) console.log(`⏱️ motor con reloj: todo fuera a los ${T} s y no volver (${JSON.stringify(resto)})`); MOTORES.set(k, m); } return MOTORES.get(k); }
const MOTOR = motorDe(ESTRATEGIA);   // el de ESTRATEGIA (⭐⭐): para cuando no hay plan o ninguna regla encaja
const horaEsp = (ms) => +new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", hour: "2-digit", hourCycle: "h23" }).format(new Date(ms));
function reglaDe(c) { if (!PLAN) return null; const n = c.nac || {}; const seg = n.seg, ten = n.compradores, top5 = n.top5, hg = horaEsp(c.t0);
  for (let i = 0; i < PLAN.reglas.length; i++) { const r = PLAN.reglas[i]; if (r.ten && !(ten != null && ten >= r.ten[0] && ten <= r.ten[1])) continue; if (r.seg && !(seg != null && seg >= r.seg[0] && seg <= r.seg[1])) continue;
    if (r.top5 && !(top5 != null && top5 >= r.top5[0] && top5 <= r.top5[1])) continue; if (r.horas && r.horas.length && !r.horas.includes(hg)) continue; return { i, r }; } return null; }
const LOTE_SOL = +(process.env.LOTE_SOL || 0.1), K = LOTE_SOL / 0.01;   // el motor cuenta en lotes de 0,01: se escala
// ═══ [12-oct] LO DE VERDAD: lo que se ganaría operando de verdad en PumpSwap (las cuentas «real» de antes eran demasiado optimistas) ═══
//  · el precio: la piscina pone precio con su cuenta de wSOL + 17,585 SOL que la migración deja aparte (medido swap a swap en 20 piscinas y en las
//    transacciones crudas). Este server graba wSOL/tokens (se deja así para que las curvas sigan siendo comparables); lo de verdad se calcula aparte.
//  · la comisión de PumpSwap por capitalización de verdad (1,25 % por debajo de 420 SOL, 1,2 % de 420 a 1.470, … 0,3 % por encima de 98.240), por lado
//  · el impacto por tamaño con la reserva de verdad · la orden llenada RETRASO_S después de la señal · TX_SOL por transacción (prioridad + firma + propina)
const RESERVA_OCULTA = +(process.env.RESERVA_OCULTA_SOL || 17.584505289), K_MIG = (67.405855 + RESERVA_OCULTA) * 206.9e6;
const RETRASO_S = +(process.env.RETRASO_S || 1), TX_SOL = +(process.env.TX_SOL || 0.000515);
const TRAMOS_PS = [[420, 1.25], [1470, 1.2], [2460, 1.15], [3440, 1.1], [4420, 1.05], [9820, 1.0], [14740, 0.95], [19650, 0.9], [24560, 0.85], [29470, 0.8], [34380, 0.75], [39300, 0.7], [44210, 0.65], [49120, 0.6], [54030, 0.55], [58940, 0.525], [63860, 0.5], [68770, 0.475], [73681, 0.45], [78590, 0.425], [83500, 0.4], [88400, 0.375], [93330, 0.35], [98240, 0.325], [Infinity, 0.3]];
const comisionPS = (mc) => { for (const [h, f] of TRAMOS_PS) if (mc < h) return f; return 0.3; };
function precioVerdad(f, r0) { const r = Math.max(f, 1e-9) * r0, C = RESERVA_OCULTA; const rt = (-C + Math.sqrt(C * C + 4 * r * K_MIG)) / (2 * r), rt0 = (-C + Math.sqrt(C * C + 4 * r0 * K_MIG)) / (2 * r0);
  const pi = r + C / rt, pi0 = r0 + C / rt0; return { phi: pi / pi0, S: r * rt + C, mc: pi * 1e9 }; }
function precioEnPts(P, t) { let v = P.length ? 1 + P[0].p / 100 : 1; for (const q of P) { if (q.t > t) break; v = 1 + q.p / 100; } return v; }
function netoVerdad(q, P, r0) { r0 = r0 > 0 ? r0 : 3.2579e-7; const comp = q.compras && q.compras.length ? q.compras : [{ t: 0, p: 0, precio: q.entrada }]; let u = 0;
  for (const b of comp) { const tb = (b.t === 0 && b.p === 0) ? 3 + RETRASO_S : b.t + RETRASO_S; const v = precioVerdad(precioEnPts(P, tb), r0); const x = LOTE_SOL * (1 - comisionPS(v.mc) / 100); u += x / (v.phi * (1 + x / v.S)); }
  const v = precioVerdad(precioEnPts(P, q.tC + RETRASO_S), r0); const bruto = u * v.phi; const neto = bruto * (1 - Math.min(0.99, bruto / v.S)) * (1 - comisionPS(v.mc) / 100);
  return Math.max(-LOTE_SOL * comp.length, neto - LOTE_SOL * comp.length) - TX_SOL * (comp.length + 1); }
function valoraPendientes(c, forzar) { if (!c.pendVer || !c.pendVer.length || !c.rec) return; const P0 = c.rec.puntos; const tUlt = P0.length ? P0[P0.length - 1].t : 0; const quedan = [];
  for (const q of c.pendVer) { if (!forzar && tUlt < q.tC + RETRASO_S) { quedan.push(q); continue; }
    const v = netoVerdad(q, P0, c.precioIni); P.verdad = (P.verdad || 0) + v; P.verdadN = (P.verdadN || 0) + 1; if (v > 0) P.verdadGan = (P.verdadGan || 0) + 1; c.verdad = (c.verdad || 0) + v; alGrafico(v, "1"); apuntaSuma(c, "l1", v);
    log(`[SOLVER] sym=${c.sym} mint=${c.mint} motivo=${q.motivo} lotes=${(q.compras || []).length || 1} verdad=${v.toFixed(4)}SOL acum_verdad=${P.verdad.toFixed(3)}SOL`); }
  c.pendVer = quedan; }
// ═══ [18-oct] EL 2º LOTE A LOS 60 s (papel) ═══
//  Lo que el server mide él mismo en el 1er minuto (c.min1: SOL que entra y swaps, por WebSocket; ve ~1/3 de los swaps y ~57 % del SOL que ve Helius,
//  por eso los umbrales son los suyos: NUEVO/vivo/lote2_server.py sobre 1.934 tokens del 11 al 18-oct → entra ≥ 100 y ≤ 120 swaps: 37 ops +82 SOL con 9).
//  Se decide en el primer punto desde los LOTE2_T s (llena RETRASO_S después), sale al ×LOTE2_OBJ sobre su precio de entrada o a los LOTE2_RELOJ s.
const LOTE2_SOL = +(process.env.LOTE2_SOL || 0), LOTE2_T = +(process.env.LOTE2_T || 60), LOTE2_ENTRA = +(process.env.LOTE2_ENTRA || 100), LOTE2_SWAPS = +(process.env.LOTE2_SWAPS || 120);
const LOTE2_OBJ = +(process.env.LOTE2_OBJ || 2), LOTE2_RELOJ = +(process.env.LOTE2_RELOJ || 270);
// [23-oct] el 2º lote y el cazador también en los tokens que el 1er lote no compra por el precio a los 3 s («nosube», «cae»; se graban igual): así es
//   «Hoy» en las pruebas (14 → 23-oct 12:14, sin bundles ni ballenas, 2º lote a 1 SOL: +14,8 en 59 tokens, sin las 3 mejores +10,8, frente a +10,8 en 31
//   solo con los del corte; NUEVO/cazador/final.py). Ballenas, bundles, piscinas raras,
//   «no es PumpSwap», sin nacimiento y ≤ 5 tenedores siguen fuera. LOTES_SIN_CORTE=0 vuelve a lo de antes (solo con el 1er lote).
const LOTES_SIN_CORTE = process.env.LOTES_SIN_CORTE !== "0";
const lotesPuede = (c) => (!!c.rec && !c.veto) || (LOTES_SIN_CORTE && (c.veto === "nosube" || c.veto === "cae") && !reglaNoEntra(c));
const reglaNoEntra = (c) => c.reglaNo ?? (c.reglaNo = (() => { const rg = reglaDe(c); return !!(rg && rg.r.acc < 0); })());   // la regla 1 (≤ 5 tenedores) se mira después del corte
// [23-oct] lo de cada token en el diario del panel (1er lote, 2º, 3er lote, cazador) y la gráfica con TODO lo de verdad (antes solo el 1er lote)
const ULTVER_MAX = 2000;
function alGrafico(v, k) { (P.ultVer = P.ultVer || []).unshift({ t: Date.now(), v: +v.toFixed(4), k }); if (P.ultVer.length > ULTVER_MAX) P.ultVer.length = ULTVER_MAX; }
function apuntaSuma(c, campo, v) { const d = DIARIO.get(c.mint); if (!d) return; const a = d[campo] || (d[campo] = { n: 0, pnl: 0 }); a.n++; a.pnl = +(a.pnl + v).toFixed(4); }
// una operación con la piscina de verdad, como bancoOpera en la página: compra decidida en te (llena en te+RETRASO_S), vende en tx (llena en tx+RETRASO_S), nuestro SOL dentro
function lote2Opera(P0, r0, te, tx, L) { const tfin = P0.length ? P0[P0.length - 1].t : 0; const pis = (t) => { const v = precioVerdad(precioEnPts(P0, Math.min(t, tfin)), r0); return { S: v.S, RT: K_MIG / v.S, mc: v.mc }; };
  const a = pis(te + RETRASO_S), fee = comisionPS(a.mc) / 100, x = L * (1 - fee), u = a.RT * x / (a.S + x);
  const b = pis(tx + RETRASO_S), s2 = b.S + x, rt2 = K_MIG / s2, bruto = s2 * u / (rt2 + u), neto = bruto * (1 - comisionPS(b.mc) / 100);
  return { pnl: neto - L - 2 * TX_SOL, pe: precioEnPts(P0, Math.min(te + RETRASO_S, tfin)), px: precioEnPts(P0, Math.min(tx + RETRASO_S, tfin)) }; }
function lote2Mira(c, t, p) { if (!(LOTE2_SOL > 0) || c.fin || !lotesPuede(c)) return;
  if (!c.lote2) { if (t < LOTE2_T || c.lote2Visto) return; c.lote2Visto = true; if (t > LOTE2_T + 15) { c.lote2No = "tarde"; return; } const m1 = c.min1 || {}; const entra = +(m1.solIn || 0), swaps = +(m1.swaps || 0);
    if (!(entra >= LOTE2_ENTRA) || swaps > LOTE2_SWAPS) { c.lote2No = `entra ${entra.toFixed(1)} swaps ${swaps}`; return; }
    c.lote2 = { te: t, pe: null, x: LOTE2_SOL, entra: +entra.toFixed(1), swaps }; P.lote2Abiertas = (P.lote2Abiertas || 0) + 1;   // pe: el precio al llenar (RETRASO_S después), como el banco
    log(`[SOLLOTE2] sym=${c.sym} mint=${c.mint} abre t=${Math.round(t)} entra=${entra.toFixed(1)}SOL swaps=${swaps} compradores=${m1.compradores ? m1.compradores.size : "-"} p=${(1 + p / 100).toFixed(4)} lote=${LOTE2_SOL}`); return; }
  const l = c.lote2; if (l.tx != null) { lote2Valora(c, false); return; }
  const pr = 1 + p / 100; if (l.pe == null) { if (t < l.te + RETRASO_S) return; l.pe = precioEnPts(c.puntos, l.te + RETRASO_S); if (t <= l.te + RETRASO_S) return; }   // [18-oct, noche] llena al precio del segundo te+1 (precioEnPts, como el banco); el punto de ese segundo no es candidato a salir, el siguiente sí (antes se saltaba el primer punto posterior)
  if (LOTE2_OBJ > 0 && pr >= l.pe * LOTE2_OBJ) { l.tx = t; l.por = "objetivo"; } else if (LOTE2_RELOJ > 0 && t - l.te >= LOTE2_RELOJ) { l.tx = t; l.por = "tiempo"; }
  if (l.tx != null) lote2Valora(c, false); }
// se valora cuando ya hay un punto RETRASO_S después de la venta (o al cerrar la cámara, forzando)
function lote2Valora(c, forzar) { const l = c.lote2; if (!l || l.tx == null || l.hecho) return; const P0 = c.puntos; const tUlt = P0.length ? P0[P0.length - 1].t : 0; if (!forzar && tUlt < l.tx + RETRASO_S) return;
  l.hecho = true; const v = lote2Opera(P0, c.precioIni, l.te, l.tx, l.x); l.pe = l.pe ?? v.pe; const v1 = lote2Opera(P0, c.precioIni, 3, l.tx, LOTE_SOL);   // y el 1er lote (3,5 s → llena a los 4) si hubiera salido con el 2º
  P.lote2 = (P.lote2 || 0) + v.pnl; P.lote2N = (P.lote2N || 0) + 1; if (v.pnl > 0) P.lote2Gan = (P.lote2Gan || 0) + 1; P.lote2Abiertas = Math.max(0, (P.lote2Abiertas || 0) - 1); P.lote2Con1 = (P.lote2Con1 || 0) + v1.pnl; c.lote2Verdad = v.pnl;
  alGrafico(v.pnl, "2"); apuntaSuma(c, "l2", v.pnl);   // [23-oct] al panel
  log(`[SOLLOTE2] sym=${c.sym} mint=${c.mint} cierra motivo=${l.por} dentro=${Math.round(l.tx - l.te)}s pe=${v.pe.toFixed(4)} px=${v.px.toFixed(4)} entra=${l.entra} swaps=${l.swaps} lote=${l.x} verdad=${v.pnl.toFixed(4)}SOL acum_lote2=${P.lote2.toFixed(3)}SOL verdad1con2=${v1.pnl.toFixed(4)}SOL`); }
// ═══ [18-oct, noche] EL 3er LOTE: AL MOMENTUM (papel) · Tony: «prepáralo todo» ═══
//  Lo que hacen las carteras que ganan en la piscina (GoXL33/HkUarW): no entrar a un segundo fijo sino cuando está entrando dinero. En la página (sim.py
//  sobre Helius, 4.550 tokens 3-17 oct): entrar en [10-120] s cuando en los últimos 10 s han entrado ≥ 20 SOL netos de carteras normales, con el corte,
//  salir al ×1,5 sobre el precio de llenado o a los 270 s: lote 0,5 → 334 ops +19,4 SOL (65 % ganan, 11/14 días, peor −0,2; 3-9 +13,5 · 10-14 +5,7 ·
//  15-17 +0,3), independiente del 2º lote (275 de 334 fuera de él). El server ve ~57 % del SOL y no distingue carteras (NUEVO/vivo/lote3_server.py sobre
//  los 420 tokens con ptx del 14 al 18-oct: umbrales 12-20 cerca de cero, 5 días no bastan para elegir) → se llevan VARIOS UMBRALES A LA VEZ
//  (LOTE3_UMBRALES, de serie 12,16,20), cada uno con su cuenta, y lo vivo decide. Flujo = suma del SOL neto de los puntos (s: entra − sale, todas las
//  carteras) de los últimos LOTE3_VENTANA s. Una operación por umbral y token, decidida en el punto en que el flujo llega al umbral (llena RETRASO_S después),
//  sale al ×LOTE3_OBJ sobre su precio de llenado o a los LOTE3_RELOJ s. [SOLLOTE3] abre/cierra · «3er lote» en [SALUD]. De serie apagado (LOTE3_SOL=0).
const LOTE3_SOL = +(process.env.LOTE3_SOL || 0), LOTE3_UMBRALES = (process.env.LOTE3_UMBRALES || "12,16,20").split(",").map(Number).filter(x => x > 0);
const LOTE3_DESDE = +(process.env.LOTE3_DESDE || 10), LOTE3_HASTA = +(process.env.LOTE3_HASTA || 120), LOTE3_VENTANA = +(process.env.LOTE3_VENTANA || 10), LOTE3_OBJ = +(process.env.LOTE3_OBJ || 1.5), LOTE3_RELOJ = +(process.env.LOTE3_RELOJ || 270);
function lote3Flujo(c, t) { let s = 0; for (let i = c.puntos.length - 1; i >= 0; i--) { const q = c.puntos[i]; if (q.t <= t - LOTE3_VENTANA) break; s += q.s || 0; } return s; }
function lote3Mira(c, t, p) { if (!(LOTE3_SOL > 0) || c.fin || c.veto || !c.rec) return; const pr = 1 + p / 100; const L = c.lote3 || (c.lote3 = {}); let flujo = null;
  for (const u of LOTE3_UMBRALES) { const l = L[u];
    if (!l) { if (t < LOTE3_DESDE || t > LOTE3_HASTA) continue; if (flujo == null) flujo = lote3Flujo(c, t); if (flujo < u) continue;
      L[u] = { te: t, pe: null, x: LOTE3_SOL, flujo: +flujo.toFixed(2), u }; P.lote3Abiertas = (P.lote3Abiertas || 0) + 1;
      log(`[SOLLOTE3] sym=${c.sym} mint=${c.mint} umbral=${u} abre t=${t.toFixed(1)} flujo=${flujo.toFixed(2)}SOL p=${pr.toFixed(4)} lote=${LOTE3_SOL}`); continue; }
    if (l.tx != null) { lote3Valora(c, l, false); continue; }
    if (l.pe == null) { if (t < l.te + RETRASO_S) continue; l.pe = precioEnPts(c.puntos, l.te + RETRASO_S); if (t <= l.te + RETRASO_S) continue; }   // llena al precio del segundo te+1 (como el banco); el punto de ese segundo no es candidato a salir, el siguiente sí
    if (LOTE3_OBJ > 0 && pr >= l.pe * LOTE3_OBJ) { l.tx = t; l.por = "objetivo"; } else if (LOTE3_RELOJ > 0 && t - l.te >= LOTE3_RELOJ) { l.tx = t; l.por = "tiempo"; }
    if (l.tx != null) lote3Valora(c, l, false); } }
function lote3Valora(c, l, forzar) { if (!l || l.tx == null || l.hecho) return; const P0 = c.puntos; const tUlt = P0.length ? P0[P0.length - 1].t : 0; if (!forzar && tUlt < l.tx + RETRASO_S) return;
  l.hecho = true; const v = lote2Opera(P0, c.precioIni, l.te, l.tx, l.x); const A = P.lote3 || (P.lote3 = {}); const a = A[l.u] || (A[l.u] = { n: 0, gan: 0, pnl: 0 }); a.n++; if (v.pnl > 0) a.gan++; a.pnl += v.pnl; P.lote3Abiertas = Math.max(0, (P.lote3Abiertas || 0) - 1); l.verdad = v.pnl;
  alGrafico(v.pnl, "3"); apuntaSuma(c, "l3", v.pnl);   // [23-oct] al panel
  log(`[SOLLOTE3] sym=${c.sym} mint=${c.mint} umbral=${l.u} cierra motivo=${l.por} te=${l.te.toFixed(1)} dentro=${Math.round(l.tx - l.te)}s pe=${v.pe.toFixed(4)} px=${v.px.toFixed(4)} flujo=${l.flujo} lote=${l.x} verdad=${v.pnl.toFixed(4)}SOL acum_umbral=${a.pnl.toFixed(3)}SOL n=${a.n}`); }
function lote3Cierra(c) { if (!c.lote3) return; for (const l of Object.values(c.lote3)) { if (l.hecho) continue; if (l.tx == null && c.puntos.length) { l.tx = Math.max(l.te + RETRASO_S, c.puntos[c.puntos.length - 1].t); l.por = "fin"; } lote3Valora(c, l, true); } }
const lote3Txt = () => { const A = P.lote3 || {}; return LOTE3_UMBRALES.map(u => { const a = A[u] || { n: 0, gan: 0, pnl: 0 }; return `≥${u}: ${a.n} ops (${a.gan} ganadas) ${a.pnl >= 0 ? "+" : ""}${a.pnl.toFixed(3)}`; }).join(" · "); };
// ═══ [23-oct] EL CAZADOR (papel) · Tony: «pon en el server la de hoy + cazador y lánzalo» ═══
//  Cuando el 2º lote ya está dentro, si el token llega a ×2 sobre el precio de migrar y siguen entrando SOL netos, compra CAZA_SOL repartido en mini-lotes,
//  uno por combinación probada (las 10 más parejas del 3-13, NUEVO/cazador/tarde_congelada.json, sin el filtro al nacer): entra en el primer punto en
//  [10, hasta] s con el precio ≥ ×X y los SOL netos de los últimos W s ≥ F (como el flujo del 3er lote: la s de los puntos con t > t−W); sale cuando el
//  precio cae `trail` desde su máximo desde que llenó, cuando los SOL netos de los últimos 30 s llegan a −G (desde 30 s después de comprar) o a los H s, y
//  si la cámara se cierra antes, al final. OJO: la prueba (cazador/sin_filtro.py: +12,67 en 54 tokens) tenía una FUGA: el flujo iba en tramos de 5 s (15 s
//  desde los 15 min) y cada tramo sumaba ya lo de sus segundos siguientes → la salida por ventas veía hasta 4-14 s del futuro (cazador/caza_diag3.js: con
//  la fuga se reproduce la prueba, +6,80 / +6,75; sin ella +3,6). Con ESTE código sobre las curvas del server (cazador/caza_vivo.js + final.py, 14 → 23-oct,
//  sin bundles): +4,59 SOL en 48 tokens, sin las 3 mejores −2,39, 5 de 11 tandas en verde → en papel, para verlo en vivo; no pasa los criterios.
//  [SOLCAZA] abre/cierra · «cazador» en [SALUD].
const CAZA_SOL = +(process.env.CAZA_SOL ?? 1);
const CAZA_COMBOS = [   // W s · F SOL netos · ×X · hasta s · trail · G SOL de ventas netas en 30 s (null = sin esa salida) · H s
  { W: 60, F: 30, X: 2, hasta: 600, trail: 0.6, G: 30, H: 3600 }, { W: 60, F: 30, X: 2, hasta: 1800, trail: 0.6, G: 30, H: 3600 },
  { W: 30, F: 10, X: 2, hasta: 1800, trail: 0.6, G: 30, H: 3600 }, { W: 60, F: 30, X: 2, hasta: 600, trail: 0.6, G: 30, H: 1800 },
  { W: 60, F: 30, X: 2, hasta: 1800, trail: 0.6, G: 30, H: 1800 }, { W: 60, F: 10, X: 2, hasta: 1800, trail: 0.6, G: 30, H: 3600 },
  { W: 30, F: 30, X: 2, hasta: 600, trail: 0.6, G: 30, H: 1800 }, { W: 30, F: 10, X: 2, hasta: 1800, trail: 0.6, G: 30, H: 1800 },
  { W: 60, F: 30, X: 2, hasta: 1800, trail: 0.6, G: null, H: 600 }, { W: 60, F: 30, X: 2, hasta: 1800, trail: 0.4, G: 30, H: 3600 }];
function cazaFlujo(c, t, W) { let s = 0; for (let i = c.puntos.length - 1; i >= 0; i--) { const q = c.puntos[i]; if (q.t <= t - W) break; s += q.s || 0; } return s; }
function cazaMira(c, t, p) { if (!(CAZA_SOL > 0) || c.fin || t < 10 || !lotesPuede(c)) return; const pr = 1 + p / 100; const fl = {}; const flujo = (W) => fl[W] ?? (fl[W] = cazaFlujo(c, t, W));
  for (let i = 0; i < CAZA_COMBOS.length; i++) { const k = CAZA_COMBOS[i]; const l = c.caza && c.caza[i];
    if (!l) { if (t > k.hasta || pr < k.X || flujo(k.W) < k.F) continue; const L = c.caza || (c.caza = []);
      // la PRIMERA vez que se cumple: si el 2º lote aún no está dentro, esta combinación ya no entra en este token (así se probó: «confirmado por Hoy»)
      if (!(c.lote2 && c.lote2.te < t)) { L[i] = { i, te: t, hecho: true, sinConfirmar: true }; continue; }
      L[i] = { i, te: t, pe: null, pk: null, x: CAZA_SOL / CAZA_COMBOS.length, flujo: +flujo(k.W).toFixed(2) }; P.cazaAbiertas = (P.cazaAbiertas || 0) + 1;
      if (!c.cazaContado) { c.cazaContado = true; P.cazaTokens = (P.cazaTokens || 0) + 1; }
      log(`[SOLCAZA] sym=${c.sym} mint=${c.mint} combo=${i + 1} abre t=${t.toFixed(1)} flujo${k.W}=${flujo(k.W).toFixed(2)}SOL p=${pr.toFixed(4)} lote2=${Math.round(c.lote2.te)}s lote=${L[i].x}`); continue; }
    if (l.hecho) continue;
    if (l.tx != null) { cazaValora(c, l, false); continue; }
    if (l.pe == null) { if (t < l.te + RETRASO_S) continue; l.pe = precioEnPts(c.puntos, l.te + RETRASO_S); l.pk = l.pe; if (t <= l.te + RETRASO_S) continue; }   // llena en te+1 (como el banco); ese punto no es candidato a salir
    if (pr > l.pk) l.pk = pr;
    if (pr <= l.pk * (1 - k.trail)) { l.tx = t; l.por = "cae"; } else if (k.G != null && t >= l.te + 30 && flujo(30) <= -k.G) { l.tx = t; l.por = "ventas"; } else if (t - l.te >= k.H) { l.tx = t; l.por = "tiempo"; }
    if (l.tx != null) cazaValora(c, l, false); } }
function cazaValora(c, l, forzar) { if (!l || l.tx == null || l.hecho) return; const P0 = c.puntos; const tUlt = P0.length ? P0[P0.length - 1].t : 0; if (!forzar && tUlt < l.tx + RETRASO_S) return;
  l.hecho = true; const v = lote2Opera(P0, c.precioIni, l.te, l.tx, l.x); const A = P.caza || (P.caza = { n: 0, gan: 0, pnl: 0 }); A.n++; if (v.pnl > 0) A.gan++; A.pnl += v.pnl; P.cazaAbiertas = Math.max(0, (P.cazaAbiertas || 0) - 1); l.verdad = v.pnl;
  alGrafico(v.pnl, "caza"); apuntaSuma(c, "caza", v.pnl);
  log(`[SOLCAZA] sym=${c.sym} mint=${c.mint} combo=${l.i + 1} cierra motivo=${l.por} te=${l.te.toFixed(1)} dentro=${Math.round(l.tx - l.te)}s pe=${v.pe.toFixed(4)} px=${v.px.toFixed(4)} maximo=${(l.pk || v.pe).toFixed(4)} flujo=${l.flujo} lote=${l.x} verdad=${v.pnl.toFixed(4)}SOL acum_caza=${A.pnl.toFixed(3)}SOL n=${A.n}`); }
function cazaCierra(c) { if (!c.caza) return; for (const l of c.caza) { if (!l || l.hecho) continue; if (l.tx == null && c.puntos.length) { l.tx = Math.max(l.te + RETRASO_S, c.puntos[c.puntos.length - 1].t); l.por = "fin"; } cazaValora(c, l, true); } }
const cazaTxt = () => { const A = P.caza || { n: 0, gan: 0, pnl: 0 }; return `${P.cazaTokens || 0} tokens · ${A.n} mini-lotes cerrados (${A.gan} ganados) · abiertos ${P.cazaAbiertas || 0} · de verdad ${A.pnl >= 0 ? "+" : ""}${A.pnl.toFixed(3)} SOL`; };
const NAC_ESPERA_S = +(process.env.NAC_ESPERA_S || 10), VETO_BALLENA = process.env.VETO_BALLENA !== "0";
const VETO_BUNDLE = process.env.VETO_BUNDLE !== "0", VETO_SALTO = +(process.env.VETO_SALTO || 2), NAC_REINTENTO_MS = +(process.env.NAC_REINTENTO_MS || 2000);   // [13-oct]
const GRABA_VETADAS = process.env.GRABA_VETADAS === "1", GRABA_MUESTRA = +(process.env.GRABA_MUESTRA ?? 20);   // [13-oct] las ballenas y bundles vetados no se graban (1 de cada 20 sí)
// [13-oct] no comprar si a los 3 s el precio de verdad ya está un X % por debajo del de migrar (de serie 2; «no» = comprar siempre; 0 = en cuanto esté por debajo)
const NO_COMPRA_SI_CAE = (() => { const v = String(process.env.NO_COMPRA_SI_CAE ?? "2").trim().replace(",", "."); if (/^(no|off|)$/i.test(v)) return null; return Number.isFinite(+v) && +v >= 0 ? +v : 2; })();
// [16-oct] no comprar si a los 3 s el precio que graba no ha subido al menos un X % sobre el de migrar (de serie apagado: vacío o «no»; 0 = que no esté por debajo)
const NO_COMPRA_SI_NO_SUBE = (() => { const v = String(process.env.NO_COMPRA_SI_NO_SUBE ?? "").trim().replace(",", "."); if (/^(no|off|)$/i.test(v)) return null; return Number.isFinite(+v) && +v >= 0 && +v <= 100 ? +v : null; })();
const PISCINA_MIN = +(process.env.PISCINA_MIN || 100), PISCINA_MAX = +(process.env.PISCINA_MAX || 5000);
const SOLO_PUMPSWAP = process.env.SOLO_PUMPSWAP !== "0";   // [11-oct] de serie solo se operan piscinas de PumpSwap   // [10-oct] SOL de MC al migrar: fuera de [100, 5000] no se opera
const ESTADO_FILE = process.env.ESTADO_FILE || (fs.existsSync("/data") ? "/data/solana_papel.json" : "/tmp/solana_papel.json");
const P = { papel: 0, real: 0, verdad: 0, verdadN: 0, verdadGan: 0, cerradas: 0, ganadas: 0, abiertas: 0, entradas: 0, vetadas: 0, sinNac: 0, porVeto: {}, ultimas: [], desde: Date.now(), porMotivo: {}, maxExpuesto: 0 };
// ═══ [7-oct] EL DIARIO DE TOKENS PARA EL PANEL: cada token con su regla o su veto, dónde entró y salió, y lo que hizo después (36 h, en memoria) ═══
const DIARIO = new Map(), DIARIO_H = 36;
function apunta(c, extra = {}) { let d = DIARIO.get(c.mint); if (!d) { const n = c.nac || {}; d = { mint: c.mint, sym: c.sym, t0: c.t0, ten: n.compradores ?? null, seg: n.seg ?? null, top5: n.top5 ?? null, regla: null, nota: null, veto: null, ops: [], max: null, fin: null, puntos: null }; DIARIO.set(c.mint, d); } Object.assign(d, extra); if (c.sym && d.sym !== c.sym) d.sym = c.sym; return d; }
function resumenCurva(P0) { const n = P0.length; if (!n) return []; const paso = Math.max(1, Math.ceil(n / 240)); const out = []; for (let i = 0; i < n; i += paso) out.push([Math.round(P0[i].t), +P0[i].p.toFixed(1)]); const u = P0[n - 1]; out.push([Math.round(u.t), +u.p.toFixed(1)]); return out; }
setInterval(() => { const lim = Date.now() - DIARIO_H * 3.6e6; for (const [k, d] of DIARIO) if (d.t0 < lim) DIARIO.delete(k); }, 10 * 60e3);
// [7-oct] las cuentas en papel van con el PLAN: si el guardado era de otro plan (o del server viejo), se empieza de cero (lo de antes queda en P.anterior)
const PLAN_ID = PLAN ? "plan:" + PLAN.nombre : "estrategia:" + JSON.stringify(ESTRATEGIA); P.plan = PLAN_ID;
try { const g = JSON.parse(fs.readFileSync(ESTADO_FILE, "utf-8"));
  if (g.plan === PLAN_ID) Object.assign(P, g);
  else { P.anterior = { plan: g.plan || "el server viejo (⭐⭐)", desde: g.desde, hasta: Date.now(), cerradas: g.cerradas, real: g.real, papel: g.papel, verdad: g.verdad, lote2: g.lote2, lote2N: g.lote2N, lote3: g.lote3, caza: g.caza }; console.log(`📊 cuentas en papel NUEVAS para «${PLAN ? PLAN.nombre : "ESTRATEGIA"}» · las de antes (${P.anterior.plan}: ${g.cerradas || 0} cerradas, real ${(g.real || 0).toFixed(2)} SOL, con las operaciones falsas del server viejo) quedan aparte`); } } catch {}
// [19-oct] al arrancar no hay ninguna cámara abierta: un 2º o 3er lote que se quedó «abierto» porque al server de antes lo mataron sin apagar
//   (SOLFI ≥12, 19-oct 04:39Z, despliegue sin drainingSeconds) ya no se cierra nunca y el contador se quedaba en 1 para siempre
P.lote2Abiertas = 0; P.lote3Abiertas = 0; P.cazaAbiertas = 0;
const guardaP = () => { try { const tmp = ESTADO_FILE + ".tmp"; fs.writeFileSync(tmp, JSON.stringify({ ...P, ultimas: P.ultimas.slice(0, 300) })); fs.renameSync(tmp, ESTADO_FILE); } catch (e) { } };   // [12-oct] a .tmp y renombrar: con el disco lleno no se pierden las cuentas
setInterval(guardaP, 60e3);

// ── el estado ──
const S = { creadores: new Map(), arranque: Date.now(), solUsd: 0, solUsdT: 0, migs: 0, ultimaMig: 0, curvas: 0, swaps: 0, bytes: 0, wsPor: {}, errores: 0,
  camaras: new Map(),        // mint → la cámara
  vistas: new Set(),         // mints ya vistos (para no repetir)
  pp: { ws: null, ok: false, reconexiones: 0 }, hel: [], nacHechos: 0, nacFallos: 0, ultimas: [] };
// [15-oct] LOS TOKENS YA VISTOS, TAMBIÉN EN EL DISCO. «vistas» solo vivía en memoria: al reiniciarse, el server volvía a «ver migrar» mints que ya
//   había grabado otros días, en su misma piscina (muchas acciones tokenizadas: NVDAx, TSLAx, SPYx, NFLX…; también Ads, DOGE, ZEC…): 26 en las
//   16 h siguientes al despliegue del 14-oct, 5 de ellos operados. El laboratorio y las páginas solo tienen la primera vez de cada mint.
//   Ahora la lista se guarda en VISTAS_FILE cada minuto y se carga al arrancar; si aún no existe, de vistas_semilla.json (los mints grabados
//   hasta el 15-oct). Un mint visto antes del arranque no se opera ni se graba: sale una línea [SOLVETO] tipo=repetido para contarlos.
const VISTAS_FILE = process.env.VISTAS_FILE || (fs.existsSync("/data") ? "/data/solana_vistas.json" : "/tmp/solana_vistas.json");
const PREVIAS = new Set(); let VISTAS_DE = "nada";
try { for (const k of JSON.parse(fs.readFileSync(VISTAS_FILE, "utf-8"))) PREVIAS.add(k); VISTAS_DE = VISTAS_FILE; }
catch { try { for (const k of JSON.parse(fs.readFileSync(path.join(DIR, "vistas_semilla.json"), "utf-8"))) PREVIAS.add(k); VISTAS_DE = "vistas_semilla.json"; } catch {} }
for (const k of PREVIAS) S.vistas.add(k);
const repetidos = { n: 0, ultimos: [] };
const guardaVistas = () => { try { const tmp = VISTAS_FILE + ".tmp"; fs.writeFileSync(tmp, JSON.stringify([...S.vistas])); fs.renameSync(tmp, VISTAS_FILE); } catch (e) { } };
setInterval(guardaVistas, 60e3);
const log = (m) => console.log(m);
log(`📚 ${PREVIAS.size} tokens ya vistos antes del arranque (de ${VISTAS_DE}): si vuelven a «migrar», no se operan ni se graban`);
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
async function leeMigracion(firma, mint, poolConocida = null) {   // [20-oct] poolConocida: la canónica de PumpSwap (solo sus reservas)
  for (let i = 0; i < 6; i++) {
    try { const tx = await rpc("getTransaction", [firma, { encoding: "jsonParsed", commitment: "confirmed", maxSupportedTransactionVersion: 0 }]);
      if (tx) { const r = reservasDe(tx.meta?.postTokenBalances, mint, poolConocida); if (r) { r.vaults = vaultsDe(tx, mint, r.pool); return r; } return null; } } catch (e) { if (i === 5) throw e; }
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
    ws.on("message", (raw) => { S.bytes += raw.length; let m; try { m = JSON.parse(raw); } catch { return; }
      { const tipo = m.method === "accountNotification" ? "cuentas" : m.method === "transactionNotification" ? (H.subMig != null && m.params?.subscription === H.subMig ? "migraciones" : "tx") : "otros"; S.wsPor[tipo] = (S.wsPor[tipo] || 0) + raw.length; }   // [12-oct] para saber en qué se van los créditos (Helius cobra 2 por cada 0,1 MB)
      mensajeHelius(H, m); });
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
    const q = H.porSub.get(m.params?.subscription); if (!q) return; const c = S.camaras.get(q.mint); if (!c || c.fin) return; const sl = m.params?.result?.context?.slot; if (sl > 0) c.ultSlot = sl;
    const amt = +(m.params?.result?.value?.data?.parsed?.info?.tokenAmount?.uiAmountString || m.params?.result?.value?.data?.parsed?.info?.tokenAmount?.uiAmount || 0); if (!(amt >= 0)) return;
    if (q.cual === "base") c.resBase = amt; else c.resQuote = amt;
    clearTimeout(c.timerPrecio); c.timerPrecio = setTimeout(() => precioDesdeReservas(c), 150);   // las dos cuentas cambian a la vez: se espera a tener las dos
    return; }
  if (m.method !== "transactionNotification") return;
  const q0 = H.porSub.get(m.params?.subscription); const mint = q0 && q0.mint; if (!mint) return; const c = S.camaras.get(mint); if (!c || c.fin) return; { const sl = m.params?.result?.context?.slot; if (sl > 0) c.ultSlot = sl; }
  const firma = m.params?.result?.signature; if (firma) { if (c.vistas.has(firma)) return; c.vistas.add(firma); if (c.vistas.size > 4000) c.vistas = new Set([...c.vistas].slice(-2000)); }
  const meta = m.params?.result?.transaction?.meta; const r = reservasDe(meta?.postTokenBalances, mint, c.pool || piscinaCanonica(mint)); if (!r) return;   // [20-oct] solo la de PumpSwap: los swaps en otras piscinas del token no cuentan
  if (!c.pool || !c.vaults) { const nueva = !c.pool; c.pool = r.pool; if (c.rec && !c.rec.pool) c.rec.pool = c.pool; const tx = m.params?.result?.transaction; const v = vaultsDe({ meta, transaction: tx?.transaction || tx }, mint, c.pool);
    if (v) c.vaults = v;
    if (nueva || v) { log(`🏊 ${c.sym}: piscina ${corto(c.pool)} ${nueva ? "descubierta" : "vista"} en el 1er swap${v ? " · paso a vigilar solo sus dos cuentas" : ""}`); if (v) { desuscribe(H, c); suscribe(H, c); } } }
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
  flujoApunta(c, t, compra === true ? r.sol - (solAntes ?? r.sol) : 0, compra === false ? (solAntes ?? r.sol) - r.sol : 0);
  punto(c, t, precio, { s: compra === true ? r.sol - (solAntes ?? r.sol) : compra === false ? -((solAntes ?? r.sol) - r.sol) : 0 });
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
  flujoApunta(c, t, subeSol === true ? dSol : 0, subeSol === false ? dSol : 0);
  punto(c, t, precio, { s: subeSol === true ? dSol : subeSol === false ? -dSol : 0 }); }
// [9-oct] EL FLUJO POR MINUTOS: cuántos swaps y cuánto SOL entra y sale de la piscina en cada minuto de la cámara (para ver si sigue entrando gente)
function flujoApunta(c, t, solIn, solOut) { if (!c.flujo || !(t >= 0)) return; const i = Math.min(1500, Math.floor(t / 60)); const f = c.flujo[i] || (c.flujo[i] = { s: 0, i: 0, o: 0 }); f.s++; if (solIn > 0) f.i += solIn; if (solOut > 0) f.o += solOut; }
// [11-oct] el detalle de cada punto, en el mismo orden que pts=: «n|s|h|l» (swaps en ese segundo | SOL neto, 2 decimales, vacío si 0 | máximo y mínimo del segundo, solo si hubo más de un swap)
function ptxTxt(c) { return c.puntos.map(q => { const n = q.n || 0; const s = q.s && Math.abs(q.s) >= 0.005 ? q.s.toFixed(2) : ""; if (n <= 1) return s ? `${n}|${s}` : String(n); return `${n}|${s}|${f2(q.h)}|${f2(q.l)}`; }).join(","); }
function flujoTxt(c) { if (!c.flujo || !c.flujo.length) return ""; const o = []; for (let i = 0; i < c.flujo.length; i++) { const f = c.flujo[i]; o.push(f ? `${f.s}/${f.i.toFixed(1)}/${f.o.toFixed(1)}` : "0/0/0"); } return o.join(","); }
function punto(c, t, precio, sw) { const p = (precio / c.precioIni - 1) * 100; const ult = c.puntos[c.puntos.length - 1];
  // [11-oct] cada punto lleva también lo que pasó dentro de su segundo: n swaps, SOL neto (entra − sale), máximo y mínimo (sw = null en el latido)
  if (ult && t - ult.t < 1) { ult.p = p; if (sw) { ult.n = (ult.n || 0) + 1; ult.s = (ult.s || 0) + (sw.s || 0); if (p > (ult.h ?? -Infinity)) ult.h = p; if (p < (ult.l ?? Infinity)) ult.l = p; } lote2Mira(c, t, p); lote3Mira(c, t, p); cazaMira(c, t, p); return; }            // como mucho un punto por segundo
  c.puntos.push({ t, p, n: sw ? 1 : 0, s: sw ? (sw.s || 0) : 0, h: p, l: p }); alMotor(c, c.puntos.length - 1); /* [5-oct] los anteriores ya no cambian: al motor */ if (p > c.max.p) c.max = { t, p }; if (p < c.min.p) c.min = { t, p }; c.ultPrecio = precio; lote2Mira(c, t, p); lote3Mira(c, t, p); cazaMira(c, t, p); }

// ═══ [13-oct] LAS BALLENAS QUE SE COLABAN (Tony: «quitarnos las que podamos») ═══
//  La regla de siempre (migra en ≤5 s con los 5 mayores ≥80 %) necesitaba los segundos hasta migrar, y desde el 7-oct el nacimiento no los trae en ~30 %
//  de los tokens: justo los que se crean y migran de golpe. En los 6.694 grabados del 3 al 12-oct se colaron 1.063 ballenas y 713 bundles, y el server en
//  papel operó 139 de esas ballenas y 457 bundles. Con lo que el server ya sabe a los 3 s de migrar, fuera (herramientas/solana/nuevo/clasifica.py):
//   · la ballena de siempre: migra en ≤5 s con los 5 mayores ≥80 %
//   · salto al migrar: el precio de verdad ×VETO_SALTO (2) o más a los 3 s (alguien mete decenas de SOL al abrir la piscina)
//   · una cartera con toda la curva de bonos (el mayor ≥79 %) o ≤5 tenedores (si el nacimiento se leyó: 0 tenedores con 0 % = no se pudo leer)
//   · bundle (VETO_BUNDLE): migra en ≤5 s sin salto, o sin dato de cuándo se creó (de esos, Helius: 662 de 662 migraron en ≤5 s)
//  En lo grabado no quita ningún token limpio: todos tardan más de 5 s en migrar, no saltan y tienen más de 5 tenedores.
function precioA3s(c) { let v = null; for (const q of c.puntos) { if (Math.round(q.t) > 3) break; v = q.p; } return v; }
function x3De(c) { const p3 = precioA3s(c); return p3 != null ? 1 + p3 / 100 : null; }   // [13-oct, 15:40] el precio que graba a los 3 s / el de migrar (el del papel; ya no el de verdad)
function vetoBallena(c) { const n = c.nac || {}; const leido = !((n.compradores || 0) === 0 && (n.top5 || 0) === 0);
  if (n.seg != null && n.top5 != null && n.seg <= 5 && n.top5 >= 80) return { tipo: "ballena", txt: `ballena de siempre (${n.seg} s hasta migrar, los 5 mayores ${n.top5} %)` };
  const p3 = precioA3s(c), x3 = p3 != null && c.precioIni > 0 ? precioVerdad(1 + p3 / 100, c.precioIni).phi : null;
  if (x3 != null && x3 >= VETO_SALTO) return { tipo: "ballena", txt: `salto al migrar (×${x3.toFixed(1)} de verdad en los 3 primeros segundos)` };
  if (leido && n.top1 != null && n.top1 >= 79) return { tipo: "ballena", txt: `una cartera con toda la curva (el mayor ${n.top1} %)` };
  if (leido && n.compradores != null && n.compradores <= 5) return { tipo: "ballena", txt: `${n.compradores} tenedores al migrar` };
  if (n.seg == null) return { tipo: "bundle", txt: "bundle: no se sabe cuándo se creó (se crea y migra de golpe)" };
  if (n.seg <= 5) return { tipo: "bundle", txt: `bundle: migra ${n.seg} s después de crearse, sin salto` };
  return null; }
// [13-oct] la ballena o el bundle que no se graba: una línea corta con lo que se sabe (para contarlas y poder revisar el veto) y se cierra su cámara sin rescate
function vetoSinGrabar(c, vb) { const p3 = precioA3s(c), x3 = p3 != null && c.precioIni > 0 ? precioVerdad(1 + p3 / 100, c.precioIni).phi : null;
  S.sinGrabar = S.sinGrabar || {}; S.sinGrabar[vb.tipo] = (S.sinGrabar[vb.tipo] || 0) + 1;
  log(`[SOLVETO] sym=${c.sym} mint=${c.mint} pool=${c.pool || "-"} tipo=${vb.tipo} motivo=${vb.txt.replace(/\s+/g, "_")} precioIni=${c.precioIni ? c.precioIni.toExponential(4) : "-"} x3=${x3 != null ? x3.toFixed(3) : "-"} swaps=${c.swaps} ${c.nacTxt ? `nac=${c.nacTxt} ` : ""}pts=${c.puntos.map(q => `${Math.round(q.t)}:${f2(q.p)}`).join(",")}`);
  cierra(c, `${vb.tipo}: no se graba`); }
// el veto: las ballenas y los bundles no se operan; el resto, al motor
function decide(c) { if (c.fin || c.rec || c.veto) return; clearTimeout(c.timerNac); const n = c.nac || {};
  // [11-oct] solo PumpSwap: si la piscina es de otro programa (Meteora, Orca, o el «11111111» de una piscina que no supimos leer) no se opera. Tony: piscina
  //   «11111111», precio inicial 220 veces más bajo que el real → la curva salta a +21.900 % a los 49 s y el papel «ganó» 4,55 SOL de mentira
  if (SOLO_PUMPSWAP && c.esPumpSwap === false) { c.veto = "no es PumpSwap"; P.vetadas++; apunta(c, { nota: "no es PumpSwap", veto: `no es PumpSwap (${(c.programa || "?").slice(0, 8)})` }); log(`🚫 ${c.sym}: la piscina no es de PumpSwap (${(c.programa || "?").slice(0, 8)}) · no se opera`); return; }
  // [10-oct] la piscina rara: una migración de pump.fun vale ~325 SOL de MC; con menos de PISCINA_MIN SOL (de juguete: el lote sería toda la piscina y el
  //   precio es de mentira) o más de PISCINA_MAX (no es de pump.fun: EURC, TRX, QUBT…) no se opera (se graba)
  { const mcSol = c.precioIni > 0 ? c.precioIni * (c.supply || 1e9) : null; if (mcSol != null && (mcSol < PISCINA_MIN || mcSol > PISCINA_MAX)) { c.veto = "piscina rara"; P.vetadas++; apunta(c, { nota: "piscina rara", veto: `piscina rara (${mcSol < 1000 ? mcSol.toFixed(1) : Math.round(mcSol)} SOL de MC)` }); log(`🫧 ${c.sym}: piscina rara (${mcSol < 1000 ? mcSol.toFixed(2) : Math.round(mcSol)} SOL de MC al migrar; lo normal son ~325) · no se opera`); return; } }
  if (VETO_BALLENA || NO_COMPRA_SI_CAE != null || NO_COMPRA_SI_NO_SUBE != null) {   // [13-oct] se mira a los 3 s de migrar (el salto, y si ya cae); si el nacimiento llega antes, se espera
    const t = (Date.now() - c.t0) / 1000; if (t < 3.5 && !c.esperaSalto) { c.esperaSalto = true; setTimeout(() => decide(c), Math.ceil((3.5 - t) * 1000)); return; } }
  if (VETO_BALLENA) {
    const vb = vetoBallena(c); if (vb && (vb.tipo === "ballena" || VETO_BUNDLE)) { c.veto = vb.tipo; P.vetadas++; P.porVeto = P.porVeto || {}; P.porVeto[vb.tipo] = (P.porVeto[vb.tipo] || 0) + 1; apunta(c, { nota: vb.txt, veto: vb.txt, ballena: vb.tipo === "ballena" });
      // [13-oct] y no se graba (ahorra créditos de Helius); 1 de cada GRABA_MUESTRA sí, entera, para comprobar que el veto sigue acertando
      const graba = GRABA_VETADAS || (GRABA_MUESTRA > 0 && (S.vetadasVistas = (S.vetadasVistas || 0) + 1) % GRABA_MUESTRA === 0);
      log(`${vb.tipo === "bundle" ? "📦" : "🐋"} ${c.sym}: ${vb.txt} · no se opera${graba ? (GRABA_VETADAS ? "" : ` · se graba entera como muestra (1 de cada ${GRABA_MUESTRA})`) : " ni se graba"}`);
      if (!graba) vetoSinGrabar(c, vb); return; } }
  // [13-oct] Tony: «si desde que migra hasta que lanza la compra ha bajado un X %, que cancele». No se compra, pero se graba (para comprobar que acierta).
  //   En papel (el limpio con el plan, en los 1.768 limpios que pasan el plan): con 2 % compra 1.021 y pasa de +42,1 a +45,5 SOL; con 10 %, 1.399 y +47,4.
//   [13-oct, 18:45] sin los vetos de hora y 6-60 s, en los 2.661 limpios: con 2 % compra 1.571 y pasa de +31,7 a +53,6 SOL.
  if (NO_COMPRA_SI_CAE != null) { const x3 = x3De(c); if (x3 != null && x3 < 1 - NO_COMPRA_SI_CAE / 100) {
    c.veto = "cae"; P.vetadas++; P.porVeto = P.porVeto || {}; P.porVeto.cae = (P.porVeto.cae || 0) + 1;
    const txt = `ya cae al ir a comprar (${((x3 - 1) * 100).toFixed(1)} % a los 3 s; el corte, −${NO_COMPRA_SI_CAE} %)`; apunta(c, { nota: txt, veto: txt });
    log(`📉 ${c.sym}: ${txt} · no se compra (se graba)`); return; } }
  // [16-oct] la criba 📈 de «Todas»: si a los 3 s no ha subido al menos NO_COMPRA_SI_NO_SUBE %, no se compra (se graba igual, para comprobarlo)
  if (NO_COMPRA_SI_NO_SUBE != null) { const x3 = x3De(c); if (x3 != null && x3 < 1 + NO_COMPRA_SI_NO_SUBE / 100) {
    c.veto = "nosube"; P.vetadas++; P.porVeto = P.porVeto || {}; P.porVeto.nosube = (P.porVeto.nosube || 0) + 1;
    const txt = `no ha subido lo bastante al ir a comprar (${x3 >= 1 ? "+" : ""}${((x3 - 1) * 100).toFixed(1)} % a los 3 s; hace falta +${NO_COMPRA_SI_NO_SUBE} %)`; apunta(c, { nota: txt, veto: txt });
    log(`📈 ${c.sym}: ${txt} · no se compra (se graba)`); return; } }
  const rg = reglaDe(c); if (rg && rg.r.acc < 0) { c.veto = "regla " + (rg.i + 1); c.regla = rg.i + 1; P.vetadas++; const esBallena = /ballena/i.test(rg.r.nota || ""); if (esBallena) { P.porVeto = P.porVeto || {}; P.porVeto.pocos = (P.porVeto.pocos || 0) + 1; } apunta(c, { regla: rg.i + 1, nota: rg.r.nota || "no entrar", veto: rg.r.nota || "no entrar", ballena: esBallena }); log(`⛔ ${c.sym}: regla ${rg.i + 1} (${rg.r.nota || "no entrar"}) · ${n.seg ?? "?"} s · ${n.compradores ?? "?"} tenedores · 5 mayores ${n.top5 ?? "?"} % · ${horaEsp(c.t0)}h · no se opera`); return; }
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
    if (p.motivo === "NEURONA" && (c.motor || MOTOR).reloj) p.motivo = "RELOJ";   // [14-oct] la venta del reloj (el motor la apunta como NEURONA)
    const papel = (p.neto || 0) * K, real = W.netoReal({ fee: p.feePct, tam: p.tamaño, compras: p.compras || [], entrada: p.entrada, cierre: p.precioCierre }, { _P: c.rec.puntos, _ds: 0.58 }) * K;
    { const P0 = c.rec.puntos; (c.pendVer = c.pendVer || []).push({ compras: (p.compras || []).map(q => ({ t: q.t, p: q.p, precio: q.precio })), entrada: p.entrada, tC: P0.length ? P0[P0.length - 1].t : 0, motivo: p.motivo }); }   // [12-oct] lo de verdad
    P.papel += papel; P.real += real; P.cerradas++; if (real > 0) P.ganadas++; P.porMotivo[p.motivo] = (P.porMotivo[p.motivo] || 0) + real;
    const lotes = (p.compras || []).length || 1; c.papel = (c.papel || 0) + papel; c.real = (c.real || 0) + real; c.ops = (c.ops || 0) + 1;
    log(`[SOLPOS] sym=${c.sym} mint=${c.mint} motivo=${p.motivo} lotes=${lotes} pnl=${(p.pnlPct ?? 0).toFixed(1)}% papel=${papel.toFixed(4)}SOL real=${real.toFixed(4)}SOL acum_real=${P.real.toFixed(3)}SOL`);
    { const comp = p.compras || []; apunta(c).ops.push({ entraT: comp.length ? Math.round(c.t0 + 1000 * comp[0].t) : null, entraPct: comp.length ? +((comp[0].precio - 1) * 100).toFixed(1) : null, saleT: Date.now(), salePct: +(((p.precioCierre || 1) - 1) * 100).toFixed(1), motivo: p.motivo, lotes, real: +real.toFixed(4) }); }
    P.ultimas.unshift({ sym: c.sym, mint: c.mint, motivo: p.motivo, lotes, pnl: +(p.pnlPct ?? 0).toFixed(1), papel: +papel.toFixed(4), real: +real.toFixed(4), t: Date.now() }); if (P.ultimas.length > 300) P.ultimas.length = 300; }
  valoraPendientes(c, false);
  P.abiertas = abiertas; if (expuesto > P.maxExpuesto) P.maxExpuesto = +expuesto.toFixed(3); P.expuesto = +expuesto.toFixed(3); }

// ── PumpPortal: las migraciones ──
function abrePumpPortal() {
  const ws = new WebSocket(PP_WS); S.pp.ws = ws;
  ws.on("open", () => { S.pp.ok = true; ws.send(JSON.stringify({ method: "subscribeMigration" })); if (CURVAS.activo) ws.send(JSON.stringify({ method: "subscribeNewToken" })); log(`🔌 PumpPortal: escuchando las migraciones${CURVAS.activo ? " y los tokens nuevos (para la muestra de curvas)" : ""}${PUMPPORTAL_API_KEY ? " (con clave)" : " (sin clave)"}`); });
  ws.on("message", (raw) => { let m; try { m = JSON.parse(raw); } catch { return; } if (m && m.txType === "create") { CURVAS.nueva(m); return; }   // [18-oct] un token nuevo en pump.fun: a la muestra de curvas, no es una migración
    if (m && m.mint && (m.txType === "migrate" || m.txType === "migration" || m.signature) && apuntaLlegada(m.mint, "pumpportal")) migracion({ ...m, fuente: "pumpportal" }).catch(e => { S.errores++; log(`⚠️ migración ${corto(m.mint)}: ${e.message}`); }); });
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
  const mint = m.mint;
  if (S.vistas.has(mint)) {   // [15-oct] el visto antes del arranque (otro día): se cuenta una vez y no se opera ni se graba
    if (PREVIAS.has(mint)) { PREVIAS.delete(mint); repetidos.n++; const sym = String(m.symbol || m.name || corto(mint)).replace(/\s+/g, "_").slice(0, 20);
      repetidos.ultimos.unshift({ sym, mint, t: Date.now(), fuente: m.fuente || "pumpportal" }); repetidos.ultimos.length = Math.min(repetidos.ultimos.length, 30);
      log(`[SOLVETO] sym=${sym} mint=${mint} pool=${(m.reservas && m.reservas.pool) || "-"} tipo=repetido motivo=ya_visto_antes_del_arranque fuente=${m.fuente || "pumpportal"} swaps=0`); }
    return; }
  S.vistas.add(mint); if (S.vistas.size > 20000) S.vistas = new Set([...S.vistas].slice(-10000));
  S.migs++; S.ultimaMig = Date.now();
  const c = { mint, sym: m.symbol || m.name || corto(mint), firma: m.signature || null, t0: Date.now(), pool: null, precioIni: 0, ref: "migración", puntos: [], max: { t: 0, p: 0 }, min: { t: 0, p: 0 },
    swaps: 0, vistas: new Set(), ultReservas: null, ultSwap: 0, supply: null, mayhem: false, nac: null, nacTxt: null, fin: false, hel: null, min1: { swaps: 0, compradores: new Set(), vendedores: new Set(), solIn: 0, solOut: 0 }, flujo: [] };   // [9-oct] flujo: por minuto, swaps / SOL que entra / SOL que sale, toda la cámara
  S.camaras.set(mint, c); try { REAL.prepara(); } catch {}   // [18-oct] la prioridad y el blockhash calientes para la orden real de los 4 s
  // 1) la piscina y el precio inicial, de la transacción de la migración
  c.fuente = m.fuente || "pumpportal";
  // [20-oct] LA PISCINA, SIEMPRE LA CANÓNICA DE PUMPSWAP (piscina.js: la que crea pump.fun al migrar, calculada sin llamar a nadie; cuadra con las
  //   5.567 piscinas de PumpSwap grabadas). Antes se cogía «el dueño con más tokens y WSOL» de la transacción y, si la migración llegaba sin reservas,
  //   la del 1er swap: en 327 de 9.803 tokens era otra (la autoridad de una piscina de Raydium o Meteora del mismo token, compartida por muchos tokens,
  //   o una cartera: HLnp…TLcC en 42 tokens) → la cámara grababa un precio que no era el de PumpSwap y la orden real salía «sin piscina» por Jupiter.
  const canon = piscinaCanonica(mint); c.canon = canon;
  if (m.reservas && m.reservas.tok > 0 && m.reservas.sol > 0 && (!canon || m.reservas.pool === canon)) { const r = m.reservas; c.pool = r.pool; c.precioIni = r.sol / r.tok; c.reservasIni = r; c.vaults = r.vaults || null; c.ultPrecio = c.precioIni; c.puntos.push({ t: 0, p: 0 }); }
  else {
    if (m.reservas && m.reservas.pool && canon && m.reservas.pool !== canon) { S.piscinasCambiadas = (S.piscinasCambiadas || 0) + 1; log(`⚠️ ${c.sym}: la migración trae la piscina ${corto(m.reservas.pool)}, que no es la de PumpSwap (${corto(canon)}): uso la de PumpSwap`); }
    if (c.firma) { try { const r = await leeMigracion(c.firma, mint, canon); if (r) { c.pool = r.pool; c.precioIni = r.sol / r.tok; c.reservasIni = r; c.ultPrecio = c.precioIni; c.puntos.push({ t: 0, p: 0 }); } } catch (e) { log(`⚠️ ${c.sym}: no pude leer la migración (${e.message}); el precio inicial será el del 1er swap`); } } }
  if (!c.pool && canon) c.pool = canon;   // sin reservas todavía: la canónica; el precio inicial será el del 1er swap EN ELLA
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
  log(`🐣 MIGRACIÓN${c.esPumpSwap === false ? " (NO es de PumpSwap: " + (c.programa || "?").slice(0, 8) + ")" : c.esPumpSwap == null ? " (piscina sin confirmar aún)" : ""} ${c.sym} (${corto(mint)})${c.pool ? ` · piscina ${corto(c.pool)}${c.precioIni ? ` · precio inicial ${c.precioIni.toExponential(3)} SOL` : " (la de PumpSwap, sin reservas aún: el precio inicial será el del 1er swap)"}` : " · piscina aún no (se verá en el 1er swap)"} · grabando ${CAMARA_MIN} min`);
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
let CURVAS = CURVASM.nulo();   // [18-oct] la muestra de curvas de pump.fun (curvas.js): cuántas de las que llegan a X % migran; se crea en el arranque
// ── el nacimiento por Helius (cuando la API de pump.fun no contesta): símbolo (getAsset), tenedores y concentración (cuentas del token),
//    y edad (la firma más antigua que menciona el mint). "compradores" = tenedores al migrar (los que compraron y aún tienen).
const TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
async function nacimientoHelius(c) {
  const n = { seg: null, compradores: 0, compras: 0, ventas: 0, exentas: 0, top1: 0, top5: 0, top10: 0, mayorTxPct: 0, mayorTxDest: 1, prim3Pct: 0, creadorPct: 0, vendidoPct: 0, snipeSol: 0, tradesLeidos: 0, fuente: "helius" };
  try { const a = await rpc("getAsset", { id: c.mint }); const sym = a?.content?.metadata?.symbol; if (sym) c.sym = String(sym).slice(0, 14);
    c.asset = { nombre: a?.content?.metadata?.name ?? null, uri: a?.content?.json_uri ?? null, links: a?.content?.links || null, descripcion: (a?.content?.metadata?.description || "").slice(0, 300), mutable: a?.mutable ?? null, autoridades: (a?.authorities || []).map(x => x.address) }; } catch {}   // [12-oct] se guarda para el rescate (así no lo vuelve a pedir)
  try { let antes = undefined, viejo = null;
    const firmas = async () => { antes = undefined; viejo = null; n.txsCurva = 0; for (let i = 0; i < 5; i++) { const sigs = await rpc("getSignaturesForAddress", [c.mint, { limit: 1000, before: antes, commitment: "confirmed" }]); if (!sigs || !sigs.length) break; n.txsCurva += sigs.length; viejo = sigs[sigs.length - 1]; if (sigs.length < 1000) break; antes = viejo.signature; } };
    await firmas();
    // [13-oct] el que se crea y migra de golpe: Helius aún no tiene sus firmas (o sin hora) cuando se le pregunta → se espera un poco y se vuelve a pedir
    if ((!viejo || !viejo.blockTime) && NAC_REINTENTO_MS > 0) { await new Promise(r => setTimeout(r, NAC_REINTENTO_MS)); await firmas(); n.reintento = 1; S.nacReintentos = (S.nacReintentos || 0) + 1; if (viejo && viejo.blockTime) S.nacReintentosOk = (S.nacReintentosOk || 0) + 1; }
    if (n.txsCurva >= 5000) n.txsCurvaTope = true;   // [11-oct] txsCurva = transacciones que mencionan el mint antes de migrar ≈ swaps en la curva de bonos (+ crear, metadatos); tope 5.000
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
    c.nacTxt = [n.seg ?? "-", n.compradores, 0, n.top5, n.top1, n.mayorTxPct, 1, n.prim3Pct, n.creadorPct, n.vendidoPct, c.creador || "-", n.creadorTokens || 1, n.txsCurva != null ? n.txsCurva + (n.txsCurvaTope ? "+" : "") : "-"].join("/"); S.nacHechos++;   // [11-oct] 13º campo: transacciones en la curva de bonos
    log(`👶 NACIMIENTO ${c.sym}: ${n.seg != null ? (n.seg < 120 ? n.seg + " s" : Math.round(n.seg / 60) + " min") : "?"} hasta migrar · ${n.txsCurva != null ? n.txsCurva + (n.txsCurvaTope ? "+" : "") + " tx en la curva · " : ""}${n.compradores} tenedores al migrar · los 5 mayores ${n.top5} % · el mayor ${n.top1} % · el creador ${corto(c.creador)} tiene ${n.creadorPct} %${(n.creadorTokens || 1) > 1 ? ` · 🔁 su token nº ${n.creadorTokens}` : ""}${c.mayhem ? " · ⚠️ MAYHEM" : ""}`); return; }
  let fallo = null; const coin = await pumpApi(`/coins/${c.mint}`).catch(e => { fallo = e.message; return null; });
  if (coin) { if (coin.symbol) c.sym = String(coin.symbol).slice(0, 14); c.creador = coin.creator || null; c.creado = coin.created_timestamp ? +coin.created_timestamp : null; c.redes = { tg: !!coin.telegram, tw: !!coin.twitter, web: !!coin.website };
    if (!c.supply && coin.total_supply) c.supply = +coin.total_supply / 1e6; }
  const trades = []; if (coin) for (let off = 0; off < 4000; off += 200) { const L = await pumpApi(`/trades/all/${c.mint}?limit=200&offset=${off}&minimumSize=0`).catch(e => { fallo = e.message; return null; }); if (!Array.isArray(L) || !L.length) break; trades.push(...L); if (L.length < 200) break; }
  if (!coin || !trades.length) {   // la API de pump.fun no contesta (o no da trades): lo mismo por Helius
    if (avisoPump < 3) { avisoPump++; log(`ℹ️ la API de pump.fun no contestó para ${c.sym} (${fallo || "sin trades"}) → nacimiento por Helius (tenedores al migrar y edad del mint)`); }
    c.nac = await nacimientoHelius(c); const n = c.nac;
    c.nacTxt = [n.seg ?? "-", n.compradores, 0, n.top5, n.top1, n.mayorTxPct, 1, n.prim3Pct, n.creadorPct, n.vendidoPct, (c.creador || "-").slice(0, 10), n.creadorTokens || 1, n.txsCurva != null ? n.txsCurva + (n.txsCurvaTope ? "+" : "") : "-"].join("/"); S.nacHechos++;
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
  c.nacTxt = [seg ?? "-", c.nac.compradores, 0, c.nac.top5, c.nac.top1, c.nac.mayorTxPct, 1, c.nac.prim3Pct, c.nac.creadorPct, c.nac.vendidoPct, (c.creador || "-").slice(0, 10), 1, antes.length].join("/");   // [11-oct] 13º campo: trades de pump.fun antes de migrar
  S.nacHechos++;
  log(`👶 NACIMIENTO ${c.sym}: ${seg != null ? (seg < 120 ? seg + " s" : Math.round(seg / 60) + " min") : "?"} hasta migrar · ${c.nac.compradores} compradores (${compras} compras, ${ventas} ventas) · los 5 mayores ${c.nac.top5} % · la mayor compra ${c.nac.mayorTxPct} % · el creador ${c.nac.creadorPct} % · revendido ${c.nac.vendidoPct} %${c.mayhem ? " · ⚠️ MAYHEM" : ""}`);
}

// [12-oct] lo que necesita el rescate de cada token (rescate.js): todo sale de la cámara; si se cerró por apagar el server, se rehace la cámara entera
function infoRescate(c, motivo) { const n = c.nac || {}; const P0 = c.puntos || []; const pA = (s) => { let v = null; for (const q of P0) { if (q.t > s) break; v = q.p; } return v == null ? null : +(+v).toFixed(2); };
  const roto = /apagado/.test(motivo || ""); const ultT = P0.length ? Math.round(P0[P0.length - 1].t) : 0;
  const plan = c.muyLarga ? CAMARA_MUY_LARGA_MIN : c.larga ? CAMARA_LARGA_MIN : CAMARA_MIN; const minV = +(process.env.RESCATE_PISCINA_MIN || 10);
  let swV = 0; if (Array.isArray(c.flujo)) for (let i = 0; i < Math.min(c.flujo.length, Math.ceil(minV)); i++) if (c.flujo[i]) swV += c.flujo[i].s;
  return { mint: c.mint, sym: c.sym, pool: c.pool || null, firma: c.firma || null, t0: c.t0, seg: n.seg ?? null, txsCurva: n.txsCurva ?? null, txsCurvaTope: !!n.txsCurvaTope, creador: c.creador || null,
    precioIni: c.precioIni || null, supply: c.supply || 1e9, dur: roto ? Math.max(plan * 60, ultT) : ultT, swaps: c.swaps || 0, swapsVentana: swV, ops: c.ops || 0, motor: !!c.rec, veto: c.veto || null, regla: c.regla ?? null,
    p60: pA(60), p300: pA(300), roto, motivo: motivo || null, pumpswap: c.esPumpSwap ?? null, asset: c.asset || null }; }
// ── el cierre de una cámara: la línea [SOLREC] ──
function cierra(c, motivo) {
  if (c.fin) return;
  if (c.rec) { const m = c.motor || MOTOR; alMotor(c, c.puntos.length); try { m.estrategiaFin(c.rec); } catch (e) { log(`⚠️ motor (fin) ${c.sym}: ${e.message}`); } cuentas(c);
    valoraPendientes(c, true);
    m.sb.state.posiciones = m.sb.state.posiciones.filter(p => p.estado === "ABIERTA"); }   // lo cerrado ya está contado
  // [23-oct] el 2º lote, el 3er lote y el cazador se cierran fuera del «if (c.rec)»: el 2º lote y el cazador también operan en los que el 1er lote no compró
  if (c.lote2 && !c.lote2.hecho) { if (c.lote2.tx == null && c.puntos.length) { c.lote2.tx = Math.max(c.lote2.te + RETRASO_S, c.puntos[c.puntos.length - 1].t); c.lote2.por = "fin"; } lote2Valora(c, true); }   // [18-oct] el 2º lote
  lote3Cierra(c);   // [18-oct, noche] el 3er lote (al momentum)
  cazaCierra(c);   // [23-oct] el cazador
  { const d = DIARIO.get(c.mint); if (d) { d.puntos = resumenCurva(c.puntos); d.max = c.max ? { t: Math.round(c.max.t), p: +c.max.p.toFixed(1) } : null; d.fin = c.puntos.length ? +c.puntos[c.puntos.length - 1].p.toFixed(1) : null; } }
  clearTimeout(c.timerNac); c.fin = true; if (c.rec) { CERRADAS.set(c.mint, c); setTimeout(() => CERRADAS.delete(c.mint), 30 * 60e3); } clearTimeout(c.timerFin); if (c.hel) desuscribe(c.hel, c); S.camaras.delete(c.mint);
  if (motivo.startsWith("Mayhem")) { if (!SOLO_LOG) log(`🚫 ${c.sym}: Mayhem (supply ${Math.round((c.supply || 0) / 1e6)}M) · no se graba`); return; }
  if (/: no se graba$/.test(motivo)) return;   // [13-oct] ballena o bundle vetados: ni [SOLREC] ni rescate (ya salió su [SOLVETO])
  if (c.puntos.length < 2 || !c.precioIni) { log(`🗑️ ${c.sym}: sin curva (${c.swaps} swaps, ${c.puntos.length} puntos) · ${motivo}`); return; }
  S.curvas++;
  const mc = c.precioIni * (c.supply || 1e9) * (S.solUsd || 0); const pts = c.puntos.map(q => `${Math.round(q.t)}:${f2(q.p)}`).join(",");
  const m1 = c.min1; const conducta = `60:${m1.compradores.size}/${m1.vendedores.size}/${f2(m1.solIn)}/${f2(m1.solOut)}/${m1.swaps}`;
  const linea = `[SOLREC] sym=${c.sym} mint=${c.mint} pool=${c.pool || "-"} firma=${c.firma || "-"} MC=$${(mc / 1000).toFixed(1)}K supply=${Math.round((c.supply || 1e9) / 1e6)}M MIN=${f2(c.min.p)}%@${Math.round(c.min.t)}s MAX=${f2(c.max.p)}%@${Math.round(c.max.t)}s par=SOL precioIni=${c.precioIni.toExponential(4)} ref=${c.ref} swaps=${c.swaps} puntos=${c.puntos.length} mayhem=${c.mayhem ? 1 : 0} prog=${c.esPumpSwap === true ? "pumpswap" : c.programa ? c.programa.slice(0, 8) : "-"} veto=${c.veto ? c.veto.replace(" ", "_") : "-"} regla=${c.regla ?? "-"} bot=${c.ops || 0}/${(c.papel || 0).toFixed(4)}/${(c.real || 0).toFixed(4)} verdad=${(c.verdad || 0).toFixed(4)} lote2=${c.lote2 ? `${c.lote2.por || "abierto"}/${(c.lote2Verdad || 0).toFixed(4)}` : c.lote2No ? "no" : "-"} liq=${c.liquidez || 0}/${c.raros || 0} conducta=${conducta} ${c.nacTxt ? `nac=${c.nacTxt} ` : ""}${c.flujo && c.flujo.length ? `flujo=${flujoTxt(c)} ` : ""}ptx=${ptxTxt(c)} pts=${pts}`;
  log(linea);
  try { RESC.encola(infoRescate(c, motivo)); } catch (e) { log(`⚠️ rescate (cola) ${c.sym}: ${e.message}`); }   // [12-oct] la curva de bonos, la piscina con cartera y los extras, en cuanto la cadena lo dé por definitivo
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

// [12-oct] lo que se gasta en Helius (estimado): WebSocket 2 créditos por cada 0,1 MB; llamadas 1 (getAsset y getTokenAccounts 10); y el rescate (contado de verdad)
function vivoDia() { const h = Math.max(0.05, (Date.now() - S.arranque) / 3600e3); const pm = S.porMetodo || {}; let rpcC = 0; for (const [k, v] of Object.entries(pm)) rpcC += v * (k === "getAsset" || k === "getTokenAccounts" ? 10 : 1);
  return { dia: Math.round((S.bytes / 1e5 * 2 + rpcC) / h * 24), horas: h }; }
function creditos() { const h = Math.max(0.05, (Date.now() - S.arranque) / 3600e3); const pm = S.porMetodo || {}; let rpcC = 0; for (const [k, v] of Object.entries(pm)) rpcC += v * (k === "getAsset" || k === "getTokenAccounts" ? 10 : 1);
  const ws = S.bytes / 1e5 * 2; const dia = (x) => Math.round(x / h * 24); const r = RESC.resumen(); const resc = r.ultimos7 && r.ultimos7.length ? Math.round(r.ultimos7.reduce((a, x) => a + (x.creditos || 0), 0) / r.ultimos7.length) : 0;
  const wsPor = Object.fromEntries(Object.entries(S.wsPor || {}).map(([k, v]) => [k, Math.round(v / 1e6)]));
  return { horas: +h.toFixed(1), wsDia: dia(ws), rpcDia: dia(rpcC), rescateDia: resc, rescateHoy: RESC.gastoHoy(), rescateTope: r.topeDia, mesSinRescate: Math.round((dia(ws) + dia(rpcC)) * 30.4), mes: Math.round((dia(ws) + dia(rpcC) + resc) * 30.4), wsMBPor: wsPor }; }
const kM = (x) => x >= 1e6 ? (x / 1e6).toFixed(1).replace(".", ",") + " M" : Math.round(x / 1e3) + " k";
function creditosTxt() { const c = creditos(); return `💳 Helius ≈ ${kM(c.wsDia + c.rpcDia)} créditos/día el server (WebSocket ${kM(c.wsDia)} · llamadas ${kM(c.rpcDia)}; MB: ${Object.entries(c.wsMBPor).map(([k, v]) => k + " " + v).join(", ") || "—"}) + rescate ${kM(c.rescateHoy)} hoy (tope ${kM(c.rescateTope)}) → ≈ ${kM(c.mes)} al mes`; }
// ── la salud, cada 10 minutos ──
setInterval(() => { const h = ((Date.now() - S.arranque) / 3600e3).toFixed(1); const subs = S.hel.reduce((s, H) => s + H.subs.size, 0);
  log(`[SALUD] ${h}h · migraciones=${S.migs} (última hace ${S.ultimaMig ? Math.round((Date.now() - S.ultimaMig) / 60e3) + " min" : "—"}) · repetidos=${repetidos.n}${S.piscinasCambiadas ? ` · piscinas que no eran la de PumpSwap=${S.piscinasCambiadas}` : ""} · grabando=${S.camaras.size} · curvas=${S.curvas} · swaps=${S.swaps} · nacimientos=${S.nacHechos}/${S.nacHechos + S.nacFallos} · 📡 migraciones: Helius primero ${MIGF.helius} · PumpPortal primero ${MIGF.pumpportal}${MIGF.adelanto.length ? ` · Helius se adelanta ${Math.round(MIGF.adelanto.slice().sort((a, b) => a - b)[Math.floor(MIGF.adelanto.length / 2)])} ms (mediana)` : ""} · 💰 REAL: ${(() => { const r = REAL.resumen(); return !r.listo ? "sin cartera" : `${r.armado ? "ARMADO" : "desarmado"} · lote ${r.lote}${r.sombra ? " (sombra del papel " + r.lotePapel + ")" : ""} · saldo ${r.saldo != null ? r.saldo.toFixed(3) : "?"} SOL · dentro ${r.dentro} · cuadres ${r.totales.cuadres} · REAL ${r.totales.real >= 0 ? "+" : ""}${r.totales.real.toFixed(4)} SOL (hoy ${r.hoy.real >= 0 ? "+" : ""}${(r.hoy.real || 0).toFixed(4)}) · fallos ${r.totales.fallos}${r.latencia && r.latencia.ordenes ? ` · ⏱️ latencia (${r.latencia.ok} órdenes, ${r.latencia.fallos} fallos, ${r.latencia.colgadas} colgadas): espera ${r.latencia.esperaMed ?? "-"} ms · envío ${r.latencia.envioMed ?? "-"} ms (p90 ${r.latencia.envioP90 ?? "-"}) · confirmación ${r.latencia.confMed ?? "-"} ms (p90 ${r.latencia.confP90 ?? "-"}) · total ${r.latencia.totalMed ?? "-"} ms (p90 ${r.latencia.totalP90 ?? "-"}) · ${r.latencia.slotsMed ?? "-"} slots tarde (p90 ${r.latencia.slotsP90 ?? "-"})` : ""}`; })()} · 📊 PAPEL (desde ${new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" }).format(new Date(P.desde))}): entradas ${P.entradas} · vetadas ${P.vetadas} (🐋 ballenas ${(P.porVeto || {}).ballena || 0} · 📦 bundles ${(P.porVeto || {}).bundle || 0} · 📉 ya caían ${(P.porVeto || {}).cae || 0}; sin grabar ${((S.sinGrabar || {}).ballena || 0) + ((S.sinGrabar || {}).bundle || 0)}) · sin nacimiento ${P.sinNac} · nacimientos reintentados ${S.nacReintentos || 0} (con hora a la 2ª ${S.nacReintentosOk || 0}) · cerradas ${P.cerradas} (${P.ganadas} ganadas) · abiertas ${P.abiertas} (${(P.expuesto || 0).toFixed(2)} SOL; máx ${P.maxExpuesto} SOL) · papel ${P.papel >= 0 ? "+" : ""}${P.papel.toFixed(3)} SOL · DE VERDAD ${(P.verdad || 0) >= 0 ? "+" : ""}${(P.verdad || 0).toFixed(3)} SOL (${P.verdadN || 0} valoradas) · real estimado viejo ${P.real >= 0 ? "+" : ""}${P.real.toFixed(3)} SOL ·${LOTE2_SOL > 0 ? ` 2º lote (${LOTE2_SOL} SOL a los ${LOTE2_T} s si entran ≥ ${LOTE2_ENTRA} con ≤ ${LOTE2_SWAPS} swaps): abiertas ${P.lote2Abiertas || 0} · cerradas ${P.lote2N || 0} (${P.lote2Gan || 0} ganadas) · de verdad ${(P.lote2 || 0) >= 0 ? "+" : ""}${(P.lote2 || 0).toFixed(3)} SOL · el 1º con el 2º ${(P.lote2Con1 || 0) >= 0 ? "+" : ""}${(P.lote2Con1 || 0).toFixed(3)} ·` : ""} ${LOTE3_SOL > 0 ? ` 3er lote (${LOTE3_SOL} SOL al momentum, [${LOTE3_DESDE}-${LOTE3_HASTA}] s, ≥ umbral SOL netos en ${LOTE3_VENTANA} s, ×${LOTE3_OBJ} o ${LOTE3_RELOJ} s): abiertas ${P.lote3Abiertas || 0} · ${lote3Txt()} ·` : ""}${CAZA_SOL > 0 ? ` 🎯 cazador (${CAZA_SOL} SOL en ${CAZA_COMBOS.length} mini-lotes, con el 2º lote dentro): ${cazaTxt()} ·` : ""} Mayhem fuera=${S.mayhemFuera || 0} · creadores repetidos=${[...S.creadores.values()].filter(L => L.length >= 2).length} · por sondeo ${S.porSondeo || 0} / por WebSocket ${S.porWs || 0} · Helius: ${S.llamadas || 0} llamadas (${Math.round((S.llamadas || 0) / Math.max(0.01, +h) / 60)}/min; ${Object.entries(S.porMetodo || {}).sort((a, b) => b[1] - a[1]).slice(0, 4).map(([k, v]) => k + " " + v).join(", ")}) · WS ${S.hel.filter(H => H.ok).length}/${S.hel.length} con ${subs} tokens, ≈${(S.bytes / 1e6).toFixed(0)} MB · PumpPortal ${S.pp.ok ? "ok" : "CAÍDO"} · SOL $${S.solUsd.toFixed(0)} · errores ${S.errores} · ${RESC.saludTxt()} · ${creditosTxt()}`); }, 10 * 60e3);

// ── un HTTP mínimo para ver que vive ──
let PANEL_HTML = null; try { PANEL_HTML = fs.readFileSync(path.join(DIR, "panel.html"), "utf-8"); } catch { console.log("ℹ️ no hay panel.html junto al server: uso el panel sencillo"); }
// [8-oct] estado(): lo usaban /estado y el panel viejo, pero NO existía → «ReferenceError: estado is not defined» tumbaba el server
//   (pasaba con cualquier visita: el navegador pide /favicon.ico, que caía en el panel viejo). Ahora existe, y ningún error de una ruta tumba el server.
function estado() { return { arranque: S.arranque, migraciones: S.migs, grabando: S.camaras.size, curvas: S.curvas, swaps: S.swaps, errores: S.errores, nacimientos: S.nacHechos, nacFallos: S.nacFallos, solUsd: S.solUsd,
  plan: PLAN ? PLAN.nombre : null, papel: { desde: P.desde, real: P.real, verdad: P.verdad || 0, papel: P.papel, cerradas: P.cerradas, ganadas: P.ganadas, abiertas: P.abiertas, expuesto: P.expuesto || 0, entradas: P.entradas, vetadas: P.vetadas } }; }
const atiende = async (req, res) => {
  if (req.url.startsWith("/favicon")) { res.statusCode = 204; res.end(); return; }
  if (req.method === "GET" && req.url.startsWith("/rescate") && await RESC.atiende(req, res)) return;
  if (req.method === "GET" && req.url.startsWith("/curvas") && await CURVAS.atiende(req, res)) return;   // [18-oct] la muestra de curvas (JSON; /curvas/todo el fichero entero)   // [12-oct] lo del rescate y el registro, para bajarlo al PC
  if (req.method === "POST") { let cuerpo = ""; for await (const ch of req) { cuerpo += ch; if (cuerpo.length > 2000) break; } const q = new URLSearchParams(cuerpo); const u = req.url.split("?")[0];
    const ok = REAL.CLAVE && q.get("clave") === REAL.CLAVE; let msg = !REAL.CLAVE ? "Falta la variable PANEL_CLAVE en Railway: ponle la clave que quieras y vuelve a probar" : "Clave incorrecta: tiene que ser la misma que la variable PANEL_CLAVE de Railway";
    if (ok && u === "/papel-a-cero") { P.anterior = { plan: P.plan, desde: P.desde, hasta: Date.now(), cerradas: P.cerradas, real: P.real, verdad: P.verdad || 0, papel: P.papel };   // [8-oct] poner a cero las cuentas en papel desde el panel (lo de antes queda en P.anterior)
      Object.assign(P, { papel: 0, real: 0, verdad: 0, verdadN: 0, verdadGan: 0, ultVer: [], cerradas: 0, ganadas: 0, entradas: 0, vetadas: 0, sinNac: 0, porVeto: {}, ultimas: [], porMotivo: {}, maxExpuesto: 0, desde: Date.now(), lote2: 0, lote2N: 0, lote2Gan: 0, lote2Con1: 0, lote3: {}, caza: { n: 0, gan: 0, pnl: 0 }, cazaTokens: 0 }); guardaP(); log("📊 cuentas en papel a cero desde el panel"); msg = "Cuentas en papel a cero: empiezan a contar desde ahora (las posiciones abiertas siguen y contarán al cerrar)"; }
    else if (ok && u === "/armar") msg = REAL.arma(); else if (ok && u === "/desarmar") msg = REAL.desarma("a mano desde el panel"); else if (ok && u === "/vender" && q.get("mint")) msg = await REAL.vendeAMano(q.get("mint"));
    if (req.headers["x-panel"]) { res.setHeader("content-type", "application/json"); res.end(JSON.stringify({ ok, msg })); return; }
    res.setHeader("content-type", "text/html; charset=utf-8"); res.end(`<meta http-equiv="refresh" content="2;url=/"><p style="font:16px system-ui;padding:20px">${msg}</p>`); return; }
  const json = (o) => { res.setHeader("content-type", "application/json"); res.setHeader("cache-control", "no-store"); res.end(JSON.stringify(o)); };
  if (req.url.startsWith("/datos")) { const r = REAL.resumen(); const serie = (P.ultVer && P.ultVer.length) ? P.ultVer.filter(o => o.t >= P.desde).slice().reverse().map(o => [o.t, o.v]) : P.ultimas.filter(o => o.t >= P.desde).slice().reverse().map(o => [o.t, o.real]);   // [12-oct] la gráfica, con lo de verdad
    const med = MIGF.adelanto.length ? Math.round(MIGF.adelanto.slice().sort((a, b) => a - b)[Math.floor(MIGF.adelanto.length / 2)]) : null;
    return json({ ahora: Date.now(), hora: horaEsp(Date.now()), plan: PLAN ? { nombre: PLAN.nombre, reglas: PLAN.reglas.length, vetos: PLAN.reglas.filter(x => x.acc < 0).length } : null, lote: LOTE_SOL, solUsd: S.solUsd || 0,
      papel: { desde: P.desde, real: +P.real.toFixed(4), verdad: +(P.verdad || 0).toFixed(4), verdadN: P.verdadN || 0, papel: +P.papel.toFixed(4), cerradas: P.cerradas, ganadas: P.ganadas, abiertas: P.abiertas, expuesto: P.expuesto || 0, entradas: P.entradas, vetadas: P.vetadas, porVeto: P.porVeto || {}, sinNac: P.sinNac, porMotivo: P.porMotivo,
        lote2: +(P.lote2 || 0).toFixed(4), lote2N: P.lote2N || 0, lote2Abiertas: P.lote2Abiertas || 0, lote3: +Object.values(P.lote3 || {}).reduce((s, a) => s + a.pnl, 0).toFixed(4), lote3N: Object.values(P.lote3 || {}).reduce((s, a) => s + a.n, 0), lote3Abiertas: P.lote3Abiertas || 0,
        caza: +((P.caza || {}).pnl || 0).toFixed(4), cazaN: (P.caza || {}).n || 0, cazaTokens: P.cazaTokens || 0, cazaAbiertas: P.cazaAbiertas || 0, lote2Sol: LOTE2_SOL, lote3Sol: LOTE3_SOL, cazaSol: CAZA_SOL }, serie,
      real: { listo: r.listo, armado: r.armado, motivo: r.motivo, simulado: r.simulado, saldo: r.saldo, totales: r.totales, hoy: r.hoy, topes: r.topes, pendientes: r.pendientes, huerfanos: Object.entries(r.huerfanos || {}).map(([m, h]) => ({ mint: m, sym: h.sym })) },
      salud: { arranque: S.arranque || null, sinGrabar: S.sinGrabar || {}, migraciones: S.migs, grabando: S.camaras.size, curvas: S.curvas, nacimientos: S.nacHechos, nacFallos: S.nacFallos, errores: S.errores, heliusPrimero: MIGF.helius, pumpportalPrimero: MIGF.pumpportal, adelantoMs: med, rescate: RESC.resumen(), creditos: creditos() } }); }
  if (req.url.startsWith("/tokens")) { const out = []; for (const d of DIARIO.values()) { if (d.ballena) continue; const c = S.camaras.get(d.mint);   // [23-oct] sin las ballenas de siempre (Tony)
      const pts = c ? c.puntos : (d.puntos || []).map(([t, p]) => ({ t, p })); const ult = pts.length ? pts[pts.length - 1] : null;
      const abiertas = c && c.motor ? c.motor.sb.state.posiciones.filter(q => q.estado === "ABIERTA" && (q.token === d.mint || q.poolId === d.mint)) : [];
      let abierta = null; if (abiertas.length && ult) { const comp = abiertas.flatMap(q => q.compras || []); const media = comp.length ? comp.reduce((a, q) => a + q.precio, 0) / comp.length : 1; abierta = { lotes: comp.length, entraT: comp.length ? Math.round(d.t0 + 1000 * Math.min(...comp.map(q => q.t))) : null, va: +(((1 + ult.p / 100) / media - 1) * 100).toFixed(1) }; }
      const sale = d.ops.length ? d.ops[d.ops.length - 1].saleT : null; let despues = null; if (sale) for (const q of pts) if (d.t0 + q.t * 1000 > sale && (!despues || q.p > despues.p)) despues = { t: Math.round(q.t), p: +(+q.p).toFixed(1) };
      const mx = c ? c.max : d.max; out.push({ mint: d.mint, sym: d.sym, t0: d.t0, ten: d.ten, seg: d.seg, top5: d.top5, regla: d.regla, nota: d.nota, veto: d.veto, ops: d.ops, l1: d.l1 || null, l2: d.l2 || null, l3: d.l3 || null, caza: d.caza || null, abierta, grabando: !!c, max: mx ? { t: Math.round(mx.t), p: +(+mx.p).toFixed(1) } : null, fin: ult ? +(+ult.p).toFixed(1) : d.fin, despues }); }
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
<div class="c k"><div>DE VERDAD<b class="${(P.verdad || 0) >= 0 ? "v" : "r"}">${d(P.verdad || 0)}</b></div><div>real estimado (viejo)<b>${d(P.real)}</b></div><div>papel<b>${d(P.papel)}</b></div>
<div>operaciones<b>${P.cerradas}</b><small>${P.ganadas} ganadas (${P.cerradas ? Math.round(100 * P.ganadas / P.cerradas) : 0} %)</small></div><div>abiertas ahora<b>${P.abiertas}</b><small>${(P.expuesto || 0).toFixed(2)} SOL · máx ${P.maxExpuesto} SOL</small></div>
<div>tokens al motor<b>${P.entradas}</b></div><div>vetados<b>${P.vetadas}</b><small>🐋 ${(P.porVeto || {}).ballena || 0} ballenas · 📦 ${(P.porVeto || {}).bundle || 0} bundles (${((S.sinGrabar || {}).ballena || 0) + ((S.sinGrabar || {}).bundle || 0)} sin grabar) · 📉 ${(P.porVeto || {}).cae || 0} ya caían · 📈 ${(P.porVeto || {}).nosube || 0} no subían · ${P.sinNac} sin nacimiento</small></div></div>
${(() => { const r = REAL.resumen(); if (!r.listo) return `<div class="c"><b>💰 Real</b><br><small>sin cartera: el server solo hace papel. Para operar de verdad, pon SOLANA_PRIVATE_KEY y PANEL_CLAVE en Railway.</small></div>`;
  const cu = r.cuadres.slice(0, 15).map(c => `<tr><td>${new Date(c.t).toLocaleTimeString("es-ES", { timeZone: "Europe/Madrid" })}</td><td>${c.sym}</td><td>${c.compras}</td><td class="${c.real >= 0 ? "v" : "r"}">${c.real >= 0 ? "+" : ""}${c.real.toFixed(4)}</td><td>${c.papel >= 0 ? "+" : ""}${c.papel.toFixed(4)}</td><td>${c.estimado >= 0 ? "+" : ""}${c.estimado.toFixed(4)}</td></tr>`).join("");
  const hu = Object.entries(r.huerfanos).map(([m, h]) => `<form method="post" action="/vender" style="margin:4px 0"><input type="hidden" name="mint" value="${m}"><input name="clave" type="password" placeholder="clave" style="width:90px"> <button>vender ${h.sym}</button> <small>${h.motivo}</small></form>`).join("");
  return `<div class="c" style="border:2px solid ${r.armado ? "#1f7a4b" : "#b3322e"}"><b>💰 Real ${r.simulado ? "(SIMULADO)" : ""}: ${r.armado ? "🟢 ARMADO" : "🔴 desarmado"}</b>${r.motivo ? ` <small>(${r.motivo})</small>` : ""}<br><small>cartera ${String(r.cartera).slice(0, 4)}…${String(r.cartera).slice(-4)} · saldo ${r.saldo != null ? r.saldo.toFixed(4) : "?"} SOL · lote ${r.lote} SOL${r.sombra ? ` (sombra: el papel va con ${r.lotePapel})` : ""} · dentro ${r.dentro} · en cola ${r.cola}${r.latencia && r.latencia.ordenes ? ` · ⏱️ ${r.latencia.ok} órdenes: envío ${r.latencia.envioMed ?? "-"} ms, confirmación ${r.latencia.confMed ?? "-"} ms, total ${r.latencia.totalMed ?? "-"} ms (p90 ${r.latencia.totalP90 ?? "-"}), ${r.latencia.slotsMed ?? "-"} slots · fallos ${r.latencia.fallos} · colgadas ${r.latencia.colgadas}` : ""}</small>
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
  .listen(PORT, () => log(`🌐 panel en :${PORT} (/ , /viejo , /datos , /tokens , /estado , /rescate)`));
// [8-oct] y por si acaso: un error suelto en una promesa se apunta, no tumba el server (perdería las cámaras y las posiciones en papel en curso)
process.on("unhandledRejection", (e) => { S.errores++; log(`⚠️ error sin atrapar: ${e && (e.stack || e.message || e)}`.slice(0, 400)); });

// ── arranque ──
log(`🟣 SOLANA EN PAPEL · ${PLAN ? "PLAN «" + PLAN.nombre + "»: " + PLAN.reglas.length + " reglas (" + PLAN.reglas.filter(r => r.acc < 0).length + " de no entrar) · si ninguna encaja" : "sin plan"}, motor de Pons con ${JSON.stringify(ESTRATEGIA) === JSON.stringify(ESTRELLA2) ? "⭐⭐" : "ESTRATEGIA " + JSON.stringify(ESTRATEGIA)} · veto de ballenas ${VETO_BALLENA ? `sí (la de siempre, sin segundos, salto ×${VETO_SALTO} a los 3 s, una cartera con todo o ≤5 tenedores${VETO_BUNDLE ? "; y los bundles" : "; los bundles NO"}; ${GRABA_VETADAS ? "se graban igual" : `no se graban, salvo 1 de cada ${GRABA_MUESTRA}`})` : "NO"} · no compra si a los 3 s ya cae ${NO_COMPRA_SI_CAE != null ? `un ${NO_COMPRA_SI_CAE} % o más (de verdad, desde el precio de migrar; se graba igual)` : "NO (compra siempre)"} · ${NO_COMPRA_SI_NO_SUBE != null ? `no compra si a los 3 s no ha subido al menos un ${NO_COMPRA_SI_NO_SUBE} % (se graba igual)` : "compra aunque no suba a los 3 s"} · lote ${LOTE_SOL} SOL · 2º lote ${LOTE2_SOL > 0 ? `${LOTE2_SOL} SOL a los ${LOTE2_T} s si entran ≥ ${LOTE2_ENTRA} SOL con ≤ ${LOTE2_SWAPS} swaps, sale al ×${LOTE2_OBJ} o a los ${LOTE2_RELOJ} s (papel)${LOTES_SIN_CORTE ? "; también en los que no compra el 1er lote por el precio a los 3 s" : ""}` : "apagado"} · 3er lote ${LOTE3_SOL > 0 ? `${LOTE3_SOL} SOL al momentum: en [${LOTE3_DESDE}-${LOTE3_HASTA}] s cuando entran ≥ ${LOTE3_UMBRALES.join("/")} SOL netos en ${LOTE3_VENTANA} s (un lote por umbral), sale al ×${LOTE3_OBJ} o a los ${LOTE3_RELOJ} s (papel)` : "apagado"} · 🎯 cazador ${CAZA_SOL > 0 ? `${CAZA_SOL} SOL en ${CAZA_COMBOS.length} mini-lotes de ${+(CAZA_SOL / CAZA_COMBOS.length).toFixed(4)}, solo con el 2º lote ya dentro: ×${CAZA_COMBOS[0].X} y SOL netos entrando (${[...new Set(CAZA_COMBOS.map(k => k.F + " en " + k.W + " s"))].join(" / ")}), sale con −${Math.round(CAZA_COMBOS[0].trail * 100)} % desde el máximo, ventas de ${CAZA_COMBOS[0].G} SOL en 30 s o a los ${[...new Set(CAZA_COMBOS.map(k => k.H / 60))].join("/")} min (papel)` : "apagado"} · espera del nacimiento ${NAC_ESPERA_S} s · cuentas DE VERDAD: precio con ${RESERVA_OCULTA} SOL apartados, comisión de PumpSwap por tramo, orden ${RETRASO_S} s tarde, ${TX_SOL} SOL por transacción · cuentas en ${ESTADO_FILE}${P.cerradas ? ` (sigue: ${P.cerradas} cerradas, real ${P.real.toFixed(3)} SOL)` : ""}`);
log(`🟣 GRABADOR DE SOLANA · solo graba (no opera) · cámara ${CAMARA_MIN} min · latido ${LATIDO_S} s · muerto a los ${MUERTO_MIN} min · hasta ${WS_MAX} conexiones de Helius con ${MAX_SUBS_WS} suscripciones cada una · nacimiento por ${PUMP_API ? "pump.fun (y Helius si falla)" : "HELIUS (tenedores al migrar, concentración y edad)"} · precio ${MODO_WS ? "por WebSocket, swap a swap" : `leyendo la piscina cada ${POLL_S} s (getMultipleAccounts)`}`);
log(RESC.arranqueTxt());
await REAL.init({ log, papelDe });
// [19-oct] UNA VEZ: las líneas que Railway cortó (más de 81.920 caracteres: [SOLREC] de los tokens más movidos) se vuelven a sacar por la consola, en
//   trozos [PARTE … ts=<hora original>], leyéndolas del registro del disco (rescate/registro/AAAA-MM-DD.log[.gz]). Se hace una vez (marca
//   rescate/reemitidas_v2.txt) desde el 1-oct; REEMITE_DESDE=AAAA-MM-DD lo repite desde ese día. Solo consola: el disco no se toca.
//   DESPACIO (REEMITE_PAUSA_MS 400 entre trozos, REEMITE_PAUSA_LINEA_MS 1500 entre líneas, esperando a que la consola saque cada trozo): la 1ª vez
//   salieron 132 trozos de 75.000 de golpe y Railway solo guardó los últimos 22.
async function reemiteLargas() {
  const base = process.env.RESCATE_DIR || path.join(fs.existsSync("/data") ? "/data" : "/tmp", "rescate"); const dir = path.join(base, "registro"), marca = path.join(base, "reemitidas_v2.txt");   // v2: la v1 salió de golpe y Railway se quedó solo con el final
  const PAUSA_TROZO = +(process.env.REEMITE_PAUSA_MS ?? 400), PAUSA_LINEA = +(process.env.REEMITE_PAUSA_LINEA_MS ?? 1500); const duerme = (ms) => ms > 0 ? new Promise(r => setTimeout(r, ms)) : Promise.resolve();
  const escribe = (t) => new Promise(r => { try { if (process.stdout.write(t + "\n")) r(); else process.stdout.once("drain", r); } catch { r(); } });   // espera a que la consola lo haya sacado
  const pedido = String(process.env.REEMITE_DESDE || "").trim(); if (!pedido && fs.existsSync(marca)) return; const d0 = /^\d{4}-\d{2}-\d{2}$/.test(pedido) ? pedido : "2026-10-01"; const CORTE = 81900;
  let L = []; try { L = fs.readdirSync(dir).filter(f => /^\d{4}-\d{2}-\d{2}\.log(\.gz)?$/.test(f) && f.slice(0, 10) >= d0).sort(); } catch { return; }
  let n = 0, trozos = 0, leidas = 0;
  for (const f of L) { try { const st = fs.createReadStream(path.join(dir, f)); const ent = f.endsWith(".gz") ? st.pipe(zlib.createGunzip()) : st; const rl = readline.createInterface({ input: ent, crlfDelay: Infinity });
      for await (const l of rl) { leidas++; if (l.length < CORTE) continue; let x; try { x = JSON.parse(l); } catch { continue; } const m = x && x.message; if (typeof m !== "string" || m.length < CORTE) continue;
        const T = trozosDe(m, x.timestamp); for (const t of T) { await escribe(t); await duerme(PAUSA_TROZO); } trozos += T.length; n++; await duerme(PAUSA_LINEA); } }
    catch (e) { log(`⚠️ re-emitir ${f}: ${(e && e.message || "").slice(0, 80)}`); } }
  try { fs.writeFileSync(marca, `${new Date().toISOString()} · ${n} líneas desde ${d0}\n`); } catch {}
  log(`[REEMITE] ${n} líneas largas (las que Railway cortó) vueltas a sacar en ${trozos} trozos desde ${d0} · ${L.length} días del registro, ${leidas} líneas leídas`); }
setTimeout(() => reemiteLargas().catch(e => log(`⚠️ re-emitir: ${e && e.message}`)), +(process.env.REEMITE_ESPERA_S || 90) * 1000);
await precioSol(); setInterval(precioSol, 5 * 60e3); log(`💱 SOL a $${S.solUsd.toFixed(2)}`);
try { CURVAS = CURVASM.crea({ log, pumpApi, solUsd: () => S.solUsd, clave: REAL.CLAVE || "" }); } catch (e) { log(`⚠️ la muestra de curvas no arranca (${e && e.message}): el server sigue sin ella`); }   // [18-oct]
log(CURVAS.arranqueTxt());
if ((process.env.MIRA_CARTERA || "").trim()) { const A = process.env.MIRA_CARTERA.trim(); log(`🔎 MIRA_CARTERA: a los 45 s leo las últimas ${+(process.env.MIRA_N || 300)} transacciones de ${A} (Helius, ~${Math.ceil(+(process.env.MIRA_N || 300) / 100) * 10} créditos) → líneas [CARTERA]`);   // [18-oct] de dónde le llegan los tokens a una cartera (una vez por arranque)
  setTimeout(() => MIRA.mira(A, { rpc, log, n: Math.min(1000, +(process.env.MIRA_N || 300)) }).catch(e => log(`⚠️ MIRA_CARTERA: ${e && e.message}`)), 45e3); }
if ((process.env.MIRA_CARTERAS || "").trim()) { const L = process.env.MIRA_CARTERAS.split(/[,\s]+/).map(x => x.trim()).filter(Boolean); log(`🔎 MIRA_CARTERAS: a los 60 s miro ${L.length} carteras (${+(process.env.MIRA_N2 || 100)} tx cada una, ~${L.length * 10} créditos) → líneas [CARTERA62]`);   // [18-oct] el repaso de las 62 «bots»: ¿bot de verdad o app que firma por otros?
  setTimeout(() => MIRA.miraVarias(L, { rpc, log, n: +(process.env.MIRA_N2 || 100) }).catch(e => log(`⚠️ MIRA_CARTERAS: ${e && e.message}`)), 60e3); }
if ((process.env.MIRA_BOTS || "").trim()) { const L = process.env.MIRA_BOTS.split(/[,\s]+/).map(x => x.trim()).filter(Boolean); const nB = +(process.env.MIRA_BOTS_TX || 2000); log(`🔎 MIRA_BOTS: a los 90 s leo lo que compraron ${L.length} bots de la curva hace ${+(process.env.MIRA_BOTS_DESDE_H || 30)}-${+(process.env.MIRA_BOTS_HASTA_H || 24)} h (hasta ${nB} tx cada uno, ~${Math.ceil(nB / 100) * 10 * L.length} créditos) → líneas [BOT]`);   // [18-oct] ¿qué parte de lo que compran migra?
  setTimeout(() => MIRA.miraBots(L, { rpc, log, n: nB, vistas: S.vistas, desdeH: +(process.env.MIRA_BOTS_DESDE_H || 30), hastaH: +(process.env.MIRA_BOTS_HASTA_H || 24) }).catch(e => log(`⚠️ MIRA_BOTS: ${e && e.message}`)), 90e3); }
if (MODO_WS) heliusNueva(); abrePumpPortal();
// [19-oct] APAGAR DE VERDAD: con «npm start» la señal no le llegaba al server y Railway lo mataba sin esperar (0 s): cada despliegue o cambio de
//   variable perdía TODAS las cámaras abiertas (sin [SOLREC], sin rescate, sin cuentas: 37 tokens el 18-oct). Ahora railway.toml arranca con
//   «node server.js» y da 20 s (drainingSeconds): se cierran las cámaras (y sus lotes de papel), se guarda todo y, si hay órdenes reales en marcha
//   (ventas del cierre), se esperan hasta 15 s.
let APAGANDO = false;
process.on("SIGTERM", () => { if (APAGANDO) return; APAGANDO = true; const n = S.camaras.size;
  for (const c of [...S.camaras.values()]) { try { cierra(c, "apagado"); } catch (e) {} } try { guardaP(); } catch (e) {} try { CURVAS.cierra(); } catch (e) {}
  log(`🛑 apagando (SIGTERM): ${n} cámaras cerradas y guardadas${REAL.ocupado() ? " · espero a las órdenes reales en marcha (máx 15 s)" : ""}`);
  const hasta = Date.now() + 15000; const sal = () => { if (!REAL.ocupado() || Date.now() > hasta) { try { RESC.cierra(); } catch (e) {} setTimeout(() => process.exit(0), 300); } else setTimeout(sal, 250); }; setTimeout(sal, 500); });   // [12-oct] la cola del rescate se guarda: al volver, los cortados se rehacen
