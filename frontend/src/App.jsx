import { useState, useEffect, useCallback } from "react";

const BACKEND_WS = import.meta.env.VITE_BACKEND_WS || "ws://localhost:3001";
const BACKEND_HTTP = import.meta.env.VITE_BACKEND_HTTP || "http://localhost:3001";

function formatMC(n) {
 if (!n) return "—";
 if (n >= 1_000_000) return `$${(n/1_000_000).toFixed(2)}M`;
 if (n >= 1_000) return `$${(n/1_000).toFixed(1)}K`;
 return `$${Math.round(n)}`;
}
function formatTime(ts) { return new Date(ts).toLocaleTimeString("es-ES", { hour12: false }); }
function formatDate(ts) { return new Date(ts).toLocaleDateString("es-ES", { day: "2-digit", month: "2-digit" }); }
function toDateKey(ts) { return new Date(ts).toISOString().split("T")[0]; }
function elapsed(ts) {
 const s = Math.floor((Date.now() - ts) / 1000);
 if (s < 60) return `${s}s`;
 if (s < 3600) return `${Math.floor(s/60)}m${s%60}s`;
 return `${Math.floor(s/3600)}h${Math.floor((s%3600)/60)}m`;
}
function pctColor(p) { return p >= 0 ? "#22c55e" : "#ef4444"; }

// ── BACKEND HOOK ───────────────────────────────────────────────
function useBackend() {
 const [migWatching, setMigWatching] = useState([]);
 const [migMonitored, setMigMonitored] = useState([]);
 const [momMonitored, setMomMonitored] = useState([]);
 const [signals, setSignals] = useState([]);
 const [rechazadas, setRechazadas] = useState([]);   // [31-ago] migraciones descartadas por los filtros
 const [demoTrades, setDemoTrades] = useState([]);
 const [realTrades, setRealTrades] = useState([]);
 const [movements, setMovements] = useState([]);
 const [log, setLog] = useState([]);
 const [stats, setStats] = useState({});
 const [postCierre, setPostCierre] = useState({});   // [5-ago] qué hizo el token DESPUÉS de vender
 const [shadow, setShadow] = useState(null);
 const [setFzJuicio] = useState(null);
 const [wsStatus, setWsStatus] = useState("connecting");

 useEffect(() => {
   let ws; let t;
   const connect = () => {
     setWsStatus("connecting");
     ws = new WebSocket(BACKEND_WS);
     ws.onopen = () => setWsStatus("connected");
     ws.onmessage = (evt) => {
       try {
         const { event, data } = JSON.parse(evt.data);
         if (event === "fullState") {
           setMigWatching(data.migWatching || []);
           setMigMonitored(data.migMonitored || []);
           setMomMonitored(data.momMonitored || []);
           setSignals(data.signals || []);
           if (Array.isArray(data.rechazadas)) setRechazadas(data.rechazadas);   // [31-ago] con sus veredictos
           setDemoTrades(data.demoTrades || []);
           setRealTrades(data.realTrades || []);
           setMovements(data.movements || []);
           setLog(data.log || []);
           setStats(data.stats || {});
           setShadow(data.shadow || null);
           setWsStatus(data.wsStatus || "connected");
           return;
         }
         if (event === "stats") { setStats(data); return; }
         if (event === "postCierre") { setPostCierre(p => ({ ...p, [data.mint]: data })); return; }   // [5-ago]
         if (event === "shadow") { setShadow(data); return; }
         if (event === "migWatchUpdate") { setMigWatching(p => p.map(w => w.mint === data.mint ? { ...w, ...data } : w)); return; }
         if (event === "newMigToken") { setMigMonitored(p => p.find(t => t.mint === data.mint) ? p : [data, ...p]); return; }
         if (event === "migTokenUpdate") { setMigMonitored(p => p.map(t => t.mint === data.mint ? { ...t, ...data } : t)); return; }
         if (event === "removeToken") { setMigMonitored(p => p.filter(t => t.mint !== data.mint)); setMomMonitored(p => p.filter(t => t.mint !== data.mint)); return; }
         if (event === "newSignal") { setSignals(p => [data, ...p].slice(0, 100)); if (navigator.vibrate) navigator.vibrate([200,100,200]); return; }
         if (event === "migRechazada") { setRechazadas(p => [data, ...p].slice(0, 150)); return; }
         if (event === "migRechazadaVeredicto") { setRechazadas(p => p.map(r => r.id === data.id ? { ...r, veredicto: data.veredicto, maxVisto: data.maxVisto } : r)); return; }
         if (event === "newDemoTrade" || event === "demoTradeOpened") { const d = { ...data, _lastUp: Date.now() }; setDemoTrades(p => p.find(t => t.id === d.id) ? p : [d, ...p].slice(0, 500)); return; }
         if (event === "demoTradeUpdate") { setDemoTrades(p => p.map(t => t.id === data.id ? { ...t, ...data, _lastUp: Date.now() } : t)); return; }
         if (event === "demoTradeClosed") { setDemoTrades(p => p.map(t => t.id === data.id ? { ...data, _lastUp: Date.now() } : t)); return; }
         if (event === "newRealTrade" || event === "realTradeOpened") { const d = { ...data, _lastUp: Date.now() }; setRealTrades(p => p.find(t => t.id === d.id) ? p : [d, ...p].slice(0, 200)); return; }
         if (event === "realTradeUpdate") { setRealTrades(p => p.map(t => t.id === data.id ? { ...t, ...data, _lastUp: Date.now() } : t)); return; }
         if (event === "realTradeClosed") { setRealTrades(p => p.map(t => t.id === data.id ? { ...data, _lastUp: Date.now() } : t)); return; }
         if (event === "newMovement") { setMovements(p => [...p, data]); return; }
         if (event === "movementDeleted") { setMovements(p => p.filter(m => m.id !== data.id)); return; }
         if (event === "log") { setLog(p => [data, ...p].slice(0, 200)); return; }
       } catch {}
     };
     ws.onerror = () => setWsStatus("error");
     ws.onclose = () => { setWsStatus("disconnected"); t = setTimeout(connect, 4000); };
   };
   connect();
   return () => { ws?.close(); clearTimeout(t); };
 }, []);

 return { migWatching, migMonitored, momMonitored, signals, rechazadas, demoTrades, realTrades, movements, setMovements, log, stats, shadow, wsStatus, postCierre };
}

// ── COMPONENTES ────────────────────────────────────────────────
function StrategyBadge({ strategy }) {
 const map = {
   migration:   { label: "🌉 MIG",   color: "#facc15", bg: "#3b2f00" },
   unida:       { label: "🤝 UNI",   color: "#22c55e", bg: "#052e16" },
   unida2:      { label: "🧼 UNI-W", color: "#0ea5e9", bg: "#082f49" },
 };
 const s = map[strategy] || { label: (strategy||"?").toUpperCase(), color: "#94a3b8", bg: "#1e2d40" };
 return (
   <span style={{ fontSize: 9, fontFamily: "monospace", color: s.color, background: s.bg, padding: "1px 5px", borderRadius: 6 }}>
     {s.label}
   </span>
 );
}

function TradeCard({ trade, isReal, post: postLive }) {
 const [abierto, setAbierto] = useState(false);   // [5-ago] desglose plegable
 const isOpen = trade.status === "OPEN";
 const sym = trade.symbol && trade.symbol !== "???" ? trade.symbol : `${(trade.mint||"").slice(0,6)}…`;
 const slPct = trade.slPct ?? (trade.sl && trade.entryPrice ? ((trade.sl / trade.entryPrice) - 1) * 100 : null);
 const beat = isOpen && trade._lastUp ? Math.floor((Date.now() - trade._lastUp) / 1000) : null;
 const beatFrio = beat !== null && beat > 45;
 const restanteMs = isOpen && trade.expiresAt ? Math.max(0, trade.expiresAt - Date.now()) : null;
 const restante = restanteMs !== null ? `${Math.floor(restanteMs/60000)}m` : null;
 const isWin = trade.result === "WIN";
 const isLoss = trade.result === "LOSS";
 const color = isOpen ? (isReal ? "#f97316" : "#38bdf8") : isWin ? "#22c55e" : isLoss ? "#ef4444" : "#64748b";
 const statusLabel = isOpen ? (isReal ? "🔴 REAL" : "🔵 DEMO") : isWin ? "✅ WIN" : isLoss ? "❌ LOSS" : trade.result === "EXPIRED" ? "⏱️ EXP" : trade.result?.includes("WIN") ? "⏱️ +EXP" : "⏱️ -EXP";
 // [5-ago] qué hizo el token DESPUÉS de que vendiéramos (la cámara sigue grabando 60 min)
 // [16-ago] el evento en vivo manda; si no hay (p.ej. tras recargar), usamos el guardado en el trade
 const post = postLive || trade.post || null;
 const vered = post ? (post.veredicto || (post.max >= 50 && post.desde >= 15 ? "pronto" : post.max >= 50 ? "repunte" : post.max >= 15 ? "justo" : "bien")) : null;
 const vCol = vered === "pronto" ? "#ef4444" : vered === "justo" ? "#facc15" : "#22c55e";
 const vTxt = vered === "pronto" ? "vendiste pronto" : vered === "repunte" ? "buen cierre (repuntó y murió)"
   : vered === "justo" ? "justo" : "buen cierre";
 return (
   <div style={{ background: "#0d1117", border: `1px solid ${color}${isOpen?"55":"33"}`, borderRadius: 10, padding: "10px 14px" }}>
     <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
       <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
         <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: "#f1f5f9" }}>{sym}</span>
         <span style={{ fontSize: 10, color, fontWeight: 700, background: `${color}22`, padding: "1px 6px", borderRadius: 10, fontFamily: "monospace" }}>{statusLabel}</span>
         <StrategyBadge strategy={trade.strategy} />
       </div>
       <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>{formatTime(trade.openTime)}{!isOpen && trade.closeTime ? ` → ${formatTime(trade.closeTime)}` : ""}</span>
     </div>
     <div style={{ display: "flex", gap: 0, border: "1px solid #1e2d40", borderRadius: 8, overflow: "hidden", marginBottom: 6 }}>
       {[
         { label: "Actual", value: `${(trade.currentPct||0)>0?"+":""}${(trade.currentPct||0).toFixed(1)}%`, color: pctColor(trade.currentPct||0) },
         { label: "Max ↑", value: `+${(trade.maxGainPct||0).toFixed(1)}%`, color: "#22c55e" },
         { label: "Min ↓", value: `${(trade.maxLossPct||0).toFixed(1)}%`, color: "#ef4444" },
         { label: "🛑 SL", value: slPct === null ? "—" : `${slPct >= 0 ? "+" : ""}${slPct.toFixed(1)}%`, color: slPct === null ? "#64748b" : slPct >= 0 ? "#22c55e" : "#ef4444" },
         { label: "Trailing", value: trade.trailingPhase||"INITIAL", color: trade.trailingPhase !== "INITIAL" ? "#facc15" : "#64748b" },
         { label: isOpen ? "⏱️" : "Dur", value: isOpen ? elapsed(trade.openTime) : `${Math.round(((trade.closeTime||Date.now())-trade.openTime)/1000)}s`, color: "#94a3b8" },
       ].map((m, i) => (
         <div key={i} style={{ flex: 1, padding: "5px 4px", textAlign: "center", borderRight: i < 5 ? "1px solid #1e2d40" : "none" }}>
           <div style={{ fontSize: 9, color: "#64748b", textTransform: "uppercase", marginBottom: 2 }}>{m.label}</div>
           <div style={{ fontFamily: "monospace", fontSize: 10, fontWeight: 700, color: m.color }}>{m.value}</div>
         </div>
       ))}
     </div>
     {!isOpen && <div style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginBottom: 4 }}>P&L: <span style={{ color: pctColor(trade.pnlPct||0) }}>{(trade.pnlPct||0)>0?"+":""}{(trade.pnlPct||0).toFixed(2)}%</span>{isReal && trade.pnlSol !== null && <span style={{ color: pctColor(trade.pnlSol||0), marginLeft: 8 }}>{(trade.pnlSol||0)>0?"+":""}{(trade.pnlSol||0).toFixed(4)} SOL</span>}</div>}
     {isReal && trade.buySignature && <div style={{ fontFamily: "monospace", fontSize: 9, color: "#64748b", marginBottom: 2 }}>Buy: <a href={`https://solscan.io/tx/${trade.buySignature}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8", textDecoration: "none" }}>{trade.buySignature.slice(0,12)}…</a></div>}
     {isReal && trade.sellSignature && <div style={{ fontFamily: "monospace", fontSize: 9, color: "#64748b", marginBottom: 4 }}>Sell: <a href={`https://solscan.io/tx/${trade.sellSignature}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8", textDecoration: "none" }}>{trade.sellSignature.slice(0,12)}…</a></div>}
     {!isOpen && post && (
       <div style={{ fontFamily: "monospace", fontSize: 10, marginBottom: 5, padding: "4px 7px", background: `${vCol}14`, border: `1px solid ${vCol}44`, borderRadius: 7, display: "flex", justifyContent: "space-between", flexWrap: "wrap", gap: 6 }}>
         <span style={{ color: "#94a3b8" }}>
           🔭 tras vender: <b style={{ color: post.desde >= 0 ? "#22c55e" : "#ef4444" }}>{post.desde >= 0 ? "+" : ""}{post.desde}%</b>
           {post.max > 0 && <span style={{ color: "#64748b" }}> · pico <b style={{ color: "#22c55e" }}>+{post.max}%</b></span>}
           {post.mcAhora > 0 && <span style={{ color: "#64748b" }}> · {formatMC(post.mcAhora)}</span>}
           <span style={{ color: "#475569" }}> · {post.min}m{post.final ? "" : "/120m"}</span>
           {post.vivo === false && <span style={{ color: "#f59e0b" }}> 🔇</span>}
         </span>
         <span style={{ color: vCol, fontWeight: 700 }}>{post.final ? (vered === "pronto" ? "❌ " : vered === "justo" ? "🟡 " : "✅ ") : "⏳ "}{vTxt}</span>
       </div>
     )}
     <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
       <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
         <a href={`https://pump.fun/coin/${trade.mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 9, color: "#38bdf8", textDecoration: "none" }}>💊 pump.fun</a>
         <button onClick={() => setAbierto(v => !v)} style={{ fontFamily: "monospace", fontSize: 9, color: "#94a3b8", background: "#131c2b", border: "1px solid #1e2d40", borderRadius: 6, padding: "2px 7px", cursor: "pointer" }}>
           {abierto ? "▾ detalle" : "▸ detalle"}
         </button>
       </div>
       {isOpen && (
         <div style={{ display: "flex", gap: 10, fontFamily: "monospace", fontSize: 9 }}>
           {restante !== null && <span style={{ color: restanteMs < 300000 ? "#facc15" : "#475569" }}>⏳ {restante}</span>}
           <span style={{ color: beatFrio ? "#f97316" : "#475569" }}>📶 {beat === null ? "—" : `${beat}s`}{beatFrio ? " 🥶" : ""}</span>
         </div>
       )}
     </div>
     {abierto && <TradeDetalle trade={trade} isOpen={isOpen} post={post} />}
   </div>
 );
}

// [5-ago] desglose de la op: market caps, dinero y lo que dejó sobre la mesa
// ═══════════ [27-ago] VISTA NUEVA DEL DEMO ═══════════
// 1 tarjeta por TOKEN (no por trade) · minigráfico · 3 cifras · color por resultado ·
// agrupado por día con su resumen · insignias de las reglas de protección.

const SOL_NETO = (t) => (t.sizeSol || 0.5) * (((t.pnlPct || 0) - 4.5) / 100);

// minigráfico: la curva del token con los hitos marcados
function Spark({ datos, compras, salida, alto = 42 }) {
  if (!datos || datos.length < 3) return null;
  const W = 150, H = alto, min = Math.min(...datos), max = Math.max(...datos);
  const rango = Math.max(1, max - min);
  const x = (i) => (i / (datos.length - 1)) * (W - 4) + 2;
  const y = (v) => H - 3 - ((v - min) / rango) * (H - 8);
  const d = datos.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const cero = min <= 0 && max >= 0 ? y(0) : null;
  const fin = datos[datos.length - 1];
  const col = fin >= 0 ? "#22c55e" : "#ef4444";
  // los hitos se colocan por proporción de tiempo (la curva ya viene reducida)
  const punto = (frac, color) => {
    const i = Math.max(0, Math.min(datos.length - 1, Math.round(frac * (datos.length - 1))));
    return <circle cx={x(i)} cy={y(datos[i])} r="2.4" fill={color} />;
  };
  return (
    <svg width={W} height={H} style={{ display: "block" }}>
      {cero != null && <line x1="0" y1={cero} x2={W} y2={cero} stroke="#1e2d40" strokeWidth="1" strokeDasharray="2 3" />}
      <path d={d} fill="none" stroke={col} strokeWidth="1.6" strokeLinejoin="round" />
      {(compras || []).map((f, k) => <g key={k}>{punto(f, "#22c55e")}</g>)}
      {salida != null && punto(salida, "#fb923c")}
    </svg>
  );
}

// insignia del motivo de cierre
function Insignia({ reason }) {
  const M = { MUERTO: ["💀", "#ef4444", "liquidado por muerto"], ROJA: ["🔴", "#22c55e", "vendido en la envolvente"],
    SL: ["🛑", "#f97316", "stop del bot"], NO_LAUNCH: ["✂️", "#64748b", "cortado a los 10s"],
    EXPIRED: ["⏱️", "#94a3b8", "fin de cámara"], PSL: ["🩹", "#facc15", "stop del paquete"] };
  const m = M[reason]; if (!m) return null;
  return <span title={m[2]} style={{ fontSize: 11, color: m[1], background: m[1] + "18", border: "1px solid " + m[1] + "44",
    borderRadius: 5, padding: "1px 5px", fontWeight: 700 }}>{m[0]} {reason}</span>;
}

// TARJETA POR TOKEN: junta la pierna del bot y sus paquetes
function TokenCard({ mint, trades, post }) {
  const [abierto, setAbierto] = useState(false);
  const bot = trades.find(t => t.trailingPhase === "UNI_BOT") || trades.find(t => t.strategy !== "unida");
  const packs = trades.filter(t => t !== bot);
  const vivo = trades.some(t => t.status === "OPEN");
  const sol = trades.filter(t => t.status !== "OPEN").reduce((s, t) => s + SOL_NETO(t), 0);
  const ini = Math.min(...trades.map(t => t.openTime));
  const fin = Math.max(...trades.map(t => t.closeTime || Date.now()));
  const dur = Math.round((fin - ini) / 1000);
  const sym = (trades[0].symbol && trades[0].symbol !== "???") ? trades[0].symbol : mint.slice(0, 6);
  const conCurva = trades.find(t => t.spark && t.spark.length > 2);
  const lotes = packs.reduce((s, p) => s + Math.round((p.sizeSol || 0.5) / 0.5), 0);
  // recorrido de la op: lo mejor y lo peor que llegó a marcar cualquiera de sus piernas
  // [28-ago] posición viva: % actual del paquete (ponderado por lotes) y SOL flotantes
  const abiertos = trades.filter(t => t.status === "OPEN");
  const solVivo = abiertos.reduce((s, t) => s + (t.sizeSol || 0.5) * (((t.currentPct || 0) - 4.5) / 100), 0)
                + trades.filter(t => t.status !== "OPEN").reduce((s, t) => s + SOL_NETO(t), 0);
  const pesoAb = abiertos.reduce((s, t) => s + (t.sizeSol || 0.5), 0) || 1;
  const pctVivo = abiertos.reduce((s, t) => s + (t.currentPct || 0) * (t.sizeSol || 0.5), 0) / pesoAb;
  const maxOp = Math.max(0, ...trades.map(t => t.maxGainPct || 0));
  const minOp = Math.min(0, ...trades.map(t => t.maxLossPct || 0));
  const col = vivo ? "#38bdf8" : sol >= 0 ? "#22c55e" : "#ef4444";
  const strat = trades[0].strategy;
  const esUni = strat === "unida" || strat === "unida2";
  const colBarra = strat === "unida" ? "#22c55e" : strat === "unida2" ? "#0ea5e9" : "#facc15";
  const fracs = (conCurva && conCurva.compras && conCurva.spark)
    ? conCurva.compras.map(x => Math.min(1, x.t / Math.max(1, dur))) : [];
  return (
    <div style={{ background: "#0d1117", border: "1px solid " + col + "44", borderLeft: "3px solid " + colBarra,
      borderRadius: 10, padding: "10px 12px", marginBottom: 8 }}>
      <div onClick={() => setAbierto(!abierto)} style={{ display: "flex", alignItems: "center", gap: 10, cursor: "pointer" }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
            <span style={{ fontFamily: "monospace", fontWeight: 700, color: "#e2e8f0", fontSize: 14 }}>{sym}</span>
            {strat === "unida2" && <span style={{ fontSize: 10, color: "#0ea5e9", border: "1px solid #0ea5e944", borderRadius: 4, padding: "0 4px" }}>🧼 W</span>}
            {vivo && <span style={{ fontSize: 10, color: "#38bdf8", border: "1px solid #38bdf844", borderRadius: 4, padding: "0 4px" }}>EN CURSO</span>}
            {!vivo && trades.filter(t => t.closeReason).slice(-1).map((t, i) => <Insignia key={i} reason={t.closeReason} />)}
          </div>
          <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
            <span style={{ fontFamily: "monospace", fontSize: 20, fontWeight: 700, color: vivo ? (pctVivo >= 0 ? "#22c55e" : "#ef4444") : (sol >= 0 ? "#22c55e" : "#ef4444") }}>
              {vivo ? (pctVivo >= 0 ? "+" : "") + pctVivo.toFixed(1) + "%" : (sol >= 0 ? "+" : "") + sol.toFixed(2)}
            </span>
            <span style={{ fontSize: 11, color: "#64748b" }}>{vivo ? "ahora" : "SOL"}</span>
            {vivo && <span style={{ fontFamily: "monospace", fontSize: 12, color: solVivo >= 0 ? "#22c55e" : "#ef4444" }}>
              ({solVivo >= 0 ? "+" : ""}{solVivo.toFixed(2)} SOL)</span>}
            <span style={{ fontSize: 12, color: "#94a3b8", marginLeft: 4 }}>
              {lotes > 0 ? `🛒 ×${lotes}` : "🤖 solo bot"} · {dur < 90 ? dur + "s" : Math.round(dur / 60) + "m"}
            </span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 2, fontSize: 11, fontFamily: "monospace" }}>
            <span style={{ color: "#64748b" }}>🕐 {new Date(ini).toLocaleTimeString("es-ES", { hour: "2-digit", minute: "2-digit", second: "2-digit" })}</span>
            <span style={{ color: "#22c55e" }}>▲ +{maxOp.toFixed(0)}%</span>
            <span style={{ color: "#ef4444" }}>▼ {minOp.toFixed(0)}%</span>
          </div>
        </div>
        {conCurva && <Spark datos={conCurva.spark} compras={fracs} salida={bot && bot.closeTime ? (bot.closeTime - ini) / Math.max(1, fin - ini) : null} />}
      </div>
      {abierto && (
        <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #1e2d40", fontFamily: "monospace", fontSize: 12 }}>
          {trades.map(t => (
            <div key={t.id} style={{ marginBottom: 6 }}>
              <div style={{ display: "flex", justifyContent: "space-between", color: "#94a3b8" }}>
                <span>{t.trailingPhase === "UNI_BOT" ? "🤖 pierna del bot" : `🛒 paquete ×${Math.round((t.sizeSol || 0.5) / 0.5)}`}
                  {t.closeReason ? ` · ${t.closeReason}` : t.status === "OPEN" ? " · abierto" : ""}</span>
                <span style={{ color: (t.pnlPct || 0) >= 0 ? "#22c55e" : "#ef4444", fontWeight: 700 }}>
                  {(t.pnlPct || 0) >= 0 ? "+" : ""}{(t.pnlPct || 0).toFixed(1)}% · {t.status === "OPEN" ? "—" : (SOL_NETO(t) >= 0 ? "+" : "") + SOL_NETO(t).toFixed(2) + " SOL"}
                </span>
              </div>
              {t.compras && t.compras.map((x, i) => (
                <div key={i} style={{ display: "flex", justifyContent: "space-between", color: "#475569", paddingLeft: 12 }}>
                  <span>C{i + 1} · t+{x.t}s</span><span style={{ color: x.p >= 0 ? "#16a34a" : "#dc2626" }}>{x.p >= 0 ? "+" : ""}{x.p}%</span>
                </div>
              ))}
            </div>
          ))}
          {trades[0].calidad && (
            <div style={{ color: "#64748b", paddingTop: 4, borderTop: "1px dotted #1e2d40" }}>
              🧼 wash {trades[0].calidad.wash} · 👥 {trades[0].calidad.buyers} compradores · 🐋 top {trades[0].calidad.top}%
            </div>
          )}
          <div style={{ display: "flex", gap: 12, marginTop: 4 }}>
            <a href={`https://pump.fun/coin/${mint}`} target="_blank" rel="noreferrer" style={{ color: "#4ade80", fontSize: 11 }}>💊 pump.fun</a>
            <a href={`https://solscan.io/token/${mint}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8", fontSize: 11 }}>🔎 Solscan</a>
            <span onClick={(e) => { e.stopPropagation(); navigator.clipboard && navigator.clipboard.writeText(mint); }} style={{ color: "#94a3b8", fontSize: 11, cursor: "pointer" }}>📋 copiar mint</span>
          </div>
        </div>
      )}
    </div>
  );
}

// [31-ago] tarjeta de una migración RECHAZADA
function Rechazada({ r, onVeredicto }) {
  const [abierta, setAbierta] = useState(false);
  const [maxTxt, setMaxTxt] = useState(r.maxVisto != null ? String(r.maxVisto) : "");
  const ultimo = r.ultimo == null ? null : r.ultimo;
  const col = ultimo == null ? "#64748b" : ultimo >= 0 ? "#22c55e" : "#ef4444";
  const sym = (r.symbol && r.symbol !== "???") ? r.symbol : r.mint.slice(0, 6);
  const serie = (r.serie || []).map(x => x[1]);
  const maxS = serie.length ? Math.max(...serie) : null, minS = serie.length ? Math.min(...serie) : null;
  return (
    <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderLeft: "3px solid #f97316", borderRadius: 10, padding: "10px 12px", marginBottom: 8 }}>
      <div onClick={() => setAbierta(!abierta)} style={{ display: "flex", alignItems: "center", gap: 10, cursor: "pointer" }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap" }}>
            <span style={{ fontFamily: "monospace", fontWeight: 700, color: "#e2e8f0", fontSize: 14 }}>{sym}</span>
            <span style={{ fontSize: 10, color: "#f97316", background: "#f9731618", border: "1px solid #f9731644", borderRadius: 5, padding: "1px 5px" }}>{r.motivo}</span>
            {r.veredicto === "bien" && <span style={{ fontSize: 10, color: "#22c55e" }}>✅ bien descartada{r.maxVisto != null ? ` (máx +${r.maxVisto}%)` : ""}</span>}
            {r.veredicto === "mal" && <span style={{ fontSize: 10, color: "#ef4444", fontWeight: 700 }}>❌ MAL descartada{r.maxVisto != null ? ` (llegó a +${r.maxVisto}%)` : ""}</span>}
          </div>
          <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginTop: 3, fontFamily: "monospace", fontSize: 12 }}>
            <span style={{ color: col, fontSize: 16, fontWeight: 700 }}>{ultimo == null ? "—" : (ultimo >= 0 ? "+" : "") + ultimo.toFixed(1) + "%"}</span>
            <span style={{ color: "#64748b" }}>al descartar · {r.dur}s vigilada</span>
          </div>
          <div style={{ display: "flex", gap: 10, marginTop: 2, fontSize: 11, fontFamily: "monospace", color: "#64748b" }}>
            <span>🕐 {new Date(r.ts).toLocaleTimeString("es-ES", { hour: "2-digit", minute: "2-digit" })}</span>
            {r.mc != null && <span>MC ${Math.round(r.mc / 1000)}K</span>}
            {maxS != null && <span style={{ color: "#22c55e" }}>▲ +{maxS.toFixed(0)}%</span>}
            {minS != null && <span style={{ color: "#ef4444" }}>▼ {minS.toFixed(0)}%</span>}
          </div>
        </div>
        {serie.length > 2 && <Spark datos={serie} compras={[]} salida={null} />}
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 8, flexWrap: "wrap" }}>
        <input value={maxTxt} onChange={e => setMaxTxt(e.target.value)} onClick={e => e.stopPropagation()} placeholder="máx % visto" inputMode="decimal"
          style={{ width: 92, background: "#161b22", color: "#e6edf3", border: "1px solid #30363d", borderRadius: 8, padding: "8px 8px", fontSize: 13 }} />
        <button onClick={(e) => { e.stopPropagation(); onVeredicto(r.id, "bien", maxTxt); }}
          style={{ flex: 1, minHeight: 38, background: r.veredicto === "bien" ? "#22c55e33" : "#0d1117", border: `1px solid ${r.veredicto === "bien" ? "#22c55e" : "#30363d"}`, color: "#22c55e", borderRadius: 8, fontSize: 13, cursor: "pointer" }}>✅ bien</button>
        <button onClick={(e) => { e.stopPropagation(); onVeredicto(r.id, "mal", maxTxt); }}
          style={{ flex: 1, minHeight: 38, background: r.veredicto === "mal" ? "#ef444433" : "#0d1117", border: `1px solid ${r.veredicto === "mal" ? "#ef4444" : "#30363d"}`, color: "#ef4444", borderRadius: 8, fontSize: 13, cursor: "pointer" }}>❌ mal</button>
        {r.veredicto && <button onClick={(e) => { e.stopPropagation(); onVeredicto(r.id, null, ""); }}
          style={{ minHeight: 38, background: "transparent", border: "1px solid #30363d", color: "#64748b", borderRadius: 8, padding: "0 10px", fontSize: 12, cursor: "pointer" }}>quitar</button>}
      </div>
      {abierta && (
        <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #1e2d40", fontFamily: "monospace", fontSize: 11, color: "#94a3b8" }}>
          <div>volumen ${r.vol} · {r.trades} trades{r.sig != null ? ` · señal ${r.sig >= 0 ? "+" : ""}${r.sig}%` : ""}{r.mov2s != null ? ` · mov2s ${r.mov2s >= 0 ? "+" : ""}${r.mov2s}%` : ""}</div>
          <div style={{ display: "flex", gap: 12, marginTop: 6 }}>
            <a href={`https://pump.fun/coin/${r.mint}`} target="_blank" rel="noreferrer" style={{ color: "#4ade80" }}>💊 pump.fun</a>
            <a href={`https://solscan.io/token/${r.mint}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8" }}>🔎 Solscan</a>
            <span onClick={(e) => { e.stopPropagation(); navigator.clipboard && navigator.clipboard.writeText(r.mint); }} style={{ cursor: "pointer" }}>📋 copiar mint</span>
          </div>
        </div>
      )}
    </div>
  );
}

// curva de banca del día
function CurvaDia({ cierres }) {
  if (!cierres.length) return null;
  const orden = [...cierres].sort((a, b) => (a.closeTime || 0) - (b.closeTime || 0));
  let acc = 0; const serie = orden.map(t => (acc += SOL_NETO(t)));
  const W = 320, H = 54, min = Math.min(0, ...serie), max = Math.max(0, ...serie), r = Math.max(0.5, max - min);
  const x = i => (i / Math.max(1, serie.length - 1)) * (W - 4) + 2;
  const y = v => H - 4 - ((v - min) / r) * (H - 10);
  const d = serie.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const fin = serie[serie.length - 1], col = fin >= 0 ? "#22c55e" : "#ef4444";
  return (
    <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: "10px 12px", marginBottom: 10 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div>
          <div style={{ fontSize: 10, color: "#64748b", letterSpacing: .5 }}>HOY · {cierres.length} cierres</div>
          <div style={{ fontFamily: "monospace", fontSize: 26, fontWeight: 700, color: col }}>{fin >= 0 ? "+" : ""}{fin.toFixed(2)} <span style={{ fontSize: 13, color: "#64748b" }}>SOL</span></div>
        </div>
        <svg width={W / 2} height={H} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
          <line x1="0" y1={y(0)} x2={W} y2={y(0)} stroke="#1e2d40" strokeDasharray="2 3" />
          <path d={d} fill="none" stroke={col} strokeWidth="2" />
        </svg>
      </div>
    </div>
  );
}

function TradeDetalle({ trade, isOpen, post }) {
 const esUni = trade.strategy === "unida";   // [23-ago] detalle limpio para la unida
 const mcE = trade.mcEntry ?? (trade.entryPrice ? trade.entryPrice * 1e9 : null);
 const f = v => 1 + (v || 0) / 100;
 // [5-ago] los MC se DERIVAN siempre del MC de entrada y los porcentajes (que sí están al día).
 // Antes se leía trade.mcMax/mcMin/mcClose, pero el server no los refresca al cerrar y llegaban
 // con el valor inicial: el MC pico salía igual que el de entrada aunque la op hubiera hecho +126%.
 const mcAhora = mcE ? mcE * f(trade.currentPct) : null;
 const mcMax = mcE ? mcE * f(trade.maxGainPct) : (trade.mcMax ?? null);
 const mcMin = mcE ? mcE * f(trade.maxLossPct) : (trade.mcMin ?? null);
 const mcFin = !isOpen && mcE ? mcE * f(trade.pnlPct) : (trade.mcClose ?? null);
 const fmt = v => v == null ? "—" : v >= 1e6 ? `$${(v/1e6).toFixed(2)}M` : `$${(v/1000).toFixed(1)}K`;
 const lote = trade.lote ?? trade.sizeSol ?? null;
 const bruto = trade.brutoSol ?? (lote && trade.pnlPct != null ? lote * trade.pnlPct / 100 : null);
 const neto = trade.netoSol ?? (lote && trade.pnlPct != null ? lote * (trade.pnlPct - 4.5) / 100 : null);
 const dejado = trade.dejadoPts ?? (trade.pnlPct != null ? trade.maxGainPct - trade.pnlPct : null);
 const sol = v => v == null ? "—" : `${v >= 0 ? "+" : ""}${v.toFixed(3)} SOL`;
 const F = ({ k, v, c }) => (
   <div style={{ display: "flex", justifyContent: "space-between", padding: "2px 0" }}>
     <span style={{ color: "#64748b" }}>{k}</span>
     <span style={{ color: c || "#e2e8f0", fontWeight: 700 }}>{v}</span>
   </div>
 );
 return (
   <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px solid #1e2d40", fontFamily: "monospace", fontSize: 10 }}>
     <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 18px" }}>
       <F k="MC entrada" v={fmt(mcE)} />
       <F k="MC pico (en la op)" v={fmt(mcMax)} c="#22c55e" />
       <F k={isOpen ? "MC ahora" : "MC salida"} v={fmt(isOpen ? mcAhora : mcFin)} />
       <F k="MC suelo" v={fmt(mcMin)} c="#ef4444" />
       <F k="lote" v={lote != null ? `${lote} SOL` : "—"} />
       {!esUni && <F k="dejó sin coger" v={dejado != null ? `${dejado.toFixed(0)} pts` : "—"} c="#facc15" />}
       {!isOpen && <F k="bruto" v={sol(bruto)} c={pctColor(trade.pnlPct || 0)} />}
       {!isOpen && <F k="neto (fee 4.5%)" v={sol(neto)} c={neto >= 0 ? "#22c55e" : "#ef4444"} />}
       {trade.velSeg != null && <F k="vel entrada" v={`${trade.velSeg}s`} />}
       {trade.sigPct != null && <F k="señal" v={`+${trade.sigPct}%`} />}
       {trade.mov2s != null && <F k="mov2s" v={`${trade.mov2s >= 0 ? "+" : ""}${trade.mov2s.toFixed(1)}%`} />}
       {trade.cfg?.nom && <F k="config" v={trade.cfg.nom} />}
       {!esUni && post && <F k="MC ahora" v={fmt(post.mcAhora)} c={post.desde >= 0 ? "#22c55e" : "#ef4444"} />}
       {!esUni && post && <F k="MC pico REAL" v={fmt(mcFin != null && post.max != null ? mcFin * (1 + post.max / 100) : null)} c="#facc15" />}
       {!esUni && post && <F k="subió tras vender" v={`${post.max >= 0 ? "+" : ""}${post.max}%`} c="#facc15" />}
       {!esUni && post && <F k="ahora vs salida" v={`${post.desde >= 0 ? "+" : ""}${post.desde}%`} c={post.desde >= 0 ? "#22c55e" : "#ef4444"} />}
     </div>
     {esUni && !trade.compras?.length && (
       <div style={{ marginTop: 6, paddingTop: 6, borderTop: "1px dashed #1e2d40" }}>
         <div style={{ display: "flex", justifyContent: "space-between", padding: "1px 0" }}>
           <span style={{ color: "#94a3b8" }}>t+0s · COMPRA (pierna bot)</span><span style={{ color: "#e2e8f0" }}>+1</span>
         </div>
         {!isOpen && (
           <div style={{ display: "flex", justifyContent: "space-between", padding: "1px 0" }}>
             <span style={{ color: "#fb923c" }}>t+{Math.round(((trade.closeTime || Date.now()) - trade.openTime) / 1000)}s · 🤖 sale el BOT{trade.closeReason ? ` (${trade.closeReason})` : ""}</span>
             <span style={{ color: (trade.pnlPct || 0) >= 0 ? "#22c55e" : "#ef4444", fontWeight: 700 }}>{(trade.pnlPct || 0) >= 0 ? "+" : ""}{(trade.pnlPct || 0).toFixed(1)}%</span>
           </div>
         )}
       </div>
     )}
     {trade.compras?.length > 0 && (
       <div style={{ marginTop: 6, paddingTop: 6, borderTop: "1px dashed #1e2d40" }}>
         <div style={{ color: "#64748b", marginBottom: 2 }}>🛒 paquete del relevo · {trade.compras.length} lote{trade.compras.length > 1 ? "s" : ""} de 0.5 <span style={{ color: "#475569" }}>(% vs la entrada de la op)</span></div>
         {trade.compras.map((c2, i) => (
           <div key={i} style={{ display: "flex", justifyContent: "space-between", padding: "1px 0" }}>
             <span style={{ color: "#94a3b8" }}>C{i + 1} · t+{c2.t}s · COMPRA</span>
             <span style={{ color: c2.p >= 0 ? "#22c55e" : "#ef4444", fontWeight: 700 }}>{c2.p >= 0 ? "+" : ""}{c2.p}%</span>
           </div>
         ))}
         {!isOpen && (
           <div style={{ display: "flex", justifyContent: "space-between", padding: "1px 0", borderTop: "1px dotted #1e2d40", marginTop: 2 }}>
             <span style={{ color: "#fb923c", fontWeight: 700 }}>t+{trade.compras[0].t + Math.round(((trade.closeTime || Date.now()) - trade.openTime) / 1000)}s · VENDE el paquete{trade.closeReason ? ` (${trade.closeReason})` : ""} ×{trade.compras.length}</span>
             <span style={{ color: (trade.pnlPct || 0) >= 0 ? "#22c55e" : "#ef4444", fontWeight: 700 }}>{(trade.pnlPct || 0) >= 0 ? "+" : ""}{(trade.pnlPct || 0).toFixed(1)}% <span style={{ color: "#64748b", fontWeight: 400 }}>de la media</span></span>
           </div>
         )}
       </div>
     )}
     <div style={{ marginTop: 6, display: "flex", gap: 10 }}>
       <a href={`https://solscan.io/token/${trade.mint}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8", fontSize: 9, textDecoration: "none" }}>🔎 Solscan</a>
       <span onClick={() => navigator.clipboard?.writeText(trade.mint)} style={{ color: "#64748b", fontSize: 9, cursor: "pointer" }}>📋 copiar mint</span>
     </div>
   </div>
 );
}

function StatsRow({ label, val, color, desc }) {
 return (
   <div style={{ padding: desc ? "8px 0" : "6px 0", borderBottom: "1px solid #1e2d40" }}>
     <div style={{ display: "flex", justifyContent: "space-between", marginBottom: desc ? 2 : 0 }}>
       <span style={{ fontSize: 12, color: "#94a3b8" }}>{label}</span>
       <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: color || "#f1f5f9" }}>{val}</span>
     </div>
     {desc && <div style={{ fontSize: 10, color: "#475569" }}>{desc}</div>}
   </div>
 );
}

function FilterBar({ statusFilter, setStatusFilter, stratFilter, setStratFilter, accentColor }) {
 return (
   <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
     <div style={{ display: "flex", gap: 6 }}>
       {["all", "open", "closed"].map(f => (
         <button key={f} onClick={() => setStatusFilter(f)} style={{ flex: 1, padding: "6px", border: `1px solid ${statusFilter === f ? accentColor : "#1e2d40"}`, borderRadius: 8, background: statusFilter === f ? `${accentColor}22` : "none", color: statusFilter === f ? accentColor : "#64748b", fontFamily: "monospace", fontSize: 11, cursor: "pointer" }}>
           {f === "all" ? "Todas" : f === "open" ? "Abiertas" : "Cerradas"}
         </button>
       ))}
     </div>
     <div style={{ display: "flex", gap: 6 }}>
       {[{ id: "all", label: "Todas" }, { id: "unida", label: "🤝" }, { id: "unida2", label: "🧼" }, { id: "migration", label: "🌉" }].map(f => (
         <button key={f.id} onClick={() => setStratFilter(f.id)} style={{ flex: 1, padding: "5px", border: `1px solid ${stratFilter === f.id ? "#94a3b8" : "#1e2d40"}`, borderRadius: 8, background: stratFilter === f.id ? "#1e2d4055" : "none", color: stratFilter === f.id ? "#f1f5f9" : "#64748b", fontFamily: "monospace", fontSize: 10, cursor: "pointer" }}>
           {f.label}
         </button>
       ))}
     </div>
   </div>
 );
}

// ── GRÁFICO PNL ────────────────────────────────────────────────
function PnlChart({ realTrades, movements, period }) {
 const now = Date.now();
 const msPerDay = 86_400_000;

 // Generar puntos según período
 const days = period === "daily" ? 7 : period === "weekly" ? 4 : 30;
 const labels = [];
 const pnlData = [];
 const cumData = [];

 let cumulative = 0;

 for (let i = days - 1; i >= 0; i--) {
   const dayStart = new Date(now - i * msPerDay);
   dayStart.setHours(0,0,0,0);
   const dayEnd = new Date(dayStart); dayEnd.setHours(23,59,59,999);

   const dayTrades = realTrades.filter(t =>
     t.status === "CLOSED" && t.closeTime >= dayStart.getTime() && t.closeTime <= dayEnd.getTime()
   );
   const dayPnl = dayTrades.reduce((sum, t) => sum + (t.pnlSol || 0), 0);

   const dayMovements = movements.filter(m => {
     const mDate = new Date(m.date + "T12:00:00");
     return mDate >= dayStart && mDate <= dayEnd;
   });
   const movTotal = dayMovements.reduce((sum, m) => sum + (m.type === "deposit" ? m.amount : -m.amount), 0);

   cumulative += dayPnl;

   if (period === "weekly") {
     const weekLabel = `S${Math.ceil((days - i) / 7)}`;
     if ((days - i) % 7 === 0 || i === days - 1) {
       labels.push(weekLabel);
       pnlData.push(+dayPnl.toFixed(4));
       cumData.push(+cumulative.toFixed(4));
     }
   } else {
     const d = dayStart;
     labels.push(`${d.getDate()}/${d.getMonth()+1}`);
     pnlData.push(+dayPnl.toFixed(4));
     cumData.push(+cumulative.toFixed(4));
   }
 }

 const maxVal = Math.max(...pnlData.map(Math.abs), 0.001);
 const chartH = 120;
 const chartW = 300;
 const barW = Math.floor(chartW / labels.length) - 4;

 return (
   <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 14 }}>
     <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
       <span style={{ fontFamily: "monospace", fontSize: 11, color: "#94a3b8", fontWeight: 700 }}>
         P&L REAL — {period === "daily" ? "7 días" : period === "weekly" ? "4 semanas" : "30 días"}
       </span>
       <span style={{ fontFamily: "monospace", fontSize: 12, color: pctColor(cumulative), fontWeight: 700 }}>
         {cumulative >= 0 ? "+" : ""}{cumulative.toFixed(4)} SOL
       </span>
     </div>

     {/* Barras */}
     <div style={{ overflowX: "auto" }}>
       <svg width={Math.max(chartW, labels.length * (barW + 4))} height={chartH + 30} style={{ display: "block" }}>
         {/* Línea cero */}
         <line x1="0" y1={chartH/2} x2={chartW} y2={chartH/2} stroke="#1e2d40" strokeWidth="1" strokeDasharray="4,4" />

         {labels.map((label, i) => {
           const val = pnlData[i];
           const barH = Math.abs(val) / maxVal * (chartH/2 - 8);
           const x = i * (barW + 4) + 2;
           const isPos = val >= 0;
           const y = isPos ? chartH/2 - barH : chartH/2;
           return (
             <g key={i}>
               <rect x={x} y={y} width={barW} height={Math.max(barH, 1)} fill={isPos ? "#22c55e" : "#ef4444"} rx={2} opacity={0.8} />
               <text x={x + barW/2} y={chartH + 14} textAnchor="middle" fill="#64748b" fontSize="9" fontFamily="monospace">{label}</text>
               {val !== 0 && (
                 <text x={x + barW/2} y={isPos ? y - 3 : y + barH + 10} textAnchor="middle" fill={isPos ? "#22c55e" : "#ef4444"} fontSize="8" fontFamily="monospace">
                   {val > 0 ? "+" : ""}{val.toFixed(3)}
                 </text>
               )}
             </g>
           );
         })}
       </svg>
     </div>
   </div>
 );
}

// ── CALENDARIO ─────────────────────────────────────────────────
function Calendar({ realTrades, movements, setMovements }) {
 const [selectedDay, setSelectedDay] = useState(null);
 const [showModal, setShowModal] = useState(false);
 const [movType, setMovType] = useState("withdrawal");
 const [movAmount, setMovAmount] = useState("");
 const [movNote, setMovNote] = useState("");
 const [loading, setLoading] = useState(false);

 const now = new Date();
 const year = now.getFullYear();
 const month = now.getMonth();
 const daysInMonth = new Date(year, month + 1, 0).getDate();
 const firstDay = new Date(year, month, 1).getDay();
 const adjustedFirst = firstDay === 0 ? 6 : firstDay - 1;

 // Calcular P&L por día
 const dayData = {};
 for (let d = 1; d <= daysInMonth; d++) {
   const dateKey = `${year}-${String(month+1).padStart(2,"0")}-${String(d).padStart(2,"0")}`;
   const dayStart = new Date(year, month, d, 0, 0, 0, 0).getTime();
   const dayEnd = new Date(year, month, d, 23, 59, 59, 999).getTime();

   const trades = realTrades.filter(t => t.status === "CLOSED" && t.closeTime >= dayStart && t.closeTime <= dayEnd);
   const pnl = trades.reduce((sum, t) => sum + (t.pnlSol || 0), 0);
   const movs = movements.filter(m => m.date === dateKey);

   if (trades.length > 0 || movs.length > 0) {
     dayData[d] = { pnl, trades: trades.length, movs };
   }
 }

 const monthNames = ["Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];

 async function addMovement() {
   if (!movAmount || isNaN(parseFloat(movAmount))) return;
   setLoading(true);
   try {
     const dateKey = `${year}-${String(month+1).padStart(2,"0")}-${String(selectedDay).padStart(2,"0")}`;
     const res = await fetch(`${BACKEND_HTTP}/api/movement`, {
       method: "POST",
       headers: { "Content-Type": "application/json" },
       body: JSON.stringify({ date: dateKey, amount: parseFloat(movAmount), type: movType, note: movNote }),
     });
     if (res.ok) {
       setMovAmount(""); setMovNote(""); setShowModal(false);
     }
   } catch (e) { console.error(e); }
   setLoading(false);
 }

 async function deleteMovement(id) {
   try {
     await fetch(`${BACKEND_HTTP}/api/movement/${id}`, { method: "DELETE" });
   } catch (e) { console.error(e); }
 }

 const selectedData = selectedDay ? dayData[selectedDay] : null;
 const selectedDateKey = selectedDay ? `${year}-${String(month+1).padStart(2,"0")}-${String(selectedDay).padStart(2,"0")}` : null;
 const selectedMovements = selectedDay ? movements.filter(m => m.date === selectedDateKey) : [];
 const selectedTrades = selectedDay ? (() => {
   const dayStart = new Date(year, month, selectedDay, 0, 0, 0, 0).getTime();
   const dayEnd = new Date(year, month, selectedDay, 23, 59, 59, 999).getTime();
   return realTrades.filter(t => t.status === "CLOSED" && t.closeTime >= dayStart && t.closeTime <= dayEnd);
 })() : [];

 return (
   <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
     {/* Cabecera mes */}
     <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 14 }}>
       <div style={{ fontFamily: "monospace", fontSize: 13, color: "#f1f5f9", fontWeight: 700, textAlign: "center", marginBottom: 12 }}>
         📅 {monthNames[month]} {year}
       </div>

       {/* Días semana */}
       <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 1fr)", gap: 2, marginBottom: 4 }}>
         {["L","M","X","J","V","S","D"].map(d => (
           <div key={d} style={{ textAlign: "center", fontFamily: "monospace", fontSize: 9, color: "#475569", padding: "2px 0" }}>{d}</div>
         ))}
       </div>

       {/* Días */}
       <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 1fr)", gap: 2 }}>
         {Array(adjustedFirst).fill(null).map((_, i) => <div key={`e${i}`} />)}
         {Array(daysInMonth).fill(null).map((_, i) => {
           const d = i + 1;
           const data = dayData[d];
           const isToday = d === now.getDate();
           const isSelected = d === selectedDay;
           const hasTrades = data?.trades > 0;
           const pnl = data?.pnl || 0;
           const hasMovs = data?.movs?.length > 0;
           let bg = "transparent";
           let border = "1px solid #1e2d4033";
           if (isSelected) { bg = "#1e3a5f"; border = "1px solid #38bdf8"; }
           else if (hasTrades) { bg = pnl >= 0 ? "#052e16" : "#1f0a0a"; border = `1px solid ${pnl >= 0 ? "#22c55e33" : "#ef444433"}`; }
           else if (isToday) { border = "1px solid #facc1566"; }

           return (
             <div key={d} onClick={() => setSelectedDay(d === selectedDay ? null : d)} style={{ background: bg, border, borderRadius: 6, padding: "4px 2px", textAlign: "center", cursor: "pointer", minHeight: 44, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 1 }}>
               <span style={{ fontFamily: "monospace", fontSize: 11, color: isToday ? "#facc15" : "#94a3b8", fontWeight: isToday ? 700 : 400 }}>{d}</span>
               {hasTrades && <span style={{ fontFamily: "monospace", fontSize: 8, color: pnl >= 0 ? "#22c55e" : "#ef4444" }}>{pnl >= 0 ? "+" : ""}{pnl.toFixed(3)}</span>}
               {hasMovs && <span style={{ fontSize: 8 }}>{data.movs.some(m => m.type === "withdrawal") ? "💸" : "💰"}</span>}
             </div>
           );
         })}
       </div>
     </div>

     {/* Detalle día seleccionado */}
     {selectedDay && (
       <div style={{ background: "#0d1117", border: "1px solid #38bdf855", borderRadius: 10, padding: 14 }}>
         <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
           <span style={{ fontFamily: "monospace", fontSize: 12, color: "#38bdf8", fontWeight: 700 }}>
             📅 {selectedDay}/{month+1}/{year}
           </span>
           <button onClick={() => setShowModal(true)} style={{ background: "#1e3a5f", border: "1px solid #38bdf8", borderRadius: 8, color: "#38bdf8", fontFamily: "monospace", fontSize: 10, padding: "4px 10px", cursor: "pointer" }}>
             + Movimiento
           </button>
         </div>

         {/* Trades del día */}
         {selectedTrades.length > 0 && (
           <div style={{ marginBottom: 10 }}>
             <div style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginBottom: 6 }}>TRADES ({selectedTrades.length})</div>
             {selectedTrades.map(t => (
               <div key={t.id} style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid #1e2d4044" }}>
                 <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                   <StrategyBadge strategy={t.strategy} />
                   <span style={{ fontFamily: "monospace", fontSize: 11, color: "#f1f5f9" }}>{t.symbol}</span>
                 </div>
                 <span style={{ fontFamily: "monospace", fontSize: 11, color: pctColor(t.pnlSol||0) }}>
                   {(t.pnlSol||0) >= 0 ? "+" : ""}{(t.pnlSol||0).toFixed(4)} SOL
                 </span>
               </div>
             ))}
             <div style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", marginTop: 4 }}>
               <span style={{ fontFamily: "monospace", fontSize: 11, color: "#94a3b8" }}>Total trades</span>
               <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: pctColor(selectedData?.pnl||0) }}>
                 {(selectedData?.pnl||0) >= 0 ? "+" : ""}{(selectedData?.pnl||0).toFixed(4)} SOL
               </span>
             </div>
           </div>
         )}

         {/* Movimientos del día */}
         {selectedMovements.length > 0 && (
           <div>
             <div style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginBottom: 6 }}>MOVIMIENTOS</div>
             {selectedMovements.map(m => (
               <div key={m.id} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "4px 0", borderBottom: "1px solid #1e2d4044" }}>
                 <div>
                   <span style={{ fontSize: 11 }}>{m.type === "withdrawal" ? "💸 Retiro" : "💰 Depósito"}</span>
                   {m.note && <span style={{ fontFamily: "monospace", fontSize: 9, color: "#64748b", marginLeft: 6 }}>{m.note}</span>}
                 </div>
                 <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                   <span style={{ fontFamily: "monospace", fontSize: 11, color: m.type === "withdrawal" ? "#ef4444" : "#22c55e" }}>
                     {m.type === "withdrawal" ? "-" : "+"}{m.amount} SOL
                   </span>
                   <button onClick={() => deleteMovement(m.id)} style={{ background: "none", border: "none", color: "#ef4444", cursor: "pointer", fontSize: 12 }}>✕</button>
                 </div>
               </div>
             ))}
           </div>
         )}

         {selectedTrades.length === 0 && selectedMovements.length === 0 && (
           <div style={{ fontFamily: "monospace", fontSize: 11, color: "#475569", textAlign: "center", padding: 10 }}>Sin actividad este día</div>
         )}
       </div>
     )}

     {/* Modal añadir movimiento */}
     {showModal && (
       <div style={{ position: "fixed", inset: 0, background: "#000000cc", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center", padding: 20 }}>
         <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 14, padding: 20, width: "100%", maxWidth: 360 }}>
           <div style={{ fontFamily: "monospace", fontSize: 13, color: "#f1f5f9", fontWeight: 700, marginBottom: 16 }}>
             Registrar movimiento — {selectedDay}/{month+1}
           </div>

           {/* Tipo */}
           <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
             {["withdrawal", "deposit"].map(t => (
               <button key={t} onClick={() => setMovType(t)} style={{ flex: 1, padding: "8px", border: `1px solid ${movType === t ? (t === "withdrawal" ? "#ef4444" : "#22c55e") : "#1e2d40"}`, borderRadius: 8, background: movType === t ? `${t === "withdrawal" ? "#ef4444" : "#22c55e"}22` : "none", color: movType === t ? (t === "withdrawal" ? "#ef4444" : "#22c55e") : "#64748b", fontFamily: "monospace", fontSize: 11, cursor: "pointer" }}>
                 {t === "withdrawal" ? "💸 Retiro" : "💰 Depósito"}
               </button>
             ))}
           </div>

           {/* Cantidad */}
           <div style={{ marginBottom: 12 }}>
             <div style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginBottom: 4 }}>CANTIDAD (SOL)</div>
             <input
               type="number" step="0.001" placeholder="0.000"
               value={movAmount} onChange={e => setMovAmount(e.target.value)}
               style={{ width: "100%", background: "#080c14", border: "1px solid #1e2d40", borderRadius: 8, padding: "10px 12px", color: "#f1f5f9", fontFamily: "monospace", fontSize: 14, outline: "none" }}
             />
           </div>

           {/* Nota */}
           <div style={{ marginBottom: 16 }}>
             <div style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginBottom: 4 }}>NOTA (opcional)</div>
             <input
               type="text" placeholder="Ej: Retiro para gastos"
               value={movNote} onChange={e => setMovNote(e.target.value)}
               style={{ width: "100%", background: "#080c14", border: "1px solid #1e2d40", borderRadius: 8, padding: "10px 12px", color: "#f1f5f9", fontFamily: "monospace", fontSize: 12, outline: "none" }}
             />
           </div>

           <div style={{ display: "flex", gap: 8 }}>
             <button onClick={() => { setShowModal(false); setMovAmount(""); setMovNote(""); }} style={{ flex: 1, padding: "10px", border: "1px solid #1e2d40", borderRadius: 8, background: "none", color: "#64748b", fontFamily: "monospace", fontSize: 12, cursor: "pointer" }}>
               Cancelar
             </button>
             <button onClick={addMovement} disabled={loading || !movAmount} style={{ flex: 1, padding: "10px", border: "none", borderRadius: 8, background: movType === "withdrawal" ? "#ef4444" : "#22c55e", color: "#fff", fontFamily: "monospace", fontSize: 12, fontWeight: 700, cursor: "pointer", opacity: loading || !movAmount ? 0.5 : 1 }}>
               {loading ? "..." : "Guardar"}
             </button>
           </div>
         </div>
       </div>
     )}
   </div>
 );
}

// ── APP PRINCIPAL ──────────────────────────────────────────────
export default function App() {
 const { migWatching, migMonitored, momMonitored, signals, rechazadas, demoTrades, realTrades, movements, setMovements, log, stats, shadow, wsStatus, postCierre } = useBackend();   // [5-ago] +postCierre · [31-ago] +rechazadas
 const [tab, setTab] = useState("migration");
 const [demoStatusFilter, setDemoStatusFilter] = useState("all");
 const [demoStratFilter, setDemoStratFilter] = useState("unida");
 const [rejFilter, setRejFilter] = useState("all");   // [31-ago] filtro por motivo de rechazo
 const mandarVeredicto = async (id, veredicto, maxVisto) => {
   const mv = (maxVisto === "" || maxVisto == null || isNaN(+maxVisto)) ? null : +maxVisto;
   const antes = rechazadas;
   setRechazadas(p => p.map(r => r.id === id ? { ...r, veredicto, maxVisto: mv } : r));   // se marca al instante
   try {
     const r0 = antes.find(x => x.id === id) || {};
     const res = await fetch(`${BACKEND_HTTP}/api/rechazada/veredicto`, { method: "POST", headers: { "Content-Type": "application/json" },
       body: JSON.stringify({ id, veredicto, maxVisto: mv, mint: r0.mint, symbol: r0.symbol, motivo: r0.motivo,
                              ts: r0.ts, dur: r0.dur, mc: r0.mc, vol: r0.vol, trades: r0.trades, sig: r0.sig, mov2s: r0.mov2s, ultimo: r0.ultimo }) });
     if (!res.ok) throw new Error("HTTP " + res.status);
   } catch (e) {
     setRechazadas(antes);                                   // deshacer
     alert("No se pudo guardar el veredicto: " + e.message);  // que se vea, en vez de fallar en silencio
   }
 };   // [23-ago] demo arranca enseñando solo la unida
 const [realStatusFilter, setRealStatusFilter] = useState("all");
 const [realStratFilter, setRealStratFilter] = useState("all");
 const [chartPeriod, setChartPeriod] = useState("daily");
 const [, tick] = useState(0);
 useEffect(() => { const t = setInterval(() => tick(n => n+1), 1000); return () => clearInterval(t); }, []);

 const statusColor = { connected: "#22c55e", connecting: "#facc15", disconnected: "#6b7280", error: "#ef4444" }[wsStatus] || "#6b7280";
 const statusLabel = { connected: "LIVE", connecting: "...", disconnected: "OFF", error: "ERR" }[wsStatus] || "—";

 const migWR = (stats.mig_demoWins||0) + (stats.mig_demoLosses||0) > 0 ? Math.round((stats.mig_demoWins||0) / ((stats.mig_demoWins||0) + (stats.mig_demoLosses||0)) * 100) : 0;
 const momWR = (stats.mom_demoWins||0) + (stats.mom_demoLosses||0) > 0 ? Math.round((stats.mom_demoWins||0) / ((stats.mom_demoWins||0) + (stats.mom_demoLosses||0)) * 100) : 0;
 const totalPnlSol = (stats.mig_realPnLSol||0) + (stats.mom_realPnLSol||0);


 const filteredDemo = demoTrades.filter(t => {
   const statusOk = demoStatusFilter === "all" ? true : demoStatusFilter === "open" ? t.status === "OPEN" : t.status !== "OPEN";
   const stratOk = demoStratFilter === "all" ? true : t.strategy === demoStratFilter;
   return statusOk && stratOk;
 });

 const filteredReal = realTrades.filter(t => {
   const statusOk = realStatusFilter === "all" ? true : realStatusFilter === "open" ? t.status === "OPEN" : t.status !== "OPEN";
   const stratOk = realStratFilter === "all" ? true : t.strategy === realStratFilter;
   return statusOk && stratOk;
 });

 const migDemoOpen = demoTrades.filter(t => t.status === "OPEN" && t.strategy === "migration").length;
 // [27-ago] SOL netos por estrategia (lote real × pnl − fee), que es lo único que significa algo
 // [28-ago] la tarjeta de MIGRACIÓN se calcula desde los trades (fuente de verdad),
 // no desde contadores del server que pueden desincronizarse entre reinicios.
 const migCard = (() => {
   const T = demoTrades.filter(t => t.strategy === "migration");
   const cerr = T.filter(t => t.status !== "OPEN");
   const tokens = new Set(T.map(t => t.mint)).size;
   const sol = cerr.reduce((s, t) => s + (t.sizeSol || 0.5) * (((t.pnlPct || 0) - 4.5) / 100), 0);
   const pct = cerr.reduce((s, t) => s + (t.pnlPct || 0), 0);
   const n = Math.max(1, cerr.length);
   return {
     tokens, abiertas: T.length - cerr.length,
     wins: cerr.filter(t => (t.pnlPct || 0) > 0).length,
     losses: cerr.filter(t => (t.pnlPct || 0) <= 0).length,
     sol, pct,
     avgMaxGain: cerr.reduce((s, t) => s + (t.maxGainPct || 0), 0) / n,
     avgMaxLoss: cerr.reduce((s, t) => s + (t.maxLossPct || 0), 0) / n,
   };
 })();
 const solNeto = (strat) => demoTrades
   .filter(t => t.strategy === strat && t.status !== "OPEN")
   .reduce((s, t) => s + (t.sizeSol || 0.5) * (((t.pnlPct || 0) - 4.5) / 100), 0);
 const hoyKey = toDateKey(Date.now());
 const cerradasHoy = demoTrades.filter(t => t.status !== "OPEN" && t.closeTime && toDateKey(t.closeTime) === hoyKey);
 const hoyNeto = cerradasHoy.reduce((s, t) => s + (t.sizeSol || 0.5) * (((t.pnlPct || 0) - 4.5) / 100), 0);

 return (
   <div style={{ background: "#080c14", minHeight: "100dvh", color: "#e2e8f0", fontFamily: "sans-serif", display: "flex", flexDirection: "column", maxWidth: 480, margin: "0 auto" }}>
     <style>{`* { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; } body { overscroll-behavior: none; background: #080c14; } ::-webkit-scrollbar { display: none; } input { box-sizing: border-box; }`}</style>

     {/* ── HEADER DOBLE FILA ── */}
     <div style={{ background: "#0d1117", borderBottom: "1px solid #1e2d40", padding: "10px 16px", position: "sticky", top: 0, zIndex: 50 }}>
       {/* Fila 1: Logo + Status */}
       <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
         <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
           <span style={{ fontFamily: "monospace", fontSize: 18, fontWeight: 700, color: "#38bdf8" }}>SOL<span style={{ color: "#facc15" }}>SCAN</span></span>
           <div style={{ display: "flex", alignItems: "center", gap: 5, background: "#111827", border: "1px solid #1e2d40", padding: "3px 8px", borderRadius: 20 }}>
             <div style={{ width: 7, height: 7, borderRadius: "50%", background: statusColor }} />
             <span style={{ fontFamily: "monospace", fontSize: 11, color: statusColor, fontWeight: 700 }}>{statusLabel}</span>
           </div>
         </div>
         <div style={{ textAlign: "right" }}>
           <div style={{ fontFamily: "monospace", fontSize: 14, fontWeight: 700, color: "#f97316" }}>{(stats.walletBalance||0).toFixed(4)} SOL</div>
           <div style={{ fontSize: 9, color: "#64748b" }}>BALANCE</div>
         </div>
       </div>

       {/* Fila 2: Stats */}
       <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 6 }}>
         {[
           { label: "🌉 MIG W%", val: `${migWR}%`, color: migWR >= 50 ? "#22c55e" : "#ef4444" },
           { label: `☀️ HOY (${cerradasHoy.length})`, val: `${hoyNeto >= 0 ? "+" : ""}${hoyNeto.toFixed(2)}`, color: pctColor(hoyNeto) },
           // [27-ago] antes sumaba PORCENTAJES de trades con lotes distintos (un -80% de 0.5 SOL
           // pesaba igual que un +180% de 4 SOL): un número sin sentido. Ahora, SOL netos de verdad.
           { label: "🤝 UNIDA", val: `${solNeto("unida") >= 0 ? "+" : ""}${solNeto("unida").toFixed(1)}`, color: solNeto("unida") >= 0 ? "#22c55e" : "#ef4444" },
           { label: "🧼 UNI-W", val: `${solNeto("unida2") >= 0 ? "+" : ""}${solNeto("unida2").toFixed(1)}`, color: solNeto("unida2") >= 0 ? "#22c55e" : "#ef4444" },
           { label: "🌉 MIG", val: `${solNeto("migration") >= 0 ? "+" : ""}${solNeto("migration").toFixed(1)}`, color: solNeto("migration") >= 0 ? "#22c55e" : "#ef4444" },
           { label: "🔴 REAL", val: `${totalPnlSol >= 0 ? "+" : ""}${totalPnlSol.toFixed(3)}`, color: pctColor(totalPnlSol) },
         ].map(s => (
           <div key={s.label} style={{ background: "#111827", borderRadius: 8, padding: "6px 4px", textAlign: "center" }}>
             <div style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: s.color }}>{s.val}</div>
             <div style={{ fontSize: 8, color: "#64748b", marginTop: 1 }}>{s.label}</div>
           </div>
         ))}
       </div>
     </div>

     {/* TABS */}
     <div style={{ display: "flex", background: "#0d1117", borderBottom: "1px solid #1e2d40", overflowX: "auto" }}>
       {[
         { id: "migration", label: "🌉 Mig", badge: migWatching.length + migMonitored.length, accent: "#facc15" },
         { id: "signals", label: "🚫", badge: rechazadas.length, accent: "#f97316" },
         { id: "demo", label: "💰 Demo", badge: (stats.demoOpen||0) },
         { id: "real", label: "🔴 Real", badge: (stats.realOpen||0), accent: "#f97316" },
         { id: "shadow", label: "🏟️", badge: shadow?.n || 0, accent: "#facc15" },
         { id: "stats", label: "📈" },
         { id: "calendar", label: "📅" },
         { id: "log", label: "📋" },
       ].map(t => (
         <button key={t.id} onClick={() => setTab(t.id)} style={{ flex: "0 0 auto", padding: "10px 10px", border: "none", background: "none", fontSize: 11, fontWeight: 600, color: tab === t.id ? (t.accent || "#38bdf8") : "#64748b", borderBottom: tab === t.id ? `2px solid ${t.accent || "#38bdf8"}` : "2px solid transparent", cursor: "pointer", display: "flex", alignItems: "center", gap: 3, whiteSpace: "nowrap" }}>
           {t.label}
           {t.badge > 0 && <span style={{ background: "#1e2d40", color: t.accent || "#38bdf8", fontSize: 9, padding: "1px 4px", borderRadius: 10, fontFamily: "monospace" }}>{t.badge}</span>}
         </button>
       ))}
     </div>

     {/* CONTENT */}
     <div style={{ flex: 1, overflowY: "auto", padding: 12, display: "flex", flexDirection: "column", gap: 10 }}>

       {tab === "migration" && (
         <>
           {migWatching.length > 0 && (
             <div style={{ background: "#0d1117", border: "1px solid #facc1544", borderRadius: 10, padding: 12 }}>
               <div style={{ fontFamily: "monospace", fontSize: 11, color: "#facc15", marginBottom: 8, fontWeight: 700 }}>🌉 VENTANA — 60s</div>
               {migWatching.map(w => (
                 <div key={w.mint} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e2d4044" }}>
                   <div>
                     <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#f1f5f9" }}>{w.symbol}</span>
                     <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginLeft: 8 }}>{formatMC(w.migratedMcUsd)}</span>
                   </div>
                   <div style={{ display: "flex", gap: 10, fontFamily: "monospace", fontSize: 11 }}>
                     <span style={{ color: (w.volumeUSD||0) >= 2000 ? "#22c55e" : "#facc15" }}>${Math.round(w.volumeUSD||0)}</span>
                     <span style={{ color: "#64748b" }}>{Math.max(0, Math.round((w.timeLeft||0)/1000))}s</span>
                   </div>
                 </div>
               ))}
             </div>
           )}
           {migMonitored.length > 0 && (
             <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 12 }}>
               <div style={{ fontFamily: "monospace", fontSize: 11, color: "#38bdf8", marginBottom: 2, fontWeight: 700 }}>🔭 QUÉ PASÓ TRAS VENDER ({Object.keys(postCierre).length})</div>
               <div style={{ fontSize: 10, color: "#64748b", marginBottom: 8 }}>Cuánto se movió cada token DESDE que el bot salió. Ordenado por lo que más subió después: si aquí hay mucho verde, estás vendiendo pronto.</div>
               {Object.values(postCierre).sort((a, b) => (b.max || 0) - (a.max || 0)).map(p => (
                 <div key={p.mint} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e2d4044" }}>
                   <div style={{ minWidth: 0 }}>
                     <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#f1f5f9" }}>{(p.symbol && p.symbol !== "???") ? p.symbol : p.mint.slice(0, 6)}</span>
                     <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginLeft: 8 }}>{formatMC(p.mcAhora)}</span>
                     {!p.vivo && <span style={{ fontSize: 9, color: "#64748b", marginLeft: 6 }}>· sin ticks</span>}
                   </div>
                   <div style={{ display: "flex", alignItems: "center", gap: 10, fontFamily: "monospace", fontSize: 11 }}>
                     <span title="pico desde que vendimos" style={{ color: (p.max || 0) >= 20 ? "#facc15" : "#64748b", fontWeight: (p.max || 0) >= 20 ? 700 : 400 }}>
                       pico {(p.max || 0) >= 0 ? "+" : ""}{Math.round(p.max || 0)}%</span>
                     <span title="ahora, respecto al precio de venta" style={{ color: (p.desde || 0) >= 0 ? "#22c55e" : "#ef4444", minWidth: 52, textAlign: "right" }}>
                       {(p.desde || 0) >= 0 ? "+" : ""}{Math.round(p.desde || 0)}%</span>
                     <span style={{ color: "#475569", minWidth: 34, textAlign: "right" }}>{p.min}m</span>
                   </div>
                 </div>
               ))}
               {Object.keys(postCierre).length === 0 && <div style={{ color: "#475569", fontSize: 11 }}>Aún no hay ventas que seguir.</div>}
             </div>
           )}
           {migWatching.length === 0 && migMonitored.length === 0 && <EmptyState icon="🌉" text="Esperando migraciones de pump.fun…" />}
         </>
       )}

       {tab === "signals" && (
         <>
           <div style={{ fontSize: 11, color: "#64748b", marginBottom: 8 }}>🚫 Migraciones que los filtros descartaron, con lo que hizo el precio mientras se vigilaban. Toca una para desplegar.</div>
           {(() => {
             // [31-ago] chips por MOTIVO: se construyen solos con los motivos que van llegando
             const cuenta = {}, bien = {}, mal = {};
             for (const r of rechazadas) { cuenta[r.motivo] = (cuenta[r.motivo] || 0) + 1; if (r.veredicto === "bien") bien[r.motivo] = (bien[r.motivo] || 0) + 1; if (r.veredicto === "mal") mal[r.motivo] = (mal[r.motivo] || 0) + 1; }
             const motivos = Object.entries(cuenta).sort((a, b) => b[1] - a[1]);
             const totBien = Object.values(bien).reduce((a, b) => a + b, 0), totMal = Object.values(mal).reduce((a, b) => a + b, 0);
             const corto = (m) => m.replace(/^MIG\s+/i, "").replace(/^sin tiempo tras el examen:.*/i, "sin tiempo").slice(0, 16);
             const lista = rejFilter === "all" ? rechazadas : rechazadas.filter(r => r.motivo === rejFilter);
             return (
               <>
                 {motivos.length > 0 && (
                   <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 10 }}>
                     {[["all", "Todas", rechazadas.length], ...motivos.map(([m, n]) => [m, corto(m), n])].map(([id, label, n]) => (
                       <button key={id} onClick={() => setRejFilter(id)}
                         style={{ background: rejFilter === id ? "#f9731622" : "#0d1117", border: `1px solid ${rejFilter === id ? "#f97316" : "#1e2d40"}`,
                                  color: rejFilter === id ? "#f97316" : "#94a3b8", borderRadius: 8, padding: "5px 9px", fontSize: 11, fontFamily: "monospace", cursor: "pointer" }}>
                         {label} <span style={{ opacity: .7 }}>{n}</span>
                         {(id === "all" ? (totBien + totMal) : ((bien[id] || 0) + (mal[id] || 0))) > 0 && (
                           <span style={{ marginLeft: 4 }}>
                             <span style={{ color: "#22c55e" }}>✅{id === "all" ? totBien : (bien[id] || 0)}</span>
                             <span style={{ color: "#ef4444", marginLeft: 3 }}>❌{id === "all" ? totMal : (mal[id] || 0)}</span>
                           </span>
                         )}
                       </button>
                     ))}
                   </div>
                 )}
                 {lista.length === 0 && <EmptyState icon="🚫" text="Aquí aparecerán las migraciones rechazadas." />}
                 {rejFilter !== "all" && ((bien[rejFilter] || 0) + (mal[rejFilter] || 0)) > 0 && (
                   <div style={{ fontSize: 11, color: "#94a3b8", marginBottom: 8, fontFamily: "monospace" }}>
                     este filtro acierta el <b style={{ color: "#22c55e" }}>{Math.round(100 * (bien[rejFilter] || 0) / ((bien[rejFilter] || 0) + (mal[rejFilter] || 0)))}%</b> de las que has revisado
                     {(mal[rejFilter] || 0) > 0 && <span> · se equivocó en {mal[rejFilter]}</span>}
                   </div>
                 )}
                 {lista.map(r => <Rechazada key={r.id} r={r} onVeredicto={mandarVeredicto} />)}
               </>
             );
           })()}
           {signals.length > 0 && <div style={{ fontSize: 11, color: "#475569", margin: "14px 0 6px" }}>🎯 señales antiguas</div>}
           {signals.map(s => (
             <div key={s.id} style={{ background: "#0d1117", border: `1px solid ${s.strategy === "migration" ? "#facc15" : "#a78bfa"}33`, borderRadius: 10, padding: "10px 14px" }}>
               <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                 <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                   <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: "#f1f5f9" }}>{s.symbol}</span>
                   <StrategyBadge strategy={s.strategy} />
                 </div>
                 <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>{formatTime(s.time)}</span>
               </div>
               <div style={{ display: "flex", gap: 12, fontFamily: "monospace", fontSize: 11, marginBottom: 8 }}>
                 <span style={{ color: "#94a3b8" }}>MC {formatMC(s.mcUsd)}</span>
                 <span style={{ color: "#22c55e" }}>TP {s.strategy === "migration" ? "+80%" : "+6%"}</span>
                 <span style={{ color: "#ef4444" }}>SL {s.strategy === "migration" ? "-18%" : "-3%"}</span>
               </div>
               <a href={`https://pump.fun/coin/${s.mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 10, color: "#38bdf8", textDecoration: "none" }}>💊 pump.fun</a>
             </div>
           ))}
         </>
       )}

       {tab === "demo" && (
         <>
           <div style={{ display: "flex", gap: 8 }}>
             <div style={{ flex: 1, background: "#0d1117", border: "1px solid #facc1533", borderRadius: 8, padding: "8px 10px", textAlign: "center" }}>
               <div style={{ fontFamily: "monospace", fontSize: 14, fontWeight: 700, color: "#facc15" }}>{migDemoOpen}</div>
               <div style={{ fontSize: 9, color: "#64748b" }}>🌉 Mig abiertas</div>
             </div>
             <div style={{ flex: 1, background: "#0d1117", border: "1px solid #22c55e33", borderRadius: 8, padding: "8px 10px", textAlign: "center" }}>
               <div style={{ fontFamily: "monospace", fontSize: 14, fontWeight: 700, color: "#22c55e" }}>{demoTrades.filter(t => t.status === "OPEN" && t.strategy === "unida").length}</div>
               <div style={{ fontSize: 9, color: "#64748b" }}>🤝 Unida abiertas</div>
             </div>

             <div style={{ flex: 1, background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 8, padding: "8px 10px", textAlign: "center" }}>
               <div style={{ fontFamily: "monospace", fontSize: 14, fontWeight: 700, color: "#f1f5f9" }}>{demoTrades.filter(t => t.status !== "OPEN").length}</div>
               <div style={{ fontSize: 9, color: "#64748b" }}>Total cerradas</div>
             </div>
           </div>
           <FilterBar statusFilter={demoStatusFilter} setStatusFilter={setDemoStatusFilter} stratFilter={demoStratFilter} setStratFilter={setDemoStratFilter} accentColor="#38bdf8" />
           <CurvaDia cierres={cerradasHoy} />
           {filteredDemo.length === 0 && <EmptyState icon="💰" text="No hay operaciones con estos filtros." />}
           {(() => {
             // agrupar por día y, dentro, por token
             const porDia = {};
             for (const t of filteredDemo) {
               const k = toDateKey(t.closeTime || t.openTime);
               (porDia[k] = porDia[k] || []).push(t);
             }
             return Object.keys(porDia).sort().reverse().map(dia => {
               const trades = porDia[dia];
               const sol = trades.filter(t => t.status !== "OPEN").reduce((s, t) => s + SOL_NETO(t), 0);
               const solA = trades.filter(t => t.status !== "OPEN" && t.strategy === "unida").reduce((s, t) => s + SOL_NETO(t), 0);
               const solB = trades.filter(t => t.status !== "OPEN" && t.strategy === "unida2").reduce((s, t) => s + SOL_NETO(t), 0);
               const hayB = trades.some(t => t.strategy === "unida2");
               const rojas = trades.filter(t => t.closeReason === "ROJA").length;
               // [27-ago] agrupar por TOKEN + VARIANTE: si no, unida y unida2 se sumaban
               // en la misma tarjeta y el resultado no era de ninguna de las dos.
               const tok = {};
               for (const t of trades) { const k = t.mint + "|" + t.strategy; (tok[k] = tok[k] || []).push(t); }
               const orden = Object.keys(tok).sort((a, b) => Math.max(...tok[b].map(t => t.openTime)) - Math.max(...tok[a].map(t => t.openTime)));
               return (
                 <div key={dia} style={{ marginBottom: 14 }}>
                   <div style={{ display: "flex", alignItems: "baseline", gap: 8, padding: "6px 2px", position: "sticky", top: 0,
                     background: "#010409", zIndex: 5, borderBottom: "1px solid #1e2d40", marginBottom: 8 }}>
                     <span style={{ fontSize: 12, color: "#94a3b8", fontWeight: 700 }}>📅 {dia.slice(5)}</span>
                     <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: sol >= 0 ? "#22c55e" : "#ef4444" }}>
                       {sol >= 0 ? "+" : ""}{sol.toFixed(2)} SOL</span>
                     <span style={{ fontSize: 11, color: "#475569" }}>{orden.length} tarjetas{rojas ? ` · ${rojas} 🔴` : ""}</span>
                     {hayB && <span style={{ fontSize: 11, marginLeft: "auto" }}>
                       <b style={{ color: solA >= 0 ? "#22c55e" : "#ef4444" }}>🤝 {solA >= 0 ? "+" : ""}{solA.toFixed(1)}</b>
                       <b style={{ color: solB >= 0 ? "#0ea5e9" : "#ef4444", marginLeft: 8 }}>🧼 {solB >= 0 ? "+" : ""}{solB.toFixed(1)}</b>
                     </span>}
                   </div>
                   {orden.map(k => <TokenCard key={k} mint={k.split("|")[0]} trades={tok[k]} post={postCierre[k.split("|")[0]]} />)}
                 </div>
               );
             });
           })()}
         </>
       )}

       {tab === "real" && (
         <>
           <div style={{ background: "#0d1117", border: "1px solid #f9741633", borderRadius: 10, padding: 12 }}>
             <div style={{ fontFamily: "monospace", fontSize: 11, color: "#f97316", marginBottom: 8, fontWeight: 700 }}>🔴 TRADING REAL</div>
             <div style={{ display: "flex", justifyContent: "space-around" }}>
               {[
                 { label: "Balance", val: `${(stats.walletBalance||0).toFixed(4)} SOL`, color: "#f97316" },
                 { label: "P&L SOL", val: `${totalPnlSol >= 0 ? "+" : ""}${totalPnlSol.toFixed(4)}`, color: pctColor(totalPnlSol) },
                 { label: "🌉 W/L", val: `${stats.mig_realWins||0}/${stats.mig_realLosses||0}`, color: "#facc15" },
                 { label: "⚡ W/L", val: `${stats.mom_realWins||0}/${stats.mom_realLosses||0}`, color: "#a78bfa" },
               ].map(s => (
                 <div key={s.label} style={{ textAlign: "center" }}>
                   <div style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: s.color }}>{s.val}</div>
                   <div style={{ fontSize: 9, color: "#64748b" }}>{s.label}</div>
                 </div>
               ))}
             </div>
           </div>
           <FilterBar statusFilter={realStatusFilter} setStatusFilter={setRealStatusFilter} stratFilter={realStratFilter} setStratFilter={setRealStratFilter} accentColor="#f97316" />
           {filteredReal.length === 0 && <EmptyState icon="🔴" text="No hay operaciones con estos filtros." />}
           {filteredReal.map(t => <TradeCard post={postCierre[t?.mint]} key={t.id} trade={t} isReal={true} />)}
         </>
       )}

       {tab === "shadow" && (
         <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
           {(!shadow || !shadow.n) && <EmptyState icon="🏟️" text="El torneo de sombras empieza a puntuar con las próximas grabaciones… (cada op se re-juega contra 6 configs, fuera-de-muestra desde el alta)" />}
           {shadow && shadow.n > 0 && (() => {
             const filas = Object.entries(shadow.libretas || {}).map(([id, L]) => ({ id, ...L, wr: L.n ? Math.round(100 * L.w / L.n) : 0, media: L.n ? L.neto / L.n * 1000 : 0 })).sort((a, b) => b.neto - a.neto);
             const maxAbs = Math.max(...filas.map(f => Math.abs(f.neto)), 0.01);
             const horas = Array.from({ length: 24 }, (_, h) => ({ h, ...(shadow.horas?.[h] || { n: 0, neto: 0 }) }));
             const maxH = Math.max(...horas.map(x => Math.abs(x.neto)), 0.01);
             const dnom = ["dom","lun","mar","mié","jue","vie","sáb"];
             const dias = Array.from({ length: 7 }, (_, d) => ({ d, nom: dnom[d], ...(shadow.dias?.[d] || { n: 0, neto: 0 }) }));
             const segs = Array.from({ length: 11 }, (_, s) => ({ s, ...(shadow.delays?.[s] || { n: 0, neto: 0 }) })).map(x => ({ ...x, m: x.n ? x.neto / x.n * 1000 : 0 }));
             const maxS = Math.max(...segs.map(x => Math.abs(x.m)), 1);
             return (
               <>
                 {shadow.propuesta && (
                   <div style={{ background: "#1a1502", border: "1px solid #facc15", borderRadius: 10, padding: 12 }}>
                     <div style={{ fontFamily: "monospace", fontSize: 12, color: "#facc15", fontWeight: 700, marginBottom: 6 }}>🏆 PROPUESTA DEL TRIBUNAL</div>
                     <div style={{ fontFamily: "monospace", fontSize: 12, color: "#f1f5f9" }}><b>{shadow.propuesta.id}</b> supera a la sofá: <span style={{ color: "#22c55e", fontWeight: 700 }}>+{shadow.propuesta.delta} SOL</span> · días {shadow.propuesta.dias} · sin-top3 +{shadow.propuesta.sinTop3}</div>
                     <div style={{ fontSize: 9, color: "#94a3b8", marginTop: 4 }}>Todas las puertas pasadas (n≥300 · ≥7 días · día a día · sin-top3). La promoción es decisión humana.</div>
                   </div>
                 )}
                 <div style={{ background: "#0d1117", border: "1px solid #facc1544", borderRadius: 10, padding: 12 }}>
                   <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
                     <span style={{ fontFamily: "monospace", fontSize: 12, color: "#facc15", fontWeight: 700 }}>🏟️ CLASIFICACIÓN</span>
                     <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>n={shadow.n} · desde {new Date(shadow.alta).toLocaleDateString("es-ES", { day: "2-digit", month: "2-digit" })}</span>
                   </div>
                   {filas.map((f, i) => (
                     <div key={f.id} style={{ marginBottom: 6 }}>
                       <div style={{ display: "flex", justifyContent: "space-between", fontFamily: "monospace", fontSize: 11, marginBottom: 2 }}>
                         <span style={{ color: "#f1f5f9" }}>{i === 0 ? "🥇 " : i === 1 ? "🥈 " : i === 2 ? "🥉 " : ""}{f.id}{f.id === "sofa" ? " ⭐" : ""}</span>
                         <span style={{ color: pctColor(f.neto), fontWeight: 700 }}>{f.neto >= 0 ? "+" : ""}{f.neto.toFixed(2)} SOL <span style={{ color: "#64748b", fontWeight: 400 }}>· {f.wr}% · {f.media.toFixed(0)}m/op{f.skip ? ` · 🚪${f.skip}` : ""}</span></span>
                       </div>
                       <div style={{ height: 5, background: "#1e2d40", borderRadius: 3, overflow: "hidden" }}>
                         <div style={{ height: "100%", width: `${Math.abs(f.neto) / maxAbs * 100}%`, background: f.neto >= 0 ? "#22c55e" : "#ef4444", borderRadius: 3 }} />
                       </div>
                     </div>
                   ))}
                 </div>
                 <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 12 }}>
                   <div style={{ fontFamily: "monospace", fontSize: 11, color: "#38bdf8", fontWeight: 700, marginBottom: 8 }}>🕐 POR HORA (ES) — neto sofá</div>
                   <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 3 }}>
                     {horas.map(x => {
                       const inten = Math.min(1, Math.abs(x.neto) / maxH);
                       const bg = x.n < 3 ? "#111827" : x.neto >= 0 ? `rgba(34,197,94,${0.12 + inten * 0.5})` : `rgba(239,68,68,${0.12 + inten * 0.5})`;
                       return (
                         <div key={x.h} style={{ background: bg, borderRadius: 6, padding: "5px 2px", textAlign: "center" }}>
                           <div style={{ fontFamily: "monospace", fontSize: 9, color: "#94a3b8" }}>{x.h}h</div>
                           <div style={{ fontFamily: "monospace", fontSize: 9, fontWeight: 700, color: x.n < 3 ? "#475569" : pctColor(x.neto) }}>{x.n < 3 ? "·" : `${x.neto >= 0 ? "+" : ""}${x.neto.toFixed(1)}`}</div>
                         </div>
                       );
                     })}
                   </div>
                 </div>
                 <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 12 }}>
                   <div style={{ fontFamily: "monospace", fontSize: 11, color: "#a78bfa", fontWeight: 700, marginBottom: 8 }}>📆 POR DÍA DE LA SEMANA</div>
                   <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 1fr)", gap: 3 }}>
                     {dias.map(x => (
                       <div key={x.d} style={{ background: "#111827", borderRadius: 6, padding: "5px 2px", textAlign: "center" }}>
                         <div style={{ fontFamily: "monospace", fontSize: 9, color: "#94a3b8" }}>{x.nom}</div>
                         <div style={{ fontFamily: "monospace", fontSize: 9, fontWeight: 700, color: x.n ? pctColor(x.neto) : "#475569" }}>{x.n ? `${x.neto >= 0 ? "+" : ""}${x.neto.toFixed(1)}` : "·"}</div>
                         <div style={{ fontFamily: "monospace", fontSize: 8, color: "#475569" }}>{x.n || ""}</div>
                       </div>
                     ))}
                   </div>
                 </div>
                 <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 12 }}>
                   <div style={{ fontFamily: "monospace", fontSize: 11, color: "#f472b6", fontWeight: 700, marginBottom: 8 }}>⏱️ ESCALERA s0-s10 — mSOL/op entrando en cada segundo</div>
                   {segs.map(x => (
                     <div key={x.s} style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
                       <span style={{ fontFamily: "monospace", fontSize: 9, color: x.s === 0 ? "#38bdf8" : "#64748b", width: 22, fontWeight: x.s === 0 ? 700 : 400 }}>s{x.s}</span>
                       <div style={{ flex: 1, height: 7, background: "#1e2d40", borderRadius: 3, overflow: "hidden" }}>
                         <div style={{ height: "100%", width: `${Math.abs(x.m) / maxS * 100}%`, background: x.m >= 0 ? (x.s === 0 ? "#38bdf8" : "#22c55e") : "#ef4444", borderRadius: 3 }} />
                       </div>
                       <span style={{ fontFamily: "monospace", fontSize: 9, color: pctColor(x.m), width: 40, textAlign: "right", fontWeight: 700 }}>{x.m >= 0 ? "+" : ""}{x.m.toFixed(0)}</span>
                     </div>
                   ))}
                   <div style={{ fontSize: 9, color: "#475569", marginTop: 6 }}>Solo pata migración, config sofá. El s0 azul es la entrada real del bot.</div>
                 </div>
               </>
             );
           })()}
         </div>
       )}

       {tab === "stats" && (
         <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
           {/* Gráfico P&L */}
           <div style={{ display: "flex", gap: 6, marginBottom: 4 }}>
             {[{ id: "daily", label: "7 días" }, { id: "weekly", label: "4 semanas" }, { id: "monthly", label: "30 días" }].map(p => (
               <button key={p.id} onClick={() => setChartPeriod(p.id)} style={{ flex: 1, padding: "6px", border: `1px solid ${chartPeriod === p.id ? "#38bdf8" : "#1e2d40"}`, borderRadius: 8, background: chartPeriod === p.id ? "#1e3a5f" : "none", color: chartPeriod === p.id ? "#38bdf8" : "#64748b", fontFamily: "monospace", fontSize: 10, cursor: "pointer" }}>
                 {p.label}
               </button>
             ))}
           </div>
           <PnlChart realTrades={realTrades} movements={movements} period={chartPeriod} />

           <div style={{ background: "#0d1117", border: "1px solid #facc1533", borderRadius: 10, padding: 14 }}>
             <div style={{ fontFamily: "monospace", fontSize: 12, color: "#facc15", marginBottom: 10, fontWeight: 700 }}>🌉 MIGRACIÓN</div>
             <StatsRow label="Migraciones" val={stats.mig_migrations||0} />
             <StatsRow label="Entradas" val={migCard.tokens} color="#22c55e" desc={stats.mig_entered && stats.mig_entered !== migCard.tokens ? `el contador del server dice ${stats.mig_entered}` : ""} />
             <StatsRow label="Rechazados" val={stats.mig_rejected||0} color="#ef4444" />
             <StatsRow label="Demo Wins" val={migCard.wins} color="#22c55e" />
             <StatsRow label="Demo Losses" val={migCard.losses} color="#ef4444" />
             <StatsRow label="Win Rate" val={`${migWR}%`} color={migWR >= 50 ? "#22c55e" : "#ef4444"} />
             <StatsRow label="P&L Demo" val={`${migCard.sol>=0?"+":""}${migCard.sol.toFixed(3)} SOL`} color={pctColor(migCard.sol)} desc={`suma de % (referencia): ${migCard.pct>=0?"+":""}${Math.round(migCard.pct)}%`} />
             <StatsRow label="Ganancia máx media" val={`+${(migCard.avgMaxGain).toFixed(1)}%`} color="#22c55e" desc="Media del máximo que suben" />
             <StatsRow label="Pérdida máx media" val={`${(migCard.avgMaxLoss).toFixed(1)}%`} color="#ef4444" desc="Media del máximo que bajan" />
           </div>



           <div style={{ background: "#0d1117", border: "1px solid #f9741633", borderRadius: 10, padding: 14 }}>
             <div style={{ fontFamily: "monospace", fontSize: 12, color: "#f97316", marginBottom: 10, fontWeight: 700 }}>🔴 REAL</div>
             <StatsRow label="Balance" val={`${(stats.walletBalance||0).toFixed(4)} SOL`} color="#f97316" />
             <StatsRow label="🌉 Wins" val={stats.mig_realWins||0} color="#22c55e" />
             <StatsRow label="🌉 Losses" val={stats.mig_realLosses||0} color="#ef4444" />
             <StatsRow label="🌉 P&L SOL" val={`${(stats.mig_realPnLSol||0)>=0?"+":""}${(stats.mig_realPnLSol||0).toFixed(4)}`} color={pctColor(stats.mig_realPnLSol||0)} />
             <StatsRow label="⚡ Wins" val={stats.mom_realWins||0} color="#22c55e" />
             <StatsRow label="⚡ Losses" val={stats.mom_realLosses||0} color="#ef4444" />
             <StatsRow label="⚡ P&L SOL" val={`${(stats.mom_realPnLSol||0)>=0?"+":""}${(stats.mom_realPnLSol||0).toFixed(4)}`} color={pctColor(stats.mom_realPnLSol||0)} />
             <StatsRow label="TOTAL P&L" val={`${totalPnlSol>=0?"+":""}${totalPnlSol.toFixed(4)} SOL`} color={pctColor(totalPnlSol)} />
           </div>
         </div>
       )}

       {tab === "calendar" && (
         <Calendar realTrades={realTrades} movements={movements} setMovements={setMovements} />
       )}

       {tab === "log" && log.map((entry, i) => (
         <div key={i} style={{ display: "flex", gap: 8, padding: "3px 0", borderBottom: "1px solid #0d1117" }}>
           <span style={{ fontFamily: "monospace", fontSize: 10, color: "#334155", flexShrink: 0 }}>{formatTime(entry.time)}</span>
           <span style={{ fontFamily: "monospace", fontSize: 10, color: { info:"#64748b", filter:"#475569", accept:"#22c55e", signal:"#facc15", warn:"#f97316", error:"#ef4444", demo:"#a78bfa", win:"#22c55e", loss:"#ef4444", expire:"#f97316", trail:"#facc15", real:"#f97316", realwin:"#22c55e", realloss:"#ef4444" }[entry.type] || "#64748b" }}>{entry.msg}</span>
         </div>
       ))}

     </div>
     <div style={{ height: "env(safe-area-inset-bottom, 16px)" }} />
   </div>
 );
}

function EmptyState({ icon, text }) {
 return (
   <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: 200, gap: 12, color: "#334155" }}>
     <div style={{ fontSize: 40, opacity: 0.3 }}>{icon}</div>
     <p style={{ fontSize: 13, textAlign: "center", lineHeight: 1.6 }}>{text}</p>
   </div>
 );
}
