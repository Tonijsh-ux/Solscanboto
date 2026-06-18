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
 const [demoTrades, setDemoTrades] = useState([]);
 const [realTrades, setRealTrades] = useState([]);
 const [movements, setMovements] = useState([]);
 const [log, setLog] = useState([]);
 const [stats, setStats] = useState({});
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
           setDemoTrades(data.demoTrades || []);
           setRealTrades(data.realTrades || []);
           setMovements(data.movements || []);
           setLog(data.log || []);
           setStats(data.stats || {});
           setWsStatus(data.wsStatus || "connected");
           return;
         }
         if (event === "stats") { setStats(data); return; }
         if (event === "migWatchUpdate") { setMigWatching(p => p.map(w => w.mint === data.mint ? { ...w, ...data } : w)); return; }
         if (event === "newMigToken") { setMigMonitored(p => p.find(t => t.mint === data.mint) ? p : [data, ...p]); return; }
         if (event === "migTokenUpdate") { setMigMonitored(p => p.map(t => t.mint === data.mint ? { ...t, ...data } : t)); return; }
         if (event === "removeToken") { setMigMonitored(p => p.filter(t => t.mint !== data.mint)); setMomMonitored(p => p.filter(t => t.mint !== data.mint)); return; }
         if (event === "newMomToken") { setMomMonitored(p => p.find(t => t.mint === data.mint) ? p : [data, ...p]); return; }
         if (event === "momTokenUpdate") { setMomMonitored(p => p.map(t => t.mint === data.mint ? { ...t, ...data } : t)); return; }
         if (event === "newSignal") { setSignals(p => [data, ...p].slice(0, 100)); if (navigator.vibrate) navigator.vibrate([200,100,200]); return; }
         if (event === "newDemoTrade") { setDemoTrades(p => [data, ...p].slice(0, 500)); return; }
         if (event === "demoTradeUpdate") { setDemoTrades(p => p.map(t => t.id === data.id ? { ...t, ...data } : t)); return; }
         if (event === "demoTradeClosed") { setDemoTrades(p => p.map(t => t.id === data.id ? data : t)); return; }
         if (event === "newRealTrade") { setRealTrades(p => [data, ...p].slice(0, 200)); return; }
         if (event === "realTradeUpdate") { setRealTrades(p => p.map(t => t.id === data.id ? { ...t, ...data } : t)); return; }
         if (event === "realTradeClosed") { setRealTrades(p => p.map(t => t.id === data.id ? data : t)); return; }
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

 return { migWatching, migMonitored, momMonitored, signals, demoTrades, realTrades, movements, setMovements, log, stats, wsStatus };
}

// ── COMPONENTES ────────────────────────────────────────────────
function StrategyBadge({ strategy }) {
 const map = {
   migration:   { label: "🌉 MIG",   color: "#facc15", bg: "#3b2f00" },
   momentum:    { label: "⚡ MOM",   color: "#a78bfa", bg: "#2d1b69" },
 };
 const s = map[strategy] || map.momentum;
 return (
   <span style={{ fontSize: 9, fontFamily: "monospace", color: s.color, background: s.bg, padding: "1px 5px", borderRadius: 6 }}>
     {s.label}
   </span>
 );
}

function TradeCard({ trade, isReal }) {
 const isOpen = trade.status === "OPEN";
 const isWin = trade.result === "WIN";
 const isLoss = trade.result === "LOSS";
 const color = isOpen ? (isReal ? "#f97316" : "#38bdf8") : isWin ? "#22c55e" : isLoss ? "#ef4444" : "#64748b";
 const statusLabel = isOpen ? (isReal ? "🔴 REAL" : "🔵 DEMO") : isWin ? "✅ WIN" : isLoss ? "❌ LOSS" : trade.result === "EXPIRED" ? "⏱️ EXP" : trade.result?.includes("WIN") ? "⏱️ +EXP" : "⏱️ -EXP";
 return (
   <div style={{ background: "#0d1117", border: `1px solid ${color}${isOpen?"55":"33"}`, borderRadius: 10, padding: "10px 14px" }}>
     <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
       <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
         <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: "#f1f5f9" }}>{trade.symbol}</span>
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
         { label: "Trailing", value: trade.trailingPhase||"INITIAL", color: trade.trailingPhase !== "INITIAL" ? "#facc15" : "#64748b" },
         { label: isOpen ? "⏱️" : "Dur", value: isOpen ? elapsed(trade.openTime) : `${Math.round(((trade.closeTime||Date.now())-trade.openTime)/1000)}s`, color: "#94a3b8" },
       ].map((m, i) => (
         <div key={i} style={{ flex: 1, padding: "5px 4px", textAlign: "center", borderRight: i < 4 ? "1px solid #1e2d40" : "none" }}>
           <div style={{ fontSize: 9, color: "#64748b", textTransform: "uppercase", marginBottom: 2 }}>{m.label}</div>
           <div style={{ fontFamily: "monospace", fontSize: 10, fontWeight: 700, color: m.color }}>{m.value}</div>
         </div>
       ))}
     </div>
     {!isOpen && <div style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginBottom: 4 }}>P&L: <span style={{ color: pctColor(trade.pnlPct||0) }}>{(trade.pnlPct||0)>0?"+":""}{(trade.pnlPct||0).toFixed(2)}%</span>{isReal && trade.pnlSol !== null && <span style={{ color: pctColor(trade.pnlSol||0), marginLeft: 8 }}>{(trade.pnlSol||0)>0?"+":""}{(trade.pnlSol||0).toFixed(4)} SOL</span>}</div>}
     {isReal && trade.buySignature && <div style={{ fontFamily: "monospace", fontSize: 9, color: "#64748b", marginBottom: 2 }}>Buy: <a href={`https://solscan.io/tx/${trade.buySignature}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8", textDecoration: "none" }}>{trade.buySignature.slice(0,12)}…</a></div>}
     {isReal && trade.sellSignature && <div style={{ fontFamily: "monospace", fontSize: 9, color: "#64748b", marginBottom: 4 }}>Sell: <a href={`https://solscan.io/tx/${trade.sellSignature}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8", textDecoration: "none" }}>{trade.sellSignature.slice(0,12)}…</a></div>}
     <a href={`https://dexscreener.com/solana/${trade.mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 9, color: "#38bdf8", textDecoration: "none" }}>📊 DexScreener</a>
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
       {[{ id: "all", label: "Todas" }, { id: "migration", label: "🌉" }, { id: "momentum", label: "⚡" }].map(f => (
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
 const { migWatching, migMonitored, momMonitored, signals, demoTrades, realTrades, movements, setMovements, log, stats, wsStatus } = useBackend();
 const [tab, setTab] = useState("migration");
 const [demoStatusFilter, setDemoStatusFilter] = useState("all");
 const [demoStratFilter, setDemoStratFilter] = useState("all");
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

 // Abortos: totales y % de acierto del filtro
 const abortTotal = (stats.abort_correct||0) + (stats.abort_missed||0);
 const abortAccuracy = abortTotal > 0 ? Math.round((stats.abort_correct||0) / abortTotal * 100) : 0;

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
 const momDemoOpen = demoTrades.filter(t => t.status === "OPEN" && t.strategy === "momentum").length;

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
           { label: "⚡ MOM W%", val: `${momWR}%`, color: momWR >= 50 ? "#22c55e" : "#ef4444" },
           { label: "💰 DEMO", val: `${((stats.mig_demoPnL||0)+(stats.mom_demoPnL||0)) >= 0 ? "+" : ""}${Math.round((stats.mig_demoPnL||0)+(stats.mom_demoPnL||0))}%`, color: ((stats.mig_demoPnL||0)+(stats.mom_demoPnL||0)) >= 0 ? "#22c55e" : "#ef4444" },
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
         { id: "momentum", label: "⚡ Mom", badge: momMonitored.length, accent: "#a78bfa" },
         { id: "signals", label: "🎯", badge: signals.length, accent: "#38bdf8" },
         { id: "demo", label: "💰 Demo", badge: (stats.demoOpen||0) },
         { id: "real", label: "🔴 Real", badge: (stats.realOpen||0), accent: "#f97316" },
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
               <div style={{ fontFamily: "monospace", fontSize: 11, color: "#38bdf8", marginBottom: 8, fontWeight: 700 }}>📊 POST-MIGRACIÓN ({migMonitored.length})</div>
               {migMonitored.map(t => (
                 <div key={t.mint} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e2d4044" }}>
                   <div>
                     <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#f1f5f9" }}>{t.symbol}</span>
                     <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginLeft: 8 }}>{formatMC(t.mc)}</span>
                   </div>
                   <div style={{ fontFamily: "monospace", fontSize: 11, color: "#64748b" }}>{elapsed(t.detectedAt)}</div>
                 </div>
               ))}
             </div>
           )}
           {migWatching.length === 0 && migMonitored.length === 0 && <EmptyState icon="🌉" text="Esperando migraciones de pump.fun…" />}
         </>
       )}

       {tab === "momentum" && (
         <>
           {momMonitored.length > 0 && (
             <div style={{ background: "#0d1117", border: "1px solid #a78bfa44", borderRadius: 10, padding: 12 }}>
               <div style={{ fontFamily: "monospace", fontSize: 11, color: "#a78bfa", marginBottom: 8, fontWeight: 700 }}>⚡ MOMENTUM ACTIVO ({momMonitored.length})</div>
               {momMonitored.map(t => (
                 <div key={t.mint} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "7px 0", borderBottom: "1px solid #1e2d4044" }}>
                   <div>
                     <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#f1f5f9" }}>{t.symbol}</span>
                     <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginLeft: 8 }}>{formatMC(t.mc)}</span>
                   </div>
                   <div style={{ display: "flex", gap: 8, fontFamily: "monospace", fontSize: 11 }}>
                     <span style={{ color: "#22c55e" }}>+{(t.pct1h||0).toFixed(1)}%</span>
                     <span style={{ color: "#64748b" }}>{elapsed(t.detectedAt)}</span>
                   </div>
                 </div>
               ))}
             </div>
           )}
           {momMonitored.length === 0 && <EmptyState icon="⚡" text="Escaneando tokens con momentum…" />}
         </>
       )}

       {tab === "signals" && (
         <>
           {signals.length === 0 && <EmptyState icon="🎯" text="Las señales aparecerán aquí." />}
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
               <a href={`https://dexscreener.com/solana/${s.mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 10, color: "#38bdf8", textDecoration: "none" }}>📊 DexScreener →</a>
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
             <div style={{ flex: 1, background: "#0d1117", border: "1px solid #a78bfa33", borderRadius: 8, padding: "8px 10px", textAlign: "center" }}>
               <div style={{ fontFamily: "monospace", fontSize: 14, fontWeight: 700, color: "#a78bfa" }}>{momDemoOpen}</div>
               <div style={{ fontSize: 9, color: "#64748b" }}>⚡ Mom abiertas</div>
             </div>
             <div style={{ flex: 1, background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 8, padding: "8px 10px", textAlign: "center" }}>
               <div style={{ fontFamily: "monospace", fontSize: 14, fontWeight: 700, color: "#f1f5f9" }}>{demoTrades.filter(t => t.status !== "OPEN").length}</div>
               <div style={{ fontSize: 9, color: "#64748b" }}>Total cerradas</div>
             </div>
           </div>
           <FilterBar statusFilter={demoStatusFilter} setStatusFilter={setDemoStatusFilter} stratFilter={demoStratFilter} setStratFilter={setDemoStratFilter} accentColor="#38bdf8" />
           {filteredDemo.length === 0 && <EmptyState icon="💰" text="No hay operaciones con estos filtros." />}
           {filteredDemo.map(t => <TradeCard key={t.id} trade={t} isReal={false} />)}
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
           {filteredReal.map(t => <TradeCard key={t.id} trade={t} isReal={true} />)}
         </>
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
             <StatsRow label="Entradas" val={stats.mig_entered||0} color="#22c55e" />
             <StatsRow label="Rechazados" val={stats.mig_rejected||0} color="#ef4444" />
             <StatsRow label="Demo Wins" val={stats.mig_demoWins||0} color="#22c55e" />
             <StatsRow label="Demo Losses" val={stats.mig_demoLosses||0} color="#ef4444" />
             <StatsRow label="Win Rate" val={`${migWR}%`} color={migWR >= 50 ? "#22c55e" : "#ef4444"} />
             <StatsRow label="P&L Demo" val={`${(stats.mig_demoPnL||0)>=0?"+":""}${Math.round(stats.mig_demoPnL||0)}%`} color={pctColor(stats.mig_demoPnL||0)} />
             <StatsRow label="Ganancia máx media" val={`+${(stats.mig_avgMaxGain||0).toFixed(1)}%`} color="#22c55e" desc="Media del máximo que suben" />
             <StatsRow label="Pérdida máx media" val={`-${(stats.mig_avgMaxLoss||0).toFixed(1)}%`} color="#ef4444" desc="Media del máximo que bajan" />
           </div>

           {/* ── ABORTOS (instrumentación de filtros de entrada) ── */}
           <div style={{ background: "#0d1117", border: "1px solid #38bdf833", borderRadius: 10, padding: 14 }}>
             <div style={{ fontFamily: "monospace", fontSize: 12, color: "#38bdf8", marginBottom: 4, fontWeight: 700 }}>🔭 ABORTOS (filtros de entrada)</div>
             <div style={{ fontSize: 10, color: "#475569", marginBottom: 10 }}>¿Los filtros aciertan o cuestan ganadores? (5 min de observación tras abortar)</div>
             {abortTotal === 0 ? (
               <div style={{ fontFamily: "monospace", fontSize: 11, color: "#475569", textAlign: "center", padding: 10 }}>Sin abortos evaluados todavía</div>
             ) : (
               <>
                 <StatsRow label="✅ Correctos" val={stats.abort_correct||0} color="#22c55e" desc="El token cayó o quedó plano: filtro acertó" />
                 <StatsRow label="❌ Erróneos" val={stats.abort_missed||0} color="#ef4444" desc="Subió +20% tras abortar: ganador perdido" />
                 <StatsRow label="Acierto del filtro" val={`${abortAccuracy}%`} color={abortAccuracy >= 70 ? "#22c55e" : abortAccuracy >= 50 ? "#facc15" : "#ef4444"} />
                 <div style={{ marginTop: 10, padding: "8px 0", borderTop: "1px solid #1e2d40" }}>
                   <div style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginBottom: 8 }}>DESGLOSE POR MOTIVO (correcto / erróneo)</div>
                   {[
                     { motivo: "📉 Bajista", c: stats.abort_correct_bajista||0, m: stats.abort_missed_bajista||0, hint: "trend < 0 en 5s" },
                     { motivo: "⬇️ Vertical", c: stats.abort_correct_vertical||0, m: stats.abort_missed_vertical||0, hint: "colapso >15% desde pico" },
                     { motivo: "⏱️ Delay", c: stats.abort_correct_delay||0, m: stats.abort_missed_delay||0, hint: "cayó >5% en 3s" },
                   ].map(r => {
                     const tot = r.c + r.m;
                     const badRatio = tot > 0 ? r.m / tot : 0;
                     return (
                       <div key={r.motivo} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "5px 0", borderBottom: "1px solid #1e2d4044" }}>
                         <div>
                           <span style={{ fontSize: 11, color: "#94a3b8" }}>{r.motivo}</span>
                           <span style={{ fontFamily: "monospace", fontSize: 9, color: "#475569", marginLeft: 6 }}>{r.hint}</span>
                         </div>
                         <div style={{ display: "flex", gap: 8, fontFamily: "monospace", fontSize: 11 }}>
                           <span style={{ color: "#22c55e" }}>{r.c}</span>
                           <span style={{ color: "#475569" }}>/</span>
                           <span style={{ color: r.m > 0 && badRatio >= 0.5 ? "#ef4444" : "#64748b" }}>{r.m}</span>
                         </div>
                       </div>
                     );
                   })}
                 </div>
                 {(stats.abort_missed_bajista||0) > (stats.abort_missed_vertical||0) + (stats.abort_missed_delay||0) && (stats.abort_missed_bajista||0) >= 2 && (
                   <div style={{ marginTop: 8, padding: "8px 10px", background: "#1f1500", border: "1px solid #facc1533", borderRadius: 8, fontSize: 10, color: "#facc15", lineHeight: 1.5 }}>
                     ⚠️ El motivo "bajista" domina en los errores. El check trend &lt; 0 puede ser demasiado estricto — considerar relajarlo a trend &lt; -0.03.
                   </div>
                 )}
               </>
             )}
           </div>

           <div style={{ background: "#0d1117", border: "1px solid #a78bfa33", borderRadius: 10, padding: 14 }}>
             <div style={{ fontFamily: "monospace", fontSize: 12, color: "#a78bfa", marginBottom: 10, fontWeight: 700 }}>⚡ MOMENTUM</div>
             <StatsRow label="Señales" val={stats.mom_signals||0} color="#a78bfa" />
             <StatsRow label="Descartadas/Canceladas" val={stats.mom_cancelled||0} color="#64748b" desc="Tendencia plana/bajista o sin precio real" />
             <StatsRow label="Demo Wins" val={stats.mom_demoWins||0} color="#22c55e" />
             <StatsRow label="Demo Losses" val={stats.mom_demoLosses||0} color="#ef4444" />
             <StatsRow label="Win Rate" val={`${momWR}%`} color={momWR >= 50 ? "#22c55e" : "#ef4444"} />
             <StatsRow label="P&L Demo" val={`${(stats.mom_demoPnL||0)>=0?"+":""}${Math.round(stats.mom_demoPnL||0)}%`} color={pctColor(stats.mom_demoPnL||0)} />
             <StatsRow label="Ganancia máx media" val={`+${(stats.mom_avgMaxGain||0).toFixed(1)}%`} color="#22c55e" desc="Media del máximo que suben" />
             <StatsRow label="Pérdida máx media" val={`-${(stats.mom_avgMaxLoss||0).toFixed(1)}%`} color="#ef4444" desc="Media del máximo que bajan" />
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
