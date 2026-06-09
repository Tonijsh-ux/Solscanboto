import { useState, useEffect, useRef } from "react";

const BACKEND_WS = import.meta.env.VITE_BACKEND_WS || "ws://localhost:3001";

function formatUSD(n) {
 if (!n && n !== 0) return "—";
 if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
 if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
 if (n >= 1) return `$${n.toFixed(4)}`;
 return `$${n.toExponential(2)}`;
}
function formatMC(n) {
 if (!n || n === 0) return "—";
 if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
 if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
 return `$${Math.round(n)}`;
}
function formatTime(ts) { return new Date(ts).toLocaleTimeString("es-ES", { hour12: false }); }
function shortAddr(addr) { return addr ? `${addr.slice(0, 4)}…${addr.slice(-4)}` : "—"; }
function elapsed(ts) {
 const s = Math.floor((Date.now() - ts) / 1000);
 if (s < 60) return `${s}s`;
 if (s < 3600) return `${Math.floor(s / 60)}m${s % 60}s`;
 return `${Math.floor(s / 3600)}h${Math.floor((s % 3600) / 60)}m`;
}
function pctColor(pct) { return pct >= 0 ? "#22c55e" : "#ef4444"; }

function useBackend() {
 const [watching, setWatching] = useState([]);
 const [monitored, setMonitored] = useState([]);
 const [signals, setSignals] = useState([]);
 const [demoTrades, setDemoTrades] = useState([]);
 const [realTrades, setRealTrades] = useState([]);
 const [log, setLog] = useState([]);
 const [stats, setStats] = useState({
   migrations: 0, watched: 0, entered: 0, rejected: 0,
   demoOpen: 0, demoWins: 0, demoLosses: 0, demoExpired: 0, demoPnL: 0,
   realOpen: 0, realWins: 0, realLosses: 0, realExpired: 0,
   realPnL: 0, realPnLSol: 0,
   avgMaxGain: 0, avgMaxLoss: 0, closedCount: 0, walletBalance: 0,
 });
 const [wsStatus, setWsStatus] = useState("connecting");

 useEffect(() => {
   let ws; let reconnectTimer;
   const connect = () => {
     setWsStatus("connecting");
     ws = new WebSocket(BACKEND_WS);
     ws.onopen = () => setWsStatus("connected");
     ws.onmessage = (evt) => {
       try {
         const { event, data } = JSON.parse(evt.data);
         if (event === "fullState") {
           setWatching(data.watching || []);
           setMonitored(data.monitored || []);
           setSignals(data.signals || []);
           setDemoTrades(data.demoTrades || []);
           setRealTrades(data.realTrades || []);
           setLog(data.log || []);
           setStats(data.stats || {});
           setWsStatus(data.wsStatus || "connected");
           return;
         }
         if (event === "stats") { setStats(data); return; }
         if (event === "watchUpdate") {
           setWatching(p => p.map(w => w.mint === data.mint ? { ...w, ...data } : w));
           return;
         }
         if (event === "newToken") { setMonitored(p => p.find(t => t.mint === data.mint) ? p : [data, ...p]); return; }
         if (event === "removeToken") { setMonitored(p => p.filter(t => t.mint !== data.mint)); return; }
         if (event === "tokenUpdate") { setMonitored(p => p.map(t => t.mint === data.mint ? { ...t, ...data } : t)); return; }
         if (event === "newSignal") {
           setSignals(p => p.find(s => s.id === data.id) ? p : [data, ...p].slice(0, 100));
           if (navigator.vibrate) navigator.vibrate([200, 100, 200]);
           return;
         }
         if (event === "newDemoTrade") { setDemoTrades(p => [data, ...p].slice(0, 200)); return; }
         if (event === "demoTradeUpdate") { setDemoTrades(p => p.map(t => t.id === data.id ? { ...t, ...data } : t)); return; }
         if (event === "demoTradeClosed") {
           setDemoTrades(p => p.map(t => t.id === data.id ? data : t));
           if (navigator.vibrate) navigator.vibrate(data.result === "WIN" ? [100, 50, 100, 50, 300] : [500]);
           return;
         }
         if (event === "newRealTrade") { setRealTrades(p => [data, ...p].slice(0, 200)); return; }
         if (event === "realTradeUpdate") { setRealTrades(p => p.map(t => t.id === data.id ? { ...t, ...data } : t)); return; }
         if (event === "realTradeClosed") {
           setRealTrades(p => p.map(t => t.id === data.id ? data : t));
           if (navigator.vibrate) navigator.vibrate(data.result === "WIN" ? [100, 50, 100, 50, 500] : [800]);
           return;
         }
         if (event === "log") { setLog(p => [data, ...p].slice(0, 200)); return; }
       } catch {}
     };
     ws.onerror = () => setWsStatus("error");
     ws.onclose = () => { setWsStatus("disconnected"); reconnectTimer = setTimeout(connect, 4000); };
   };
   connect();
   return () => { ws?.close(); clearTimeout(reconnectTimer); };
 }, []);

 return { watching, monitored, signals, demoTrades, realTrades, log, stats, wsStatus };
}

function DemoTradeCard({ trade }) {
 const isOpen = trade.status === "OPEN";
 const isWin = trade.result === "WIN" || trade.result === "EXPIRED_WIN";
 const color = isOpen ? "#38bdf8" : isWin ? "#22c55e" : "#ef4444";
 const statusLabel = isOpen ? "🔵 ABIERTA" : trade.result === "WIN" ? "✅ WIN" : trade.result === "LOSS" ? "❌ LOSS" : trade.result === "EXPIRED_WIN" ? "⏱️ +EXP" : "⏱️ -EXP";
 return (
   <div style={{ background: "#0d1117", border: `1px solid ${color}33`, borderRadius: 10, padding: "10px 14px" }}>
     <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
       <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
         <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: "#f1f5f9" }}>{trade.symbol}</span>
         <span style={{ fontSize: 10, color, fontWeight: 700, background: `${color}22`, padding: "1px 6px", borderRadius: 10, fontFamily: "monospace" }}>{statusLabel}</span>
         <span style={{ fontSize: 9, color: "#38bdf8", fontFamily: "monospace" }}>🎯 SNIPER</span>
       </div>
       <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>{formatTime(trade.openTime)}</span>
     </div>
     <div style={{ display: "flex", gap: 0, border: "1px solid #1e2d40", borderRadius: 8, overflow: "hidden", marginBottom: 6 }}>
       {[
         { label: "Actual %", value: `${(trade.currentPct || 0) > 0 ? "+" : ""}${(trade.currentPct || 0).toFixed(1)}%`, color: pctColor(trade.currentPct || 0) },
         { label: "Max ↑", value: `+${(trade.maxGainPct || 0).toFixed(1)}%`, color: "#22c55e" },
         { label: "Trailing", value: trade.trailingPhase || "INITIAL", color: trade.trailingPhase !== "INITIAL" ? "#facc15" : "#64748b" },
         { label: isOpen ? "⏱️" : "Duración", value: isOpen ? elapsed(trade.openTime) : `${Math.round((trade.closeTime - trade.openTime) / 1000)}s` },
       ].map((m, i) => (
         <div key={i} style={{ flex: 1, padding: "5px 4px", textAlign: "center", borderRight: i < 3 ? "1px solid #1e2d40" : "none" }}>
           <div style={{ fontSize: 9, color: "#64748b", textTransform: "uppercase", marginBottom: 2 }}>{m.label}</div>
           <div style={{ fontFamily: "monospace", fontSize: 11, fontWeight: 700, color: m.color || "#94a3b8" }}>{m.value}</div>
         </div>
       ))}
     </div>
     {!isOpen && (
       <div style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginBottom: 6 }}>
         P&L: <span style={{ color: pctColor(trade.pnlPct) }}>{trade.pnlPct > 0 ? "+" : ""}{trade.pnlPct}%</span>
       </div>
     )}
     <a href={`https://dexscreener.com/solana/${trade.mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 9, color: "#38bdf8", textDecoration: "none" }}>📊 Ver en DexScreener</a>
   </div>
 );
}

function RealTradeCard({ trade }) {
 const isOpen = trade.status === "OPEN";
 const isWin = trade.result === "WIN" || trade.result === "EXPIRED_WIN";
 const color = isOpen ? "#f97316" : isWin ? "#22c55e" : "#ef4444";
 const statusLabel = isOpen ? "🔴 REAL" : trade.result === "WIN" ? "✅ WIN" : trade.result === "LOSS" ? "❌ LOSS" : "⏱️ EXP";
 return (
   <div style={{ background: "#0d1117", border: `1px solid ${color}55`, borderRadius: 10, padding: "10px 14px", boxShadow: isOpen ? `0 0 20px ${color}22` : "none" }}>
     <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
       <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
         <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: "#f1f5f9" }}>{trade.symbol}</span>
         <span style={{ fontSize: 10, color, fontWeight: 700, background: `${color}22`, padding: "1px 6px", borderRadius: 10, fontFamily: "monospace" }}>{statusLabel}</span>
         <span style={{ fontSize: 9, color: "#38bdf8", fontFamily: "monospace" }}>🎯 SNIPER</span>
       </div>
       <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>{formatTime(trade.openTime)}</span>
     </div>
     <div style={{ display: "flex", gap: 0, border: "1px solid #1e2d40", borderRadius: 8, overflow: "hidden", marginBottom: 6 }}>
       {[
         { label: "Actual %", value: `${(trade.currentPct || 0) > 0 ? "+" : ""}${(trade.currentPct || 0).toFixed(1)}%`, color: pctColor(trade.currentPct || 0) },
         { label: "Max ↑", value: `+${(trade.maxGainPct || 0).toFixed(1)}%`, color: "#22c55e" },
         { label: "Trailing", value: trade.trailingPhase || "INITIAL", color: trade.trailingPhase !== "INITIAL" ? "#facc15" : "#64748b" },
         { label: isOpen ? "⏱️" : "P&L SOL", value: isOpen ? elapsed(trade.openTime) : `${(trade.pnlSol || 0) > 0 ? "+" : ""}${(trade.pnlSol || 0).toFixed(4)}`, color: isOpen ? "#94a3b8" : pctColor(trade.pnlSol || 0) },
       ].map((m, i) => (
         <div key={i} style={{ flex: 1, padding: "5px 4px", textAlign: "center", borderRight: i < 3 ? "1px solid #1e2d40" : "none" }}>
           <div style={{ fontSize: 9, color: "#64748b", textTransform: "uppercase", marginBottom: 2 }}>{m.label}</div>
           <div style={{ fontFamily: "monospace", fontSize: 10, fontWeight: 700, color: m.color || "#94a3b8" }}>{m.value}</div>
         </div>
       ))}
     </div>
     {trade.buySignature && (
       <div style={{ fontFamily: "monospace", fontSize: 9, color: "#64748b", marginBottom: 2 }}>
         Buy: <a href={`https://solscan.io/tx/${trade.buySignature}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8", textDecoration: "none" }}>{trade.buySignature.slice(0, 12)}…</a>
       </div>
     )}
     {trade.sellSignature && (
       <div style={{ fontFamily: "monospace", fontSize: 9, color: "#64748b", marginBottom: 4 }}>
         Sell: <a href={`https://solscan.io/tx/${trade.sellSignature}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8", textDecoration: "none" }}>{trade.sellSignature.slice(0, 12)}…</a>
       </div>
     )}
     <a href={`https://dexscreener.com/solana/${trade.mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 9, color: "#38bdf8", textDecoration: "none" }}>📊 Ver en DexScreener</a>
   </div>
 );
}

export default function App() {
 const { watching, monitored, signals, demoTrades, realTrades, log, stats, wsStatus } = useBackend();
 const [tab, setTab] = useState("monitor");
 const [demoFilter, setDemoFilter] = useState("all");
 const [realFilter, setRealFilter] = useState("all");
 const [, tick] = useState(0);
 useEffect(() => { const t = setInterval(() => tick(n => n + 1), 1000); return () => clearInterval(t); }, []);

 const statusColor = { connected: "#22c55e", connecting: "#facc15", disconnected: "#6b7280", error: "#ef4444" }[wsStatus] || "#6b7280";
 const statusLabel = { connected: "LIVE", connecting: "...", disconnected: "OFF", error: "ERR" }[wsStatus] || "—";
 const winRate = stats.demoWins + stats.demoLosses > 0 ? Math.round(stats.demoWins / (stats.demoWins + stats.demoLosses) * 100) : 0;
 const realWinRate = stats.realWins + stats.realLosses > 0 ? Math.round(stats.realWins / (stats.realWins + stats.realLosses) * 100) : 0;
 const filteredDemo = demoTrades.filter(t => demoFilter === "all" ? true : demoFilter === "open" ? t.status === "OPEN" : t.status !== "OPEN");
 const filteredReal = realTrades.filter(t => realFilter === "all" ? true : realFilter === "open" ? t.status === "OPEN" : t.status !== "OPEN");

 return (
   <div style={{ background: "#080c14", minHeight: "100dvh", color: "#e2e8f0", fontFamily: "sans-serif", display: "flex", flexDirection: "column", maxWidth: 480, margin: "0 auto" }}>
     <style>{`* { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; } body { overscroll-behavior: none; background: #080c14; } ::-webkit-scrollbar { display: none; }`}</style>

     {/* HEADER */}
     <div style={{ background: "#0d1117", borderBottom: "1px solid #1e2d40", padding: "10px 16px", display: "flex", justifyContent: "space-between", alignItems: "center", position: "sticky", top: 0, zIndex: 50 }}>
       <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
         <span style={{ fontFamily: "monospace", fontSize: 16, fontWeight: 700, color: "#38bdf8" }}>SOL<span style={{ color: "#facc15" }}>SCAN</span></span>
         <div style={{ display: "flex", alignItems: "center", gap: 5, background: "#111827", border: "1px solid #1e2d40", padding: "3px 8px", borderRadius: 20 }}>
           <div style={{ width: 6, height: 6, borderRadius: "50%", background: statusColor }} />
           <span style={{ fontFamily: "monospace", fontSize: 10, color: statusColor }}>{statusLabel}</span>
         </div>
       </div>
       <div style={{ display: "flex", gap: 10 }}>
         {[
           { label: "W%", val: `${winRate}%`, color: winRate >= 50 ? "#22c55e" : "#ef4444" },
           { label: "DEMO", val: `${(stats.demoPnL || 0) > 0 ? "+" : ""}${Math.round(stats.demoPnL || 0)}%`, color: (stats.demoPnL || 0) >= 0 ? "#22c55e" : "#ef4444" },
           { label: "SOL", val: `${(stats.walletBalance || 0).toFixed(3)}`, color: "#f97316" },
           { label: "REAL", val: `${(stats.realPnLSol || 0) >= 0 ? "+" : ""}${(stats.realPnLSol || 0).toFixed(3)}`, color: pctColor(stats.realPnLSol || 0) },
         ].map(s => (
           <div key={s.label} style={{ textAlign: "center" }}>
             <div style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: s.color }}>{s.val}</div>
             <div style={{ fontSize: 8, color: "#64748b" }}>{s.label}</div>
           </div>
         ))}
       </div>
     </div>

     {/* TABS */}
     <div style={{ display: "flex", background: "#0d1117", borderBottom: "1px solid #1e2d40", overflowX: "auto" }}>
       {[
         { id: "monitor", label: "🚀", badge: watching.length + monitored.length },
         { id: "signals", label: "🎯", badge: signals.length, accent: "#facc15" },
         { id: "demo", label: "💰 Demo", badge: stats.demoOpen },
         { id: "real", label: "🔴 Real", badge: stats.realOpen, accent: "#f97316" },
         { id: "stats", label: "📈 Stats" },
         { id: "log", label: "📋" },
       ].map(t => (
         <button key={t.id} onClick={() => setTab(t.id)} style={{ flex: "0 0 auto", padding: "10px 12px", border: "none", background: "none", fontFamily: "sans-serif", fontSize: 11, fontWeight: 600, color: tab === t.id ? (t.accent || "#38bdf8") : "#64748b", borderBottom: tab === t.id ? `2px solid ${t.accent || "#38bdf8"}` : "2px solid transparent", cursor: "pointer", display: "flex", alignItems: "center", gap: 3, whiteSpace: "nowrap" }}>
           {t.label}
           {t.badge > 0 && <span style={{ background: t.accent === "#facc15" ? "#3b2f00" : t.accent === "#f97316" ? "#3d1f00" : "#1e3a5f", color: t.accent || "#38bdf8", fontSize: 9, padding: "1px 4px", borderRadius: 10, fontFamily: "monospace" }}>{t.badge}</span>}
         </button>
       ))}
     </div>

     {/* CONTENT */}
     <div style={{ flex: 1, overflowY: "auto", padding: 12, display: "flex", flexDirection: "column", gap: 10 }}>

       {tab === "monitor" && (
         <>
           {/* Tokens en ventana sniper */}
           {watching.length > 0 && (
             <div style={{ background: "#0d1117", border: "1px solid #facc1544", borderRadius: 10, padding: 12 }}>
               <div style={{ fontFamily: "monospace", fontSize: 11, color: "#facc15", marginBottom: 8, fontWeight: 700 }}>
                 🌉 MIGRACIONES — ventana {60}s
               </div>
               {watching.map(w => (
                 <div key={w.mint} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "7px 0", borderBottom: "1px solid #1e2d4044" }}>
                   <div>
                     <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#f1f5f9" }}>{w.symbol}</span>
                     <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginLeft: 8 }}>MC {formatMC(w.migratedMcUsd)}</span>
                   </div>
                   <div style={{ display: "flex", gap: 10, fontFamily: "monospace", fontSize: 11, alignItems: "center" }}>
                     <span style={{ color: (w.volumeUSD || 0) >= 10000 ? "#22c55e" : "#facc15" }}>${Math.round(w.volumeUSD || 0)}</span>
                     <span style={{ color: "#64748b", fontSize: 10 }}>{Math.max(0, Math.round((w.timeLeft || 0) / 1000))}s</span>
                   </div>
                 </div>
               ))}
             </div>
           )}

           {/* Tokens monitorizando post-entrada */}
           {monitored.length > 0 && (
             <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 12 }}>
               <div style={{ fontFamily: "monospace", fontSize: 11, color: "#38bdf8", marginBottom: 8, fontWeight: 700 }}>
                 📊 MONITORIZANDO ({monitored.length})
               </div>
               {monitored.map(t => (
                 <div key={t.mint} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "7px 0", borderBottom: "1px solid #1e2d4044" }}>
                   <div>
                     <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#f1f5f9" }}>{t.symbol}</span>
                     <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginLeft: 8 }}>MC {formatMC(t.mc)}</span>
                   </div>
                   <div style={{ display: "flex", gap: 10, fontFamily: "monospace", fontSize: 11 }}>
                     <span style={{ color: "#94a3b8" }}>{t.tradeCount || 0} trades</span>
                     <span style={{ color: "#64748b" }}>{elapsed(t.detectedAt)}</span>
                   </div>
                 </div>
               ))}
             </div>
           )}

           {watching.length === 0 && monitored.length === 0 && (
             <EmptyState icon="🌉" text="Esperando migraciones de pump.fun a PumpSwap…" />
           )}
         </>
       )}

       {tab === "signals" && (
         <>
           {signals.length === 0 && <EmptyState icon="🎯" text="Las señales sniper aparecerán aquí." />}
           {signals.map(s => (
             <div key={s.id} style={{ background: "#0d1117", border: "1px solid #22c55e33", borderRadius: 10, padding: "10px 14px" }}>
               <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                 <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                   <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: "#f1f5f9" }}>{s.symbol}</span>
                   <span style={{ fontSize: 9, color: "#38bdf8", fontFamily: "monospace", background: "#1e3a5f", padding: "1px 5px", borderRadius: 6 }}>🎯 SNIPER</span>
                 </div>
                 <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>{formatTime(s.time)}</span>
               </div>
               <div style={{ display: "flex", gap: 12, fontFamily: "monospace", fontSize: 11, marginBottom: 8 }}>
                 <span style={{ color: "#94a3b8" }}>MC {formatMC(s.mcUsd)}</span>
                 <span style={{ color: "#64748b" }}>Vol ${Math.round(s.volumeUSD || 0)}</span>
                 <span style={{ color: "#22c55e" }}>TP +40%</span>
                 <span style={{ color: "#ef4444" }}>SL -10%</span>
               </div>
               <a href={`https://dexscreener.com/solana/${s.mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 10, color: "#38bdf8", textDecoration: "none" }}>📊 Ver en DexScreener →</a>
             </div>
           ))}
         </>
       )}

       {tab === "demo" && (
         <>
           <div style={{ display: "flex", gap: 6 }}>
             {["all", "open", "closed"].map(f => (
               <button key={f} onClick={() => setDemoFilter(f)} style={{ flex: 1, padding: "6px", border: `1px solid ${demoFilter === f ? "#38bdf8" : "#1e2d40"}`, borderRadius: 8, background: demoFilter === f ? "#1e3a5f" : "none", color: demoFilter === f ? "#38bdf8" : "#64748b", fontFamily: "monospace", fontSize: 11, cursor: "pointer" }}>
                 {f === "all" ? "Todas" : f === "open" ? "Abiertas" : "Cerradas"}
               </button>
             ))}
           </div>
           {filteredDemo.length === 0 && <EmptyState icon="💰" text="Las operaciones demo aparecerán aquí." />}
           {filteredDemo.map(t => <DemoTradeCard key={t.id} trade={t} />)}
         </>
       )}

       {tab === "real" && (
         <>
           <div style={{ background: "#0d1117", border: "1px solid #f9741633", borderRadius: 10, padding: 12 }}>
             <div style={{ fontFamily: "monospace", fontSize: 11, color: "#f97316", marginBottom: 8, fontWeight: 700 }}>🔴 TRADING REAL</div>
             <div style={{ display: "flex", justifyContent: "space-around" }}>
               {[
                 { label: "Balance", val: `${(stats.walletBalance || 0).toFixed(4)} SOL`, color: "#f97316" },
                 { label: "Wins", val: stats.realWins || 0, color: "#22c55e" },
                 { label: "Losses", val: stats.realLosses || 0, color: "#ef4444" },
                 { label: "P&L SOL", val: `${(stats.realPnLSol || 0) >= 0 ? "+" : ""}${(stats.realPnLSol || 0).toFixed(4)}`, color: pctColor(stats.realPnLSol || 0) },
                 { label: "Win%", val: `${realWinRate}%`, color: realWinRate >= 50 ? "#22c55e" : "#ef4444" },
               ].map(s => (
                 <div key={s.label} style={{ textAlign: "center" }}>
                   <div style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: s.color }}>{s.val}</div>
                   <div style={{ fontSize: 9, color: "#64748b" }}>{s.label}</div>
                 </div>
               ))}
             </div>
           </div>
           <div style={{ display: "flex", gap: 6 }}>
             {["all", "open", "closed"].map(f => (
               <button key={f} onClick={() => setRealFilter(f)} style={{ flex: 1, padding: "6px", border: `1px solid ${realFilter === f ? "#f97316" : "#1e2d40"}`, borderRadius: 8, background: realFilter === f ? "#3d1f00" : "none", color: realFilter === f ? "#f97316" : "#64748b", fontFamily: "monospace", fontSize: 11, cursor: "pointer" }}>
                 {f === "all" ? "Todas" : f === "open" ? "Abiertas" : "Cerradas"}
               </button>
             ))}
           </div>
           {filteredReal.length === 0 && <EmptyState icon="🔴" text="Las operaciones reales aparecerán aquí." />}
           {filteredReal.map(t => <RealTradeCard key={t.id} trade={t} />)}
         </>
       )}

       {tab === "stats" && (
         <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
           <div style={{ background: "#0d1117", border: "1px solid #facc1533", borderRadius: 10, padding: 14 }}>
             <div style={{ fontFamily: "monospace", fontSize: 12, color: "#facc15", marginBottom: 10 }}>🌉 SNIPER</div>
             {[
               { label: "Migraciones detectadas", val: stats.migrations || 0 },
               { label: "Entraron en ventana", val: stats.watched || 0, color: "#38bdf8" },
               { label: "Pasaron filtro vol", val: stats.entered || 0, color: "#22c55e" },
               { label: "Rechazados", val: stats.rejected || 0, color: "#ef4444" },
               { label: "Tasa entrada", val: stats.watched > 0 ? `${Math.round((stats.entered / stats.watched) * 100)}%` : "—", color: "#facc15" },
             ].map(s => (
               <div key={s.label} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e2d40" }}>
                 <span style={{ fontSize: 12, color: "#94a3b8" }}>{s.label}</span>
                 <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: s.color || "#f1f5f9" }}>{s.val}</span>
               </div>
             ))}
           </div>

           <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 14 }}>
             <div style={{ fontFamily: "monospace", fontSize: 12, color: "#64748b", marginBottom: 10 }}>DEMO</div>
             {[
               { label: "Cerradas", val: stats.closedCount || 0 },
               { label: "Wins", val: stats.demoWins || 0, color: "#22c55e" },
               { label: "Losses", val: stats.demoLosses || 0, color: "#ef4444" },
               { label: "Expiradas", val: stats.demoExpired || 0, color: "#f97316" },
               { label: "Win Rate", val: `${winRate}%`, color: winRate >= 50 ? "#22c55e" : "#ef4444" },
               { label: "P&L Total", val: `${(stats.demoPnL || 0) > 0 ? "+" : ""}${Math.round(stats.demoPnL || 0)}%`, color: (stats.demoPnL || 0) >= 0 ? "#22c55e" : "#ef4444" },
             ].map(s => (
               <div key={s.label} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e2d40" }}>
                 <span style={{ fontSize: 12, color: "#94a3b8" }}>{s.label}</span>
                 <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: s.color || "#f1f5f9" }}>{s.val}</span>
               </div>
             ))}
           </div>

           <div style={{ background: "#0d1117", border: "1px solid #f9741633", borderRadius: 10, padding: 14 }}>
             <div style={{ fontFamily: "monospace", fontSize: 12, color: "#f97316", marginBottom: 10 }}>REAL</div>
             {[
               { label: "Balance wallet", val: `${(stats.walletBalance || 0).toFixed(4)} SOL`, color: "#f97316" },
               { label: "Wins", val: stats.realWins || 0, color: "#22c55e" },
               { label: "Losses", val: stats.realLosses || 0, color: "#ef4444" },
               { label: "Win Rate", val: `${realWinRate}%`, color: realWinRate >= 50 ? "#22c55e" : "#ef4444" },
               { label: "P&L SOL", val: `${(stats.realPnLSol || 0) >= 0 ? "+" : ""}${(stats.realPnLSol || 0).toFixed(4)} SOL`, color: pctColor(stats.realPnLSol || 0) },
             ].map(s => (
               <div key={s.label} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e2d40" }}>
                 <span style={{ fontSize: 12, color: "#94a3b8" }}>{s.label}</span>
                 <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: s.color || "#f1f5f9" }}>{s.val}</span>
               </div>
             ))}
           </div>

           <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 14 }}>
             <div style={{ fontFamily: "monospace", fontSize: 12, color: "#64748b", marginBottom: 10 }}>ANÁLISIS</div>
             {(stats.closedCount || 0) === 0 ? (
               <div style={{ fontFamily: "monospace", fontSize: 11, color: "#475569", textAlign: "center", padding: 10 }}>Necesitas operaciones cerradas</div>
             ) : [
               { label: "Ganancia máx media", val: `+${(stats.avgMaxGain || 0).toFixed(1)}%`, color: "#22c55e", desc: "Media del máximo que suben" },
               { label: "Pérdida máx media", val: `-${(stats.avgMaxLoss || 0).toFixed(1)}%`, color: "#ef4444", desc: "Media del máximo que bajan" },
               { label: "Trailing óptimo", val: (stats.avgMaxGain || 0) > 0 ? `+${Math.max(5, Math.round((stats.avgMaxGain || 0) * 0.6))}%` : "—", color: "#facc15", desc: "60% del máximo de ganancia" },
             ].map(s => (
               <div key={s.label} style={{ padding: "8px 0", borderBottom: "1px solid #1e2d40" }}>
                 <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 2 }}>
                   <span style={{ fontSize: 12, color: "#94a3b8" }}>{s.label}</span>
                   <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: s.color }}>{s.val}</span>
                 </div>
                 <div style={{ fontSize: 10, color: "#475569" }}>{s.desc}</div>
               </div>
             ))}
           </div>
         </div>
       )}

       {tab === "log" && log.map((entry, i) => (
         <div key={i} style={{ display: "flex", gap: 8, padding: "3px 0", borderBottom: "1px solid #0d1117" }}>
           <span style={{ fontFamily: "monospace", fontSize: 10, color: "#334155", flexShrink: 0 }}>{formatTime(entry.time)}</span>
           <span style={{ fontFamily: "monospace", fontSize: 10, color: { info: "#64748b", filter: "#475569", accept: "#22c55e", signal: "#facc15", warn: "#f97316", error: "#ef4444", demo: "#a78bfa", win: "#22c55e", loss: "#ef4444", expire: "#f97316", trail: "#facc15", real: "#f97316", realwin: "#22c55e", realloss: "#ef4444" }[entry.type] || "#64748b" }}>{entry.msg}</span>
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
