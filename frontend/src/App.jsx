import { useState, useEffect } from "react";

const BACKEND_WS = import.meta.env.VITE_BACKEND_WS || "ws://localhost:3001";

function formatMC(n) {
  if (!n) return "—";
  if (n >= 1_000_000) return `$${(n/1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `$${(n/1_000).toFixed(1)}K`;
  return `$${Math.round(n)}`;
}
function formatTime(ts) { return new Date(ts).toLocaleTimeString("es-ES", { hour12: false }); }
function shortAddr(a) { return a ? `${a.slice(0,4)}…${a.slice(-4)}` : "—"; }
function elapsed(ts) {
  const s = Math.floor((Date.now() - ts) / 1000);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s/60)}m${s%60}s`;
  return `${Math.floor(s/3600)}h${Math.floor((s%3600)/60)}m`;
}
function pctColor(p) { return p >= 0 ? "#22c55e" : "#ef4444"; }

function useBackend() {
  const [migWatching, setMigWatching] = useState([]);
  const [migMonitored, setMigMonitored] = useState([]);
  const [momMonitored, setMomMonitored] = useState([]);
  const [signals, setSignals] = useState([]);
  const [demoTrades, setDemoTrades] = useState([]);
  const [realTrades, setRealTrades] = useState([]);
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
            setLog(data.log || []);
            setStats(data.stats || {});
            setWsStatus(data.wsStatus || "connected");
            return;
          }
          if (event === "stats") { setStats(data); return; }
          if (event === "migWatchUpdate") { setMigWatching(p => p.map(w => w.mint === data.mint ? { ...w, ...data } : w)); return; }
          if (event === "newMigToken") { setMigMonitored(p => p.find(t => t.mint === data.mint) ? p : [data, ...p]); return; }
          if (event === "migTokenUpdate") { setMigMonitored(p => p.map(t => t.mint === data.mint ? { ...t, ...data } : t)); return; }
          if (event === "newMomToken") { setMomMonitored(p => p.find(t => t.mint === data.mint) ? p : [data, ...p]); return; }
          if (event === "momTokenUpdate") { setMomMonitored(p => p.map(t => t.mint === data.mint ? { ...t, ...data } : t)); return; }
          if (event === "newSignal") { setSignals(p => [data, ...p].slice(0, 100)); if (navigator.vibrate) navigator.vibrate([200,100,200]); return; }
          if (event === "newDemoTrade") { setDemoTrades(p => [data, ...p].slice(0, 200)); return; }
          if (event === "demoTradeUpdate") { setDemoTrades(p => p.map(t => t.id === data.id ? { ...t, ...data } : t)); return; }
          if (event === "demoTradeClosed") { setDemoTrades(p => p.map(t => t.id === data.id ? data : t)); return; }
          if (event === "newRealTrade") { setRealTrades(p => [data, ...p].slice(0, 200)); return; }
          if (event === "realTradeUpdate") { setRealTrades(p => p.map(t => t.id === data.id ? { ...t, ...data } : t)); return; }
          if (event === "realTradeClosed") { setRealTrades(p => p.map(t => t.id === data.id ? data : t)); return; }
          if (event === "log") { setLog(p => [data, ...p].slice(0, 200)); return; }
        } catch {}
      };
      ws.onerror = () => setWsStatus("error");
      ws.onclose = () => { setWsStatus("disconnected"); t = setTimeout(connect, 4000); };
    };
    connect();
    return () => { ws?.close(); clearTimeout(t); };
  }, []);

  return { migWatching, migMonitored, momMonitored, signals, demoTrades, realTrades, log, stats, wsStatus };
}

function StrategyBadge({ strategy }) {
  const isMig = strategy === "migration";
  return (
    <span style={{ fontSize: 9, fontFamily: "monospace", color: isMig ? "#facc15" : "#a78bfa", background: isMig ? "#3b2f00" : "#2d1b69", padding: "1px 5px", borderRadius: 6 }}>
      {isMig ? "🌉 MIG" : "⚡ MOM"}
    </span>
  );
}

function TradeCard({ trade, isReal }) {
  const isOpen = trade.status === "OPEN";
  const isWin = trade.result === "WIN" || trade.result === "EXPIRED_WIN";
  const color = isOpen ? (isReal ? "#f97316" : "#38bdf8") : isWin ? "#22c55e" : "#ef4444";
  const statusLabel = isOpen ? (isReal ? "🔴 REAL" : "🔵 DEMO") : trade.result === "WIN" ? "✅ WIN" : trade.result === "LOSS" ? "❌ LOSS" : trade.result?.includes("WIN") ? "⏱️ +EXP" : "⏱️ -EXP";
  const tpPct = trade.strategy === "migration" ? "+40%" : "+30%";
  const slPct = trade.strategy === "migration" ? "-10%" : "-8%";

  return (
    <div style={{ background: "#0d1117", border: `1px solid ${color}${isOpen ? "55" : "33"}`, borderRadius: 10, padding: "10px 14px", boxShadow: isOpen && isReal ? `0 0 16px ${color}22` : "none" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: "#f1f5f9" }}>{trade.symbol}</span>
          <span style={{ fontSize: 10, color, fontWeight: 700, background: `${color}22`, padding: "1px 6px", borderRadius: 10, fontFamily: "monospace" }}>{statusLabel}</span>
          <StrategyBadge strategy={trade.strategy} />
        </div>
        <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>{formatTime(trade.openTime)}</span>
      </div>
      <div style={{ display: "flex", gap: 0, border: "1px solid #1e2d40", borderRadius: 8, overflow: "hidden", marginBottom: 6 }}>
        {[
          { label: "Actual", value: `${(trade.currentPct||0)>0?"+":""}${(trade.currentPct||0).toFixed(1)}%`, color: pctColor(trade.currentPct||0) },
          { label: "Max ↑", value: `+${(trade.maxGainPct||0).toFixed(1)}%`, color: "#22c55e" },
          { label: "Trailing", value: trade.trailingPhase||"INITIAL", color: trade.trailingPhase !== "INITIAL" ? "#facc15" : "#64748b" },
          { label: isOpen ? "⏱️" : isReal ? "P&L SOL" : "Dur", value: isOpen ? elapsed(trade.openTime) : isReal ? `${(trade.pnlSol||0)>=0?"+":""}${(trade.pnlSol||0).toFixed(4)}` : `${Math.round((trade.closeTime-trade.openTime)/1000)}s`, color: isOpen ? "#94a3b8" : isReal ? pctColor(trade.pnlSol||0) : "#94a3b8" },
        ].map((m, i) => (
          <div key={i} style={{ flex: 1, padding: "5px 4px", textAlign: "center", borderRight: i < 3 ? "1px solid #1e2d40" : "none" }}>
            <div style={{ fontSize: 9, color: "#64748b", textTransform: "uppercase", marginBottom: 2 }}>{m.label}</div>
            <div style={{ fontFamily: "monospace", fontSize: 10, fontWeight: 700, color: m.color }}>{m.value}</div>
          </div>
        ))}
      </div>
      {!isOpen && <div style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginBottom: 4 }}>P&L: <span style={{ color: pctColor(trade.pnlPct) }}>{trade.pnlPct>0?"+":""}{trade.pnlPct}%</span></div>}
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

export default function App() {
  const { migWatching, migMonitored, momMonitored, signals, demoTrades, realTrades, log, stats, wsStatus } = useBackend();
  const [tab, setTab] = useState("migration");
  const [demoFilter, setDemoFilter] = useState("all");
  const [realFilter, setRealFilter] = useState("all");
  const [, tick] = useState(0);
  useEffect(() => { const t = setInterval(() => tick(n => n+1), 1000); return () => clearInterval(t); }, []);

  const statusColor = { connected: "#22c55e", connecting: "#facc15", disconnected: "#6b7280", error: "#ef4444" }[wsStatus] || "#6b7280";
  const statusLabel = { connected: "LIVE", connecting: "...", disconnected: "OFF", error: "ERR" }[wsStatus] || "—";

  // Stats migración
  const migWR = (stats.mig_demoWins||0) + (stats.mig_demoLosses||0) > 0 ? Math.round((stats.mig_demoWins||0) / ((stats.mig_demoWins||0) + (stats.mig_demoLosses||0)) * 100) : 0;
  const momWR = (stats.mom_demoWins||0) + (stats.mom_demoLosses||0) > 0 ? Math.round((stats.mom_demoWins||0) / ((stats.mom_demoWins||0) + (stats.mom_demoLosses||0)) * 100) : 0;

  const filteredDemo = demoTrades.filter(t => demoFilter === "all" ? true : demoFilter === "open" ? t.status === "OPEN" : t.status !== "OPEN");
  const filteredReal = realTrades.filter(t => realFilter === "all" ? true : realFilter === "open" ? t.status === "OPEN" : t.status !== "OPEN");

  const migDemoOpen = demoTrades.filter(t => t.status === "OPEN" && t.strategy === "migration").length;
  const momDemoOpen = demoTrades.filter(t => t.status === "OPEN" && t.strategy === "momentum").length;
  const migRealOpen = realTrades.filter(t => t.status === "OPEN" && t.strategy === "migration").length;
  const momRealOpen = realTrades.filter(t => t.status === "OPEN" && t.strategy === "momentum").length;

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
        <div style={{ display: "flex", gap: 8 }}>
          {[
            { label: "MIG W%", val: `${migWR}%`, color: migWR >= 50 ? "#22c55e" : "#ef4444" },
            { label: "MOM W%", val: `${momWR}%`, color: momWR >= 50 ? "#22c55e" : "#ef4444" },
            { label: "SOL", val: `${(stats.walletBalance||0).toFixed(3)}`, color: "#f97316" },
          ].map(s => (
            <div key={s.label} style={{ textAlign: "center" }}>
              <div style={{ fontFamily: "monospace", fontSize: 11, fontWeight: 700, color: s.color }}>{s.val}</div>
              <div style={{ fontSize: 8, color: "#64748b" }}>{s.label}</div>
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

        {/* ── MIGRACIÓN ── */}
        {tab === "migration" && (
          <>
            {migWatching.length > 0 && (
              <div style={{ background: "#0d1117", border: "1px solid #facc1544", borderRadius: 10, padding: 12 }}>
                <div style={{ fontFamily: "monospace", fontSize: 11, color: "#facc15", marginBottom: 8, fontWeight: 700 }}>🌉 VENTANA MIGRACIÓN — 60s</div>
                {migWatching.map(w => (
                  <div key={w.mint} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e2d4044" }}>
                    <div>
                      <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#f1f5f9" }}>{w.symbol}</span>
                      <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginLeft: 8 }}>{formatMC(w.migratedMcUsd)}</span>
                    </div>
                    <div style={{ display: "flex", gap: 10, fontFamily: "monospace", fontSize: 11 }}>
                      <span style={{ color: (w.volumeUSD||0) >= 10000 ? "#22c55e" : "#facc15" }}>${Math.round(w.volumeUSD||0)}</span>
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

        {/* ── MOMENTUM ── */}
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
                      <span style={{ color: "#22c55e" }}>+{(t.pct5m||0).toFixed(1)}%</span>
                      <span style={{ color: "#64748b" }}>{elapsed(t.detectedAt)}</span>
                    </div>
                  </div>
                ))}
              </div>
            )}
            {momMonitored.length === 0 && <EmptyState icon="⚡" text="Escaneando tokens con momentum…" />}
          </>
        )}

        {/* ── SEÑALES ── */}
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
                  {s.pct5m && <span style={{ color: "#22c55e" }}>+{s.pct5m?.toFixed(1)}% 5m</span>}
                  <span style={{ color: "#22c55e" }}>TP {s.strategy === "migration" ? "+40%" : "+30%"}</span>
                  <span style={{ color: "#ef4444" }}>SL {s.strategy === "migration" ? "-10%" : "-8%"}</span>
                </div>
                <a href={`https://dexscreener.com/solana/${s.mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 10, color: "#38bdf8", textDecoration: "none" }}>📊 DexScreener →</a>
              </div>
            ))}
          </>
        )}

        {/* ── DEMO ── */}
        {tab === "demo" && (
          <>
            {/* Mini resumen por estrategia */}
            <div style={{ display: "flex", gap: 8 }}>
              {[
                { label: "🌉 Mig abiertas", val: migDemoOpen, color: "#facc15" },
                { label: "⚡ Mom abiertas", val: momDemoOpen, color: "#a78bfa" },
              ].map(s => (
                <div key={s.label} style={{ flex: 1, background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 8, padding: "8px 10px", textAlign: "center" }}>
                  <div style={{ fontFamily: "monospace", fontSize: 14, fontWeight: 700, color: s.color }}>{s.val}</div>
                  <div style={{ fontSize: 9, color: "#64748b" }}>{s.label}</div>
                </div>
              ))}
            </div>
            <div style={{ display: "flex", gap: 6 }}>
              {["all", "open", "closed"].map(f => (
                <button key={f} onClick={() => setDemoFilter(f)} style={{ flex: 1, padding: "6px", border: `1px solid ${demoFilter === f ? "#38bdf8" : "#1e2d40"}`, borderRadius: 8, background: demoFilter === f ? "#1e3a5f" : "none", color: demoFilter === f ? "#38bdf8" : "#64748b", fontFamily: "monospace", fontSize: 11, cursor: "pointer" }}>
                  {f === "all" ? "Todas" : f === "open" ? "Abiertas" : "Cerradas"}
                </button>
              ))}
            </div>
            {filteredDemo.length === 0 && <EmptyState icon="💰" text="Las operaciones demo aparecerán aquí." />}
            {filteredDemo.map(t => <TradeCard key={t.id} trade={t} isReal={false} />)}
          </>
        )}

        {/* ── REAL ── */}
        {tab === "real" && (
          <>
            <div style={{ background: "#0d1117", border: "1px solid #f9741633", borderRadius: 10, padding: 12 }}>
              <div style={{ fontFamily: "monospace", fontSize: 11, color: "#f97316", marginBottom: 8, fontWeight: 700 }}>🔴 TRADING REAL</div>
              <div style={{ display: "flex", justifyContent: "space-around" }}>
                {[
                  { label: "Balance", val: `${(stats.walletBalance||0).toFixed(4)} SOL`, color: "#f97316" },
                  { label: "🌉 Abierta", val: migRealOpen, color: "#facc15" },
                  { label: "⚡ Abierta", val: momRealOpen, color: "#a78bfa" },
                  { label: "P&L SOL", val: `${((stats.mig_realPnLSol||0)+(stats.mom_realPnLSol||0))>=0?"+":""}${((stats.mig_realPnLSol||0)+(stats.mom_realPnLSol||0)).toFixed(4)}`, color: pctColor((stats.mig_realPnLSol||0)+(stats.mom_realPnLSol||0)) },
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
            {filteredReal.map(t => <TradeCard key={t.id} trade={t} isReal={true} />)}
          </>
        )}

        {/* ── STATS ── */}
        {tab === "stats" && (
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {/* Migración */}
            <div style={{ background: "#0d1117", border: "1px solid #facc1533", borderRadius: 10, padding: 14 }}>
              <div style={{ fontFamily: "monospace", fontSize: 12, color: "#facc15", marginBottom: 10, fontWeight: 700 }}>🌉 MIGRACIÓN</div>
              <StatsRow label="Migraciones" val={stats.mig_migrations||0} />
              <StatsRow label="En ventana" val={stats.mig_watched||0} color="#38bdf8" />
              <StatsRow label="Entradas" val={stats.mig_entered||0} color="#22c55e" />
              <StatsRow label="Rechazados" val={stats.mig_rejected||0} color="#ef4444" />
              <StatsRow label="Demo Wins" val={stats.mig_demoWins||0} color="#22c55e" />
              <StatsRow label="Demo Losses" val={stats.mig_demoLosses||0} color="#ef4444" />
              <StatsRow label="Win Rate" val={`${migWR}%`} color={migWR >= 50 ? "#22c55e" : "#ef4444"} />
              <StatsRow label="P&L Demo" val={`${(stats.mig_demoPnL||0)>=0?"+":""}${Math.round(stats.mig_demoPnL||0)}%`} color={pctColor(stats.mig_demoPnL||0)} />
              <StatsRow label="Ganancia máx media" val={`+${(stats.mig_avgMaxGain||0).toFixed(1)}%`} color="#22c55e" desc="Media del máximo que suben" />
              <StatsRow label="Pérdida máx media" val={`-${(stats.mig_avgMaxLoss||0).toFixed(1)}%`} color="#ef4444" desc="Media del máximo que bajan" />
            </div>

            {/* Momentum */}
            <div style={{ background: "#0d1117", border: "1px solid #a78bfa33", borderRadius: 10, padding: 14 }}>
              <div style={{ fontFamily: "monospace", fontSize: 12, color: "#a78bfa", marginBottom: 10, fontWeight: 700 }}>⚡ MOMENTUM</div>
              <StatsRow label="Tokens escaneados" val={stats.mom_scanned||0} />
              <StatsRow label="Señales" val={stats.mom_signals||0} color="#a78bfa" />
              <StatsRow label="Demo Wins" val={stats.mom_demoWins||0} color="#22c55e" />
              <StatsRow label="Demo Losses" val={stats.mom_demoLosses||0} color="#ef4444" />
              <StatsRow label="Win Rate" val={`${momWR}%`} color={momWR >= 50 ? "#22c55e" : "#ef4444"} />
              <StatsRow label="P&L Demo" val={`${(stats.mom_demoPnL||0)>=0?"+":""}${Math.round(stats.mom_demoPnL||0)}%`} color={pctColor(stats.mom_demoPnL||0)} />
              <StatsRow label="Ganancia máx media" val={`+${(stats.mom_avgMaxGain||0).toFixed(1)}%`} color="#22c55e" desc="Media del máximo que suben" />
              <StatsRow label="Pérdida máx media" val={`-${(stats.mom_avgMaxLoss||0).toFixed(1)}%`} color="#ef4444" desc="Media del máximo que bajan" />
            </div>

            {/* Real */}
            <div style={{ background: "#0d1117", border: "1px solid #f9741633", borderRadius: 10, padding: 14 }}>
              <div style={{ fontFamily: "monospace", fontSize: 12, color: "#f97316", marginBottom: 10, fontWeight: 700 }}>🔴 REAL</div>
              <StatsRow label="Balance" val={`${(stats.walletBalance||0).toFixed(4)} SOL`} color="#f97316" />
              <StatsRow label="🌉 Wins" val={stats.mig_realWins||0} color="#22c55e" />
              <StatsRow label="🌉 Losses" val={stats.mig_realLosses||0} color="#ef4444" />
              <StatsRow label="🌉 P&L SOL" val={`${(stats.mig_realPnLSol||0)>=0?"+":""}${(stats.mig_realPnLSol||0).toFixed(4)}`} color={pctColor(stats.mig_realPnLSol||0)} />
              <StatsRow label="⚡ Wins" val={stats.mom_realWins||0} color="#22c55e" />
              <StatsRow label="⚡ Losses" val={stats.mom_realLosses||0} color="#ef4444" />
              <StatsRow label="⚡ P&L SOL" val={`${(stats.mom_realPnLSol||0)>=0?"+":""}${(stats.mom_realPnLSol||0).toFixed(4)}`} color={pctColor(stats.mom_realPnLSol||0)} />
            </div>
          </div>
        )}

        {/* ── LOG ── */}
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
