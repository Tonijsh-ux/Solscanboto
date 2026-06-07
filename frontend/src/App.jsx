import { useState, useEffect, useRef, useCallback } from "react";

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
  return `${Math.floor(s / 60)}m${s % 60}s`;
}
function pctColor(pct) { return pct >= 0 ? "#22c55e" : "#ef4444"; }
function priceToMC(price) { return price * 1_000_000_000; }

function useBackend() {
  const [monitored, setMonitored] = useState([]);
  const [signals, setSignals] = useState([]);
  const [demoTrades, setDemoTrades] = useState([]);
  const [realTrades, setRealTrades] = useState([]);
  const [log, setLog] = useState([]);
  const [stats, setStats] = useState({
    seen: 0, filtered: 0, signals: 0,
    demoOpen: 0, demoWins: 0, demoLosses: 0, demoExpired: 0, demoPnL: 0,
    realOpen: 0, realWins: 0, realLosses: 0, realExpired: 0, realPnL: 0, realPnLSol: 0,
    avgMaxGain: 0, avgMaxLoss: 0, closedCount: 0, walletBalance: 0,
  });
  const [wsStatus, setWsStatus] = useState("connecting");
  const wsRef = useRef(null);

  const removeToken = useCallback((mint) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ action: "removeToken", mint }));
    }
  }, []);

  useEffect(() => {
    let ws; let reconnectTimer;
    const connect = () => {
      setWsStatus("connecting");
      ws = new WebSocket(BACKEND_WS);
      wsRef.current = ws;
      ws.onopen = () => setWsStatus("connected");
      ws.onmessage = (evt) => {
        try {
          const { event, data } = JSON.parse(evt.data);
          if (event === "fullState") {
            setMonitored(data.monitored || []);
            setSignals(data.signals || []);
            setDemoTrades(data.demoTrades || []);
            setRealTrades(data.realTrades || []);
            setLog(data.log || []);
            setStats(data.stats || {});
            setWsStatus(data.wsStatus || "connected");
            return;
          }
          if (event === "wsStatus") { setWsStatus(data); return; }
          if (event === "stats") { setStats(data); return; }
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

  return { monitored, signals, demoTrades, realTrades, log, stats, wsStatus, removeToken };
}

function Sparkline({ candles, bb }) {
  if (!candles || candles.length < 3) return <div style={{ fontSize: 10, color: "#334155", fontFamily: "monospace" }}>acumulando…</div>;
  const w = 100, h = 36;
  const prices = candles.map(c => c.close);
  const min = Math.min(...prices); const max = Math.max(...prices); const range = max - min || 1;
  const pts = prices.map((p, i) => `${(i / (prices.length - 1)) * w},${h - ((p - min) / range) * (h - 4) - 2}`).join(" ");
  const bbY = v => h - ((v - min) / range) * (h - 4) - 2;
  const isUp = prices[prices.length - 1] >= prices[0];
  return (
    <svg width={w} height={h} style={{ display: "block" }}>
      {bb && (<><line x1="0" y1={bbY(bb.upper)} x2={w} y2={bbY(bb.upper)} stroke="#ef4444" strokeWidth="0.6" strokeDasharray="2,2" opacity="0.6" /><line x1="0" y1={bbY(bb.middle)} x2={w} y2={bbY(bb.middle)} stroke="#facc15" strokeWidth="0.8" opacity="0.8" /><line x1="0" y1={bbY(bb.lower)} x2={w} y2={bbY(bb.lower)} stroke="#22c55e" strokeWidth="0.6" strokeDasharray="2,2" opacity="0.6" /></>)}
      <polyline points={pts} fill="none" stroke={isUp ? "#22c55e" : "#ef4444"} strokeWidth="1.5" strokeLinejoin="round" />
    </svg>
  );
}

function TokenCard({ token, onRemove }) {
  const { mint, name, symbol, mc, price, bb, signal, candleCount, candles, detectedAt, tp, sl, twitter, website } = token;
  const hasBB = candleCount >= 20;
  const signalColor = signal === "LOWER" ? "#22c55e" : signal === "MIDDLE" ? "#facc15" : null;
  return (
    <div style={{ background: "#0d1117", border: `1px solid ${signalColor || "#1e2d40"}`, borderRadius: 12, padding: "12px 14px", display: "flex", flexDirection: "column", gap: 8, boxShadow: signal ? `0 0 16px ${signalColor}22` : "none" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontFamily: "monospace", fontSize: 15, fontWeight: 700, color: "#f1f5f9" }}>{symbol}</span>
          <span style={{ fontSize: 11, color: "#64748b" }}>{name?.length > 16 ? name.slice(0, 14) + "…" : name}</span>
        </div>
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          {twitter && <a href={twitter} target="_blank" rel="noreferrer" style={{ fontSize: 11, color: "#94a3b8", background: "#1e2d40", padding: "2px 6px", borderRadius: 4, textDecoration: "none" }}>𝕏</a>}
          {website && <a href={website} target="_blank" rel="noreferrer" style={{ fontSize: 11, color: "#94a3b8", background: "#1e2d40", padding: "2px 6px", borderRadius: 4, textDecoration: "none" }}>🌐</a>}
          <button onClick={() => onRemove(mint)} style={{ background: "none", border: "none", color: "#475569", cursor: "pointer", fontSize: 14, padding: "0 2px" }}>✕</button>
        </div>
      </div>
      <div style={{ display: "flex", gap: 0, border: "1px solid #1e2d40", borderRadius: 8, overflow: "hidden" }}>
        {[{ label: "Precio", value: formatUSD(price) }, { label: "MC", value: formatMC(mc) }, { label: "Velas", value: `${candleCount}/20`, color: hasBB ? "#22c55e" : "#facc15" }, { label: "Tiempo", value: elapsed(detectedAt) }].map((m, i) => (
          <div key={i} style={{ flex: 1, padding: "5px 4px", textAlign: "center", borderRight: i < 3 ? "1px solid #1e2d40" : "none" }}>
            <div style={{ fontSize: 9, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.4px", marginBottom: 2 }}>{m.label}</div>
            <div style={{ fontFamily: "monospace", fontSize: 11, fontWeight: 700, color: m.color || "#94a3b8" }}>{m.value}</div>
          </div>
        ))}
      </div>
      <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
        <Sparkline candles={candles} bb={bb} />
        {bb && (<div style={{ display: "flex", flexDirection: "column", gap: 1, fontFamily: "monospace", fontSize: 9 }}><span style={{ color: "#ef4444" }}>↑{formatUSD(bb.upper)}</span><span style={{ color: "#facc15" }}>—{formatUSD(bb.middle)}</span><span style={{ color: "#22c55e" }}>↓{formatUSD(bb.lower)}</span></div>)}
      </div>
      {signal && (
        <div style={{ background: `${signalColor}18`, border: `1px solid ${signalColor}`, borderRadius: 8, padding: "8px 10px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: signalColor }}>🎯 {signal}</span>
          <div style={{ display: "flex", gap: 10, fontFamily: "monospace", fontSize: 11 }}>
            <span style={{ color: "#22c55e" }}>TP {formatMC(priceToMC(tp))}</span>
            <span style={{ color: "#ef4444" }}>SL {formatMC(priceToMC(sl))}</span>
          </div>
        </div>
      )}
      <a href={`https://dexscreener.com/solana/${mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 9, color: "#38bdf8", textDecoration: "none" }}>📊 {shortAddr(mint)} — Ver en DexScreener</a>
    </div>
  );
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
        </div>
        <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>{formatTime(trade.openTime)}</span>
      </div>
      <div style={{ display: "flex", gap: 0, border: "1px solid #1e2d40", borderRadius: 8, overflow: "hidden", marginBottom: 6 }}>
        {[
          { label: "MC Entrada", value: formatMC(priceToMC(trade.entryPrice)) },
          { label: "MC TP", value: formatMC(priceToMC(trade.tp)), color: "#22c55e" },
          { label: "MC SL", value: formatMC(priceToMC(trade.sl)), color: "#ef4444" },
        ].map((m, i) => (
          <div key={i} style={{ flex: 1, padding: "5px 4px", textAlign: "center", borderRight: i < 2 ? "1px solid #1e2d40" : "none" }}>
            <div style={{ fontSize: 9, color: "#64748b", textTransform: "uppercase", marginBottom: 2 }}>{m.label}</div>
            <div style={{ fontFamily: "monospace", fontSize: 11, fontWeight: 700, color: m.color || "#94a3b8" }}>{m.value}</div>
          </div>
        ))}
      </div>
      <div style={{ display: "flex", gap: 0, border: "1px solid #1e2d40", borderRadius: 8, overflow: "hidden", marginBottom: 6 }}>
        {[
          { label: "Actual %", value: `${(trade.currentPct || 0) > 0 ? "+" : ""}${(trade.currentPct || 0).toFixed(1)}%`, color: pctColor(trade.currentPct || 0) },
          { label: "Max ↑", value: `+${(trade.maxGainPct || 0).toFixed(1)}%`, color: "#22c55e" },
          { label: "Max ↓", value: `${(trade.maxLossPct || 0).toFixed(1)}%`, color: "#ef4444" },
          { label: isOpen ? "⏱️" : "Duración", value: isOpen ? elapsed(trade.openTime) : `${Math.round((trade.closeTime - trade.openTime) / 1000)}s` },
        ].map((m, i) => (
          <div key={i} style={{ flex: 1, padding: "5px 4px", textAlign: "center", borderRight: i < 3 ? "1px solid #1e2d40" : "none" }}>
            <div style={{ fontSize: 9, color: "#64748b", textTransform: "uppercase", marginBottom: 2 }}>{m.label}</div>
            <div style={{ fontFamily: "monospace", fontSize: 11, fontWeight: 700, color: m.color || "#94a3b8" }}>{m.value}</div>
          </div>
        ))}
      </div>
      {trade.trailingPhase && trade.trailingPhase !== "INITIAL" && (
        <div style={{ fontFamily: "monospace", fontSize: 9, color: "#facc15", marginBottom: 4 }}>
          🔄 Trailing: {trade.trailingPhase} {trade.trailingLevel !== null ? `(SL en +${trade.trailingLevel}%)` : ""}
        </div>
      )}
      {!isOpen && (
        <div style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b", marginBottom: 6 }}>
          Cerrada @ MC {formatMC(priceToMC(trade.closePrice || trade.entryPrice))} — P&L: <span style={{ color: pctColor(trade.pnlPct) }}>{trade.pnlPct > 0 ? "+" : ""}{trade.pnlPct}%</span>
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
  const statusLabel = isOpen ? "🔴 REAL ABIERTA" : trade.result === "WIN" ? "✅ WIN REAL" : trade.result === "LOSS" ? "❌ LOSS REAL" : "⏱️ EXP REAL";
  return (
    <div style={{ background: "#0d1117", border: `1px solid ${color}55`, borderRadius: 10, padding: "10px 14px", boxShadow: isOpen ? `0 0 20px ${color}22` : "none" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: "#f1f5f9" }}>{trade.symbol}</span>
          <span style={{ fontSize: 10, color, fontWeight: 700, background: `${color}22`, padding: "1px 6px", borderRadius: 10, fontFamily: "monospace" }}>{statusLabel}</span>
        </div>
        <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>{formatTime(trade.openTime)}</span>
      </div>

      <div style={{ display: "flex", gap: 0, border: "1px solid #1e2d40", borderRadius: 8, overflow: "hidden", marginBottom: 6 }}>
        {[
          { label: "SOL invertido", value: `${trade.solAmount} SOL`, color: "#f97316" },
          { label: "MC Entrada", value: formatMC(priceToMC(trade.entryPrice)) },
          { label: "MC TP", value: formatMC(priceToMC(trade.tp)), color: "#22c55e" },
          { label: "MC SL", value: formatMC(priceToMC(trade.sl)), color: "#ef4444" },
        ].map((m, i) => (
          <div key={i} style={{ flex: 1, padding: "5px 4px", textAlign: "center", borderRight: i < 3 ? "1px solid #1e2d40" : "none" }}>
            <div style={{ fontSize: 9, color: "#64748b", textTransform: "uppercase", marginBottom: 2 }}>{m.label}</div>
            <div style={{ fontFamily: "monospace", fontSize: 10, fontWeight: 700, color: m.color || "#94a3b8" }}>{m.value}</div>
          </div>
        ))}
      </div>

      <div style={{ display: "flex", gap: 0, border: "1px solid #1e2d40", borderRadius: 8, overflow: "hidden", marginBottom: 6 }}>
        {[
          { label: "Actual %", value: `${(trade.currentPct || 0) > 0 ? "+" : ""}${(trade.currentPct || 0).toFixed(1)}%`, color: pctColor(trade.currentPct || 0) },
          { label: "Max ↑", value: `+${(trade.maxGainPct || 0).toFixed(1)}%`, color: "#22c55e" },
          { label: "Trailing", value: trade.trailingPhase || "INITIAL", color: trade.trailingPhase !== "INITIAL" ? "#facc15" : "#64748b" },
          { label: isOpen ? "⏱️" : "P&L SOL", value: isOpen ? elapsed(trade.openTime) : `${trade.pnlSol > 0 ? "+" : ""}${trade.pnlSol} SOL`, color: isOpen ? "#94a3b8" : pctColor(trade.pnlSol || 0) },
        ].map((m, i) => (
          <div key={i} style={{ flex: 1, padding: "5px 4px", textAlign: "center", borderRight: i < 3 ? "1px solid #1e2d40" : "none" }}>
            <div style={{ fontSize: 9, color: "#64748b", textTransform: "uppercase", marginBottom: 2 }}>{m.label}</div>
            <div style={{ fontFamily: "monospace", fontSize: 10, fontWeight: 700, color: m.color || "#94a3b8" }}>{m.value}</div>
          </div>
        ))}
      </div>

      {trade.buySignature && (
        <div style={{ fontFamily: "monospace", fontSize: 9, color: "#64748b", marginBottom: 4 }}>
          Buy TX: <a href={`https://solscan.io/tx/${trade.buySignature}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8", textDecoration: "none" }}>{trade.buySignature.slice(0, 12)}…</a>
        </div>
      )}
      {trade.sellSignature && (
        <div style={{ fontFamily: "monospace", fontSize: 9, color: "#64748b", marginBottom: 4 }}>
          Sell TX: <a href={`https://solscan.io/tx/${trade.sellSignature}`} target="_blank" rel="noreferrer" style={{ color: "#38bdf8", textDecoration: "none" }}>{trade.sellSignature.slice(0, 12)}…</a>
        </div>
      )}
      <a href={`https://dexscreener.com/solana/${trade.mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 9, color: "#38bdf8", textDecoration: "none" }}>📊 Ver en DexScreener</a>
    </div>
  );
}

export default function App() {
  const { monitored, signals, demoTrades, realTrades, log, stats, wsStatus, removeToken } = useBackend();
  const [tab, setTab] = useState("monitor");
  const [demoFilter, setDemoFilter] = useState("all");
  const [realFilter, setRealFilter] = useState("all");

  const statusColor = { connected: "#22c55e", connecting: "#facc15", disconnected: "#6b7280", error: "#ef4444" }[wsStatus] || "#6b7280";
  const statusLabel = { connected: "LIVE", connecting: "...", disconnected: "OFF", error: "ERR" }[wsStatus] || "—";
  const winRate = stats.demoWins + stats.demoLosses > 0 ? Math.round(stats.demoWins / (stats.demoWins + stats.demoLosses) * 100) : 0;
  const realWinRate = stats.realWins + stats.realLosses > 0 ? Math.round(stats.realWins / (stats.realWins + stats.realLosses) * 100) : 0;
  const filteredDemo = demoTrades.filter(t => demoFilter === "all" ? true : demoFilter === "open" ? t.status === "OPEN" : t.status === "CLOSED");
  const filteredReal = realTrades.filter(t => realFilter === "all" ? true : realFilter === "open" ? t.status === "OPEN" : t.status === "CLOSED");

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
            { label: "DEMO", val: `${stats.demoPnL > 0 ? "+" : ""}${Math.round(stats.demoPnL)}%`, color: stats.demoPnL >= 0 ? "#22c55e" : "#ef4444" },
            { label: "SOL", val: `${(stats.walletBalance || 0).toFixed(3)}`, color: "#f97316" },
            { label: "REAL", val: `${stats.realPnLSol >= 0 ? "+" : ""}${(stats.realPnLSol || 0).toFixed(3)}`, color: pctColor(stats.realPnLSol || 0) },
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
          { id: "monitor", label: "📊", badge: monitored.length },
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

        {tab === "monitor" && monitored.length === 0 && <EmptyState icon="🔍" text="Escaneando la blockchain..." />}
        {tab === "monitor" && monitored.map(t => <TokenCard key={t.mint} token={t} onRemove={removeToken} />)}

        {tab === "signals" && signals.length === 0 && <EmptyState icon="🎯" text="Esperando señales..." />}
        {tab === "signals" && signals.map(s => (
          <div key={s.id} style={{ background: "#0d1117", border: `1px solid ${s.zone === "LOWER" ? "#22c55e" : "#facc15"}22`, borderRadius: 10, padding: "10px 14px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
              <span style={{ fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: "#f1f5f9" }}>{s.symbol || shortAddr(s.mint)}</span>
              <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>{formatTime(s.time)}</span>
            </div>
            <div style={{ display: "flex", gap: 10, fontFamily: "monospace", fontSize: 12, marginBottom: 6 }}>
              <span style={{ color: s.zone === "LOWER" ? "#22c55e" : "#facc15", fontWeight: 700 }}>BANDA {s.zone}</span>
              <span style={{ color: "#94a3b8" }}>MC {formatMC(priceToMC(s.price))}</span>
            </div>
            <div style={{ display: "flex", gap: 14, fontFamily: "monospace", fontSize: 11, marginBottom: 8 }}>
              <span style={{ color: "#22c55e" }}>TP {formatMC(priceToMC(s.tp))}</span>
              <span style={{ color: "#ef4444" }}>SL {formatMC(priceToMC(s.sl))}</span>
            </div>
            <a href={`https://dexscreener.com/solana/${s.mint}`} target="_blank" rel="noreferrer" style={{ fontFamily: "monospace", fontSize: 10, color: "#38bdf8", textDecoration: "none" }}>📊 Ver en DexScreener →</a>
          </div>
        ))}

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
            {/* Balance y stats reales */}
            <div style={{ background: "#0d1117", border: "1px solid #f9741633", borderRadius: 10, padding: 12 }}>
              <div style={{ fontFamily: "monospace", fontSize: 11, color: "#f97316", marginBottom: 8, fontWeight: 700 }}>🔴 TRADING REAL</div>
              <div style={{ display: "flex", justifyContent: "space-around" }}>
                {[
                  { label: "Balance", val: `${(stats.walletBalance || 0).toFixed(4)} SOL`, color: "#f97316" },
                  { label: "Abiertas", val: stats.realOpen, color: "#38bdf8" },
                  { label: "Wins", val: stats.realWins, color: "#22c55e" },
                  { label: "Losses", val: stats.realLosses, color: "#ef4444" },
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
            {filteredReal.length === 0 && <EmptyState icon="🔴" text="Las operaciones reales aparecerán aquí automáticamente." />}
            {filteredReal.map(t => <RealTradeCard key={t.id} trade={t} />)}
          </>
        )}

        {tab === "stats" && (
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 14 }}>
              <div style={{ fontFamily: "monospace", fontSize: 12, color: "#64748b", marginBottom: 10 }}>DEMO</div>
              {[
                { label: "Cerradas", val: stats.closedCount },
                { label: "Wins", val: stats.demoWins, color: "#22c55e" },
                { label: "Losses", val: stats.demoLosses, color: "#ef4444" },
                { label: "Expiradas", val: stats.demoExpired, color: "#f97316" },
                { label: "Win Rate", val: `${winRate}%`, color: winRate >= 50 ? "#22c55e" : "#ef4444" },
                { label: "P&L Total", val: `${stats.demoPnL > 0 ? "+" : ""}${Math.round(stats.demoPnL)}%`, color: stats.demoPnL >= 0 ? "#22c55e" : "#ef4444" },
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
                { label: "Wins", val: stats.realWins, color: "#22c55e" },
                { label: "Losses", val: stats.realLosses, color: "#ef4444" },
                { label: "Expiradas", val: stats.realExpired, color: "#f97316" },
                { label: "Win Rate Real", val: `${realWinRate}%`, color: realWinRate >= 50 ? "#22c55e" : "#ef4444" },
                { label: "P&L SOL", val: `${(stats.realPnLSol || 0) >= 0 ? "+" : ""}${(stats.realPnLSol || 0).toFixed(4)} SOL`, color: pctColor(stats.realPnLSol || 0) },
                { label: "P&L %", val: `${(stats.realPnL || 0) >= 0 ? "+" : ""}${Math.round(stats.realPnL || 0)}%`, color: pctColor(stats.realPnL || 0) },
              ].map(s => (
                <div key={s.label} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e2d40" }}>
                  <span style={{ fontSize: 12, color: "#94a3b8" }}>{s.label}</span>
                  <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: s.color || "#f1f5f9" }}>{s.val}</span>
                </div>
              ))}
            </div>

            <div style={{ background: "#0d1117", border: "1px solid #1e2d40", borderRadius: 10, padding: 14 }}>
              <div style={{ fontFamily: "monospace", fontSize: 12, color: "#64748b", marginBottom: 10 }}>ANÁLISIS DE PRECIO</div>
              {[
                { label: "Ganancia máx media", val: `+${stats.avgMaxGain}%`, color: "#22c55e", desc: "Media del máximo que suben" },
                { label: "Pérdida máx media", val: `-${stats.avgMaxLoss}%`, color: "#ef4444", desc: "Media del máximo que bajan" },
                { label: "Trailing stop óptimo", val: stats.avgMaxGain > 0 ? `+${Math.max(5, Math.round(stats.avgMaxGain * 0.6))}%` : "—", color: "#facc15", desc: "60% del máximo de ganancia" },
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
            <span style={{ fontFamily: "monospace", fontSize: 10, color: { info: "#64748b", filter: "#475569", accept: "#22c55e", signal: "#facc15", monitor: "#38bdf8", warn: "#f97316", error: "#ef4444", demo: "#a78bfa", win: "#22c55e", loss: "#ef4444", expire: "#f97316", trail: "#facc15", real: "#f97316", realwin: "#22c55e", realloss: "#ef4444" }[entry.type] || "#64748b" }}>{entry.msg}</span>
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
