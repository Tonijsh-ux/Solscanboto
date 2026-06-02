import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import cors from "cors";
import fetch from "node-fetch";

const PORT = process.env.PORT || 3001;
const MAX_MONITORED = 20;
const BOLLINGER_PERIOD = 20;
const BOLLINGER_MULT = 2;
const MIN_MC_USD = 3000;
const CANDLE_MS = 1000;
const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data";
const BIRDEYE_BASE = "https://public-api.birdeye.so/defi";

const state = {
  monitored: new Map(),
  signals: [],
  log: [],
  stats: { seen: 0, filtered: 0, signals: 0, uptime: Date.now() },
};

const frontendClients = new Set();

function addLog(msg, type = "info") {
  const entry = { msg, type, time: Date.now() };
  state.log.unshift(entry);
  if (state.log.length > 200) state.log.pop();
  broadcast({ event: "log", data: entry });
}

function broadcast(payload) {
  const str = JSON.stringify(payload);
  for (const client of frontendClients) {
    if (client.readyState === WebSocket.OPEN) client.send(str);
  }
}

function shortAddr(addr) {
  return addr ? `${addr.slice(0, 4)}…${addr.slice(-4)}` : "—";
}

function calcBollinger(candles) {
  if (candles.length < BOLLINGER_PERIOD) return null;
  const slice = candles.slice(-BOLLINGER_PERIOD);
  const closes = slice.map((c) => c.close);
  const mean = closes.reduce((a, b) => a + b, 0) / closes.length;
  const variance = closes.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / closes.length;
  const std = Math.sqrt(variance);
  return {
    upper: +(mean + BOLLINGER_MULT * std).toFixed(10),
    middle: +mean.toFixed(10),
    lower: +(mean - BOLLINGER_MULT * std).toFixed(10),
  };
}

async function fetchPrice(mint) {
  try {
    const res = await fetch(`${BIRDEYE_BASE}/price?address=${mint}`, {
      headers: { "x-chain": "solana" },
      signal: AbortSignal.timeout(3000),
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data?.data?.value ?? null;
  } catch { return null; }
}

async function fetchTokenInfo(mint) {
  try {
    const res = await fetch(`${BIRDEYE_BASE}/token_overview?address=${mint}`, {
      headers: { "x-chain": "solana" },
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data?.data ?? null;
  } catch { return null; }
}

function updateCandle(mint, price) {
  const token = state.monitored.get(mint);
  if (!token) return;
  const now = Date.now();
  const currentSecond = Math.floor(now / CANDLE_MS) * CANDLE_MS;
  if (!token.currentCandle || token.currentCandle.time !== currentSecond) {
    if (token.currentCandle) {
      token.candles.push({ ...token.currentCandle });
      if (token.candles.length > 300) token.candles.shift();
    }
    token.currentCandle = { time: currentSecond, open: price, high: price, low: price, close: price };
  } else {
    token.currentCandle.high = Math.max(token.currentCandle.high, price);
    token.currentCandle.low = Math.min(token.currentCandle.low, price);
    token.currentCandle.close = price;
  }
  const allCandles = [...token.candles, token.currentCandle];
  const bb = calcBollinger(allCandles);
  token.price = price;
  token.bb = bb;
  token.candleCount = allCandles.length;
  token.lastUpdate = now;
  token.candles50 = allCandles.slice(-50);
  if (bb) checkSignal(mint, price, bb, allCandles.length);
  broadcast({ event: "tokenUpdate", data: { mint, price, bb, candleCount: allCandles.length, candles: token.candles50, signal: token.signal, signalPrice: token.signalPrice, tp: token.tp, sl: token.sl, lastUpdate: now } });
}

function checkSignal(mint, price, bb, candleCount) {
  if (candleCount < BOLLINGER_PERIOD) return;
  const token = state.monitored.get(mint);
  if (!token) return;
  if (token.lastSignalTime && Date.now() - token.lastSignalTime < 60_000) return;
  const touchedLower = price <= bb.lower * 1.02;
  const touchedMiddle = !touchedLower && Math.abs(price - bb.middle) / bb.middle < 0.015;
  if (touchedLower || touchedMiddle) {
    const zone = touchedLower ? "LOWER" : "MIDDLE";
    const tp = +(price * 1.5).toFixed(10);
    const sl = +(price * 0.8).toFixed(10);
    token.signal = zone;
    token.signalPrice = price;
    token.tp = tp;
    token.sl = sl;
    token.lastSignalTime = Date.now();
    state.stats.signals++;
    const signal = { id: Date.now(), mint, name: token.name, symbol: token.symbol, zone, price, tp, sl, time: Date.now(), status: "OPEN" };
    state.signals.unshift(signal);
    if (state.signals.length > 100) state.signals.pop();
    addLog(`🎯 SEÑAL ${zone} en ${token.symbol} @ ${price.toExponential(3)}`, "signal");
    broadcast({ event: "newSignal", data: signal });
    broadcast({ event: "stats", data: state.stats });
  }
}

function startMonitoring(token) {
  if (state.monitored.has(token.mint)) return;
  const entry = { ...token, candles: [], currentCandle: null, bb: null, candleCount: 0, signal: null, lastUpdate: Date.now(), candles50: [] };
  state.monitored.set(token.mint, entry);
  const delay = Math.random() * 800;
  setTimeout(() => {
    const interval = setInterval(async () => {
      if (!state.monitored.has(token.mint)) { clearInterval(interval); return; }
      const price = await fetchPrice(token.mint);
      if (price && price > 0) updateCandle(token.mint, price);
    }, CANDLE_MS);
    entry.interval = interval;
  }, delay);
  addLog(`📊 Monitorizando ${token.symbol || shortAddr(token.mint)}`, "monitor");
  broadcast({ event: "newToken", data: entry });
  broadcast({ event: "stats", data: state.stats });
}

function stopMonitoring(mint) {
  const token = state.monitored.get(mint);
  if (token?.interval) clearInterval(token.interval);
  state.monitored.delete(mint);
  addLog(`⏹ Detenido ${shortAddr(mint)}`, "info");
  broadcast({ event: "removeToken", data: { mint } });
}

function hasRequiredSocials(t) {
  return !!(t.twitter || t.website || t.telegram);
}

async function processNewToken(raw) {
  state.stats.seen++;
  broadcast({ event: "stats", data: state.stats });
  if (!hasRequiredSocials(raw)) { addLog(`⛔ Sin sociales: ${raw.name || shortAddr(raw.mint)}`, "filter"); return; }
  const mcEstimate = raw.usdMarketCap || (raw.marketCapSol || 0) * 150;
  if (mcEstimate > 0 && mcEstimate < MIN_MC_USD) { addLog(`⛔ MC bajo (~$${Math.round(mcEstimate)}): ${raw.name}`, "filter"); return; }
  const info = await fetchTokenInfo(raw.mint);
  const mc = info?.mc ?? mcEstimate ?? 0;
  if (mc < MIN_MC_USD) { addLog(`⛔ MC real bajo ($${Math.round(mc)}): ${raw.name}`, "filter"); return; }
  const wallets = info?.uniqueWallet24h ?? 0;
  const trades = info?.trade24h ?? 0;
  if (info && wallets < 2 && trades < 5) { addLog(`⛔ Sin traders pro (${wallets} wallets): ${raw.name}`, "filter"); return; }
  state.stats.filtered++;
  const candidate = { mint: raw.mint, name: raw.name || "Unknown", symbol: raw.symbol || "???", twitter: raw.twitter || null, website: raw.website || null, telegram: raw.telegram || null, mc, traders: wallets, price: info?.price ?? 0, detectedAt: Date.now() };
  addLog(`✅ ${candidate.symbol} — MC $${Math.round(mc)} — ${wallets} wallets`, "accept");
  if (state.monitored.size >= MAX_MONITORED) {
    let oldest = null;
    for (const [mint, t] of state.monitored.entries()) {
      if (!t.signal && (!oldest || t.detectedAt < oldest.detectedAt)) oldest = t;
    }
    if (oldest) stopMonitoring(oldest.mint);
    else { addLog(`⚠️ Cola llena, descartando ${candidate.symbol}`, "warn"); return; }
  }
  startMonitoring(candidate);
}

function connectPumpPortal() {
  addLog("🔌 Conectando a PumpPortal...", "info");
  broadcast({ event: "wsStatus", data: "connecting" });
  const ws = new WebSocket(PUMPPORTAL_WS);
  ws.on("open", () => { addLog("✅ PumpPortal conectado", "info"); broadcast({ event: "wsStatus", data: "connected" }); ws.send(JSON.stringify({ method: "subscribeNewToken" })); });
  ws.on("message", (data) => { try { const msg = JSON.parse(data.toString()); if (msg.mint) processNewToken(msg); } catch {} });
  ws.on("error", (err) => { addLog(`❌ Error: ${err.message}`, "error"); broadcast({ event: "wsStatus", data: "error" }); });
  ws.on("close", () => { addLog("🔄 Reconectando en 5s...", "warn"); broadcast({ event: "wsStatus", data: "disconnected" }); setTimeout(connectPumpPortal, 5000); });
}

const app = express();
app.use(cors());
app.use(express.json());
app.get("/api/state", (req, res) => { res.json({ monitored: Array.from(state.monitored.values()), signals: state.signals.slice(0, 50), log: state.log.slice(0, 100), stats: state.stats }); });
app.delete("/api/token/:mint", (req, res) => { stopMonitoring(req.params.mint); res.json({ ok: true }); });

const server = createServer(app);
const wss = new WebSocketServer({ server });
wss.on("connection", (ws) => {
  frontendClients.add(ws);
  ws.send(JSON.stringify({ event: "fullState", data: { monitored: Array.from(state.monitored.values()), signals: state.signals.slice(0, 50), log: state.log.slice(0, 100), stats: state.stats, wsStatus: "connected" } }));
  ws.on("close", () => frontendClients.delete(ws));
  ws.on("message", (data) => { try { const msg = JSON.parse(data.toString()); if (msg.action === "removeToken") stopMonitoring(msg.mint); } catch {} });
});

server.listen(PORT, () => { console.log(`🚀 SolScanBot corriendo en puerto ${PORT}`); connectPumpPortal(); });
