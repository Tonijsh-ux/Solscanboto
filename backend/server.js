import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import cors from "cors";
import fetch from "node-fetch";

const PORT = process.env.PORT || 3001;
const MAX_MONITORED = 20;
const BOLLINGER_PERIOD = 20;
const BOLLINGER_MULT = 2;
const MIN_MC_USD = 2000;
const CANDLE_MS = 1000;
const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data";

const state = {
 monitored: new Map(),
 signals: [],
 log: [],
 stats: { seen: 0, filtered: 0, signals: 0, uptime: Date.now() },
};

const frontendClients = new Set();
const seenMints = new Set();

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

async function fetchTokenMetadata(uri) {
 if (!uri) return null;
 try {
   const res = await fetch(uri, { signal: AbortSignal.timeout(3000) });
   if (!res.ok) return null;
   const data = await res.json();
   return {
     twitter: data.twitter || data.extensions?.twitter || null,
     website: data.website || data.extensions?.website || null,
     telegram: data.telegram || data.extensions?.telegram || null,
   };
 } catch { return null; }
}

async function fetchPrice(mint) {
 try {
   const res = await fetch(
     `https://price.jup.ag/v6/price?ids=${mint}`,
     { signal: AbortSignal.timeout(3000) }
   );
   if (res.ok) {
     const data = await res.json();
     const price = data?.data?.[mint]?.price;
     if (price && price > 0) return price;
   }
 } catch {}

 try {
   const res = await fetch(
     `https://api.dexscreener.com/latest/dex/tokens/${mint}`,
     { signal: AbortSignal.timeout(4000) }
   );
   if (!res.ok) return null;
   const data = await res.json();
   const pair = data?.pairs?.[0];
   return pair?.priceUsd ? parseFloat(pair.priceUsd) : null;
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

async function processNewToken(raw) {
 if (seenMints.has(raw.mint)) return;
 seenMints.add(raw.mint);
 if (seenMints.size > 5000) seenMints.clear();

 state.stats.seen++;
 broadcast({ event: "stats", data: state.stats });

 const mcEstimate = raw.usdMarketCap || (raw.marketCapSol || 0) * 150;
 if (mcEstimate > 0 && mcEstimate < MIN_MC_USD) {
   addLog(`⛔ MC bajo (~$${Math.round(mcEstimate)}): ${raw.name}`, "filter");
   return;
 }

 let twitter = raw.twitter || null;
 let website = raw.website || null;
 let telegram = raw.telegram || null;

 if (!twitter && !website && raw.uri) {
   const meta = await fetchTokenMetadata(raw.uri);
   if (meta) {
     twitter = meta.twitter;
     website = meta.website;
     telegram = meta.telegram;
   }
 }

 if (!twitter && !website && !telegram) {
   addLog(`⛔ Sin sociales: ${raw.name || shortAddr(raw.mint)}`, "filter");
   return;
 }

 state.stats.filtered++;

 const candidate = {
   mint: raw.mint,
   name: raw.name || "Unknown",
   symbol: raw.symbol || "???",
   twitter,
   website,
   telegram,
   mc: mcEstimate,
   price: 0,
   detectedAt: Date.now(),
 };

 addLog(`✅ ${candidate.symbol} — MC ~$${Math.round(mcEstimate)} — ${twitter ? "𝕏" : ""}${website ? "🌐" : ""}${telegram ? "✈️" : ""}`, "accept");

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
