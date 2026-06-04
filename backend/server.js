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
const SIGNAL_COOLDOWN_MS = 5 * 60 * 1000;
const MIN_MONITOR_MS = 2 * 60 * 1000;
const TP_PCT = 1.5;
const SL_PCT = 0.8;
const MAX_TRADE_DURATION_MS = 15 * 60 * 1000;
const TOTAL_SUPPLY = 1_000_000_000;
const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data";

const state = {
  monitored: new Map(),
  signals: [],
  demoTrades: [],
  log: [],
  stats: {
    seen: 0, filtered: 0, signals: 0,
    demoOpen: 0, demoWins: 0, demoLosses: 0, demoExpired: 0,
    demoPnL: 0, avgMaxGain: 0, avgMaxLoss: 0,
    maxGainSum: 0, maxLossSum: 0, closedCount: 0,
  },
};

const frontendClients = new Set();
const seenMints = new Set();
const signalCooldown = new Map();

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

function tokenToJSON(t) {
  return {
    mint: t.mint, name: t.name, symbol: t.symbol,
    twitter: t.twitter, website: t.website, telegram: t.telegram,
    mc: t.mc, price: t.price, bb: t.bb,
    candleCount: t.candleCount, candles: t.candles50,
    signal: t.signal, signalPrice: t.signalPrice,
    tp: t.tp, sl: t.sl, detectedAt: t.detectedAt, lastUpdate: t.lastUpdate,
    tradeCount: t.tradeCount, volumeUSD: t.volumeUSD,
    priceHigh: t.priceHigh, priceLow: t.priceLow,
  };
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

let solPriceUSD = 150;
async function updateSolPrice() {
  try {
    const res = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd");
    const data = await res.json();
    solPriceUSD = data?.solana?.usd ?? 150;
  } catch {}
}
setInterval(updateSolPrice, 60_000);
updateSolPrice();

// Precio correcto desde bonding curve de PumpPortal
function calcPriceFromMsg(msg) {
  try {
    const vSol = msg.vSolInBondingCurve;
    const vTokens = msg.vTokensInBondingCurve;
    if (vSol && vTokens && vTokens > 0) {
      return (vSol / vTokens) * solPriceUSD;
    }
  } catch {}
  return null;
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

// ── Demo Trading ──
function openDemoTrade(signal) {
  const trade = {
    id: `demo-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
    mint: signal.mint, symbol: signal.symbol, name: signal.name,
    zone: signal.zone, entryPrice: signal.price,
    tp: signal.tp, sl: signal.sl,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null,
    maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    status: "OPEN", expiresAt: Date.now() + MAX_TRADE_DURATION_MS,
  };
  state.demoTrades.unshift(trade);
  if (state.demoTrades.length > 500) state.demoTrades.pop();
  state.stats.demoOpen++;
  broadcast({ event: "newDemoTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  addLog(`📝 DEMO: ${signal.symbol} @ MC $${Math.round(signal.price * TOTAL_SUPPLY / 1000)}K | TP +50% | SL -20%`, "demo");
  return trade;
}

function closeDemoTrade(trade, price, reason) {
  trade.closePrice = price;
  trade.closeTime = Date.now();
  trade.status = "CLOSED";
  trade.pnlPct = +((price - trade.entryPrice) / trade.entryPrice * 100).toFixed(2);
  if (reason === "TP") {
    trade.result = "WIN";
    state.stats.demoWins++;
    state.stats.demoPnL += 50;
    addLog(`✅ WIN: ${trade.symbol} +50% en ${Math.round((trade.closeTime - trade.openTime) / 1000)}s | MaxGain: +${trade.maxGainPct.toFixed(1)}%`, "win");
  } else if (reason === "SL") {
    trade.result = "LOSS";
    state.stats.demoLosses++;
    state.stats.demoPnL -= 20;
    addLog(`❌ LOSS: ${trade.symbol} -20% en ${Math.round((trade.closeTime - trade.openTime) / 1000)}s | MaxGain fue: +${trade.maxGainPct.toFixed(1)}%`, "loss");
  } else {
    trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS";
    state.stats.demoExpired++;
    state.stats.demoPnL += trade.pnlPct;
    addLog(`⏱️ EXP: ${trade.symbol} ${trade.pnlPct > 0 ? "+" : ""}${trade.pnlPct}% | MaxGain: +${trade.maxGainPct.toFixed(1)}%`, "expire");
  }
  state.stats.demoOpen = Math.max(0, state.stats.demoOpen - 1);
  state.stats.maxGainSum += trade.maxGainPct;
  state.stats.maxLossSum += Math.abs(trade.maxLossPct);
  state.stats.closedCount++;
  state.stats.avgMaxGain = +(state.stats.maxGainSum / state.stats.closedCount).toFixed(1);
  state.stats.avgMaxLoss = +(state.stats.maxLossSum / state.stats.closedCount).toFixed(1);
  broadcast({ event: "demoTradeClosed", data: trade });
  broadcast({ event: "stats", data: state.stats });
}

function updateDemoTrades(mint, price) {
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN") continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    if (price >= trade.tp) closeDemoTrade(trade, price, "TP");
    else if (price <= trade.sl) closeDemoTrade(trade, price, "SL");
    else if (now >= trade.expiresAt) closeDemoTrade(trade, price, "EXPIRED");
    else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, maxLossPct: trade.maxLossPct } });
  }
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.status !== "OPEN") continue;
    if (now >= trade.expiresAt) {
      const token = state.monitored.get(trade.mint);
      const price = token?.price || trade.entryPrice;
      closeDemoTrade(trade, price, "EXPIRED");
    }
  }
}, 30_000);

function updateCandle(mint, price, volumeUSD = 0) {
  const token = state.monitored.get(mint);
  if (!token) return;
  const now = Date.now();
  const currentSecond = Math.floor(now / CANDLE_MS) * CANDLE_MS;

  if (!token.currentCandle || token.currentCandle.time !== currentSecond) {
    if (token.currentCandle) {
      token.candles.push({ ...token.currentCandle });
      if (token.candles.length > 300) token.candles.shift();
    }
    const prevClose = token.currentCandle?.close ?? price;
    token.currentCandle = { time: currentSecond, open: prevClose, high: price, low: price, close: price };
  } else {
    token.currentCandle.high = Math.max(token.currentCandle.high, price);
    token.currentCandle.low = Math.min(token.currentCandle.low, price);
    token.currentCandle.close = price;
  }

  token.price = price;
  token.mc = price * TOTAL_SUPPLY;
  token.priceHigh = Math.max(token.priceHigh || 0, price);
  token.priceLow = token.priceLow === 0 ? price : Math.min(token.priceLow, price);
  token.tradeCount = (token.tradeCount || 0) + 1;
  token.volumeUSD = (token.volumeUSD || 0) + volumeUSD;
  token.lastUpdate = now;

  if (token.mc < MIN_MC_USD * 0.5) {
    addLog(`🗑️ ${token.symbol} eliminado — MC $${Math.round(token.mc)}`, "filter");
    stopMonitoring(mint);
    return;
  }

  const allCandles = [...token.candles, token.currentCandle];
  const bb = calcBollinger(allCandles);
  token.bb = bb;
  token.candleCount = allCandles.length;
  token.candles50 = allCandles.slice(-50);

  updateDemoTrades(mint, price);
  if (bb) checkSignal(mint, price, bb, allCandles.length);
  broadcast({ event: "tokenUpdate", data: tokenToJSON(token) });
}

function checkSignal(mint, price, bb, candleCount) {
  if (candleCount < BOLLINGER_PERIOD) return;
  const token = state.monitored.get(mint);
  if (!token) return;
  const lastSignal = signalCooldown.get(mint) || 0;
  if (Date.now() - lastSignal < SIGNAL_COOLDOWN_MS) return;
  if ((token.tradeCount || 0) < 3) return;
  if ((token.volumeUSD || 0) < 5) return;
  if (token.priceHigh > 0) {
    const dropFromHigh = (token.priceHigh - price) / token.priceHigh;
    if (dropFromHigh > 0.35) return;
  }

  const touchedLower = price <= bb.lower * 1.02;
  const touchedMiddle = !touchedLower && Math.abs(price - bb.middle) / bb.middle < 0.015;

  if (touchedLower || touchedMiddle) {
    const zone = touchedLower ? "LOWER" : "MIDDLE";
    const tp = +(price * TP_PCT).toFixed(10);
    const sl = +(price * SL_PCT).toFixed(10);
    token.signal = zone;
    token.signalPrice = price;
    token.tp = tp;
    token.sl = sl;
    signalCooldown.set(mint, Date.now());
    state.stats.signals++;

    const signal = {
      id: `${mint}-${Date.now()}`,
      mint, name: token.name, symbol: token.symbol,
      zone, price, tp, sl, mc: token.mc,
      time: Date.now(), status: "OPEN",
      tradeCount: token.tradeCount, volumeUSD: token.volumeUSD,
    };

    state.signals.unshift(signal);
    if (state.signals.length > 100) state.signals.pop();
    addLog(`🎯 SEÑAL ${zone} en ${token.symbol} @ MC $${Math.round(token.mc / 1000)}K`, "signal");
    broadcast({ event: "newSignal", data: signal });
    broadcast({ event: "stats", data: state.stats });
    openDemoTrade(signal);
  }
}

function startMonitoring(token, wsPortal) {
  if (state.monitored.has(token.mint)) return;
  const entry = {
    ...token, candles: [], currentCandle: null, bb: null,
    candleCount: 0, signal: null, lastUpdate: Date.now(), candles50: [],
    priceHigh: token.price || 0, priceLow: token.price || 0,
    tradeCount: 0, volumeUSD: 0,
  };
  state.monitored.set(token.mint, entry);

  if (wsPortal?.readyState === WebSocket.OPEN) {
    wsPortal.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [token.mint] }));
  }

  addLog(`📊 Monitorizando ${token.symbol} — MC $${Math.round(token.mc / 1000)}K`, "monitor");
  broadcast({ event: "newToken", data: tokenToJSON(entry) });
  broadcast({ event: "stats", data: state.stats });
}

function stopMonitoring(mint, wsPortal) {
  state.monitored.delete(mint);
  if (wsPortal?.readyState === WebSocket.OPEN) {
    wsPortal.send(JSON.stringify({ method: "unsubscribeTokenTrade", keys: [mint] }));
  }
  broadcast({ event: "removeToken", data: { mint } });
}

async function processNewToken(raw, wsPortal) {
  if (seenMints.has(raw.mint)) return;
  seenMints.add(raw.mint);
  if (seenMints.size > 5000) seenMints.clear();

  state.stats.seen++;
  broadcast({ event: "stats", data: state.stats });

  const mcEstimate = raw.usdMarketCap || (raw.marketCapSol || 0) * solPriceUSD;
  if (mcEstimate > 0 && mcEstimate < MIN_MC_USD) {
    addLog(`⛔ MC bajo (~$${Math.round(mcEstimate)}): ${raw.name}`, "filter");
    return;
  }

  let twitter = raw.twitter || null;
  let website = raw.website || null;
  let telegram = raw.telegram || null;

  if (!twitter && !website && raw.uri) {
    const meta = await fetchTokenMetadata(raw.uri);
    if (meta) { twitter = meta.twitter; website = meta.website; telegram = meta.telegram; }
  }

  if (!twitter && !website && !telegram) {
    addLog(`⛔ Sin sociales: ${raw.name || shortAddr(raw.mint)}`, "filter");
    return;
  }

  state.stats.filtered++;
  const initialPrice = calcPriceFromMsg(raw) ?? 0;
  const mc = initialPrice * TOTAL_SUPPLY;

  addLog(`✅ ${raw.symbol} — MC $${Math.round(mc / 1000)}K — ${twitter ? "𝕏" : ""}${website ? "🌐" : ""}${telegram ? "✈️" : ""}`, "accept");

  if (state.monitored.size >= MAX_MONITORED) {
    let oldest = null;
    for (const [m, t] of state.monitored.entries()) {
      const age = Date.now() - t.detectedAt;
      if (!t.signal && age >= MIN_MONITOR_MS && (!oldest || t.detectedAt < oldest.detectedAt)) oldest = t;
    }
    if (oldest) stopMonitoring(oldest.mint, wsPortal);
    else { addLog(`⚠️ Cola llena, descartando ${raw.symbol}`, "warn"); return; }
  }

  startMonitoring({
    mint: raw.mint, name: raw.name || "Unknown", symbol: raw.symbol || "???",
    twitter, website, telegram, mc, price: initialPrice, detectedAt: Date.now(),
  }, wsPortal);
}

function connectPumpPortal() {
  addLog("🔌 Conectando a PumpPortal...", "info");
  broadcast({ event: "wsStatus", data: "connecting" });

  const ws = new WebSocket(PUMPPORTAL_WS);

  ws.on("open", () => {
    addLog("✅ PumpPortal conectado", "info");
    broadcast({ event: "wsStatus", data: "connected" });
    ws.send(JSON.stringify({ method: "subscribeNewToken" }));
  });

  ws.on("message", async (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.txType === "create" || (msg.mint && !msg.txType)) {
        processNewToken(msg, ws);
        return;
      }

      if ((msg.txType === "buy" || msg.txType === "sell") && state.monitored.has(msg.mint)) {
        const price = calcPriceFromMsg(msg);
        const volumeUSD = (msg.solAmount || 0) * solPriceUSD;
        if (price && price > 0) {
          // Solo compras generan velas — ventas solo actualizan precio
          if (msg.txType === "buy") {
            updateCandle(msg.mint, price, volumeUSD);
          } else {
            const token = state.monitored.get(msg.mint);
            if (token) {
              token.price = price;
              token.mc = price * TOTAL_SUPPLY;
              updateDemoTrades(msg.mint, price);
              broadcast({ event: "tokenUpdate", data: tokenToJSON(token) });
            }
          }
        }
      }
    } catch {}
  });

  ws.on("error", (err) => {
    addLog(`❌ Error: ${err.message}`, "error");
    broadcast({ event: "wsStatus", data: "error" });
  });

  ws.on("close", () => {
    addLog("🔄 Reconectando en 5s...", "warn");
    broadcast({ event: "wsStatus", data: "disconnected" });
    state.monitored.clear();
    setTimeout(connectPumpPortal, 5000);
  });
}

const app = express();
app.use(cors());
app.use(express.json());
app.get("/api/state", (req, res) => {
  res.json({
    monitored: Array.from(state.monitored.values()).map(tokenToJSON),
    signals: state.signals.slice(0, 50),
    demoTrades: state.demoTrades.slice(0, 200),
    log: state.log.slice(0, 100),
    stats: state.stats
  });
});
app.delete("/api/token/:mint", (req, res) => { stopMonitoring(req.params.mint); res.json({ ok: true }); });

const server = createServer(app);
const wss = new WebSocketServer({ server });
wss.on("connection", (ws) => {
  frontendClients.add(ws);
  ws.send(JSON.stringify({ event: "fullState", data: { monitored: Array.from(state.monitored.values()).map(tokenToJSON), signals: state.signals.slice(0, 50), demoTrades: state.demoTrades.slice(0, 200), log: state.log.slice(0, 100), stats: state.stats, wsStatus: "connected" } }));
  ws.on("close", () => frontendClients.delete(ws));
  ws.on("message", (data) => { try { const msg = JSON.parse(data.toString()); if (msg.action === "removeToken") stopMonitoring(msg.mint); } catch {}; });
});

server.listen(PORT, () => {
  console.log(`🚀 SolScanBot — PumpPortal + bonding curve correcto`);
  connectPumpPortal();
});
