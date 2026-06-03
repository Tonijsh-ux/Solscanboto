import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import cors from "cors";
import fetch from "node-fetch";

const PORT = process.env.PORT || 3001;
const MAX_MONITORED = 20;
const CANDLE_MS = 5000; // velas de 5 segundos
const MIN_MC_USD = 2000;
const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data";

// ── Parámetros estrategia ──
const MOMENTUM_VOLUME_MIN = 300;    // volumen mínimo en 15s para señal momentum
const MOMENTUM_PRICE_UP = 0.15;     // precio debe subir >15% para momentum
const MOMENTUM_TRADES_MIN = 5;      // mínimo 5 trades distintos
const REBOTE_DROP = 0.10;           // caída >10% desde máximo para señal rebote
const REBOTE_VOLUME_MIN = 500;      // volumen mínimo previo para considerar rebote válido
const SIGNAL_COOLDOWN_MS = 5 * 60 * 1000; // 5 min entre señales del mismo token

const state = {
  monitored: new Map(),
  signals: [],
  log: [],
  stats: { seen: 0, filtered: 0, signals: 0, uptime: Date.now() },
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
    mc: t.mc, price: t.price, bb: null,
    candleCount: t.candleCount, candles: t.candles50,
    signal: t.signal, signalType: t.signalType,
    signalPrice: t.signalPrice, tp: t.tp, sl: t.sl,
    detectedAt: t.detectedAt, lastUpdate: t.lastUpdate,
    volumeTotal: t.volumeTotal, tradeCount: t.tradeCount,
    priceHigh: t.priceHigh, priceLow: t.priceLow,
  };
}

function shortAddr(addr) {
  return addr ? `${addr.slice(0, 4)}…${addr.slice(-4)}` : "—";
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

function calcPriceFromTrade(msg) {
  try {
    const vSol = msg.vSolInBondingCurve;
    const vTokens = msg.vTokensInBondingCurve;
    if (vSol && vTokens && vTokens > 0) return (vSol / vTokens) * 150;
  } catch {}
  return null;
}

function calcVolumeUSD(msg) {
  try {
    const sol = msg.solAmount || 0;
    return sol * 150;
  } catch {}
  return 0;
}

function checkSignals(mint) {
  const token = state.monitored.get(mint);
  if (!token || token.price <= 0) return;

  const lastSignal = signalCooldown.get(mint) || 0;
  if (Date.now() - lastSignal < SIGNAL_COOLDOWN_MS) return;

  const now = Date.now();
  const age = (now - token.detectedAt) / 1000; // segundos de vida

  // ── SEÑAL MOMENTUM ──
  // Volumen alto + precio subiendo fuerte en primeros 30s
  if (age <= 30) {
    const priceChange = token.priceStart > 0 ? (token.price - token.priceStart) / token.priceStart : 0;
    if (
      token.volumeTotal >= MOMENTUM_VOLUME_MIN &&
      token.tradeCount >= MOMENTUM_TRADES_MIN &&
      priceChange >= MOMENTUM_PRICE_UP
    ) {
      emitSignal(mint, "MOMENTUM", token.price, token.price * 1.5, token.price * 0.85);
      return;
    }
  }

  // ── SEÑAL REBOTE ──
  // Cayó >10% desde máximo y tiene volumen previo suficiente
  if (token.priceHigh > 0 && token.volumeTotal >= REBOTE_VOLUME_MIN) {
    const dropFromHigh = (token.priceHigh - token.price) / token.priceHigh;
    if (dropFromHigh >= REBOTE_DROP && token.price > token.priceStart * 0.8) {
      emitSignal(mint, "REBOTE", token.price, token.price * 1.3, token.price * 0.85);
      return;
    }
  }
}

function emitSignal(mint, type, price, tp, sl) {
  const token = state.monitored.get(mint);
  if (!token) return;

  token.signal = type;
  token.signalType = type;
  token.signalPrice = price;
  token.tp = +tp.toFixed(10);
  token.sl = +sl.toFixed(10);
  signalCooldown.set(mint, Date.now());
  state.stats.signals++;

  const signal = {
    id: `${mint}-${Date.now()}`,
    mint, name: token.name, symbol: token.symbol,
    zone: type, price, tp: token.tp, sl: token.sl,
    time: Date.now(), status: "OPEN",
    volumeTotal: token.volumeTotal,
    tradeCount: token.tradeCount,
  };

  state.signals.unshift(signal);
  if (state.signals.length > 100) state.signals.pop();
  addLog(`🎯 ${type} en ${token.symbol} | Vol $${Math.round(token.volumeTotal)} | ${token.tradeCount} trades`, "signal");
  broadcast({ event: "newSignal", data: signal });
  broadcast({ event: "stats", data: state.stats });
}

function processTrade(mint, price, volumeUSD, wsPortal) {
  const token = state.monitored.get(mint);
  if (!token) return;

  const now = Date.now();
  const currentCandle = Math.floor(now / CANDLE_MS) * CANDLE_MS;

  // Actualizar precio y estadísticas
  if (token.priceStart === 0) token.priceStart = price;
  token.price = price;
  token.lastUpdate = now;
  token.volumeTotal = (token.volumeTotal || 0) + volumeUSD;
  token.tradeCount = (token.tradeCount || 0) + 1;
  token.priceHigh = Math.max(token.priceHigh || 0, price);
  token.priceLow = token.priceLow === 0 ? price : Math.min(token.priceLow, price);

  // Velas de 5s
  if (!token.currentCandle || token.currentCandle.time !== currentCandle) {
    if (token.currentCandle) {
      token.candles.push({ ...token.currentCandle });
      if (token.candles.length > 200) token.candles.shift();
    }
    const prevClose = token.currentCandle?.close ?? price;
    token.currentCandle = { time: currentCandle, open: prevClose, high: price, low: price, close: price, volume: volumeUSD };
  } else {
    token.currentCandle.high = Math.max(token.currentCandle.high, price);
    token.currentCandle.low = Math.min(token.currentCandle.low, price);
    token.currentCandle.close = price;
    token.currentCandle.volume = (token.currentCandle.volume || 0) + volumeUSD;
  }

  token.candleCount = token.candles.length + 1;
  token.candles50 = [...token.candles, token.currentCandle].slice(-50);

  broadcast({ event: "tokenUpdate", data: tokenToJSON(token) });
  checkSignals(mint);
}

function startMonitoring(token, wsPortal) {
  if (state.monitored.has(token.mint)) return;
  const entry = {
    ...token,
    candles: [], currentCandle: null,
    candleCount: 0, signal: null, signalType: null,
    lastUpdate: Date.now(), candles50: [],
    volumeTotal: 0, tradeCount: 0,
    priceHigh: 0, priceLow: 0, priceStart: 0,
    ticker: null,
  };
  state.monitored.set(token.mint, entry);

  if (wsPortal && wsPortal.readyState === WebSocket.OPEN) {
    wsPortal.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [token.mint] }));
  }

  // Ticker para cerrar velas aunque no haya trades
  entry.ticker = setInterval(() => {
    const t = state.monitored.get(token.mint);
    if (!t) { clearInterval(entry.ticker); return; }
    if (t.price > 0 && t.currentCandle) {
      const now = Date.now();
      const currentCandle = Math.floor(now / CANDLE_MS) * CANDLE_MS;
      if (t.currentCandle.time !== currentCandle) {
        t.candles.push({ ...t.currentCandle });
        if (t.candles.length > 200) t.candles.shift();
        t.currentCandle = { time: currentCandle, open: t.price, high: t.price, low: t.price, close: t.price, volume: 0 };
        t.candleCount = t.candles.length + 1;
        t.candles50 = [...t.candles, t.currentCandle].slice(-50);
        broadcast({ event: "tokenUpdate", data: tokenToJSON(t) });
      }
    }
  }, CANDLE_MS);

  addLog(`📊 Monitorizando ${token.symbol || shortAddr(token.mint)}`, "monitor");
  broadcast({ event: "newToken", data: tokenToJSON(entry) });
  broadcast({ event: "stats", data: state.stats });
}

function stopMonitoring(mint, wsPortal) {
  const token = state.monitored.get(mint);
  if (token?.ticker) clearInterval(token.ticker);
  state.monitored.delete(mint);
  if (wsPortal && wsPortal.readyState === WebSocket.OPEN) {
    wsPortal.send(JSON.stringify({ method: "unsubscribeTokenTrade", keys: [mint] }));
  }
  addLog(`⏹ Detenido ${shortAddr(mint)}`, "info");
  broadcast({ event: "removeToken", data: { mint } });
}

async function processNewToken(raw, wsPortal) {
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
    if (meta) { twitter = meta.twitter; website = meta.website; telegram = meta.telegram; }
  }

  if (!twitter && !website && !telegram) {
    addLog(`⛔ Sin sociales: ${raw.name || shortAddr(raw.mint)}`, "filter");
    return;
  }

  state.stats.filtered++;
  const initialPrice = calcPriceFromTrade(raw) ?? 0;

  const candidate = {
    mint: raw.mint, name: raw.name || "Unknown", symbol: raw.symbol || "???",
    twitter, website, telegram, mc: mcEstimate,
    price: initialPrice, detectedAt: Date.now(),
  };

  addLog(`✅ ${candidate.symbol} — MC ~$${Math.round(mcEstimate)} — ${twitter ? "𝕏" : ""}${website ? "🌐" : ""}${telegram ? "✈️" : ""}`, "accept");

  if (state.monitored.size >= MAX_MONITORED) {
    let oldest = null;
    for (const [mint, t] of state.monitored.entries()) {
      if (!t.signal && (!oldest || t.detectedAt < oldest.detectedAt)) oldest = t;
    }
    if (oldest) stopMonitoring(oldest.mint, wsPortal);
    else { addLog(`⚠️ Cola llena, descartando ${candidate.symbol}`, "warn"); return; }
  }

  startMonitoring(candidate, wsPortal);
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

  ws.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.txType === "create" || (msg.mint && !msg.txType)) {
        processNewToken(msg, ws);
        return;
      }
      if ((msg.txType === "buy" || msg.txType === "sell") && state.monitored.has(msg.mint)) {
        const price = calcPriceFromTrade(msg);
        const volume = calcVolumeUSD(msg);
        if (price && price > 0) processTrade(msg.mint, price, volume, ws);
        return;
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
    for (const [, token] of state.monitored.entries()) {
      if (token.ticker) clearInterval(token.ticker);
    }
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
    log: state.log.slice(0, 100),
    stats: state.stats
  });
});
app.delete("/api/token/:mint", (req, res) => {
  stopMonitoring(req.params.mint);
  res.json({ ok: true });
});

const server = createServer(app);
const wss = new WebSocketServer({ server });
wss.on("connection", (ws) => {
  frontendClients.add(ws);
  ws.send(JSON.stringify({
    event: "fullState", data: {
      monitored: Array.from(state.monitored.values()).map(tokenToJSON),
      signals: state.signals.slice(0, 50),
      log: state.log.slice(0, 100),
      stats: state.stats, wsStatus: "connected"
    }
  }));
  ws.on("close", () => frontendClients.delete(ws));
  ws.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.action === "removeToken") stopMonitoring(msg.mint);
    } catch {}
  });
});

server.listen(PORT, () => {
  console.log(`🚀 SolScanBot corriendo en puerto ${PORT}`);
  connectPumpPortal();
});
