import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import cors from "cors";
import fetch from "node-fetch";

const PORT = process.env.PORT || 3001;
const MAX_MONITORED = 20;
const BOLLINGER_PERIOD = 20;
const BOLLINGER_MULT = 2;
const MIN_MC_USD = 15000;
const MAX_MC_USD = 500000;
const CANDLE_MS = 10000; // velas de 10s para tokens con menos actividad
const SIGNAL_COOLDOWN_MS = 10 * 60 * 1000;
const SCAN_INTERVAL_MS = 30000; // escanear cada 30s

const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data";

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
    mc: t.mc, price: t.price, bb: t.bb,
    candleCount: t.candleCount, candles: t.candles50,
    signal: t.signal, signalPrice: t.signalPrice,
    tp: t.tp, sl: t.sl, detectedAt: t.detectedAt, lastUpdate: t.lastUpdate,
    tradeCount: t.tradeCount, volumeUSD: t.volumeUSD,
    priceChange5m: t.priceChange5m, priceChange1h: t.priceChange1h,
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

// Escanear tokens activos de DexScreener
async function scanDexScreener() {
  try {
    // Buscar tokens de Solana recientes con buena actividad
    const urls = [
      "https://api.dexscreener.com/latest/dex/search?q=solana&rankBy=trendingScoreH1&order=desc",
      "https://api.dexscreener.com/token-boosts/top/v1",
    ];

    for (const url of urls) {
      try {
        const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
        if (!res.ok) continue;
        const data = await res.json();

        const pairs = data.pairs || data || [];
        if (!Array.isArray(pairs)) continue;

        for (const pair of pairs) {
          if (pair.chainId !== "solana") continue;

          const mint = pair.baseToken?.address;
          if (!mint) continue;
          if (seenMints.has(mint)) continue;

          const mc = pair.fdv || pair.marketCap || 0;
          if (mc < MIN_MC_USD || mc > MAX_MC_USD) continue;

          const volume5m = pair.volume?.m5 || 0;
          const volume1h = pair.volume?.h1 || 0;
          if (volume5m < 100 && volume1h < 500) continue; // necesita actividad real

          const price = parseFloat(pair.priceUsd || 0);
          if (price <= 0) continue;

          const twitter = pair.info?.socials?.find(s => s.type === "twitter")?.url || null;
          const website = pair.info?.websites?.[0]?.url || null;
          if (!twitter && !website) continue;

          state.stats.seen++;
          broadcast({ event: "stats", data: state.stats });

          seenMints.add(mint);
          if (seenMints.size > 5000) seenMints.clear();

          state.stats.filtered++;
          addLog(`✅ ${pair.baseToken?.symbol} — MC $${Math.round(mc/1000)}K — Vol5m $${Math.round(volume5m)}`, "accept");

          if (state.monitored.size >= MAX_MONITORED) {
            let oldest = null;
            for (const [m, t] of state.monitored.entries()) {
              if (!t.signal && (!oldest || t.detectedAt < oldest.detectedAt)) oldest = t;
            }
            if (oldest) stopMonitoring(oldest.mint);
            else continue;
          }

          startMonitoring({
            mint,
            name: pair.baseToken?.name || "Unknown",
            symbol: pair.baseToken?.symbol || "???",
            twitter, website, telegram: null,
            mc, price,
            priceChange5m: pair.priceChange?.m5 || 0,
            priceChange1h: pair.priceChange?.h1 || 0,
            volumeUSD: volume5m,
            tradeCount: pair.txns?.m5?.buys + pair.txns?.m5?.sells || 0,
            detectedAt: Date.now(),
          });
        }
      } catch {}
    }
  } catch (e) {
    addLog(`⚠️ Error scan: ${e.message}`, "warn");
  }
}

// Actualizar precios de tokens monitorizados
async function updatePrices() {
  const mints = Array.from(state.monitored.keys());
  if (mints.length === 0) return;

  // DexScreener permite hasta 30 tokens por llamada
  const chunks = [];
  for (let i = 0; i < mints.length; i += 30) {
    chunks.push(mints.slice(i, i + 30));
  }

  for (const chunk of chunks) {
    try {
      const res = await fetch(
        `https://api.dexscreener.com/latest/dex/tokens/${chunk.join(",")}`,
        { signal: AbortSignal.timeout(5000) }
      );
      if (!res.ok) continue;
      const data = await res.json();
      const pairs = data.pairs || [];

      for (const pair of pairs) {
        if (pair.chainId !== "solana") continue;
        const mint = pair.baseToken?.address;
        if (!mint || !state.monitored.has(mint)) continue;

        const price = parseFloat(pair.priceUsd || 0);
        const volume = pair.volume?.m5 || 0;
        const mc = pair.fdv || pair.marketCap || 0;
        const txns = (pair.txns?.m5?.buys || 0) + (pair.txns?.m5?.sells || 0);

        if (price > 0) {
          const token = state.monitored.get(mint);
          token.mc = mc;
          token.tradeCount = (token.tradeCount || 0) + txns;
          token.volumeUSD = (token.volumeUSD || 0) + volume;
          token.priceChange5m = pair.priceChange?.m5 || 0;
          token.priceChange1h = pair.priceChange?.h1 || 0;
          updateCandle(mint, price, volume);
        }
      }
    } catch {}
  }
}

function updateCandle(mint, price, volumeUSD = 0) {
  const token = state.monitored.get(mint);
  if (!token) return;
  const now = Date.now();
  const currentCandle = Math.floor(now / CANDLE_MS) * CANDLE_MS;

  if (!token.currentCandle || token.currentCandle.time !== currentCandle) {
    if (token.currentCandle) {
      token.candles.push({ ...token.currentCandle });
      if (token.candles.length > 300) token.candles.shift();
    }
    const prevClose = token.currentCandle?.close ?? price;
    token.currentCandle = { time: currentCandle, open: prevClose, high: price, low: price, close: price };
  } else {
    token.currentCandle.high = Math.max(token.currentCandle.high, price);
    token.currentCandle.low = Math.min(token.currentCandle.low, price);
    token.currentCandle.close = price;
  }

  token.price = price;
  token.priceHigh = Math.max(token.priceHigh || 0, price);
  token.lastUpdate = now;

  const allCandles = [...token.candles, token.currentCandle];
  const bb = calcBollinger(allCandles);
  token.bb = bb;
  token.candleCount = allCandles.length;
  token.candles50 = allCandles.slice(-50);

  if (bb) checkSignal(mint, price, bb, allCandles.length);
  broadcast({ event: "tokenUpdate", data: tokenToJSON(token) });
}

function checkSignal(mint, price, bb, candleCount) {
  if (candleCount < BOLLINGER_PERIOD) return;
  const token = state.monitored.get(mint);
  if (!token) return;

  const lastSignal = signalCooldown.get(mint) || 0;
  if (Date.now() - lastSignal < SIGNAL_COOLDOWN_MS) return;
  if ((token.tradeCount || 0) < 5) return;
  if ((token.volumeUSD || 0) < 50) return;

  if (token.priceHigh > 0) {
    const dropFromHigh = (token.priceHigh - price) / token.priceHigh;
    if (dropFromHigh > 0.40) return;
  }

  const touchedLower = price <= bb.lower * 1.02;
  const touchedMiddle = !touchedLower && Math.abs(price - bb.middle) / bb.middle < 0.02;

  if (touchedLower || touchedMiddle) {
    const zone = touchedLower ? "LOWER" : "MIDDLE";
    const tp = +(price * 1.5).toFixed(10);
    const sl = +(price * 0.8).toFixed(10);
    token.signal = zone;
    token.signalPrice = price;
    token.tp = tp;
    token.sl = sl;
    signalCooldown.set(mint, Date.now());
    state.stats.signals++;

    const signal = {
      id: `${mint}-${Date.now()}`,
      mint, name: token.name, symbol: token.symbol,
      zone, price, tp, sl, time: Date.now(), status: "OPEN",
      tradeCount: token.tradeCount, volumeUSD: token.volumeUSD,
    };

    state.signals.unshift(signal);
    if (state.signals.length > 100) state.signals.pop();
    addLog(`🎯 SEÑAL ${zone} en ${token.symbol} | Vol $${Math.round(token.volumeUSD)} | MC $${Math.round(token.mc/1000)}K`, "signal");
    broadcast({ event: "newSignal", data: signal });
    broadcast({ event: "stats", data: state.stats });
  }
}

function startMonitoring(token) {
  if (state.monitored.has(token.mint)) return;
  const entry = {
    ...token, candles: [], currentCandle: null, bb: null,
    candleCount: 0, signal: null, lastUpdate: Date.now(), candles50: [],
    priceHigh: token.price || 0, ticker: null,
  };
  state.monitored.set(token.mint, entry);

  addLog(`📊 Monitorizando ${token.symbol} — MC $${Math.round(token.mc/1000)}K`, "monitor");
  broadcast({ event: "newToken", data: tokenToJSON(entry) });
  broadcast({ event: "stats", data: state.stats });
}

function stopMonitoring(mint) {
  const token = state.monitored.get(mint);
  if (token?.ticker) clearInterval(token.ticker);
  state.monitored.delete(mint);
  addLog(`⏹ Detenido ${shortAddr(mint)}`, "info");
  broadcast({ event: "removeToken", data: { mint } });
}

// PumpPortal para tokens nuevos que pasan de $15K
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
      if (msg.mint && (msg.txType === "create" || !msg.txType)) {
        const mc = msg.usdMarketCap || (msg.marketCapSol || 0) * 150;
        if (mc >= MIN_MC_USD) {
          // Token nuevo que ya cumple MC — raro pero posible
          addLog(`🆕 Token nuevo con MC alto: ${msg.name} $${Math.round(mc/1000)}K`, "info");
        }
      }
    } catch {}
  });

  ws.on("error", () => broadcast({ event: "wsStatus", data: "error" }));
  ws.on("close", () => {
    broadcast({ event: "wsStatus", data: "disconnected" });
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

  // Escanear tokens activos cada 30s
  scanDexScreener();
  setInterval(scanDexScreener, SCAN_INTERVAL_MS);

  // Actualizar precios cada 10s
  setInterval(updatePrices, 10_000);

  // Conectar PumpPortal
  connectPumpPortal();
});
