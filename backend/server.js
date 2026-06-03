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

const HELIUS_API_KEY = "7c210bdf-079b-4a47-aed8-57ddb7354971";
const HELIUS_WS = `wss://atlas-mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

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
    addLog(`💲 SOL = $${solPriceUSD}`, "info");
  } catch {}
}
setInterval(updateSolPrice, 60_000);
updateSolPrice();

async function fetchTokenMetadata(mint) {
  try {
    const res = await fetch(
      `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          jsonrpc: "2.0", id: "1", method: "getAsset",
          params: { id: mint }
        }),
        signal: AbortSignal.timeout(5000),
      }
    );
    const data = await res.json();
    const meta = data?.result;
    if (!meta) return null;

    const links = meta.content?.links || {};
    const jsonUri = meta.content?.json_uri;

    let twitter = links.twitter || null;
    let website = links.external_url || null;
    let telegram = null;
    let name = meta.content?.metadata?.name || null;
    let symbol = meta.content?.metadata?.symbol || null;

    if ((!twitter && !website) && jsonUri) {
      try {
        const r = await fetch(jsonUri, { signal: AbortSignal.timeout(3000) });
        const j = await r.json();
        twitter = twitter || j.twitter || j.extensions?.twitter || null;
        website = website || j.website || j.extensions?.website || null;
        telegram = j.telegram || j.extensions?.telegram || null;
        if (!name) name = j.name;
        if (!symbol) symbol = j.symbol;
      } catch {}
    }

    return { twitter, website, telegram, name, symbol };
  } catch { return null; }
}

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
  token.priceHigh = Math.max(token.priceHigh || 0, price);
  token.tradeCount = (token.tradeCount || 0) + 1;
  token.volumeUSD = (token.volumeUSD || 0) + volumeUSD;
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
  if ((token.volumeUSD || 0) < 10) return;

  if (token.priceHigh > 0) {
    const dropFromHigh = (token.priceHigh - price) / token.priceHigh;
    if (dropFromHigh > 0.30) return;
  }

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
    addLog(`🎯 SEÑAL ${zone} en ${token.symbol} | ${token.tradeCount} trades | $${Math.round(token.volumeUSD)} vol`, "signal");
    broadcast({ event: "newSignal", data: signal });
    broadcast({ event: "stats", data: state.stats });
  }
}

function startMonitoring(token) {
  if (state.monitored.has(token.mint)) return;
  const entry = {
    ...token, candles: [], currentCandle: null, bb: null,
    candleCount: 0, signal: null, lastUpdate: Date.now(), candles50: [],
    priceHigh: token.price || 0, tradeCount: 0, volumeUSD: 0, ticker: null,
  };
  state.monitored.set(token.mint, entry);

  entry.ticker = setInterval(() => {
    const t = state.monitored.get(token.mint);
    if (!t) { clearInterval(entry.ticker); return; }
    if (t.price > 0) updateCandle(token.mint, t.price, 0);
  }, CANDLE_MS);

  addLog(`📊 Monitorizando ${token.symbol || shortAddr(token.mint)}`, "monitor");
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

async function processNewToken(mint, solAmount, tokenAmount) {
  if (seenMints.has(mint)) return;
  seenMints.add(mint);
  if (seenMints.size > 5000) seenMints.clear();

  state.stats.seen++;
  broadcast({ event: "stats", data: state.stats });

  const price = solAmount > 0 && tokenAmount > 0
    ? (solAmount / tokenAmount) * solPriceUSD
    : 0;

  const mc = price * 1_000_000_000;
  if (mc > 0 && mc < MIN_MC_USD) {
    addLog(`⛔ MC bajo: ${shortAddr(mint)}`, "filter");
    return;
  }

  const meta = await fetchTokenMetadata(mint);
  if (!meta) {
    addLog(`⛔ Sin metadata: ${shortAddr(mint)}`, "filter");
    return;
  }

  const { twitter, website, telegram, name, symbol } = meta;

  if (!twitter && !website && !telegram) {
    addLog(`⛔ Sin sociales: ${name || shortAddr(mint)}`, "filter");
    return;
  }

  state.stats.filtered++;
  addLog(`✅ ${symbol} — MC ~$${Math.round(mc)} — ${twitter ? "𝕏" : ""}${website ? "🌐" : ""}${telegram ? "✈️" : ""}`, "accept");

  if (state.monitored.size >= MAX_MONITORED) {
    let oldest = null;
    for (const [m, t] of state.monitored.entries()) {
      const age = Date.now() - t.detectedAt;
      if (!t.signal && age >= MIN_MONITOR_MS && (!oldest || t.detectedAt < oldest.detectedAt)) {
        oldest = t;
      }
    }
    if (oldest) stopMonitoring(oldest.mint);
    else { addLog(`⚠️ Cola llena, descartando ${symbol}`, "warn"); return; }
  }

  startMonitoring({
    mint, name: name || "Unknown", symbol: symbol || "???",
    twitter, website, telegram, mc, price, detectedAt: Date.now(),
  });
}

function connectHelius() {
  addLog("🔌 Conectando a Helius...", "info");
  broadcast({ event: "wsStatus", data: "connecting" });

  const ws = new WebSocket(HELIUS_WS);
  let pingInterval;

  ws.on("open", () => {
    addLog("✅ Helius conectado — escuchando Pump.fun", "info");
    broadcast({ event: "wsStatus", data: "connected" });

    // Usar transactionSubscribe — viene todo parseado
    ws.send(JSON.stringify({
      jsonrpc: "2.0",
      id: 420,
      method: "transactionSubscribe",
      params: [
        {
          accountInclude: [PUMP_PROGRAM],
          failed: false,
        },
        {
          commitment: "processed",
          encoding: "jsonParsed",
          transactionDetails: "full",
          maxSupportedTransactionVersion: 0,
        }
      ]
    }));

    pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ jsonrpc: "2.0", id: 999, method: "ping" }));
      }
    }, 20_000);
  });

  ws.on("message", async (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      const tx = msg?.params?.result?.transaction;
      if (!tx) return;

      const meta = tx.meta;
      const message = tx.transaction?.message;
      if (!meta || !message) return;

      // Extraer mint del token
      const tokenBalances = meta.postTokenBalances || [];
      if (tokenBalances.length === 0) return;
      const mint = tokenBalances[0]?.mint;
      if (!mint) return;

      // Calcular SOL movido y tokens movidos
      const preSOL = meta.preBalances?.[0] || 0;
      const postSOL = meta.postBalances?.[0] || 0;
      const solDiff = Math.abs(postSOL - preSOL) / 1e9;

      let tokenDiff = 0;
      const preTokenBalances = meta.preTokenBalances || [];
      for (const post of tokenBalances) {
        const pre = preTokenBalances.find(p => p.accountIndex === post.accountIndex);
        const postAmt = parseFloat(post.uiTokenAmount?.uiAmount || 0);
        const preAmt = parseFloat(pre?.uiTokenAmount?.uiAmount || 0);
        if (Math.abs(postAmt - preAmt) > 0) {
          tokenDiff = Math.abs(postAmt - preAmt);
          break;
        }
      }

      if (solDiff === 0) return;

      const price = tokenDiff > 0 ? (solDiff / tokenDiff) * solPriceUSD : 0;
      const volumeUSD = solDiff * solPriceUSD;

      // Detectar si es creación (InitializeMint en logs)
      const logs = meta.logMessages || [];
      const isCreate = logs.some(l =>
        l.includes("InitializeMint") ||
        l.includes("initialize_mint") ||
        l.includes("create\n") ||
        l.includes("Instruction: Create")
      );

      if (isCreate) {
        await processNewToken(mint, solDiff, tokenDiff);
      } else if (state.monitored.has(mint) && price > 0) {
        updateCandle(mint, price, volumeUSD);
      }

    } catch {}
  });

  ws.on("error", (err) => {
    addLog(`❌ Error: ${err.message}`, "error");
    broadcast({ event: "wsStatus", data: "error" });
  });

  ws.on("close", () => {
    clearInterval(pingInterval);
    addLog("🔄 Reconectando en 5s...", "warn");
    broadcast({ event: "wsStatus", data: "disconnected" });
    for (const [, token] of state.monitored.entries()) {
      if (token.ticker) clearInterval(token.ticker);
    }
    state.monitored.clear();
    setTimeout(connectHelius, 5000);
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
  console.log(`🚀 SolScanBot Helius corriendo en puerto ${PORT}`);
  connectHelius();
});
