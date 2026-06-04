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

// Pump.fun constants
const PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMP_FEE_ACCOUNT = "CebN5WGQ4jvEPvsVU4EoHEpgznyQHeEPRsrGABNfhFmz";
const TOTAL_SUPPLY = 1_000_000_000; // 1B tokens supply fijo en pump.fun

const HELIUS_API_KEY = "86268796-07db-4bab-8e4f-abc4f697f64d";
const HELIUS_WS = `wss://atlas-mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

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

// ── Parser del bonding curve de Pump.fun ──
// Fórmula: precio = vSOL_en_curva / vTokens_en_curva * solPriceUSD
// MC = precio * TOTAL_SUPPLY
function parsePumpTx(tx) {
  try {
    const meta = tx.meta;
    const message = tx.transaction?.message;
    if (!meta || !message) return null;

    const accountKeys = message.accountKeys?.map(k =>
      typeof k === "string" ? k : k.pubkey
    ) || [];

    const logs = meta.logMessages || [];
    const isCreate = logs.some(l =>
      l.includes("InitializeMint") ||
      l.includes("Instruction: Create")
    );
    const isBuy = logs.some(l => l.includes("Instruction: Buy"));
    const isSell = logs.some(l => l.includes("Instruction: Sell"));

    if (!isCreate && !isBuy && !isSell) return null;

    // Encontrar mint del token
    const tokenBalances = meta.postTokenBalances || [];
    if (tokenBalances.length === 0) return null;
    const mint = tokenBalances[0]?.mint;
    if (!mint) return null;

    // ── Precio desde bonding curve ──
    // En Pump.fun el bonding curve account es el que tiene el balance de SOL
    // y es diferente al fee account y al programa
    // Buscamos la cuenta que NO es el usuario, NO es el programa, NO es el fee
    const preBalances = meta.preBalances || [];
    const postBalances = meta.postBalances || [];

    // El bonding curve es típicamente accountKeys[3] en buy/sell
    // y accountKeys[2] en create
    let vSolAfter = 0;
    let vSolBefore = 0;

    // Buscar la cuenta del bonding curve — tiene el mayor balance de SOL
    // que no sea el programa ni el fee account
    const skipAccounts = new Set([PUMP_PROGRAM, PUMP_FEE_ACCOUNT]);
    let bondingCurveIdx = -1;
    let maxBalance = 0;

    for (let i = 0; i < accountKeys.length; i++) {
      if (skipAccounts.has(accountKeys[i])) continue;
      const balance = postBalances[i] || 0;
      if (balance > maxBalance && balance > 10_000_000) { // > 0.01 SOL
        maxBalance = balance;
        bondingCurveIdx = i;
      }
    }

    if (bondingCurveIdx === -1) return null;

    vSolAfter = (postBalances[bondingCurveIdx] || 0) / 1e9;
    vSolBefore = (preBalances[bondingCurveIdx] || 0) / 1e9;

    // Tokens en el bonding curve (post-tx)
    let vTokensAfter = 0;
    for (const tb of tokenBalances) {
      if (accountKeys[tb.accountIndex] !== mint) {
        vTokensAfter = parseFloat(tb.uiTokenAmount?.uiAmount || 0);
        if (vTokensAfter > 0) break;
      }
    }

    if (vSolAfter <= 0 || vTokensAfter <= 0) return null;

    // Precio real del bonding curve
    const priceInSOL = vSolAfter / vTokensAfter;
    const priceUSD = priceInSOL * solPriceUSD;
    const mcUSD = priceUSD * TOTAL_SUPPLY;

    // Volumen de esta tx
    const solMoved = Math.abs(vSolAfter - vSolBefore);
    const volumeUSD = solMoved * solPriceUSD;

    return {
      mint,
      price: priceUSD,
      mc: mcUSD,
      volumeUSD,
      isCreate,
      isBuy,
      isSell,
      vSol: vSolAfter,
      vTokens: vTokensAfter,
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
      zone, price, tp, sl,
      time: Date.now(), status: "OPEN",
      tradeCount: token.tradeCount,
      volumeUSD: token.volumeUSD,
      mc: token.mc,
    };

    state.signals.unshift(signal);
    if (state.signals.length > 100) state.signals.pop();
    addLog(`🎯 SEÑAL ${zone} en ${token.symbol} @ MC $${Math.round(token.mc / 1000)}K`, "signal");
    broadcast({ event: "newSignal", data: signal });
    broadcast({ event: "stats", data: state.stats });
    openDemoTrade(signal);
  }
}

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

function startMonitoring(token) {
  if (state.monitored.has(token.mint)) return;
  const entry = {
    ...token, candles: [], currentCandle: null, bb: null,
    candleCount: 0, signal: null, lastUpdate: Date.now(), candles50: [],
    priceHigh: token.price || 0, priceLow: token.price || 0,
    tradeCount: 0, volumeUSD: 0,
  };
  state.monitored.set(token.mint, entry);
  addLog(`📊 Monitorizando ${token.symbol} — MC $${Math.round(token.mc / 1000)}K`, "monitor");
  broadcast({ event: "newToken", data: tokenToJSON(entry) });
  broadcast({ event: "stats", data: state.stats });
}

function stopMonitoring(mint) {
  state.monitored.delete(mint);
  broadcast({ event: "removeToken", data: { mint } });
}

async function processNewToken(mint, price, mc, volumeUSD) {
  if (seenMints.has(mint)) return;
  seenMints.add(mint);
  if (seenMints.size > 5000) seenMints.clear();

  state.stats.seen++;
  broadcast({ event: "stats", data: state.stats });

  if (mc > 0 && mc < MIN_MC_USD) {
    addLog(`⛔ MC bajo ($${Math.round(mc)}): ${shortAddr(mint)}`, "filter");
    return;
  }

  const meta = await fetchTokenMetadata(mint);
  if (!meta) { addLog(`⛔ Sin metadata: ${shortAddr(mint)}`, "filter"); return; }

  const { twitter, website, telegram, name, symbol } = meta;
  if (!twitter && !website && !telegram) {
    addLog(`⛔ Sin sociales: ${name || shortAddr(mint)}`, "filter");
    return;
  }

  state.stats.filtered++;
  addLog(`✅ ${symbol} — MC $${Math.round(mc / 1000)}K — ${twitter ? "𝕏" : ""}${website ? "🌐" : ""}${telegram ? "✈️" : ""}`, "accept");

  if (state.monitored.size >= MAX_MONITORED) {
    let oldest = null;
    for (const [m, t] of state.monitored.entries()) {
      const age = Date.now() - t.detectedAt;
      if (!t.signal && age >= MIN_MONITOR_MS && (!oldest || t.detectedAt < oldest.detectedAt)) oldest = t;
    }
    if (oldest) stopMonitoring(oldest.mint);
    else { addLog(`⚠️ Cola llena, descartando ${symbol}`, "warn"); return; }
  }

  startMonitoring({ mint, name: name || "Unknown", symbol: symbol || "???", twitter, website, telegram, mc, price, detectedAt: Date.now() });
}

function connectHelius() {
  addLog("🔌 Conectando a Helius...", "info");
  broadcast({ event: "wsStatus", data: "connecting" });

  const ws = new WebSocket(HELIUS_WS);
  let pingInterval;

  ws.on("open", () => {
    addLog("✅ Helius conectado — bonding curve parser activo 🚀", "info");
    broadcast({ event: "wsStatus", data: "connected" });
    ws.send(JSON.stringify({
      jsonrpc: "2.0", id: 420,
      method: "transactionSubscribe",
      params: [
        { accountInclude: [PUMP_PROGRAM], failed: false },
        { commitment: "processed", encoding: "jsonParsed", transactionDetails: "full", maxSupportedTransactionVersion: 0 }
      ]
    }));
    pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ jsonrpc: "2.0", id: 999, method: "ping" }));
    }, 20_000);
  });

  ws.on("message", async (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      const tx = msg?.params?.result?.transaction;
      if (!tx) return;
      if (tx.meta?.err) return;

      const parsed = parsePumpTx(tx);
      if (!parsed || parsed.price <= 0) return;

      const { mint, price, mc, volumeUSD, isCreate, isBuy } = parsed;

      if (isCreate) {
        await processNewToken(mint, price, mc, volumeUSD);
      } else if (isBuy && state.monitored.has(mint)) {
        // Solo actualizamos en compras — precio sube = señal válida
        updateCandle(mint, price, volumeUSD);
      } else if (!isBuy && state.monitored.has(mint)) {
        // En ventas actualizamos precio pero no generamos señal
        const token = state.monitored.get(mint);
        if (token) {
          token.price = price;
          token.mc = mc;
          updateDemoTrades(mint, price);
          broadcast({ event: "tokenUpdate", data: tokenToJSON(token) });
        }
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
  console.log(`🚀 SolScanBot — Helius bonding curve parser`);
  connectHelius();
});
