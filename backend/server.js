import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import cors from "cors";
import fetch from "node-fetch";
import {
 Connection,
 Keypair,
 PublicKey,
 VersionedTransaction,
 LAMPORTS_PER_SOL,
} from "@solana/web3.js";
import bs58 from "bs58";

const PORT = process.env.PORT || 3001;
const SOL_PER_TRADE = 0.05;
const MAX_REAL_TRADES = 1;
const TP_PCT = 1.9;
const SL_PCT = 0.88;
const MAX_TRADE_DURATION_MS = 15 * 60 * 1000;
const TRAILING_BREAKEVEN_AT = 0.30;
const TRAILING_LOCK_AT = 0.63;
const TRAILING_FOLLOW_PCT = 0.20;

const BB_PERIOD = 20;
const BB_MULT = 2;
const CANDLE_MS = 60_000;

const MIN_VOL_24H = 200_000;
const MIN_MC = 100_000;
const MAX_MC = 1_000_000;
const MIN_AGE_MS = 60 * 60 * 1000;
const MAX_AGE_MS = 12 * 60 * 60 * 1000;
const MAX_MONITORED = 20;
const SCAN_INTERVAL_MS = 60_000;
const SIGNAL_COOLDOWN_MS = 5 * 60 * 1000;

const HELIUS_API_KEY = "86268796-07db-4bab-8e4f-abc4f697f64d";
const HELIUS_RPC = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const HELIUS_WS = `wss://atlas-mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data";
const GECKO_PUMPSWAP = "https://api.geckoterminal.com/api/v2/networks/solana/dexes/pumpswap/pools";

let wallet = null;
let connection = null;
let pumpPortalWs = null;

function initWallet() {
 try {
   const privateKeyStr = process.env.WALLET_PRIVATE_KEY;
   if (!privateKeyStr) {
     addLog("⚠️ WALLET_PRIVATE_KEY no configurada — modo solo demo", "warn");
     return;
   }
   const privateKeyBytes = bs58.decode(privateKeyStr);
   wallet = Keypair.fromSecretKey(privateKeyBytes);
   connection = new Connection(HELIUS_RPC, "confirmed");
   addLog(`✅ Wallet cargada: ${wallet.publicKey.toString()}`, "info");
 } catch (e) {
   addLog(`❌ Error cargando wallet: ${e.message}`, "error");
 }
}

async function getWalletBalance() {
 if (!wallet || !connection) return 0;
 try {
   const balance = await connection.getBalance(wallet.publicKey);
   return balance / LAMPORTS_PER_SOL;
 } catch { return 0; }
}

async function getTokenBalance(mint) {
 if (!wallet || !connection) return 0;
 try {
   const accounts = await connection.getParsedTokenAccountsByOwner(
     wallet.publicKey, { mint: new PublicKey(mint) }
   );
   if (accounts.value.length === 0) return 0;
   return accounts.value[0].account.data.parsed.info.tokenAmount.uiAmount || 0;
 } catch { return 0; }
}

const state = {
 monitored: new Map(),
 signals: [],
 demoTrades: [],
 realTrades: [],
 log: [],
 stats: {
   scanned: 0, added: 0, filtered: 0,
   demoOpen: 0, demoWins: 0, demoLosses: 0, demoExpired: 0, demoPnL: 0,
   realOpen: 0, realWins: 0, realLosses: 0, realExpired: 0,
   realPnL: 0, realPnLSol: 0,
   avgMaxGain: 0, avgMaxLoss: 0, closedCount: 0,
   maxGainSum: 0, maxLossSum: 0,
   walletBalance: 0,
 },
};

const frontendClients = new Set();
const seenPools = new Set();
const signalCooldown = new Map();

function addLog(msg, type = "info") {
 const entry = { msg, type, time: Date.now() };
 state.log.unshift(entry);
 if (state.log.length > 300) state.log.pop();
 broadcast({ event: "log", data: entry });
 console.log(`[${type.toUpperCase()}] ${msg}`);
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

function formatMC(n) {
 if (!n || n === 0) return "$0";
 if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
 if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
 return `$${Math.round(n)}`;
}

function tokenToJSON(t) {
 return {
   mint: t.mint,
   poolAddress: t.poolAddress,
   name: t.name,
   symbol: t.symbol,
   mc: t.mc,
   price: t.price,
   vol24h: t.vol24h,
   ageMs: Date.now() - t.createdAt,
   bb: t.bb,
   candleCount: t.candleCount,
   candles: t.candles50 || [],
   signal: t.signal,
   tp: t.tp,
   sl: t.sl,
   detectedAt: t.detectedAt,
   lastUpdate: t.lastUpdate,
   tradeCount: t.tradeCount,
   volumeUSD: t.volumeUSD,
   priceHigh: t.priceHigh,
   priceLow: t.priceLow,
   pricePct1h: t.pricePct1h,
   pricePct6h: t.pricePct6h,
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

setInterval(async () => {
 if (wallet) {
   state.stats.walletBalance = await getWalletBalance();
   broadcast({ event: "stats", data: state.stats });
 }
}, 30_000);

function calcBollinger(candles) {
 if (candles.length < BB_PERIOD) return null;
 const slice = candles.slice(-BB_PERIOD);
 const closes = slice.map(c => c.close);
 const mean = closes.reduce((a, b) => a + b, 0) / closes.length;
 const variance = closes.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / closes.length;
 const std = Math.sqrt(variance);
 return {
   upper: +(mean + BB_MULT * std).toFixed(12),
   middle: +mean.toFixed(12),
   lower: +(mean - BB_MULT * std).toFixed(12),
 };
}

// ── GECKOTERMINAL SCANNER ──────────────────────────────────────
async function scanPumpSwap() {
 addLog("🔍 Escaneando PumpSwap via GeckoTerminal...", "info");
 try {
   let added = 0;
   for (let page = 1; page <= 3; page++) {
     const res = await fetch(
       `${GECKO_PUMPSWAP}?page=${page}&order=h24_volume_usd_desc`,
       {
         headers: { "Accept": "application/json;version=20230302" },
         signal: AbortSignal.timeout(10000),
       }
     );
     if (!res.ok) { addLog(`❌ GeckoTerminal ${res.status}`, "error"); break; }
     const json = await res.json();
     const pools = json?.data || [];

     for (const pool of pools) {
       const attr = pool.attributes || {};
       const poolAddr = attr.address || pool.id?.replace("solana_", "");
       if (!poolAddr || seenPools.has(poolAddr)) continue;

       const createdAt = attr.pool_created_at ? new Date(attr.pool_created_at).getTime() : null;
       if (!createdAt) continue;
       const ageMs = Date.now() - createdAt;
       if (ageMs < MIN_AGE_MS || ageMs > MAX_AGE_MS) continue;

       const vol24h = parseFloat(attr.volume_usd?.h24 || 0);
       if (vol24h < MIN_VOL_24H) continue;

       const mc = parseFloat(attr.fdv_usd || attr.market_cap_usd || 0);
       if (mc < MIN_MC || mc > MAX_MC) continue;

       const price = parseFloat(attr.base_token_price_usd || 0);
       if (price <= 0) continue;

       const pricePct1h = parseFloat(attr.price_change_percentage?.h1 || 0);
       const pricePct6h = parseFloat(attr.price_change_percentage?.h6 || 0);
       if (pricePct1h < -30) continue;

       const relationships = pool.relationships || {};
       const baseTokenId = relationships.base_token?.data?.id || "";
       const mint = baseTokenId.replace("solana_", "") || null;
       if (!mint || mint.length < 32) continue;

       seenPools.add(poolAddr);
       state.stats.scanned++;

       if (state.monitored.size >= MAX_MONITORED) {
         let oldest = null;
         for (const [, t] of state.monitored.entries()) {
           if (!t.signal && (!oldest || t.detectedAt < oldest.detectedAt)) oldest = t;
         }
         if (oldest) stopMonitoring(oldest.mint);
         else continue;
       }

       const rawName = attr.name || "";
       const symbol = rawName.split(" / ")[0] || mint.slice(0, 8);
       const ageH = Math.round(ageMs / 3600000);

       addLog(`📊 ${symbol} | MC ${formatMC(mc)} | Vol ${formatMC(vol24h)} | ${ageH}h | 1h:${pricePct1h > 0 ? "+" : ""}${pricePct1h.toFixed(1)}%`, "accept");

       startMonitoring({ mint, poolAddress: poolAddr, name: symbol, symbol, mc, price, vol24h, createdAt, pricePct1h, pricePct6h });
       added++;
       state.stats.added++;
     }
     await new Promise(r => setTimeout(r, 600));
   }
   if (added > 0) addLog(`✅ ${added} tokens añadidos`, "info");
   else addLog("ℹ️ Sin tokens nuevos", "info");
   broadcast({ event: "stats", data: state.stats });
 } catch (e) {
   addLog(`❌ Error scanner: ${e.message}`, "error");
 }
}

function startMonitoring(token) {
 if (state.monitored.has(token.mint)) return;
 const entry = {
   ...token,
   detectedAt: Date.now(), lastUpdate: Date.now(),
   candles: [], currentCandle: null, candleCount: 0, candles50: [],
   bb: null, signal: null, tp: null, sl: null,
   tradeCount: 0, volumeUSD: 0,
   priceHigh: token.price, priceLow: token.price,
   ticker: null,
 };
 state.monitored.set(token.mint, entry);
 entry.ticker = setInterval(() => {
   const t = state.monitored.get(token.mint);
   if (!t) { clearInterval(entry.ticker); return; }
   if (t.price > 0) updateCandle(token.mint, t.price, 0);
 }, CANDLE_MS);
 if (pumpPortalWs?.readyState === WebSocket.OPEN) {
   pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [token.mint] }));
 }
 broadcast({ event: "newToken", data: tokenToJSON(entry) });
}

function stopMonitoring(mint) {
 const token = state.monitored.get(mint);
 if (token?.ticker) clearInterval(token.ticker);
 state.monitored.delete(mint);
 broadcast({ event: "removeToken", data: { mint } });
}

function updateCandle(mint, price, volumeUSD = 0) {
 const token = state.monitored.get(mint);
 if (!token) return;
 const now = Date.now();
 const currentMin = Math.floor(now / CANDLE_MS) * CANDLE_MS;
 if (!token.currentCandle || token.currentCandle.time !== currentMin) {
   if (token.currentCandle) {
     token.candles.push({ ...token.currentCandle });
     if (token.candles.length > 300) token.candles.shift();
   }
   const prevClose = token.currentCandle?.close ?? price;
   token.currentCandle = { time: currentMin, open: prevClose, high: price, low: price, close: price };
 } else {
   token.currentCandle.high = Math.max(token.currentCandle.high, price);
   token.currentCandle.low = Math.min(token.currentCandle.low, price);
   token.currentCandle.close = price;
 }
 token.price = price;
 token.mc = price * 1_000_000_000;
 token.priceHigh = Math.max(token.priceHigh || 0, price);
 token.priceLow = token.priceLow === 0 ? price : Math.min(token.priceLow, price);
 token.tradeCount++;
 token.volumeUSD += volumeUSD;
 token.lastUpdate = now;
 const allCandles = [...token.candles, token.currentCandle];
 const bb = calcBollinger(allCandles);
 token.bb = bb;
 token.candleCount = allCandles.length;
 token.candles50 = allCandles.slice(-50);
 updateDemoTrades(mint, price);
 updateRealTrades(mint, price);
 if (bb && allCandles.length >= BB_PERIOD) checkSignal(mint, price, bb);
 broadcast({ event: "tokenUpdate", data: tokenToJSON(token) });
}

function checkSignal(mint, price, bb) {
 const token = state.monitored.get(mint);
 if (!token) return;
 const lastSignal = signalCooldown.get(mint) || 0;
 if (Date.now() - lastSignal < SIGNAL_COOLDOWN_MS) return;
 if ((token.tradeCount || 0) < 5) return;
 if (token.priceHigh > 0 && (token.priceHigh - price) / token.priceHigh > 0.35) return;
 const touchedLower = price <= bb.lower * 1.02;
 const touchedMiddle = !touchedLower && Math.abs(price - bb.middle) / bb.middle < 0.015;
 if (!touchedLower && !touchedMiddle) return;
 const zone = touchedLower ? "LOWER" : "MIDDLE";
 const tp = +(price * TP_PCT).toFixed(12);
 const sl = +(price * SL_PCT).toFixed(12);
 token.signal = zone; token.tp = tp; token.sl = sl;
 signalCooldown.set(mint, Date.now());
 state.stats.filtered++;
 const signal = {
   id: `${mint}-${Date.now()}`,
   mint, poolAddress: token.poolAddress, name: token.name, symbol: token.symbol,
   zone, price, tp, sl, mc: token.mc, vol24h: token.vol24h, time: Date.now(),
 };
 state.signals.unshift(signal);
 if (state.signals.length > 100) state.signals.pop();
 addLog(`🎯 SEÑAL ${zone}: ${token.symbol} @ MC ${formatMC(token.mc)} | Vol ${formatMC(token.vol24h)}`, "signal");
 broadcast({ event: "newSignal", data: signal });
 broadcast({ event: "stats", data: state.stats });
 openDemoTrade(signal);
 openRealTrade(signal);
}

// ── REAL TRADING ───────────────────────────────────────────────
async function buyToken(mint, solAmount) {
 if (!wallet || !connection) return null;
 try {
   addLog(`💳 Comprando ${solAmount} SOL de ${shortAddr(mint)}...`, "real");
   const response = await fetch("https://pumpportal.fun/api/trade-local", {
     method: "POST",
     headers: { "Content-Type": "application/json" },
     body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "buy", mint, denominatedInSol: "true", amount: solAmount, slippage: 15, priorityFee: 0.0005, pool: "pump" }),
     signal: AbortSignal.timeout(10000),
   });
   if (!response.ok) { addLog(`❌ Error compra: ${response.status}`, "error"); return null; }
   const txData = await response.arrayBuffer();
   const tx = VersionedTransaction.deserialize(new Uint8Array(txData));
   tx.sign([wallet]);
   const signature = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: "confirmed" });
   await connection.confirmTransaction(signature, "confirmed");
   addLog(`✅ COMPRA OK: ${shortAddr(mint)} | TX: ${signature}`, "real");
   return signature;
 } catch (e) { addLog(`❌ Error compra: ${e.message}`, "error"); return null; }
}

async function sellToken(mint) {
 if (!wallet || !connection) return null;
 try {
   const tokenBalance = await getTokenBalance(mint);
   if (tokenBalance <= 0) { addLog(`⚠️ Sin tokens: ${shortAddr(mint)}`, "warn"); return null; }
   const response = await fetch("https://pumpportal.fun/api/trade-local", {
     method: "POST",
     headers: { "Content-Type": "application/json" },
     body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "sell", mint, denominatedInSol: "false", amount: tokenBalance, slippage: 15, priorityFee: 0.0005, pool: "pump" }),
     signal: AbortSignal.timeout(10000),
   });
   if (!response.ok) { addLog(`❌ Error venta: ${response.status}`, "error"); return null; }
   const txData = await response.arrayBuffer();
   const tx = VersionedTransaction.deserialize(new Uint8Array(txData));
   tx.sign([wallet]);
   const signature = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: "confirmed" });
   await connection.confirmTransaction(signature, "confirmed");
   addLog(`✅ VENTA OK: ${shortAddr(mint)} | TX: ${signature}`, "real");
   return signature;
 } catch (e) { addLog(`❌ Error venta: ${e.message}`, "error"); return null; }
}

async function openRealTrade(signal) {
 if (!wallet) return;
 if (state.realTrades.filter(t => t.status === "OPEN").length >= MAX_REAL_TRADES) { addLog(`⚠️ Máximo trades reales`, "warn"); return; }
 const balance = await getWalletBalance();
 if (balance < SOL_PER_TRADE + 0.01) { addLog(`⚠️ Balance insuficiente: ${balance.toFixed(3)} SOL`, "warn"); return; }
 const signature = await buyToken(signal.mint, SOL_PER_TRADE);
 if (!signature) return;
 const trade = {
   id: `real-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
   mint: signal.mint, symbol: signal.symbol, name: signal.name, zone: signal.zone,
   entryPrice: signal.price, tp: signal.tp, sl: signal.sl, solAmount: SOL_PER_TRADE,
   buySignature: signature, sellSignature: null,
   openTime: Date.now(), closeTime: null, closePrice: null,
   result: null, pnlPct: null, pnlSol: null,
   maxGainPct: 0, maxLossPct: 0, currentPct: 0,
   trailingPhase: "INITIAL", status: "OPEN",
   expiresAt: Date.now() + MAX_TRADE_DURATION_MS, sellRetries: 0,
 };
 state.realTrades.unshift(trade);
 if (state.realTrades.length > 200) state.realTrades.pop();
 state.stats.realOpen++;
 state.stats.walletBalance = await getWalletBalance();
 broadcast({ event: "newRealTrade", data: trade });
 broadcast({ event: "stats", data: state.stats });
 addLog(`🔴 REAL: ${signal.symbol} | ${SOL_PER_TRADE} SOL | Banda ${signal.zone}`, "real");
}

async function closeRealTrade(trade, price, reason) {
 if (trade.status !== "OPEN") return;
 trade.status = "CLOSING";
 const signature = await sellToken(trade.mint);
 if (!signature) {
   trade.sellRetries = (trade.sellRetries || 0) + 1;
   if (trade.sellRetries <= 3) { trade.status = "OPEN"; setTimeout(() => closeRealTrade(trade, price, reason), 15000); return; }
   trade.status = "SELL_FAILED"; broadcast({ event: "realTradeClosed", data: trade }); return;
 }
 trade.sellSignature = signature; trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
 const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
 trade.pnlPct = +pnlPct.toFixed(2);
 trade.pnlSol = +(trade.solAmount * pnlPct / 100).toFixed(4);
 const durationSec = Math.round((trade.closeTime - trade.openTime) / 1000);
 if (reason === "TP" || (reason === "SL" && trade.pnlPct >= 0)) {
   trade.result = "WIN"; state.stats.realWins++; state.stats.realPnL += trade.pnlPct; state.stats.realPnLSol += trade.pnlSol;
   addLog(`✅ REAL WIN: ${trade.symbol} +${trade.pnlPct}% en ${durationSec}s`, "realwin");
 } else if (reason === "SL") {
   trade.result = "LOSS"; state.stats.realLosses++; state.stats.realPnL += trade.pnlPct; state.stats.realPnLSol += trade.pnlSol;
   addLog(`❌ REAL LOSS: ${trade.symbol} ${trade.pnlPct}% en ${durationSec}s`, "realloss");
 } else {
   trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS"; state.stats.realExpired++;
   state.stats.realPnL += trade.pnlPct; state.stats.realPnLSol += trade.pnlSol;
   addLog(`⏱️ REAL EXP: ${trade.symbol} ${trade.pnlPct > 0 ? "+" : ""}${trade.pnlPct}%`, "real");
 }
 state.stats.realOpen = Math.max(0, state.stats.realOpen - 1);
 state.stats.walletBalance = await getWalletBalance();
 broadcast({ event: "realTradeClosed", data: trade });
 broadcast({ event: "stats", data: state.stats });
}

function updateRealTrades(mint, price) {
 const now = Date.now();
 for (const trade of state.realTrades) {
   if (trade.mint !== mint || trade.status !== "OPEN") continue;
   const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
   trade.currentPct = +currentPct.toFixed(2);
   trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
   trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
   const gainPct = (price - trade.entryPrice) / trade.entryPrice;
   if (trade.trailingPhase === "FOLLOWING") {
     const newSl = price * (1 - TRAILING_FOLLOW_PCT);
     if (newSl > trade.sl) trade.sl = +newSl.toFixed(12);
   } else if (gainPct >= TRAILING_LOCK_AT) {
     trade.trailingPhase = "FOLLOWING";
     trade.sl = +Math.max(trade.sl, price * (1 - TRAILING_FOLLOW_PCT)).toFixed(12);
   } else if (gainPct >= TRAILING_BREAKEVEN_AT && trade.trailingPhase === "INITIAL") {
     trade.trailingPhase = "BREAKEVEN"; trade.sl = +trade.entryPrice.toFixed(12);
   }
   if (price >= trade.tp) closeRealTrade(trade, price, "TP");
   else if (price <= trade.sl) closeRealTrade(trade, price, "SL");
   else if (now >= trade.expiresAt) closeRealTrade(trade, price, "EXPIRED");
   else broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
 }
}

setInterval(() => {
 const now = Date.now();
 for (const trade of state.realTrades) {
   if (trade.status !== "OPEN" || now < trade.expiresAt) continue;
   const token = state.monitored.get(trade.mint);
   closeRealTrade(trade, token?.price || trade.entryPrice, "EXPIRED");
 }
}, 30_000);

// ── DEMO TRADING ───────────────────────────────────────────────
function openDemoTrade(signal) {
 const trade = {
   id: `demo-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
   mint: signal.mint, symbol: signal.symbol, name: signal.name, zone: signal.zone,
   entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
   openTime: Date.now(), closeTime: null, closePrice: null,
   result: null, pnlPct: null, maxGainPct: 0, maxLossPct: 0, currentPct: 0,
   trailingPhase: "INITIAL", status: "OPEN",
   expiresAt: Date.now() + MAX_TRADE_DURATION_MS,
 };
 state.demoTrades.unshift(trade);
 if (state.demoTrades.length > 500) state.demoTrades.pop();
 state.stats.demoOpen++;
 broadcast({ event: "newDemoTrade", data: trade });
 broadcast({ event: "stats", data: state.stats });
 addLog(`📝 DEMO: ${signal.symbol} | Banda ${signal.zone} | TP +90% SL -12%`, "demo");
}

function updateDemoTrades(mint, price) {
 const now = Date.now();
 for (const trade of state.demoTrades) {
   if (trade.mint !== mint || trade.status !== "OPEN") continue;
   const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
   trade.currentPct = +currentPct.toFixed(2);
   trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
   trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
   const gainPct = (price - trade.entryPrice) / trade.entryPrice;
   if (trade.trailingPhase === "FOLLOWING") {
     const newSl = price * (1 - TRAILING_FOLLOW_PCT);
     if (newSl > trade.sl) trade.sl = +newSl.toFixed(12);
   } else if (gainPct >= TRAILING_LOCK_AT) {
     trade.trailingPhase = "FOLLOWING";
     trade.sl = +Math.max(trade.sl, price * (1 - TRAILING_FOLLOW_PCT)).toFixed(12);
     addLog(`🔄 FOLLOWING: ${trade.symbol}`, "trail");
   } else if (gainPct >= TRAILING_BREAKEVEN_AT && trade.trailingPhase === "INITIAL") {
     trade.trailingPhase = "BREAKEVEN"; trade.sl = +trade.entryPrice.toFixed(12);
     addLog(`⚖️ BREAKEVEN: ${trade.symbol}`, "trail");
   }
   if (price >= trade.tp) closeDemoTrade(trade, price, "TP");
   else if (price <= trade.sl) closeDemoTrade(trade, price, "SL");
   else if (now >= trade.expiresAt) closeDemoTrade(trade, price, "EXPIRED");
   else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
 }
}

function closeDemoTrade(trade, price, reason) {
 trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
 const durationSec = Math.round((trade.closeTime - trade.openTime) / 1000);
 const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
 trade.pnlPct = +pnlPct.toFixed(2);
 if (reason === "TP") {
   trade.result = "WIN"; state.stats.demoWins++; state.stats.demoPnL += (TP_PCT - 1) * 100;
   addLog(`✅ WIN [TP]: ${trade.symbol} +${((TP_PCT-1)*100).toFixed(0)}% en ${durationSec}s`, "win");
 } else if (reason === "SL") {
   state.stats.demoPnL += trade.pnlPct;
   if (trade.pnlPct >= 0) { trade.result = "WIN"; state.stats.demoWins++; addLog(`✅ WIN [${trade.trailingPhase}]: ${trade.symbol} +${trade.pnlPct}% en ${durationSec}s`, "win"); }
   else { trade.result = "LOSS"; state.stats.demoLosses++; addLog(`❌ LOSS: ${trade.symbol} ${trade.pnlPct}% en ${durationSec}s`, "loss"); }
 } else {
   trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS"; state.stats.demoExpired++;
   state.stats.demoPnL += trade.pnlPct;
   addLog(`⏱️ EXP: ${trade.symbol} ${trade.pnlPct > 0 ? "+" : ""}${trade.pnlPct}%`, "expire");
 }
 state.stats.demoOpen = Math.max(0, state.stats.demoOpen - 1);
 state.stats.maxGainSum += trade.maxGainPct || 0;
 state.stats.maxLossSum += Math.abs(trade.maxLossPct || 0);
 state.stats.closedCount++;
 state.stats.avgMaxGain = +(state.stats.maxGainSum / state.stats.closedCount).toFixed(1);
 state.stats.avgMaxLoss = +(state.stats.maxLossSum / state.stats.closedCount).toFixed(1);
 broadcast({ event: "demoTradeClosed", data: trade });
 broadcast({ event: "stats", data: state.stats });
}

setInterval(() => {
 const now = Date.now();
 for (const trade of state.demoTrades) {
   if (trade.status !== "OPEN" || now < trade.expiresAt) continue;
   const token = state.monitored.get(trade.mint);
   closeDemoTrade(trade, token?.price || trade.entryPrice, "EXPIRED");
 }
}, 30_000);

// ── PUMPPORTAL WS ──────────────────────────────────────────────
function connectPumpPortal() {
 addLog("🔌 Conectando a PumpPortal...", "info");
 pumpPortalWs = new WebSocket(PUMPPORTAL_WS);
 pumpPortalWs.on("open", () => {
   addLog("✅ PumpPortal conectado", "info");
   for (const [mint] of state.monitored.entries()) {
     pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
   }
 });
 pumpPortalWs.on("message", async (raw) => {
   try {
     const data = JSON.parse(raw.toString());
     if (data.message || data.errors) return;
     if ((data.txType === "buy" || data.txType === "sell") && data.mint && state.monitored.has(data.mint)) {
       const walletPubkey = wallet?.publicKey?.toString();
       if (walletPubkey && data.traderPublicKey === walletPubkey) return;
       const solAmount = data.solAmount || 0;
       const tokenAmount = data.tokenAmount || 0;
       if (tokenAmount > 0 && solAmount > 0) {
         const price = (solAmount / tokenAmount) * solPriceUSD;
         if (price > 0) updateCandle(data.mint, price, solAmount * solPriceUSD);
       }
     }
   } catch {}
 });
 pumpPortalWs.on("error", (err) => addLog(`❌ PumpPortal: ${err.message}`, "error"));
 pumpPortalWs.on("close", () => { addLog("🔄 PumpPortal reconectando...", "warn"); setTimeout(connectPumpPortal, 5000); });
}

// ── HELIUS WS — escucha PumpSwap + PumpFun ─────────────────────
function connectHelius() {
 addLog("🔌 Conectando a Helius...", "info");
 const ws = new WebSocket(HELIUS_WS);
 let pingInterval;
 ws.on("open", () => {
   addLog("✅ Helius conectado 🚀", "info");
   ws.send(JSON.stringify({
     jsonrpc: "2.0", id: 420,
     method: "transactionSubscribe",
     params: [
       {
         accountInclude: [PUMPSWAP_PROGRAM, PUMPFUN_PROGRAM],
         failed: false
       },
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
     const meta = tx.meta;
     if (!meta || meta.err) return;
     const accountKeys = tx.transaction?.message?.accountKeys || [];
     const walletPubkey = wallet?.publicKey?.toString();
     if (walletPubkey && accountKeys.some(k => (k.pubkey || k) === walletPubkey)) return;
     const tokenBalances = meta.postTokenBalances || [];
     if (!tokenBalances.length) return;
     const mint = tokenBalances[0]?.mint;
     if (!mint || !state.monitored.has(mint)) return;
     const solDiff = Math.abs((meta.preBalances?.[0] || 0) - (meta.postBalances?.[0] || 0)) / 1e9;
     let tokenDiff = 0;
     const preTokenBalances = meta.preTokenBalances || [];
     for (const post of tokenBalances) {
       const pre = preTokenBalances.find(p => p.accountIndex === post.accountIndex);
       const diff = Math.abs(parseFloat(post.uiTokenAmount?.uiAmount || 0) - parseFloat(pre?.uiTokenAmount?.uiAmount || 0));
       if (diff > 0) { tokenDiff = diff; break; }
     }
     if (solDiff === 0 || tokenDiff === 0) return;
     const price = (solDiff / tokenDiff) * solPriceUSD;
     if (price > 0) updateCandle(mint, price, solDiff * solPriceUSD);
   } catch {}
 });
 ws.on("error", (err) => addLog(`❌ Helius: ${err.message}`, "error"));
 ws.on("close", () => { clearInterval(pingInterval); addLog("🔄 Helius reconectando...", "warn"); setTimeout(connectHelius, 5000); });
}

// ── EXPRESS + WS ───────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());
app.get("/api/state", (req, res) => {
 res.json({
   monitored: Array.from(state.monitored.values()).map(tokenToJSON),
   signals: state.signals.slice(0, 50),
   demoTrades: state.demoTrades.slice(0, 200),
   realTrades: state.realTrades.slice(0, 200),
   log: state.log.slice(0, 100),
   stats: state.stats,
 });
});

const server = createServer(app);
const wss = new WebSocketServer({ server });
wss.on("connection", (ws) => {
 frontendClients.add(ws);
 ws.send(JSON.stringify({
   event: "fullState",
   data: {
     monitored: Array.from(state.monitored.values()).map(tokenToJSON),
     signals: state.signals.slice(0, 50),
     demoTrades: state.demoTrades.slice(0, 200),
     realTrades: state.realTrades.slice(0, 200),
     log: state.log.slice(0, 100),
     stats: state.stats,
     wsStatus: "connected",
   }
 }));
 ws.on("close", () => frontendClients.delete(ws));
});

server.listen(PORT, () => {
 console.log(`🚀 SolScanBot v3 — PumpSwap Bollinger | PumpSwap+PumpFun Helius`);
 initWallet();
 connectPumpPortal();
 connectHelius();
 setTimeout(scanPumpSwap, 3000);
 setInterval(scanPumpSwap, SCAN_INTERVAL_MS);
});
