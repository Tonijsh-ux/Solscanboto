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
const MAX_MONITORED = 10;
const BOLLINGER_PERIOD = 20;
const BOLLINGER_MULT = 2;
const MIN_MC_USD = 1000;
const CANDLE_MS = 1000;
const SIGNAL_COOLDOWN_MS = 5 * 60 * 1000;
const MIN_MONITOR_MS = 2 * 60 * 1000;
const TP_PCT = 1.9;
const SL_PCT = 0.88;
const MAX_TRADE_DURATION_MS = 15 * 60 * 1000;
const MAX_TOKEN_AGE_MS = 5 * 60 * 1000;
const SOL_PER_TRADE = 0.05;
const MAX_REAL_TRADES = 1;

const TRAILING_BREAKEVEN_AT = 0.30;
const TRAILING_LOCK_AT      = 0.63;
const TRAILING_FOLLOW_PCT   = 0.20;

const HELIUS_API_KEY = "86268796-07db-4bab-8e4f-abc4f697f64d";
const HELIUS_WS = `wss://atlas-mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const HELIUS_RPC = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMP_API = "https://frontend-api.pump.fun/coins?offset=0&limit=20&sort=created_timestamp&order=DESC&includeNsfw=false";

let wallet = null;
let connection = null;

function initWallet() {
 try {
   const privateKeyStr = process.env.WALLET_PRIVATE_KEY;
   addLog(`🔑 Key length: ${privateKeyStr?.length || 0} | First 4: ${privateKeyStr?.slice(0,4) || 'NONE'}`, "info");
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
   const mintPubkey = new PublicKey(mint);
   const accounts = await connection.getParsedTokenAccountsByOwner(wallet.publicKey, { mint: mintPubkey });
   if (accounts.value.length === 0) return 0;
   return accounts.value[0].account.data.parsed.info.tokenAmount.uiAmount || 0;
 } catch (e) {
   addLog(`❌ Error balance token: ${e.message}`, "error");
   return 0;
 }
}

const state = {
 monitored: new Map(),
 signals: [],
 demoTrades: [],
 realTrades: [],
 log: [],
 stats: {
   seen: 0, filtered: 0, signals: 0,
   demoOpen: 0, demoWins: 0, demoLosses: 0, demoExpired: 0,
   demoPnL: 0,
   realOpen: 0, realWins: 0, realLosses: 0, realExpired: 0,
   realPnL: 0, realPnLSol: 0,
   avgMaxGain: 0, avgMaxLoss: 0,
   maxGainSum: 0, maxLossSum: 0, closedCount: 0,
   walletBalance: 0,
 },
};

const frontendClients = new Set();
const seenMints = new Map();
const signalCooldown = new Map();

function addLog(msg, type = "info") {
 const entry = { msg, type, time: Date.now() };
 state.log.unshift(entry);
 if (state.log.length > 200) state.log.pop();
 broadcast({ event: "log", data: entry });
 console.log(`[${type.toUpperCase()}] ${msg}`);
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

setInterval(async () => {
 if (wallet) {
   state.stats.walletBalance = await getWalletBalance();
   broadcast({ event: "stats", data: state.stats });
 }
}, 30_000);

// ── PUMP.FUN API ───────────────────────────────────────────────
async function fetchNewPumpTokens() {
 try {
   const res = await fetch(PUMP_API, {
     headers: {
       "User-Agent": "Mozilla/5.0",
       "Accept": "application/json",
     },
     signal: AbortSignal.timeout(8000),
   });
   addLog(`🔍 Pump API status: ${res.status}`, "info");
   if (!res.ok) {
     addLog(`❌ Pump API error: ${res.status}`, "error");
     return;
   }
   const coins = await res.json();
   addLog(`🔍 Coins recibidos: ${Array.isArray(coins) ? coins.length : typeof coins}`, "info");
   if (!Array.isArray(coins)) {
     addLog(`❌ Formato inesperado: ${JSON.stringify(coins).slice(0, 100)}`, "error");
     return;
   }

   let nuevos = 0;
   for (const coin of coins) {
     const createdAt = coin.created_timestamp;
     if (!createdAt) continue;
     const ageMs = Date.now() - createdAt;
     const ageSec = Math.round(ageMs / 1000);
     if (ageMs > MAX_TOKEN_AGE_MS) continue;
     if (seenMints.has(coin.mint)) continue;
     nuevos++;

     const mcUsd = coin.usd_market_cap || 0;
     if (mcUsd < MIN_MC_USD) {
       addLog(`⛔ MC bajo ($${Math.round(mcUsd)}): ${coin.symbol}`, "filter");
       seenMints.set(coin.mint, Date.now());
       continue;
     }

     const twitter = coin.twitter || null;
     const website = coin.website || null;
     const telegram = coin.telegram || null;
     if (!twitter && !website && !telegram) {
       addLog(`⛔ Sin sociales: ${coin.name || coin.symbol}`, "filter");
       seenMints.set(coin.mint, Date.now());
       continue;
     }

     const price = mcUsd / 1_000_000_000;
     addLog(`🆕 Token ${ageSec}s: ${coin.symbol} — MC ~$${Math.round(mcUsd)} — ${twitter ? "𝕏" : ""}${website ? "🌐" : ""}${telegram ? "✈️" : ""}`, "accept");
     state.stats.seen++;
     state.stats.filtered++;
     broadcast({ event: "stats", data: state.stats });

     if (state.monitored.size >= MAX_MONITORED) {
       let oldest = null;
       for (const [, t] of state.monitored.entries()) {
         const age = Date.now() - t.detectedAt;
         if (!t.signal && age >= MIN_MONITOR_MS && (!oldest || t.detectedAt < oldest.detectedAt)) oldest = t;
       }
       if (oldest) stopMonitoring(oldest.mint);
       else {
         addLog(`⚠️ Cola llena, descartando ${coin.symbol}`, "warn");
         continue;
       }
     }

     seenMints.set(coin.mint, Date.now());
     startMonitoring({
       mint: coin.mint,
       name: coin.name || "Unknown",
       symbol: coin.symbol || "???",
       twitter, website, telegram,
       mc: mcUsd,
       price,
       detectedAt: Date.now(),
     });
   }
   if (nuevos === 0) addLog(`🔍 Sin tokens nuevos (<5min) en este ciclo`, "info");
 } catch (e) {
   addLog(`❌ Error pump.fun API: ${e.message}`, "error");
 }
}

setInterval(fetchNewPumpTokens, 10_000);
fetchNewPumpTokens();

// ── PUMP.FUN TRADE ─────────────────────────────────────────────
async function buyToken(mint, solAmount) {
 if (!wallet || !connection) return null;
 try {
   addLog(`💳 Comprando ${solAmount} SOL de ${shortAddr(mint)}...`, "real");
   const response = await fetch("https://pumpportal.fun/api/trade-local", {
     method: "POST",
     headers: { "Content-Type": "application/json" },
     body: JSON.stringify({
       publicKey: wallet.publicKey.toString(),
       action: "buy",
       mint,
       denominatedInSol: "true",
       amount: solAmount,
       slippage: 15,
       priorityFee: 0.0005,
       pool: "pump"
     }),
     signal: AbortSignal.timeout(10000),
   });
   if (!response.ok) {
     addLog(`❌ Error API compra: ${response.status}`, "error");
     return null;
   }
   const txData = await response.arrayBuffer();
   const tx = VersionedTransaction.deserialize(new Uint8Array(txData));
   tx.sign([wallet]);
   const signature = await connection.sendRawTransaction(tx.serialize(), {
     skipPreflight: false,
     preflightCommitment: "confirmed",
   });
   await connection.confirmTransaction(signature, "confirmed");
   addLog(`✅ COMPRA OK: ${shortAddr(mint)} | TX: ${signature}`, "real");
   return signature;
 } catch (e) {
   addLog(`❌ Error compra: ${e.message}`, "error");
   return null;
 }
}

async function sellToken(mint) {
 if (!wallet || !connection) return null;
 try {
   const tokenBalance = await getTokenBalance(mint);
   if (tokenBalance <= 0) {
     addLog(`⚠️ Sin tokens para vender: ${shortAddr(mint)}`, "warn");
     return null;
   }
   addLog(`💳 Vendiendo ${tokenBalance} tokens de ${shortAddr(mint)}...`, "real");
   const response = await fetch("https://pumpportal.fun/api/trade-local", {
     method: "POST",
     headers: { "Content-Type": "application/json" },
     body: JSON.stringify({
       publicKey: wallet.publicKey.toString(),
       action: "sell",
       mint,
       denominatedInSol: "false",
       amount: tokenBalance,
       slippage: 15,
       priorityFee: 0.0005,
       pool: "pump"
     }),
     signal: AbortSignal.timeout(10000),
   });
   if (!response.ok) {
     const errText = await response.text();
     addLog(`❌ Error API venta ${response.status}: ${errText}`, "error");
     return null;
   }
   const txData = await response.arrayBuffer();
   const tx = VersionedTransaction.deserialize(new Uint8Array(txData));
   tx.sign([wallet]);
   const signature = await connection.sendRawTransaction(tx.serialize(), {
     skipPreflight: false,
     preflightCommitment: "confirmed",
   });
   await connection.confirmTransaction(signature, "confirmed");
   addLog(`✅ VENTA OK: ${shortAddr(mint)} | ${tokenBalance} tokens | TX: ${signature}`, "real");
   return signature;
 } catch (e) {
   addLog(`❌ Error venta: ${e.message}`, "error");
   return null;
 }
}

// ── REAL TRADING ───────────────────────────────────────────────
async function openRealTrade(signal) {
 if (!wallet) return;
 if (state.realTrades.filter(t => t.status === "OPEN").length >= MAX_REAL_TRADES) {
   addLog(`⚠️ Máximo de trades reales alcanzado`, "warn");
   return;
 }
 const balance = await getWalletBalance();
 if (balance < SOL_PER_TRADE + 0.01) {
   addLog(`⚠️ Balance insuficiente: ${balance.toFixed(3)} SOL`, "warn");
   return;
 }
 const signature = await buyToken(signal.mint, SOL_PER_TRADE);
 if (!signature) return;
 const trade = {
   id: `real-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
   mint: signal.mint, symbol: signal.symbol, name: signal.name,
   zone: signal.zone, entryPrice: signal.price,
   tp: signal.tp, sl: signal.sl, initialSl: signal.sl,
   solAmount: SOL_PER_TRADE,
   buySignature: signature, sellSignature: null,
   openTime: Date.now(), closeTime: null, closePrice: null,
   result: null, pnlPct: null, pnlSol: null,
   maxGainPct: 0, maxLossPct: 0, currentPct: 0,
   trailingPhase: "INITIAL", trailingLevel: null,
   status: "OPEN",
   expiresAt: Date.now() + MAX_TRADE_DURATION_MS,
   sellRetries: 0,
 };
 state.realTrades.unshift(trade);
 if (state.realTrades.length > 200) state.realTrades.pop();
 state.stats.realOpen++;
 state.stats.walletBalance = await getWalletBalance();
 broadcast({ event: "newRealTrade", data: trade });
 broadcast({ event: "stats", data: state.stats });
 addLog(`🔴 REAL ABIERTA: ${signal.symbol} | ${SOL_PER_TRADE} SOL | TP +90% | SL -12%`, "real");
}

async function closeRealTrade(trade, price, reason) {
 if (trade.status !== "OPEN") return;
 trade.status = "CLOSING";
 const signature = await sellToken(trade.mint);
 if (!signature) {
   trade.sellRetries = (trade.sellRetries || 0) + 1;
   if (trade.sellRetries <= 3) {
     addLog(`⚠️ VENTA FALLIDA (intento ${trade.sellRetries}/3): ${trade.symbol} — reintentando en 15s`, "error");
     trade.status = "OPEN";
     setTimeout(() => closeRealTrade(trade, price, reason), 15000);
     return;
   } else {
     addLog(`🚨 VENTA FALLIDA 3 VECES: ${trade.symbol} — CIERRA MANUALMENTE EN PHANTOM`, "error");
     trade.status = "SELL_FAILED";
     broadcast({ event: "realTradeClosed", data: trade });
     return;
   }
 }
 trade.sellSignature = signature;
 trade.closePrice = price;
 trade.closeTime = Date.now();
 trade.status = "CLOSED";
 const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
 trade.pnlPct = +pnlPct.toFixed(2);
 trade.pnlSol = +(trade.solAmount * pnlPct / 100).toFixed(4);
 const durationSec = Math.round((trade.closeTime - trade.openTime) / 1000);
 if (reason === "TP" || (reason === "SL" && trade.pnlPct >= 0)) {
   trade.result = "WIN";
   state.stats.realWins++;
   state.stats.realPnL += trade.pnlPct;
   state.stats.realPnLSol += trade.pnlSol;
   addLog(`✅ REAL WIN: ${trade.symbol} +${trade.pnlPct}% (+${trade.pnlSol} SOL) en ${durationSec}s`, "realwin");
 } else if (reason === "SL") {
   trade.result = "LOSS";
   state.stats.realLosses++;
   state.stats.realPnL += trade.pnlPct;
   state.stats.realPnLSol += trade.pnlSol;
   addLog(`❌ REAL LOSS: ${trade.symbol} ${trade.pnlPct}% (${trade.pnlSol} SOL) en ${durationSec}s`, "realloss");
 } else {
   trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS";
   state.stats.realExpired++;
   state.stats.realPnL += trade.pnlPct;
   state.stats.realPnLSol += trade.pnlSol;
   addLog(`⏱️ REAL EXP: ${trade.symbol} ${trade.pnlPct > 0 ? "+" : ""}${trade.pnlPct}% (${trade.pnlSol} SOL)`, "real");
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
   updateTrailingStopReal(trade, price);
   if (price >= trade.tp) closeRealTrade(trade, price, "TP");
   else if (price <= trade.sl) closeRealTrade(trade, price, "SL");
   else if (now >= trade.expiresAt) closeRealTrade(trade, price, "EXPIRED");
   else broadcast({ event: "realTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
 }
}

function updateTrailingStopReal(trade, price) {
 const gainPct = (price - trade.entryPrice) / trade.entryPrice;
 if (trade.trailingPhase === "FOLLOWING") {
   const newSl = price * (1 - TRAILING_FOLLOW_PCT);
   if (newSl > trade.sl) trade.sl = +newSl.toFixed(10);
   return;
 }
 if (gainPct >= TRAILING_LOCK_AT) {
   const newSl = price * (1 - TRAILING_FOLLOW_PCT);
   trade.trailingPhase = "FOLLOWING";
   trade.sl = +Math.max(trade.sl, newSl).toFixed(10);
   addLog(`🔄 REAL FOLLOWING: ${trade.symbol} SL sigue precio -20%`, "real");
   return;
 }
 if (gainPct >= TRAILING_BREAKEVEN_AT && trade.trailingPhase === "INITIAL") {
   trade.trailingPhase = "BREAKEVEN";
   trade.sl = +trade.entryPrice.toFixed(10);
   addLog(`⚖️ REAL BREAKEVEN: ${trade.symbol} SL → entrada`, "real");
 }
}

setInterval(() => {
 const now = Date.now();
 for (const trade of state.realTrades) {
   if (trade.status !== "OPEN") continue;
   if (now >= trade.expiresAt) {
     const token = state.monitored.get(trade.mint);
     closeRealTrade(trade, token?.price || trade.entryPrice, "EXPIRED");
   }
 }
}, 30_000);

// ── DEMO TRADING ───────────────────────────────────────────────
function openDemoTrade(signal) {
 const trade = {
   id: `demo-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
   mint: signal.mint, symbol: signal.symbol, name: signal.name,
   zone: signal.zone, entryPrice: signal.price,
   tp: signal.tp, sl: signal.sl, initialSl: signal.sl,
   openTime: Date.now(), closeTime: null, closePrice: null,
   result: null, pnlPct: null,
   maxGainPct: 0, maxLossPct: 0, currentPct: 0,
   trailingPhase: "INITIAL", trailingLevel: null,
   status: "OPEN", expiresAt: Date.now() + MAX_TRADE_DURATION_MS,
 };
 state.demoTrades.unshift(trade);
 if (state.demoTrades.length > 500) state.demoTrades.pop();
 state.stats.demoOpen++;
 broadcast({ event: "newDemoTrade", data: trade });
 broadcast({ event: "stats", data: state.stats });
 addLog(`📝 DEMO: ${signal.symbol} @ ${signal.price.toExponential(3)} | TP +90% | SL -12%`, "demo");
 return trade;
}

function updateTrailingStop(trade, price) {
 const gainPct = (price - trade.entryPrice) / trade.entryPrice;
 if (trade.trailingPhase === "FOLLOWING") {
   const newSl = price * (1 - TRAILING_FOLLOW_PCT);
   if (newSl > trade.sl) {
     trade.sl = +newSl.toFixed(10);
     trade.trailingLevel = +((gainPct - TRAILING_FOLLOW_PCT) * 100).toFixed(1);
     broadcast({ event: "demoTradeUpdate", data: { id: trade.id, sl: trade.sl, trailingPhase: trade.trailingPhase, trailingLevel: trade.trailingLevel } });
   }
   return;
 }
 if (gainPct >= TRAILING_LOCK_AT) {
   const newSl = price * (1 - TRAILING_FOLLOW_PCT);
   trade.trailingPhase = "FOLLOWING";
   trade.sl = +Math.max(trade.sl, newSl).toFixed(10);
   trade.trailingLevel = +((gainPct - TRAILING_FOLLOW_PCT) * 100).toFixed(1);
   addLog(`🔄 FOLLOWING: ${trade.symbol} SL sigue precio -20%`, "trail");
   broadcast({ event: "demoTradeUpdate", data: { id: trade.id, sl: trade.sl, trailingPhase: trade.trailingPhase, trailingLevel: trade.trailingLevel } });
   return;
 }
 if (gainPct >= TRAILING_BREAKEVEN_AT && trade.trailingPhase === "INITIAL") {
   trade.trailingPhase = "BREAKEVEN";
   trade.sl = +trade.entryPrice.toFixed(10);
   trade.trailingLevel = 0;
   addLog(`⚖️ BREAKEVEN: ${trade.symbol} SL → entrada (0%)`, "trail");
   broadcast({ event: "demoTradeUpdate", data: { id: trade.id, sl: trade.sl, trailingPhase: trade.trailingPhase, trailingLevel: trade.trailingLevel } });
 }
}

function closeDemoTrade(trade, price, reason) {
 trade.closePrice = price;
 trade.closeTime = Date.now();
 trade.status = "CLOSED";
 const durationSec = Math.round((trade.closeTime - trade.openTime) / 1000);
 const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
 trade.pnlPct = +pnlPct.toFixed(2);
 if (reason === "TP") {
   trade.result = "WIN";
   state.stats.demoWins++;
   state.stats.demoPnL += (TP_PCT - 1) * 100;
   addLog(`✅ WIN [TP]: ${trade.symbol} +${((TP_PCT-1)*100).toFixed(0)}% en ${durationSec}s`, "win");
 } else if (reason === "SL") {
   state.stats.demoPnL += trade.pnlPct;
   if (trade.pnlPct >= 0) {
     trade.result = "WIN"; state.stats.demoWins++;
     addLog(`✅ WIN [${trade.trailingPhase}]: ${trade.symbol} +${trade.pnlPct}% en ${durationSec}s`, "win");
   } else {
     trade.result = "LOSS"; state.stats.demoLosses++;
     addLog(`❌ LOSS [${trade.trailingPhase}]: ${trade.symbol} ${trade.pnlPct}% en ${durationSec}s`, "loss");
   }
 } else {
   trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS";
   state.stats.demoExpired++;
   state.stats.demoPnL += trade.pnlPct;
   addLog(`⏱️ EXP: ${trade.symbol} ${trade.pnlPct > 0 ? "+" : ""}${trade.pnlPct}%`, "expire");
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
   updateTrailingStop(trade, price);
   if (price >= trade.tp) closeDemoTrade(trade, price, "TP");
   else if (price <= trade.sl) closeDemoTrade(trade, price, "SL");
   else if (now >= trade.expiresAt) closeDemoTrade(trade, price, "EXPIRED");
   else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, maxLossPct: trade.maxLossPct, sl: trade.sl, trailingPhase: trade.trailingPhase, trailingLevel: trade.trailingLevel } });
 }
}

setInterval(() => {
 const now = Date.now();
 for (const trade of state.demoTrades) {
   if (trade.status !== "OPEN") continue;
   if (now >= trade.expiresAt) {
     const token = state.monitored.get(trade.mint);
     closeDemoTrade(trade, token?.price || trade.entryPrice, "EXPIRED");
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
 token.priceHigh = Math.max(token.priceHigh || 0, price);
 token.priceLow = token.priceLow === 0 ? price : Math.min(token.priceLow, price);
 token.tradeCount = (token.tradeCount || 0) + 1;
 token.volumeUSD = (token.volumeUSD || 0) + volumeUSD;
 token.lastUpdate = now;
 token.mc = price * 1_000_000_000;
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
 updateRealTrades(mint, price);
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
     zone, price, tp, sl, time: Date.now(), status: "OPEN",
     tradeCount: token.tradeCount, volumeUSD: token.volumeUSD,
   };
   state.signals.unshift(signal);
   if (state.signals.length > 100) state.signals.pop();
   addLog(`🎯 SEÑAL ${zone} en ${token.symbol} @ ${price.toExponential(3)}`, "signal");
   broadcast({ event: "newSignal", data: signal });
   broadcast({ event: "stats", data: state.stats });
   openDemoTrade(signal);
   openRealTrade(signal);
 }
}

function startMonitoring(token) {
 if (state.monitored.has(token.mint)) return;
 const entry = {
   ...token, candles: [], currentCandle: null, bb: null,
   candleCount: 0, signal: null, lastUpdate: Date.now(), candles50: [],
   priceHigh: token.price || 0, priceLow: token.price || 0,
   tradeCount: 0, volumeUSD: 0, ticker: null,
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
 broadcast({ event: "removeToken", data: { mint } });
}

// ── HELIUS WEBSOCKET — solo precios ────────────────────────────
function connectHelius() {
 addLog("🔌 Conectando a Helius...", "info");
 broadcast({ event: "wsStatus", data: "connecting" });
 const ws = new WebSocket(HELIUS_WS);
 let pingInterval;
 ws.on("open", () => {
   addLog("✅ Helius conectado 🚀", "info");
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
     const result = msg?.params?.result;
     const tx = result?.transaction;
     if (!tx) return;
     const meta = tx.meta;
     if (!meta || meta.err) return;

     // Filtrar transacciones propias
     const accountKeys = tx.transaction?.message?.accountKeys || [];
     const walletPubkey = wallet?.publicKey?.toString();
     if (walletPubkey) {
       const isOwnTx = accountKeys.some(k => (k.pubkey || k) === walletPubkey);
       if (isOwnTx) return;
     }

     const tokenBalances = meta.postTokenBalances || [];
     if (tokenBalances.length === 0) return;
     const mint = tokenBalances[0]?.mint;
     if (!mint) return;

     // Solo actualizar precio si ya monitorizamos este token
     if (!state.monitored.has(mint)) return;

     const preSOL = meta.preBalances?.[0] || 0;
     const postSOL = meta.postBalances?.[0] || 0;
     const solDiff = Math.abs(postSOL - preSOL) / 1e9;
     let tokenDiff = 0;
     const preTokenBalances = meta.preTokenBalances || [];
     for (const post of tokenBalances) {
       const pre = preTokenBalances.find(p => p.accountIndex === post.accountIndex);
       const postAmt = parseFloat(post.uiTokenAmount?.uiAmount || 0);
       const preAmt = parseFloat(pre?.uiTokenAmount?.uiAmount || 0);
       if (Math.abs(postAmt - preAmt) > 0) { tokenDiff = Math.abs(postAmt - preAmt); break; }
     }
     if (solDiff === 0 || tokenDiff === 0) return;
     const price = (solDiff / tokenDiff) * solPriceUSD;
     const volumeUSD = solDiff * solPriceUSD;
     if (price <= 0) return;
     updateCandle(mint, price, volumeUSD);
   } catch {}
 });
 ws.on("error", (err) => {
   addLog(`❌ Error WS: ${err.message}`, "error");
   broadcast({ event: "wsStatus", data: "error" });
 });
 ws.on("close", () => {
   clearInterval(pingInterval);
   addLog("🔄 Reconectando en 5s...", "warn");
   broadcast({ event: "wsStatus", data: "disconnected" });
   for (const [, token] of state.monitored.entries()) { if (token.ticker) clearInterval(token.ticker); }
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
   realTrades: state.realTrades.slice(0, 200),
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
     demoTrades: state.demoTrades.slice(0, 200),
     realTrades: state.realTrades.slice(0, 200),
     log: state.log.slice(0, 100),
     stats: state.stats,
     wsStatus: "connected"
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
 console.log(`🚀 SolScanBot — Real Trading Activo`);
 initWallet();
 connectHelius();
});
