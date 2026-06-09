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

// ── CONFIG ─────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
const SOL_PER_TRADE = 0.05;
const MAX_REAL_TRADES = 1;
const TP_PCT = 1.40;        // +40%
const SL_PCT = 0.90;        // -10%
const MAX_TRADE_DURATION_MS = 10 * 60 * 1000; // 10 minutos
const TRAILING_BREAKEVEN_AT = 0.15; // breakeven a +15%
const TRAILING_LOCK_AT = 0.25;      // following a +25%
const TRAILING_FOLLOW_PCT = 0.12;   // SL sigue -12% desde máximo

// Ventana sniper post-migración
const SNIPER_WINDOW_MS = 60_000;       // 60 segundos
const SNIPER_MIN_VOL_USD = 10_000;     // $10K volumen mínimo en ventana
const SNIPER_MIN_MC_USD = 50_000;      // MC mínimo post-migración
const SNIPER_MAX_MC_USD = 2_000_000;   // MC máximo post-migración

const HELIUS_API_KEY = "86268796-07db-4bab-8e4f-abc4f697f64d";
const HELIUS_RPC = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const HELIUS_WS = `wss://atlas-mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPPORTAL_WS = "wss://pumpportal.fun/api/data";

let wallet = null;
let connection = null;
let pumpPortalWs = null;

// ── WALLET ─────────────────────────────────────────────────────
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

// ── STATE ──────────────────────────────────────────────────────
const state = {
 watching: new Map(),   // tokens en ventana sniper (60s post-migración)
 monitored: new Map(),  // tokens que pasaron filtro, monitorizando precio
 signals: [],
 demoTrades: [],
 realTrades: [],
 log: [],
 stats: {
   migrations: 0,     // migraciones detectadas
   watched: 0,        // entraron en ventana
   entered: 0,        // pasaron filtro de volumen
   rejected: 0,       // no pasaron filtro
   demoOpen: 0, demoWins: 0, demoLosses: 0, demoExpired: 0, demoPnL: 0,
   realOpen: 0, realWins: 0, realLosses: 0, realExpired: 0,
   realPnL: 0, realPnLSol: 0,
   avgMaxGain: 0, avgMaxLoss: 0, closedCount: 0,
   maxGainSum: 0, maxLossSum: 0,
   walletBalance: 0,
 },
};

const frontendClients = new Set();
const seenMints = new Set();

// ── LOGGING ────────────────────────────────────────────────────
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
   mint: t.mint, name: t.name, symbol: t.symbol,
   mc: t.mc, price: t.price,
   detectedAt: t.detectedAt, lastUpdate: t.lastUpdate,
   tradeCount: t.tradeCount, volumeUSD: t.volumeUSD,
   priceHigh: t.priceHigh, priceLow: t.priceLow,
 };
}

// ── SOL PRICE ──────────────────────────────────────────────────
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

// ── VENTANA SNIPER ─────────────────────────────────────────────
function startWatching(coin) {
 if (seenMints.has(coin.mint)) return;
 seenMints.add(coin.mint);
 state.stats.migrations++;

 // Filtro MC en migración
 const mcUsd = (coin.marketCapSol || 0) * solPriceUSD;
 if (mcUsd < SNIPER_MIN_MC_USD || mcUsd > SNIPER_MAX_MC_USD) {
   addLog(`⛔ MC fuera de rango (${formatMC(mcUsd)}): ${coin.symbol}`, "filter");
   broadcast({ event: "stats", data: state.stats });
   return;
 }

 const entry = {
   mint: coin.mint,
   name: coin.name || "Unknown",
   symbol: coin.symbol || "???",
   startTime: Date.now(),
   migratedMcUsd: mcUsd,
   migratedMcSol: coin.marketCapSol || 0,
   volumeUSD: 0,
   tradeCount: 0,
   firstPrice: null,
   lastPrice: null,
   timer: null,
 };

 state.watching.set(coin.mint, entry);
 state.stats.watched++;
 broadcast({ event: "stats", data: state.stats });
 addLog(`🚀 MIGRACIÓN: ${coin.symbol} | MC ${formatMC(mcUsd)} — observando ${SNIPER_WINDOW_MS/1000}s`, "accept");

 // Suscribir a trades en tiempo real
 if (pumpPortalWs?.readyState === WebSocket.OPEN) {
   pumpPortalWs.send(JSON.stringify({
     method: "subscribeTokenTrade",
     keys: [coin.mint]
   }));
 }

 // Evaluar al final de la ventana
 entry.timer = setTimeout(() => evaluateEntry(coin.mint), SNIPER_WINDOW_MS);
}

function updateWatching(mint, price, solAmount, tokenAmount) {
 const entry = state.watching.get(mint);
 if (!entry) return;

 const volumeUSD = solAmount * solPriceUSD;
 entry.volumeUSD += volumeUSD;
 entry.tradeCount++;
 entry.lastPrice = price;
 if (!entry.firstPrice && price > 0) entry.firstPrice = price;

 broadcast({ event: "watchUpdate", data: {
   mint,
   symbol: entry.symbol,
   volumeUSD: entry.volumeUSD,
   tradeCount: entry.tradeCount,
   needed: SNIPER_MIN_VOL_USD,
   timeLeft: Math.max(0, SNIPER_WINDOW_MS - (Date.now() - entry.startTime)),
   mc: price * 1_000_000_000,
 }});
}

function evaluateEntry(mint) {
 const entry = state.watching.get(mint);
 if (!entry) return;
 state.watching.delete(mint);
 const elapsed = ((Date.now() - entry.startTime) / 1000).toFixed(1);

 if (entry.volumeUSD >= SNIPER_MIN_VOL_USD && entry.firstPrice) {
   const mcAtEntry = entry.firstPrice * 1_000_000_000;
   addLog(`✅ SNIPER ENTRADA: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol | MC ${formatMC(mcAtEntry)} | ${elapsed}s`, "accept");
   state.stats.entered++;
   broadcast({ event: "stats", data: state.stats });
   openTrades(entry);
 } else {
   addLog(`❌ RECHAZADO: ${entry.symbol} | solo $${Math.round(entry.volumeUSD)} vol en ${elapsed}s`, "filter");
   state.stats.rejected++;
   broadcast({ event: "stats", data: state.stats });
 }
}

function openTrades(entry) {
 const price = entry.firstPrice;
 const tp = +(price * TP_PCT).toFixed(12);
 const sl = +(price * SL_PCT).toFixed(12);
 const mcUsd = price * 1_000_000_000;

 const signal = {
   id: `${entry.mint}-${Date.now()}`,
   mint: entry.mint, name: entry.name, symbol: entry.symbol,
   price, tp, sl, mcUsd,
   volumeUSD: entry.volumeUSD,
   time: Date.now(),
   strategy: "sniper",
 };

 state.signals.unshift(signal);
 if (state.signals.length > 100) state.signals.pop();
 broadcast({ event: "newSignal", data: signal });

 // Empezar monitorización
 startMonitoring(entry, price);
 openDemoTrade(signal);
 openRealTrade(signal);
}

// ── MONITORIZACIÓN ─────────────────────────────────────────────
function startMonitoring(entry, initialPrice) {
 if (state.monitored.has(entry.mint)) return;
 const token = {
   mint: entry.mint, name: entry.name, symbol: entry.symbol,
   price: initialPrice,
   mc: initialPrice * 1_000_000_000,
   priceHigh: initialPrice, priceLow: initialPrice,
   tradeCount: entry.tradeCount,
   volumeUSD: entry.volumeUSD,
   detectedAt: entry.startTime,
   lastUpdate: Date.now(),
 };
 state.monitored.set(entry.mint, token);
 broadcast({ event: "newToken", data: tokenToJSON(token) });
}

function updatePrice(mint, price, solAmount) {
 // Actualizar ventana sniper
 if (state.watching.has(mint)) {
   updateWatching(mint, price, solAmount, 0);
   return;
 }
 // Actualizar token monitorizando
 const token = state.monitored.get(mint);
 if (!token) return;
 const volumeUSD = solAmount * solPriceUSD;
 token.price = price;
 token.mc = price * 1_000_000_000;
 token.priceHigh = Math.max(token.priceHigh, price);
 token.priceLow = Math.min(token.priceLow, price);
 token.tradeCount++;
 token.volumeUSD += volumeUSD;
 token.lastUpdate = Date.now();
 updateDemoTrades(mint, price);
 updateRealTrades(mint, price);
 broadcast({ event: "tokenUpdate", data: tokenToJSON(token) });
}

// ── REAL TRADING ───────────────────────────────────────────────
async function buyToken(mint, solAmount) {
 if (!wallet || !connection) return null;
 try {
   addLog(`💳 Comprando ${solAmount} SOL de ${shortAddr(mint)}...`, "real");
   const response = await fetch("https://pumpportal.fun/api/trade-local", {
     method: "POST",
     headers: { "Content-Type": "application/json" },
     body: JSON.stringify({
       publicKey: wallet.publicKey.toString(),
       action: "buy", mint,
       denominatedInSol: "true", amount: solAmount,
       slippage: 15, priorityFee: 0.0005, pool: "pump"
     }),
     signal: AbortSignal.timeout(10000),
   });
   if (!response.ok) { addLog(`❌ Error compra: ${response.status}`, "error"); return null; }
   const txData = await response.arrayBuffer();
   const tx = VersionedTransaction.deserialize(new Uint8Array(txData));
   tx.sign([wallet]);
   const signature = await connection.sendRawTransaction(tx.serialize(), {
     skipPreflight: false, preflightCommitment: "confirmed"
   });
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
   addLog(`💳 Vendiendo ${tokenBalance} tokens de ${shortAddr(mint)}...`, "real");
   const response = await fetch("https://pumpportal.fun/api/trade-local", {
     method: "POST",
     headers: { "Content-Type": "application/json" },
     body: JSON.stringify({
       publicKey: wallet.publicKey.toString(),
       action: "sell", mint,
       denominatedInSol: "false", amount: tokenBalance,
       slippage: 15, priorityFee: 0.0005, pool: "pump"
     }),
     signal: AbortSignal.timeout(10000),
   });
   if (!response.ok) {
     const errText = await response.text();
     addLog(`❌ Error venta ${response.status}: ${errText}`, "error");
     return null;
   }
   const txData = await response.arrayBuffer();
   const tx = VersionedTransaction.deserialize(new Uint8Array(txData));
   tx.sign([wallet]);
   const signature = await connection.sendRawTransaction(tx.serialize(), {
     skipPreflight: false, preflightCommitment: "confirmed"
   });
   await connection.confirmTransaction(signature, "confirmed");
   addLog(`✅ VENTA OK: ${shortAddr(mint)} | TX: ${signature}`, "real");
   return signature;
 } catch (e) { addLog(`❌ Error venta: ${e.message}`, "error"); return null; }
}

async function openRealTrade(signal) {
 if (!wallet) return;
 if (state.realTrades.filter(t => t.status === "OPEN").length >= MAX_REAL_TRADES) {
   addLog(`⚠️ Máximo trades reales`, "warn"); return;
 }
 const balance = await getWalletBalance();
 if (balance < SOL_PER_TRADE + 0.01) {
   addLog(`⚠️ Balance insuficiente: ${balance.toFixed(3)} SOL`, "warn"); return;
 }
 const signature = await buyToken(signal.mint, SOL_PER_TRADE);
 if (!signature) return;
 const trade = {
   id: `real-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
   mint: signal.mint, symbol: signal.symbol, name: signal.name,
   strategy: "sniper",
   entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
   solAmount: SOL_PER_TRADE,
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
 addLog(`🔴 REAL: ${signal.symbol} | ${SOL_PER_TRADE} SOL | MC ${formatMC(signal.mcUsd)}`, "real");
}

async function closeRealTrade(trade, price, reason) {
 if (trade.status !== "OPEN") return;
 trade.status = "CLOSING";
 const signature = await sellToken(trade.mint);
 if (!signature) {
   trade.sellRetries = (trade.sellRetries || 0) + 1;
   if (trade.sellRetries <= 3) {
     addLog(`⚠️ VENTA FALLIDA (${trade.sellRetries}/3) — reintentando en 15s`, "error");
     trade.status = "OPEN";
     setTimeout(() => closeRealTrade(trade, price, reason), 15000);
     return;
   }
   addLog(`🚨 VENTA FALLIDA 3 VECES: ${trade.symbol} — CIERRA MANUALMENTE`, "error");
   trade.status = "SELL_FAILED";
   broadcast({ event: "realTradeClosed", data: trade });
   return;
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
   trade.result = "WIN"; state.stats.realWins++;
   state.stats.realPnL += trade.pnlPct; state.stats.realPnLSol += trade.pnlSol;
   addLog(`✅ REAL WIN: ${trade.symbol} +${trade.pnlPct}% (+${trade.pnlSol} SOL) en ${durationSec}s`, "realwin");
 } else if (reason === "SL") {
   trade.result = "LOSS"; state.stats.realLosses++;
   state.stats.realPnL += trade.pnlPct; state.stats.realPnLSol += trade.pnlSol;
   addLog(`❌ REAL LOSS: ${trade.symbol} ${trade.pnlPct}% (${trade.pnlSol} SOL) en ${durationSec}s`, "realloss");
 } else {
   trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS";
   state.stats.realExpired++;
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
     addLog(`🔄 REAL FOLLOWING: ${trade.symbol} SL sigue precio`, "real");
   } else if (gainPct >= TRAILING_BREAKEVEN_AT && trade.trailingPhase === "INITIAL") {
     trade.trailingPhase = "BREAKEVEN";
     trade.sl = +trade.entryPrice.toFixed(12);
     addLog(`⚖️ REAL BREAKEVEN: ${trade.symbol}`, "real");
   }
   if (price >= trade.tp) closeRealTrade(trade, price, "TP");
   else if (price <= trade.sl) closeRealTrade(trade, price, "SL");
   else if (now >= trade.expiresAt) closeRealTrade(trade, price, "EXPIRED");
   else broadcast({ event: "realTradeUpdate", data: {
     id: trade.id, currentPct: trade.currentPct,
     maxGainPct: trade.maxGainPct, sl: trade.sl,
     trailingPhase: trade.trailingPhase
   }});
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
   mint: signal.mint, symbol: signal.symbol, name: signal.name,
   strategy: "sniper",
   entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
   openTime: Date.now(), closeTime: null, closePrice: null,
   result: null, pnlPct: null,
   maxGainPct: 0, maxLossPct: 0, currentPct: 0,
   trailingPhase: "INITIAL", status: "OPEN",
   expiresAt: Date.now() + MAX_TRADE_DURATION_MS,
 };
 state.demoTrades.unshift(trade);
 if (state.demoTrades.length > 500) state.demoTrades.pop();
 state.stats.demoOpen++;
 broadcast({ event: "newDemoTrade", data: trade });
 broadcast({ event: "stats", data: state.stats });
 addLog(`📝 DEMO SNIPER: ${signal.symbol} | MC ${formatMC(signal.mcUsd)} | TP +40% SL -10%`, "demo");
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
     trade.trailingPhase = "BREAKEVEN";
     trade.sl = +trade.entryPrice.toFixed(12);
     addLog(`⚖️ BREAKEVEN: ${trade.symbol}`, "trail");
   }
   if (price >= trade.tp) closeDemoTrade(trade, price, "TP");
   else if (price <= trade.sl) closeDemoTrade(trade, price, "SL");
   else if (now >= trade.expiresAt) closeDemoTrade(trade, price, "EXPIRED");
   else broadcast({ event: "demoTradeUpdate", data: {
     id: trade.id, currentPct: trade.currentPct,
     maxGainPct: trade.maxGainPct, sl: trade.sl,
     trailingPhase: trade.trailingPhase
   }});
 }
}

function closeDemoTrade(trade, price, reason) {
 trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
 const durationSec = Math.round((trade.closeTime - trade.openTime) / 1000);
 const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
 trade.pnlPct = +pnlPct.toFixed(2);
 if (reason === "TP") {
   trade.result = "WIN"; state.stats.demoWins++;
   state.stats.demoPnL += (TP_PCT - 1) * 100;
   addLog(`✅ WIN [TP]: ${trade.symbol} +${((TP_PCT-1)*100).toFixed(0)}% en ${durationSec}s`, "win");
 } else if (reason === "SL") {
   state.stats.demoPnL += trade.pnlPct;
   if (trade.pnlPct >= 0) {
     trade.result = "WIN"; state.stats.demoWins++;
     addLog(`✅ WIN [${trade.trailingPhase}]: ${trade.symbol} +${trade.pnlPct}% en ${durationSec}s`, "win");
   } else {
     trade.result = "LOSS"; state.stats.demoLosses++;
     addLog(`❌ LOSS: ${trade.symbol} ${trade.pnlPct}% en ${durationSec}s`, "loss");
   }
 } else {
   trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS";
   state.stats.demoExpired++;
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
   // Suscribir a migraciones
   pumpPortalWs.send(JSON.stringify({ method: "subscribeMigration" }));
   // Re-suscribir a tokens ya en ventana
   for (const [mint] of state.watching.entries()) {
     pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
   }
   // Re-suscribir a tokens monitorizados
   for (const [mint] of state.monitored.entries()) {
     pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
   }
 });

 pumpPortalWs.on("message", async (raw) => {
   try {
     const data = JSON.parse(raw.toString());
     if (data.message || data.errors) return;

     // ── Evento de migración ────────────────────────────────
     if (data.txType === "migrate" && data.mint) {
       addLog(`🌉 Migración detectada: ${data.symbol || data.mint.slice(0,8)} | mcSol: ${data.marketCapSol?.toFixed(1)}`, "info");
       startWatching({
         mint: data.mint,
         name: data.name || "Unknown",
         symbol: data.symbol || "???",
         marketCapSol: data.marketCapSol || 0,
       });
       return;
     }

     // ── Trade — actualizar precio ──────────────────────────
     if ((data.txType === "buy" || data.txType === "sell") && data.mint) {
       const walletPubkey = wallet?.publicKey?.toString();
       if (walletPubkey && data.traderPublicKey === walletPubkey) return;
       const solAmount = data.solAmount || 0;
       const tokenAmount = data.tokenAmount || 0;
       if (tokenAmount > 0 && solAmount > 0) {
         const price = (solAmount / tokenAmount) * solPriceUSD;
         if (price > 0) updatePrice(data.mint, price, solAmount);
       }
     }
   } catch (e) {
     console.log("PP error:", e.message);
   }
 });

 pumpPortalWs.on("error", (err) => addLog(`❌ PumpPortal: ${err.message}`, "error"));
 pumpPortalWs.on("close", () => {
   addLog("🔄 PumpPortal reconectando...", "warn");
   setTimeout(connectPumpPortal, 5000);
 });
}

// ── HELIUS WS — precios backup ─────────────────────────────────
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
       { accountInclude: [PUMPSWAP_PROGRAM, PUMPFUN_PROGRAM], failed: false },
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
     if (!mint) return;
     if (!state.monitored.has(mint) && !state.watching.has(mint)) return;
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
     if (price > 0) updatePrice(mint, price, solDiff);
   } catch {}
 });

 ws.on("error", (err) => addLog(`❌ Helius: ${err.message}`, "error"));
 ws.on("close", () => {
   clearInterval(pingInterval);
   addLog("🔄 Helius reconectando...", "warn");
   setTimeout(connectHelius, 5000);
 });
}

// ── EXPRESS + WS ───────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

app.get("/api/state", (req, res) => {
 res.json({
   watching: Array.from(state.watching.values()).map(w => ({
     mint: w.mint, symbol: w.symbol, name: w.name,
     volumeUSD: w.volumeUSD, tradeCount: w.tradeCount,
     migratedMcUsd: w.migratedMcUsd,
     timeLeft: Math.max(0, SNIPER_WINDOW_MS - (Date.now() - w.startTime)),
   })),
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
     watching: Array.from(state.watching.values()).map(w => ({
       mint: w.mint, symbol: w.symbol, name: w.name,
       volumeUSD: w.volumeUSD, tradeCount: w.tradeCount,
       migratedMcUsd: w.migratedMcUsd,
       timeLeft: Math.max(0, SNIPER_WINDOW_MS - (Date.now() - w.startTime)),
     })),
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
 console.log(`🚀 SolScanBot v4 — Migration Sniper | TP +40% SL -10% | Ventana ${SNIPER_WINDOW_MS/1000}s`);
 initWallet();
 connectPumpPortal();
 connectHelius();
});
