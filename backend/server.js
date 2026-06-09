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

// ── CONFIG GLOBAL ──────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
const SOL_PER_TRADE = 0.05;
const MAX_REAL_TRADES = 2; // 1 por estrategia máximo

// ── CONFIG MIGRACIÓN ───────────────────────────────────────────
const MIG_TP = 1.40;
const MIG_SL = 0.90;
const MIG_DURATION_MS = 10 * 60 * 1000;
const MIG_WINDOW_MS = 60_000;
const MIG_MIN_VOL = 10_000;
const MIG_MIN_MC = 50_000;
const MIG_MAX_MC = 2_000_000;
const MIG_BREAKEVEN_AT = 0.15;
const MIG_LOCK_AT = 0.25;
const MIG_FOLLOW_PCT = 0.12;

// ── CONFIG MOMENTUM ────────────────────────────────────────────
const MOM_TP = 1.30;
const MOM_SL = 0.92;
const MOM_DURATION_MS = 5 * 60 * 1000;
const MOM_MIN_PCT_5M = 15;    // +15% en 5 minutos mínimo
const MOM_MIN_VOL_5M = 50_000; // $50K volumen en 5 minutos
const MOM_MIN_MC = 100_000;
const MOM_MAX_MC = 1_000_000;
const MOM_SCAN_MS = 30_000;   // escanear cada 30s
const MOM_BREAKEVEN_AT = 0.10;
const MOM_LOCK_AT = 0.20;
const MOM_FOLLOW_PCT = 0.08;

// ── APIs ───────────────────────────────────────────────────────
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

// ── WALLET ─────────────────────────────────────────────────────
function initWallet() {
  try {
    const privateKeyStr = process.env.WALLET_PRIVATE_KEY;
    if (!privateKeyStr) { addLog("⚠️ Sin WALLET_PRIVATE_KEY — modo demo", "warn"); return; }
    const privateKeyBytes = bs58.decode(privateKeyStr);
    wallet = Keypair.fromSecretKey(privateKeyBytes);
    connection = new Connection(HELIUS_RPC, "confirmed");
    addLog(`✅ Wallet: ${wallet.publicKey.toString()}`, "info");
  } catch (e) { addLog(`❌ Wallet error: ${e.message}`, "error"); }
}

async function getWalletBalance() {
  if (!wallet || !connection) return 0;
  try { return (await connection.getBalance(wallet.publicKey)) / LAMPORTS_PER_SOL; } catch { return 0; }
}

async function getTokenBalance(mint) {
  if (!wallet || !connection) return 0;
  try {
    const accounts = await connection.getParsedTokenAccountsByOwner(wallet.publicKey, { mint: new PublicKey(mint) });
    if (!accounts.value.length) return 0;
    return accounts.value[0].account.data.parsed.info.tokenAmount.uiAmount || 0;
  } catch { return 0; }
}

// ── STATE ──────────────────────────────────────────────────────
const state = {
  // Migración
  migWatching: new Map(),
  migMonitored: new Map(),
  // Momentum
  momCandidates: new Map(),
  momMonitored: new Map(),
  // Compartido
  signals: [],
  demoTrades: [],
  realTrades: [],
  log: [],
  stats: {
    // Migración
    mig_migrations: 0, mig_watched: 0, mig_entered: 0, mig_rejected: 0,
    mig_demoWins: 0, mig_demoLosses: 0, mig_demoExpired: 0, mig_demoPnL: 0,
    mig_realWins: 0, mig_realLosses: 0, mig_realPnL: 0, mig_realPnLSol: 0,
    mig_closedCount: 0, mig_maxGainSum: 0, mig_maxLossSum: 0,
    mig_avgMaxGain: 0, mig_avgMaxLoss: 0,
    // Momentum
    mom_scanned: 0, mom_signals: 0, mom_rejected: 0,
    mom_demoWins: 0, mom_demoLosses: 0, mom_demoExpired: 0, mom_demoPnL: 0,
    mom_realWins: 0, mom_realLosses: 0, mom_realPnL: 0, mom_realPnLSol: 0,
    mom_closedCount: 0, mom_maxGainSum: 0, mom_maxLossSum: 0,
    mom_avgMaxGain: 0, mom_avgMaxLoss: 0,
    // Global
    demoOpen: 0, realOpen: 0, walletBalance: 0,
  },
};

const frontendClients = new Set();
const seenMigMints = new Set();
const seenMomPools = new Set();
const momSignalCooldown = new Map();

// ── LOG ────────────────────────────────────────────────────────
function addLog(msg, type = "info") {
  const entry = { msg, type, time: Date.now() };
  state.log.unshift(entry);
  if (state.log.length > 300) state.log.pop();
  broadcast({ event: "log", data: entry });
  console.log(`[${type.toUpperCase()}] ${msg}`);
}

function broadcast(payload) {
  const str = JSON.stringify(payload);
  for (const c of frontendClients) {
    if (c.readyState === WebSocket.OPEN) c.send(str);
  }
}

function shortAddr(a) { return a ? `${a.slice(0,4)}…${a.slice(-4)}` : "—"; }
function formatMC(n) {
  if (!n) return "$0";
  if (n >= 1_000_000) return `$${(n/1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `$${(n/1_000).toFixed(1)}K`;
  return `$${Math.round(n)}`;
}

let solPriceUSD = 150;
async function updateSolPrice() {
  try {
    const r = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd");
    const d = await r.json();
    solPriceUSD = d?.solana?.usd ?? 150;
  } catch {}
}
setInterval(updateSolPrice, 60_000);
updateSolPrice();

setInterval(async () => {
  if (wallet) { state.stats.walletBalance = await getWalletBalance(); broadcast({ event: "stats", data: state.stats }); }
}, 30_000);

// ════════════════════════════════════════════════════════════════
// ESTRATEGIA 1: SNIPER DE MIGRACIÓN
// ════════════════════════════════════════════════════════════════

function migStartWatching(coin) {
  if (seenMigMints.has(coin.mint)) return;
  seenMigMints.add(coin.mint);
  state.stats.mig_migrations++;
  const mcUsd = (coin.marketCapSol || 0) * solPriceUSD;
  if (mcUsd < MIG_MIN_MC || mcUsd > MIG_MAX_MC) {
    addLog(`⛔ MIG MC fuera rango (${formatMC(mcUsd)}): ${coin.symbol}`, "filter");
    broadcast({ event: "stats", data: state.stats });
    return;
  }
  const entry = {
    mint: coin.mint, name: coin.name || "Unknown", symbol: coin.symbol || "???",
    startTime: Date.now(), migratedMcUsd: mcUsd,
    volumeUSD: 0, tradeCount: 0, firstPrice: null, lastPrice: null, timer: null,
  };
  state.migWatching.set(coin.mint, entry);
  state.stats.mig_watched++;
  broadcast({ event: "stats", data: state.stats });
  addLog(`🌉 MIGRACIÓN: ${coin.symbol} | MC ${formatMC(mcUsd)} — ${MIG_WINDOW_MS/1000}s`, "accept");
  if (pumpPortalWs?.readyState === WebSocket.OPEN) {
    pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [coin.mint] }));
  }
  entry.timer = setTimeout(() => migEvaluate(coin.mint), MIG_WINDOW_MS);
}

function migUpdateWatching(mint, price, solAmount) {
  const entry = state.migWatching.get(mint);
  if (!entry) return;
  entry.volumeUSD += solAmount * solPriceUSD;
  entry.tradeCount++;
  entry.lastPrice = price;
  if (!entry.firstPrice && price > 0) entry.firstPrice = price;
  broadcast({ event: "migWatchUpdate", data: {
    mint, symbol: entry.symbol, volumeUSD: entry.volumeUSD,
    tradeCount: entry.tradeCount, needed: MIG_MIN_VOL,
    timeLeft: Math.max(0, MIG_WINDOW_MS - (Date.now() - entry.startTime)),
    mc: price * 1_000_000_000,
  }});
}

function migEvaluate(mint) {
  const entry = state.migWatching.get(mint);
  if (!entry) return;
  state.migWatching.delete(mint);
  const elapsed = ((Date.now() - entry.startTime) / 1000).toFixed(1);
  if (entry.volumeUSD >= MIG_MIN_VOL && entry.firstPrice) {
    addLog(`✅ MIG ENTRADA: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol | ${elapsed}s`, "accept");
    state.stats.mig_entered++;
    broadcast({ event: "stats", data: state.stats });
    migOpenTrades(entry);
  } else {
    addLog(`❌ MIG RECHAZADO: ${entry.symbol} | $${Math.round(entry.volumeUSD)} vol`, "filter");
    state.stats.mig_rejected++;
    broadcast({ event: "stats", data: state.stats });
  }
}

function migOpenTrades(entry) {
  const price = entry.firstPrice;
  const signal = {
    id: `mig-${entry.mint}-${Date.now()}`,
    strategy: "migration",
    mint: entry.mint, name: entry.name, symbol: entry.symbol,
    price, tp: +(price * MIG_TP).toFixed(12), sl: +(price * MIG_SL).toFixed(12),
    mcUsd: price * 1_000_000_000, volumeUSD: entry.volumeUSD, time: Date.now(),
  };
  state.signals.unshift(signal);
  if (state.signals.length > 100) state.signals.pop();
  broadcast({ event: "newSignal", data: signal });
  // Monitorizar
  if (!state.migMonitored.has(entry.mint)) {
    state.migMonitored.set(entry.mint, {
      mint: entry.mint, name: entry.name, symbol: entry.symbol,
      price, mc: price * 1_000_000_000,
      priceHigh: price, priceLow: price,
      tradeCount: entry.tradeCount, volumeUSD: entry.volumeUSD,
      detectedAt: entry.startTime, lastUpdate: Date.now(),
    });
    broadcast({ event: "newMigToken", data: state.migMonitored.get(entry.mint) });
  }
  openDemoTrade(signal);
  openRealTrade(signal);
}

function migUpdatePrice(mint, price, solAmount) {
  if (state.migWatching.has(mint)) { migUpdateWatching(mint, price, solAmount); return; }
  const token = state.migMonitored.get(mint);
  if (!token) return;
  token.price = price; token.mc = price * 1_000_000_000;
  token.priceHigh = Math.max(token.priceHigh, price);
  token.priceLow = Math.min(token.priceLow, price);
  token.tradeCount++; token.volumeUSD += solAmount * solPriceUSD;
  token.lastUpdate = Date.now();
  updateDemoTrades(mint, price, "migration");
  updateRealTrades(mint, price, "migration");
  broadcast({ event: "migTokenUpdate", data: token });
}

// ════════════════════════════════════════════════════════════════
// ESTRATEGIA 2: MOMENTUM PUMPSWAP
// ════════════════════════════════════════════════════════════════

async function momentumScan() {
  try {
    for (let page = 1; page <= 3; page++) {
      const res = await fetch(
        `${GECKO_PUMPSWAP}?page=${page}&order=h24_volume_usd_desc`,
        { headers: { "Accept": "application/json;version=20230302" }, signal: AbortSignal.timeout(10000) }
      );
      if (!res.ok) break;
      const json = await res.json();
      const pools = json?.data || [];
      for (const pool of pools) {
        const attr = pool.attributes || {};
        const poolAddr = attr.address || pool.id?.replace("solana_", "");
        if (!poolAddr || seenMomPools.has(poolAddr)) continue;
        const mc = parseFloat(attr.fdv_usd || 0);
        if (mc < MOM_MIN_MC || mc > MOM_MAX_MC) continue;
        const vol5m = parseFloat(attr.volume_usd?.m5 || 0);
        const pct5m = parseFloat(attr.price_change_percentage?.m5 || 0);
        if (vol5m < MOM_MIN_VOL_5M || pct5m < MOM_MIN_PCT_5M) continue;
        const price = parseFloat(attr.base_token_price_usd || 0);
        if (price <= 0) continue;
        const relationships = pool.relationships || {};
        const mint = (relationships.base_token?.data?.id || "").replace("solana_", "");
        if (!mint || mint.length < 32) continue;
        // Cooldown de señal por mint
        const lastSig = momSignalCooldown.get(mint) || 0;
        if (Date.now() - lastSig < 10 * 60 * 1000) continue;
        seenMomPools.add(poolAddr);
        state.stats.mom_scanned++;
        const symbol = (attr.name || "").split(" / ")[0] || mint.slice(0, 8);
        addLog(`⚡ MOMENTUM: ${symbol} | +${pct5m.toFixed(1)}% 5m | Vol ${formatMC(vol5m)} | MC ${formatMC(mc)}`, "signal");
        momSignalCooldown.set(mint, Date.now());
        state.stats.mom_signals++;
        // Monitorizar y abrir trade
        if (!state.momMonitored.has(mint)) {
          state.momMonitored.set(mint, {
            mint, symbol, name: symbol, mc, price,
            priceHigh: price, priceLow: price,
            pct5m, vol5m,
            tradeCount: 0, volumeUSD: 0,
            detectedAt: Date.now(), lastUpdate: Date.now(),
          });
          if (pumpPortalWs?.readyState === WebSocket.OPEN) {
            pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
          }
          broadcast({ event: "newMomToken", data: state.momMonitored.get(mint) });
        }
        const signal = {
          id: `mom-${mint}-${Date.now()}`,
          strategy: "momentum",
          mint, name: symbol, symbol,
          price, tp: +(price * MOM_TP).toFixed(12), sl: +(price * MOM_SL).toFixed(12),
          mcUsd: mc, vol5m, pct5m, time: Date.now(),
        };
        state.signals.unshift(signal);
        if (state.signals.length > 100) state.signals.pop();
        broadcast({ event: "newSignal", data: signal });
        openDemoTrade(signal);
        openRealTrade(signal);
      }
      await new Promise(r => setTimeout(r, 500));
    }
    broadcast({ event: "stats", data: state.stats });
  } catch (e) {
    addLog(`❌ Momentum scan error: ${e.message}`, "error");
  }
}

function momUpdatePrice(mint, price, solAmount) {
  const token = state.momMonitored.get(mint);
  if (!token) return;
  token.price = price; token.mc = price * 1_000_000_000;
  token.priceHigh = Math.max(token.priceHigh, price);
  token.priceLow = Math.min(token.priceLow, price);
  token.tradeCount++; token.volumeUSD += solAmount * solPriceUSD;
  token.lastUpdate = Date.now();
  updateDemoTrades(mint, price, "momentum");
  updateRealTrades(mint, price, "momentum");
  broadcast({ event: "momTokenUpdate", data: token });
}

// ════════════════════════════════════════════════════════════════
// TRADING COMPARTIDO
// ════════════════════════════════════════════════════════════════

async function buyToken(mint, solAmount) {
  if (!wallet || !connection) return null;
  try {
    const response = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "buy", mint, denominatedInSol: "true", amount: solAmount, slippage: 15, priorityFee: 0.0005, pool: "pump" }),
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) { addLog(`❌ Compra error: ${response.status}`, "error"); return null; }
    const tx = VersionedTransaction.deserialize(new Uint8Array(await response.arrayBuffer()));
    tx.sign([wallet]);
    const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: "confirmed" });
    await connection.confirmTransaction(sig, "confirmed");
    addLog(`✅ COMPRA: ${shortAddr(mint)} | ${sig}`, "real");
    return sig;
  } catch (e) { addLog(`❌ Compra: ${e.message}`, "error"); return null; }
}

async function sellToken(mint) {
  if (!wallet || !connection) return null;
  try {
    const bal = await getTokenBalance(mint);
    if (bal <= 0) { addLog(`⚠️ Sin tokens: ${shortAddr(mint)}`, "warn"); return null; }
    const response = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ publicKey: wallet.publicKey.toString(), action: "sell", mint, denominatedInSol: "false", amount: bal, slippage: 15, priorityFee: 0.0005, pool: "pump" }),
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) { addLog(`❌ Venta error: ${response.status}`, "error"); return null; }
    const tx = VersionedTransaction.deserialize(new Uint8Array(await response.arrayBuffer()));
    tx.sign([wallet]);
    const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, preflightCommitment: "confirmed" });
    await connection.confirmTransaction(sig, "confirmed");
    addLog(`✅ VENTA: ${shortAddr(mint)} | ${sig}`, "real");
    return sig;
  } catch (e) { addLog(`❌ Venta: ${e.message}`, "error"); return null; }
}

async function openRealTrade(signal) {
  if (!wallet) return;
  const openReal = state.realTrades.filter(t => t.status === "OPEN");
  // Max 1 real por estrategia
  const stratOpen = openReal.filter(t => t.strategy === signal.strategy).length;
  if (stratOpen >= 1) { addLog(`⚠️ Ya hay real abierta (${signal.strategy})`, "warn"); return; }
  if (openReal.length >= MAX_REAL_TRADES) { addLog(`⚠️ Máximo reales alcanzado`, "warn"); return; }
  const balance = await getWalletBalance();
  if (balance < SOL_PER_TRADE + 0.01) { addLog(`⚠️ Balance insuficiente: ${balance.toFixed(3)} SOL`, "warn"); return; }
  const sig = await buyToken(signal.mint, SOL_PER_TRADE);
  if (!sig) return;
  const tp = signal.strategy === "migration" ? signal.tp : signal.tp;
  const sl = signal.strategy === "migration" ? signal.sl : signal.sl;
  const duration = signal.strategy === "migration" ? MIG_DURATION_MS : MOM_DURATION_MS;
  const trade = {
    id: `real-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    strategy: signal.strategy, mint: signal.mint, symbol: signal.symbol, name: signal.name,
    entryPrice: signal.price, tp, sl, solAmount: SOL_PER_TRADE,
    buySignature: sig, sellSignature: null,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, pnlSol: null,
    maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: "INITIAL", status: "OPEN",
    expiresAt: Date.now() + duration, sellRetries: 0,
  };
  state.realTrades.unshift(trade);
  if (state.realTrades.length > 200) state.realTrades.pop();
  state.stats.realOpen++;
  state.stats.walletBalance = await getWalletBalance();
  broadcast({ event: "newRealTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  addLog(`🔴 REAL [${signal.strategy.toUpperCase()}]: ${signal.symbol} | ${SOL_PER_TRADE} SOL`, "real");
}

async function closeRealTrade(trade, price, reason) {
  if (trade.status !== "OPEN") return;
  trade.status = "CLOSING";
  const sig = await sellToken(trade.mint);
  if (!sig) {
    trade.sellRetries = (trade.sellRetries || 0) + 1;
    if (trade.sellRetries <= 3) { trade.status = "OPEN"; setTimeout(() => closeRealTrade(trade, price, reason), 15000); return; }
    trade.status = "SELL_FAILED"; broadcast({ event: "realTradeClosed", data: trade }); return;
  }
  trade.sellSignature = sig; trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
  const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
  trade.pnlPct = +pnlPct.toFixed(2);
  trade.pnlSol = +(trade.solAmount * pnlPct / 100).toFixed(4);
  const dur = Math.round((trade.closeTime - trade.openTime) / 1000);
  const prefix = `real_${trade.strategy === "migration" ? "mig" : "mom"}`;
  if (reason === "TP" || (reason === "SL" && trade.pnlPct >= 0)) {
    trade.result = "WIN"; state.stats[`${prefix}_realWins`]++;
    state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
    addLog(`✅ REAL WIN [${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "realwin");
  } else if (reason === "SL") {
    trade.result = "LOSS"; state.stats[`${prefix}_realLosses`]++;
    state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
    addLog(`❌ REAL LOSS [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "realloss");
  } else {
    trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS"; state.stats[`${prefix}_realPnL`] += trade.pnlPct; state.stats[`${prefix}_realPnLSol`] += trade.pnlSol;
  }
  state.stats.realOpen = Math.max(0, state.stats.realOpen - 1);
  state.stats.walletBalance = await getWalletBalance();
  broadcast({ event: "realTradeClosed", data: trade });
  broadcast({ event: "stats", data: state.stats });
}

function updateRealTrades(mint, price, strategy) {
  const now = Date.now();
  const breakeven = strategy === "migration" ? MIG_BREAKEVEN_AT : MOM_BREAKEVEN_AT;
  const lock = strategy === "migration" ? MIG_LOCK_AT : MOM_LOCK_AT;
  const follow = strategy === "migration" ? MIG_FOLLOW_PCT : MOM_FOLLOW_PCT;
  for (const trade of state.realTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    const gainPct = (price - trade.entryPrice) / trade.entryPrice;
    if (trade.trailingPhase === "FOLLOWING") {
      const newSl = price * (1 - follow);
      if (newSl > trade.sl) trade.sl = +newSl.toFixed(12);
    } else if (gainPct >= lock) {
      trade.trailingPhase = "FOLLOWING";
      trade.sl = +Math.max(trade.sl, price * (1 - follow)).toFixed(12);
    } else if (gainPct >= breakeven && trade.trailingPhase === "INITIAL") {
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
    const token = state.migMonitored.get(trade.mint) || state.momMonitored.get(trade.mint);
    closeRealTrade(trade, token?.price || trade.entryPrice, "EXPIRED");
  }
}, 30_000);

function openDemoTrade(signal) {
  const duration = signal.strategy === "migration" ? MIG_DURATION_MS : MOM_DURATION_MS;
  const trade = {
    id: `demo-${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
    strategy: signal.strategy, mint: signal.mint, symbol: signal.symbol, name: signal.name,
    entryPrice: signal.price, tp: signal.tp, sl: signal.sl,
    openTime: Date.now(), closeTime: null, closePrice: null,
    result: null, pnlPct: null, maxGainPct: 0, maxLossPct: 0, currentPct: 0,
    trailingPhase: "INITIAL", status: "OPEN",
    expiresAt: Date.now() + duration,
  };
  state.demoTrades.unshift(trade);
  if (state.demoTrades.length > 500) state.demoTrades.pop();
  state.stats.demoOpen++;
  broadcast({ event: "newDemoTrade", data: trade });
  broadcast({ event: "stats", data: state.stats });
  const tpPct = signal.strategy === "migration" ? "+40%" : "+30%";
  const slPct = signal.strategy === "migration" ? "-10%" : "-8%";
  addLog(`📝 DEMO [${signal.strategy}]: ${signal.symbol} | TP ${tpPct} SL ${slPct}`, "demo");
}

function updateDemoTrades(mint, price, strategy) {
  const now = Date.now();
  const tp_pct = strategy === "migration" ? MIG_TP : MOM_TP;
  const breakeven = strategy === "migration" ? MIG_BREAKEVEN_AT : MOM_BREAKEVEN_AT;
  const lock = strategy === "migration" ? MIG_LOCK_AT : MOM_LOCK_AT;
  const follow = strategy === "migration" ? MIG_FOLLOW_PCT : MOM_FOLLOW_PCT;
  for (const trade of state.demoTrades) {
    if (trade.mint !== mint || trade.status !== "OPEN" || trade.strategy !== strategy) continue;
    const currentPct = (price - trade.entryPrice) / trade.entryPrice * 100;
    trade.currentPct = +currentPct.toFixed(2);
    trade.maxGainPct = Math.max(trade.maxGainPct, currentPct);
    trade.maxLossPct = Math.min(trade.maxLossPct, currentPct);
    const gainPct = (price - trade.entryPrice) / trade.entryPrice;
    if (trade.trailingPhase === "FOLLOWING") {
      const newSl = price * (1 - follow);
      if (newSl > trade.sl) trade.sl = +newSl.toFixed(12);
    } else if (gainPct >= lock) {
      trade.trailingPhase = "FOLLOWING";
      trade.sl = +Math.max(trade.sl, price * (1 - follow)).toFixed(12);
      addLog(`🔄 FOLLOWING [${strategy}]: ${trade.symbol}`, "trail");
    } else if (gainPct >= breakeven && trade.trailingPhase === "INITIAL") {
      trade.trailingPhase = "BREAKEVEN"; trade.sl = +trade.entryPrice.toFixed(12);
      addLog(`⚖️ BREAKEVEN [${strategy}]: ${trade.symbol}`, "trail");
    }
    if (price >= trade.tp) closeDemoTrade(trade, price, "TP", tp_pct);
    else if (price <= trade.sl) closeDemoTrade(trade, price, "SL", tp_pct);
    else if (now >= trade.expiresAt) closeDemoTrade(trade, price, "EXPIRED", tp_pct);
    else broadcast({ event: "demoTradeUpdate", data: { id: trade.id, currentPct: trade.currentPct, maxGainPct: trade.maxGainPct, sl: trade.sl, trailingPhase: trade.trailingPhase } });
  }
}

function closeDemoTrade(trade, price, reason, tp_pct) {
  trade.closePrice = price; trade.closeTime = Date.now(); trade.status = "CLOSED";
  const dur = Math.round((trade.closeTime - trade.openTime) / 1000);
  const pnlPct = (price - trade.entryPrice) / trade.entryPrice * 100;
  trade.pnlPct = +pnlPct.toFixed(2);
  const prefix = trade.strategy === "migration" ? "mig" : "mom";
  if (reason === "TP") {
    trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++;
    state.stats[`${prefix}_demoPnL`] += (tp_pct - 1) * 100;
    addLog(`✅ WIN [TP][${trade.strategy}]: ${trade.symbol} +${((tp_pct-1)*100).toFixed(0)}% en ${dur}s`, "win");
  } else if (reason === "SL") {
    state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
    if (trade.pnlPct >= 0) { trade.result = "WIN"; state.stats[`${prefix}_demoWins`]++; addLog(`✅ WIN [${trade.trailingPhase}][${trade.strategy}]: ${trade.symbol} +${trade.pnlPct}% en ${dur}s`, "win"); }
    else { trade.result = "LOSS"; state.stats[`${prefix}_demoLosses`]++; addLog(`❌ LOSS [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct}% en ${dur}s`, "loss"); }
  } else {
    trade.result = trade.pnlPct >= 0 ? "EXPIRED_WIN" : "EXPIRED_LOSS";
    state.stats[`${prefix}_demoExpired`]++; state.stats[`${prefix}_demoPnL`] += trade.pnlPct;
    addLog(`⏱️ EXP [${trade.strategy}]: ${trade.symbol} ${trade.pnlPct > 0 ? "+" : ""}${trade.pnlPct}%`, "expire");
  }
  state.stats.demoOpen = Math.max(0, state.stats.demoOpen - 1);
  state.stats[`${prefix}_maxGainSum`] += trade.maxGainPct || 0;
  state.stats[`${prefix}_maxLossSum`] += Math.abs(trade.maxLossPct || 0);
  state.stats[`${prefix}_closedCount`]++;
  state.stats[`${prefix}_avgMaxGain`] = +(state.stats[`${prefix}_maxGainSum`] / state.stats[`${prefix}_closedCount`]).toFixed(1);
  state.stats[`${prefix}_avgMaxLoss`] = +(state.stats[`${prefix}_maxLossSum`] / state.stats[`${prefix}_closedCount`]).toFixed(1);
  broadcast({ event: "demoTradeClosed", data: trade });
  broadcast({ event: "stats", data: state.stats });
}

setInterval(() => {
  const now = Date.now();
  for (const trade of state.demoTrades) {
    if (trade.status !== "OPEN" || now < trade.expiresAt) continue;
    const token = state.migMonitored.get(trade.mint) || state.momMonitored.get(trade.mint);
    const tp_pct = trade.strategy === "migration" ? MIG_TP : MOM_TP;
    closeDemoTrade(trade, token?.price || trade.entryPrice, "EXPIRED", tp_pct);
  }
}, 30_000);

// ── PUMPPORTAL WS ──────────────────────────────────────────────
function connectPumpPortal() {
  addLog("🔌 Conectando a PumpPortal...", "info");
  pumpPortalWs = new WebSocket(PUMPPORTAL_WS);
  pumpPortalWs.on("open", () => {
    addLog("✅ PumpPortal conectado", "info");
    pumpPortalWs.send(JSON.stringify({ method: "subscribeMigration" }));
    for (const [mint] of state.migWatching.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
    for (const [mint] of state.migMonitored.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
    for (const [mint] of state.momMonitored.entries()) pumpPortalWs.send(JSON.stringify({ method: "subscribeTokenTrade", keys: [mint] }));
  });
  pumpPortalWs.on("message", async (raw) => {
    try {
      const data = JSON.parse(raw.toString());
      if (data.message || data.errors) return;
      if (data.txType === "migrate" && data.mint) {
        addLog(`🌉 Migración: ${data.symbol || data.mint.slice(0,8)} mcSol:${data.marketCapSol?.toFixed(1)}`, "info");
        migStartWatching({ mint: data.mint, name: data.name || "Unknown", symbol: data.symbol || "???", marketCapSol: data.marketCapSol || 0 });
        return;
      }
      if ((data.txType === "buy" || data.txType === "sell") && data.mint) {
        const walletPubkey = wallet?.publicKey?.toString();
        if (walletPubkey && data.traderPublicKey === walletPubkey) return;
        const sol = data.solAmount || 0;
        const tok = data.tokenAmount || 0;
        if (tok > 0 && sol > 0) {
          const price = (sol / tok) * solPriceUSD;
          if (price > 0) {
            if (state.migWatching.has(data.mint) || state.migMonitored.has(data.mint)) migUpdatePrice(data.mint, price, sol);
            if (state.momMonitored.has(data.mint)) momUpdatePrice(data.mint, price, sol);
          }
        }
      }
    } catch (e) { console.log("PP:", e.message); }
  });
  pumpPortalWs.on("error", (err) => addLog(`❌ PumpPortal: ${err.message}`, "error"));
  pumpPortalWs.on("close", () => { addLog("🔄 PumpPortal reconectando...", "warn"); setTimeout(connectPumpPortal, 5000); });
}

// ── HELIUS WS ──────────────────────────────────────────────────
function connectHelius() {
  addLog("🔌 Conectando a Helius...", "info");
  const ws = new WebSocket(HELIUS_WS);
  let pingInterval;
  ws.on("open", () => {
    addLog("✅ Helius conectado 🚀", "info");
    ws.send(JSON.stringify({
      jsonrpc: "2.0", id: 420, method: "transactionSubscribe",
      params: [
        { accountInclude: [PUMPSWAP_PROGRAM, PUMPFUN_PROGRAM], failed: false },
        { commitment: "processed", encoding: "jsonParsed", transactionDetails: "full", maxSupportedTransactionVersion: 0 }
      ]
    }));
    pingInterval = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ jsonrpc: "2.0", id: 999, method: "ping" })); }, 20_000);
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
      const isMig = state.migWatching.has(mint) || state.migMonitored.has(mint);
      const isMom = state.momMonitored.has(mint);
      if (!isMig && !isMom) return;
      const solDiff = Math.abs((meta.preBalances?.[0] || 0) - (meta.postBalances?.[0] || 0)) / 1e9;
      let tokenDiff = 0;
      const pre = meta.preTokenBalances || [];
      for (const post of tokenBalances) {
        const p = pre.find(x => x.accountIndex === post.accountIndex);
        const diff = Math.abs(parseFloat(post.uiTokenAmount?.uiAmount || 0) - parseFloat(p?.uiTokenAmount?.uiAmount || 0));
        if (diff > 0) { tokenDiff = diff; break; }
      }
      if (solDiff === 0 || tokenDiff === 0) return;
      const price = (solDiff / tokenDiff) * solPriceUSD;
      if (price <= 0) return;
      if (isMig) migUpdatePrice(mint, price, solDiff);
      if (isMom) momUpdatePrice(mint, price, solDiff);
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
    migWatching: Array.from(state.migWatching.values()),
    migMonitored: Array.from(state.migMonitored.values()),
    momMonitored: Array.from(state.momMonitored.values()),
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
      migWatching: Array.from(state.migWatching.values()),
      migMonitored: Array.from(state.migMonitored.values()),
      momMonitored: Array.from(state.momMonitored.values()),
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
  console.log(`🚀 SolScanBot v5 — Migración + Momentum`);
  initWallet();
  connectPumpPortal();
  connectHelius();
  // Momentum scan cada 30s
  setTimeout(momentumScan, 5000);
  setInterval(momentumScan, MOM_SCAN_MS);
});
