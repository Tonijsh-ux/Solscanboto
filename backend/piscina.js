// [20-oct] LA PISCINA CANÓNICA DE PUMPSWAP DE UN TOKEN DE PUMP.FUN, calculada (sin llamar a nadie y sin librerías).
// Cuando pump.fun migra un token crea SIEMPRE la misma piscina en PumpSwap: la dirección derivada (PDA) del programa de PumpSwap con
//   ["pool", índice 0 (u16, little endian), la autoridad de la piscina de pump.fun para ese token, el token, WSOL]
// y la autoridad = la PDA del programa de pump.fun con ["pool-authority", el token] (como canonicalPumpPoolPda de @pump-fun/pump-swap-sdk).
// Sirve para no fiarse de «el dueño con más tokens y WSOL» de una transacción: la piscina que la cámara «descubría en el 1er swap»
// a veces era otra (la autoridad de una piscina de Raydium o Meteora del mismo token, compartida por muchos tokens, o una cartera),
// y entonces se grababa un precio que no era el de PumpSwap y la orden real no encontraba la piscina («sin piscina» → Jupiter).
import crypto from "crypto";

const ALFABETO = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
export function b58decode(s) {
  let n = 0n; for (const ch of s) { const i = ALFABETO.indexOf(ch); if (i < 0) throw new Error("base58 no válido"); n = n * 58n + BigInt(i); }
  const out = []; while (n > 0n) { out.push(Number(n & 0xffn)); n >>= 8n; }
  for (const ch of s) { if (ch === "1") out.push(0); else break; }
  return Buffer.from(out.reverse());
}
export function b58encode(buf) {
  let n = 0n; for (const b of buf) n = (n << 8n) + BigInt(b);
  let s = ""; while (n > 0n) { s = ALFABETO[Number(n % 58n)] + s; n /= 58n; }
  for (const b of buf) { if (b === 0) s = "1" + s; else break; }
  return s;
}
// ¿son esos 32 bytes un punto de la curva ed25519? (una PDA tiene que NO serlo)
const P = 2n ** 255n - 19n, D = (-121665n * inv(121666n)) % P;
function pot(b, e) { let r = 1n; b %= P; if (b < 0n) b += P; while (e > 0n) { if (e & 1n) r = r * b % P; b = b * b % P; e >>= 1n; } return r; }
function inv(x) { return pot(x, P - 2n); }
function enCurva(bytes) {
  const b = Buffer.from(bytes); b[31] &= 0x7f;   // y (el bit alto es el signo de x)
  let y = 0n; for (let i = 31; i >= 0; i--) y = (y << 8n) + BigInt(b[i]);
  if (y >= P) return false;
  const y2 = y * y % P, u = (y2 - 1n + P) % P, v = (D * y2 + 1n) % P;
  const x2 = u * inv(v) % P; if (x2 === 0n) return true;
  return pot(x2, (P - 1n) / 2n) === 1n;   // tiene raíz cuadrada → hay x → está en la curva
}
function pda(semillas, programa) {
  const prog = b58decode(programa);
  for (let bump = 255; bump >= 0; bump--) {
    const h = crypto.createHash("sha256"); for (const s of semillas) h.update(s); h.update(Buffer.from([bump])); h.update(prog); h.update(Buffer.from("ProgramDerivedAddress"));
    const d = h.digest(); if (!enCurva(d)) return b58encode(d);
  }
  throw new Error("sin PDA");
}
export const PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", PUMPSWAP = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", WSOL = "So11111111111111111111111111111111111111112";
const MEMO = new Map();
export function piscinaCanonica(mint) {
  if (MEMO.has(mint)) return MEMO.get(mint);
  let r = null;
  try { const m = b58decode(mint); if (m.length === 32) { const aut = b58decode(pda([Buffer.from("pool-authority"), m], PUMP));
      const idx = Buffer.alloc(2); idx.writeUInt16LE(0); r = pda([Buffer.from("pool"), idx, aut, m, b58decode(WSOL)], PUMPSWAP); } } catch { r = null; }
  if (MEMO.size > 50000) MEMO.clear(); MEMO.set(mint, r); return r;
}
