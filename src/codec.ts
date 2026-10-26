// base58, compact-u16, ed25519 on-curve check, PDA derivation, Solana tx key decoding. No deps.
import { createHash } from "node:crypto";

const ALPHA = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const MAP = new Map([...ALPHA].map((c, i) => [c, i]));

export function b58encode(buf: Uint8Array): string {
  const digits: number[] = [];
  for (const byte of buf) {
    let carry = byte;
    for (let j = 0; j < digits.length; j++) { carry += digits[j] << 8; digits[j] = carry % 58; carry = (carry / 58) | 0; }
    while (carry) { digits.push(carry % 58); carry = (carry / 58) | 0; }
  }
  let out = "";
  for (let i = 0; i < buf.length && buf[i] === 0; i++) out += "1";
  for (let i = digits.length - 1; i >= 0; i--) out += ALPHA[digits[i]];
  return out;
}

export function b58decode(s: string): Buffer {
  if (typeof s !== "string") throw new Error("invalid base58");
  const bytes: number[] = [];
  for (const c of s) {
    const v = MAP.get(c); if (v === undefined) throw new Error("invalid base58 char");
    let carry = v;
    for (let j = 0; j < bytes.length; j++) { carry += bytes[j] * 58; bytes[j] = carry & 0xff; carry >>= 8; }
    while (carry) { bytes.push(carry & 0xff); carry >>= 8; }
  }
  for (let i = 0; i < s.length && s[i] === "1"; i++) bytes.push(0);
  return Buffer.from(bytes.reverse());
}

export const isPubkey = (s: any) => { try { return typeof s === "string" && b58decode(s).length === 32; } catch { return false; } };

// ---- ed25519 on-curve (for PDA derivation) ----
const P = (1n << 255n) - 19n;
const modp = (a: bigint) => ((a % P) + P) % P;
function powmod(b: bigint, e: bigint) { let r = 1n; b = modp(b); while (e > 0n) { if (e & 1n) r = (r * b) % P; b = (b * b) % P; e >>= 1n; } return r; }
const D = modp(-121665n * powmod(121666n, P - 2n));
export function isOnCurve(bytes: Uint8Array): boolean {
  const b = Buffer.from(bytes); b[31] &= 0x7f;
  let y = 0n; for (let i = 31; i >= 0; i--) y = (y << 8n) | BigInt(b[i]);
  if (y >= P) return false;
  const y2 = (y * y) % P;
  const u = modp(y2 - 1n), v = modp(D * y2 + 1n);
  const x2 = (u * powmod(v, P - 2n)) % P;
  return x2 === 0n || powmod(x2, (P - 1n) / 2n) === 1n;
}

export function findProgramAddress(seeds: Uint8Array[], programId: string): [string, number] {
  const pid = b58decode(programId);
  for (let bump = 255; bump >= 0; bump--) {
    const h = createHash("sha256");
    for (const s of seeds) h.update(s);
    h.update(Buffer.from([bump])); h.update(pid); h.update("ProgramDerivedAddress");
    const out = h.digest();
    if (!isOnCurve(out)) return [b58encode(out), bump];
  }
  throw new Error("no PDA found");
}

// ---- Solana wire transaction ----
/** highest tx version we decode and request from upstream (mainnet has v1 since Agave 4.x) */
export const MAX_TX_VERSION = Number(process.env.MAX_TX_VERSION ?? 1);
function readCompactU16(b: Buffer, o: number): [number, number] {
  let v = 0, s = 0, i = 0;
  for (;;) { if (o + i >= b.length) throw new Error("truncated"); const x = b[o + i++]; v |= (x & 0x7f) << s; if (!(x & 0x80)) break; s += 7; if (i > 3) throw new Error("bad compact-u16"); }
  return [v, o + i];
}

export interface DecodedTx { version: "legacy" | number; signatures: string[]; accountKeys: string[]; numRequiredSignatures: number; lookupTables: { account: string; writable: number[]; readonly: number[] }[]; instructions: { programIdIndex: number; accounts: number[]; data: Buffer }[]; recentBlockhash?: string; config?: TxV1Config }
/** v1 (SIMD-0385) in-message compute budget config */
export interface TxV1Config { priorityFee?: number; computeUnitLimit?: number; loadedAccountsDataSizeLimit?: number; heapSize?: number }

/** Decode a serialized (base64 or base58) legacy/v0 transaction: static keys + lookup table refs. */
export function decodeTransaction(tx: string, encoding?: "base64" | "base58"): DecodedTx {
  const b = encoding === "base58" ? b58decode(tx) : encoding === "base64" ? Buffer.from(tx, "base64")
    : /^[A-Za-z0-9+/=]+$/.test(tx) && /[+/=0OIl]/.test(tx) ? Buffer.from(tx, "base64") : (() => { try { return b58decode(tx); } catch { return Buffer.from(tx, "base64"); } })();
  if (b[0] === 0x81) return decodeV1(b);
  let [nsig, o] = readCompactU16(b, 0);
  if (nsig > 127 || o + nsig * 64 > b.length) throw new Error("bad signature section");
  const signatures: string[] = [];
  for (let i = 0; i < nsig; i++) { signatures.push(b58encode(b.subarray(o, o + 64))); o += 64; }
  let version: "legacy" | number = "legacy";
  if (b[o] & 0x80) { version = b[o] & 0x7f; o++; if (version !== 0) throw new Error(`unsupported tx version ${version}`); }
  if (o + 3 > b.length) throw new Error("truncated header");
  const numRequiredSignatures = b[o]; o += 3;
  let nkeys; [nkeys, o] = readCompactU16(b, o);
  if (o + nkeys * 32 > b.length) throw new Error("truncated keys");
  const accountKeys: string[] = [];
  for (let i = 0; i < nkeys; i++) { accountKeys.push(b58encode(b.subarray(o, o + 32))); o += 32; }
  o += 32; // recent blockhash
  let nix; [nix, o] = readCompactU16(b, o);
  const instructions: DecodedTx["instructions"] = [];
  for (let i = 0; i < nix; i++) {
    const programIdIndex = b[o++]; let n; [n, o] = readCompactU16(b, o); const accounts = [...b.subarray(o, o + n)]; o += n;
    [n, o] = readCompactU16(b, o); const data = Buffer.from(b.subarray(o, o + n)); o += n;
    instructions.push({ programIdIndex, accounts, data });
  }
  const lookupTables: DecodedTx["lookupTables"] = [];
  if (version === 0 && o < b.length) {
    let nl; [nl, o] = readCompactU16(b, o);
    for (let i = 0; i < nl; i++) {
      const account = b58encode(b.subarray(o, o + 32)); o += 32;
      let n; [n, o] = readCompactU16(b, o); const writable = [...b.subarray(o, o + n)]; o += n;
      [n, o] = readCompactU16(b, o); const readonly = [...b.subarray(o, o + n)]; o += n;
      lookupTables.push({ account, writable, readonly });
    }
  }
  if (o > b.length) throw new Error("truncated transaction");
  return { version, signatures, accountKeys, numRequiredSignatures, lookupTables, instructions };
}

/** v1 layout: 0x81 | header(3) | configMask u32 | blockhash | nIx u8 | nKeys u8 | keys | config u32 slots | ix headers (prog u8, nAcc u8, dataLen u16) | ix payloads | signatures */
function decodeV1(b: Buffer): DecodedTx {
  const need = (n: number) => { if (o + n > b.length) throw new Error("truncated v1 transaction"); };
  let o = 1; need(3 + 4 + 32 + 2);
  const numRequiredSignatures = b[o]; o += 3;
  const mask = b.readUInt32LE(o); o += 4;
  const recentBlockhash = b58encode(b.subarray(o, o + 32)); o += 32;
  const nix = b[o++], nkeys = b[o++]; need(nkeys * 32);
  const accountKeys: string[] = []; for (let i = 0; i < nkeys; i++) { accountKeys.push(b58encode(b.subarray(o, o + 32))); o += 32; }
  const slots: number[] = []; for (let i = 0; i < 32; i++) if (mask & (1 << i)) { need(4); slots[i] = b.readUInt32LE(o); o += 4; }
  const config: TxV1Config = {};
  if (mask & 3) config.priorityFee = (slots[0] ?? 0) + (slots[1] ?? 0) * 2 ** 32;
  if (mask & 4) config.computeUnitLimit = slots[2];
  if (mask & 8) config.loadedAccountsDataSizeLimit = slots[3];
  if (mask & 16) config.heapSize = slots[4];
  need(nix * 4); const hdrs: [number, number, number][] = [];
  for (let i = 0; i < nix; i++) { hdrs.push([b[o], b[o + 1], b.readUInt16LE(o + 2)]); o += 4; }
  const instructions: DecodedTx["instructions"] = [];
  for (const [programIdIndex, n, dl] of hdrs) { need(n + dl); const accounts = [...b.subarray(o, o + n)]; o += n; instructions.push({ programIdIndex, accounts, data: Buffer.from(b.subarray(o, o + dl)) }); o += dl; }
  need(numRequiredSignatures * 64);
  const signatures: string[] = []; for (let i = 0; i < numRequiredSignatures; i++) { signatures.push(b58encode(b.subarray(o, o + 64))); o += 64; }
  return { version: 1, signatures, accountKeys, numRequiredSignatures, lookupTables: [], instructions, recentBlockhash, config };
}

/** Test helper: build a minimal legacy tx (zero signatures) with given keys. */
export function buildLegacyTx(keys: string[], numSigners = 1, ix: { program: number; accounts: number[]; data: Uint8Array }[] = []): Buffer {
  const cu = (n: number) => { const out = []; do { let x = n & 0x7f; n >>= 7; if (n) x |= 0x80; out.push(x); } while (n); return Buffer.from(out); };
  const parts: Buffer[] = [cu(numSigners), Buffer.alloc(64 * numSigners), Buffer.from([numSigners, 0, 1]), cu(keys.length), ...keys.map(b58decode), Buffer.alloc(32, 1), cu(ix.length)];
  for (const i of ix) parts.push(Buffer.from([i.program]), cu(i.accounts.length), Buffer.from(i.accounts), cu(i.data.length), Buffer.from(i.data));
  return Buffer.concat(parts);
}
