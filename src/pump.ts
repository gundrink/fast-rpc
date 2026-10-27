// pump.fun bonding-curve + PumpSwap AMM swap stream and curve pricing (read-only).
// Low latency: decodes anchor events straight from processed-commitment logsSubscribe "Program data:" lines.
import { createHash } from "node:crypto";
import type { StreamHub, Entry } from "./stream.js";
import { b58encode, b58decode, findProgramAddress, isPubkey } from "./codec.js";
import { INVALID_PARAMS } from "./errors.js";

export const PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
export const PUMP_AMM_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
export const WSOL = "So11111111111111111111111111111111111111112";
/** Pyth SOL/USD price-feed account (pull oracle, sponsored feed) */
export const PYTH_SOL_USD = "7UVimffxr9ow1uXYxsr4LHAcV58mLzhmwaeKvJ1pjLiE";
/** PriceUpdateV2: disc8, write_authority32, verification_level(1 byte Full | 2 bytes Partial), feed_id32, price i64, conf u64, expo i32, publish_time i64 */
export interface LaunchRisk { creator: string | null; createSlot: number; creatorPct: number; bundleWallets: number; bundlePct: number; sameSlotBuys: number; earlyBuyers: number; sharedFunder?: { funder: string; wallets: number } | null; creatorFundedWallets?: number; score: number; final: boolean }
/** stablecoins priced via Pyth SOL/USD */
export const PUMP_TOKEN = "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn";
export const STABLES: Record<string, string> = { EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v: "USDC", Es9vMFrzaCERmJfrF4H2FYD4KCoNkY2NoAjW1dEYN6EM: "USDT", USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB: "USD1" };
/** SOL value of a swap: solAmount, or solEquivalent for non-SOL quotes */
export const solOf = (e: { solAmount: number; solEquivalent?: number | null }) => e.solEquivalent ?? e.solAmount;
export function decodePythPrice(b: Buffer) {
  if (b.length < 100) return null;
  const o = 40 + (b[40] === 0 ? 2 : 1) + 32;
  const price = Number(b.readBigInt64LE(o)), expo = b.readInt32LE(o + 16), publishTime = Number(b.readBigInt64LE(o + 20));
  return { price: price * 10 ** expo, conf: Number(b.readBigUInt64LE(o + 8)) * 10 ** expo, publishTime };
}
const disc = (s: string) => createHash("sha256").update(s).digest().subarray(0, 8).toString("hex");
export const PUMP_DISC = {
  trade: disc("event:TradeEvent"), create: disc("event:CreateEvent"), complete: disc("event:CompleteEvent"),
  ammBuy: disc("event:BuyEvent"), ammSell: disc("event:SellEvent"), curve: disc("account:BondingCurve"),
};
/** pump.fun constants: 1e9 total supply, 793.1M sellable on the curve, 6 decimals */
const TOKEN_DEC = 1e6, SOL = 1e9, INITIAL_REAL_TOKEN = 793_100_000_000_000n, TOTAL_SUPPLY = 1_000_000_000_000_000n;

class R { o = 8; constructor(public b: Buffer) {}
  pk() { const v = b58encode(this.b.subarray(this.o, this.o + 32)); this.o += 32; return v; }
  u64() { const v = this.b.readBigUInt64LE(this.o); this.o += 8; return v; }
  i64() { const v = this.b.readBigInt64LE(this.o); this.o += 8; return v; }
  bool() { return this.b[this.o++] === 1; }
}

export interface PumpSwapEvent {
  launchRisk?: LaunchRisk;
  signature: string; slot: number; blockTime: number | null; wallet: string; mint: string | null; side: "buy" | "sell";
  solAmount: number; tokenAmount: number; solAmountRaw: string; tokenAmountRaw: string;
  virtualSolReserves: string; virtualTokenReserves: string; price: number | null; create: boolean;
  program: "pump" | "pumpswap"; pool?: string; realTokenReserves?: string; progress?: number; marketCapSol?: number | null;
  commitment: "processed" | "confirmed"; source: "logs" | "balanceDiff"; tag?: string; pctOfPositionSold?: number | null;
  update?: "confirmed" | "position"; receivedAt: number; quoteNonSol?: boolean;
  /** price validity: only valid prices feed high/drawdown/alerts */
  priceValid: boolean; priceInvalidReason?: "nonSolQuote" | "nonSolPool" | "nonCanonicalPool" | "noPool" | "badReserves" | "outlier" | "fillPrice";
  /** "curve" = pump bonding curve, "amm" = canonical PumpSwap pool (migrated), "amm-other" = any other PumpSwap pool */
  venue: "curve" | "amm" | "amm-other"; priceSource: "curveEventReserves" | "poolEventReserves" | "balanceDiff";
  /** price excluded from high/drawdown (dust swap below minSolForHigh) */
  dust?: boolean; tokenDecimals?: number;
  /** non-SOL quote (buy_v2/sell_v2 curves, non-WSOL PumpSwap pools): amounts in the quote token + SOL conversion (null + reason if no on-chain route) */
  quoteMint?: string; quoteAmountRaw?: string; quoteAmount?: number | null; quoteDecimals?: number | null; priceQuote?: number | null;
  quoteSolRate?: number | null; solEquivalent?: number | null; priceSol?: number | null; solEquivalentSource?: string | null; solEquivalentReason?: string;
  quoteSource?: "event" | "balanceDiff"; /** fees in quote raw units (curve: fee + creator fee; pool: lp + protocol fee); quoteAmount is net of fees */ quoteFeeRaw?: string; solUsd?: number | null; priceUsd?: number | null; marketCapUsd?: number | null; complete?: boolean;
}

/** pump.fun TradeEvent (bonding curve). */
export function decodePumpTrade(b: Buffer) {
  const r = new R(b);
  const t = { mint: r.pk(), solAmount: r.u64(), tokenAmount: r.u64(), isBuy: r.bool(), user: r.pk(), timestamp: r.i64(),
    virtualSolReserves: r.u64(), virtualTokenReserves: r.u64(), realSolReserves: r.u64(), realTokenReserves: r.u64() };
  // v2 tail (after fee/creator fields and the ix_name string at 258): quote_mint, quote_amount, virtual_quote_reserves, real_quote_reserves
  let quote: { mint: string; amount: bigint; virtualReserves: bigint; realReserves: bigint; fee: bigint } | undefined;
  if (b.length >= 262) {
    const n = b.readUInt32LE(258), E = 262 + n;
    if (n > 0 && n < 64 && b.length >= E + 93 && /^[a-z_]+$/.test(b.subarray(262, E).toString("latin1"))) {
      const qm = b58encode(b.subarray(E + 37, E + 69));
      // fee (169) + creator_fee (217) are in quote units on v2/v3 curves
      if (qm !== "11111111111111111111111111111111") quote = { mint: qm, amount: b.readBigUInt64LE(E + 69), virtualReserves: b.readBigUInt64LE(E + 77), realReserves: b.readBigUInt64LE(E + 85), fee: b.readBigUInt64LE(169) + b.readBigUInt64LE(217) };
    }
  }
  return { ...t, quote };
}
/** PumpSwap BuyEvent/SellEvent: shared prefix layout up to pool + user. Reserves are pool reserves *before* the swap. */
export function decodeAmmEvent(b: Buffer, buy: boolean) {
  const r = new R(b);
  const timestamp = r.i64(), baseAmount = r.u64(), _limit = r.u64(), _ub = r.u64(), _uq = r.u64(), poolBase = r.u64(), poolQuote = r.u64(), quoteAmount = r.u64();
  const _lpBps = r.u64(), lpFee = r.u64(), _pBps = r.u64(), protocolFee = r.u64(), _q2 = r.u64(), userQuote = r.u64();
  const pool = r.pk(), user = r.pk();
  // reserves after: buy -> base out, quote in (quoteAmount incl. lp fee stays in pool); sell -> base in, quote out (+ protocol fee leaves)
  const baseAfter = buy ? poolBase - baseAmount : poolBase + baseAmount;
  const quoteAfter = buy ? poolQuote + quoteAmount + lpFee : poolQuote - quoteAmount + lpFee; void protocolFee;
  return { timestamp, buy, baseAmount, quoteAmount, userQuote, pool, user, poolBase, poolQuote, baseAfter, quoteAfter, fee: lpFee + protocolFee };
}
/** BondingCurve account. */
export function decodeBondingCurve(data: Buffer) {
  if (data.length < 49 || data.subarray(0, 8).toString("hex") !== PUMP_DISC.curve) throw new Error("not a pump BondingCurve account");
  const r = new R(data);
  const c = { virtualTokenReserves: r.u64(), virtualSolReserves: r.u64(), realTokenReserves: r.u64(), realSolReserves: r.u64(), tokenTotalSupply: r.u64(), complete: r.bool(),
    creator: data.length >= 81 ? r.pk() : null };
  return c;
}
export const curvePda = (mint: string) => findProgramAddress([Buffer.from("bonding-curve"), b58decode(mint)], PUMP_PROGRAM)[0];
export function curveMetrics(vSol: bigint, vTok: bigint, realTok: bigint, supply: bigint = TOTAL_SUPPLY) {
  const price = vTok > 0n ? (Number(vSol) / SOL) / (Number(vTok) / TOKEN_DEC) : null;
  const progress = Math.max(0, Math.min(100, Number((INITIAL_REAL_TOKEN - realTok) * 10_000n / INITIAL_REAL_TOKEN) / 100));
  return { price, marketCapSol: price == null ? null : price * Number(supply) / TOKEN_DEC, progress };
}

/** Decode every pump / pumpswap event in one tx's logs. */
export function parsePumpLogs(logs: string[]) {
  const out: ({ k: "pump"; t: ReturnType<typeof decodePumpTrade> } | { k: "amm"; t: ReturnType<typeof decodeAmmEvent> })[] = [];
  let create = false, complete = false; const completeMints: string[] = [];
  // only trust events emitted while pump / pumpswap is the executing program (other anchor programs reuse names like BuyEvent)
  const stack: string[] = [];
  for (const l of logs) {
    if (l.startsWith("Program ") && !l.startsWith("Program data: ") && !l.startsWith("Program log: ") && !l.startsWith("Program return: ")) {
      const w = l.split(" ");
      if (w[2] === "invoke") stack.push(w[1]); else if ((w[2] === "success" || w[2] === "failed" || w[2]?.startsWith("failed")) && stack[stack.length - 1] === w[1]) stack.pop();
      continue;
    }
    if (!l.startsWith("Program data: ")) continue;
    const cur = stack[stack.length - 1];
    if (cur !== undefined && cur !== PUMP_PROGRAM && cur !== PUMP_AMM_PROGRAM) continue;
    let b: Buffer; try { b = Buffer.from(l.slice(14), "base64"); } catch { continue; }
    if (b.length < 8) continue;
    const d = b.subarray(0, 8).toString("hex");
    try {
      const isPump = cur === undefined || cur === PUMP_PROGRAM, isAmm = cur === undefined || cur === PUMP_AMM_PROGRAM;
      if (isPump && d === PUMP_DISC.trade && b.length >= 8 + 32 + 17 + 32 + 40) out.push({ k: "pump", t: decodePumpTrade(b) });
      else if (isPump && d === PUMP_DISC.create) create = true;
      else if (isPump && d === PUMP_DISC.complete) { complete = true; if (b.length >= 8 + 64) completeMints.push(b58encode(b.subarray(40, 72))); }
      else if (isAmm && (d === PUMP_DISC.ammBuy || d === PUMP_DISC.ammSell) && b.length >= 8 + 14 * 8 + 64) out.push({ k: "amm", t: decodeAmmEvent(b, d === PUMP_DISC.ammBuy) });
    } catch {}
  }
  return { events: out, create, complete, completeMints, truncated: logs.some((l) => l.startsWith("Log truncated")) };
}

export interface PumpSubParams { wallets?: string[]; mints?: string[]; allSwaps?: boolean; tags?: Record<string, string>; confirmations?: boolean; position?: boolean;
  /** drawdown thresholds in pct (e.g. [10,20,30]); one alert per threshold per high, re-armed on a new high */
  drawdownAlerts?: number[]; /** emit {type:"rebound"} when price recovers this pct off the low after a drawdown alert */ reboundPct?: number;
  /** attach nearest support/resistance (`levels`) from candles; needs pump market enabled */ levels?: boolean;
  /** default true: after a tagged wallet trades a mint, also stream all swaps on that mint */ trackTaggedMints?: boolean;
  /** swaps below this SOL size don't move high/drawdown (default 0.001) */ minSolForHigh?: number;
  /** distinct tagged wallets buying the same mint within confluenceWindowSec (default 600) -> {type:"confluence"} at these counts */
  confluenceAlerts?: number[]; confluenceWindowSec?: number;
  /** default true: attach per-mint aggregates + per-wallet PnL (tracked mints only) */ stats?: boolean }
export function validatePumpParams(p: any): PumpSubParams {
  if (p == null) p = {};
  if (typeof p !== "object" || Array.isArray(p)) throw INVALID_PARAMS("pumpSwapSubscribe: params[0] must be an object");
  for (const k of ["wallets", "mints"] as const) if (p[k] !== undefined && (!Array.isArray(p[k]) || p[k].length > 5000 || !p[k].every(isPubkey))) throw INVALID_PARAMS(`pumpSwapSubscribe: ${k} must be an array of base58 pubkeys (max 5000)`);
  if (p.tags !== undefined && (typeof p.tags !== "object" || Array.isArray(p.tags) || !Object.values(p.tags).every((v) => typeof v === "string" && v.length <= 64))) throw INVALID_PARAMS("pumpSwapSubscribe: tags must be {wallet: string}");
  if (p.drawdownAlerts !== undefined && (!Array.isArray(p.drawdownAlerts) || p.drawdownAlerts.length > 20 || !p.drawdownAlerts.every((x: any) => typeof x === "number" && x > 0 && x < 100))) throw INVALID_PARAMS("pumpSwapSubscribe: drawdownAlerts must be up to 20 numbers in (0,100)");
  if (p.reboundPct !== undefined && !(typeof p.reboundPct === "number" && p.reboundPct > 0 && p.reboundPct <= 1000)) throw INVALID_PARAMS("pumpSwapSubscribe: reboundPct must be a number in (0,1000]");
  if (p.minSolForHigh !== undefined && !(typeof p.minSolForHigh === "number" && p.minSolForHigh >= 0)) throw INVALID_PARAMS("pumpSwapSubscribe: minSolForHigh must be >= 0");
  if (p.confluenceAlerts !== undefined && (!Array.isArray(p.confluenceAlerts) || p.confluenceAlerts.length > 20 || !p.confluenceAlerts.every((x: any) => Number.isInteger(x) && x >= 2))) throw INVALID_PARAMS("pumpSwapSubscribe: confluenceAlerts must be integers >= 2");
  if (p.confluenceWindowSec !== undefined && !(typeof p.confluenceWindowSec === "number" && p.confluenceWindowSec > 0 && p.confluenceWindowSec <= 86400)) throw INVALID_PARAMS("pumpSwapSubscribe: confluenceWindowSec in (0,86400]");
  if (!p.wallets?.length && !p.mints?.length && p.allSwaps !== true) throw INVALID_PARAMS("pumpSwapSubscribe: give wallets, mints, or allSwaps:true");
  return p;
}

type Rpc = (method: string, params: any[]) => Promise<any>;
type Basis = { high: number; fired: Set<number>; low?: number; rebounded?: boolean };
type Track = { sub: Basis; tag?: Basis };
type Agg = { buys: number; sells: number; buySol: number; sellSol: number; buyers: Set<string>; sellers: Set<string>; first: number; tagBuys: Map<string, { t: number; tag: string }>; conflFired: Set<number> };
type Pnl = { boughtSol: number; soldSol: number; boughtTok: number; soldTok: number; buys: number; sells: number };
type Listener = { p: PumpSubParams; w?: Set<string>; m?: Set<string>; cb: (e: any) => void; tr: Map<string, Track>; agg: Map<string, Agg>; pnl: Map<string, Pnl> };
/** per-wallet PnL on one mint from the swaps seen since subscribe (average-cost method) */
export function pnlView(x: Pnl, price: number | null) {
  const avg = x.boughtTok > 0 ? x.boughtSol / x.boughtTok : null;
  const hold = Math.max(0, x.boughtTok - x.soldTok);
  const realized = avg == null ? x.soldSol : x.soldSol - Math.min(x.soldTok, x.boughtTok) * avg;
  const unreal = avg != null && price != null ? hold * (price - avg) : null;
  return { boughtSol: x.boughtSol, soldSol: x.soldSol, buys: x.buys, sells: x.sells, avgEntryPrice: avg, holdingTokens: hold, realizedSol: realized, unrealizedSol: unreal,
    pnlPct: avg != null && x.boughtSol > 0 ? ((realized + (unreal ?? 0)) / x.boughtSol) * 100 : null };
}
export interface DrawdownEvent { type: "drawdown" | "rebound"; mint: string; pct: number; threshold?: number; high: number; low?: number; price: number; basis: "subscribe" | "firstTag"; signature: string; slot: number; wallet: string; tag?: string; receivedAt: number }

/** Update one basis with a new price; returns alerts to emit. */
export function stepBasis(b: Basis, price: number, alerts: number[] = [], rebound?: number): { kind: "drawdown" | "rebound"; pct: number; threshold?: number; low?: number }[] {
  const out: { kind: "drawdown" | "rebound"; pct: number; threshold?: number; low?: number }[] = [];
  if (price > b.high) { b.high = price; b.fired.clear(); b.low = undefined; b.rebounded = false; return out; }
  const dd = b.high > 0 ? (1 - price / b.high) * 100 : 0;
  for (const t of [...alerts].sort((x, y) => x - y)) if (dd >= t && !b.fired.has(t)) { b.fired.add(t); out.push({ kind: "drawdown", pct: dd, threshold: t }); b.rebounded = false; }
  if (b.fired.size) {
    if (b.low === undefined || price < b.low) { b.low = price; b.rebounded = false; }
    else if (rebound && !b.rebounded && price >= b.low * (1 + rebound / 100)) { b.rebounded = true; out.push({ kind: "rebound", pct: (price / b.low - 1) * 100, low: b.low }); }
  }
  return out;
}
const ddPct = (b: Basis | undefined, price: number) => (b && b.high > 0 ? Math.max(0, (1 - price / b.high) * 100) : null);

export interface PoolInfo { base: string; quote: string; baseDec: number | null; quoteDec: number | null; canonical: boolean; creator?: string }

/** PumpSwap event -> swap event with explicit orientation (WSOL may be base or quote) and validity. */
export function ammEvent(t: ReturnType<typeof decodeAmmEvent>, pool: PoolInfo | null, sig: string, slot: number, at: number): PumpSwapEvent {
  const solSide = !pool ? null : pool.quote === WSOL ? "quote" : pool.base === WSOL ? "base" : null;
  const mint = !pool ? null : solSide === "quote" ? pool.base : solSide === "base" ? pool.quote : pool.base;
  const tokDec = !pool ? null : solSide === "base" ? pool.quoteDec : pool.baseDec;
  // amounts/reserves per side; buy/sell is from the token's perspective (buying the token for SOL)
  const solAmt = solSide === "base" ? t.baseAmount : t.quoteAmount, tokAmt = solSide === "base" ? t.quoteAmount : t.baseAmount;
  const solRes = solSide === "base" ? t.baseAfter : t.quoteAfter, tokRes = solSide === "base" ? t.quoteAfter : t.baseAfter;
  const side: "buy" | "sell" = solSide === "base" ? (t.buy ? "sell" : "buy") : (t.buy ? "buy" : "sell");
  let reason: PumpSwapEvent["priceInvalidReason"];
  if (!pool) reason = "noPool"; else if (!solSide) reason = "nonSolPool"; else if (tokDec == null || solRes <= 0n || tokRes <= 0n) reason = "badReserves"; else if (!pool.canonical) reason = "nonCanonicalPool";
  const td = 10 ** (tokDec ?? 6);
  const price = solSide && tokDec != null && tokRes > 0n ? (Number(solRes) / SOL) / (Number(tokRes) / td) : null;
  return { signature: sig, slot, blockTime: Number(t.timestamp), wallet: t.user, mint, side,
    solAmount: solSide ? Number(solAmt) / SOL : 0, tokenAmount: Number(tokAmt) / td, solAmountRaw: String(solSide ? solAmt : 0n), tokenAmountRaw: String(tokAmt),
    virtualSolReserves: String(solSide ? solRes : 0n), virtualTokenReserves: String(tokRes), price: solSide ? price : null,
    create: false, program: "pumpswap", pool: t.pool, marketCapSol: price == null || !solSide ? null : price * Number(TOTAL_SUPPLY) / TOKEN_DEC, progress: 100,
    commitment: "processed", source: "logs", receivedAt: at,
    venue: pool?.canonical ? "amm" : "amm-other", priceSource: "poolEventReserves", priceValid: !reason && price != null, ...(reason ? { priceInvalidReason: reason } : {}), ...(tokDec != null ? { tokenDecimals: tokDec } : {}) };
}

/** Shared engine: one processed logsSubscribe per program while any listener exists. */
export class PumpEngine {
  private ls = new Set<Listener>();
  private detach: (() => void)[] = [];
  private seen = new Map<string, number>();
  private pools = new Map<string, Promise<PoolInfo | null>>();
  private pos = new Map<string, bigint>(); // wallet:mint -> raw token balance (tracked)
  stats = { txs: 0, events: 0, truncated: 0, fallback: 0, outliers: 0 };
  /** SOL/USD from the Pyth pull-oracle feed account (on-chain, free), refreshed every 10 s while subscribed */
  solUsd: number | null = null; solUsdAt = 0; private pyth?: NodeJS.Timeout;
  async refreshSolUsd() {
    try {
      const a = await this.rpc("getAccountInfo", [PYTH_SOL_USD, { encoding: "base64", commitment: "confirmed" }]);
      const v = decodePythPrice(Buffer.from(a.value.data[0], "base64"));
      if (v && v.price > 1 && v.price < 100_000) { this.solUsd = v.price; this.solUsdAt = v.publishTime * 1000; }
    } catch {}
    return this.solUsd;
  }
  private migration(mint: string, sig: string, slot: number, at: number) {
    for (const l of this.ls) if (l.m?.has(mint) || l.tr.has(mint + "|curve") || l.p.allSwaps) try { l.cb({ type: "migration", mint, signature: sig, slot, receivedAt: at, note: "bonding curve complete; further prices come from venue amm" }); } catch {}
  }
  constructor(private hub: StreamHub, private rpc: Rpc, private opts: { commitment?: string; log?: (...a: any[]) => void; solUsd?: boolean } = {}) {}

  /** analytics hooks: see every swap event (after validity/USD enrichment) before listeners; may add fields */
  hooks: ((e: PumpSwapEvent) => void)[] = [];
  /** first tag any current subscriber gave this wallet */
  tagOf(wallet: string): string | undefined { for (const l of this.ls) { const t = l.p.tags?.[wallet]; if (t) return t; } return undefined; }
  /** wallets explicitly watched by current subscribers */
  watched(wallet: string) { for (const l of this.ls) if (l.w?.has(wallet)) return true; return false; }
  subscribe(p: PumpSubParams, cb: (e: PumpSwapEvent) => void) {
    const l: Listener = { p, cb, tr: new Map(), agg: new Map(), pnl: new Map(), w: p.wallets?.length ? new Set(p.wallets) : undefined, m: p.mints?.length ? new Set(p.mints) : undefined };
    this.ls.add(l); this.ensure();
    return () => { this.ls.delete(l); if (!this.ls.size) this.release(); };
  }
  private ensure() {
    if (this.detach.length) return;
    if (this.opts.solUsd !== false) { void this.refreshSolUsd(); this.pyth = setInterval(() => void this.refreshSolUsd(), 10_000); this.pyth.unref?.(); }
    for (const prog of [PUMP_PROGRAM, PUMP_AMM_PROGRAM])
      this.detach.push(this.hub.subscribe("logs", { mentions: prog, commitment: this.opts.commitment ?? "processed" }, { deliver: (e) => this.onLog(e) }).detach);
  }
  private release() { for (const d of this.detach) d(); this.detach = []; if (this.pyth) clearInterval(this.pyth); this.pyth = undefined; }

  private onLog(e: Entry) {
    const v = e.data; const sig = v?.signature; if (!sig || v.err) return;
    if (this.seen.has(sig)) return; this.seen.set(sig, Date.now());
    if (this.seen.size > 50_000) for (const k of [...this.seen.keys()].slice(0, 10_000)) this.seen.delete(k);
    this.stats.txs++;
    const at = Date.now();
    const r = parsePumpLogs(v.logs ?? []);
    if (!r.events.length) { if (r.truncated) { this.stats.truncated++; void this.fallback(sig, e.slot, at); } return; }
    for (const ev of r.events) {
      if (ev.k === "pump") {
        const t = ev.t; const m = curveMetrics(t.virtualSolReserves, t.virtualTokenReserves, t.realTokenReserves);
        // buy_v2/sell_v2 on non-SOL-quote curves log zero SOL fields: no SOL price from the event
        const nonSol = t.virtualSolReserves === 0n; if (nonSol) { m.price = null; m.marketCapSol = null; }
        const bad = !nonSol && (t.virtualTokenReserves === 0n || t.virtualTokenReserves < t.realTokenReserves);
        const se: PumpSwapEvent = ({ signature: sig, slot: e.slot, blockTime: Number(t.timestamp), wallet: t.user, mint: t.mint, side: t.isBuy ? "buy" : "sell",
          solAmount: Number(t.solAmount) / SOL, tokenAmount: Number(t.tokenAmount) / TOKEN_DEC, solAmountRaw: String(t.solAmount), tokenAmountRaw: String(t.tokenAmount),
          virtualSolReserves: String(t.virtualSolReserves), virtualTokenReserves: String(t.virtualTokenReserves), realTokenReserves: String(t.realTokenReserves),
          price: m.price, marketCapSol: m.marketCapSol, progress: m.progress, create: r.create, program: "pump", ...(nonSol ? { quoteNonSol: true } : {}), commitment: "processed", source: "logs", receivedAt: at,
          venue: "curve", priceSource: "curveEventReserves", priceValid: !nonSol && !bad && m.price != null, ...(nonSol ? { priceInvalidReason: "nonSolQuote" as const } : bad ? { priceInvalidReason: "badReserves" as const } : {}), tokenDecimals: 6,
          ...(r.completeMints.includes(t.mint) ? { complete: true } : {}) });
        if (nonSol) void this.curveQuote(se, t).then(() => this.emit(se)); else this.emit(se);
        if (r.completeMints.includes(t.mint)) this.migration(t.mint, sig, e.slot, at);
      } else {
        const t = ev.t;
        const base = (pool: Awaited<ReturnType<PumpEngine["pool"]>>) => {
          const ev = ammEvent(t, pool, sig, e.slot, at);
          if (pool && pool.quote !== WSOL && pool.base !== WSOL) {
            const pq = pool.quoteDec != null && pool.baseDec != null && t.baseAfter > 0n ? (Number(t.quoteAfter) / 10 ** pool.quoteDec) / (Number(t.baseAfter) / 10 ** pool.baseDec) : null;
            ev.quoteFeeRaw = String(t.fee);
            void this.withQuote(ev, pool.quote, t.quoteAmount, pool.quoteDec, pq, "event").then(() => this.emit(ev));
          } else this.emit(ev);
        };
        const cached = this.pools.get(t.pool);
        // fast path: wallet-only listeners don't need the mint; still resolve it, but synchronously when cached
        if (cached) void cached.then(base); else void this.pool(t.pool).then(base);
      }
    }
  }
  poolInfo(addr: string) { return this.pool(addr); }
  private pool(addr: string) {
    let p = this.pools.get(addr);
    if (!p) {
      p = (async () => {
        const a = await this.rpc("getAccountInfo", [addr, { encoding: "base64", commitment: "processed" }]);
        const d = a?.value?.data?.[0] ? Buffer.from(a.value.data[0], "base64") : null;
        if (!d || d.length < 107) return null;
        const base = b58encode(d.subarray(43, 75)), quote = b58encode(d.subarray(75, 107)), creator = b58encode(d.subarray(11, 43));
        // mint decimals at byte 44 for both SPL Token and Token-2022 (same base layout); null if unreadable -> price invalid
        const dec = async (mint: string) => mint === WSOL ? 9 : (await this.rpc("getAccountInfo", [mint, { encoding: "base64" }]).then((x) => { const b = Buffer.from(x.value.data[0], "base64"); return b.length >= 82 ? b[44] : null; }).catch(() => null));
        const token = quote === WSOL ? base : base === WSOL ? quote : null;
        // canonical = pool created by pump's migration (creator == pool-authority PDA of the token mint)
        const canonical = token != null && creator === findProgramAddress([Buffer.from("pool-authority"), b58decode(token)], PUMP_PROGRAM)[0];
        return { base, quote, baseDec: await dec(base), quoteDec: await dec(quote), canonical, creator };
      })().catch(() => { this.pools.delete(addr); return null; });
      this.pools.set(addr, p);
      if (this.pools.size > 20_000) this.pools.delete(this.pools.keys().next().value!);
    }
    return p;
  }
  private match(l: Listener, e: PumpSwapEvent) {
    if (l.w?.has(e.wallet)) return true;
    if (e.mint && l.m?.has(e.mint)) return true;
    return !l.w && !l.m && l.p.allSwaps === true;
  }
  private med = new Map<string, number[]>();
  /** reject a price that jumps >3x vs the venue's rolling median on a swap too small to move it (< 1% of SOL reserves) */
  private rej = new Map<string, number[]>();
  private guard(e: PumpSwapEvent) {
    if (!e.priceValid || e.price == null || !e.mint || e.update) return;
    const k = `${e.mint}|${e.venue}|${e.pool ?? ""}`; const h = this.med.get(k) ?? [];
    if (h.length >= 3) {
      const m = [...h].sort((a, b) => a - b)[h.length >> 1]; const r = e.price / m;
      const res = Number(e.virtualSolReserves) / SOL;
      if ((r > 3 || r < 1 / 3) && res > 0 && e.solAmount < res * 0.01) {
        // regime change: 3 consecutive rejects that agree with each other (within 1.5x) replace the history
        const rj = this.rej.get(k) ?? []; rj.push(e.price);
        if (rj.length >= 3 && Math.max(...rj) / Math.min(...rj) < 1.5) { this.rej.delete(k); this.med.set(k, rj); return; }
        if (rj.length >= 3) rj.shift(); this.rej.set(k, rj);
        e.priceValid = false; e.priceInvalidReason = "outlier"; this.stats.outliers++; return;
      }
      this.rej.delete(k);
    }
    h.push(e.price); if (h.length > 7) h.shift(); this.med.set(k, h);
    if (this.med.size > 50_000) this.med.delete(this.med.keys().next().value!);
    if (this.rej.size > 5_000) this.rej.delete(this.rej.keys().next().value!);
  }
  private emit(e: PumpSwapEvent) {
    this.stats.events++;
    if (e.priceValid === undefined) { e.priceValid = e.price != null; e.venue ??= e.program === "pump" ? "curve" : "amm-other"; e.priceSource ??= "balanceDiff"; }
    this.guard(e);
    if (solOf(e) < 0.000_01 && (e.priceValid || e.solEquivalent != null)) e.dust = true;
    if (this.solUsd) { e.solUsd = this.solUsd; e.priceUsd = e.priceValid && e.price != null ? e.price * this.solUsd : null; e.marketCapUsd = e.priceValid && e.marketCapSol != null ? e.marketCapSol * this.solUsd : null; }
    for (const h of this.hooks) try { h(e); } catch {}
    const ls = [...this.ls].filter((l) => this.match(l, e)); if (!ls.length) { this.track(e); return; }
    const key = `${e.wallet}:${e.mint}`; const raw = BigInt(e.tokenAmountRaw);
    // pctOfPositionSold from tracked balance when we already know it (sync), else follow-up
    let pct: number | null | undefined;
    if (e.side === "sell") { const b = this.pos.get(key); pct = b != null && b > 0n ? Math.min(1, Number(raw) / Number(b)) : null; }
    this.track(e);
    for (const l of ls) {
      const tag = l.p.tags?.[e.wallet];
      const extra: any = {}; const alerts: any[] = [];
      const tracked = !!e.mint && (l.m?.has(e.mint) || !!tag || l.tr.has(e.mint + "|curve") || l.tr.has(e.mint + "|amm"));
      if (e.mint && tag && l.p.trackTaggedMints !== false && !(l.m?.has(e.mint)) && (l.m?.size ?? 0) < 500) (l.m ??= new Set()).add(e.mint);
      // high/drawdown: valid prices only, per venue (curve vs canonical AMM), dust excluded
      const use = tracked && e.priceValid && e.price != null && e.price > 0 && e.venue !== "amm-other" && !e.dust && solOf(e) >= (l.p.minSolForHigh ?? 0.001);
      if (use) {
        const k = `${e.mint}|${e.venue}`; let t = l.tr.get(k);
        if (!t) { if (l.tr.size >= 5000) l.tr.delete(l.tr.keys().next().value!); t = { sub: { high: e.price!, fired: new Set() } }; l.tr.set(k, t); }
        if (tag && !t.tag) t.tag = { high: e.price!, fired: new Set() };
        for (const [basis, b] of [["subscribe", t.sub], ["firstTag", t.tag]] as const) {
          if (!b) continue;
          for (const a of stepBasis(b, e.price!, l.p.drawdownAlerts, l.p.reboundPct))
            alerts.push({ type: a.kind, mint: e.mint, venue: e.venue, pct: Math.round(a.pct * 100) / 100, ...(a.threshold != null ? { threshold: a.threshold } : {}), high: b.high, ...(a.low != null ? { low: a.low } : {}), price: e.price, ...(e.solUsd ? { priceUsd: e.price! * e.solUsd } : {}), basis, signature: e.signature, slot: e.slot, wallet: e.wallet, ...(tag ? { tag } : {}), receivedAt: e.receivedAt });
        }
      }
      if (tracked) {
        const t = l.tr.get(`${e.mint}|${e.venue}`); const px = e.priceValid ? e.price : null;
        Object.assign(extra, { highSinceSubscribe: t?.sub.high ?? null, drawdownPct: t && px ? ddPct(t.sub, px) : null, highSinceFirstTag: t?.tag?.high ?? null, drawdownSinceTagPct: t?.tag && px ? ddPct(t.tag, px) : null, trackVenue: e.venue });
        if (l.p.stats !== false && solOf(e) > 0) {
          let g = l.agg.get(e.mint!);
          if (!g) { if (l.agg.size >= 2000) l.agg.delete(l.agg.keys().next().value!); g = { buys: 0, sells: 0, buySol: 0, sellSol: 0, buyers: new Set(), sellers: new Set(), first: e.receivedAt, tagBuys: new Map(), conflFired: new Set() }; l.agg.set(e.mint!, g); }
          if (e.side === "buy") { g.buys++; g.buySol += solOf(e); if (g.buyers.size < 20000) g.buyers.add(e.wallet); } else { g.sells++; g.sellSol += solOf(e); if (g.sellers.size < 20000) g.sellers.add(e.wallet); }
          const win = (l.p.confluenceWindowSec ?? 600) * 1000;
          if (tag && e.side === "buy") g.tagBuys.set(e.wallet, { t: e.receivedAt, tag });
          for (const [w, v] of g.tagBuys) if (e.receivedAt - v.t > win) g.tagBuys.delete(w);
          const tags: Record<string, number> = {}; for (const v of g.tagBuys.values()) tags[v.tag] = (tags[v.tag] ?? 0) + 1;
          extra.mintStats = { buys: g.buys, sells: g.sells, buySol: g.buySol, sellSol: g.sellSol, netSol: g.buySol - g.sellSol, uniqueBuyers: g.buyers.size, uniqueSellers: g.sellers.size,
            buySellRatio: g.sellSol > 0 ? g.buySol / g.sellSol : null, sinceMs: e.receivedAt - g.first, taggedBuyersInWindow: g.tagBuys.size, taggedBuyersByTag: tags };
          if (tag && e.side === "buy") for (const th of l.p.confluenceAlerts ?? []) if (g.tagBuys.size >= th && !g.conflFired.has(th)) {
            g.conflFired.add(th); alerts.push({ type: "confluence", mint: e.mint, threshold: th, count: g.tagBuys.size, wallets: [...g.tagBuys.keys()], tags, price: px, signature: e.signature, slot: e.slot, receivedAt: e.receivedAt });
          }
          if (g.tagBuys.size < Math.min(...(l.p.confluenceAlerts ?? [Infinity]))) g.conflFired.clear(); // re-arm when the window empties
          if (tag || l.w?.has(e.wallet)) {
            const pk = `${e.wallet}|${e.mint}`; let x = l.pnl.get(pk);
            if (!x) { if (l.pnl.size >= 5000) l.pnl.delete(l.pnl.keys().next().value!); x = { boughtSol: 0, soldSol: 0, boughtTok: 0, soldTok: 0, buys: 0, sells: 0 }; l.pnl.set(pk, x); }
            if (e.side === "buy") { x.buys++; x.boughtSol += solOf(e); x.boughtTok += e.tokenAmount; } else { x.sells++; x.soldSol += solOf(e); x.soldTok += e.tokenAmount; }
            extra.walletPnl = pnlView(x, px);
          }
        }
      }
      try { l.cb({ type: "swap", ...e, ...(pct !== undefined ? { pctOfPositionSold: pct } : {}), ...(tag ? { tag } : {}), ...extra }); } catch {}
      for (const a of alerts) try { l.cb(a); } catch {}
    }
    // position lookups only for explicitly watched wallets (allSwaps would flood the upstream)
    const pl = ls.filter((l) => l.p.position !== false && l.w?.has(e.wallet));
    if (e.side === "sell" && pct == null && e.mint && pl.length) void this.positionFollowUp(e, pl);
    if (ls.some((l) => l.p.confirmations)) void this.confirmFollowUp(e, ls.filter((l) => l.p.confirmations));
  }
  private track(e: PumpSwapEvent) {
    if (!e.mint) return; const key = `${e.wallet}:${e.mint}`; const b = this.pos.get(key); if (b == null) return;
    const raw = BigInt(e.tokenAmountRaw); this.pos.set(key, e.side === "buy" ? b + raw : (b > raw ? b - raw : 0n));
  }
  /** after a sell at processed, the owner's balance is already post-sell: pre = post + sold */
  private async positionFollowUp(e: PumpSwapEvent, ls: Listener[]) {
    try {
      const r = await this.rpc("getTokenAccountsByOwner", [e.wallet, { mint: e.mint }, { encoding: "jsonParsed", commitment: "processed" }]);
      let post = 0n; for (const a of r?.value ?? []) post += BigInt(a.account?.data?.parsed?.info?.tokenAmount?.amount ?? 0);
      const pre = post + BigInt(e.tokenAmountRaw);
      this.pos.set(`${e.wallet}:${e.mint}`, post);
      const pct = pre > 0n ? Number(BigInt(e.tokenAmountRaw) * 10_000n / pre) / 10_000 : null;
      for (const l of ls) if (l.p.position !== false) try { l.cb({ type: "swap", ...e, pctOfPositionSold: pct, update: "position", receivedAt: Date.now(), ...(l.p.tags?.[e.wallet] ? { tag: l.p.tags[e.wallet] } : {}) }); } catch {}
    } catch {}
  }
  /** batched: one getSignatureStatuses per ~400ms for all pending sigs (max 256 per call) */
  private pend = new Map<string, { e: PumpSwapEvent; ls: Listener[]; tries: number }>();
  private ctimer?: NodeJS.Timeout;
  private confirmFollowUp(e: PumpSwapEvent, ls: Listener[]) {
    if (this.pend.size >= 5000) return;
    const cur = this.pend.get(e.signature + "|" + e.wallet + "|" + e.mint);
    if (!cur) this.pend.set(e.signature + "|" + e.wallet + "|" + e.mint, { e, ls, tries: 0 });
    this.ctimer ??= setTimeout(() => void this.flushConfirm(), 400);
  }
  private async flushConfirm() {
    this.ctimer = undefined;
    const items = [...this.pend.entries()];
    const sigs = [...new Set(items.map(([, v]) => v.e.signature))].slice(0, 256);
    try {
      const s = await this.rpc("getSignatureStatuses", [sigs]);
      const st = new Map(sigs.map((g, i) => [g, s?.value?.[i]]));
      for (const [k, v] of items) {
        if (!st.has(v.e.signature)) continue;
        const x: any = st.get(v.e.signature);
        if (x?.err || ++v.tries > 20) { this.pend.delete(k); continue; }
        if (x?.confirmationStatus === "confirmed" || x?.confirmationStatus === "finalized") {
          this.pend.delete(k);
          for (const l of v.ls) if (this.ls.has(l)) try { l.cb({ type: "swap", ...v.e, commitment: "confirmed", update: "confirmed", receivedAt: Date.now(), ...(l.p.tags?.[v.e.wallet] ? { tag: l.p.tags[v.e.wallet] } : {}) }); } catch {}
        }
      }
    } catch {}
    if (this.pend.size) this.ctimer ??= setTimeout(() => void this.flushConfirm(), 400);
  }
  /** logs truncated (very large tx): fall back to getTransaction balance diffs for the fee payer */
  private async fallback(sig: string, slot: number, at: number) {
    this.stats.fallback++;
    let tx: any;
    for (let i = 0; i < 4 && !tx; i++) { try { tx = await this.rpc("getTransaction", [sig, { encoding: "jsonParsed", commitment: "confirmed", maxSupportedTransactionVersion: 1 }]); } catch {} if (!tx) await new Promise((r) => setTimeout(r, 500 * (i + 1))); }
    for (const e of balanceDiffEvents(tx, sig, slot, at)) this.emit(e);
  }
  private decs = new Map<string, Promise<number | null>>();
  /** mint decimals (byte 44, SPL Token and Token-2022), cached */
  decimals(mint: string) {
    if (mint === WSOL) return Promise.resolve(9);
    let p = this.decs.get(mint);
    if (!p) { p = this.rpc("getAccountInfo", [mint, { encoding: "base64" }]).then((x) => { const b = Buffer.from(x.value.data[0], "base64"); return b.length >= 82 ? b[44] : null; }).catch(() => { this.decs.delete(mint); return null; }); this.decs.set(mint, p); if (this.decs.size > 20_000) this.decs.delete(this.decs.keys().next().value!); }
    return p;
  }
  private qcache = new Map<string, { at: number; ttl: number; v: Promise<{ sol: number | null; via: string | null; reason?: string }> }>();
  /** SOL per 1 ui unit of `mint`, from free on-chain sources: Pyth (stables), deepest PumpSwap mint/WSOL pool (>= 5 SOL), or a live SOL bonding curve */
  quoteSol(mint: string, depth = 0): Promise<{ sol: number | null; via: string | null; reason?: string }> {
    const c = this.qcache.get(mint); if (c && Date.now() - c.at < c.ttl) return c.v;
    const v = (async (): Promise<{ sol: number | null; via: string | null; reason?: string }> => {
      if (mint === WSOL) return { sol: 1, via: "wsol" };
      if (STABLES[mint]) { const u = this.solUsd ?? await this.refreshSolUsd(); return u ? { sol: 1 / u, via: `pyth:${STABLES[mint]}` } : { sol: null, via: null, reason: "noSolUsd" }; }
      const dec = await this.decimals(mint); if (dec == null) return { sol: null, via: null, reason: "unknownDecimals" };
      // deepest PumpSwap pool vs WSOL, else vs a leg with its own SOL route (stables via Pyth, PUMP); >= 5 SOL of leg-side liquidity
      let best: { sol: number; liq: number; pool: string; leg: string } | null = null;
      for (const leg of [WSOL, ...Object.keys(STABLES), PUMP_TOKEN]) {
        if (leg === mint) continue;
        const ls = leg === WSOL ? { sol: 1 } : depth === 0 ? await this.quoteSol(leg, 1) : { sol: null };
        if (ls.sol == null) continue;
        const legDec = leg === WSOL ? 9 : await this.decimals(leg); if (legDec == null) continue;
        const pools: string[] = [];
        for (const [x, y] of [[43, 75], [75, 43]]) {
          const r = await this.rpc("getProgramAccounts", [PUMP_AMM_PROGRAM, { encoding: "base64", dataSlice: { offset: 0, length: 0 }, filters: [{ memcmp: { offset: x, bytes: mint } }, { memcmp: { offset: y, bytes: leg } }] }]).catch(() => []);
          for (const p of r ?? []) pools.push(p.pubkey);
        }
        if (!pools.length) continue;
        const pa = await this.rpc("getMultipleAccounts", [pools.slice(0, 50), { encoding: "base64", dataSlice: { offset: 0, length: 203 } }]).catch(() => null);
        const meta = (pa?.value ?? []).map((x: any, i: number) => { const d = x ? Buffer.from(x.data[0], "base64") : null; return d && d.length >= 203 ? { pool: pools[i], baseIsLeg: b58encode(d.subarray(43, 75)) === leg, bv: b58encode(d.subarray(139, 171)), qv: b58encode(d.subarray(171, 203)) } : null; }).filter(Boolean) as any[];
        const va = meta.length ? await this.rpc("getMultipleAccounts", [meta.flatMap((m) => [m.bv, m.qv]), { encoding: "base64", dataSlice: { offset: 64, length: 8 } }]).catch(() => null) : null;
        meta.forEach((m, i) => {
          const amt = (j: number) => { const x = va?.value?.[j]; return x ? Number(Buffer.from(x.data[0], "base64").readBigUInt64LE(0)) : 0; };
          const b = amt(2 * i), q = amt(2 * i + 1); const legRaw = m.baseIsLeg ? b : q, tokRaw = m.baseIsLeg ? q : b;
          const liq = legRaw / 10 ** legDec * ls.sol!;
          if (liq >= 5 && tokRaw > 0) { const sol = (legRaw / 10 ** legDec) / (tokRaw / 10 ** dec) * ls.sol!; if (!best || liq > best.liq) best = { sol, liq, pool: m.pool, leg }; }
        });
        if (best) break;
      }
      if (best) { const bb = best as any; return { sol: bb.sol, via: `pumpswap:${bb.pool}${bb.leg === WSOL ? "" : `:via:${STABLES[bb.leg] ?? bb.leg}`}` }; }
      const cv = await this.curvePrice(mint).catch(() => null);
      if (cv && !cv.complete && cv.price) return { sol: cv.price, via: `curve:${cv.bondingCurve}` };
      return { sol: null, via: null, reason: "noSolRoute" };
    })().catch(() => ({ sol: null, via: null, reason: "lookupFailed" }));
    const ent = { at: Date.now(), ttl: 30_000, v }; this.qcache.set(mint, ent);
    void v.then((r) => { if (r.sol == null) ent.ttl = 900_000; });
    if (this.qcache.size > 5_000) this.qcache.delete(this.qcache.keys().next().value!);
    return v;
  }
  /** attach quote amount / decimals / SOL equivalent to a non-SOL-quote swap */
  async withQuote(e: PumpSwapEvent, qm: string, raw: bigint, qdec: number | null | undefined, priceQuote: number | null, src: "event" | "balanceDiff") {
    if (qdec == null) qdec = await this.decimals(qm);
    e.quoteMint = qm; e.quoteAmountRaw = String(raw); e.quoteDecimals = qdec; e.quoteAmount = qdec != null ? Number(raw) / 10 ** qdec : null; e.quoteSource = src;
    e.priceQuote = priceQuote;
    const r = await this.quoteSol(qm);
    e.quoteSolRate = r.sol; e.solEquivalentSource = r.via;
    e.solEquivalent = r.sol != null && e.quoteAmount != null ? e.quoteAmount * r.sol : null;
    e.priceSol = r.sol != null && priceQuote != null ? priceQuote * r.sol : null;
    if (e.solEquivalent == null) e.solEquivalentReason = r.reason ?? (qdec == null ? "unknownDecimals" : "unknown");
    if (this.solUsd && e.priceSol != null) e.priceUsd = null; // priceUsd stays reserved for valid SOL prices; use priceSol * solUsd
  }
  private async curveQuote(e: PumpSwapEvent, t: ReturnType<typeof decodePumpTrade>) {
    try {
      if (t.quote) {
        const qd = await this.decimals(t.quote.mint);
        const pq = qd != null && t.virtualTokenReserves > 0n ? (Number(t.quote.virtualReserves) / 10 ** qd) / (Number(t.virtualTokenReserves) / TOKEN_DEC) : null;
        await this.withQuote(e, t.quote.mint, t.quote.amount, qd, pq, "event"); e.quoteFeeRaw = String(t.quote.fee); return;
      }
      // event lacks the quote tail: user's token-balance diff (largest non-mint, non-SOL change) from the confirmed tx
      let tx: any;
      for (let i = 0; i < 3 && !tx; i++) { await new Promise((r) => setTimeout(r, 800 * (i + 1))); tx = await this.rpc("getTransaction", [e.signature, { encoding: "jsonParsed", commitment: "confirmed", maxSupportedTransactionVersion: 0 }]).catch(() => null); }
      const pre = new Map<number, any>((tx?.meta?.preTokenBalances ?? []).map((b: any) => [b.accountIndex, b]));
      let best: { mint: string; d: bigint; dec: number } | null = null;
      for (const b of tx?.meta?.postTokenBalances ?? []) {
        if (b.owner !== e.wallet || b.mint === e.mint || b.mint === WSOL) continue;
        const d = BigInt(b.uiTokenAmount.amount) - BigInt(pre.get(b.accountIndex)?.uiTokenAmount?.amount ?? 0); const a = d < 0n ? -d : d;
        if (a > 0n && (!best || a > (best.d < 0n ? -best.d : best.d))) best = { mint: b.mint, d, dec: b.uiTokenAmount.decimals };
      }
      if (best) await this.withQuote(e, best.mint, best.d < 0n ? -best.d : best.d, best.dec, null, "balanceDiff");
      else e.solEquivalentReason = "quoteNotFound";
    } catch { e.solEquivalentReason = "lookupFailed"; }
  }
  async curvePrice(mint: string) {
    if (!isPubkey(mint)) throw INVALID_PARAMS("pumpCurvePrice: mint must be a base58 pubkey");
    const pda = curvePda(mint);
    const a = await this.rpc("getAccountInfo", [pda, { encoding: "base64", commitment: "processed" }]);
    if (!a?.value) throw INVALID_PARAMS(`pumpCurvePrice: no bonding curve for ${mint}`);
    const c = decodeBondingCurve(Buffer.from(a.value.data[0], "base64"));
    const m = curveMetrics(c.virtualSolReserves, c.virtualTokenReserves, c.realTokenReserves, c.tokenTotalSupply);
    return { mint, bondingCurve: pda, slot: a.context?.slot, price: m.price, marketCapSol: m.marketCapSol, progress: c.complete ? 100 : m.progress, complete: c.complete,
      virtualSolReserves: String(c.virtualSolReserves), virtualTokenReserves: String(c.virtualTokenReserves), realSolReserves: String(c.realSolReserves), realTokenReserves: String(c.realTokenReserves),
      tokenTotalSupply: String(c.tokenTotalSupply), creator: c.creator };
  }
}

export function balanceDiffEvents(tx: any, sig: string, slot: number, at = Date.now()): PumpSwapEvent[] {
  if (!tx?.meta || tx.meta.err) return [];
  const keys = (tx.transaction?.message?.accountKeys ?? []).map((k: any) => (typeof k === "string" ? k : k.pubkey));
  const wallet = keys[0]; if (!wallet) return [];
  const sol = BigInt(tx.meta.preBalances[0]) - BigInt(tx.meta.postBalances[0]) - BigInt(tx.meta.fee ?? 0);
  const prog = keys.includes(PUMP_PROGRAM) || (tx.meta.logMessages ?? []).some((l: string) => l.includes(PUMP_PROGRAM)) ? "pump" : "pumpswap";
  const tok = new Map<string, bigint>();
  for (const b of tx.meta.postTokenBalances ?? []) if (b.owner === wallet && b.mint !== WSOL) tok.set(b.mint, (tok.get(b.mint) ?? 0n) + BigInt(b.uiTokenAmount.amount));
  for (const b of tx.meta.preTokenBalances ?? []) if (b.owner === wallet && b.mint !== WSOL) tok.set(b.mint, (tok.get(b.mint) ?? 0n) - BigInt(b.uiTokenAmount.amount));
  const out: PumpSwapEvent[] = [];
  for (const [mint, d] of tok) {
    if (d === 0n) continue; const buy = d > 0n; const ta = buy ? d : -d; const sa = sol < 0n ? -sol : sol;
    out.push({ signature: sig, slot, blockTime: tx.blockTime ?? null, wallet, mint, side: buy ? "buy" : "sell", solAmount: Number(sa) / SOL, tokenAmount: Number(ta) / TOKEN_DEC,
      solAmountRaw: String(sa), tokenAmountRaw: String(ta), virtualSolReserves: "", virtualTokenReserves: "", price: ta > 0n ? (Number(sa) / SOL) / (Number(ta) / TOKEN_DEC) : null,
      create: (tx.meta.logMessages ?? []).some((l: string) => l === "Program log: Instruction: Create"), program: prog, commitment: "confirmed", source: "balanceDiff", receivedAt: at,
      venue: prog === "pump" ? "curve" : "amm-other", priceSource: "balanceDiff", priceValid: false, priceInvalidReason: "fillPrice" });
  }
  return out;
}
