// pump.fun bonding-curve + PumpSwap AMM swap stream and curve pricing (read-only).
// Low latency: decodes anchor events straight from processed-commitment logsSubscribe "Program data:" lines.
import { createHash } from "node:crypto";
import type { StreamHub, Entry } from "./stream.js";
import { b58encode, b58decode, findProgramAddress, isPubkey } from "./codec.js";
import { INVALID_PARAMS } from "./errors.js";

export const PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
export const PUMP_AMM_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
export const WSOL = "So11111111111111111111111111111111111111112";
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
  signature: string; slot: number; blockTime: number | null; wallet: string; mint: string | null; side: "buy" | "sell";
  solAmount: number; tokenAmount: number; solAmountRaw: string; tokenAmountRaw: string;
  virtualSolReserves: string; virtualTokenReserves: string; price: number | null; create: boolean;
  program: "pump" | "pumpswap"; pool?: string; realTokenReserves?: string; progress?: number; marketCapSol?: number | null;
  commitment: "processed" | "confirmed"; source: "logs" | "balanceDiff"; tag?: string; pctOfPositionSold?: number | null;
  update?: "confirmed" | "position"; receivedAt: number; quoteNonSol?: boolean;
}

/** pump.fun TradeEvent (bonding curve). */
export function decodePumpTrade(b: Buffer) {
  const r = new R(b);
  const t = { mint: r.pk(), solAmount: r.u64(), tokenAmount: r.u64(), isBuy: r.bool(), user: r.pk(), timestamp: r.i64(),
    virtualSolReserves: r.u64(), virtualTokenReserves: r.u64(), realSolReserves: r.u64(), realTokenReserves: r.u64() };
  return t;
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
  return { timestamp, buy, baseAmount, quoteAmount, userQuote, pool, user, poolBase, poolQuote, baseAfter, quoteAfter };
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
  let create = false, complete = false;
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
      else if (isPump && d === PUMP_DISC.complete) complete = true;
      else if (isAmm && (d === PUMP_DISC.ammBuy || d === PUMP_DISC.ammSell) && b.length >= 8 + 14 * 8 + 64) out.push({ k: "amm", t: decodeAmmEvent(b, d === PUMP_DISC.ammBuy) });
    } catch {}
  }
  return { events: out, create, complete, truncated: logs.some((l) => l.startsWith("Log truncated")) };
}

export interface PumpSubParams { wallets?: string[]; mints?: string[]; allSwaps?: boolean; tags?: Record<string, string>; confirmations?: boolean; position?: boolean;
  /** drawdown thresholds in pct (e.g. [10,20,30]); one alert per threshold per high, re-armed on a new high */
  drawdownAlerts?: number[]; /** emit {type:"rebound"} when price recovers this pct off the low after a drawdown alert */ reboundPct?: number;
  /** default true: after a tagged wallet trades a mint, also stream all swaps on that mint */ trackTaggedMints?: boolean }
export function validatePumpParams(p: any): PumpSubParams {
  if (p == null) p = {};
  if (typeof p !== "object" || Array.isArray(p)) throw INVALID_PARAMS("pumpSwapSubscribe: params[0] must be an object");
  for (const k of ["wallets", "mints"] as const) if (p[k] !== undefined && (!Array.isArray(p[k]) || p[k].length > 5000 || !p[k].every(isPubkey))) throw INVALID_PARAMS(`pumpSwapSubscribe: ${k} must be an array of base58 pubkeys (max 5000)`);
  if (p.tags !== undefined && (typeof p.tags !== "object" || Array.isArray(p.tags) || !Object.values(p.tags).every((v) => typeof v === "string" && v.length <= 64))) throw INVALID_PARAMS("pumpSwapSubscribe: tags must be {wallet: string}");
  if (p.drawdownAlerts !== undefined && (!Array.isArray(p.drawdownAlerts) || p.drawdownAlerts.length > 20 || !p.drawdownAlerts.every((x: any) => typeof x === "number" && x > 0 && x < 100))) throw INVALID_PARAMS("pumpSwapSubscribe: drawdownAlerts must be up to 20 numbers in (0,100)");
  if (p.reboundPct !== undefined && !(typeof p.reboundPct === "number" && p.reboundPct > 0 && p.reboundPct <= 1000)) throw INVALID_PARAMS("pumpSwapSubscribe: reboundPct must be a number in (0,1000]");
  if (!p.wallets?.length && !p.mints?.length && p.allSwaps !== true) throw INVALID_PARAMS("pumpSwapSubscribe: give wallets, mints, or allSwaps:true");
  return p;
}

type Rpc = (method: string, params: any[]) => Promise<any>;
type Basis = { high: number; fired: Set<number>; low?: number; rebounded?: boolean };
type Track = { sub: Basis; tag?: Basis };
type Listener = { p: PumpSubParams; w?: Set<string>; m?: Set<string>; cb: (e: any) => void; tr: Map<string, Track> };
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

/** Shared engine: one processed logsSubscribe per program while any listener exists. */
export class PumpEngine {
  private ls = new Set<Listener>();
  private detach: (() => void)[] = [];
  private seen = new Map<string, number>();
  private pools = new Map<string, Promise<{ base: string; quote: string; baseDec: number; quoteDec: number } | null>>();
  private pos = new Map<string, bigint>(); // wallet:mint -> raw token balance (tracked)
  stats = { txs: 0, events: 0, truncated: 0, fallback: 0 };
  constructor(private hub: StreamHub, private rpc: Rpc, private opts: { commitment?: string; log?: (...a: any[]) => void } = {}) {}

  subscribe(p: PumpSubParams, cb: (e: PumpSwapEvent) => void) {
    const l: Listener = { p, cb, tr: new Map(), w: p.wallets?.length ? new Set(p.wallets) : undefined, m: p.mints?.length ? new Set(p.mints) : undefined };
    this.ls.add(l); this.ensure();
    return () => { this.ls.delete(l); if (!this.ls.size) this.release(); };
  }
  private ensure() {
    if (this.detach.length) return;
    for (const prog of [PUMP_PROGRAM, PUMP_AMM_PROGRAM])
      this.detach.push(this.hub.subscribe("logs", { mentions: prog, commitment: this.opts.commitment ?? "processed" }, { deliver: (e) => this.onLog(e) }).detach);
  }
  private release() { for (const d of this.detach) d(); this.detach = []; }

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
        this.emit({ signature: sig, slot: e.slot, blockTime: Number(t.timestamp), wallet: t.user, mint: t.mint, side: t.isBuy ? "buy" : "sell",
          solAmount: Number(t.solAmount) / SOL, tokenAmount: Number(t.tokenAmount) / TOKEN_DEC, solAmountRaw: String(t.solAmount), tokenAmountRaw: String(t.tokenAmount),
          virtualSolReserves: String(t.virtualSolReserves), virtualTokenReserves: String(t.virtualTokenReserves), realTokenReserves: String(t.realTokenReserves),
          price: m.price, marketCapSol: m.marketCapSol, progress: m.progress, create: r.create, program: "pump", ...(nonSol ? { quoteNonSol: true } : {}), commitment: "processed", source: "logs", receivedAt: at });
      } else {
        const t = ev.t;
        const base = (pool: Awaited<ReturnType<PumpEngine["pool"]>>) => {
          const solIsQuote = !pool || pool.quote === WSOL;
          const bd = 10 ** (pool?.baseDec ?? 6), qd = 10 ** (pool?.quoteDec ?? 9);
          const price = t.baseAfter > 0n ? (Number(t.quoteAfter) / qd) / (Number(t.baseAfter) / bd) : null;
          this.emit({ signature: sig, slot: e.slot, blockTime: Number(t.timestamp), wallet: t.user, mint: pool ? (solIsQuote ? pool.base : pool.quote) : null, side: t.buy ? "buy" : "sell",
            solAmount: Number(t.quoteAmount) / qd, tokenAmount: Number(t.baseAmount) / bd, solAmountRaw: String(t.quoteAmount), tokenAmountRaw: String(t.baseAmount),
            virtualSolReserves: String(t.quoteAfter), virtualTokenReserves: String(t.baseAfter), price: solIsQuote ? price : (price ? 1 / price : null),
            create: false, program: "pumpswap", pool: t.pool, marketCapSol: price == null ? null : price * Number(TOTAL_SUPPLY) / TOKEN_DEC, progress: 100,
            commitment: "processed", source: "logs", receivedAt: at });
        };
        const cached = this.pools.get(t.pool);
        // fast path: wallet-only listeners don't need the mint; still resolve it, but synchronously when cached
        if (cached) void cached.then(base); else void this.pool(t.pool).then(base);
      }
    }
  }
  private pool(addr: string) {
    let p = this.pools.get(addr);
    if (!p) {
      p = (async () => {
        const a = await this.rpc("getAccountInfo", [addr, { encoding: "base64", commitment: "processed" }]);
        const d = a?.value?.data?.[0] ? Buffer.from(a.value.data[0], "base64") : null;
        if (!d || d.length < 107) return null;
        const base = b58encode(d.subarray(43, 75)), quote = b58encode(d.subarray(75, 107));
        const dec = async (mint: string) => mint === WSOL ? 9 : (await this.rpc("getAccountInfo", [mint, { encoding: "base64" }]).then((x) => Buffer.from(x.value.data[0], "base64")[44]).catch(() => 6));
        return { base, quote, baseDec: await dec(base), quoteDec: await dec(quote) };
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
  private emit(e: PumpSwapEvent) {
    this.stats.events++;
    const ls = [...this.ls].filter((l) => this.match(l, e)); if (!ls.length) { this.track(e); return; }
    const key = `${e.wallet}:${e.mint}`; const raw = BigInt(e.tokenAmountRaw);
    // pctOfPositionSold from tracked balance when we already know it (sync), else follow-up
    let pct: number | null | undefined;
    if (e.side === "sell") { const b = this.pos.get(key); pct = b != null && b > 0n ? Math.min(1, Number(raw) / Number(b)) : null; }
    this.track(e);
    for (const l of ls) {
      const tag = l.p.tags?.[e.wallet];
      const extra: any = {}; const alerts: DrawdownEvent[] = [];
      if (e.mint && e.price != null && e.price > 0 && (l.m?.has(e.mint) || tag || l.tr.has(e.mint))) {
        let t = l.tr.get(e.mint);
        if (!t) { if (l.tr.size >= 5000) l.tr.delete(l.tr.keys().next().value!); t = { sub: { high: e.price, fired: new Set() } }; l.tr.set(e.mint, t); }
        if (tag && !t.tag) {
          t.tag = { high: e.price, fired: new Set() };
          // follow every swap on a mint once a tagged wallet touches it, so its high/drawdown is real (trackTaggedMints:false disables)
          if (l.p.trackTaggedMints !== false && (l.m?.size ?? 0) < 500) (l.m ??= new Set()).add(e.mint);
        }
        for (const [basis, b] of [["subscribe", t.sub], ["firstTag", t.tag]] as const) {
          if (!b) continue;
          for (const a of stepBasis(b, e.price, l.p.drawdownAlerts, l.p.reboundPct))
            alerts.push({ type: a.kind, mint: e.mint, pct: Math.round(a.pct * 100) / 100, ...(a.threshold != null ? { threshold: a.threshold } : {}), high: b.high, ...(a.low != null ? { low: a.low } : {}), price: e.price, basis, signature: e.signature, slot: e.slot, wallet: e.wallet, ...(tag ? { tag } : {}), receivedAt: e.receivedAt });
        }
        Object.assign(extra, { highSinceSubscribe: t.sub.high, drawdownPct: ddPct(t.sub, e.price), highSinceFirstTag: t.tag?.high ?? null, drawdownSinceTagPct: ddPct(t.tag, e.price) });
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
      create: (tx.meta.logMessages ?? []).some((l: string) => l === "Program log: Instruction: Create"), program: prog, commitment: "confirmed", source: "balanceDiff", receivedAt: at });
  }
  return out;
}
