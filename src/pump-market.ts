// Pump market data on top of PumpEngine: OHLCV candles (live + backfill), support/resistance levels, market regime.
import { PumpEngine, parsePumpLogs, curveMetrics, curvePda, ammEvent, solOf, type PumpSwapEvent } from "./pump.js";
import { INVALID_PARAMS } from "./errors.js";
import { isPubkey } from "./codec.js";
import fs from "node:fs"; import path from "node:path";

type Rpc = (m: string, p: any[]) => Promise<any>;
export const INTERVALS = { "1s": 1, "5s": 5, "1m": 60 } as const;
export type Interval = keyof typeof INTERVALS;
const KEEP: Record<Interval, number> = { "1s": 900, "5s": 720, "1m": 1440 };
export interface Candle { t: number; o: number; h: number; l: number; c: number; v: number; n: number }
interface Hist { trades: { ts: number; p: number; v: number; sig?: string }[]; sigs: Set<string>; newest: string | null; oldest: string | null; oldestTs?: number | null; historyEnd: boolean; txs: number; txMissing: number; fromDisk: number }
interface Job { maxTx: number; fromMs?: number; done: boolean; fetched: number; truncated: null | "maxTx" | "rateLimited" | "signaturesError"; p: Promise<any> }
interface Series { mint: string; c: Record<Interval, Candle[]>; lastUse: number; h?: Hist; job?: Job; addr?: string; unit?: "SOL" | "quote"; lv?: { at: number; v: any }; venue?: string; pool?: string; quoteMint?: string }
const MAX_TRADES = 50_000;
/** candles from raw trades in [fromMs, toMs] */
export function buildCandles(trades: { ts: number; p: number; v: number }[], sec: number, fromMs: number, toMs: number) {
  const out: Candle[] = [];
  for (const t of trades) { if (t.ts < fromMs || t.ts > toMs) continue; addCandle(out, sec * 1000, t.ts, t.p, t.v, Infinity); }
  return out;
}
const r6 = (x: number) => Math.round(x * 1e6) / 1e6;

/** add a trade to a candle series (sorted by t; out-of-order backfill inserts) */
export function addCandle(arr: Candle[], sec: number, ts: number, price: number, vol: number, keep: number) {
  const t = Math.floor(ts / sec) * sec;
  let i = arr.length - 1; while (i >= 0 && arr[i].t > t) i--;
  if (i >= 0 && arr[i].t === t) { const k = arr[i]; k.h = Math.max(k.h, price); k.l = Math.min(k.l, price); if (ts >= t && i === arr.length - 1) k.c = price; k.v += vol; k.n++; return k; }
  const k = { t, o: price, h: price, l: price, c: price, v: vol, n: 1 }; arr.splice(i + 1, 0, k); if (arr.length > keep) arr.splice(0, arr.length - keep); return k;
}
/** support/resistance from pivot highs/lows (±2 candles) and volume clusters (log-price histogram) */
export function computeLevels(cs: Candle[], price: number) {
  if (cs.length < 5 || !(price > 0)) return null;
  const lv: { price: number; kind: "pivot" | "volume"; strength: number }[] = [];
  for (let i = 2; i < cs.length - 2; i++) {
    const w = cs.slice(i - 2, i + 3);
    if (cs[i].h === Math.max(...w.map((x) => x.h))) lv.push({ price: cs[i].h, kind: "pivot", strength: 1 });
    if (cs[i].l === Math.min(...w.map((x) => x.l))) lv.push({ price: cs[i].l, kind: "pivot", strength: 1 });
  }
  const lo = Math.min(...cs.map((x) => x.l)), hi = Math.max(...cs.map((x) => x.h));
  if (hi > lo) {
    const B = 24, ll = Math.log(lo), step = (Math.log(hi) - ll) / B, h = new Array(B).fill(0);
    for (const k of cs) h[Math.min(B - 1, Math.floor((Math.log((k.h + k.l) / 2) - ll) / step))] += k.v;
    const tot = h.reduce((a, b) => a + b, 0) || 1;
    h.map((v, i) => ({ v, i })).sort((a, b) => b.v - a.v).slice(0, 3).filter((x) => x.v / tot >= 0.08)
      .forEach((x) => lv.push({ price: Math.exp(ll + (x.i + 0.5) * step), kind: "volume", strength: Math.round(x.v / tot * 100) / 100 }));
  }
  const below = lv.filter((x) => x.price < price).sort((a, b) => b.price - a.price), above = lv.filter((x) => x.price > price).sort((a, b) => a.price - b.price);
  const fmt = (x?: typeof lv[0]) => x ? { price: x.price, kind: x.kind, strength: x.strength, distancePct: r6((x.price / price - 1) * 100) } : null;
  return { support: fmt(below[0]), resistance: fmt(above[0]), supports: below.slice(0, 3).map(fmt), resistances: above.slice(0, 3).map(fmt), candles: cs.length };
}

export interface MarketOptions { maxTracked?: number; backfillTxs?: number; backfillPerMin?: number; warmPerMin?: number; /** per-mint trade cache dir (repeat backfills are free) */ cacheDir?: string; now?: () => number }

export class PumpMarket {
  series = new Map<string, Series>();
  private subs = new Set<{ mint: string; iv: Interval; cb: (c: Candle & { mint: string; interval: Interval }) => void }>();
  private stop?: () => void; private timers: NodeJS.Timeout[] = [];
  // regime state
  solHist: { at: number; p: number }[] = [];
  flow: { s: number; buy: number; sell: number }[] = []; // per-second, last hour
  recent = new Map<string, { at: number; high: number; last: number }>(); // curve mints created in the last hour
  private bfBudget: { at: number; n: number } = { at: 0, n: 0 };
  private now: () => number;
  constructor(private eng: PumpEngine, private rpc: Rpc, private o: MarketOptions = {}) { this.now = o.now ?? Date.now; }
  start() {
    if (this.stop) return this;
    this.eng.hooks.push((e) => this.onSwap(e));
    this.stop = this.eng.subscribe({ allSwaps: true, position: false }, () => {});
    const t = setInterval(() => this.sample(), 10_000); t.unref?.(); this.timers.push(t);
    return this;
  }
  close() { for (const t of this.timers) clearInterval(t); this.timers = []; this.stop?.(); this.stop = undefined; }
  sample() {
    const now = this.now();
    if (this.eng.solUsd) { this.solHist.push({ at: now, p: this.eng.solUsd }); if (this.solHist.length > 2000) this.solHist.shift(); }
    for (const [m, s] of this.series) if (now - s.lastUse > 30 * 60_000 && ![...this.subs].some((x) => x.mint === m)) this.series.delete(m);
    for (const [m, r] of this.recent) if (now - r.at > 3600_000) this.recent.delete(m);
  }
  track(mint: string) {
    let s = this.series.get(mint);
    if (!s) {
      s = { mint, c: { "1s": [], "5s": [], "1m": [] }, lastUse: this.now() }; this.series.set(mint, s);
      if (this.series.size > (this.o.maxTracked ?? 500)) { const old = [...this.series.values()].sort((a, b) => a.lastUse - b.lastUse)[0]; this.series.delete(old.mint); }
    }
    s.lastUse = this.now(); return s;
  }
  private add(s: Series, tsMs: number, price: number, vol: number, live: boolean) {
    for (const iv of Object.keys(INTERVALS) as Interval[]) {
      const k = addCandle(s.c[iv], INTERVALS[iv] * 1000, tsMs, price, vol, KEEP[iv]);
      if (live) for (const x of this.subs) if (x.mint === s.mint && x.iv === iv) try { x.cb({ mint: s.mint, interval: iv, ...k }); } catch {}
    }
  }
  onSwap(e: PumpSwapEvent) {
    if (!e.mint || e.update) return;
    const now = this.now(), sec = Math.floor(now / 1000);
    // pump-wide flow (SOL value incl. converted non-SOL quotes), curve + canonical AMM only
    if (e.venue !== "amm-other" || e.solEquivalent != null) {
      let b = this.flow[this.flow.length - 1]; if (!b || b.s !== sec) { b = { s: sec, buy: 0, sell: 0 }; this.flow.push(b); while (this.flow.length && this.flow[0].s < sec - 3600) this.flow.shift(); }
      if (e.side === "buy") b.buy += solOf(e); else b.sell += solOf(e);
    }
    const conv = e.quoteMint != null && e.priceSol != null && e.priceSol > 0 && !e.dust; // non-SOL quote, converted at current route rate
    const valid = conv || (e.priceValid && e.price != null && !e.dust && (e.venue === "curve" || e.venue === "amm"));
    const px = conv ? e.priceSol! : e.price!;
    if (e.venue === "curve" && valid) {
      let r = this.recent.get(e.mint);
      if (!r && e.create && this.recent.size < 20_000) { r = { at: now, high: px, last: px }; this.recent.set(e.mint, r); }
      if (r) { r.high = Math.max(r.high, px); r.last = px; }
    }
    if (!e.dust && (this.eng.mintWatched?.(e.mint) || this.eng.watched?.(e.wallet) || this.eng.tagOf?.(e.wallet))) this.warm(e.mint, e.venue !== "curve" ? e.pool : undefined);
    const s = this.series.get(e.mint); if (!s) return;
    const unit = e.quoteMint ? "quote" : "SOL";
    const lp = unit === "quote" ? e.priceQuote : valid ? e.price : null, lv = unit === "quote" ? e.quoteAmount : solOf(e);
    if (lp == null || !(lp > 0) || lv == null || e.dust || (unit === "SOL" && !valid)) return;
    s.unit ??= unit; if (s.unit !== unit) return;
    if (e.venue !== "curve") { s.venue = e.venue; s.pool = e.pool; } else s.venue ??= "curve";
    if (e.quoteMint) s.quoteMint = e.quoteMint;
    this.addTrade(s, e.blockTime ? Math.min(Math.max(now, e.blockTime * 1000), e.blockTime * 1000 + 999) : now, lp, lv, this.liveKey(s, e.signature), true);
  }
  /** auto-warm: one capped backfill per mint traded by subscribed/tagged wallets or on subscribed mints; at most warmPerMin new mints/min */
  warmed = 0; private warmBudget = { at: 0, n: 0 };
  warm(mint: string, pool?: string) {
    const s = this.series.get(mint); if (s?.job) { s.lastUse = this.now(); return false; }
    const now = this.now(); if (now - this.warmBudget.at > 60_000) this.warmBudget = { at: now, n: 0 };
    if (this.warmBudget.n >= (this.o.warmPerMin ?? 20)) { this.track(mint); return false; }
    this.warmBudget.n++; this.warmed++; void this.backfill(mint, pool).catch(() => {}); return true;
  }
  // ---- backfill: signatures on the bonding curve (or canonical pool) -> getTransaction -> trade events ----
  // paginated (before/until), rate-limited (backfillPerMin tx/min, waits rather than aborts), cached to disk per mint
  private async budget(n: number) {
    for (;;) { const now = this.now(); if (now - this.bfBudget.at > 60_000) this.bfBudget = { at: now, n: 0 }; const cap = this.o.backfillPerMin ?? 600;
      if (this.bfBudget.n + n <= cap) { this.bfBudget.n += n; return true; }
      if (this.o.now) return false; // fake clock (tests): no waiting
      this.rateLimitedWaits++; await new Promise((r) => setTimeout(r, Math.max(500, 60_000 - (now - this.bfBudget.at)))); }
  }
  rateLimitedWaits = 0;
  private file(mint: string) { return this.o.cacheDir ? path.join(this.o.cacheDir, `${mint}.json`) : null; }
  private load(s: Series) {
    if (s.h) return s.h;
    s.h = { trades: [], sigs: new Set(), newest: null, oldest: null, historyEnd: false, txs: 0, txMissing: 0, fromDisk: 0 };
    const f = this.file(s.mint);
    if (f && fs.existsSync(f)) try {
      const j = JSON.parse(fs.readFileSync(f, "utf8"));
      Object.assign(s.h, { newest: j.newest, oldest: j.oldest, historyEnd: !!j.historyEnd, txMissing: j.txMissing ?? 0 });
      s.unit ??= j.unit; s.quoteMint ??= j.quoteMint ?? undefined; s.venue ??= j.venue ?? undefined; s.pool ??= j.pool ?? undefined; s.addr = j.addr;
      for (const [ts, p, v, sig] of j.trades as [number, number, number, string][]) { this.addTrade(s, ts, p, v, sig, false); s.h.sigs.add(sig.split(":")[0]); }
      s.h.fromDisk = s.h.trades.length;
    } catch {}
    return s.h;
  }
  private save(s: Series) {
    const f = this.file(s.mint); if (!f || !s.h) return;
    try { fs.mkdirSync(path.dirname(f), { recursive: true }); const h = s.h;
      fs.writeFileSync(f + ".tmp", JSON.stringify({ mint: s.mint, addr: s.addr, unit: s.unit, quoteMint: s.quoteMint ?? null, venue: s.venue ?? null, pool: s.pool ?? null, newest: h.newest, oldest: h.oldest, historyEnd: h.historyEnd, txMissing: h.txMissing,
        trades: h.trades.filter((t) => t.sig).map((t) => [t.ts, t.p, t.v, t.sig]) }));
      fs.renameSync(f + ".tmp", f); } catch {}
  }
  private liveN = new Map<string, number>();
  /** same key scheme as backfill (`sig:n` = n-th trade of this mint in the tx) so live + backfilled trades dedupe */
  private liveKey(s: Series, sig?: string) {
    if (!sig) return undefined; const k = `${s.mint}:${sig}`; const n = this.liveN.get(k) ?? 0; this.liveN.set(k, n + 1);
    if (this.liveN.size > 50_000) this.liveN.delete(this.liveN.keys().next().value!);
    (s.h ?? this.load(s)).sigs.add(sig); return `${sig}:${n}`;
  }
  /** raw trade store (unit = s.unit); also feeds the rolling candle arrays (live ones notify subscribers) */
  private addTrade(s: Series, ts: number, p: number, v: number, sig: string | undefined, live: boolean) {
    const h = s.h ?? this.load(s);
    if (sig) { if (h.sigs.has(sig)) return false; h.sigs.add(sig); }
    let i = h.trades.length - 1; while (i >= 0 && h.trades[i].ts > ts) i--;
    h.trades.splice(i + 1, 0, { ts, p, v, sig });
    if (h.trades.length > MAX_TRADES) { const x = h.trades.shift()!; if (x.sig) h.sigs.delete(x.sig); }
    this.add(s, ts, p, v, live); return true;
  }
  /** fetch more history: newer than the cached newest signature, then older than the cached oldest, until maxTx this run or `untilMs` reached */
  backfill(mint: string, pool?: string, want: { maxTx?: number; fromMs?: number } = {}) {
    const s = this.track(mint); const h = this.load(s);
    const maxTx = Math.min(want.maxTx ?? this.o.backfillTxs ?? 200, 5000);
    const need = (j: Job | undefined) => !j || (!h.historyEnd && ((want.maxTx ?? 0) > j.maxTx || (want.fromMs != null && (j.fromMs == null || want.fromMs < j.fromMs))));
    if (s.job && !s.job.done && !need(s.job)) return s.job.p;
    if (s.job?.done && !need(s.job) && !(want.maxTx || want.fromMs)) return s.job.p;
    const prev = s.job?.p ?? Promise.resolve();
    const job: Job = { maxTx, fromMs: want.fromMs, done: false, fetched: 0, truncated: null, p: null as any };
    job.p = prev.catch(() => {}).then(async () => {
      s.addr ??= pool ?? s.pool ?? await this.canonicalPool(mint) ?? curvePda(mint); if (s.addr !== curvePda(mint)) s.pool ??= s.addr;
      const fetchSigs = async (opt: any) => ((await this.rpc("getSignaturesForAddress", [s.addr, { limit: 1000, ...opt }]).catch(() => null)) ?? null) as any[] | null;
      const run = async (sigs: any[]) => {
        const ok = sigs.filter((x) => !x.err && !h.sigs.has(x.signature));
        for (let i = 0; i < ok.length; i += 4) {
          if (job.fetched >= maxTx) { job.truncated = "maxTx"; return false; }
          if (!(await this.budget(4))) { job.truncated = "rateLimited"; return false; }
          const part = ok.slice(i, i + 4);
          const got = await Promise.all(part.map((x) => this.rpc("getTransaction", [x.signature, { encoding: "json", commitment: "confirmed", maxSupportedTransactionVersion: 0 }]).catch(() => null)));
          for (let j = 0; j < got.length; j++) {
            job.fetched++; h.txs++; const tx = got[j]; if (!tx?.meta) { h.txMissing++; continue; }
            await this.ingestTx(s, tx, part[j]);
          }
          if (want.fromMs != null && (part[part.length - 1].blockTime ?? Infinity) * 1000 < want.fromMs && sigs === older) return false;
        }
        return true;
      };
      let older: any[] = [];
      // 1) newer than cached newest (pages until we meet it)
      if (h.newest) {
        let before: string | undefined; const pages: any[] = [];
        for (let k = 0; k < 10; k++) { const pg = await fetchSigs({ until: h.newest, ...(before ? { before } : {}) }); if (!pg?.length) break; pages.push(...pg); before = pg[pg.length - 1].signature; if (pg.length < 1000) break; }
        if (pages.length) { const top = pages[0].signature; await run(pages); if (!job.truncated) h.newest = top; }
      }
      // 2) older than cached oldest (or from the tip on first run)
      while (!job.truncated && !h.historyEnd && job.fetched < maxTx) {
        if (want.fromMs != null && h.oldestTs != null && h.oldestTs * 1000 < want.fromMs && (want.maxTx == null)) break;
        const pg = await fetchSigs(h.oldest ? { before: h.oldest } : {}); if (pg == null) { job.truncated = "signaturesError"; break; }
        if (!pg.length) { h.historyEnd = true; break; }
        h.newest ??= pg[0].signature; older = pg;
        const cont = await run(pg);
        if (!job.truncated) { h.oldest = pg[pg.length - 1].signature; h.oldestTs = pg[pg.length - 1].blockTime ?? h.oldestTs; if (pg.length < 1000) h.historyEnd = true; }
        else { // resume point = last signature actually fetched
          const lastDone = [...pg].reverse().find((x) => h.sigs.has(x.signature)); if (lastDone) { h.oldest = lastDone.signature; h.oldestTs = lastDone.blockTime ?? h.oldestTs; } }
        if (!cont) break;
        if (want.fromMs != null && (h.oldestTs ?? Infinity) * 1000 < want.fromMs) break;
      }
      if (!job.truncated && !h.historyEnd && job.fetched >= maxTx && !(want.fromMs != null && (h.oldestTs ?? Infinity) * 1000 < want.fromMs)) job.truncated = "maxTx";
      if (!s.venue) { const c = await this.eng.curvePrice(mint).catch(() => null); s.venue = c && !c.complete ? "curve" : s.pool ? "amm" : undefined; }
      this.save(s);
    }).finally(() => { job.done = true; });
    s.job = job; return job.p;
  }
  private async ingestTx(s: Series, tx: any, sigInfo: any) {
    const mint = s.mint, sig = sigInfo.signature as string;
    const bt = (tx.blockTime ?? sigInfo.blockTime ?? 0) * 1000; if (!bt) return;
    let n = 0;
    for (const ev of parsePumpLogs(tx.meta.logMessages ?? []).events) {
      let price: number | null = null, vol = 0, unit: "SOL" | "quote" = "SOL";
      if (ev.k === "pump") {
        const t = ev.t; if (t.mint !== mint || t.virtualTokenReserves === 0n) continue;
        s.venue ??= "curve";
        if (t.virtualSolReserves > 0n) { price = curveMetrics(t.virtualSolReserves, t.virtualTokenReserves, t.realTokenReserves).price; vol = Number(t.solAmount) / 1e9; }
        else if (t.quote) {
          const qd = await this.eng.decimals(t.quote.mint); s.quoteMint = t.quote.mint; if (qd == null) continue; unit = "quote";
          price = (Number(t.quote.virtualReserves) / 10 ** qd) / (Number(t.virtualTokenReserves) / 1e6); vol = Number(t.quote.amount) / 10 ** qd;
        } else continue;
      } else {
        const info = await this.eng.poolInfo(ev.t.pool); const a = ammEvent(ev.t, info, "", 0, 0); if (a.mint !== mint || !a.priceValid) continue; price = a.price; vol = a.solAmount; s.venue ??= a.venue;
      }
      if (price == null || !(price > 0) || (unit === "SOL" && vol < 0.00001)) continue;
      s.unit ??= unit; if (s.unit !== unit) continue;
      this.addTrade(s, bt, price, vol, `${sig}:${n++}`, false);
    }
    s.h!.sigs.add(sig);
  }
  /** migrated mint -> PumpSwap mint/WSOL pool (one getProgramAccounts), else null (still on the curve) */
  private async canonicalPool(mint: string): Promise<string | null> {
    const c = await this.eng.curvePrice(mint).catch(() => null);
    if (c && !c.complete) return null;
    const accs: any[] = await this.rpc("getProgramAccounts", ["pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", { encoding: "base64", dataSlice: { offset: 0, length: 0 },
      filters: [{ memcmp: { offset: 43, bytes: mint } }, { memcmp: { offset: 75, bytes: "So11111111111111111111111111111111111111112" } }] }]).catch(() => []) ?? [];
    return accs[0]?.pubkey ?? null;
  }
  /** params: [mint, {interval: "1s"|"5s"|"1m" (default "1m"), lookback?: candles (default 60), from?/to?: unix sec, backfill?: true, maxTx?: <=5000, waitMs?: <=120000, pool?}] */
  async candles(mint: any, cfg: any = {}) {
    if (!isPubkey(mint)) throw INVALID_PARAMS("pumpCandles: params [mint, {interval, lookback | from, to}]");
    cfg ??= {}; const iv: Interval = cfg.interval ?? "1m";
    if (!(iv in INTERVALS)) throw INVALID_PARAMS("pumpCandles: interval one of 1s, 5s, 1m");
    const sec = INTERVALS[iv];
    for (const k of ["from", "to"]) if (cfg[k] != null && !(Number.isFinite(cfg[k]) && cfg[k] > 1e9 && cfg[k] < 1e11)) throw INVALID_PARAMS(`pumpCandles: ${k} must be unix seconds`);
    if (cfg.maxTx != null && !(Number.isInteger(cfg.maxTx) && cfg.maxTx >= 0 && cfg.maxTx <= 5000)) throw INVALID_PARAMS("pumpCandles: maxTx integer 0..5000");
    const nowS = Math.floor(this.now() / 1000), to = cfg.to ?? nowS;
    const lb = cfg.lookback ?? 60;
    const from = cfg.from ?? (Math.floor(to / sec) - lb + 1) * sec;
    if (!(from <= to)) throw INVALID_PARAMS("pumpCandles: from must be <= to");
    if ((to - from) / sec > 5000) throw INVALID_PARAMS("pumpCandles: range too large (max 5000 candles; use a bigger interval)");
    if (cfg.from == null && !(Number.isInteger(lb) && lb >= 1 && lb <= 5000)) throw INVALID_PARAMS("pumpCandles: lookback integer 1..5000");
    const s = this.track(mint); const h = this.load(s);
    if (cfg.backfill !== false) {
      const p = this.backfill(mint, cfg.pool, { maxTx: cfg.maxTx, fromMs: cfg.from != null ? from * 1000 : undefined });
      let t: any; await Promise.race([p, new Promise((r) => { t = setTimeout(r, Math.min(Number(cfg.waitMs ?? 20_000), 120_000)); })]); clearTimeout(t);
    }
    const cs = buildCandles(h.trades, sec, from * 1000, to * 1000 + 999);
    const unit = s.unit ?? "SOL";
    let rate: { sol: number | null; via: string | null; reason?: string } = { sol: 1, via: "SOL" };
    if (unit === "quote" && s.quoteMint) rate = await this.eng.quoteSol(s.quoteMint).catch(() => ({ sol: null, via: null, reason: "lookupFailed" }));
    const covFrom = h.trades.length ? Math.floor(h.trades[0].ts / 1000) : null, covTo = h.trades.length ? Math.floor(h.trades[h.trades.length - 1].ts / 1000) : null;
    const j = s.job;
    return { mint, interval: iv, from, to, venue: s.venue ?? null, priceUnit: unit === "quote" ? "quote" : "SOL", quoteMint: s.quoteMint ?? null,
      quoteSolRate: unit === "quote" ? rate.sol : 1, solConversion: unit === "quote" ? (rate.sol != null ? `current rate via ${rate.via} (not historical)` : `unavailable: ${rate.reason ?? "noSolRoute"}`) : "native",
      candles: cs.map((k) => unit === "quote" && rate.sol != null ? { ...k, oSol: k.o * rate.sol!, hSol: k.h * rate.sol!, lSol: k.l * rate.sol!, cSol: k.c * rate.sol!, vSol: k.v * rate.sol! } : k),
      coverage: { from: covFrom, to: covTo, trades: h.trades.length, fromDiskCache: h.fromDisk, txsFetched: h.txs, txMissing: h.txMissing, historyEnd: h.historyEnd,
        requestCovered: h.historyEnd || (covFrom != null && covFrom <= from), inProgress: j ? !j.done : false, truncated: j?.truncated ?? null, rateLimitedWaits: this.rateLimitedWaits,
        note: h.txMissing ? "some getTransaction calls returned null (upstream may lack archive depth)" : !h.historyEnd ? "older history exists beyond coverage.from; raise maxTx or pass from" : "full signature history fetched" },
      note: `OHLC in ${unit === "quote" ? "quote-token units (priceQuote); *Sol fields = converted at the current on-chain rate" : "SOL"} from valid, non-dust trades; v = volume in priceUnit; empty buckets omitted` };
  }
  subscribeCandles(mint: string, iv: Interval, cb: (c: any) => void) {
    if (!isPubkey(mint)) throw INVALID_PARAMS("pumpCandlesSubscribe: params [mint, {interval}]");
    if (!(iv in INTERVALS)) throw INVALID_PARAMS("pumpCandlesSubscribe: interval one of 1s, 5s, 1m");
    this.track(mint); void this.backfill(mint);
    const x = { mint, iv, cb }; this.subs.add(x); return () => { this.subs.delete(x); };
  }
  /** levels from live candles of this mint (tracking starts on first call; no upstream backfill, so it is free) */
  levels(mint: string, price: number | null) {
    const s = this.track(mint); if (price == null) return null;
    const now = this.now();
    if (!s.lv || now - s.lv.at > 5000) {
      const cs = s.c["1m"].length >= 10 ? s.c["1m"] : s.c["5s"];
      s.lv = { at: now, v: { interval: cs === s.c["1m"] ? "1m" : "5s", cs: cs.slice(-240) } };
    }
    const l = computeLevels(s.lv.v.cs, price); return l ? { interval: s.lv.v.interval, priceUnit: s.unit === "quote" ? "quote" : "SOL", ...l } : null;
  }
  // ---- market regime ----
  regime() {
    const now = this.now(), sec = Math.floor(now / 1000);
    const chg = (w: number) => {
      const first = this.solHist[0], last = this.solHist[this.solHist.length - 1];
      if (!last) return { changePct: null, complete: false, note: "no SOL/USD samples yet" };
      if (!first || now - first.at < w * 1000 - 15_000) return { changePct: null, complete: false, note: `insufficient history: ${Math.round((now - (first?.at ?? now)) / 60_000)}m of ${w / 60}m since startup` };
      const ref = [...this.solHist].reverse().find((x) => x.at <= now - w * 1000) ?? first;
      return { changePct: r6((last.p / ref.p - 1) * 100), from: ref.p, to: last.p, complete: true };
    };
    const flowW = (w: number) => {
      const fs = this.flow.filter((b) => b.s > sec - w); const buy = fs.reduce((a, b) => a + b.buy, 0), sell = fs.reduce((a, b) => a + b.sell, 0);
      const cover = this.flow.length ? Math.min(w, sec - this.flow[0].s + 1) : 0;
      const v = { buySol: r6(buy), sellSol: r6(sell), netSol: r6(buy - sell), buySellRatio: sell > 0 ? r6(buy / sell) : null };
      // incomplete window: headline numbers are null (they would just repeat the shorter partial history); partial sums kept separately
      return cover >= w ? { ...v, coveredSec: cover, complete: true } : { buySol: null, sellSol: null, netSol: null, buySellRatio: null, coveredSec: cover, complete: false, partial: v };
    };
    const rec = [...this.recent.values()].filter((r) => now - r.at >= 60_000);
    const dd = rec.filter((r) => r.last / r.high - 1 <= -0.3).length;
    const f5 = flowW(300), f15 = flowW(900), net = (f15.complete ? f15.netSol : f5.netSol) ?? 0, solCh = chg(3600).changePct ?? chg(900).changePct;
    const warming = !f5.complete || rec.length < 10;
    const label = warming ? null : (dd / rec.length > 0.8 || net < 0) && (solCh == null || solCh <= 0) ? "risk-off" : dd / rec.length < 0.6 && net > 0 ? "risk-on" : "neutral";
    return { generatedAt: now, solUsd: this.eng.solUsd, solChange: { "15m": chg(900), "1h": chg(3600) },
      netFlow: { "1m": flowW(60), "5m": flowW(300), "15m": flowW(900), "1h": flowW(3600) },
      recentCurveTokens: { window: "created in the last 60m (seen live), at least 1m old", count: rec.length, drawdownOver30Pct: dd, pct: rec.length ? r6(dd / rec.length * 100) : null },
      label, labelStatus: warming ? "warming" : "ready", labelNote: warming ? `label needs a complete 5m flow window (${f5.coveredSec}/300s) and >=10 recent curve tokens (${rec.length})` : "uses 15m flow when complete, else 5m",
      units: { changePct: "percent (5 = +5%)", recentCurveTokens_pct: "percent", flow: "SOL" }, note: "SOL/USD from Pyth on-chain samples since startup; flow = pump curve + canonical PumpSwap swaps (non-SOL quotes via solEquivalent)" };
  }
}
