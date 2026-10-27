// Pump market data on top of PumpEngine: OHLCV candles (live + backfill), support/resistance levels, market regime.
import { PumpEngine, parsePumpLogs, curveMetrics, curvePda, ammEvent, solOf, type PumpSwapEvent } from "./pump.js";
import { INVALID_PARAMS } from "./errors.js";
import { isPubkey } from "./codec.js";

type Rpc = (m: string, p: any[]) => Promise<any>;
export const INTERVALS = { "1s": 1, "5s": 5, "1m": 60 } as const;
export type Interval = keyof typeof INTERVALS;
const KEEP: Record<Interval, number> = { "1s": 900, "5s": 720, "1m": 1440 };
export interface Candle { t: number; o: number; h: number; l: number; c: number; v: number; n: number }
interface Series { mint: string; c: Record<Interval, Candle[]>; lastUse: number; backfill?: Promise<any>; backfilled?: { txs: number; trades: number; from: number | null; capped: boolean }; lv?: { at: number; v: any }; venue?: string; pool?: string; quoteMint?: string }
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

export interface MarketOptions { maxTracked?: number; backfillTxs?: number; backfillPerMin?: number; warmPerMin?: number; now?: () => number }

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
    const s = this.series.get(e.mint); if (!s || !valid) return;
    if (e.venue !== "curve") { s.venue = e.venue; s.pool = e.pool; } else s.venue ??= "curve";
    if (conv) s.quoteMint = e.quoteMint;
    this.add(s, e.blockTime ? Math.min(Math.max(now, e.blockTime * 1000), e.blockTime * 1000 + 999) : now, px, solOf(e), true);
  }
  /** auto-warm: one capped backfill per mint traded by subscribed/tagged wallets or on subscribed mints; at most warmPerMin new mints/min */
  warmed = 0; private warmBudget = { at: 0, n: 0 };
  warm(mint: string, pool?: string) {
    const s = this.series.get(mint); if (s?.backfill) { s.lastUse = this.now(); return false; }
    const now = this.now(); if (now - this.warmBudget.at > 60_000) this.warmBudget = { at: now, n: 0 };
    if (this.warmBudget.n >= (this.o.warmPerMin ?? 20)) { this.track(mint); return false; }
    this.warmBudget.n++; this.warmed++; void this.backfill(mint, pool).catch(() => {}); return true;
  }
  // ---- backfill: signatures on the bonding curve (or canonical pool) -> getTransaction -> trade events ----
  private budget(n: number) { const now = this.now(); if (now - this.bfBudget.at > 60_000) this.bfBudget = { at: now, n: 0 }; const cap = this.o.backfillPerMin ?? 600; if (this.bfBudget.n + n > cap) return false; this.bfBudget.n += n; return true; }
  backfill(mint: string, pool?: string) {
    const s = this.track(mint); if (s.backfill) return s.backfill;
    s.backfill = (async () => {
      const addr = pool ?? s.pool ?? await this.canonicalPool(mint) ?? curvePda(mint); s.pool ??= addr === curvePda(mint) ? undefined : addr; const cap = this.o.backfillTxs ?? 200;
      const sigs: any[] = (await this.rpc("getSignaturesForAddress", [addr, { limit: Math.min(1000, cap) }]).catch(() => [])) ?? [];
      let txs = 0, trades = 0, capped = false, from: number | null = null;
      const ok = sigs.filter((x) => !x.err);
      for (let i = 0; i < ok.length; i += 4) {
        if (!this.budget(4)) { capped = true; break; }
        const got = await Promise.all(ok.slice(i, i + 4).map((x) => this.rpc("getTransaction", [x.signature, { encoding: "json", commitment: "confirmed", maxSupportedTransactionVersion: 0 }]).catch(() => null)));
        for (let j = 0; j < got.length; j++) {
          const tx = got[j]; if (!tx?.meta) continue; txs++;
          const bt = (tx.blockTime ?? ok[i + j].blockTime ?? 0) * 1000; if (!bt) continue;
          for (const ev of parsePumpLogs(tx.meta.logMessages ?? []).events) {
            let price: number | null = null, vol = 0;
            if (ev.k === "pump") {
              const t = ev.t; if (t.mint !== mint || t.virtualTokenReserves === 0n) continue;
              s.venue ??= "curve";
              if (t.virtualSolReserves > 0n) { price = curveMetrics(t.virtualSolReserves, t.virtualTokenReserves, t.realTokenReserves).price; vol = Number(t.solAmount) / 1e9; }
              else if (t.quote) { // non-SOL-quote curve: quote price converted at the current on-chain route rate
                const [qd, q] = await Promise.all([this.eng.decimals(t.quote.mint), this.eng.quoteSol(t.quote.mint)]);
                if (qd == null || q.sol == null) { s.quoteMint = t.quote.mint; continue; }
                price = (Number(t.quote.virtualReserves) / 10 ** qd) / (Number(t.virtualTokenReserves) / 1e6) * q.sol; vol = Number(t.quote.amount) / 10 ** qd * q.sol; s.quoteMint = t.quote.mint;
              } else continue;
            }
            else { const info = await this.eng.poolInfo(ev.t.pool); const a = ammEvent(ev.t, info, "", 0, 0); if (a.mint !== mint || !a.priceValid) continue; price = a.price; vol = a.solAmount; s.venue ??= a.venue; }
            if (price == null || !(price > 0) || vol < 0.00001) continue;
            this.add(s, bt, price, vol, false); trades++; from = from == null ? bt : Math.min(from, bt);
          }
        }
      }
      if (ok.length >= cap) capped = true;
      if (!s.venue) { const c = await this.eng.curvePrice(mint).catch(() => null); s.venue = c && !c.complete ? "curve" : s.pool ? "amm" : undefined; }
      s.backfilled = { txs, trades, from, capped };
      return s.backfilled;
    })();
    return s.backfill;
  }
  /** migrated mint -> PumpSwap mint/WSOL pool (one getProgramAccounts), else null (still on the curve) */
  private async canonicalPool(mint: string): Promise<string | null> {
    const c = await this.eng.curvePrice(mint).catch(() => null);
    if (c && !c.complete) return null;
    const accs: any[] = await this.rpc("getProgramAccounts", ["pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", { encoding: "base64", dataSlice: { offset: 0, length: 0 },
      filters: [{ memcmp: { offset: 43, bytes: mint } }, { memcmp: { offset: 75, bytes: "So11111111111111111111111111111111111111112" } }] }]).catch(() => []) ?? [];
    return accs[0]?.pubkey ?? null;
  }
  /** params: [mint, {interval: "1s"|"5s"|"1m" (default "1m"), lookback: candles (default 60), backfill?: bool (default true), pool?}] */
  async candles(mint: any, cfg: any = {}) {
    if (!isPubkey(mint)) throw INVALID_PARAMS("pumpCandles: params [mint, {interval, lookback}]");
    cfg ??= {}; const iv: Interval = cfg.interval ?? "1m";
    if (!(iv in INTERVALS)) throw INVALID_PARAMS("pumpCandles: interval one of 1s, 5s, 1m");
    const lb = cfg.lookback ?? 60; if (!(Number.isInteger(lb) && lb >= 1 && lb <= KEEP[iv])) throw INVALID_PARAMS(`pumpCandles: lookback integer 1..${KEEP[iv]} for ${iv}`);
    const s = this.track(mint);
    if (cfg.backfill !== false) { let to: any; await Promise.race([this.backfill(mint, cfg.pool), new Promise((r) => { to = setTimeout(r, 20_000); })]); clearTimeout(to); }
    const arr = s.c[iv], since = Math.floor(this.now() / 1000 / INTERVALS[iv]) * INTERVALS[iv] * 1000 - (lb - 1) * INTERVALS[iv] * 1000;
    return { mint, interval: iv, venue: s.venue ?? null, quoteMint: s.quoteMint ?? null, candles: arr.filter((k) => k.t >= since), backfill: s.backfilled ?? null,
      note: "OHLC in SOL from valid, non-dust prices (curve or canonical AMM; non-SOL quotes via priceSol at the current route rate); v = SOL volume; empty buckets omitted; backfill = getSignaturesForAddress(curve/pool) + getTransaction, capped" };
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
    const l = computeLevels(s.lv.v.cs, price); return l ? { interval: s.lv.v.interval, ...l } : null;
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
