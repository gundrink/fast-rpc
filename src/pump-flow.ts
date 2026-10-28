// Trending tokens + per-mint flow metrics + large-seller / dev-sell alerts on top of PumpEngine (all swaps, in memory since startup).
import { PumpEngine, curvePda, solOf, type PumpSwapEvent } from "./pump.js";
import { INVALID_PARAMS } from "./errors.js";
import { isPubkey, b58encode } from "./codec.js";

type Rpc = (m: string, p: any[]) => Promise<any>;
const B = 10; // seconds per bucket
const r6 = (x: number) => Math.round(x * 1e6) / 1e6;
interface Bucket { s: number; buySol: number; sellSol: number; buys: number; sells: number; open: number | null; close: number | null; prog?: number | null }
interface MintStat {
  mint: string; b: Bucket[]; buyers: Map<string, number>; firstSeen: number; createdAt: number | null; creator: string | null; creatorChecked?: number; sellers?: Map<string, number>; devSoldAt?: number; createSlot?: number; createSlotBuys?: number; earlyBuys?: number; wtx?: Map<string, { b: number; s: number; at: number }>; ageInfo?: any;
  last?: { price: number | null; priceUsd: number | null; marketCapSol: number | null; marketCapUsd: number | null; liquiditySol: number | null; liquidityUsd: number | null; liquidityReason?: string; progress: number | null; venue: string; quoteMint: string | null; at: number; priceSol: number | null };
  alerts: any[];
}
export const TREND_SORTS = ["volume1m", "txs1m", "buyers1m", "netFlow1m", "volume5m", "volume1h", "txs5m", "txs1h", "buyers5m", "buyers1h", "netFlow5m", "netFlow1h", "change5m", "change1h", "marketCap", "liquidity"] as const;
export interface FlowAlertCfg { mints?: string[]; allMints?: boolean; largeSellPct?: number; minSellSol?: number; devSells?: boolean }
export interface FlowOptions { maxMints?: number; txPerMin?: number; enrichTtlMs?: number; now?: () => number }

export const SCORE_FORMULA = "washTrade = txs5m>=20 && (repeatWalletTxRatio>0.5 || roundTripRatio>0.3); bundled = buys by non-creator wallets in create slot >=3 or within create slot+2 >=5. score = 25*min(1,uniqueBuyers5m/50) + 20*clamp(netFlowSol5m/10) + 15*clamp((buySellRatio5m-1)/2) + 15*clamp(bondingVelocityPctPerMin/2) + 10*min(1,holderCount/200) + 15*clamp(1-|bondingProgress-60|/60) − min(15,(top10Pct−30)/2 if >30) − 20 devSold − 10 devHolding>10% − 10 bundled − 10 washTrade − 5 freshWalletRatio>0.6; clamped 0..100 (buySellRatio null with buys only counts as 3)";
export class PumpFlow {
  stats = new Map<string, MintStat>();
  private subs = new Set<{ cfg: FlowAlertCfg; set?: Set<string>; cb: (a: any) => void }>();
  private stop?: () => void; private now: () => number;
  private txBudget = { at: 0, n: 0 }; private enrichCache = new Map<string, { at: number; v: Promise<any> }>();
  droppedChecks = 0;
  constructor(private eng: PumpEngine, private rpc: Rpc, private o: FlowOptions = {}) { this.now = o.now ?? Date.now; this.startedAt = this.now(); }
  start() { if (this.stop) return this; this.eng.hooks.push((e) => this.onSwap(e)); this.stop = this.eng.subscribe({ allSwaps: true, position: false }, () => {}); return this; }
  close() { this.stop?.(); this.stop = undefined; }

  onSwap(e: PumpSwapEvent) {
    if (!e.mint || e.update || e.dust) return;
    const now = this.now(), sec = Math.floor(now / 1000), bs = Math.floor(sec / B) * B;
    let st = this.stats.get(e.mint);
    if (!st) {
      st = { mint: e.mint, b: [], buyers: new Map(), firstSeen: now, createdAt: null, creator: null, alerts: [] }; this.stats.set(e.mint, st);
      if (this.stats.size > (this.o.maxMints ?? 8000)) this.prune(now);
    }
    if (e.create) { st.createdAt = (e.blockTime ?? sec) * 1000; st.creator ??= e.wallet; st.createSlot = e.slot; st.createSlotBuys = 0; st.earlyBuys = 0; }
    else if (st.createSlot != null && e.side === "buy" && e.slot != null && e.wallet !== st.creator) { if (e.slot === st.createSlot) st.createSlotBuys = (st.createSlotBuys ?? 0) + 1; if (e.slot - st.createSlot <= 2) st.earlyBuys = (st.earlyBuys ?? 0) + 1; }
    { const w = (st.wtx ??= new Map()); let x = w.get(e.wallet); if (!x || now - x.at > 300_000) { x = { b: 0, s: 0, at: now }; w.delete(e.wallet); w.set(e.wallet, x); } if (e.side === "buy") x.b++; else x.s++; if (w.size > 3000) w.delete(w.keys().next().value!); }
    let b = st.b[st.b.length - 1]; if (!b || b.s !== bs) { b = { s: bs, buySol: 0, sellSol: 0, buys: 0, sells: 0, open: null, close: null }; st.b.push(b); while (st.b[0].s < bs - 3600) st.b.shift(); }
    const v = solOf(e); const conv = e.solEquivalent != null || !e.quoteMint;
    if (conv) { if (e.side === "buy") b.buySol += v; else b.sellSol += v; }
    if (e.side === "buy") { b.buys++; st.buyers.set(e.wallet, now); if (st.buyers.size > 3000) st.buyers.delete(st.buyers.keys().next().value!); } else { b.sells++; (st.sellers ??= new Map()).set(e.wallet, now); if (st.sellers.size > 3000) st.sellers.delete(st.sellers.keys().next().value!); if (st.creator && e.wallet === st.creator) st.devSoldAt = now; }
    this.lastSwapAt = now;
    const px = e.quoteMint ? e.priceSol ?? null : e.priceValid ? e.price : null;
    if (px != null && px > 0 && (e.venue !== "amm-other" || e.quoteMint)) { b.open ??= px; b.close = px; }
    if (e.venue === "curve" && typeof e.progress === "number") b.prog = e.progress;
    if (px != null || !st.last) st.last = { price: px, priceSol: px, priceUsd: px != null && e.solUsd ? px * e.solUsd : null, marketCapSol: e.marketCapSol ?? null, marketCapUsd: e.marketCapUsd ?? null,
      liquiditySol: e.liquiditySol ?? null, liquidityUsd: e.liquidityUsd ?? null, liquidityReason: e.liquidityReason, progress: e.venue === "curve" ? e.progress ?? null : e.venue === "amm" ? 100 : null, venue: e.venue, quoteMint: e.quoteMint ?? null, at: now };
    if (e.side === "sell") this.checkSell(st, e, v, conv);
  }
  private prune(now: number) { const old = [...this.stats.values()].sort((a, b) => (a.b[a.b.length - 1]?.s ?? 0) - (b.b[b.b.length - 1]?.s ?? 0)); for (const x of old.slice(0, Math.ceil(old.length / 10))) this.stats.delete(x.mint); }

  // ---- alerts ----
  private interested(mint: string) { for (const x of this.subs) if (x.cfg.allMints || x.set?.has(mint)) return true; return false; }
  private budget() { const now = this.now(); if (now - this.txBudget.at > 60_000) this.txBudget = { at: now, n: 0 }; if (this.txBudget.n >= (this.o.txPerMin ?? 120)) return false; this.txBudget.n++; return true; }
  private async creatorOf(st: MintStat) {
    if ((st as any).creatorP) return (st as any).creatorP;
    if (st.creator || (st.creatorChecked && this.now() - st.creatorChecked < 3600_000)) return st.creator;
    if ((st as any).creatorP) return (st as any).creatorP;
    st.creatorChecked = this.now();
    const p = (st as any).creatorP = this.eng.curvePrice(st.mint).catch(() => null).then((c: any) => { st.creator = c?.creator ?? null; (st as any).creatorP = undefined; return st.creator; });
    return p;
  }
  private checkSell(st: MintStat, e: PumpSwapEvent, sol: number, conv: boolean) {
    if (!this.interested(st.mint)) return;
    void (async () => {
      const creator = await this.creatorOf(st);
      const isDev = creator != null && e.wallet === creator;
      const wantLarge = [...this.subs].some((x) => (x.cfg.allMints || x.set?.has(st.mint)) && x.cfg.largeSellPct != null && (!conv || sol >= (x.cfg.minSellSol ?? 0.5)));
      if (!isDev && !wantLarge) return;
      // exact share of the wallet's position sold: pre/post token balances from the confirmed tx (budgeted)
      let pct: number | null = null, reason: string | undefined, pre: number | null = null;
      if (typeof e.pctOfPositionSold === "number") pct = e.pctOfPositionSold;
      else if (!e.signature || !this.budget()) { reason = "txBudgetExhausted"; this.droppedChecks++; }
      else {
        let tx: any = null;
        for (let i = 0; i < 3 && !tx; i++) { await new Promise((r) => setTimeout(r, this.o.now ? 0 : 1500 * (i + 1))); tx = await this.rpc("getTransaction", [e.signature, { encoding: "jsonParsed", commitment: "confirmed", maxSupportedTransactionVersion: 0 }]).catch(() => null); }
        if (!tx?.meta) reason = "txNotFound";
        else {
          const sum = (arr: any[]) => (arr ?? []).filter((x) => x.owner === e.wallet && x.mint === st.mint).reduce((a, x) => a + Number(x.uiTokenAmount.amount), 0);
          const a = sum(tx.meta.preTokenBalances), b = sum(tx.meta.postTokenBalances); pre = a / 1e6;
          if (a > 0) pct = Math.min(100, Math.max(0, (a - b) / a * 100)); else reason = "noPreBalance";
        }
      }
      const base = { mint: st.mint, wallet: e.wallet, signature: e.signature, slot: e.slot, blockTime: e.blockTime ?? null, solAmount: conv ? r6(sol) : null, ...(conv ? {} : { solAmountReason: e.solEquivalentReason ?? "noSolRoute" }),
        tokenAmount: e.tokenAmount, pctOfPositionSold: pct != null ? Math.round(pct * 100) / 100 : null, ...(pct == null ? { pctOfPositionSoldReason: reason ?? "unknown" } : {}), positionBeforeTokens: pre, venue: e.venue, receivedAt: e.receivedAt };
      const fire = (a: any) => { st.alerts.push(a); if (st.alerts.length > 50) st.alerts.shift(); for (const x of this.subs) if (x.cfg.allMints || x.set?.has(st.mint)) {
        if (a.type === "devSell" && x.cfg.devSells === false) continue;
        if (a.type === "largeSell" && (x.cfg.largeSellPct == null || a.pctOfPositionSold == null || a.pctOfPositionSold < x.cfg.largeSellPct || (a.solAmount != null && a.solAmount < (x.cfg.minSellSol ?? 0.5)))) continue;
        try { x.cb(a); } catch {} } };
      if (isDev) st.devSoldAt = this.now();
      if (isDev) fire({ type: "devSell", creator, ...base });
      else if (pct != null) fire({ type: "largeSell", ...base });
    })().catch(() => {});
  }
  /** register alert listener; returns stop(). cfg.mints and/or allMints:true */
  watch(cfg: FlowAlertCfg, cb: (a: any) => void) {
    validateFlowCfg(cfg); const x = { cfg, set: cfg.mints ? new Set(cfg.mints) : undefined, cb }; this.subs.add(x);
    return { stop: () => { this.subs.delete(x); }, add: (mint: string) => { (x.set ??= new Set()).add(mint); } };
  }

  // ---- metrics ----
  private win(st: MintStat, w: number) {
    const now = Math.floor(this.now() / 1000), from = now - w;
    let buySol = 0, sellSol = 0, buys = 0, sells = 0, open: number | null = null, openAt = 0;
    for (const b of st.b) { if (b.s + B <= from) continue; buySol += b.buySol; sellSol += b.sellSol; buys += b.buys; sells += b.sells; if (open == null && b.open != null) { open = b.open; openAt = b.s; } }
    const buyers = [...st.buyers.values()].filter((t) => t >= from * 1000).length;
    const close = st.last?.price ?? null;
    let changePct: number | null = null, changeReason: string | undefined;
    const born = st.createdAt != null ? st.createdAt / 1000 : null, seen = st.firstSeen / 1000;
    if (open == null || close == null) changeReason = "noValidPrice";
    else { changePct = r6((close / open - 1) * 100);
      if (openAt > from + B) changeReason = born != null && born > from ? "youngerThanWindow (change since launch)" : seen > from ? "insufficientHistory (change since first seen after startup)" : "noTradesAtWindowStart (change since first trade in window)"; }
    const covered = Math.min(w, now - Math.floor(seen));
    return { volumeSol: r6(buySol + sellSol), buySol: r6(buySol), sellSol: r6(sellSol), netFlowSol: r6(buySol - sellSol), txs: buys + sells, buys, sells, uniqueBuyers: buyers,
      buySellRatio: sellSol > 0 ? r6(buySol / sellSol) : null, ...(sellSol > 0 ? {} : { buySellRatioReason: buySol > 0 ? "noSells" : "noTrades" }),
      buySellCountRatio: sells > 0 ? r6(buys / sells) : null, priceChangePct: changePct, ...(changeReason ? { priceChangeReason: changeReason } : {}), coveredSec: covered, complete: covered >= w };
  }
  flow(mint: any) {
    if (!isPubkey(mint)) throw INVALID_PARAMS("pumpFlow: params [mint]");
    const st = this.stats.get(mint);
    if (!st) return { mint, tracked: false, reason: "noTradesSinceStartup", windows: null, alerts: [] };
    return { mint, tracked: true, since: st.firstSeen, last: st.last ?? null, windows: { "1m": this.win(st, 60), "5m": this.win(st, 300), "15m": this.win(st, 900), "1h": this.win(st, 3600) },
      creator: st.creator, ...(st.creator ? {} : { creatorReason: "unknown until create seen or a dev/large-sell check ran" }), recentAlerts: st.alerts.slice(-20),
      note: "in-memory since startup (or first trade seen); SOL volumes include converted non-SOL quotes, unconvertible ones count in txs only" };
  }
  /** holders / top10 / dev holding (cached); top10 excludes the bonding curve and pool vault owners */
  enrich(mint: string, st?: MintStat) {
    const c = this.enrichCache.get(mint); if (c && this.now() - c.at < (this.o.enrichTtlMs ?? 60_000)) return c.v;
    const v = (async () => {
      const out: any = {};
      const isCurve = st?.last?.venue === "curve";
      const ageP = isCurve || !st ? this.ageOf(mint, st).catch(() => null) : null;
      if (isCurve) { const fh = await this.curveHolders(mint, st).catch((e: any) => ({ err: String(e?.message ?? e).slice(0, 80) }));
        if (!(fh as any).err) { const age = await ageP; if (age) Object.assign(out, age); out.enrichedAt = this.now(); return Object.assign(out, fh); }
        out.fastPathError = (fh as any).err; }
      const [lg, sup] = await Promise.all([this.retry(() => this.rpc("getTokenLargestAccounts", [mint])).catch((e: any) => ({ err: String(e?.message ?? e).slice(0, 80) })), this.retry(() => this.rpc("getTokenSupply", [mint])).catch(() => null)]);
      if (ageP) { const age = await ageP; if (age) Object.assign(out, age); }
      const supply = sup?.value ? Number(sup.value.amount) : 1e15;
      if (lg?.err || !lg?.value) { out.top10Pct = null; out.top10Reason = `largestAccountsFailed: ${lg?.err ?? "empty"}`; out.holderCount = null; out.holderCountReason = "largestAccountsFailed"; }
      else {
        const n = lg._meta?.holdersNonZero; out.holderCount = typeof n === "number" ? n : null; if (out.holderCount == null) out.holderCountReason = "upstreamNativeMethodHasNoCount";
        const accs = lg.value.slice(0, 20); const own = await this.rpc("getMultipleAccounts", [accs.map((a: any) => a.address), { encoding: "jsonParsed" }]).catch(() => null);
        const curve = curvePda(mint); const owners = accs.map((_: any, i: number) => own?.value?.[i]?.data?.parsed?.info?.owner ?? null);
        const pools = new Set<string>(); if (st?.last?.venue !== "curve") for (const o of owners) if (o) { const info = await this.eng.poolInfo(o).catch(() => null); if (info) pools.add(o); }
        const ex = accs.map((a: any, i: number) => ({ raw: Number(a.amount), owner: owners[i], skip: owners[i] === curve || pools.has(owners[i]) }));
        out.curveOrPoolPct = r6(ex.filter((x: any) => x.skip).reduce((a: number, x: any) => a + x.raw, 0) / supply * 100);
        out.top10Pct = r6(ex.filter((x: any) => !x.skip).slice(0, 10).reduce((a: number, x: any) => a + x.raw, 0) / supply * 100);
        if (owners.some((o: any) => o == null)) out.top10Note = "some owners unknown (account fetch failed)";
      }
      const creator = st ? await this.creatorOf(st) : ((await this.eng.curvePrice(mint).catch(() => null)) as any)?.creator ?? null;
      if (!creator) { out.devHoldingPct = null; out.devHoldingReason = "creatorUnknown"; }
      else {
        const t = await this.rpc("getTokenAccountsByOwner", [creator, { mint }, { encoding: "jsonParsed" }]).catch(() => null);
        if (!t) { out.devHoldingPct = null; out.devHoldingReason = "lookupFailed"; }
        else out.devHoldingPct = r6((t.value ?? []).reduce((a: number, x: any) => a + Number(x.account?.data?.parsed?.info?.tokenAmount?.amount ?? 0), 0) / supply * 100);
      }
      out.creator = creator; out.enrichedAt = this.now(); return out;
    })();
    // failed/partial results are kept only 10 s so the next call retries
    v.then((r: any) => { if (r.holderCount == null || r.top10Pct == null || r.devHoldingPct == null) { const c = this.enrichCache.get(mint); if (c?.v === v) c.at = this.now() - (this.o.enrichTtlMs ?? 60_000) + 10_000; } }, () => this.enrichCache.delete(mint));
    this.enrichCache.set(mint, { at: this.now(), v }); if (this.enrichCache.size > 2000) this.enrichCache.delete(this.enrichCache.keys().next().value!);
    void v.catch(() => this.enrichCache.delete(mint));
    return v;
  }
  /** params: [{sort?, limit? (<=100, default 20), venue?: "pump" (default: curve+PumpSwap amm) |"curve"|"amm"|"any" (also amm-other: pump tokens on non-PumpSwap AMMs), minLiquiditySol?, minTxs5m?, enrich? (default true; first 30 rows)}] */
  async trending(q: any = {}) {
    q ??= {}; if (typeof q !== "object") throw INVALID_PARAMS("pumpTrending: params [{sort, limit}]");
    const sort = q.sort ?? "volume5m"; if (!TREND_SORTS.includes(sort)) throw INVALID_PARAMS(`pumpTrending: sort one of ${TREND_SORTS.join(", ")}`);
    const limit = q.limit ?? 20; if (!(Number.isInteger(limit) && limit >= 1 && limit <= (q._internal ? 2000 : 100))) throw INVALID_PARAMS("pumpTrending: limit integer 1..100");
    const venue = q.venue ?? "pump"; if (!["pump", "curve", "amm", "any"].includes(venue)) throw INVALID_PARAMS("pumpTrending: venue pump|curve|amm|any");
    const now = this.now();
    const rows = [...this.stats.values()].filter((st) => st.last && now - st.last.at < 3600_000 && (venue === "any" || (venue === "pump" ? st.last.venue === "curve" || st.last.venue === "amm" : st.last.venue === venue))).map((st) => {
      const m1 = this.win(st, 60), w5 = this.win(st, 300), w1 = this.win(st, 3600), L = st.last!;
      return { st, row: { mint: st.mint, venue: L.venue, quoteMint: L.quoteMint, priceSol: L.priceSol, priceUsd: L.priceUsd, ...(L.priceSol == null ? { priceReason: "noValidPrice" } : {}),
        marketCapSol: L.marketCapSol, marketCapUsd: L.marketCapUsd, ...(L.marketCapSol == null ? { marketCapReason: "noValidPrice" } : {}), liquiditySol: L.liquiditySol, liquidityUsd: L.liquidityUsd, ...(L.liquiditySol == null ? { liquidityReason: L.liquidityReason ?? "unknown" } : {}),
        bondingProgress: L.progress, ...(L.progress == null ? { bondingProgressReason: L.venue === "curve" ? "unknown" : "notPumpSwapPool" } : {}), complete: L.venue !== "curve",
        ageSec: st.createdAt != null ? Math.round((now - st.createdAt) / 1000) : null, ...(st.createdAt == null ? { ageReason: "createNotSeenSinceStartup", firstSeenSec: Math.round((now - st.firstSeen) / 1000) } : {}),
        volumeSol: { "1m": m1.volumeSol, "5m": w5.volumeSol, "1h": w1.volumeSol }, buySol: { "1m": m1.buySol, "5m": w5.buySol, "1h": w1.buySol }, sellSol: { "1m": m1.sellSol, "5m": w5.sellSol, "1h": w1.sellSol },
        txs: { "1m": m1.txs, "5m": w5.txs, "1h": w1.txs }, uniqueBuyers: { "1m": m1.uniqueBuyers, "5m": w5.uniqueBuyers, "1h": w1.uniqueBuyers }, netFlowSol: { "1m": m1.netFlowSol, "5m": w5.netFlowSol, "1h": w1.netFlowSol },
        buySellRatio: { "1m": m1.buySellRatio, "5m": w5.buySellRatio, "1h": w1.buySellRatio },
        priceChangePct: { "1m": m1.priceChangePct, "5m": w5.priceChangePct, "1h": w1.priceChangePct }, ...(m1.priceChangeReason || w5.priceChangeReason || w1.priceChangeReason ? { priceChangeReason: { "1m": m1.priceChangeReason ?? null, "5m": w5.priceChangeReason ?? null, "1h": w1.priceChangeReason ?? null } } : {}),
        windowComplete: { "1m": m1.complete, "5m": w5.complete, "1h": w1.complete } } as any };
    }).filter((x) => (q.minLiquiditySol == null || (x.row.liquiditySol ?? 0) >= q.minLiquiditySol) && (q.minTxs5m == null || x.row.txs["5m"] >= q.minTxs5m));
    const key = (r: any) => ({ volume5m: r.volumeSol["5m"], volume1h: r.volumeSol["1h"], txs5m: r.txs["5m"], txs1h: r.txs["1h"], volume1m: r.volumeSol["1m"], txs1m: r.txs["1m"], buyers1m: r.uniqueBuyers["1m"], netFlow1m: r.netFlowSol["1m"], buyers5m: r.uniqueBuyers["5m"], buyers1h: r.uniqueBuyers["1h"], netFlow5m: r.netFlowSol["5m"], netFlow1h: r.netFlowSol["1h"],
      change5m: r.priceChangePct["5m"], change1h: r.priceChangePct["1h"], marketCap: r.marketCapSol, liquidity: r.liquiditySol } as any)[sort] ?? -Infinity;
    rows.sort((a, b) => key(b.row) - key(a.row));
    const top = rows.slice(0, limit);
    if (q.enrich !== false) await Promise.all(top.slice(0, 30).map(async (x) => { const en = await Promise.race([this.enrich(x.st.mint, x.st).catch(() => null), new Promise((r) => setTimeout(() => r(null), 15_000))]);
      if (en) { Object.assign(x.row, en); if (x.row.ageSec != null) { delete x.row.ageReason; delete x.row.firstSeenSec; } } else Object.assign(x.row, { holderCount: null, holderCountReason: "enrichTimeoutOrError", top10Pct: null, top10Reason: "enrichTimeoutOrError", devHoldingPct: null, devHoldingReason: "enrichTimeoutOrError" }); }));
    for (const x of top.slice(30)) Object.assign(x.row, { holderCount: null, holderCountReason: "notEnriched (rows beyond 30 or enrich:false)", top10Pct: null, top10Reason: "notEnriched", devHoldingPct: null, devHoldingReason: "notEnriched" });
    if (q.enrich === false) for (const x of top) Object.assign(x.row, { holderCount: null, holderCountReason: "notEnriched", top10Pct: null, top10Reason: "notEnriched", devHoldingPct: null, devHoldingReason: "notEnriched" });
    for (const x of top) Object.assign(x.row, this.devSold(x.st));
    return { generatedAt: now, sort, limit, venue, candidates: rows.length, tokens: top.map((x) => x.row), trackedSinceSec: Math.round((now - this.startedAt) / 1000),
      note: "ranked from live swaps seen since startup (pump curve + PumpSwap); 1h windows incomplete until the server has run 1h; holderCount = non-zero token accounts" };
  }
  private async retry<T>(f: () => Promise<T>, n = 2): Promise<T> { let err: any; for (let i = 0; i < n; i++) { try { const r: any = await Promise.race([f(), new Promise((_, rej) => setTimeout(() => rej(new Error("timeout 12s")), 12_000))]); if (r != null) return r; } catch (e) { err = e; } await new Promise((r) => setTimeout(r, this.o.now ? 0 : 400)); } throw err ?? new Error("empty"); }
  private mintProg = new Map<string, string>();
  /** curve tokens have few accounts: one getProgramAccounts (memcmp mint, 40-byte slice owner+amount) gives exact holderCount, top10, dev% and curve share */
  async curveHolders(mint: string, st?: MintStat) {
    let prog = this.mintProg.get(mint);
    if (!prog) { const a = await this.retry(() => this.rpc("getAccountInfo", [mint, { encoding: "base64", dataSlice: { offset: 0, length: 0 } }])); prog = a?.value?.owner; if (!prog) throw new Error("mintNotFound"); this.mintProg.set(mint, prog); }
    const [accs, cp] = await Promise.all([this.retry(() => this.rpc("getProgramAccounts", [prog, { encoding: "base64", dataSlice: { offset: 32, length: 40 }, filters: [{ memcmp: { offset: 0, bytes: mint } }] }])),
      st ? this.creatorOf(st).then((c) => ({ creator: c })) : this.eng.curvePrice(mint).catch(() => null)]);
    const list: any[] = Array.isArray(accs) ? accs : accs?.value ?? [];
    if (!list.length) throw new Error("noTokenAccounts");
    const byOwner = new Map<string, number>(); let total = 0;
    for (const a of list) { const d = Buffer.from(a.account?.data?.[0] ?? "", "base64"); if (d.length < 40) continue; const amt = Number(d.readBigUInt64LE(32)); if (amt <= 0) continue; const o = b58encode(d.subarray(0, 32)); byOwner.set(o, (byOwner.get(o) ?? 0) + amt); total += amt; }
    const supply = 1e15; // pump.fun fixed 1B supply, 6 decimals (curve-era tokens; nothing burnt before migration)
    const curve = curvePda(mint), creator = (cp as any)?.creator ?? null;
    const holders = [...byOwner.entries()].filter(([o]) => o !== curve).sort((a, b) => b[1] - a[1]);
    return { holderCount: holders.length, holderCountBasis: "nonZeroOwnersExclCurve", top10Pct: r6(holders.slice(0, 10).reduce((a, x) => a + x[1], 0) / supply * 100), curveOrPoolPct: r6((byOwner.get(curve) ?? 0) / supply * 100),
      creator, ...(creator ? { devHoldingPct: r6((byOwner.get(creator) ?? 0) / supply * 100) } : { devHoldingPct: null, devHoldingReason: "creatorUnknown" }), tokenAccounts: list.length, enrichSource: "gpaCurve" };
  }
  private ageCache = new Map<string, any>();
  /** creation time from the mint's oldest signature (<= 5 pages of 1000), cached; also counts txs in the create slot (+2 slots) = early/bundled buys */
  async ageOf(mint: string, st?: MintStat) {
    if (st?.ageInfo) return this.ageView(st.ageInfo);
    if (st?.createdAt != null && st.createSlot != null) return { ageSec: Math.round((this.now() - st.createdAt) / 1000), ageSource: "createSeenLive", createSlot: st.createSlot };
    const c = this.ageCache.get(mint); if (c) return this.ageView(c);
    let before: string | undefined, oldest: any[] = [], pages = 0;
    for (; pages < 5; pages++) { const r: any[] = await this.retry(() => this.rpc("getSignaturesForAddress", [mint, { limit: 1000, ...(before ? { before } : {}) }])); if (!r.length) break; oldest = r; before = r[r.length - 1].signature; if (r.length < 1000) { pages++; break; } }
    if (!oldest.length) throw new Error("noSignatures");
    const complete = oldest.length < 1000, o = oldest[oldest.length - 1];
    const v: any = { createdAtSec: o.blockTime ?? null, createSlot: o.slot, complete, createSignature: complete ? o.signature : null,
      createSlotTxs: complete ? oldest.filter((x) => x.slot === o.slot).length - 1 : null, earlySlotTxs: complete ? oldest.filter((x) => x.slot - o.slot <= 2).length - 1 : null };
    this.ageCache.set(mint, v); if (this.ageCache.size > 5000) this.ageCache.delete(this.ageCache.keys().next().value!);
    if (st && complete && v.createdAtSec) { st.createdAt = v.createdAtSec * 1000; st.createSlot ??= o.slot; st.ageInfo = v; }
    return this.ageView(v);
  }
  private ageView(v: any) {
    if (!v.complete) return { ageSec: null, ageReason: "moreThan5000Signatures (create tx not reached)", ageSecMin: v.createdAtSec ? Math.round(this.now() / 1000 - v.createdAtSec) : null };
    return { ageSec: v.createdAtSec ? Math.round(this.now() / 1000 - v.createdAtSec) : null, ageSource: "oldestSignature", createSlot: v.createSlot, createSlotTxs: v.createSlotTxs, earlySlotTxs: v.earlySlotTxs };
  }
  private walletCache = new Map<string, { at: number; fresh: boolean }>();
  /** sample recent buyers: fresh = < 10 lifetime signatures or first signature < 24h old */
  async freshRatio(st: MintStat, sample = 6) {
    const ws = [...st.buyers.entries()].sort((a, b) => b[1] - a[1]).slice(0, sample).map((x) => x[0]);
    if (!ws.length) return { freshWalletRatio: null, freshWalletReason: "noBuyers" };
    const res = await Promise.all(ws.map(async (w) => { const c = this.walletCache.get(w); if (c && this.now() - c.at < 600_000) return c.fresh;
      const r: any[] = await this.rpc("getSignaturesForAddress", [w, { limit: 50 }]).catch(() => null) as any; if (!Array.isArray(r)) return null;
      const fresh = r.length < 10 || (r.length < 50 && (r[r.length - 1]?.blockTime ?? 0) > this.now() / 1000 - 86400);
      this.walletCache.set(w, { at: this.now(), fresh }); if (this.walletCache.size > 20000) this.walletCache.delete(this.walletCache.keys().next().value!); return fresh; }));
    const ok = res.filter((x) => x != null); if (!ok.length) return { freshWalletRatio: null, freshWalletReason: "lookupFailed" };
    return { freshWalletRatio: r6(ok.filter(Boolean).length / ok.length), freshWalletSample: ok.length };
  }
  /** bonding velocity (%/min) over the last 5 min from curve progress samples */
  bonding(st: MintStat) {
    const now = Math.floor(this.now() / 1000); const pts = st.b.filter((b) => b.prog != null && b.s >= now - 300);
    const p = st.last?.progress ?? null;
    if (p == null) return { bondingVelocityPctPerMin: null, bondingEtaSec: null, bondingEtaReason: "noProgress" };
    if (pts.length < 2 || pts[pts.length - 1].s - pts[0].s < 30) return { bondingVelocityPctPerMin: null, bondingEtaSec: null, bondingEtaReason: "insufficientHistory (<30s of curve trades in 5m)" };
    const v = ((pts[pts.length - 1].prog! - pts[0].prog!) / (pts[pts.length - 1].s - pts[0].s)) * 60;
    if (!(v > 0)) return { bondingVelocityPctPerMin: r6(v), bondingEtaSec: null, bondingEtaReason: "notRising (velocity <= 0)" };
    return { bondingVelocityPctPerMin: r6(v), bondingEtaSec: Math.round(((100 - p) / v) * 60) };
  }
  /** repeat/wash: share of 5m txs from wallets with >= 3 txs, and wallets that both bought and sold */
  washStats(st: MintStat) {
    const now = this.now(); let tot = 0, rep = 0, both = 0;
    for (const x of st.wtx?.values() ?? []) { if (now - x.at > 300_000) continue; const n = x.b + x.s; tot += n; if (n >= 3) rep += n; if (x.b > 0 && x.s > 0) both++; }
    let wallets = 0; for (const x of st.wtx?.values() ?? []) if (now - x.at <= 300_000) wallets++;
    return { repeatWalletTxRatio: tot ? r6(rep / tot) : null, roundTripWallets: both, roundTripRatio: wallets ? r6(both / wallets) : null, txs: tot };
  }
  async curveScreener(q: any = {}) {
    q = q ?? {}; if (typeof q !== "object" || Array.isArray(q)) throw INVALID_PARAMS("pumpCurveScreener: params [{...}]");
    const num = (k: string, lo = -Infinity, hi = Infinity) => { const v = q[k]; if (v == null) return undefined; if (typeof v !== "number" || !(v >= lo && v <= hi)) throw INVALID_PARAMS(`pumpCurveScreener: ${k} must be a number in [${lo},${hi}]`); return v; };
    const f = { minBonding: num("minBonding", 0, 100), maxBonding: num("maxBonding", 0, 100), minAgeSec: num("minAgeSec", 0), maxAgeSec: num("maxAgeSec", 0), minUniqueBuyers5m: num("minUniqueBuyers5m", 0), minBuySellRatio: num("minBuySellRatio", 0),
      minNetFlowSol: num("minNetFlowSol"), maxTop10Pct: num("maxTop10Pct", 0, 100), maxDevHoldingPct: num("maxDevHoldingPct", 0, 100), minHolderCount: num("minHolderCount", 0), minScore: num("minScore", 0, 100) };
    const limit = num("limit", 1, 50) ?? 20; const sort = q.sort ?? "score";
    const SORTS = ["score", "bonding", "eta", "ageAsc", ...TREND_SORTS]; if (!SORTS.includes(sort)) throw INVALID_PARAMS(`pumpCurveScreener: sort one of ${SORTS.join(",")}`);
    if (q._validateOnly) return null as any;
    const tr = await this.trending({ venue: "curve", sort: TREND_SORTS.includes(sort) ? sort : "buyers5m", limit: 2000, enrich: false, _internal: true });
    const t0 = this.now();
    let rows = tr.tokens.filter((r: any) => !r.complete && (f.minBonding == null || (r.bondingProgress ?? -1) >= f.minBonding) && (f.maxBonding == null || (r.bondingProgress ?? 101) <= f.maxBonding)
      && (f.minUniqueBuyers5m == null || r.uniqueBuyers["5m"] >= f.minUniqueBuyers5m) && (f.minNetFlowSol == null || r.netFlowSol["5m"] >= f.minNetFlowSol)
      && (f.minBuySellRatio == null || (r.buySellRatio["5m"] ?? (r.buySol["5m"] > 0 ? Infinity : 0)) >= f.minBuySellRatio) && !(q.excludeDevSold && r.devSold === true));
    // cheap pre-rank, then enrich up to 40 (holders/top10/dev/age/fresh) and apply the rest
    const pre = (r: any) => this.score(r, true); rows.sort((a: any, b: any) => pre(b) - pre(a)); rows = rows.slice(0, Math.max(limit * 2, 40));
    await Promise.all(rows.map(async (r: any) => { const st = this.stats.get(r.mint)!;
      const [en, fr] = await Promise.all([Promise.race([this.enrich(r.mint, st).catch(() => null), new Promise((x) => setTimeout(() => x(null), 20_000))]), q.freshWallets === false ? null : this.freshRatio(st).catch(() => null)]);
      if (en) Object.assign(r, en); else Object.assign(r, { holderCount: null, holderCountReason: "enrichTimeoutOrError", top10Pct: null, top10Reason: "enrichTimeoutOrError", devHoldingPct: null, devHoldingReason: "enrichTimeoutOrError" });
      if (r.ageSec != null) { delete r.ageReason; delete r.firstSeenSec; }
      for (const [k, why] of [["holderCount", "holderCountReason"], ["top10Pct", "top10Reason"], ["devHoldingPct", "devHoldingReason"]]) if (r[k] != null) delete r[why];
      Object.assign(r, this.devSold(st), this.bonding(st), fr ?? { freshWalletRatio: null, freshWalletReason: q.freshWallets === false ? "disabled" : "lookupFailed" });
      const w = this.washStats(st); const early = st.earlyBuys ?? r.earlySlotTxs ?? null, cs = st.createSlotBuys ?? r.createSlotTxs ?? null;
      r.flags = { devSold: r.devSold, bundled: early == null ? null : early >= 5 || (cs ?? 0) >= 3, createSlotBuys: cs, earlyBuys3Slots: early, ...(early == null ? { bundledReason: "createNotSeen (age lookup incomplete)" } : {}),
        top10High: r.top10Pct == null ? null : r.top10Pct > 30, washTrade: w.repeatWalletTxRatio == null ? null : w.txs >= 20 && (w.repeatWalletTxRatio > 0.5 || (w.roundTripRatio ?? 0) > 0.3), repeatWalletTxRatio: w.repeatWalletTxRatio, roundTripWallets5m: w.roundTripWallets, roundTripRatio: w.roundTripRatio,
        freshWalletRatio: r.freshWalletRatio, freshHeavy: r.freshWalletRatio == null ? null : r.freshWalletRatio > 0.6 };
      r.score = this.score(r); }));
    rows = rows.filter((r: any) => (f.maxTop10Pct == null || (r.top10Pct != null && r.top10Pct <= f.maxTop10Pct)) && (f.maxDevHoldingPct == null || (r.devHoldingPct != null && r.devHoldingPct <= f.maxDevHoldingPct))
      && (f.minHolderCount == null || (r.holderCount ?? -1) >= f.minHolderCount) && (f.minAgeSec == null || (r.ageSec != null && r.ageSec >= f.minAgeSec)) && (f.maxAgeSec == null || (r.ageSec != null && r.ageSec <= f.maxAgeSec))
      && !(q.excludeDevSold && r.devSold === true) && (f.minScore == null || r.score >= f.minScore));
    const key: any = ({ score: (r: any) => r.score, bonding: (r: any) => r.bondingProgress ?? -1, eta: (r: any) => -(r.bondingEtaSec ?? Infinity), ageAsc: (r: any) => -(r.ageSec ?? Infinity) } as any)[sort];
    if (key) rows.sort((a: any, b: any) => key(b) - key(a));
    return { generatedAt: this.now(), sort, limit, filters: f, candidates: tr.candidates, afterFilters: rows.length, tokens: rows.slice(0, limit), enrichMs: this.now() - t0, trackedSinceSec: tr.trackedSinceSec,
      scoreFormula: SCORE_FORMULA, note: "curve-only (complete=false); metrics in memory since startup; filters needing enrichment drop rows whose value is null" };
  }
  /** see SCORE_FORMULA */
  score(r: any, cheap = false) {
    const cl = (x: number) => Math.max(0, Math.min(1, x));
    const bsr = r.buySellRatio?.["5m"] ?? (r.buySol?.["5m"] > 0 ? 3 : 0);
    let s = 25 * cl((r.uniqueBuyers?.["5m"] ?? 0) / 50) + 20 * cl((r.netFlowSol?.["5m"] ?? 0) / 10) + 15 * cl((bsr - 1) / 2);
    if (cheap) return s;
    s += 15 * cl((r.bondingVelocityPctPerMin ?? 0) / 2) + 10 * cl((r.holderCount ?? 0) / 200) + 15 * cl(1 - Math.abs((r.bondingProgress ?? 0) - 60) / 60);
    if (r.top10Pct != null && r.top10Pct > 30) s -= Math.min(15, (r.top10Pct - 30) / 2);
    if (r.devSold) s -= 20; if ((r.devHoldingPct ?? 0) > 10) s -= 10;
    const fl = r.flags ?? {}; if (fl.bundled) s -= 10; if (fl.washTrade) s -= 10; if (fl.freshHeavy) s -= 5;
    return Math.round(Math.max(0, Math.min(100, s)) * 10) / 10;
  }
  startedAt: number; lastSwapAt = 0;
  /** devSold: creator sold since startup (sell seen from creator wallet, checked against sellers seen in the last hour once creator is known) */
  private devSold(st: MintStat) {
    if (st.creator && !st.devSoldAt && st.sellers?.has(st.creator)) st.devSoldAt = st.sellers.get(st.creator);
    if (st.devSoldAt) return { devSold: true, devSoldAgoSec: Math.round((this.now() - st.devSoldAt) / 1000) };
    if (!st.creator) return { devSold: null, devSoldReason: "creatorUnknown" };
    return { devSold: false, devSoldReason: "noCreatorSellSeenSinceStartup" };
  }
}
export function validateFlowCfg(c: any): FlowAlertCfg {
  if (c == null || typeof c !== "object") throw INVALID_PARAMS("flow alerts: params must be an object");
  if (c.mints !== undefined && (!Array.isArray(c.mints) || c.mints.length > 2000 || !c.mints.every(isPubkey))) throw INVALID_PARAMS("flow alerts: mints must be base58 pubkeys (max 2000)");
  if (c.largeSellPct !== undefined && !(typeof c.largeSellPct === "number" && c.largeSellPct > 0 && c.largeSellPct <= 100)) throw INVALID_PARAMS("largeSellPct in (0,100]");
  if (c.minSellSol !== undefined && !(typeof c.minSellSol === "number" && c.minSellSol >= 0)) throw INVALID_PARAMS("minSellSol >= 0");
  return c;
}
