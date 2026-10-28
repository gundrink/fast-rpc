// Trending tokens + per-mint flow metrics + large-seller / dev-sell alerts on top of PumpEngine (all swaps, in memory since startup).
import { PumpEngine, curvePda, solOf, type PumpSwapEvent } from "./pump.js";
import { INVALID_PARAMS } from "./errors.js";
import { isPubkey } from "./codec.js";

type Rpc = (m: string, p: any[]) => Promise<any>;
const B = 10; // seconds per bucket
const r6 = (x: number) => Math.round(x * 1e6) / 1e6;
interface Bucket { s: number; buySol: number; sellSol: number; buys: number; sells: number; open: number | null; close: number | null }
interface MintStat {
  mint: string; b: Bucket[]; buyers: Map<string, number>; firstSeen: number; createdAt: number | null; creator: string | null; creatorChecked?: number; sellers?: Map<string, number>; devSoldAt?: number;
  last?: { price: number | null; priceUsd: number | null; marketCapSol: number | null; marketCapUsd: number | null; liquiditySol: number | null; liquidityUsd: number | null; liquidityReason?: string; progress: number | null; venue: string; quoteMint: string | null; at: number; priceSol: number | null };
  alerts: any[];
}
export const TREND_SORTS = ["volume1m", "txs1m", "buyers1m", "netFlow1m", "volume5m", "volume1h", "txs5m", "txs1h", "buyers5m", "buyers1h", "netFlow5m", "netFlow1h", "change5m", "change1h", "marketCap", "liquidity"] as const;
export interface FlowAlertCfg { mints?: string[]; allMints?: boolean; largeSellPct?: number; minSellSol?: number; devSells?: boolean }
export interface FlowOptions { maxMints?: number; txPerMin?: number; enrichTtlMs?: number; now?: () => number }

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
    if (e.create) { st.createdAt = (e.blockTime ?? sec) * 1000; st.creator ??= e.wallet; }
    let b = st.b[st.b.length - 1]; if (!b || b.s !== bs) { b = { s: bs, buySol: 0, sellSol: 0, buys: 0, sells: 0, open: null, close: null }; st.b.push(b); while (st.b[0].s < bs - 3600) st.b.shift(); }
    const v = solOf(e); const conv = e.solEquivalent != null || !e.quoteMint;
    if (conv) { if (e.side === "buy") b.buySol += v; else b.sellSol += v; }
    if (e.side === "buy") { b.buys++; st.buyers.set(e.wallet, now); if (st.buyers.size > 3000) st.buyers.delete(st.buyers.keys().next().value!); } else { b.sells++; (st.sellers ??= new Map()).set(e.wallet, now); if (st.sellers.size > 3000) st.sellers.delete(st.sellers.keys().next().value!); if (st.creator && e.wallet === st.creator) st.devSoldAt = now; }
    this.lastSwapAt = now;
    const px = e.quoteMint ? e.priceSol ?? null : e.priceValid ? e.price : null;
    if (px != null && px > 0 && (e.venue !== "amm-other" || e.quoteMint)) { b.open ??= px; b.close = px; }
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
      const [lg, sup] = await Promise.all([this.rpc("getTokenLargestAccounts", [mint]).catch((e: any) => ({ err: String(e?.message ?? e).slice(0, 80) })), this.rpc("getTokenSupply", [mint]).catch(() => null)]);
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
    this.enrichCache.set(mint, { at: this.now(), v }); if (this.enrichCache.size > 2000) this.enrichCache.delete(this.enrichCache.keys().next().value!);
    void v.catch(() => this.enrichCache.delete(mint));
    return v;
  }
  /** params: [{sort?, limit? (<=100, default 20), venue?: "pump" (default: curve+PumpSwap amm) |"curve"|"amm"|"any" (also amm-other: pump tokens on non-PumpSwap AMMs), minLiquiditySol?, minTxs5m?, enrich? (default true; first 30 rows)}] */
  async trending(q: any = {}) {
    q ??= {}; if (typeof q !== "object") throw INVALID_PARAMS("pumpTrending: params [{sort, limit}]");
    const sort = q.sort ?? "volume5m"; if (!TREND_SORTS.includes(sort)) throw INVALID_PARAMS(`pumpTrending: sort one of ${TREND_SORTS.join(", ")}`);
    const limit = q.limit ?? 20; if (!(Number.isInteger(limit) && limit >= 1 && limit <= 100)) throw INVALID_PARAMS("pumpTrending: limit integer 1..100");
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
      if (en) Object.assign(x.row, en); else Object.assign(x.row, { holderCount: null, holderCountReason: "enrichTimeoutOrError", top10Pct: null, top10Reason: "enrichTimeoutOrError", devHoldingPct: null, devHoldingReason: "enrichTimeoutOrError" }); }));
    for (const x of top.slice(30)) Object.assign(x.row, { holderCount: null, holderCountReason: "notEnriched (rows beyond 30 or enrich:false)", top10Pct: null, top10Reason: "notEnriched", devHoldingPct: null, devHoldingReason: "notEnriched" });
    if (q.enrich === false) for (const x of top) Object.assign(x.row, { holderCount: null, holderCountReason: "notEnriched", top10Pct: null, top10Reason: "notEnriched", devHoldingPct: null, devHoldingReason: "notEnriched" });
    for (const x of top) Object.assign(x.row, this.devSold(x.st));
    return { generatedAt: now, sort, limit, venue, candidates: rows.length, tokens: top.map((x) => x.row), trackedSinceSec: Math.round((now - this.startedAt) / 1000),
      note: "ranked from live swaps seen since startup (pump curve + PumpSwap); 1h windows incomplete until the server has run 1h; holderCount = non-zero token accounts" };
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
