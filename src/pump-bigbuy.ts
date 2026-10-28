// Big-buy listener: every buy >= minSol on pump.fun curve / PumpSwap, with wallet insight (creator, wallet age, repeat behavior,
// in-memory position PnL on mints it big-bought, and a win rate on prior big buys). Persisted to a JSON file.
import fs from "node:fs"; import path from "node:path";
import { solOf, solAmountOf, type PumpEngine, type PumpSwapEvent } from "./pump.js";
import type { PumpFlow } from "./pump-flow.js";
import { INVALID_PARAMS } from "./errors.js";
import { isPubkey } from "./codec.js";

type Rpc = (m: string, p: any[]) => Promise<any>;
const r6 = (x: number) => Math.round(x * 1e6) / 1e6;
export interface BigBuy { mint: string; wallet: string; solAmount: number; tokenAmount: number; priceSol: number | null; venue: string; signature: string | null; slot: number; blockTime: number | null; marketCapSol: number | null; bondingProgress: number | null; at: number; isCreator?: boolean | null; outcome?: { win: boolean; changePct: number; settledAt: number } }
interface Pos { tokens: number; cost: number; realized: number; bought: number; sold: number; untrackedSells: number }
interface WStat { bigBuys: number; totalSol: number; firstAt: number; lastAt: number; mints: Record<string, number>; pos: Record<string, Pos>; wins: number; losses: number }
export interface BigBuyOptions { dataFile?: string; recordMinSol?: number; evalSec?: number; ageLookupsPerMin?: number; keepMs?: number; maxBuys?: number; saveMs?: number; now?: () => number }
export interface BigBuySubParams { minSol?: number; venue?: "pump" | "curve" | "amm" | "any"; mints?: string[]; excludeWallets?: string[] }

const venueOk = (v: string, f = "pump") => f === "any" || (f === "pump" ? v === "curve" || v === "amm" : v === f);

export class PumpBigBuys {
  buys: BigBuy[] = []; wallets = new Map<string, WStat>(); private ages = new Map<string, { at: number; firstSec: number | null; sigs: number; complete: boolean }>();
  private pending = new Map<string, BigBuy[]>(); private subs = new Set<{ p: BigBuySubParams; cb: (x: any) => void; mints?: Set<string>; ex?: Set<string> }>();
  private now: () => number; private ageBudget = { at: 0, n: 0 }; private dirty = false; private timer?: NodeJS.Timeout; private hooked = false;
  readonly recordMinSol: number;
  constructor(private eng: PumpEngine, private rpc: Rpc, private flow?: PumpFlow, private o: BigBuyOptions = {}) {
    this.now = o.now ?? Date.now; this.recordMinSol = o.recordMinSol ?? Number(process.env.PUMP_BIGBUY_RECORD_MIN_SOL ?? 0.5); this.load();
  }
  start() {
    if (!this.hooked) { this.eng.hooks.push((e) => this.onSwap(e)); this.hooked = true; }
    if (this.o.dataFile && !this.timer) { this.timer = setInterval(() => this.save(), this.o.saveMs ?? 60_000); this.timer.unref?.(); }
    return this;
  }
  close() { clearInterval(this.timer); this.timer = undefined; this.save(); }

  onSwap(e: PumpSwapEvent) {
    if (!e.mint || e.update || e.dust || !e.wallet) return;
    const now = this.now(); const sol = solAmountOf(e).sol;
    const px = e.quoteMint ? (e as any).priceSol ?? null : e.priceValid ? e.price : null;
    // settle outcomes of prior big buys on this mint (first valid price >= evalSec after the buy)
    const pend = this.pending.get(e.mint);
    if (pend && px != null) { const ev = (this.o.evalSec ?? 900) * 1000;
      while (pend.length && now - pend[0].at >= ev) { const b = pend.shift()!; if (b.priceSol) { const ch = (px / b.priceSol - 1) * 100; b.outcome = { win: ch > 0, changePct: r6(ch), settledAt: now }; const w = this.wallets.get(b.wallet); if (w) { if (ch > 0) w.wins++; else w.losses++; } this.dirty = true; } }
      if (!pend.length) this.pending.delete(e.mint); }
    // positions for wallets we already track (only on mints they big-bought)
    const w0 = this.wallets.get(e.wallet); const p0 = w0?.pos[e.mint];
    if (p0 && sol != null) {
      if (e.side === "buy") { p0.tokens += e.tokenAmount; p0.cost += sol; p0.bought += sol; }
      else { const sell = Math.min(e.tokenAmount, p0.tokens); if (e.tokenAmount > p0.tokens) p0.untrackedSells += e.tokenAmount - p0.tokens;
        if (sell > 0) { const avg = p0.cost / p0.tokens; p0.realized += sol * (sell / e.tokenAmount) - avg * sell; p0.cost -= avg * sell; p0.tokens -= sell; } p0.sold += sol; }
      this.dirty = true;
    }
    if (e.side !== "buy" || sol == null || sol < this.recordMinSol || !venueOk(e.venue, "any")) return;
    if (p0 && sol != null) { /* already added to position above */ }
    const st = this.flow?.stats.get(e.mint);
    const b: BigBuy = { mint: e.mint, wallet: e.wallet, solAmount: r6(sol), tokenAmount: e.tokenAmount, priceSol: px, venue: e.venue, signature: e.signature, slot: e.slot, blockTime: e.blockTime ?? null,
      marketCapSol: e.marketCapSol ?? null, bondingProgress: e.venue === "curve" ? e.progress ?? null : e.venue === "amm" ? 100 : null, at: now };
    // prior stats snapshot BEFORE counting this buy
    const prior = this.walletView(e.wallet, e.mint, now);
    let w = w0; if (!w) { w = { bigBuys: 0, totalSol: 0, firstAt: now, lastAt: now, mints: {}, pos: {}, wins: 0, losses: 0 }; this.wallets.set(e.wallet, w); }
    w.bigBuys++; w.totalSol = r6(w.totalSol + sol); w.lastAt = now; w.mints[e.mint] = (w.mints[e.mint] ?? 0) + 1;
    if (!w.pos[e.mint]) w.pos[e.mint] = { tokens: e.tokenAmount, cost: sol, realized: 0, bought: sol, sold: 0, untrackedSells: 0 };
    this.buys.push(b); const keep = this.o.keepMs ?? 86_400_000; while (this.buys.length && (now - this.buys[0].at > keep || this.buys.length > (this.o.maxBuys ?? 50_000))) this.buys.shift();
    if (px) { const l = this.pending.get(e.mint) ?? []; l.push(b); this.pending.set(e.mint, l); }
    this.dirty = true;
    const targets = [...this.subs].filter((s) => sol >= (s.p.minSol ?? 1) && venueOk(e.venue, s.p.venue ?? "pump") && (!s.mints || s.mints.has(b.mint)) && !s.ex?.has(e.wallet));
    if (!targets.length) return;
    void this.insight(b, st, prior).then((x) => { for (const s of targets) try { s.cb(x); } catch {} });
  }
  private async creator(mint: string, st: any) {
    if (st?.creator) return st.creator; const c: any = await this.eng.curvePrice(mint).catch(() => null); return c?.creator ?? null;
  }
  private async insight(b: BigBuy, st: any, prior: any) {
    const [creator, age] = await Promise.all([Promise.race([this.creator(b.mint, st), new Promise((r) => setTimeout(() => r(undefined), 3000))]), Promise.race([this.walletAge(b.wallet), new Promise((r) => setTimeout(() => r({ walletAgeSec: null, freshWallet: null, walletAgeReason: "lookupPending (>3s)" }), 3000))])]);
    b.isCreator = creator === undefined || creator === null ? null : creator === b.wallet;
    return { type: "bigBuy", ...b, at: undefined, receivedAt: b.at, isCreator: b.isCreator, ...(b.isCreator == null ? { isCreatorReason: creator === undefined ? "creatorLookupTimeout" : "creatorUnknown (migrated or lookup failed)" } : {}), ...(age as any), prior };
  }
  /** wallet age from its oldest signature (<=1000 sigs scanned); fresh = < 24h old or < 10 signatures. Budgeted (default 150/min, PUMP_BIGBUY_AGE_PER_MIN), cached 6h. */
  async walletAge(wallet: string) {
    const c = this.ages.get(wallet); const now = this.now();
    if (!c || now - c.at > 6 * 3600_000) {
      if (now - this.ageBudget.at > 60_000) this.ageBudget = { at: now, n: 0 };
      if (this.ageBudget.n >= (this.o.ageLookupsPerMin ?? Number(process.env.PUMP_BIGBUY_AGE_PER_MIN ?? 150))) return { walletAgeSec: null, freshWallet: null, walletAgeReason: "lookupBudgetExhausted" };
      this.ageBudget.n++;
      const r: any[] | null = await this.rpc("getSignaturesForAddress", [wallet, { limit: 1000 }]).catch(() => null);
      if (!Array.isArray(r)) return { walletAgeSec: null, freshWallet: null, walletAgeReason: "lookupFailed" };
      this.ages.set(wallet, { at: now, firstSec: r.length ? r[r.length - 1].blockTime ?? null : null, sigs: r.length, complete: r.length < 1000 }); this.dirty = true;
      if (this.ages.size > 50_000) this.ages.delete(this.ages.keys().next().value!);
    }
    const a = this.ages.get(wallet)!; const ageSec = a.firstSec != null ? Math.round(now / 1000 - a.firstSec) : null;
    return { walletAgeSec: a.complete ? ageSec : null, ...(a.complete ? {} : { walletAgeSecMin: ageSec, walletAgeReason: "moreThan1000Signatures" }), walletSignatures: a.complete ? a.sigs : `>=${a.sigs}`,
      freshWallet: a.sigs < 10 || (a.complete && ageSec != null && ageSec < 86400) };
  }
  /** wallet stats as known before/at `now`: big-buy history, repeat behavior, position PnL, win rate */
  walletView(wallet: string, mint?: string, now = this.now()) {
    const w = this.wallets.get(wallet);
    if (!w) return { pastBigBuys: 0, note: "first big buy seen from this wallet (since stats start)" };
    const recent = this.buys.filter((b) => b.wallet === wallet && now - b.at <= 3600_000);
    let realized = 0, unrealized = 0, unknownPx = 0, open = 0;
    for (const [m, p] of Object.entries(w.pos)) { realized += p.realized; if (p.tokens > 0) { open++; const px = this.flow?.stats.get(m)?.last?.priceSol; if (px != null) unrealized += p.tokens * px - p.cost; else unknownPx++; } }
    const settled = w.wins + w.losses;
    return { pastBigBuys: w.bigBuys, pastBigBuySol: r6(w.totalSol), firstBigBuyAt: w.firstAt, lastBigBuyAt: w.lastAt, distinctMints: Object.keys(w.mints).length,
      bigBuysLast1h: recent.length, mintsLast1h: new Set(recent.map((b) => b.mint)).size, ...(mint ? { priorBigBuysThisMint: w.mints[mint] ?? 0 } : {}),
      realizedPnlSol: r6(realized), unrealizedPnlSol: unknownPx === open && open > 0 ? null : r6(unrealized), ...(unknownPx ? { unrealizedNote: `${unknownPx} open positions without a current price` } : {}), openPositions: open,
      winRate: settled ? r6(w.wins / settled) : null, ...(settled ? {} : { winRateReason: "noSettledBigBuys (settles " + (this.o.evalSec ?? 900) / 60 + " min after each buy)" }), wins: w.wins, losses: w.losses };
  }
  subscribe(p: BigBuySubParams, cb: (x: any) => void) {
    validateBigBuySub(p, this.recordMinSol); const s = { p, cb, mints: p.mints?.length ? new Set(p.mints) : undefined, ex: p.excludeWallets?.length ? new Set(p.excludeWallets) : undefined };
    this.subs.add(s); return () => this.subs.delete(s);
  }
  private win(q: any, name: string) {
    q = q ?? {}; if (typeof q !== "object" || Array.isArray(q)) throw INVALID_PARAMS(`${name}: params [{...}]`);
    const minSol = q.minSol ?? 1; if (typeof minSol !== "number" || minSol < this.recordMinSol) throw INVALID_PARAMS(`${name}: minSol >= ${this.recordMinSol} (recording threshold)`);
    const windowSec = q.windowSec ?? 3600; if (typeof windowSec !== "number" || windowSec <= 0 || windowSec > 86400) throw INVALID_PARAMS(`${name}: windowSec in (0,86400]`);
    const limit = q.limit ?? 50; if (!Number.isInteger(limit) || limit < 1 || limit > 500) throw INVALID_PARAMS(`${name}: limit 1..500`);
    const venue = q.venue ?? "pump"; if (!["pump", "curve", "amm", "any"].includes(venue)) throw INVALID_PARAMS(`${name}: venue pump|curve|amm|any`);
    const from = this.now() - windowSec * 1000; return { q, minSol, windowSec, limit, venue, list: this.buys.filter((b) => b.at >= from && b.solAmount >= minSol && venueOk(b.venue, venue)) };
  }
  /** params [{minSol=1, windowSec=3600, mint?, wallet?, venue?, limit=50}] newest first */
  recent(q: any) {
    const { minSol, windowSec, limit, venue, list } = this.win(q, "pumpBigBuys");
    if (q?.mint != null && !isPubkey(q.mint)) throw INVALID_PARAMS("pumpBigBuys: mint must be a pubkey"); if (q?.wallet != null && !isPubkey(q.wallet)) throw INVALID_PARAMS("pumpBigBuys: wallet must be a pubkey");
    const l = list.filter((b) => (!q?.mint || b.mint === q.mint) && (!q?.wallet || b.wallet === q.wallet));
    return { minSol, windowSec, venue, total: l.length, totalSol: r6(l.reduce((a, b) => a + b.solAmount, 0)), buys: l.slice(-limit).reverse().map(({ at, ...b }) => ({ ...b, receivedAt: at })), statsSince: this.statsSince };
  }
  /** params [{minSol=1, windowSec=3600, venue?, limit=50, sort: count|totalSol|winRate|realizedPnl|lastAt}] */
  rank(q: any) {
    const { minSol, windowSec, limit, venue, list } = this.win(q, "pumpBigBuyers"); const sort = q?.sort ?? "totalSol";
    if (!["count", "totalSol", "winRate", "realizedPnl", "lastAt"].includes(sort)) throw INVALID_PARAMS("pumpBigBuyers: sort count|totalSol|winRate|realizedPnl|lastAt");
    const by = new Map<string, { wallet: string; count: number; totalSol: number; mints: Set<string>; lastAt: number; maxSol: number }>();
    for (const b of list) { let x = by.get(b.wallet); if (!x) by.set(b.wallet, (x = { wallet: b.wallet, count: 0, totalSol: 0, mints: new Set(), lastAt: 0, maxSol: 0 })); x.count++; x.totalSol += b.solAmount; x.mints.add(b.mint); x.lastAt = Math.max(x.lastAt, b.at); x.maxSol = Math.max(x.maxSol, b.solAmount); }
    const rows = [...by.values()].map((x) => { const v: any = this.walletView(x.wallet); return { wallet: x.wallet, count: x.count, totalSol: r6(x.totalSol), maxSol: x.maxSol, distinctMints: x.mints.size, lastAt: x.lastAt,
      allTime: { bigBuys: v.pastBigBuys, bigBuySol: v.pastBigBuySol }, realizedPnlSol: v.realizedPnlSol ?? 0, unrealizedPnlSol: v.unrealizedPnlSol ?? null, winRate: v.winRate ?? null, wins: v.wins ?? 0, losses: v.losses ?? 0,
      ...(this.ages.has(x.wallet) ? (() => { const a = this.ages.get(x.wallet)!; return { walletSignatures: a.complete ? a.sigs : `>=${a.sigs}`, walletAgeSec: a.complete && a.firstSec ? Math.round(this.now() / 1000 - a.firstSec) : null }; })() : {}) }; });
    const key: any = { count: (r: any) => r.count, totalSol: (r: any) => r.totalSol, winRate: (r: any) => r.winRate ?? -1, realizedPnl: (r: any) => r.realizedPnlSol, lastAt: (r: any) => r.lastAt };
    rows.sort((a, b) => key[sort](b) - key[sort](a) || b.totalSol - a.totalSol);
    return { minSol, windowSec, venue, sort, wallets: rows.length, buys: list.length, top: rows.slice(0, limit), statsSince: this.statsSince };
  }
  statsSince = Date.now();
  private load() {
    const f = this.o.dataFile; if (!f || !fs.existsSync(f)) { this.statsSince = this.now(); return; }
    try { const j = JSON.parse(fs.readFileSync(f, "utf8")); this.statsSince = j.statsSince ?? this.now(); this.buys = (j.buys ?? []).filter((b: BigBuy) => this.now() - b.at < (this.o.keepMs ?? 86_400_000));
      this.wallets = new Map(Object.entries(j.wallets ?? {})); this.ages = new Map(Object.entries(j.ages ?? {}));
      for (const b of this.buys) if (!b.outcome && b.priceSol) { const l = this.pending.get(b.mint) ?? []; l.push(b); this.pending.set(b.mint, l); } } catch { this.statsSince = this.now(); }
  }
  save() {
    const f = this.o.dataFile; if (!f || !this.dirty) return; this.dirty = false;
    try { // keep the file bounded: drop wallets idle > 7 days with a single big buy
      for (const [k, w] of this.wallets) if (w.bigBuys <= 1 && this.now() - w.lastAt > 7 * 86_400_000) this.wallets.delete(k);
      fs.mkdirSync(path.dirname(f), { recursive: true }); const tmp = f + ".tmp";
      fs.writeFileSync(tmp, JSON.stringify({ v: 1, statsSince: this.statsSince, buys: this.buys.slice(-20_000), wallets: Object.fromEntries(this.wallets), ages: Object.fromEntries([...this.ages].slice(-20_000)) })); fs.renameSync(tmp, f);
    } catch {}
  }
}
export function validateBigBuySub(p: any, recordMin = 0.5): BigBuySubParams {
  if (p == null || typeof p !== "object" || Array.isArray(p)) throw INVALID_PARAMS("pumpBigBuySubscribe: params [{minSol?, venue?, mints?, excludeWallets?}]");
  if (p.minSol != null && !(typeof p.minSol === "number" && p.minSol >= recordMin)) throw INVALID_PARAMS(`pumpBigBuySubscribe: minSol >= ${recordMin}`);
  if (p.venue != null && !["pump", "curve", "amm", "any"].includes(p.venue)) throw INVALID_PARAMS("pumpBigBuySubscribe: venue pump|curve|amm|any");
  for (const k of ["mints", "excludeWallets"]) if (p[k] != null && !(Array.isArray(p[k]) && p[k].length <= 5000 && p[k].every(isPubkey))) throw INVALID_PARAMS(`pumpBigBuySubscribe: ${k} must be pubkeys`);
  return p;
}
