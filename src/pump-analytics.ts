// Pump analytics on top of PumpEngine: per-wallet forward-return ranking (persisted) and launch-risk (creator share / bundles).
import fs from "node:fs";
import path from "node:path";
import { PumpEngine, balanceDiffEvents, type PumpSwapEvent, type LaunchRisk, PUMP_PROGRAM, PUMP_AMM_PROGRAM } from "./pump.js";
import { INVALID_PARAMS } from "./errors.js";

type Rpc = (m: string, p: any[]) => Promise<any>;
const SUPPLY_TOK = 1e9; // pump tokens: 1B supply (ui units)
export const HORIZONS = [60, 300, 900, 3600] as const;
export interface WalletTrade { sig: string; wallet: string; tag: string | null; mint: string; at: number; price: number | null; sol: number; fwd: Record<string, number | null>; src: "live" | "backfill" }
interface Pos { boughtSol: number; boughtTok: number; soldSol: number; soldTok: number }
interface Launch { mint: string; createSlot: number; createdAt: number; creator: string | null; creatorTok: number; early: Map<string, { tok: number; slot: number }>; sameSlot: Set<string>; funders?: Record<string, string | null> }

export function parseWalletsFile(txt: string) {
  const m = new Map<string, string>();
  for (const line of txt.split(/\r?\n/)) { const [w, ...rest] = line.trim().split(/\s+/); if (w && /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(w)) m.set(w, rest.join(" ") || "watched"); }
  return m;
}
const med = (a: number[]) => { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); const n = s.length; return n % 2 ? s[n >> 1] : (s[n / 2 - 1] + s[n / 2]) / 2; };
const r4 = (x: number | null) => x == null ? null : Math.round(x * 10_000) / 10_000;

export interface AnalyticsOptions { dataFile?: string; wallets?: Map<string, string>; earlySlots?: number; saveMs?: number; tickMs?: number; now?: () => number; maxTrades?: number; keepMs?: number }

export class PumpAnalytics {
  trades: WalletTrade[] = []; private sigs = new Set<string>();
  pos = new Map<string, Pos>(); // wallet|mint
  price = new Map<string, { p: number; at: number }>(); // mint -> last valid price (curve or canonical amm)
  launches = new Map<string, Launch>();
  private timers: NodeJS.Timeout[] = []; private stop?: () => void; private dirty = false;
  private now: () => number; private early: number;
  constructor(private eng: PumpEngine, private rpc: Rpc, private o: AnalyticsOptions = {}) {
    this.now = o.now ?? Date.now; this.early = o.earlySlots ?? 2;
    this.load();
  }
  /** keep the pump stream on and see every swap */
  start() {
    if (this.stop) return this;
    this.eng.hooks.push((e) => this.onSwap(e));
    this.stop = this.eng.subscribe({ allSwaps: true, position: false }, () => {});
    const t1 = setInterval(() => this.tick(), this.o.tickMs ?? 10_000); t1.unref?.();
    const t2 = setInterval(() => this.save(), this.o.saveMs ?? 30_000); t2.unref?.();
    this.timers.push(t1, t2); return this;
  }
  close() { for (const t of this.timers) clearInterval(t); this.timers = []; this.stop?.(); this.stop = undefined; this.save(); }
  tagOf(w: string) { return this.o.wallets?.get(w) ?? this.eng.tagOf(w) ?? (this.eng.watched(w) ? "watched" : undefined); }

  onSwap(e: PumpSwapEvent) {
    if (!e.mint || e.update) return;
    const now = this.now();
    if (e.priceValid && e.price != null && !e.dust && (e.venue === "curve" || e.venue === "amm")) this.price.set(e.mint, { p: e.price, at: now });
    this.launch(e);
    const L = this.launches.get(e.mint); if (L) e.launchRisk = this.risk(L, e.slot);
    const tag = this.tagOf(e.wallet); if (!tag) return;
    const k = `${e.wallet}|${e.mint}`; const p = this.pos.get(k) ?? { boughtSol: 0, boughtTok: 0, soldSol: 0, soldTok: 0 };
    if (e.side === "buy") { p.boughtSol += e.solAmount; p.boughtTok += e.tokenAmount; } else { p.soldSol += e.solAmount; p.soldTok += e.tokenAmount; }
    this.pos.set(k, p); this.dirty = true;
    if (e.side === "buy" && !this.sigs.has(e.signature + e.wallet)) {
      this.sigs.add(e.signature + e.wallet);
      const ok = e.priceValid && e.price != null && e.venue !== "amm-other";
      this.trades.push({ sig: e.signature, wallet: e.wallet, tag, mint: e.mint, at: now, price: ok ? e.price : null, sol: e.solAmount, fwd: {}, src: "live" });
      if (this.trades.length > (this.o.maxTrades ?? 50_000)) this.trades.splice(0, 1000);
    }
  }
  /** fill forward returns whose horizon elapsed: current valid price / entry price - 1 */
  tick() {
    const now = this.now();
    for (const t of this.trades) {
      if (t.price == null || t.src !== "live") continue;
      for (const h of HORIZONS) if (!(h in t.fwd) && now - t.at >= h * 1000) {
        const c = this.price.get(t.mint); t.fwd[h] = c ? r4(c.p / t.price - 1) : null; this.dirty = true;
      }
    }
    const keep = now - (this.o.keepMs ?? 7 * 86400_000);
    if (this.trades.length && this.trades[0].at < keep) { this.trades = this.trades.filter((t) => t.at >= keep); this.sigs = new Set(this.trades.map((t) => t.sig + t.wallet)); }
    for (const [m, L] of this.launches) if (now - L.createdAt > 6 * 3600_000) this.launches.delete(m);
  }
  private realized(p: Pos) { const avg = p.boughtTok > 0 ? p.boughtSol / p.boughtTok : null; return avg == null ? p.soldSol : p.soldSol - Math.min(p.soldTok, p.boughtTok) * avg; }

  /** params: {tag?, windowSec? (default 86400), minTrades? (default 3), horizon? (sec, default 300), limit? (default 100)} */
  ranking(q: any = {}) {
    q ??= {};
    if (typeof q !== "object") throw INVALID_PARAMS("pumpWalletRanking: params [{tag?, windowSec?, minTrades?, horizon?, limit?}]");
    const win = q.windowSec ?? q.window ?? 86400, minT = q.minTrades ?? 3, hz = q.horizon ?? 300, lim = q.limit ?? 100;
    if (!(typeof win === "number" && win > 0 && win <= 7 * 86400)) throw INVALID_PARAMS("pumpWalletRanking: windowSec in (0, 604800]");
    if (!(Number.isInteger(minT) && minT >= 0)) throw INVALID_PARAMS("pumpWalletRanking: minTrades integer >= 0 (0 includes unscored, e.g. backfilled)");
    if (!HORIZONS.includes(hz)) throw INVALID_PARAMS(`pumpWalletRanking: horizon one of ${HORIZONS.join(",")}`);
    if (q.tag !== undefined && typeof q.tag !== "string") throw INVALID_PARAMS("pumpWalletRanking: tag must be a string");
    const since = this.now() - win * 1000;
    const by = new Map<string, WalletTrade[]>();
    for (const t of this.trades) if (t.at >= since && (!q.tag || (t.tag ?? "").toLowerCase().includes(q.tag.toLowerCase()))) (by.get(t.wallet) ?? by.set(t.wallet, []).get(t.wallet)!).push(t);
    const rows = [...by.entries()].map(([wallet, ts]) => {
      const hz2: Record<string, any> = {};
      for (const h of HORIZONS) { const r = ts.map((t) => t.fwd[h]).filter((x): x is number => typeof x === "number");
        hz2[`${h}s`] = { n: r.length, winRate: r.length ? r4(r.filter((x) => x > 0).length / r.length) : null, avgReturn: r.length ? r4(r.reduce((a, b) => a + b, 0) / r.length) : null, medianReturn: r4(med(r)) }; }
      let realizedSol = 0, boughtSol = 0, mints = new Set(ts.map((t) => t.mint));
      for (const m of mints) { const p = this.pos.get(`${wallet}|${m}`); if (p) { realizedSol += this.realized(p); boughtSol += p.boughtSol; } }
      return { wallet, tag: ts.at(-1)!.tag, buys: ts.length, mints: mints.size, buySol: r4(ts.reduce((a, t) => a + t.sol, 0)), realizedSol: r4(realizedSol), positionBoughtSol: r4(boughtSol),
        winRate: hz2[`${hz}s`].winRate, avgReturn: hz2[`${hz}s`].avgReturn, medianReturn: hz2[`${hz}s`].medianReturn, scored: hz2[`${hz}s`].n, horizons: hz2, lastBuyAt: ts.at(-1)!.at };
    }).filter((r) => r.scored >= minT)
      .sort((a, b) => (b.winRate ?? -1) - (a.winRate ?? -1) || (b.avgReturn ?? -9) - (a.avgReturn ?? -9));
    return { windowSec: win, horizon: hz, minTrades: minT, tag: q.tag ?? null, wallets: rows.slice(0, lim), trackedTrades: this.trades.length, generatedAt: this.now(),
      note: "forward return = later valid price (curve or canonical AMM) / entry price - 1; realizedSol = sells - avg cost of sold tokens, since tracking began" };
  }

  // ---- launch risk ----
  private launch(e: PumpSwapEvent) {
    if (e.program !== "pump" || e.side !== "buy" || !e.mint) return;
    let L = this.launches.get(e.mint);
    if (!L) { if (!e.create) return; L = { mint: e.mint, createSlot: e.slot, createdAt: this.now(), creator: e.wallet, creatorTok: 0, early: new Map(), sameSlot: new Set() }; this.launches.set(e.mint, L); if (this.launches.size > 20_000) this.launches.delete(this.launches.keys().next().value!); }
    if (e.slot > L.createSlot + this.early) return;
    if (e.wallet === L.creator) { L.creatorTok += e.tokenAmount; return; }
    const x = L.early.get(e.wallet) ?? { tok: 0, slot: e.slot }; x.tok += e.tokenAmount; L.early.set(e.wallet, x);
    if (e.slot === L.createSlot) L.sameSlot.add(e.wallet);
  }
  risk(L: Launch, slot?: number): LaunchRisk {
    const creatorPct = L.creatorTok / SUPPLY_TOK * 100;
    const bundleTok = [...L.early.values()].reduce((a, x) => a + x.tok, 0); const bundlePct = bundleTok / SUPPLY_TOK * 100;
    let shared: LaunchRisk["sharedFunder"] = null;
    if (L.funders) { const c = new Map<string, number>(); for (const f of Object.values(L.funders)) if (f) c.set(f, (c.get(f) ?? 0) + 1);
      const top = [...c.entries()].sort((a, b) => b[1] - a[1])[0]; if (top && top[1] >= 2) shared = { funder: top[0], wallets: top[1] }; }
    const creatorFunded = L.funders && L.creator ? Object.values(L.funders).filter((f) => f === L.creator).length : 0;
    // heuristic 0..100: supply held by creator+early buyers, same-slot snipers, shared funder
    const score = Math.min(100, Math.round(creatorPct * 2 + bundlePct * 1.5 + L.sameSlot.size * 4 + (shared ? 10 + shared.wallets * 5 : 0) + creatorFunded * 10));
    return { creator: L.creator, createSlot: L.createSlot, creatorPct: r4(creatorPct)!, bundleWallets: L.early.size, bundlePct: r4(bundlePct)!, sameSlotBuys: L.sameSlot.size, earlyBuyers: L.early.size,
      sharedFunder: shared, ...(L.funders ? { creatorFundedWallets: creatorFunded } : {}), score, final: slot != null ? slot > L.createSlot + this.early : false };
  }
  /** params: [mint, {funders?: bool}] — funders runs getFundedBy (1 hop) on up to 8 early wallets */
  async launchInfo(mint: any, cfg: any = {}) {
    if (typeof mint !== "string") throw INVALID_PARAMS("pumpLaunchInfo: params [mint, {funders?}]");
    const L = this.launches.get(mint);
    if (!L) return { mint, known: false, note: "launch not observed since this server started (only creates seen live are scored)" };
    if (cfg?.funders && !L.funders) {
      L.funders = {};
      const ws = [...L.early.entries()].sort((a, b) => b[1].tok - a[1].tok).slice(0, 8).map((x) => x[0]);
      await Promise.all(ws.map(async (w) => { try { const r = await this.rpc("getFundedBy", [w, { hops: 1 }]); L.funders![w] = r?.funder ?? null; } catch { L.funders![w] = null; } }));
    }
    return { mint, known: true, ...this.risk(L, Infinity), early: [...L.early.entries()].map(([wallet, x]) => ({ wallet, tokens: r4(x.tok), pct: r4(x.tok / SUPPLY_TOK * 100), slot: x.slot, funder: L.funders?.[wallet] })),
      creatorTokens: r4(L.creatorTok), earlySlots: this.early };
  }

  // ---- backfill (realized PnL / buy counts only; no historical forward returns) ----
  async backfill(wallets: string[], o: { perWallet?: number; delayMs?: number; log?: (...a: any[]) => void } = {}) {
    let txs = 0, evs = 0;
    for (const w of wallets) {
      let sigs: any[] = [];
      try { sigs = await this.rpc("getSignaturesForAddress", [w, { limit: o.perWallet ?? 25 }]); } catch { continue; }
      for (const s of sigs ?? []) {
        if (s.err || this.sigs.has(s.signature + w)) continue;
        await new Promise((r) => setTimeout(r, o.delayMs ?? 150));
        let tx: any; try { tx = await this.rpc("getTransaction", [s.signature, { encoding: "jsonParsed", commitment: "confirmed", maxSupportedTransactionVersion: 0 }]); } catch { continue; }
        txs++;
        const keys: string[] = (tx?.transaction?.message?.accountKeys ?? []).map((k: any) => k.pubkey ?? k);
        if (!keys.includes(PUMP_PROGRAM) && !keys.includes(PUMP_AMM_PROGRAM)) continue;
        for (const e of balanceDiffEvents(tx, s.signature, s.slot)) {
          if (e.wallet !== w || !e.mint) continue; evs++;
          const k = `${w}|${e.mint}`; const p = this.pos.get(k) ?? { boughtSol: 0, boughtTok: 0, soldSol: 0, soldTok: 0 };
          if (e.side === "buy") { p.boughtSol += e.solAmount; p.boughtTok += e.tokenAmount; } else { p.soldSol += e.solAmount; p.soldTok += e.tokenAmount; }
          this.pos.set(k, p); this.sigs.add(s.signature + w);
          if (e.side === "buy") this.trades.push({ sig: s.signature, wallet: w, tag: this.tagOf(w) ?? null, mint: e.mint, at: (s.blockTime ?? 0) * 1000 || this.now(), price: null, sol: e.solAmount, fwd: {}, src: "backfill" });
        }
      }
    }
    this.trades.sort((a, b) => a.at - b.at); this.dirty = true; this.save();
    o.log?.(`[pump-analytics] backfill: ${wallets.length} wallets, ${txs} txs, ${evs} pump events`);
    return { wallets: wallets.length, txs, events: evs };
  }

  // ---- persistence ----
  save() {
    if (!this.o.dataFile || !this.dirty) return; this.dirty = false;
    try {
      fs.mkdirSync(path.dirname(this.o.dataFile), { recursive: true });
      const tmp = this.o.dataFile + ".tmp";
      fs.writeFileSync(tmp, JSON.stringify({ v: 1, savedAt: this.now(), trades: this.trades, pos: [...this.pos.entries()], price: [...this.price.entries()].slice(-20_000) }));
      fs.renameSync(tmp, this.o.dataFile);
    } catch {}
  }
  private load() {
    if (!this.o.dataFile) return;
    try {
      const j = JSON.parse(fs.readFileSync(this.o.dataFile, "utf8"));
      this.trades = j.trades ?? []; this.pos = new Map(j.pos ?? []); this.price = new Map(j.price ?? []);
      this.sigs = new Set(this.trades.map((t) => t.sig + t.wallet));
    } catch {}
  }
}
