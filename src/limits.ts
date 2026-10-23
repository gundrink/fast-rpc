// Per-API-key token-bucket rate limit + credit accounting.
import { RpcError } from "./errors.js";

export interface KeyLimit { rps?: number; burst?: number; credits?: number }
const COST: Record<string, number> = {
  getProgramAccounts: 10, getBlock: 5, getTransaction: 2, getSignaturesForAddress: 2, sendTransaction: 1, simulateTransaction: 2,
  getAsset: 10, getAssetBatch: 10, getAssetsByOwner: 10, getAssetsByGroup: 10, searchAssets: 10, getAssetProof: 10, getSignaturesForAsset: 10,
  getPriorityFeeEstimate: 1, subscribe: 1, webhook: 1, enhanced: 100, getTransactionsForAddress: 100,
};
export const methodCost = (m: string) => COST[m] ?? 1;

export class Limiter {
  private buckets = new Map<string, { tokens: number; at: number }>();
  usage = new Map<string, { requests: number; creditsUsed: number; rateLimited: number }>();
  constructor(private limits: Record<string, KeyLimit> = {}, private dflt: KeyLimit = {}) {}
  static fromEnv() {
    let l: Record<string, KeyLimit> = {};
    try { if (process.env.KEY_LIMITS) l = JSON.parse(process.env.KEY_LIMITS); } catch { throw new Error("KEY_LIMITS must be JSON"); }
    const d: KeyLimit = {};
    if (process.env.DEFAULT_RPS) d.rps = Number(process.env.DEFAULT_RPS);
    if (process.env.DEFAULT_CREDITS) d.credits = Number(process.env.DEFAULT_CREDITS);
    return new Limiter(l, d);
  }
  private lim(key: string) { return this.limits[key] ?? this.dflt; }
  /** Throws -32429 when rate limited or out of credits; otherwise records usage. */
  take(key: string, method: string) {
    const lim = this.lim(key);
    const u = this.usage.get(key) ?? { requests: 0, creditsUsed: 0, rateLimited: 0 };
    this.usage.set(key, u);
    const cost = methodCost(method);
    if (lim.credits != null && u.creditsUsed + cost > lim.credits) { u.rateLimited++; throw new RpcError(-32429, "Credit limit exceeded for this API key"); }
    if (lim.rps) {
      const burst = lim.burst ?? lim.rps;
      const now = Date.now();
      const b = this.buckets.get(key) ?? { tokens: burst, at: now };
      b.tokens = Math.min(burst, b.tokens + ((now - b.at) / 1000) * lim.rps); b.at = now;
      this.buckets.set(key, b);
      if (b.tokens < 1) { u.rateLimited++; throw new RpcError(-32429, "Too many requests for this API key"); }
      b.tokens -= 1;
    }
    u.requests++; u.creditsUsed += cost;
  }
  report(key: string) {
    const lim = this.lim(key); const u = this.usage.get(key) ?? { requests: 0, creditsUsed: 0, rateLimited: 0 };
    return { apiKey: key, ...u, creditsLimit: lim.credits ?? null, creditsRemaining: lim.credits != null ? Math.max(0, lim.credits - u.creditsUsed) : null, rps: lim.rps ?? null };
  }
}
