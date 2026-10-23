// Read-only Solana JSON-RPC upstream: timeout, short TTL cache, in-flight dedupe, param limits.
import { RpcError, INVALID_PARAMS } from "./errors.js";
import { decodeTransaction } from "./codec.js";
import { createDas } from "./das.js";

export const upstreamUrl = process.env.SOLANA_RPC_URL ?? "https://api.mainnet-beta.solana.com";
export interface SolanaApiOptions {
  url?: string; timeoutMs?: number; gpaMaxResults?: number;
  /** extra upstreams for sendTransaction fan-out (Sender-like). Default: SEND_RPC_URLS env (comma list) */
  sendUrls?: string[];
  /** DAS-capable RPC (e.g. a Helius URL). Default: DAS_RPC_URL env. Without it, on-chain fallback for getAsset/getAssetsByOwner */
  dasUrl?: string;
}

export function createUpstream(url = upstreamUrl, timeoutMs = Number(process.env.SOLANA_TIMEOUT_MS ?? 10_000)) {
  const cache = new Map<string, { exp: number; p: Promise<any> }>();
  let seq = 0;
  return function upstream(method: string, params: any[] = [], ttlMs = 0): Promise<any> {
    const key = method + JSON.stringify(params);
    const now = Date.now();
    const hit = cache.get(key);
    if (hit && hit.exp > now) return hit.p;
    if (hit) cache.delete(key);
    const p = (async () => {
      let res: Response;
      try {
        res = await fetch(url, {
          method: "POST", headers: { "content-type": "application/json" },
          body: JSON.stringify({ jsonrpc: "2.0", id: ++seq, method, params }),
          signal: AbortSignal.timeout(timeoutMs),
        });
      } catch (e: any) {
        throw new RpcError(-32603, e?.name === "TimeoutError" ? `upstream timeout after ${timeoutMs}ms` : `upstream unreachable: ${e?.message ?? e}`);
      }
      const text = await res.text();
      let j: any;
      try { j = JSON.parse(text); } catch { throw new RpcError(res.status === 429 ? 429 : -32603, `upstream HTTP ${res.status}`); }
      if (j?.error) throw new RpcError(j.error.code ?? -32603, j.error.message ?? "upstream error", j.error.data);
      if (!res.ok) throw new RpcError(-32603, `upstream HTTP ${res.status}`);
      return j.result;
    })();
    if (ttlMs > 0) {
      cache.set(key, { exp: now + ttlMs, p });
      p.then((v) => { if (v == null) cache.delete(key); }, () => cache.delete(key)); // never cache errors / nulls
      if (cache.size > 10_000) for (const [k, v] of cache) if (v.exp <= now) cache.delete(k);
    }
    return p;
  };
}

const PRIORITY_LEVELS: Record<string, string> = {
  min: "min", low: "low", medium: "medium", high: "high", veryhigh: "veryHigh", unsafemax: "unsafeMax", default: "medium",
};

export function createSolanaApi(opts: SolanaApiOptions = {}) {
  const up = createUpstream(opts.url, opts.timeoutMs);
  const sendUrls = opts.sendUrls ?? (process.env.SEND_RPC_URLS ? process.env.SEND_RPC_URLS.split(",").map((x) => x.trim()).filter(Boolean) : []);
  const senders = [up, ...sendUrls.map((u) => createUpstream(u, opts.timeoutMs))];
  const dasUrl = opts.dasUrl ?? process.env.DAS_RPC_URL;
  const das = createDas(up, dasUrl ? createUpstream(dasUrl, opts.timeoutMs) : undefined);
  const checkTx = (m: string, a: any[]) => {
    if (typeof a[0] !== "string") throw INVALID_PARAMS(`${m}: params[0] must be a serialized transaction string`);
    const enc = a[1]?.encoding ?? "base58";
    if (enc !== "base58" && enc !== "base64") throw INVALID_PARAMS(`${m}: encoding must be base58 or base64`);
    const raw = enc === "base64" ? Buffer.from(a[0], "base64").length : Math.floor(a[0].length * 0.733);
    if (raw > 1232) throw INVALID_PARAMS(`${m}: transaction too large (${raw} > 1232 bytes)`);
    try { return decodeTransaction(a[0], enc); } catch (e: any) { throw INVALID_PARAMS(`${m}: failed to deserialize transaction: ${e.message}`); }
  };
  const gpaMax = opts.gpaMaxResults ?? Number(process.env.GPA_MAX_RESULTS ?? 10_000);
  const clean = (a: any[]) => { const b = [...a]; while (b.length && b[b.length - 1] === undefined) b.pop(); return b; };
  // Generic read-only passthrough with positional params (Solana style).
  const pass = (method: string, ttl = 0, check?: (a: any[]) => void) => (...a: any[]) => { const p = clean(a); check?.(p); return up(method, p, ttl); };
  const needKey = (m: string) => (a: any[]) => { if (typeof a[0] !== "string" || !a[0]) throw INVALID_PARAMS(`${m}: params[0] must be a base58 string`); };

  const api = {
    ...das,
    /** Relay a user-signed tx (no key custody). Fans out to all send upstreams, first success wins. */
    sendTransaction: async (...a: any[]) => {
      const p = clean(a); const tx = checkTx("sendTransaction", p);
      if (tx.numRequiredSignatures < 1) throw INVALID_PARAMS("sendTransaction: transaction has no signers");
      const results = senders.map((s) => s("sendTransaction", p, 0));
      try { return await Promise.any(results); }
      catch (e: any) { throw e.errors?.[0] ?? e; }
    },
    simulateTransaction: async (...a: any[]) => { const p = clean(a); checkTx("simulateTransaction", p); return up("simulateTransaction", p, 0); },
    getHealth: pass("getHealth", 1000),
    getVersion: pass("getVersion", 60_000),
    getSlot: pass("getSlot", 400),
    getBlockHeight: pass("getBlockHeight", 400),
    getEpochInfo: pass("getEpochInfo", 1000),
    getLatestBlockhash: pass("getLatestBlockhash", 400),
    getBalance: pass("getBalance", 1000, needKey("getBalance")),
    getAccountInfo: pass("getAccountInfo", 400, needKey("getAccountInfo")),
    getMultipleAccounts: pass("getMultipleAccounts", 400, (a) => {
      if (!Array.isArray(a[0]) || a[0].length === 0) throw INVALID_PARAMS("getMultipleAccounts: params[0] must be a non-empty array");
      if (a[0].length > 100) throw INVALID_PARAMS("getMultipleAccounts: max 100 pubkeys");
    }),
    getTransaction: pass("getTransaction", 60_000, needKey("getTransaction")),
    getSignaturesForAddress: pass("getSignaturesForAddress", 1000, (a) => {
      needKey("getSignaturesForAddress")(a);
      if (a[1]?.limit != null && (a[1].limit < 1 || a[1].limit > 1000)) throw INVALID_PARAMS("getSignaturesForAddress: limit must be 1..1000");
    }),
    getSignatureStatuses: pass("getSignatureStatuses", 0, (a) => {
      if (!Array.isArray(a[0]) || a[0].length > 256) throw INVALID_PARAMS("getSignatureStatuses: params[0] must be an array of <=256 signatures");
    }),
    getBlock: pass("getBlock", 60_000, (a) => { if (!Number.isInteger(a[0])) throw INVALID_PARAMS("getBlock: params[0] must be a slot number"); }),
    getBlockTime: pass("getBlockTime", 60_000),
    getTokenAccountsByOwner: pass("getTokenAccountsByOwner", 1000, (a) => {
      needKey("getTokenAccountsByOwner")(a);
      if (!a[1] || (typeof a[1].mint !== "string" && typeof a[1].programId !== "string")) throw INVALID_PARAMS("getTokenAccountsByOwner: params[1] must be {mint} or {programId}");
    }),
    getTokenAccountBalance: pass("getTokenAccountBalance", 1000, needKey("getTokenAccountBalance")),
    getTokenSupply: pass("getTokenSupply", 5000, needKey("getTokenSupply")),
    getMinimumBalanceForRentExemption: pass("getMinimumBalanceForRentExemption", 60_000),
    getRecentPrioritizationFees: pass("getRecentPrioritizationFees", 1000),
    getProgramAccounts: async (...a: any[]) => {
      const p = clean(a); needKey("getProgramAccounts")(p);
      const cfg = p[1] ?? {};
      if (cfg.dataSlice && (cfg.dataSlice.length > 10_000)) throw INVALID_PARAMS("getProgramAccounts: dataSlice.length max 10000");
      const r = await up("getProgramAccounts", p, 1000);
      const list = Array.isArray(r) ? r : r?.value;
      if (Array.isArray(list) && list.length > gpaMax)
        throw new RpcError(-32005, `getProgramAccounts returned ${list.length} accounts (> ${gpaMax}); add filters/dataSlice`);
      return r;
    },
    /** Helius-style getPriorityFeeEstimate, approximated from getRecentPrioritizationFees percentiles. */
    getPriorityFeeEstimate: async (req: any = {}) => {
      if (req === null || typeof req !== "object") throw INVALID_PARAMS("getPriorityFeeEstimate: params[0] must be an object");
      let keys: string[] = Array.isArray(req.accountKeys) ? req.accountKeys : [];
      if (typeof req.transaction === "string") {
        try {
          const enc = req.options?.transactionEncoding === "Base64" ? "base64" : req.options?.transactionEncoding === "Base58" ? "base58" : undefined;
          keys = [...new Set([...keys, ...decodeTransaction(req.transaction, enc).accountKeys])];
        } catch (e: any) { throw INVALID_PARAMS(`getPriorityFeeEstimate: cannot decode transaction: ${e.message}`); }
      }
      if (keys.length > 128) throw INVALID_PARAMS("getPriorityFeeEstimate: max 128 accountKeys");
      const fees: { slot: number; prioritizationFee: number }[] = await up("getRecentPrioritizationFees", keys.length ? [keys.slice(0, 128)] : [], 1000);
      const v = fees.map((f) => f.prioritizationFee).sort((x, y) => x - y);
      const pct = (q: number) => (v.length ? v[Math.min(v.length - 1, Math.max(0, Math.ceil((q / 100) * v.length) - 1))] : 0);
      const levels = { min: v[0] ?? 0, low: pct(25), medium: pct(50), high: pct(75), veryHigh: pct(95), unsafeMax: v.at(-1) ?? 0 };
      const o = req.options ?? {};
      if (o.includeAllPriorityFeeLevels) return { priorityFeeLevels: levels };
      const k = PRIORITY_LEVELS[String(o.priorityLevel ?? "Medium").toLowerCase()];
      if (!k) throw INVALID_PARAMS(`getPriorityFeeEstimate: unknown priorityLevel ${o.priorityLevel}`);
      return { priorityFeeEstimate: (levels as any)[k] };
    },
  };
  return api;
}

export const solanaApi = createSolanaApi();
export type SolanaApi = ReturnType<typeof createSolanaApi>;
