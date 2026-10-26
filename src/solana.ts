// Read-only Solana JSON-RPC upstream: timeout, short TTL cache, in-flight dedupe, param limits.
import { RpcError, INVALID_PARAMS } from "./errors.js";
import { decodeTransaction, b58decode, MAX_TX_VERSION } from "./codec.js";
import { createTokenLargest } from "./largest.js";
import { createDas } from "./das.js";
import { getTransactionsForAddress } from "./history.js";
import { ChainState } from "./chain.js";
import { createJito, type JitoOptions } from "./jito.js";
import { createRacedUpstream, type RaceOptions } from "./race.js";
import { createFundedBy, type FundedByOptions } from "./fundedby.js";
import { createSender, smartFees } from "./sender.js";
import { createTpuSender, type TpuTransport } from "./tpu.js";
import { sidecarFromEnv, SidecarTransport } from "./tpu-sidecar.js";

export const upstreamUrl = process.env.SOLANA_RPC_URL ?? "https://api.mainnet-beta.solana.com";
export interface SolanaApiOptions {
  url?: string; timeoutMs?: number; gpaMaxResults?: number;
  /** extra upstreams for sendTransaction fan-out (Sender-like). Default: SEND_RPC_URLS env (comma list) */
  sendUrls?: string[];
  /** DAS-capable RPC (e.g. a Helius URL). Default: DAS_RPC_URL env. Without it, on-chain fallback for getAsset/getAssetsByOwner */
  dasUrl?: string;
  /** slot-rate / consensus state (Alpenglow-aware TTLs and commitment). */
  chain?: ChainState;
  /** Jito block engine (JITO_URL). Dual send: sendTransaction cfg {jito:true} or SEND_JITO=1 */
  jito?: JitoOptions; jitoDualSend?: boolean;
  /** local compressed-NFT index (used when DAS_RPC_URL is unset) */
  cnft?: import("./bubblegum.js").CnftIndexer;
  /** race reads across several (free/public) RPCs: SOLANA_RPC_URLS (comma list) */
  urls?: string[]; race?: RaceOptions;
  fundedBy?: FundedByOptions;
  /** direct TPU-QUIC transport (Node has no built-in QUIC; plug one in). Mocked in tests. */
  tpuTransport?: TpuTransport;
  /** fetch used by getSmartFees for the Jito tip floor */
  fetcher?: typeof fetch;
}

export const upstreamStats = { retries429: 0 };
const isRateLimit = (e: any) => e instanceof RpcError && (e.code === 429 || e.code === -32429 || /too many requests|rate limit/i.test(e.message));
export { isRateLimit };
/** backoff for attempt n (0-based): Retry-After (s or HTTP date) if given, else 250ms*2^n with jitter, capped 8s */
export function backoffMs(n: number, retryAfter?: string | null) {
  if (retryAfter) { const s = Number(retryAfter); const ms = Number.isFinite(s) ? s * 1000 : Date.parse(retryAfter) - Date.now(); if (ms > 0) return Math.min(ms, 30_000); }
  return Math.min(8000, 250 * 2 ** n) * (0.75 + Math.random() * 0.5);
}
export function createUpstream(url = upstreamUrl, timeoutMs = Number(process.env.SOLANA_TIMEOUT_MS ?? 10_000), retries = Number(process.env.UPSTREAM_429_RETRIES ?? 3)) {
  const cache = new Map<string, { exp: number; p: Promise<any> }>();
  let seq = 0;
  const stats = upstreamStats;
  return function upstream(method: string, params: any[] = [], ttlMs = 0): Promise<any> {
    const key = method + JSON.stringify(params);
    const now = Date.now();
    const hit = cache.get(key);
    if (hit && hit.exp > now) return hit.p;
    if (hit) cache.delete(key);
    const once = async () => {
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
      const ra = res.headers.get("retry-after");
      const tag = (e: RpcError) => Object.assign(e, { retryAfter: ra });
      try { j = JSON.parse(text); } catch { throw tag(new RpcError(res.status === 429 ? 429 : -32603, `upstream HTTP ${res.status}`)); }
      if (j?.error) throw tag(new RpcError(res.status === 429 ? 429 : j.error.code ?? -32603, j.error.message ?? "upstream error", j.error.data));
      if (!res.ok) throw tag(new RpcError(res.status === 429 ? 429 : -32603, `upstream HTTP ${res.status}`));
      return j.result;
    };
    const p = (async () => {
      for (let n = 0; ; n++) {
        try { return await once(); }
        catch (e: any) { if (n >= retries || !isRateLimit(e)) throw e; stats.retries429++; await new Promise((r) => setTimeout(r, backoffMs(n, e.retryAfter))); }
      }
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
  const urls = opts.urls ?? (process.env.SOLANA_RPC_URLS ? process.env.SOLANA_RPC_URLS.split(",").map((x) => x.trim()).filter(Boolean) : []);
  const raced = urls.length > 1 ? createRacedUpstream(urls, { timeoutMs: opts.timeoutMs, ...opts.race }) : undefined;
  const up = raced ?? createUpstream(opts.url ?? urls[0], opts.timeoutMs);
  const sendUrls = opts.sendUrls ?? (process.env.SEND_RPC_URLS ? process.env.SEND_RPC_URLS.split(",").map((x) => x.trim()).filter(Boolean) : []);
  const senders = [up, ...sendUrls.map((u) => createUpstream(u, opts.timeoutMs))];
  const dasUrl = opts.dasUrl ?? process.env.DAS_RPC_URL;
  const das = createDas(up, dasUrl ? createUpstream(dasUrl, opts.timeoutMs) : undefined, opts.cnft);
  const checkTx = (m: string, a: any[]) => {
    if (typeof a[0] !== "string") throw INVALID_PARAMS(`${m}: params[0] must be a serialized transaction string`);
    const enc = a[1]?.encoding ?? "base58";
    if (enc !== "base58" && enc !== "base64") throw INVALID_PARAMS(`${m}: encoding must be base58 or base64`);
    const raw = enc === "base64" ? Buffer.from(a[0], "base64").length : Math.floor(a[0].length * 0.733);
    if (raw > 1232) throw INVALID_PARAMS(`${m}: transaction too large (${raw} > 1232 bytes)`);
    try { return decodeTransaction(a[0], enc); } catch (e: any) { throw INVALID_PARAMS(`${m}: failed to deserialize transaction: ${e.message}`); }
  };
  const gpaMax = opts.gpaMaxResults ?? Number(process.env.GPA_MAX_RESULTS ?? 10_000);
  const chain = opts.chain ?? new ChainState();
  // trailing-undefined trim + optional confirmed->finalized rewrite (ALPENGLOW_CONFIRMED_AS_FINALIZED, only once Alpenglow is detected)
  const clean = (a: any[]) => {
    const b = [...a]; while (b.length && b[b.length - 1] === undefined) b.pop();
    return b.map((x) => x && typeof x === "object" && !Array.isArray(x) && typeof x.commitment === "string" ? { ...x, commitment: chain.mapCommitment(x.commitment) } : x);
  };
  /** TTL of n slots at the current (observed) slot rate: no hardcoded 400ms. */
  const slotTtl = (n: number) => () => chain.slots(n);
  // Generic read-only passthrough with positional params (Solana style).
  const pass = (method: string, ttl: number | (() => number) = 0, check?: (a: any[]) => void) => (...a: any[]) => { const p = clean(a); check?.(p); return up(method, p, typeof ttl === "function" ? ttl() : ttl); };
  /** mainnet carries v0 + v1 txs: always ask upstream for v1 so clients sending 0 / omitting it never get -32015 */
  const txv = (a: any[]) => {
    if (typeof a[1] === "string") a[1] = { encoding: a[1] };
    const c = a[1] && typeof a[1] === "object" ? a[1] : {};
    a[1] = { ...c, maxSupportedTransactionVersion: Math.max(Number(c.maxSupportedTransactionVersion ?? 0) || 0, MAX_TX_VERSION) };
  };
  const needKey = (m: string) => (a: any[]) => { if (typeof a[0] !== "string" || !a[0]) throw INVALID_PARAMS(`${m}: params[0] must be a base58 string`); };

  /** compute-unit prices (microlamports) of non-vote txs in the latest confirmed block(s), optionally only txs touching `keys` */
  const CB = "ComputeBudget111111111111111111111111111111";
  const blockCuPrices = async (keys: string[]) => {
    const slot: number = await up("getSlot", [{ commitment: "confirmed" }], chain.slots(1));
    const want = new Set(keys); const out: number[] = [];
    for (let s = slot, tries = 0; tries < 4 && out.length < 50; s--, tries++) {
      let b: any; try { b = await up("getBlock", [s, { encoding: "json", maxSupportedTransactionVersion: MAX_TX_VERSION, transactionDetails: "full", rewards: false, commitment: "confirmed" }], 10_000); } catch { continue; }
      for (const t of b?.transactions ?? []) {
        const m = t.transaction?.message; if (!m) continue;
        const ks: string[] = [...m.accountKeys, ...(t.meta?.loadedAddresses?.writable ?? []), ...(t.meta?.loadedAddresses?.readonly ?? [])];
        if (want.size && !ks.some((k) => want.has(k))) continue;
        let price = -1;
        for (const ix of m.instructions) { if (ks[ix.programIdIndex] !== CB) continue; try { const d = b58decode(ix.data); if (d[0] === 3 && d.length >= 9) price = Number(d.readBigUInt64LE(1)); } catch {} }
        const tc = m.transactionConfig; // v1: fee lives in the message (total lamports), not ComputeBudget ixs
        if (price < 0 && tc && tc.priorityFee != null && tc.computeUnitLimit) price = Math.floor(Number(tc.priorityFee) * 1e6 / Number(tc.computeUnitLimit));
        if (price >= 0) out.push(price); // only txs that explicitly set a CU price
      }
    }
    return out.sort((x, y) => x - y);
  };
  const jito = createJito(opts.jito);
  const dual = opts.jitoDualSend ?? process.env.SEND_JITO === "1";
  const tpuTransport = opts.tpuTransport ?? sidecarFromEnv();
  const tpu = createTpuSender(up, { transport: tpuTransport });
  const sender = createSender({ read: up, senders, chain, jitoSend: (t, c) => jito.sendTransaction(t, c), jitoCheck: (t, c) => jito.checkTip(t, c), tpu });
  let fb: ReturnType<typeof createFundedBy> | undefined;
  let tokenLargest: ReturnType<typeof createTokenLargest> | undefined;
  const api = {
    ...das,
    /** Helius funded-by equivalent: params [address, {hops: 1..3 (default 2)}] */
    getFundedBy: (address: any, cfg: any = {}) => (fb ??= createFundedBy((m, p) => (api as any)[m](...p), opts.fundedBy)).fundedBy(address, cfg ?? {}),
    /** multi-path rebroadcast until target commitment / blockhash expiry (user-signed tx only) */
    sendTransactionWithRetry: (tx: any, cfg?: any) => { if (typeof tx === "string") checkTx("sendTransactionWithRetry", [tx, { encoding: cfg?.encoding ?? "base64" }]); return sender.sendWithRetry(tx, cfg ?? {}); },
    /** priority fee (microlamports/CU) + Jito tip suggestion */
    getSmartFees: (req: any = {}) => smartFees((r) => (api as any).getPriorityFeeEstimate(r), req ?? {}, undefined, opts.fetcher),
    /** next leaders' TPU-QUIC addresses (read-only) */
    getTpuLeaders: () => tpu.leaderAddrs(),
    /** read-only connectivity check: QUIC handshake to the next leaders via tpu-sidecar (no stream, nothing sent) */
    getTpuHandshake: async () => {
      if (!(tpuTransport instanceof SidecarTransport)) throw new RpcError(-32601, "getTpuHandshake needs TPU_SIDECAR_URL");
      const l = await tpu.leaderAddrs(); const r = await tpuTransport.handshake(l.map((x) => x.addr));
      return l.map((x, i) => ({ ...x, ...r[i] }));
    },
    /** read-only: is direct-TPU configured, and is the sidecar allowed to forward (TPU_SIDECAR_ALLOW_SEND)? */
    getTpuStatus: async () => {
      if (!(tpuTransport instanceof SidecarTransport)) return { enabled: false, reason: "TPU_SIDECAR_URL not set" };
      const h: any = await tpuTransport.health().catch((e: any) => ({ ok: false, error: String(e?.message ?? e) }));
      return { enabled: !!h.ok, allowSend: !!h.allowSend, identity: h.identity ?? null, fanout: Number(process.env.TPU_FANOUT ?? 4) };
    },
    getUpstreamStats: () => raced?.stats() ?? [{ url: (opts.url ?? upstreamUrl).replace(/([?&]api-key=)[^&]+/, "$1***"), single: true }],
    getClusterNodes: pass("getClusterNodes", 30_000),
    getSlotLeaders: pass("getSlotLeaders", 0),
    getLeaderSchedule: pass("getLeaderSchedule", 60_000),
    isBlockhashValid: pass("isBlockhashValid", 0),
    sendBundle: (...a: any[]) => jito.sendBundle(a[0], a[1]),
    getBundleStatuses: (...a: any[]) => jito.getBundleStatuses(a[0]),
    getInflightBundleStatuses: (...a: any[]) => jito.getInflightBundleStatuses(a[0]),
    getTipAccounts: () => jito.getTipAccounts(),
    /** Relay a user-signed tx (no key custody). Fans out to all send upstreams, first success wins. */
    sendTransaction: async (...a: any[]) => {
      const p = clean(a);
      const useJito = p[1]?.jito ?? dual;
      if (p[1] && "jito" in p[1]) { const { jito: _j, ...rest } = p[1]; p[1] = rest; }
      const tx = checkTx("sendTransaction", p);
      if (tx.numRequiredSignatures < 1) throw INVALID_PARAMS("sendTransaction: transaction has no signers");
      // Sender-style dual path: Jito (tip required, validated before anything leaves) + RPC with skipPreflight
      if (useJito && p[1]?.skipPreflight !== true) throw INVALID_PARAMS("sendTransaction: jito dual send requires skipPreflight: true");
      if (useJito) await jito.checkTip(p[0], p[1]); // invalid/missing tip -> reject before anything is sent
      const viaJito = useJito ? [jito.sendTransaction(p[0], p[1])] : [];
      const results = [...viaJito, ...senders.map((s) => s("sendTransaction", p, 0))];
      try { return await Promise.any(results); }
      catch (e: any) { throw e.errors?.[0] ?? e; }
    },
    simulateTransaction: async (...a: any[]) => { const p = clean(a); checkTx("simulateTransaction", p); return up("simulateTransaction", p, 0); },
    getHealth: pass("getHealth", 1000),
    getVersion: pass("getVersion", 60_000),
    // remaining standard read-only methods (plain passthrough)
    getBlockCommitment: pass("getBlockCommitment", 0),
    getBlockProduction: pass("getBlockProduction", 10_000),
    getBlocks: pass("getBlocks", 0),
    getBlocksWithLimit: pass("getBlocksWithLimit", 0),
    getEpochSchedule: pass("getEpochSchedule", 3_600_000),
    getFeeForMessage: pass("getFeeForMessage", 0),
    getFirstAvailableBlock: pass("getFirstAvailableBlock", 60_000),
    getGenesisHash: pass("getGenesisHash", 3_600_000),
    getHighestSnapshotSlot: pass("getHighestSnapshotSlot", 10_000),
    getIdentity: pass("getIdentity", 60_000),
    getInflationGovernor: pass("getInflationGovernor", 60_000),
    getInflationRate: pass("getInflationRate", 60_000),
    getInflationReward: pass("getInflationReward", 60_000),
    getLargestAccounts: pass("getLargestAccounts", 60_000),
    getMaxRetransmitSlot: pass("getMaxRetransmitSlot", 0),
    getMaxShredInsertSlot: pass("getMaxShredInsertSlot", 0),
    getRecentPerformanceSamples: pass("getRecentPerformanceSamples", 10_000),
    getSlotLeader: pass("getSlotLeader", 0),
    getStakeMinimumDelegation: pass("getStakeMinimumDelegation", 60_000),
    getSupply: pass("getSupply", 30_000),
    /** native first (short budget); public RPCs block it, so fall back to a streamed getProgramAccounts top-20 */
    getTokenLargestAccounts: async (...a: any[]) => {
      const p = clean(a); needKey("getTokenLargestAccounts")(p);
      if (process.env.TOKEN_LARGEST_NATIVE !== "0") {
        try {
          const v = await Promise.race([up("getTokenLargestAccounts", p, 10_000), new Promise((_, rej) => setTimeout(() => rej(new Error("native timeout")), Number(process.env.TOKEN_LARGEST_NATIVE_MS ?? 1500)))]);
          return v;
        } catch (e: any) { if (e instanceof RpcError && e.code === -32602 && !/blocked|token|disabled/i.test(e.message)) throw e; }
      }
      return (tokenLargest ??= createTokenLargest((m, q) => up(m, q, 0), { urls: urls.length ? urls : [opts.url ?? upstreamUrl], fetcher: opts.fetcher }))(p[0], p[1] ?? {});
    },
    getTokenAccountsByDelegate: pass("getTokenAccountsByDelegate", slotTtl(2), needKey("getTokenAccountsByDelegate")),
    getTransactionCount: pass("getTransactionCount", 0),
    getVoteAccounts: pass("getVoteAccounts", 10_000),
    minimumLedgerSlot: pass("minimumLedgerSlot", 60_000),
    getSlot: pass("getSlot", slotTtl(1)),
    getBlockHeight: pass("getBlockHeight", slotTtl(1)),
    getEpochInfo: pass("getEpochInfo", slotTtl(2)),
    getLatestBlockhash: pass("getLatestBlockhash", slotTtl(1)),
    getBalance: pass("getBalance", slotTtl(2), needKey("getBalance")),
    getAccountInfo: pass("getAccountInfo", slotTtl(1), needKey("getAccountInfo")),
    getMultipleAccounts: pass("getMultipleAccounts", slotTtl(1), (a) => {
      if (!Array.isArray(a[0]) || a[0].length === 0) throw INVALID_PARAMS("getMultipleAccounts: params[0] must be a non-empty array");
      if (a[0].length > 100) throw INVALID_PARAMS("getMultipleAccounts: max 100 pubkeys");
    }),
    getTransaction: pass("getTransaction", 60_000, (a) => { needKey("getTransaction")(a); txv(a); }),
    getSignaturesForAddress: pass("getSignaturesForAddress", slotTtl(2), (a) => {
      needKey("getSignaturesForAddress")(a);
      if (a[1]?.limit != null && (a[1].limit < 1 || a[1].limit > 1000)) throw INVALID_PARAMS("getSignaturesForAddress: limit must be 1..1000");
    }),
    getSignatureStatuses: pass("getSignatureStatuses", 0, (a) => {
      if (!Array.isArray(a[0]) || a[0].length > 256) throw INVALID_PARAMS("getSignatureStatuses: params[0] must be an array of <=256 signatures");
    }),
    getBlock: pass("getBlock", 60_000, (a) => { if (!Number.isInteger(a[0])) throw INVALID_PARAMS("getBlock: params[0] must be a slot number"); txv(a); }),
    getBlockTime: pass("getBlockTime", 60_000),
    getTokenAccountsByOwner: pass("getTokenAccountsByOwner", slotTtl(2), (a) => {
      needKey("getTokenAccountsByOwner")(a);
      if (!a[1] || (typeof a[1].mint !== "string" && typeof a[1].programId !== "string")) throw INVALID_PARAMS("getTokenAccountsByOwner: params[1] must be {mint} or {programId}");
    }),
    getTokenAccountBalance: pass("getTokenAccountBalance", slotTtl(2), needKey("getTokenAccountBalance")),
    getTokenSupply: pass("getTokenSupply", 5000, needKey("getTokenSupply")),
    getMinimumBalanceForRentExemption: pass("getMinimumBalanceForRentExemption", 60_000),
    getRecentPrioritizationFees: pass("getRecentPrioritizationFees", slotTtl(2)),
    /** Alpenglow detection (Agave 4.3+): null = TowerBFT, object = genesis certificate. */
    getAgGenesisCert: pass("getAgGenesisCert", 10_000),
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
    /** Helius getTransactionsForAddress (built on getSignaturesForAddress + getTransaction) */
    getTransactionsForAddress: (address: any, cfg?: any) => getTransactionsForAddress((m, p) => (api as any)[m](...p), address, cfg ?? {}),
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
      const o0 = req.options ?? {};
      if (o0.lookbackSlots !== undefined && !(Number.isInteger(o0.lookbackSlots) && o0.lookbackSlots >= 1 && o0.lookbackSlots <= 150))
        throw INVALID_PARAMS("getPriorityFeeEstimate: options.lookbackSlots must be an integer 1..150");
      let fees: { slot: number; prioritizationFee: number }[] = await up("getRecentPrioritizationFees", keys.length ? [keys.slice(0, 128)] : [], chain.slots(2));
      // window is expressed in SLOTS (node returns up to 150), not seconds: 150 slots = 60s at 400ms, 30s at 200ms
      if (o0.lookbackSlots) { const top = fees.reduce((m, f) => Math.max(m, f.slot), 0); fees = fees.filter((f) => f.slot > top - o0.lookbackSlots); }
      // Helius evaluateEmptySlotAsZero (default true): slots without fees count as 0; false => ignore them
      if (o0.evaluateEmptySlotAsZero === false) fees = fees.filter((f) => f.prioritizationFee > 0);
      let v = fees.map((f) => f.prioritizationFee).sort((x, y) => x - y);
      let source = "getRecentPrioritizationFees";
      // getRecentPrioritizationFees reports the per-slot MINIMUM fee (often all 0 on mainnet). Helius ranks actual
      // tx-level prices, so when every sample is 0 (or PRIORITY_FEE_SOURCE=blocks) sample recent block(s) instead.
      if ((process.env.PRIORITY_FEE_SOURCE === "blocks" || (v.length && v[v.length - 1] === 0)) && process.env.PRIORITY_FEE_SOURCE !== "recent") {
        try { const bv = await blockCuPrices(keys); if (bv.length) { v = bv; source = "recentBlocks"; } } catch { /* keep min-fee samples */ }
      }
      const pct = (q: number) => (v.length ? v[Math.min(v.length - 1, Math.max(0, Math.ceil((q / 100) * v.length) - 1))] : 0);
      const levels = { min: v[0] ?? 0, low: pct(25), medium: pct(50), high: pct(75), veryHigh: pct(95), unsafeMax: v.at(-1) ?? 0 };
      const o = req.options ?? {};
      const k = PRIORITY_LEVELS[String(o.priorityLevel ?? "Medium").toLowerCase()];
      if (!k) throw INVALID_PARAMS(`getPriorityFeeEstimate: unknown priorityLevel ${o.priorityLevel}`);
      // Helius `recommended`: medium with a 10,000 microlamport floor
      const est = o.recommended ? Math.max(levels.medium, Number(process.env.PRIORITY_FEE_FLOOR ?? 10_000)) : (levels as any)[k];
      if (o.includeAllPriorityFeeLevels) return { priorityFeeEstimate: est, priorityFeeLevels: levels, ...(o.includeDetails ? { source, samples: v.length } : {}) };
      return { priorityFeeEstimate: est };
    },
  };
  return api;
}

export const solanaApi = createSolanaApi();
export type SolanaApi = ReturnType<typeof createSolanaApi>;
