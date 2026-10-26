// Helius history APIs on standard RPC: getTransactionsForAddress (JSON-RPC) and Enhanced Transactions REST
// (POST /v0/transactions, GET /v0/addresses/:address/transactions).
import { RpcError, INVALID_PARAMS } from "./errors.js";
import { isPubkey, MAX_TX_VERSION } from "./codec.js";
import { parseEnhanced } from "./enhanced.js";

export type Rpc = (method: string, params: any[]) => Promise<any>;
const TOKEN_PROGRAMS = ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"];

async function pool<T, R>(xs: T[], n: number, fn: (x: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(xs.length); let i = 0;
  await Promise.all(Array.from({ length: Math.min(n, xs.length) }, async () => { while (i < xs.length) { const k = i++; out[k] = await fn(xs[k]); } }));
  return out;
}
type Range = { gte?: number; gt?: number; lte?: number; lt?: number; eq?: number };
const inRange = (v: number | null | undefined, r?: Range) => {
  if (!r) return true; if (v == null) return false;
  return (r.gte == null || v >= r.gte) && (r.gt == null || v > r.gt) && (r.lte == null || v <= r.lte) && (r.lt == null || v < r.lt) && (r.eq == null || v === r.eq);
};

export interface GtfaOptions { scanMax?: number; fullMax?: number; concurrency?: number }

/** Helius getTransactionsForAddress. transactionIndex is the position inside the slot as seen by
 *  getSignaturesForAddress (not the block index, which standard RPC does not expose without getBlock). */
export async function getTransactionsForAddress(rpc: Rpc, address: any, cfg: any = {}, o: GtfaOptions = {}) {
  if (!isPubkey(address)) throw INVALID_PARAMS("getTransactionsForAddress: params[0] must be an address");
  if (cfg === null || typeof cfg !== "object") throw INVALID_PARAMS("getTransactionsForAddress: params[1] must be an object");
  const details = cfg.transactionDetails ?? "signatures";
  if (details !== "signatures" && details !== "full") throw INVALID_PARAMS("getTransactionsForAddress: transactionDetails must be signatures|full");
  const order = cfg.sortOrder ?? "desc";
  if (order !== "asc" && order !== "desc") throw INVALID_PARAMS("getTransactionsForAddress: sortOrder must be asc|desc");
  const fullMax = o.fullMax ?? Number(process.env.GTFA_FULL_MAX ?? 100);
  const limit = cfg.limit ?? (details === "full" ? fullMax : 1000);
  if (!Number.isInteger(limit) || limit < 1 || limit > (details === "full" ? fullMax : 1000))
    throw INVALID_PARAMS(`getTransactionsForAddress: limit must be 1..${details === "full" ? fullMax : 1000}`);
  const f = cfg.filters ?? {};
  const status = f.status ?? "any";
  if (!["any", "succeeded", "failed"].includes(status)) throw INVALID_PARAMS("getTransactionsForAddress: filters.status must be succeeded|failed|any");
  const ta = f.tokenAccounts ?? "none";
  if (!["none", "balanceChanged", "all"].includes(ta)) throw INVALID_PARAMS("getTransactionsForAddress: filters.tokenAccounts must be none|balanceChanged|all");
  let tok: [number, number] | undefined;
  if (cfg.paginationToken != null) {
    const m = /^(\d+):(\d+)$/.exec(String(cfg.paginationToken)); if (!m) throw INVALID_PARAMS("getTransactionsForAddress: invalid paginationToken (expected \"slot:position\")");
    tok = [Number(m[1]), Number(m[2])];
  }
  const commitment = cfg.commitment ?? "finalized";
  const scanMax = o.scanMax ?? Number(process.env.GTFA_SCAN_MAX ?? 10_000);

  // addresses: the wallet + (optionally) every token account it owns
  const addrs = [address];
  let owned = new Set<string>();
  if (ta !== "none") {
    const rs = await Promise.all(TOKEN_PROGRAMS.map((programId) => rpc("getTokenAccountsByOwner", [address, { programId }, { encoding: "base64", commitment }]).catch(() => ({ value: [] }))));
    owned = new Set(rs.flatMap((r) => (r?.value ?? []).map((a: any) => a.pubkey)));
    addrs.push(...owned);
  }
  const okBasic = (s: any) => inRange(s.slot, f.slot) && (f.blockTime ? inRange(s.blockTime, f.blockTime) : true)
    && (status === "any" || (status === "succeeded" ? !s.err : !!s.err));
  const sigs = new Map<string, any>();
  for (const a of addrs) {
    let before: string | undefined; let got = 0; let scanned = 0;
    while (scanned < scanMax) {
      const page: any[] = await rpc("getSignaturesForAddress", [a, { limit: 1000, before, commitment, ...(cfg.minContextSlot != null ? { minContextSlot: cfg.minContextSlot } : {}) }]) ?? [];
      scanned += page.length;
      for (const s of page) if (!sigs.has(s.signature)) { sigs.set(s.signature, s); if (okBasic(s)) got++; }
      if (page.length < 1000) break;
      before = page.at(-1).signature;
      const oldest = page.at(-1).slot;
      if ((f.slot?.gte != null && oldest < f.slot.gte) || (f.slot?.gt != null && oldest <= f.slot.gt)) break;
      if (f.blockTime && page.at(-1).blockTime != null && ((f.blockTime.gte != null && page.at(-1).blockTime < f.blockTime.gte) || (f.blockTime.gt != null && page.at(-1).blockTime <= f.blockTime.gt))) break;
      if (order === "desc" && !tok && !f.signature && got > limit) break;
    }
  }
  // newest first; within a slot keep RPC order (newest first) and number positions in block order
  let all = [...sigs.values()].sort((x, y) => y.slot - x.slot);
  const posIn = new Map<string, number>(); const bySlot = new Map<number, any[]>();
  for (const s of all) { const l = bySlot.get(s.slot) ?? []; l.push(s); bySlot.set(s.slot, l); }
  for (const l of bySlot.values()) l.forEach((s, i) => posIn.set(s.signature, l.length - 1 - i));
  all = all.filter(okBasic);
  if (f.signature) {
    const idx = (sig: string) => { const s = sigs.get(sig); return s ? [s.slot, posIn.get(sig)!] as [number, number] : undefined; };
    const cmp = (s: any, ref: [number, number]) => s.slot !== ref[0] ? s.slot - ref[0] : posIn.get(s.signature)! - ref[1];
    for (const op of ["lt", "lte", "gt", "gte"] as const) {
      if (f.signature[op] == null) continue;
      const ref = idx(f.signature[op]); if (!ref) throw INVALID_PARAMS(`getTransactionsForAddress: filters.signature.${op} not found in scanned history`);
      all = all.filter((s) => { const c = cmp(s, ref); return op === "lt" ? c < 0 : op === "lte" ? c <= 0 : op === "gt" ? c > 0 : c >= 0; });
    }
  }
  if (order === "asc") all.reverse();
  if (tok) {
    const after = (s: any) => { const c = s.slot !== tok![0] ? s.slot - tok![0] : posIn.get(s.signature)! - tok![1]; return order === "asc" ? c > 0 : c < 0; };
    all = all.filter(after);
  }
  const cfgTx = { encoding: cfg.encoding ?? "json", maxSupportedTransactionVersion: Math.max(cfg.maxSupportedTransactionVersion ?? 0, MAX_TX_VERSION), commitment };
  let data: any[] = [];
  if (details === "signatures" && ta !== "balanceChanged") {
    data = all.slice(0, limit).map((s) => ({ signature: s.signature, slot: s.slot, transactionIndex: posIn.get(s.signature)!, err: s.err ?? null, memo: s.memo ?? null, blockTime: s.blockTime ?? null, confirmationStatus: s.confirmationStatus ?? null }));
  } else {
    // full mode (or balanceChanged, which needs token balances): fetch transactions page by page
    let i = 0;
    while (data.length < limit && i < all.length) {
      const chunk = all.slice(i, i + (limit - data.length)); i += chunk.length;
      const txs = await pool(chunk, o.concurrency ?? 8, (s) => rpc("getTransaction", [s.signature, cfgTx]).catch(() => null));
      chunk.forEach((s, k) => {
        const t = txs[k]; if (!t) return;
        if (ta === "balanceChanged" && sigs.get(s.signature) && !directRef(t, address) && !tokenChanged(t, owned)) return;
        if (details === "full") { const { slot, blockTime, ...rest } = t; data.push({ slot: s.slot, transactionIndex: posIn.get(s.signature)!, blockTime: blockTime ?? s.blockTime ?? null, ...rest }); }
        else data.push({ signature: s.signature, slot: s.slot, transactionIndex: posIn.get(s.signature)!, err: s.err ?? null, memo: s.memo ?? null, blockTime: s.blockTime ?? null, confirmationStatus: s.confirmationStatus ?? null });
      });
    }
    void i;
  }
  const last = data.at(-1);
  const more = last && all.findIndex((s) => s.signature === (last.signature ?? last.transaction?.signatures?.[0])) < all.length - 1;
  return { data, paginationToken: more ? `${last.slot}:${last.transactionIndex}` : null };
}
const keysOf = (t: any): string[] => [...(t.transaction?.message?.accountKeys ?? []).map((k: any) => (typeof k === "string" ? k : k.pubkey)), ...(t.meta?.loadedAddresses?.writable ?? []), ...(t.meta?.loadedAddresses?.readonly ?? [])];
const directRef = (t: any, a: string) => keysOf(t).includes(a);
function tokenChanged(t: any, owned: Set<string>) {
  const keys = keysOf(t); const amt = (l: any[], i: number) => l?.find((b: any) => b.accountIndex === i)?.uiTokenAmount?.amount ?? "0";
  return keys.some((k, i) => owned.has(k) && amt(t.meta?.preTokenBalances, i) !== amt(t.meta?.postTokenBalances, i));
}

export class HttpError extends Error { constructor(public status: number, msg: string) { super(msg); } }

/** POST /v0/transactions {transactions: string[] (<=100), commitment?} */
export async function parseTransactions(rpc: Rpc, body: any) {
  const list = body?.transactions;
  if (!Array.isArray(list) || !list.length || list.length > 100 || !list.every((s: any) => typeof s === "string"))
    throw new HttpError(400, "transactions must be an array of 1..100 signatures");
  const commitment = body.commitment ?? "finalized";
  if (commitment !== "finalized" && commitment !== "confirmed") throw new HttpError(400, "commitment must be finalized or confirmed");
  const txs = await pool(list, 8, (s: string) => rpc("getTransaction", [s, { encoding: "json", maxSupportedTransactionVersion: 1, commitment }]).catch((e) => { if (e instanceof RpcError && e.code === -32429) throw e; return null; }));
  return txs.filter(Boolean).map(parseEnhanced);
}

/** GET /v0/addresses/:address/transactions?before-signature&after-signature&type&source&limit&commitment */
export async function addressTransactions(rpc: Rpc, address: string, q: URLSearchParams) {
  if (!isPubkey(address)) throw new HttpError(400, "invalid address");
  const limit = q.has("limit") ? Number(q.get("limit")) : 100;
  if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new HttpError(400, "limit must be 1..100");
  const commitment = q.get("commitment") ?? "finalized";
  if (commitment !== "finalized" && commitment !== "confirmed") throw new HttpError(400, "commitment must be finalized or confirmed");
  const type = q.get("type") ?? undefined, source = q.get("source") ?? undefined;
  let before = q.get("before-signature") ?? q.get("before") ?? undefined;
  const until = q.get("after-signature") ?? q.get("until") ?? undefined;
  const out: any[] = []; let pages = 0;
  const maxPages = type || source ? Number(process.env.ENHANCED_SCAN_PAGES ?? 5) : 1;
  while (out.length < limit && pages++ < maxPages) {
    const sigs: any[] = await rpc("getSignaturesForAddress", [address, { limit: 100, before, until, commitment }]) ?? [];
    if (!sigs.length) break;
    const txs = await pool(sigs, 8, (s: any) => rpc("getTransaction", [s.signature, { encoding: "json", maxSupportedTransactionVersion: 1, commitment }]).catch(() => null));
    for (const t of txs) {
      if (!t) continue; const e = parseEnhanced(t);
      if ((type && e.type !== type) || (source && e.source !== source)) continue;
      out.push(e); if (out.length >= limit) break;
    }
    if (sigs.length < 100) break;
    before = sigs.at(-1).signature;
  }
  return out;
}
