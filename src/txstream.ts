// Transaction stream built on shared logsSubscribe topics + getTransaction fetch (Helius transactionSubscribe-like).
import type { StreamHub, Entry } from "./stream.js";
import { INVALID_PARAMS } from "./errors.js";
import { decodeTransaction, isPubkey } from "./codec.js";

export const VOTE_PROGRAM = "Vote111111111111111111111111111111111111111";
export interface TxFilter { accountInclude?: string[]; accountExclude?: string[]; accountRequired?: string[]; vote?: boolean; failed?: boolean; signature?: string }
export interface TxOptions { commitment?: "processed" | "confirmed" | "finalized"; encoding?: "json" | "jsonParsed" | "base64" | "base58"; transactionDetails?: "full" | "signatures" | "accounts" | "none"; showRewards?: boolean; maxSupportedTransactionVersion?: number; /** replay recorded txs (Geyser recorder, <=24h) from this slot before going live */ fromSlot?: number }
export type GetTx = (sig: string, cfg: any) => Promise<any>;
export const MAX_FILTER_ACCOUNTS = Number(process.env.TX_FILTER_MAX_ACCOUNTS ?? 100);

export const MAX_FILTER_ACCOUNTS_GEYSER = Number(process.env.TX_FILTER_MAX_ACCOUNTS_GEYSER ?? 50_000);
/** true when the hub's upstream can stream transactions natively (Yellowstone gRPC) */
export const nativeTx = (hub: StreamHub) => typeof (hub.upstream as any)?.subscribeTransactions === "function";

export function validateTxFilter(f: any, max = MAX_FILTER_ACCOUNTS): TxFilter {
  if (f == null || typeof f !== "object" || Array.isArray(f)) throw INVALID_PARAMS("transactionSubscribe: params[0] must be a filter object");
  for (const k of ["accountInclude", "accountExclude", "accountRequired"] as const) {
    if (f[k] === undefined) continue;
    if (!Array.isArray(f[k]) || !f[k].every(isPubkey)) throw INVALID_PARAMS(`transactionSubscribe: ${k} must be an array of base58 pubkeys`);
  }
  const watch = new Set([...(f.accountInclude ?? []), ...(f.accountRequired ?? [])]);
  if (!watch.size) throw INVALID_PARAMS("transactionSubscribe: accountInclude or accountRequired must be non-empty (logs-based backend)");
  if (watch.size > max) throw INVALID_PARAMS(`transactionSubscribe: max ${max} accounts`);
  return f;
}

/** Collect every account key of a getTransaction result (static + loaded via lookup tables). */
export function txAccountKeys(r: any): string[] {
  const keys: string[] = [];
  const tx = r?.transaction;
  if (Array.isArray(tx)) { try { keys.push(...decodeTransaction(tx[0], tx[1]).accountKeys); } catch {} }
  else for (const k of tx?.message?.accountKeys ?? []) keys.push(typeof k === "string" ? k : k.pubkey);
  const la = r?.meta?.loadedAddresses;
  if (la) keys.push(...(la.writable ?? []), ...(la.readonly ?? []));
  return keys;
}

export function matchesFilter(f: TxFilter, keys: string[], failed: boolean): boolean {
  if (failed && f.failed === false) return false;
  if (f.vote !== true && keys.includes(VOTE_PROGRAM)) return false;
  const set = new Set(keys);
  if (f.accountRequired?.some((k) => !set.has(k))) return false;
  if (f.accountExclude?.some((k) => set.has(k))) return false;
  if (f.accountInclude?.length && !f.accountInclude.some((k) => set.has(k))) return false;
  return true;
}

export interface TxEvent { signature: string; slot: number; tx: any; transactionIndex?: number }

export class TxWatcher {
  private detach: (() => void)[] = [];
  private seen = new Set<string>(); private seenQ: string[] = [];
  private queue: { sig: string; slot: number }[] = [];
  private inflight = 0;
  private stopped = false;
  stats = { matched: 0, filtered: 0, fetchFailed: 0, dropped: 0 };
  constructor(private hub: StreamHub, private getTx: GetTx, private filter: TxFilter, private opts: TxOptions, private onTx: (e: TxEvent) => void,
    private cfg: { concurrency: number; maxQueue: number; retries: number; retryMs?: number } = { concurrency: 8, maxQueue: 2000, retries: 4 }) {
    if (nativeTx(hub)) {
      // Geyser path: the node filters and ships full transactions; no getTransaction round trip
      const un = (hub.upstream as any).subscribeTransactions(filter, opts.commitment ?? hub.chain.defaultCommitment, (r: any) => this.onNative(r));
      this.detach.push(un);
      const rec = (hub.upstream as any).recorder;
      if (opts.fromSlot != null && rec) void this.replay(rec, opts.fromSlot);
      return;
    }
    const watch = new Set([...(filter.accountInclude ?? []), ...(filter.accountRequired ?? [])]);
    for (const a of watch) {
      const r = hub.subscribe("logs", { mentions: a, commitment: opts.commitment ?? hub.chain.defaultCommitment }, { deliver: (e: Entry) => this.onLog(e) });
      this.detach.push(r.detach);
    }
  }
  private onNative(r: any) {
    if (this.stopped) return;
    const sig = r?.transaction?.signatures?.[0]; if (!sig || this.seen.has(sig)) return;
    this.seen.add(sig); this.seenQ.push(sig); if (this.seenQ.length > 20_000) this.seen.delete(this.seenQ.shift()!);
    if ((r.isVote && this.filter.vote !== true) || !matchesFilter(this.filter, txAccountKeys(r), !!r.meta?.err)) { this.stats.filtered++; return; }
    this.stats.matched++;
    const { isVote, transactionIndex, ...tx } = r;
    try { this.onTx({ signature: sig, slot: r.slot, tx, transactionIndex }); } catch {}
  }
  private async replay(rec: any, fromSlot: number) {
    for await (const r of rec.replay(fromSlot)) {
      if (this.stopped) return;
      if (this.seen.has(r.sig)) continue;
      if ((r.vote && this.filter.vote !== true) || !matchesFilter(this.filter, r.keys, r.failed)) continue;
      this.seen.add(r.sig); this.seenQ.push(r.sig);
      this.stats.matched++;
      const { isVote, transactionIndex, ...tx } = r.tx;
      try { this.onTx({ signature: r.sig, slot: r.slot, tx, transactionIndex }); } catch {}
    }
  }
  private onLog(e: Entry) {
    const v = e.data; const sig = v?.signature;
    if (!sig || this.seen.has(sig)) return;
    this.seen.add(sig); this.seenQ.push(sig); if (this.seenQ.length > 20_000) this.seen.delete(this.seenQ.shift()!);
    if (v.err && this.filter.failed === false) { this.stats.filtered++; return; }
    // TowerBFT-only heuristic: under Alpenglow votes are not transactions, so skip the scan entirely
    if (this.filter.vote !== true && !this.hub.chain.alpenglow && (v.logs ?? []).some((l: string) => l.includes(VOTE_PROGRAM))) { this.stats.filtered++; return; }
    if (this.queue.length >= this.cfg.maxQueue) { this.queue.shift(); this.stats.dropped++; }
    this.queue.push({ sig, slot: e.slot });
    this.pump();
  }
  private pump() {
    while (!this.stopped && this.inflight < this.cfg.concurrency && this.queue.length) {
      const job = this.queue.shift()!; this.inflight++;
      this.fetch(job.sig).then((r) => {
        if (this.stopped) return;
        if (!r) { this.stats.fetchFailed++; return; }
        if (!matchesFilter(this.filter, txAccountKeys(r), !!r.meta?.err)) { this.stats.filtered++; return; }
        this.stats.matched++;
        try { this.onTx({ signature: job.sig, slot: r.slot ?? job.slot, tx: r }); } catch {}
      }).finally(() => { this.inflight--; this.pump(); });
    }
  }
  private async fetch(sig: string) {
    const cfg = { encoding: this.opts.encoding ?? "json", commitment: this.opts.commitment ?? this.hub.chain.defaultCommitment, maxSupportedTransactionVersion: this.opts.maxSupportedTransactionVersion ?? 0 };
    for (let i = 0; i <= this.cfg.retries; i++) {
      try { const r = await this.getTx(sig, cfg); if (r) return r; } catch {}
      if (this.stopped) return null;
      // retry spacing follows the observed slot rate (tx becomes visible ~1-2 slots later), not a fixed 400ms
      await new Promise((res) => setTimeout(res, (this.cfg.retryMs ?? this.hub.chain.slots(2, 100)) * (i + 1)));
    }
    return null;
  }
  stop() { this.stopped = true; for (const d of this.detach) d(); this.detach = []; this.queue = []; }
}

/** Helius transactionNotification result shape. */
export function heliusTxResult(e: TxEvent, details: TxOptions["transactionDetails"] = "full") {
  const r = e.tx;
  const inner = details === "signatures" ? { signatures: Array.isArray(r.transaction) ? undefined : r.transaction?.signatures, meta: r.meta }
    : details === "none" ? { meta: r.meta }
    : details === "accounts" ? { transaction: accountsView(r), meta: r.meta ? (({ innerInstructions, logMessages, ...m }: any) => m)(r.meta) : r.meta }
    : { transaction: r.transaction, meta: r.meta };
  return { transaction: { ...inner, version: r.version ?? "legacy" }, signature: e.signature, slot: e.slot, ...(e.transactionIndex != null ? { transactionIndex: e.transactionIndex } : {}) };
}
/** Solana transactionDetails:"accounts" view: signatures + accountKeys {pubkey, writable, signer, source} */
function accountsView(r: any) {
  const m = r.transaction?.message; if (!m || Array.isArray(r.transaction)) return r.transaction;
  const h = m.header ?? {}; const st: string[] = (m.accountKeys ?? []).map((k: any) => (typeof k === "string" ? k : k.pubkey));
  const nSig = h.numRequiredSignatures ?? 0, roS = h.numReadonlySignedAccounts ?? 0, roU = h.numReadonlyUnsignedAccounts ?? 0;
  const keys = st.map((pubkey, i) => ({ pubkey, signer: i < nSig, writable: i < nSig ? i < nSig - roS : i < st.length - roU, source: "transaction" }));
  for (const pubkey of r.meta?.loadedAddresses?.writable ?? []) keys.push({ pubkey, signer: false, writable: true, source: "lookupTable" });
  for (const pubkey of r.meta?.loadedAddresses?.readonly ?? []) keys.push({ pubkey, signer: false, writable: false, source: "lookupTable" });
  return { signatures: r.transaction.signatures, accountKeys: keys };
}
