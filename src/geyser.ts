// Optional Yellowstone gRPC (Geyser) stream source. Implements the same Upstream interface as SolanaWs for
// slot/account/program topics, and adds native transaction streams (no getTransaction round trip).
// Anything it can't express (logs/signature/root subs, non-base64 encodings) is delegated to the WS fallback.
import { b58encode } from "./codec.js";
import type { Upstream } from "./upstream-ws.js";
import { decodeTxError } from "./txerror.js";
import { TxRecorder } from "./recorder.js";

export interface GeyserStream { write(req: any): any; on(ev: string, cb: (...a: any[]) => void): any; destroy?(): void; end?(): void }
export interface GeyserClientLike { connect?(): Promise<void>; subscribe(): Promise<GeyserStream> }
export interface GeyserOptions {
  url?: string; token?: string;
  /** inject a client (tests / custom transports). Default: @triton-one/yellowstone-grpc (optional dependency) */
  client?: GeyserClientLike;
  fallback: Upstream;
  log?: (...a: any[]) => void;
  pingMs?: number; minBackoffMs?: number; maxBackoffMs?: number;
  /** wait this long for the block's blockTime (blocksMeta) before emitting a tx with blockTime:null (GEYSER_BLOCKTIME_WAIT_MS) */
  blockTimeWaitMs?: number;
  /** record every streamed tx to disk for fromSlot replay (GEYSER_RECORD dir) */
  recorder?: TxRecorder;
  /** extra always-on recording filter (GEYSER_RECORD_ACCOUNTS, comma separated) */
  recordAccounts?: string[];
}
export interface TxStreamFilter { accountInclude?: string[]; accountExclude?: string[]; accountRequired?: string[]; vote?: boolean; failed?: boolean }

const CL: Record<string, number> = { processed: 0, confirmed: 1, finalized: 2 };
const SLOT_PROCESSED = 0, SLOT_FINALIZED = 2;
const b58 = (u?: Uint8Array) => (u ? b58encode(u) : "");
const num = (x: any) => (x == null ? 0 : Number(x));

type Sub = { kind: "account" | "program" | "slot" | "tx" | "meta"; filter: any; cb: (r: any) => void };

/** Convert a Yellowstone transaction update into the JSON-RPC getTransaction(encoding:"json") shape. */
export function geyserTxToJson(u: any, blockTime: number | null = null) {
  const info = u.transaction ?? {}; const tx = info.transaction ?? {}; const m = tx.message ?? {}; const meta = info.meta ?? {};
  const keys: string[] = (m.accountKeys ?? []).map(b58);
  const ix = (i: any) => ({ programIdIndex: i.programIdIndex, accounts: [...(i.accounts ?? [])], data: b58encode(i.data ?? new Uint8Array()), stackHeight: i.stackHeight ?? null });
  const tb = (b: any) => ({ accountIndex: b.accountIndex, mint: b.mint, owner: b.owner, programId: b.programId, uiTokenAmount: b.uiTokenAmount });
  return {
    slot: num(u.slot), blockTime, version: m.versioned ? 0 : "legacy",
    transaction: {
      signatures: (tx.signatures ?? [info.signature]).map(b58),
      message: {
        header: { numRequiredSignatures: m.header?.numRequiredSignatures ?? 0, numReadonlySignedAccounts: m.header?.numReadonlySignedAccounts ?? 0, numReadonlyUnsignedAccounts: m.header?.numReadonlyUnsignedAccounts ?? 0 },
        accountKeys: keys, recentBlockhash: b58(m.recentBlockhash), instructions: (m.instructions ?? []).map(ix),
        ...(m.versioned ? { addressTableLookups: (m.addressTableLookups ?? []).map((l: any) => ({ accountKey: b58(l.accountKey), writableIndexes: [...(l.writableIndexes ?? [])], readonlyIndexes: [...(l.readonlyIndexes ?? [])] })) } : {}),
      },
    },
    meta: {
      err: meta.err ? decodeTxError(meta.err.err) : null,
      status: meta.err ? { Err: decodeTxError(meta.err.err) } : { Ok: null },
      fee: num(meta.fee), preBalances: (meta.preBalances ?? []).map(num), postBalances: (meta.postBalances ?? []).map(num),
      innerInstructions: (meta.innerInstructions ?? []).map((g: any) => ({ index: g.index, instructions: (g.instructions ?? []).map(ix) })),
      logMessages: meta.logMessagesNone ? null : (meta.logMessages ?? []),
      preTokenBalances: (meta.preTokenBalances ?? []).map(tb), postTokenBalances: (meta.postTokenBalances ?? []).map(tb),
      rewards: [], loadedAddresses: { writable: (meta.loadedWritableAddresses ?? []).map(b58), readonly: (meta.loadedReadonlyAddresses ?? []).map(b58) },
      computeUnitsConsumed: meta.computeUnitsConsumed != null ? num(meta.computeUnitsConsumed) : undefined,
    },
    isVote: !!info.isVote,
    ...(info.index != null ? { transactionIndex: num(info.index) } : {}),
  };
}

const accountValue = (a: any) => ({ lamports: num(a.lamports), owner: b58(a.owner), data: [Buffer.from(a.data ?? []).toString("base64"), "base64"], executable: !!a.executable, rentEpoch: num(a.rentEpoch), space: a.data?.length ?? 0 });

/** One gRPC stream per commitment level; the full filter set is re-written on every change (Yellowstone semantics). */
class CommitmentStream {
  subs = new Map<string, Sub>();
  private stream?: GeyserStream; private closed = false; private backoff: number; private pingT?: NodeJS.Timeout; private writeQueued = false;
  private root = 0; private lastSlot = 0;
  private blockTimes = new Map<number, number>(); private pending = new Map<number, { u: any; filters: string[] }[]>();
  constructor(private g: GeyserUpstream, readonly commitment: number) { this.backoff = g.o.minBackoffMs; }
  async ensure() {
    if (this.stream || this.closed) return;
    try {
      const c = await this.g.client();
      const s = await c.subscribe();
      if (this.closed) { s.destroy?.(); return; }
      this.stream = s; this.backoff = this.g.o.minBackoffMs;
      s.on("data", (u: any) => this.onData(u));
      const drop = (e?: any) => { if (this.stream !== s) return; this.stream = undefined; clearInterval(this.pingT); this.g.o.log("[geyser] stream closed", e?.message ?? ""); this.retry(); };
      s.on("error", drop); s.on("end", () => drop()); s.on("close", () => drop());
      this.pingT = setInterval(() => this.write(true), this.g.o.pingMs); this.pingT.unref();
      this.write();
    } catch (e: any) { this.g.o.log("[geyser] connect failed", e?.message); this.retry(); }
  }
  private retry() {
    if (this.closed || !this.subs.size) return;
    const t = setTimeout(() => void this.ensure(), this.backoff); t.unref();
    this.backoff = Math.min(this.backoff * 2, this.g.o.maxBackoffMs);
  }
  request(ping = false) {
    const req: any = { accounts: {}, slots: {}, transactions: {}, transactionsStatus: {}, blocks: {}, blocksMeta: {}, entry: {}, accountsDataSlice: [], commitment: this.commitment };
    for (const [name, s] of this.subs) {
      if (s.kind === "account" || s.kind === "program") req.accounts[name] = s.filter;
      else if (s.kind === "slot") req.slots[name] = { filterByCommitment: false };
      else if (s.kind === "tx") req.transactions[name] = s.filter;
    }
    if ([...this.subs.values()].some((s) => s.kind === "tx")) req.blocksMeta.bm = {};
    if (ping) req.ping = { id: 1 };
    return req;
  }
  write(ping = false) {
    if (ping) { this.stream?.write(this.request(true)); return; }
    if (this.writeQueued) return; this.writeQueued = true;
    queueMicrotask(() => { this.writeQueued = false; if (this.stream) this.stream.write(this.request()); else void this.ensure(); });
  }
  private onData(u: any) {
    if (u.account) {
      const a = u.account.account; if (!a) return;
      const slot = num(u.account.slot), pubkey = b58(a.pubkey), value = accountValue(a);
      for (const name of u.filters ?? []) {
        const s = this.subs.get(name); if (!s) continue;
        s.cb(s.kind === "program" ? { context: { slot }, value: { pubkey, account: value } } : { context: { slot }, value });
      }
    } else if (u.slot) {
      const slot = num(u.slot.slot);
      if (u.slot.status === SLOT_FINALIZED) this.root = Math.max(this.root, slot);
      if (u.slot.status !== SLOT_PROCESSED || slot <= this.lastSlot) return;
      this.lastSlot = slot;
      for (const name of u.filters ?? []) this.subs.get(name)?.cb({ slot, parent: u.slot.parent != null ? num(u.slot.parent) : slot - 1, root: this.root });
    } else if (u.blockMeta) {
      const slot = num(u.blockMeta.slot); const bt = u.blockMeta.blockTime?.timestamp;
      if (bt == null) return;
      this.blockTimes.set(slot, num(bt)); if (this.blockTimes.size > 512) this.blockTimes.delete(this.blockTimes.keys().next().value!);
      const q = this.pending.get(slot); if (q) { this.pending.delete(slot); for (const p of q) this.emitTx(p.u, p.filters, num(bt)); }
    } else if (u.transaction) {
      const slot = num(u.transaction.slot); const filters = u.filters ?? [];
      const bt = this.blockTimes.get(slot);
      if (bt != null || this.g.o.blockTimeWaitMs <= 0) return this.emitTx(u.transaction, filters, bt ?? null);
      let q = this.pending.get(slot);
      if (!q) {
        q = []; this.pending.set(slot, q);
        const t = setTimeout(() => { const qq = this.pending.get(slot); if (!qq) return; this.pending.delete(slot); for (const p of qq) this.emitTx(p.u, p.filters, null); }, this.g.o.blockTimeWaitMs); t.unref();
      }
      q.push({ u: u.transaction, filters });
    }
  }
  private emitTx(t: any, filters: string[], blockTime: number | null) {
    const j = geyserTxToJson(t, blockTime);
    this.g.record(j);
    for (const name of filters) { const s = this.subs.get(name); if (s?.kind === "tx") s.cb(j); }
  }
  close() { this.closed = true; clearInterval(this.pingT); const s = this.stream; this.stream = undefined; s?.end?.(); s?.destroy?.(); }
}

export class GeyserUpstream implements Upstream {
  readonly o: Required<Omit<GeyserOptions, "client" | "url" | "token">> & GeyserOptions;
  private streams = new Map<number, CommitmentStream>();
  private next = 1;
  private clientP?: Promise<GeyserClientLike>;
  stats = { subs: 0, fallbackSubs: 0 };
  readonly recorder?: TxRecorder;
  constructor(opts: GeyserOptions) {
    this.o = { log: () => {}, pingMs: 10_000, minBackoffMs: 200, maxBackoffMs: 10_000, blockTimeWaitMs: Number(process.env.GEYSER_BLOCKTIME_WAIT_MS ?? 1500), recordAccounts: [], ...opts } as any;
    this.recorder = opts.recorder ?? (process.env.GEYSER_RECORD ? new TxRecorder(process.env.GEYSER_RECORD) : undefined);
    const ra = opts.recordAccounts ?? (process.env.GEYSER_RECORD_ACCOUNTS?.split(",").filter(Boolean) ?? []);
    if (this.recorder && ra.length) this.subscribeTransactions({ accountInclude: ra, vote: false }, "confirmed", () => {});
  }
  /** called for every tx emitted by any stream (deduped by signature inside the recorder) */
  record(j: any) {
    if (!this.recorder) return;
    const keys = [...j.transaction.message.accountKeys, ...j.meta.loadedAddresses.writable, ...j.meta.loadedAddresses.readonly];
    try { this.recorder.record({ slot: j.slot, sig: j.transaction.signatures[0], keys, failed: !!j.meta.err, vote: !!j.isVote, tx: j }); } catch (e: any) { this.o.log("[geyser] record failed", e?.message); }
  }
  client(): Promise<GeyserClientLike> {
    return (this.clientP ??= (async () => {
      if (this.o.client) { await this.o.client.connect?.(); return this.o.client; }
      const mod: any = await import("@triton-one/yellowstone-grpc" as string).catch(() => { throw new Error("GEYSER_URL set but @triton-one/yellowstone-grpc is not installed"); });
      const Client = mod.default?.default ?? mod.default ?? mod.Client;
      const c = new Client(this.o.url, this.o.token, { "grpc.max_receive_message_length": 64 * 1024 * 1024 });
      await c.connect?.();
      return c;
    })().catch((e) => { this.clientP = undefined; throw e; }));
  }
  private add(commitment: string | undefined, sub: Sub) {
    const cl = CL[commitment ?? "confirmed"] ?? 1;
    let s = this.streams.get(cl); if (!s) { s = new CommitmentStream(this, cl); this.streams.set(cl, s); }
    const name = `f${this.next++}`; s.subs.set(name, sub); s.write(); this.stats.subs++;
    const st = s;
    return () => { if (st.subs.delete(name)) { this.stats.subs--; if (st.subs.size) st.write(); else { st.close(); this.streams.delete(cl); } } };
  }
  subscribe(method: string, params: any[], onData: (r: any) => void): () => void {
    const cfg = params?.[1] ?? {};
    const enc = cfg.encoding ?? "base64";
    if (method === "slotSubscribe") return this.add(cfg.commitment ?? "processed", { kind: "slot", filter: {}, cb: onData });
    if (method === "accountSubscribe" && enc === "base64" && !cfg.dataSlice)
      return this.add(cfg.commitment, { kind: "account", filter: { account: [params[0]], owner: [], filters: [] }, cb: onData });
    if (method === "programSubscribe" && enc === "base64" && !cfg.dataSlice) {
      const filters = (cfg.filters ?? []).map((f: any) => f.dataSize != null ? { datasize: String(f.dataSize) } : { memcmp: { offset: String(f.memcmp.offset), base58: f.memcmp.bytes } });
      return this.add(cfg.commitment, { kind: "program", filter: { account: [], owner: [params[0]], filters }, cb: onData });
    }
    this.stats.fallbackSubs++;
    const un = this.o.fallback.subscribe(method, params, onData);
    return () => { this.stats.fallbackSubs--; un(); };
  }
  /** Native transaction stream: filters evaluated by the Geyser node (up to its own limits, e.g. 50k accounts). */
  subscribeTransactions(f: TxStreamFilter, commitment: string | undefined, onTx: (r: any) => void) {
    return this.add(commitment, { kind: "tx", cb: onTx, filter: {
      vote: f.vote === true ? undefined : false, failed: f.failed === false ? false : undefined,
      accountInclude: f.accountInclude ?? [], accountExclude: f.accountExclude ?? [], accountRequired: f.accountRequired ?? [] } });
  }
  close() { this.recorder?.close(); for (const s of this.streams.values()) s.close(); this.streams.clear(); (this.o.fallback as any).close?.(); }
}
