// Subscription manager: one upstream sub per topic, fan-out, dedup, seq numbers, ring buffer replay.
import { SolanaWs, Upstream } from "./upstream-ws.js";
import { GeyserUpstream } from "./geyser.js";
import { INVALID_PARAMS } from "./errors.js";
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { ChainState } from "./chain.js";

export const STREAM_TYPES = ["slot", "root", "account", "program", "logs", "signature"] as const;
export type StreamType = (typeof STREAM_TYPES)[number];
/** seq: per-topic monotonically increasing; data: compact payload (binary API); raw: exact upstream result (WS compat). */
export interface Entry { seq: number; slot: number; data: any; raw: any }
export interface Subscriber { deliver(e: Entry): void; onEnd?(): void }

const isStr = (x: any) => typeof x === "string" && x.length > 0 && x.length <= 128;
const isCfg = (x: any) => x === undefined || (x !== null && typeof x === "object" && !Array.isArray(x));

/** Binary-API object params -> raw Solana subscribe params. */
export function toRawParams(type: StreamType, p: any = {}, defaultCommitment = "confirmed"): any[] {
  const cfg = (extra: object = {}) => ({ commitment: p.commitment ?? defaultCommitment, ...extra });
  switch (type) {
    case "slot": case "root": return [];
    case "account": return [p.pubkey, cfg({ encoding: p.encoding ?? "base64" })];
    case "program": return [p.programId, cfg({ encoding: p.encoding ?? "base64", ...(p.filters ? { filters: p.filters } : {}) })];
    case "logs": return [p.mentions ? { mentions: [p.mentions] } : (p.filter ?? "all"), cfg()];
    case "signature": return [p.signature, cfg()];
  }
}

const SPEC: Record<StreamType, { validate(raw: any[]): void; slot(r: any): number; key(r: any): string; data(r: any): any; once?: boolean }> = {
  slot: { validate() {}, slot: (r) => r.slot, key: (r) => `s${r.slot}`, data: (r) => r },
  root: { validate() {}, slot: (r) => r, key: (r) => `r${r}`, data: (r) => r },
  account: { validate: (a) => { if (!isStr(a[0]) || !isCfg(a[1])) throw INVALID_PARAMS("accountSubscribe: expected [pubkey, config?]"); },
    slot: (r) => r.context.slot, key: (r) => `a${r.context.slot}`, data: (r) => r.value },
  program: { validate: (a) => { if (!isStr(a[0]) || !isCfg(a[1])) throw INVALID_PARAMS("programSubscribe: expected [programId, config?]"); },
    slot: (r) => r.context.slot, key: (r) => `p${r.context.slot}:${r.value?.pubkey}`, data: (r) => r.value },
  logs: { validate: (a) => {
      const f = a[0];
      const ok = f === "all" || f === "allWithVotes" || (f && Array.isArray(f.mentions) && f.mentions.length === 1 && isStr(f.mentions[0]));
      if (!ok || !isCfg(a[1])) throw INVALID_PARAMS('logsSubscribe: expected ["all"|"allWithVotes"|{mentions:[pubkey]}, config?]');
    },
    slot: (r) => r.context.slot, key: (r) => `l${r.value.signature}`, data: (r) => r.value },
  signature: { validate: (a) => { if (!isStr(a[0]) || !isCfg(a[1])) throw INVALID_PARAMS("signatureSubscribe: expected [signature, config?]"); },
    slot: (r) => r.context.slot, key: (r) => `g${r.context.slot}:${JSON.stringify(r.value)}`, data: (r) => r.value, once: true },
};

export interface HubOptions {
  upstream?: Upstream; wsUrl?: string; /** Yellowstone gRPC endpoint (GEYSER_URL) + x-token (GEYSER_TOKEN) */ geyserUrl?: string; geyserToken?: string; ringSize?: number; lingerMs?: number; log?: (...a: any[]) => void;
  /** Directory for the append-only disk ring (survives restarts). Default: STREAM_PERSIST_DIR env. */
  persistDir?: string;
  /** Drop persisted entries older than this. Default: STREAM_RETENTION_MS env or 1h. */
  retentionMs?: number;
  /** shared slot-rate/consensus state; slot topics feed it observations */
  chain?: ChainState;
}

/** Append-only JSONL log per topic: {seq,slot,ts,k,raw}. Compacted to the ring when it grows past 2x ringSize. */
class DiskLog {
  private fd: number; private lines = 0;
  constructor(readonly file: string) { this.fd = fs.openSync(file, "a"); }
  static fileFor(dir: string, key: string) { return path.join(dir, createHash("sha1").update(key).digest("hex").slice(0, 24) + ".jsonl"); }
  static load(file: string, retentionMs: number) {
    const out: { seq: number; slot: number; ts: number; k: string; raw: any }[] = [];
    if (!fs.existsSync(file)) return out;
    const cutoff = Date.now() - retentionMs;
    for (const line of fs.readFileSync(file, "utf8").split("\n")) {
      if (!line) continue;
      try { const r = JSON.parse(line); if (typeof r.seq === "number") out.push(r); } catch {} // tolerate torn last line
    }
    // seq must survive even if all entries expired: keep max seq, return only fresh entries
    const maxSeq = out.reduce((m, r) => Math.max(m, r.seq), 0);
    return Object.assign(out.filter((r) => r.ts >= cutoff), { maxSeq });
  }
  append(rec: object) { fs.writeSync(this.fd, JSON.stringify(rec) + "\n"); this.lines++; }
  get size() { return this.lines; }
  rewrite(recs: object[], maxSeq: number) {
    fs.closeSync(this.fd);
    const tmp = this.file + ".tmp";
    // header line preserves seq high-water mark even when ring is empty
    fs.writeFileSync(tmp, [JSON.stringify({ seq: maxSeq, slot: 0, ts: 0, k: "", raw: null, hdr: 1 }), ...recs.map((r) => JSON.stringify(r))].join("\n") + "\n");
    fs.renameSync(tmp, this.file);
    this.fd = fs.openSync(this.file, "a"); this.lines = recs.length + 1;
  }
  close() { try { fs.closeSync(this.fd); } catch {} }
}

class Topic {
  seq = 0;
  ring: Entry[] = [];
  subs = new Set<Subscriber>();
  private seen = new Set<string>(); private seenQ: string[] = [];
  private stopUpstream?: () => void;
  private lingerTimer?: NodeJS.Timeout;
  private disk?: DiskLog;
  private ts = new WeakMap<Entry, { ts: number; k: string }>();
  constructor(readonly key: string, readonly type: StreamType, readonly raw: any[], private hub: StreamHub) {
    if (hub.persistDir && !SPEC[type].once) {
      const file = DiskLog.fileFor(hub.persistDir, key);
      try {
        const recs = DiskLog.load(file, hub.retentionMs) as any;
        this.seq = recs.maxSeq ?? 0;
        for (const r of recs) {
          if (r.hdr) continue;
          const e: Entry = { seq: r.seq, slot: r.slot, data: SPEC[type].data(r.raw), raw: r.raw };
          this.ring.push(e); this.ts.set(e, { ts: r.ts, k: r.k });
          this.seen.add(r.k); this.seenQ.push(r.k);
        }
        if (this.ring.length > hub.ringSize) this.ring.splice(0, this.ring.length - hub.ringSize);
        this.disk = new DiskLog(file);
        this.compact();
      } catch (e: any) { hub.log?.("[stream] persist disabled for topic", key, e.message); this.disk = undefined; }
    }
  }
  private compact() {
    const cutoff = Date.now() - this.hub.retentionMs;
    this.disk!.rewrite(this.ring.filter((e) => (this.ts.get(e)?.ts ?? Date.now()) >= cutoff).map((e) => ({ seq: e.seq, slot: e.slot, ts: this.ts.get(e)?.ts ?? Date.now(), k: this.ts.get(e)?.k ?? "", raw: e.raw })), this.seq);
  }

  start(up: Upstream) {
    const spec = SPEC[this.type];
    this.stopUpstream = up.subscribe(`${this.type}Subscribe`, this.raw, (r) => {
      let k: string, e: Entry;
      try { k = spec.key(r); e = { seq: 0, slot: spec.slot(r), data: spec.data(r), raw: r }; }
      catch { return; } // malformed upstream message
      if (this.type === "slot") this.hub.chain.observe(e.slot);
      if (this.seen.has(k)) return; // dedup (e.g. replays after upstream reconnect)
      this.seen.add(k); this.seenQ.push(k);
      if (this.seenQ.length > 8192) this.seen.delete(this.seenQ.shift()!);
      e.seq = ++this.seq;
      this.ring.push(e); if (this.ring.length > this.hub.ringSize) this.ring.shift();
      if (this.disk) {
        const ts = Date.now(); this.ts.set(e, { ts, k });
        try { this.disk.append({ seq: e.seq, slot: e.slot, ts, k, raw: r }); if (this.disk.size > 2 * this.hub.ringSize) this.compact(); }
        catch (err: any) { this.hub.log?.("[stream] persist write failed", err.message); }
      }
      for (const s of [...this.subs]) { try { s.deliver(e); } catch {} }
      if (spec.once) { for (const s of [...this.subs]) s.onEnd?.(); this.subs.clear(); this.teardown(); }
    });
  }
  add(s: Subscriber) { clearTimeout(this.lingerTimer); this.lingerTimer = undefined; this.subs.add(s); }
  remove(s: Subscriber) {
    if (!this.subs.delete(s) || this.subs.size) return;
    clearTimeout(this.lingerTimer);
    // keep topic + ring warm briefly so reconnecting clients can replay
    this.lingerTimer = setTimeout(() => { if (!this.subs.size) this.teardown(); }, this.hub.lingerMs);
    this.lingerTimer.unref();
  }
  teardown() { clearTimeout(this.lingerTimer); this.disk?.close(); this.disk = undefined; this.stopUpstream?.(); this.stopUpstream = undefined; this.hub.drop(this.key, this); }
  replay(fromSeq?: number, fromSlot?: number): { entries: Entry[]; lost: number } {
    if (fromSeq != null) {
      const entries = this.ring.filter((e) => e.seq > fromSeq);
      const first = entries[0]?.seq ?? this.seq + 1;
      return { entries, lost: Math.max(0, first - fromSeq - 1) };
    }
    if (fromSlot != null) return { entries: this.ring.filter((e) => e.slot >= fromSlot), lost: 0 };
    return { entries: [], lost: 0 };
  }
}

export class StreamHub {
  private topics = new Map<string, Topic>();
  readonly upstream: Upstream;
  readonly ringSize: number;
  readonly lingerMs: number;
  readonly persistDir?: string;
  readonly retentionMs: number;
  readonly log?: (...a: any[]) => void;
  readonly chain: ChainState;
  constructor(opts: HubOptions = {}) {
    this.chain = opts.chain ?? new ChainState();
    this.persistDir = opts.persistDir ?? process.env.STREAM_PERSIST_DIR;
    this.retentionMs = opts.retentionMs ?? Number(process.env.STREAM_RETENTION_MS ?? 3_600_000);
    this.log = opts.log;
    if (this.persistDir) fs.mkdirSync(this.persistDir, { recursive: true });
    this.upstream = opts.upstream ?? (opts.geyserUrl ?? process.env.GEYSER_URL
      ? new GeyserUpstream({ url: opts.geyserUrl ?? process.env.GEYSER_URL, token: opts.geyserToken ?? process.env.GEYSER_TOKEN, fallback: new SolanaWs(opts.wsUrl, opts.log), log: opts.log })
      : new SolanaWs(opts.wsUrl, opts.log));
    this.ringSize = opts.ringSize ?? Number(process.env.STREAM_RING ?? 2000);
    this.lingerMs = opts.lingerMs ?? 10_000;
  }

  /** Binary-API convenience: object params. */
  subscribe(type: StreamType, params: any, sub: Subscriber, from: { fromSeq?: number; fromSlot?: number } = {}) {
    if (!STREAM_TYPES.includes(type)) throw INVALID_PARAMS(`unknown stream type: ${type}`);
    return this.subscribeRaw(type, toRawParams(type, params ?? {}, this.chain.defaultCommitment), sub, from);
  }

  /**
   * Attach subscriber to (type, raw params) topic. Does NOT deliver replay itself: caller sends its ack
   * first, then delivers `replay` in order (all synchronous, so no live entry can interleave).
   */
  subscribeRaw(type: StreamType, raw: any[], sub: Subscriber, from: { fromSeq?: number; fromSlot?: number } = {}) {
    if (!STREAM_TYPES.includes(type)) throw INVALID_PARAMS(`unknown stream type: ${type}`);
    if (!Array.isArray(raw)) throw INVALID_PARAMS("params must be an array");
    SPEC[type].validate(raw);
    // optional confirmed->finalized rewrite also merges equivalent topics under Alpenglow
    raw = raw.map((x) => x && typeof x === "object" && !Array.isArray(x) && typeof x.commitment === "string" ? { ...x, commitment: this.chain.mapCommitment(x.commitment) } : x);
    const key = `${type}:${JSON.stringify(raw)}`;
    let t = this.topics.get(key);
    if (!t) { t = new Topic(key, type, raw, this); this.topics.set(key, t); t.start(this.upstream); }
    const { entries, lost } = t.replay(from.fromSeq, from.fromSlot);
    t.add(sub);
    const topic = t;
    return { key, headSeq: t.seq, replay: entries, lost, detach: () => topic.remove(sub) };
  }
  drop(key: string, t: Topic) { if (this.topics.get(key) === t) this.topics.delete(key); }
  stats() { return [...this.topics.values()].map((t) => ({ key: t.key, subscribers: t.subs.size, seq: t.seq, ring: t.ring.length })); }
  close() { for (const t of [...this.topics.values()]) t.teardown(); (this.upstream as any).close?.(); }
}
