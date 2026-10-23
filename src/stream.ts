// Subscription manager: one upstream sub per topic, fan-out, dedup, seq numbers, ring buffer replay.
import { SolanaWs, Upstream } from "./upstream-ws.js";
import { INVALID_PARAMS } from "./errors.js";

export const STREAM_TYPES = ["slot", "root", "account", "program", "logs", "signature"] as const;
export type StreamType = (typeof STREAM_TYPES)[number];
/** seq: per-topic monotonically increasing; data: compact payload (binary API); raw: exact upstream result (WS compat). */
export interface Entry { seq: number; slot: number; data: any; raw: any }
export interface Subscriber { deliver(e: Entry): void; onEnd?(): void }

const isStr = (x: any) => typeof x === "string" && x.length > 0 && x.length <= 128;
const isCfg = (x: any) => x === undefined || (x !== null && typeof x === "object" && !Array.isArray(x));

/** Binary-API object params -> raw Solana subscribe params. */
export function toRawParams(type: StreamType, p: any = {}): any[] {
  const cfg = (extra: object = {}) => ({ commitment: p.commitment ?? "confirmed", ...extra });
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

export interface HubOptions { upstream?: Upstream; wsUrl?: string; ringSize?: number; lingerMs?: number; log?: (...a: any[]) => void }

class Topic {
  seq = 0;
  ring: Entry[] = [];
  subs = new Set<Subscriber>();
  private seen = new Set<string>(); private seenQ: string[] = [];
  private stopUpstream?: () => void;
  private lingerTimer?: NodeJS.Timeout;
  constructor(readonly key: string, readonly type: StreamType, readonly raw: any[], private hub: StreamHub) {}

  start(up: Upstream) {
    const spec = SPEC[this.type];
    this.stopUpstream = up.subscribe(`${this.type}Subscribe`, this.raw, (r) => {
      let k: string, e: Entry;
      try { k = spec.key(r); e = { seq: 0, slot: spec.slot(r), data: spec.data(r), raw: r }; }
      catch { return; } // malformed upstream message
      if (this.seen.has(k)) return; // dedup (e.g. replays after upstream reconnect)
      this.seen.add(k); this.seenQ.push(k);
      if (this.seenQ.length > 8192) this.seen.delete(this.seenQ.shift()!);
      e.seq = ++this.seq;
      this.ring.push(e); if (this.ring.length > this.hub.ringSize) this.ring.shift();
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
  teardown() { clearTimeout(this.lingerTimer); this.stopUpstream?.(); this.stopUpstream = undefined; this.hub.drop(this.key, this); }
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
  constructor(opts: HubOptions = {}) {
    this.upstream = opts.upstream ?? new SolanaWs(opts.wsUrl, opts.log);
    this.ringSize = opts.ringSize ?? Number(process.env.STREAM_RING ?? 2000);
    this.lingerMs = opts.lingerMs ?? 10_000;
  }

  /** Binary-API convenience: object params. */
  subscribe(type: StreamType, params: any, sub: Subscriber, from: { fromSeq?: number; fromSlot?: number } = {}) {
    if (!STREAM_TYPES.includes(type)) throw INVALID_PARAMS(`unknown stream type: ${type}`);
    return this.subscribeRaw(type, toRawParams(type, params ?? {}), sub, from);
  }

  /**
   * Attach subscriber to (type, raw params) topic. Does NOT deliver replay itself: caller sends its ack
   * first, then delivers `replay` in order (all synchronous, so no live entry can interleave).
   */
  subscribeRaw(type: StreamType, raw: any[], sub: Subscriber, from: { fromSeq?: number; fromSlot?: number } = {}) {
    if (!STREAM_TYPES.includes(type)) throw INVALID_PARAMS(`unknown stream type: ${type}`);
    if (!Array.isArray(raw)) throw INVALID_PARAMS("params must be an array");
    SPEC[type].validate(raw);
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
