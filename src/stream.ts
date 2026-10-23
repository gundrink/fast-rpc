// Subscription manager: one upstream sub per topic, fan-out, dedup, ring buffer replay, seq numbers.
import { SolanaWs } from "./upstream-ws.js";

export type StreamType = "slot" | "account" | "logs";
export interface Entry { seq: number; slot: number; data: any }
export interface Subscriber { deliver(e: Entry): void }

const RING = Number(process.env.STREAM_RING ?? 2000);
const LINGER_MS = 10_000;

function upstreamSpec(type: StreamType, p: any): { method: string; params: any[]; slot: (r: any) => number; key: (r: any) => string } {
  const commitment = p?.commitment ?? "confirmed";
  switch (type) {
    case "slot": return { method: "slotSubscribe", params: [], slot: (r) => r.slot, key: (r) => `s${r.slot}` };
    case "account":
      if (!p?.pubkey) throw new Error("account stream needs params.pubkey");
      return { method: "accountSubscribe", params: [p.pubkey, { encoding: p.encoding ?? "base64", commitment }],
        slot: (r) => r.context.slot, key: (r) => `a${r.context.slot}` };
    case "logs": {
      const filter = p?.mentions ? { mentions: [p.mentions] } : (p?.filter ?? "all");
      return { method: "logsSubscribe", params: [filter, { commitment }],
        slot: (r) => r.context.slot, key: (r) => r.value.signature };
    }
    default: throw new Error(`unknown stream type: ${type}`);
  }
}

class Topic {
  seq = 0;
  ring: Entry[] = [];
  subs = new Set<Subscriber>();
  private seen = new Set<string>(); private seenQ: string[] = [];
  private stopUpstream?: () => void;
  private lingerTimer?: NodeJS.Timeout;
  constructor(readonly key: string, readonly type: StreamType, readonly params: any, private hub: StreamHub) {}

  start(ws: SolanaWs) {
    const spec = upstreamSpec(this.type, this.params);
    this.stopUpstream = ws.subscribe(spec.method, spec.params, (r) => {
      const k = spec.key(r);
      if (this.seen.has(k)) return; // dedup (e.g. after upstream reconnect)
      this.seen.add(k); this.seenQ.push(k);
      if (this.seenQ.length > 8192) this.seen.delete(this.seenQ.shift()!);
      const e: Entry = { seq: ++this.seq, slot: spec.slot(r), data: this.type === "logs" ? r.value : r.value ?? r };
      this.ring.push(e); if (this.ring.length > RING) this.ring.shift();
      for (const s of this.subs) s.deliver(e);
    });
  }
  add(s: Subscriber) { clearTimeout(this.lingerTimer); this.subs.add(s); }
  remove(s: Subscriber) {
    this.subs.delete(s);
    if (!this.subs.size) this.lingerTimer = setTimeout(() => { // keep ring warm briefly for reconnecting clients
      if (!this.subs.size) { this.stopUpstream?.(); this.hub.drop(this.key); }
    }, LINGER_MS);
  }
  replay(fromSeq?: number, fromSlot?: number): Entry[] {
    if (fromSeq != null) return this.ring.filter((e) => e.seq > fromSeq);
    if (fromSlot != null) return this.ring.filter((e) => e.slot >= fromSlot);
    return [];
  }
}

export class StreamHub {
  private topics = new Map<string, Topic>();
  readonly ws: SolanaWs;
  constructor(opts: { wsUrl?: string; log?: (...a: any[]) => void } = {}) { this.ws = new SolanaWs(opts.wsUrl, opts.log); }

  /** Attach subscriber; returns replayed entries (deliver them before live ones) + detach fn. */
  subscribe(type: StreamType, params: any, sub: Subscriber, from: { fromSeq?: number; fromSlot?: number } = {}) {
    upstreamSpec(type, params); // validate
    const key = `${type}:${JSON.stringify(params ?? {})}`;
    let t = this.topics.get(key);
    if (!t) { t = new Topic(key, type, params, this); this.topics.set(key, t); t.start(this.ws); }
    const replay = t.replay(from.fromSeq, from.fromSlot);
    for (const e of replay) sub.deliver(e);
    t.add(sub);
    const topic = t;
    return { key, headSeq: t.seq, replayed: replay.length, detach: () => topic.remove(sub) };
  }
  drop(key: string) { this.topics.delete(key); }
  stats() { return [...this.topics.values()].map((t) => ({ key: t.key, subscribers: t.subs.size, seq: t.seq, ring: t.ring.length })); }
}
