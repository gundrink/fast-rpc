import net from "node:net";
import { encode, FrameDecoder, MsgType } from "./protocol.js";

type Pending = { resolve: (v: any) => void; reject: (e: Error) => void };
export type StreamMsg<T = any> = { seq: number; slot: number; data: T };
type Sub = {
  type: string; params: any; onMessage: (m: StreamMsg) => void;
  lastSeq: number; resuming: boolean; fromSlot?: number;
};
export interface ClientOptions { reconnect?: boolean; pingMs?: number; maxBackoffMs?: number; log?: (...a: any[]) => void }

export class RpcClient {
  private sock?: net.Socket;
  private nextId = 1;
  private pending = new Map<number, Pending>();
  private queue: Buffer[] = [];
  private flushScheduled = false;
  private subs = new Map<number, Sub>();
  private closed = false;
  private pingTimer?: NodeJS.Timeout;
  private lastPong = Date.now();
  private backoff = 200;
  reconnects = 0;

  private constructor(private port: number, private host: string, private opts: ClientOptions) {}

  static async connect(port: number, host = "127.0.0.1", opts: ClientOptions = {}) {
    const c = new RpcClient(port, host, { reconnect: true, pingMs: 5000, maxBackoffMs: 5000, ...opts });
    await c.open();
    return c;
  }

  private open() {
    return new Promise<void>((res, rej) => {
      const sock = net.connect(this.port, this.host, () => {
        sock.off("error", rej); sock.on("error", () => {});
        this.sock = sock; this.backoff = 200; this.lastPong = Date.now();
        this.startPing();
        // Resume every stream from its last seen seq (server replays the gap from its ring buffer).
        for (const [id, s] of this.subs) this.sendSubscribe(id, s).catch(() => {});
        res();
      });
      sock.once("error", rej);
      sock.setNoDelay(true);
      const dec = new FrameDecoder((type, id, payload) => this.onFrame(type, id, payload));
      sock.on("data", (d: Buffer) => dec.push(d));
      sock.on("close", () => this.onClose(sock));
    });
  }

  private onFrame(type: MsgType, id: number, payload: any) {
    if (type === MsgType.Pong) { this.lastPong = Date.now(); return; }
    if (type === MsgType.Notification) {
      const s = this.subs.get(id); if (!s) return;
      if (payload.seq <= s.lastSeq) return;                      // duplicate (at-least-once) -> skip
      if (s.lastSeq && payload.seq > s.lastSeq + 1) {            // gap (backpressure drop) -> resume from ring
        if (!s.resuming) this.sendSubscribe(id, s).catch(() => {});
        return;
      }
      s.lastSeq = payload.seq;
      s.onMessage(payload);
      return;
    }
    const p = this.pending.get(id); if (!p) return;
    this.pending.delete(id);
    type === MsgType.Error ? p.reject(new Error(payload)) : p.resolve(payload);
  }

  private onClose(sock: net.Socket) {
    if (this.sock === sock) this.sock = undefined;
    clearInterval(this.pingTimer);
    for (const p of this.pending.values()) p.reject(new Error("connection closed"));
    this.pending.clear();
    if (this.closed || !this.opts.reconnect) return;
    const retry = () => {
      if (this.closed) return;
      this.reconnects++;
      this.opts.log?.(`[client] reconnecting (attempt ${this.reconnects})…`);
      this.open().catch(() => { this.backoff = Math.min(this.backoff * 2, this.opts.maxBackoffMs!); setTimeout(retry, this.backoff); });
    };
    setTimeout(retry, this.backoff);
  }

  private startPing() {
    clearInterval(this.pingTimer);
    const ms = this.opts.pingMs!;
    this.pingTimer = setInterval(() => {
      if (Date.now() - this.lastPong > ms * 3) { this.sock?.destroy(); return; } // dead peer -> reconnect
      this.write(encode(MsgType.Ping, 0, null));
    }, ms);
    this.pingTimer.unref();
  }

  private write(b: Buffer) {
    this.queue.push(b);
    if (!this.flushScheduled) { // batch writes per tick
      this.flushScheduled = true;
      queueMicrotask(() => {
        this.flushScheduled = false;
        const q = this.queue; this.queue = [];
        if (this.sock && !this.sock.destroyed) this.sock.write(q.length === 1 ? q[0] : Buffer.concat(q));
      });
    }
  }

  private request(type: MsgType, id: number, payload: any) {
    return new Promise<any>((resolve, reject) => {
      if (!this.sock) return reject(new Error("not connected"));
      this.pending.set(id, { resolve, reject });
      this.write(encode(type, id, payload));
    });
  }

  private id() { return (this.nextId = (this.nextId % 0xffffffff) + 1); }

  /** Multiplexed call: many in flight on one socket, matched by request id. */
  call<T = any>(method: string, ...args: any[]): Promise<T> {
    return this.request(MsgType.Request, this.id(), [method, args]);
  }

  private async sendSubscribe(id: number, s: Sub) {
    s.resuming = true;
    const from = s.lastSeq ? { fromSeq: s.lastSeq } : s.fromSlot != null ? { fromSlot: s.fromSlot } : {};
    try { return await this.request(MsgType.Subscribe, id, { type: s.type, params: s.params, ...from }); }
    finally { s.resuming = false; }
  }

  /**
   * Subscribe to a stream ('slot' | 'account' | 'logs'). Returns an async unsubscribe fn.
   * Survives reconnects: resumes from last seen seq, duplicates are filtered by seq.
   */
  async subscribe<T = any>(type: "slot" | "account" | "logs", params: any, onMessage: (m: StreamMsg<T>) => void,
    opts: { fromSlot?: number } = {}): Promise<() => Promise<void>> {
    const id = this.id();
    const s: Sub = { type, params, onMessage, lastSeq: 0, resuming: false, fromSlot: opts.fromSlot };
    this.subs.set(id, s); // replayed entries arrive before the ack and are delivered in seq order
    const from = opts.fromSlot != null ? { fromSlot: opts.fromSlot } : {};
    try { await this.request(MsgType.Subscribe, id, { type, params, ...from }); }
    catch (e) { this.subs.delete(id); throw e; }
    return async () => { this.subs.delete(id); if (this.sock) await this.request(MsgType.Unsubscribe, id, null).catch(() => {}); };
  }

  /** Async-iterator flavour: `for await (const m of client.stream('slot', {})) …` */
  async *stream<T = any>(type: "slot" | "account" | "logs", params: any, opts: { fromSlot?: number } = {}): AsyncGenerator<StreamMsg<T>> {
    const buf: StreamMsg<T>[] = []; let wake: (() => void) | undefined;
    const unsub = await this.subscribe<T>(type, params, (m) => { buf.push(m); wake?.(); }, opts);
    try {
      while (true) {
        while (buf.length) yield buf.shift()!;
        await new Promise<void>((r) => (wake = r)); wake = undefined;
      }
    } finally { await unsub(); }
  }

  /** Typed proxy: `api.add(1,2)` feels like a local call. */
  proxy<T extends Record<string, (...a: any[]) => any>>(): { [K in keyof T]: (...a: Parameters<T[K]>) => Promise<Awaited<ReturnType<T[K]>>> } {
    return new Proxy({} as any, { get: (_, name: string) => (...a: any[]) => this.call(name, ...a) });
  }

  /** Test helper: kill the TCP socket abruptly to exercise reconnect + resume. */
  simulateDrop() { this.sock?.destroy(); }
  close() { this.closed = true; clearInterval(this.pingTimer); this.sock?.end(); }
}
