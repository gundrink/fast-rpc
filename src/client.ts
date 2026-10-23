import net from "node:net";
import { encode, FrameDecoder, MsgType } from "./protocol.js";

type Pending = { resolve: (v: any) => void; reject: (e: Error) => void; sync?: (v: any) => void; timer?: NodeJS.Timeout };
export type StreamMsg<T = any> = { seq: number; slot: number; data: T };
export type StreamKind = "slot" | "root" | "account" | "program" | "logs" | "signature";
type Sub = {
  type: StreamKind; params: any; onMessage: (m: StreamMsg) => void; onGap?: (lost: number) => void;
  lastSeq: number; resuming: boolean; fromSlot?: number;
};
export interface ClientOptions {
  reconnect?: boolean; pingMs?: number; minBackoffMs?: number; maxBackoffMs?: number; requestTimeoutMs?: number;
  log?: (...a: any[]) => void;
}

export class RpcClient {
  private sock?: net.Socket;
  private nextId = 1;
  private pending = new Map<number, Pending>();
  private queue: Buffer[] = [];
  private flushScheduled = false;
  private subs = new Map<number, Sub>();
  private closed = false;
  private pingTimer?: NodeJS.Timeout;
  private retryTimer?: NodeJS.Timeout;
  private lastPong = Date.now();
  private backoff: number;
  private o: Required<Omit<ClientOptions, "log">> & { log: (...a: any[]) => void };
  reconnects = 0;

  private constructor(private port: number, private host: string, opts: ClientOptions) {
    this.o = { reconnect: true, pingMs: 5000, minBackoffMs: 200, maxBackoffMs: 5000, requestTimeoutMs: 30_000, log: () => {}, ...opts };
    this.backoff = this.o.minBackoffMs;
  }

  static async connect(port: number, host = "127.0.0.1", opts: ClientOptions = {}) {
    const c = new RpcClient(port, host, opts);
    await c.open();
    return c;
  }

  get connected() { return !!this.sock; }

  private open() {
    return new Promise<void>((res, rej) => {
      const sock = net.connect(this.port, this.host);
      sock.setNoDelay(true);
      sock.once("error", rej); // only connect errors reject; 'close' handler is attached after connect
      sock.once("connect", () => {
        sock.off("error", rej);
        sock.on("error", () => {});
        sock.on("close", () => this.onClose(sock));
        if (this.closed) { sock.destroy(); return rej(new Error("client closed")); }
        this.sock = sock; this.backoff = this.o.minBackoffMs; this.lastPong = Date.now();
        this.startPing();
        // Resume every stream from its last seen seq (server replays the gap from its ring buffer).
        for (const [id, s] of this.subs) this.sendSubscribe(id, s).catch((e) => this.o.log(`[client] resubscribe ${s.type} failed: ${e.message}`));
        this.flush();
        res();
      });
      const dec = new FrameDecoder((type, id, payload) => this.onFrame(type, id, payload), {
        onError: (e) => { this.o.log("[client] bad frame:", e.message); sock.destroy(); },
      });
      sock.on("data", (d: Buffer) => dec.push(d));
    });
  }

  private onFrame(type: MsgType, id: number, payload: any) {
    this.lastPong = Date.now(); // any traffic proves liveness
    if (type === MsgType.Pong) return;
    if (type === MsgType.Notification) {
      const s = this.subs.get(id);
      if (!s || s.resuming || !payload || typeof payload.seq !== "number") return;
      if (payload.seq <= s.lastSeq) return;                // duplicate (at-least-once) -> skip
      if (s.lastSeq && payload.seq > s.lastSeq + 1) {      // gap (backpressure drop) -> resume from ring
        this.sendSubscribe(id, s).catch(() => {});
        return;
      }
      s.lastSeq = payload.seq;
      try { s.onMessage(payload); } catch (e: any) { this.o.log("[client] onMessage threw:", e?.message); }
      return;
    }
    const p = this.pending.get(id); if (!p) return;
    this.pending.delete(id); clearTimeout(p.timer);
    if (type === MsgType.Error) p.reject(new Error(String(payload)));
    else { p.sync?.(payload); p.resolve(payload); }
  }

  private onClose(sock: net.Socket) {
    if (this.sock !== sock) return;
    this.sock = undefined;
    clearInterval(this.pingTimer);
    this.queue = [];
    for (const p of this.pending.values()) { clearTimeout(p.timer); p.reject(new Error("connection closed")); }
    this.pending.clear();
    for (const s of this.subs.values()) s.resuming = false;
    if (this.closed || !this.o.reconnect) return;
    this.scheduleReconnect();
  }

  private scheduleReconnect() {
    const jitter = Math.random() * 0.2 * this.backoff;
    this.retryTimer = setTimeout(() => {
      if (this.closed) return;
      this.reconnects++;
      this.o.log(`[client] reconnecting (attempt ${this.reconnects})…`);
      this.open().catch(() => {
        this.backoff = Math.min(this.backoff * 2, this.o.maxBackoffMs);
        if (!this.closed) this.scheduleReconnect();
      });
    }, this.backoff + jitter);
  }

  private startPing() {
    clearInterval(this.pingTimer);
    const ms = this.o.pingMs;
    this.pingTimer = setInterval(() => {
      if (Date.now() - this.lastPong > ms * 3) { this.o.log("[client] keepalive timeout"); this.sock?.destroy(); return; }
      this.write(encode(MsgType.Ping, 0, null));
    }, ms);
    this.pingTimer.unref();
  }

  private write(b: Buffer) {
    this.queue.push(b);
    if (!this.flushScheduled) { this.flushScheduled = true; queueMicrotask(() => this.flush()); } // batch writes per tick
  }
  private flush() {
    this.flushScheduled = false;
    if (!this.sock || this.sock.destroyed || !this.queue.length) return;
    const q = this.queue; this.queue = [];
    this.sock.write(q.length === 1 ? q[0] : Buffer.concat(q));
  }

  private request(type: MsgType, id: number, payload: any, sync?: (v: any) => void) {
    return new Promise<any>((resolve, reject) => {
      if (!this.sock) return reject(new Error("not connected"));
      const timer = setTimeout(() => { if (this.pending.delete(id)) reject(new Error("request timeout")); }, this.o.requestTimeoutMs);
      timer.unref();
      this.pending.set(id, { resolve, reject, sync, timer });
      this.write(encode(type, id, payload));
    });
  }

  private id() {
    do { this.nextId = (this.nextId % 0xfffffffe) + 1; } while (this.pending.has(this.nextId) || this.subs.has(this.nextId));
    return this.nextId;
  }

  /** Multiplexed call: many in flight on one socket, matched by request id. */
  call<T = any>(method: string, ...args: any[]): Promise<T> {
    return this.request(MsgType.Request, this.id(), [method, args]);
  }

  private sendSubscribe(id: number, s: Sub) {
    s.resuming = true; // ignore notifications until ack (server sends ack -> replay -> live)
    const from = s.lastSeq ? { fromSeq: s.lastSeq } : s.fromSlot != null ? { fromSlot: s.fromSlot } : {};
    return this.request(MsgType.Subscribe, id, { type: s.type, params: s.params, ...from }, (ack) => {
      // runs synchronously on ack, before the replay frames in the same chunk are processed
      s.resuming = false;
      if (ack?.lost > 0) { s.lastSeq = ack.firstSeq - 1; try { s.onGap?.(ack.lost); } catch {} }
    }).catch((e) => { s.resuming = false; throw e; });
  }

  /**
   * Subscribe to a stream. Returns an async unsubscribe fn. Survives reconnects: resumes from the last
   * seen seq (at-least-once on the wire, duplicates filtered here). `onGap` fires if the server ring
   * no longer holds the missed range.
   */
  async subscribe<T = any>(type: StreamKind, params: any, onMessage: (m: StreamMsg<T>) => void,
    opts: { fromSlot?: number; onGap?: (lost: number) => void } = {}): Promise<() => Promise<void>> {
    const id = this.id();
    const s: Sub = { type, params, onMessage, onGap: opts.onGap, lastSeq: 0, resuming: false, fromSlot: opts.fromSlot };
    this.subs.set(id, s);
    try { await this.sendSubscribe(id, s); }
    catch (e) { this.subs.delete(id); throw e; }
    return async () => {
      if (!this.subs.delete(id)) return;
      if (this.sock) await this.request(MsgType.Unsubscribe, id, null).catch(() => {});
    };
  }

  /** Async-iterator flavour: `for await (const m of client.stream('slot', {})) …` */
  async *stream<T = any>(type: StreamKind, params: any, opts: { fromSlot?: number } = {}): AsyncGenerator<StreamMsg<T>> {
    const buf: StreamMsg<T>[] = []; let wake: (() => void) | undefined;
    const unsub = await this.subscribe<T>(type, params, (m) => { buf.push(m); const w = wake; wake = undefined; w?.(); }, opts);
    try {
      while (!this.closed) {
        while (buf.length) yield buf.shift()!;
        await new Promise<void>((r) => (wake = r));
      }
    } finally { await unsub(); }
  }

  /** Typed proxy: `api.getSlot()` feels like a local call. */
  proxy<T extends Record<string, (...a: any[]) => any>>(): { [K in keyof T]: (...a: Parameters<T[K]>) => Promise<Awaited<ReturnType<T[K]>>> } {
    return new Proxy({} as any, { get: (_, name) => (typeof name === "string" && name !== "then" ? (...a: any[]) => this.call(name, ...a) : undefined) });
  }

  /** Test helper: kill the TCP socket abruptly to exercise reconnect + resume. */
  simulateDrop() { this.sock?.destroy(); }
  close() {
    this.closed = true; clearInterval(this.pingTimer); clearTimeout(this.retryTimer);
    this.sock?.end(); this.sock = undefined;
    for (const p of this.pending.values()) { clearTimeout(p.timer); p.reject(new Error("client closed")); }
    this.pending.clear();
  }
}
