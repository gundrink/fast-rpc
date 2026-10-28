// Single shared WebSocket to Solana, with auto-reconnect + resubscribe of every live handle.
import WebSocket from "ws";

/** comma-separated list allowed: rotates to the next URL when a connection dies before becoming stable */
export const wsUrl = process.env.SOLANA_WS_URL ?? "wss://api.mainnet-beta.solana.com,wss://api.mainnet.solana.com";
type Handle = { method: string; params: any[]; onData: (r: any) => void; upId?: number; closed?: boolean; lastData?: number };
export interface Upstream { subscribe(method: string, params: any[], onData: (r: any) => void): () => void }

export class SolanaWs implements Upstream {
  private ws?: WebSocket;
  private open = false;
  private nextId = 1;
  private reqs = new Map<number, { res: (m: any) => void; rej: (e: Error) => void; t: NodeJS.Timeout }>();
  private handles = new Set<Handle>();
  private byUpId = new Map<number, Handle>();
  private pingTimer?: NodeJS.Timeout;
  private idleTimer?: NodeJS.Timeout;
  private retryTimer?: NodeJS.Timeout;
  private backoff = 500;
  private shut = false;
  connects = 0;
  /** last time any subscription notification arrived (watchdog) */
  private urlIx = 0;
  lastData = 0; lastOpen = 0; watchdogDrops = 0;
  private dogTimer?: NodeJS.Timeout;
  constructor(private url = wsUrl, private log: (...a: any[]) => void = () => {}, private opts: { reqTimeoutMs: number; pingMs: number; handshakeMs?: number; silentMs?: number; dogMs?: number } = { reqTimeoutMs: 10_000, pingMs: 20_000 }) {}
  status() { return { url: this.url.split(",")[this.urlIx % this.url.split(",").length], open: this.open, connects: this.connects, handles: this.handles.size, liveSubs: this.byUpId.size, lastDataAgoMs: this.lastData ? Date.now() - this.lastData : null, watchdogDrops: this.watchdogDrops, backoffMs: this.backoff }; }
  /** Watchdog: handshake hangs, sockets that stay "open" (pongs ok) but deliver no notifications, and subs never acked. */
  private startDog() {
    if (this.dogTimer) return;
    const silent = this.opts.silentMs ?? Number(process.env.UPSTREAM_SILENT_MS ?? 90_000);
    this.dogTimer = setInterval(() => {
      if (this.shut || !this.handles.size) return;
      if (!this.ws && !this.retryTimer) { this.log("[upstream] watchdog: no socket and no retry pending -> connect"); this.connect(); return; }
      if (!this.open) return;
      // logsSubscribe on busy programs (pump) is never quiet for long: a silent logs sub means the server dropped it
      const quietLogs = [...this.handles].some((h) => h.method === "logsSubscribe" && h.upId != null && Date.now() - Math.max(h.lastData ?? 0, this.lastOpen) > silent);
      const since = quietLogs ? 0 : Math.max(this.lastData, this.lastOpen);
      const unacked = [...this.handles].some((h) => h.upId == null);
      if (Date.now() - since > silent || (unacked && Date.now() - this.lastOpen > Math.min(30_000, silent))) {
        this.watchdogDrops++; this.log(`[upstream] watchdog: ${unacked ? "unacked subs" : `no data ${Math.round((Date.now() - since) / 1000)}s`} -> reconnect`);
        this.ws?.terminate();
      }
    }, this.opts.dogMs ?? 10_000);
    this.dogTimer.unref?.();
  }

  private connect() {
    if (this.ws || this.shut) return;
    this.retryTimer = undefined; this.startDog();
    const urls = this.url.split(",").map((x) => x.trim()).filter(Boolean); const url = urls[this.urlIx % urls.length];
    const ws = (this.ws = new WebSocket(url, { handshakeTimeout: this.opts.handshakeMs ?? 10_000 }));
    let alive = true;
    ws.on("open", () => {
      this.open = true; this.connects++; this.lastOpen = Date.now();
      // reset backoff only once the link proved stable (data flowing), else a server that drops right after open causes a 500ms hot loop
      const stable = setTimeout(() => { if (this.ws === ws && this.lastData > this.lastOpen) this.backoff = 500; }, 30_000); stable.unref?.();
      this.log(`[upstream] ${new Date().toISOString()} connected ${url}`);
      for (const h of this.handles) this.sendSub(h);
      this.pingTimer = setInterval(() => {
        if (!alive) { ws.terminate(); return; } // no pong since last ping -> dead
        alive = false; ws.ping();
      }, this.opts.pingMs);
    });
    ws.on("pong", () => (alive = true));
    ws.on("message", (raw) => {
      let m: any; try { m = JSON.parse(raw.toString()); } catch { return; }
      if (m.id != null) { const r = this.reqs.get(m.id); if (r) { clearTimeout(r.t); this.reqs.delete(m.id); r.res(m); } return; }
      const sub = m.params?.subscription;
      if (sub != null) { this.lastData = Date.now(); const hh = this.byUpId.get(sub); if (hh) hh.lastData = this.lastData; try { this.byUpId.get(sub)?.onData(m.params.result); } catch (e: any) { this.log("[upstream] handler error", e?.message); } }
    });
    ws.on("close", () => {
      if (!(this.lastOpen && this.lastData > this.lastOpen && Date.now() - this.lastOpen > 30_000)) this.urlIx++; // unstable -> try next URL
      clearInterval(this.pingTimer); this.open = false; this.ws = undefined; this.byUpId.clear();
      for (const r of this.reqs.values()) { clearTimeout(r.t); r.rej(new Error("upstream closed")); }
      this.reqs.clear();
      for (const h of this.handles) h.upId = undefined;
      if (this.handles.size && !this.shut) {
        this.log(`[upstream] ${new Date().toISOString()} closed, reconnecting in ${this.backoff}ms`);
        this.retryTimer = setTimeout(() => this.connect(), this.backoff);
        this.backoff = Math.min(this.backoff * 2, 30_000);
      }
    });
    ws.on("error", (e) => this.log("[upstream] error", e.message));
  }

  /** `sync` runs inside the message handler (before any later frame), unlike the promise continuation. */
  private rpc(method: string, params: any[], sync?: (m: any) => void) {
    return new Promise<any>((res, rej) => {
      if (!this.ws || !this.open) return rej(new Error("upstream not connected"));
      const id = this.nextId++;
      const t = setTimeout(() => { this.reqs.delete(id); rej(new Error(`${method} timeout`)); }, this.opts.reqTimeoutMs);
      this.reqs.set(id, { res: (m) => { sync?.(m); res(m); }, rej, t });
      this.ws.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
    });
  }

  private async sendSub(h: Handle) {
    try {
      // Register the upstream sub id synchronously: notifications can arrive in the same TCP chunk as the ack.
      const r = await this.rpc(h.method, h.params, (m) => {
        if (m.error || h.closed) return;
        h.upId = m.result; this.byUpId.set(m.result, h);
      });
      if (r.error) { this.log(`[upstream] ${h.method} error`, r.error.message); return; }
      if (h.closed) { this.rpc(h.method.replace("Subscribe", "Unsubscribe"), [r.result]).catch(() => {}); return; }
    } catch (e: any) { this.log(`[upstream] ${h.method} failed: ${e.message}`); } // resent on reconnect
  }

  subscribe(method: string, params: any[], onData: (r: any) => void) {
    const h: Handle = { method, params, onData };
    this.handles.add(h);
    clearTimeout(this.idleTimer);
    if (this.open) this.sendSub(h); else this.connect();
    return () => {
      if (h.closed) return;
      h.closed = true; this.handles.delete(h);
      if (h.upId != null) {
        this.byUpId.delete(h.upId);
        if (this.open) this.rpc(method.replace("Subscribe", "Unsubscribe"), [h.upId]).catch(() => {});
      }
      if (!this.handles.size) {
        clearTimeout(this.idleTimer);
        this.idleTimer = setTimeout(() => { if (!this.handles.size) { clearTimeout(this.retryTimer); this.ws?.close(); } }, 5000);
        this.idleTimer.unref();
      }
    };
  }

  /** Test/ops helper: force-drop the upstream socket (it will reconnect + resubscribe). */
  forceDrop() { this.ws?.terminate(); }
  close() { this.shut = true; clearInterval(this.dogTimer); clearTimeout(this.retryTimer); clearTimeout(this.idleTimer); this.ws?.terminate(); }
}
