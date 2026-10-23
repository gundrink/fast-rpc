// Single shared WebSocket to Solana, with auto-reconnect + resubscribe of every live handle.
import WebSocket from "ws";

export const wsUrl = process.env.SOLANA_WS_URL ?? "wss://api.mainnet-beta.solana.com";
type Handle = { method: string; params: any[]; onData: (r: any) => void; upId?: number; closed?: boolean };
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
  constructor(private url = wsUrl, private log: (...a: any[]) => void = () => {}, private opts = { reqTimeoutMs: 10_000, pingMs: 20_000 }) {}

  private connect() {
    if (this.ws || this.shut) return;
    const ws = (this.ws = new WebSocket(this.url));
    let alive = true;
    ws.on("open", () => {
      this.open = true; this.backoff = 500; this.connects++;
      this.log(`[upstream] connected ${this.url}`);
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
      if (sub != null) { try { this.byUpId.get(sub)?.onData(m.params.result); } catch (e: any) { this.log("[upstream] handler error", e?.message); } }
    });
    ws.on("close", () => {
      clearInterval(this.pingTimer); this.open = false; this.ws = undefined; this.byUpId.clear();
      for (const r of this.reqs.values()) { clearTimeout(r.t); r.rej(new Error("upstream closed")); }
      this.reqs.clear();
      for (const h of this.handles) h.upId = undefined;
      if (this.handles.size && !this.shut) {
        this.log(`[upstream] closed, reconnecting in ${this.backoff}ms`);
        this.retryTimer = setTimeout(() => this.connect(), this.backoff);
        this.backoff = Math.min(this.backoff * 2, 10_000);
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
  close() { this.shut = true; clearTimeout(this.retryTimer); clearTimeout(this.idleTimer); this.ws?.terminate(); }
}
