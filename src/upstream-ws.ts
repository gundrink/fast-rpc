// Single shared WebSocket to Solana, with auto-reconnect + resubscribe.
import WebSocket from "ws";

export const wsUrl = process.env.SOLANA_WS_URL ?? "wss://api.mainnet-beta.solana.com";
type Handle = { method: string; params: any[]; onData: (r: any) => void; upId?: number; closed?: boolean };

export class SolanaWs {
  private ws?: WebSocket;
  private open = false;
  private nextId = 1;
  private reqs = new Map<number, (msg: any) => void>();
  private handles = new Set<Handle>();
  private byUpId = new Map<number, Handle>();
  private pingTimer?: NodeJS.Timeout;
  constructor(private url = wsUrl, private log = (..._a: any[]) => {}) {}

  private connect() {
    if (this.ws) return;
    const ws = (this.ws = new WebSocket(this.url));
    ws.on("open", () => {
      this.open = true; this.log(`[upstream] connected ${this.url}`);
      for (const h of this.handles) this.sendSub(h);
      this.pingTimer = setInterval(() => ws.ping(), 20_000);
    });
    ws.on("message", (raw) => {
      const m = JSON.parse(raw.toString());
      if (m.id != null) { this.reqs.get(m.id)?.(m); this.reqs.delete(m.id); return; }
      if (m.params?.subscription != null) this.byUpId.get(m.params.subscription)?.onData(m.params.result);
    });
    ws.on("close", () => {
      this.log("[upstream] closed, reconnecting in 1s");
      clearInterval(this.pingTimer); this.open = false; this.ws = undefined; this.byUpId.clear();
      if (this.handles.size) setTimeout(() => this.connect(), 1000);
    });
    ws.on("error", (e) => this.log("[upstream] error", e.message));
  }

  private rpc(method: string, params: any[]) {
    return new Promise<any>((res) => {
      const id = this.nextId++;
      this.reqs.set(id, res);
      this.ws!.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
    });
  }

  private async sendSub(h: Handle) {
    const r = await this.rpc(h.method, h.params);
    if (r.error) { this.log(`[upstream] ${h.method} error`, r.error.message); return; }
    h.upId = r.result;
    if (h.closed) { this.rpc(h.method.replace("Subscribe", "Unsubscribe"), [r.result]); return; }
    this.byUpId.set(r.result, h);
  }

  subscribe(method: string, params: any[], onData: (r: any) => void) {
    const h: Handle = { method, params, onData };
    this.handles.add(h);
    if (this.open) this.sendSub(h); else this.connect();
    return () => {
      h.closed = true; this.handles.delete(h);
      if (h.upId != null && this.open) { this.byUpId.delete(h.upId); this.rpc(method.replace("Subscribe", "Unsubscribe"), [h.upId]); }
      if (!this.handles.size) setTimeout(() => { if (!this.handles.size) this.ws?.close(); }, 5000);
    };
  }
}
