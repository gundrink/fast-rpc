import net from "node:net";
import { encode, FrameDecoder, MsgType } from "./protocol.js";
import type { StreamHub, Entry } from "./stream.js";
type Handler = (...args: any[]) => any | Promise<any>;
const HTTP_METHODS = ["POST", "GET ", "OPTI", "HEAD"];
// Backpressure: above SOFT bytes buffered, notifications are dropped (client sees a seq gap and resumes from ring);
// above HARD the slow consumer is disconnected.
const SOFT = Number(process.env.STREAM_SOFT_LIMIT ?? 1 << 20);
const HARD = Number(process.env.STREAM_HARD_LIMIT ?? 8 << 20);

export class RpcServer {
  private handlers = new Map<string, Handler>();
  private server = net.createServer((sock) => this.onConn(sock));
  private hub?: StreamHub;
  dropped = 0;
  register(name: string, fn: Handler) { this.handlers.set(name, fn); return this; }
  attachStreams(hub: StreamHub) { this.hub = hub; return this; }
  listen(port: number, host = "127.0.0.1") {
    return new Promise<void>((r) => this.server.listen(port, host, r));
  }
  close() { this.server.close(); }

  private async dispatch(method: string, args: any[]) {
    const fn = this.handlers.get(method);
    if (!fn) throw Object.assign(new Error(`Method not found: ${method}`), { code: -32601 });
    return fn(...(args ?? []));
  }

  private onConn(sock: net.Socket) {
    sock.setNoDelay(true);
    sock.on("error", () => sock.destroy());
    // Sniff first bytes: HTTP text mode vs binary frames.
    sock.once("data", (first: Buffer) => {
      const head = first.subarray(0, 4).toString("latin1");
      if (HTTP_METHODS.includes(head)) this.httpMode(sock, first);
      else this.binaryMode(sock, first);
    });
  }

  private binaryMode(sock: net.Socket, first: Buffer) {
    const subs = new Map<number, () => void>(); // subId -> detach
    const send = (b: Buffer) => { if (!sock.destroyed) sock.write(b); };
    const dec = new FrameDecoder(async (type, id, payload) => {
      switch (type) {
        case MsgType.Request: {
          const [method, args] = payload;
          try { send(encode(MsgType.Response, id, await this.dispatch(method, args))); }
          catch (e: any) { send(encode(MsgType.Error, id, String(e?.message ?? e))); }
          return;
        }
        case MsgType.Ping: send(encode(MsgType.Pong, id, payload ?? null)); return;
        case MsgType.Unsubscribe: subs.get(id)?.(); subs.delete(id); send(encode(MsgType.Response, id, true)); return;
        case MsgType.Subscribe: {
          if (!this.hub) { send(encode(MsgType.Error, id, "streaming not enabled")); return; }
          subs.get(id)?.(); // re-subscribe (resume) replaces previous registration
          try {
            const r = this.hub.subscribe(payload.type, payload.params, {
              deliver: (e: Entry) => {
                if (sock.writableLength > HARD) { sock.destroy(); return; }
                if (sock.writableLength > SOFT) { this.dropped++; return; }
                send(encode(MsgType.Notification, id, e));
              },
            }, { fromSeq: payload.fromSeq, fromSlot: payload.fromSlot });
            subs.set(id, r.detach);
            send(encode(MsgType.Response, id, { ok: true, topic: r.key, headSeq: r.headSeq, replayed: r.replayed }));
          } catch (e: any) { send(encode(MsgType.Error, id, String(e?.message ?? e))); }
          return;
        }
      }
    });
    sock.on("close", () => { for (const d of subs.values()) d(); subs.clear(); });
    dec.push(first);
    sock.on("data", (c: Buffer) => dec.push(c));
  }

  /** GET /stream?type=slot|account|logs&pubkey=..&mentions=..&fromSeq=..&fromSlot=.. as Server-Sent Events. */
  private sse(sock: net.Socket, path: string, headers: string) {
    const u = new URL(path, "http://x");
    const q = Object.fromEntries(u.searchParams);
    const lastId = /last-event-id:\s*(\d+)/i.exec(headers)?.[1];
    const params: any = {};
    if (q.pubkey) params.pubkey = q.pubkey;
    if (q.mentions) params.mentions = q.mentions;
    if (q.commitment) params.commitment = q.commitment;
    try {
      if (!this.hub) throw new Error("streaming not enabled");
      sock.write("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n");
      const r = this.hub.subscribe((q.type ?? "slot") as any, params, {
        deliver: (e) => {
          if (sock.writableLength > SOFT) { this.dropped++; return; }
          sock.write(`id: ${e.seq}\nevent: ${q.type ?? "slot"}\ndata: ${JSON.stringify(e)}\n\n`);
        },
      }, { fromSeq: lastId ? Number(lastId) : q.fromSeq ? Number(q.fromSeq) : undefined, fromSlot: q.fromSlot ? Number(q.fromSlot) : undefined });
      const hb = setInterval(() => sock.write(": ping\n\n"), 15_000);
      sock.on("close", () => { clearInterval(hb); r.detach(); });
    } catch (e: any) {
      const b = JSON.stringify({ error: String(e?.message ?? e) });
      sock.end(`HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(b)}\r\n\r\n${b}`);
    }
  }

  /** Minimal HTTP/1.1 JSON-RPC 2.0 (single or batch) + SSE stream. */
  private httpMode(sock: net.Socket, first: Buffer) {
    let buf = first;
    let streaming = false;
    const tryParse = async () => {
      while (!streaming) {
        const end = buf.indexOf("\r\n\r\n");
        if (end < 0) return;
        const headers = buf.subarray(0, end).toString("latin1");
        const [verb, path] = headers.split(" ", 2);
        if (verb === "GET" && path.startsWith("/stream")) { streaming = true; this.sse(sock, path, headers); return; }
        const len = Number(/content-length:\s*(\d+)/i.exec(headers)?.[1] ?? 0);
        if (buf.length < end + 4 + len) return;
        const body = buf.subarray(end + 4, end + 4 + len).toString("utf8");
        buf = buf.subarray(end + 4 + len);
        const keepAlive = !/connection:\s*close/i.test(headers);
        let out: any;
        try {
          const req = JSON.parse(body || "null");
          const one = async (r: any) => {
            try { return { jsonrpc: "2.0", id: r?.id ?? null, result: await this.dispatch(r.method, r.params ?? []) }; }
            catch (e: any) { return { jsonrpc: "2.0", id: r?.id ?? null, error: { code: e.code ?? -32000, message: String(e?.message ?? e) } }; }
          };
          out = Array.isArray(req) ? await Promise.all(req.map(one)) : await one(req);
        } catch { out = { jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } }; }
        const json = Buffer.from(JSON.stringify(out) + "\n");
        sock.write(`HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${json.length}\r\nConnection: ${keepAlive ? "keep-alive" : "close"}\r\n\r\n`);
        sock.write(json);
        if (!keepAlive) { sock.end(); return; }
      }
    };
    sock.on("data", (c: Buffer) => { if (!streaming) { buf = Buffer.concat([buf, c]); tryParse(); } });
    tryParse();
  }
}
