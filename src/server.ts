import net from "node:net";
import http from "node:http";
import { WebSocketServer, WebSocket } from "ws";
import { encode, FrameDecoder, MsgType } from "./protocol.js";
import { RpcError } from "./errors.js";
import { STREAM_TYPES, type StreamHub, type StreamType, type Entry } from "./stream.js";

type Handler = (...args: any[]) => any | Promise<any>;
const HTTP_PREFIXES = ["GET ", "POST", "PUT ", "HEAD", "OPTI", "DELE", "PATC"];
const SUB_RE = new RegExp(`^(${STREAM_TYPES.join("|")})(Subscribe|Unsubscribe)$`);

export interface ServerOptions {
  /** bytes buffered per consumer above which stream notifications are dropped (client resumes via seq gap) */
  softLimit?: number;
  /** bytes buffered above which a slow consumer is disconnected immediately */
  hardLimit?: number;
  /** a consumer continuously above softLimit for this long is disconnected (it can reconnect + resume) */
  slowTimeoutMs?: number;
  maxFrame?: number;
  maxBodyBytes?: number;
  /** Helius-style `?api-key=` check for HTTP/WS/SSE; empty = open */
  apiKeys?: string[];
  wsPingMs?: number;
  log?: (...a: any[]) => void;
}

type JsonRpcReply = { jsonrpc: "2.0"; id: any; result?: any; error?: { code: number; message: string; data?: any } };
const rpcErr = (id: any, code: number, message: string, data?: any): JsonRpcReply =>
  ({ jsonrpc: "2.0", id, error: data === undefined ? { code, message } : { code, message, data } });

/** Per-consumer backpressure gate: send / drop (over soft) / kill (over hard, or over soft for too long). */
function gate(o: { softLimit: number; hardLimit: number; slowTimeoutMs: number }, stats: { dropped: number; slowDisconnects: number }) {
  let slowSince = 0;
  return (buffered: number): "send" | "drop" | "kill" => {
    if (buffered > o.hardLimit) { stats.slowDisconnects++; return "kill"; }
    if (buffered <= o.softLimit) { slowSince = 0; return "send"; }
    const now = Date.now();
    if (!slowSince) slowSince = now;
    else if (now - slowSince > o.slowTimeoutMs) { stats.slowDisconnects++; return "kill"; }
    stats.dropped++;
    return "drop";
  };
}

export class RpcServer {
  private handlers = new Map<string, Handler>();
  private tcp = net.createServer((sock) => this.onConn(sock));
  private http: http.Server;
  private wss: WebSocketServer;
  private conns = new Set<net.Socket>();
  private hub?: StreamHub;
  private nextWsSub = 1;
  private o: Required<Omit<ServerOptions, "log">> & { log: (...a: any[]) => void };
  stats = { dropped: 0, slowDisconnects: 0, badFrames: 0 };

  constructor(opts: ServerOptions = {}) {
    this.o = {
      softLimit: opts.softLimit ?? Number(process.env.STREAM_SOFT_LIMIT ?? 1 << 20),
      hardLimit: opts.hardLimit ?? Number(process.env.STREAM_HARD_LIMIT ?? 8 << 20),
      slowTimeoutMs: opts.slowTimeoutMs ?? Number(process.env.STREAM_SLOW_TIMEOUT_MS ?? 10_000),
      maxFrame: opts.maxFrame ?? Number(process.env.MAX_FRAME_BYTES ?? 16 << 20),
      maxBodyBytes: opts.maxBodyBytes ?? 5 << 20,
      apiKeys: opts.apiKeys ?? (process.env.API_KEYS ? process.env.API_KEYS.split(",").map((s) => s.trim()).filter(Boolean) : []),
      wsPingMs: opts.wsPingMs ?? 30_000,
      log: opts.log ?? (() => {}),
    };
    this.http = http.createServer({ maxHeaderSize: 16 << 10 }, (req, res) => this.onHttp(req, res).catch((e) => {
      this.o.log("[http] handler error", e?.message);
      if (!res.headersSent) res.writeHead(500);
      res.end();
    }));
    this.http.keepAliveTimeout = 65_000;
    this.wss = new WebSocketServer({ noServer: true, maxPayload: 1 << 20 });
    this.http.on("upgrade", (req, sock, head) => {
      if (!this.authorized(req.url)) { sock.end("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n"); return; }
      this.wss.handleUpgrade(req, sock as net.Socket, head, (ws) => this.onWs(ws));
    });
    this.http.on("clientError", (_e, sock) => { if (sock.writable) sock.end("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"); else sock.destroy(); });
  }

  register(name: string, fn: Handler) { this.handlers.set(name, fn); return this; }
  registerAll(api: Record<string, Handler>) { for (const [k, fn] of Object.entries(api)) this.register(k, fn); return this; }
  attachStreams(hub: StreamHub) { this.hub = hub; return this; }
  listen(port: number, host = "127.0.0.1"): Promise<number> {
    return new Promise((r, j) => { this.tcp.once("error", j); this.tcp.listen(port, host, () => r((this.tcp.address() as net.AddressInfo).port)); });
  }
  async close() {
    for (const c of this.wss.clients) c.terminate();
    for (const s of this.conns) s.destroy();
    await new Promise<void>((r) => this.tcp.close(() => r()));
  }

  private authorized(url = "/") {
    if (!this.o.apiKeys.length) return true;
    const k = new URL(url, "http://x").searchParams.get("api-key");
    return !!k && this.o.apiKeys.includes(k);
  }

  private async dispatch(method: string, params: any) {
    const fn = this.handlers.get(method);
    if (!fn) throw new RpcError(-32601, "Method not found");
    const args = params === undefined || params === null ? [] : Array.isArray(params) ? params : [params];
    return fn(...args);
  }

  // ---------- connection sniffing ----------
  private onConn(sock: net.Socket) {
    this.conns.add(sock);
    sock.on("close", () => this.conns.delete(sock));
    sock.on("error", () => sock.destroy());
    sock.setNoDelay(true);
    const chunks: Buffer[] = [];
    let n = 0;
    const sniff = (c: Buffer) => {
      chunks.push(c); n += c.length;
      if (n < 4) return; // wait for enough bytes to decide
      sock.off("data", sniff);
      sock.pause();
      const first = Buffer.concat(chunks);
      if (HTTP_PREFIXES.includes(first.subarray(0, 4).toString("latin1"))) {
        sock.unshift(first);
        this.http.emit("connection", sock);
        sock.resume();
      } else { this.binaryMode(sock, first); sock.resume(); }
    };
    sock.on("data", sniff);
  }

  // ---------- binary MessagePack protocol ----------
  private binaryMode(sock: net.Socket, first: Buffer) {
    const subs = new Map<number, () => void>(); // subId -> detach
    const bp = gate(this.o, this.stats);
    const send = (b: Buffer) => { if (!sock.destroyed && sock.writable) sock.write(b); };
    const errMsg = (e: any) => String(e?.message ?? e);
    const dec = new FrameDecoder((type, id, payload) => {
      switch (type) {
        case MsgType.Request: {
          if (!Array.isArray(payload) || typeof payload[0] !== "string") { send(encode(MsgType.Error, id, "invalid request: expected [method, args[]]")); return; }
          const [method, args] = payload;
          Promise.resolve().then(() => this.dispatch(method, Array.isArray(args) ? args : []))
            .then((r) => send(encode(MsgType.Response, id, r ?? null)), (e) => send(encode(MsgType.Error, id, errMsg(e))));
          return;
        }
        case MsgType.Ping: send(encode(MsgType.Pong, id, null)); return;
        case MsgType.Unsubscribe: { const d = subs.get(id); d?.(); subs.delete(id); send(encode(MsgType.Response, id, !!d)); return; }
        case MsgType.Subscribe: {
          if (!this.hub) { send(encode(MsgType.Error, id, "streaming not enabled")); return; }
          if (!payload || typeof payload !== "object") { send(encode(MsgType.Error, id, "invalid subscribe payload")); return; }
          subs.get(id)?.(); subs.delete(id); // re-subscribe (resume) replaces previous registration
          try {
            const sub = {
              deliver: (e: Entry) => {
                if (sock.destroyed) return;
                const g = bp(sock.writableLength);
                if (g === "kill") { sock.destroy(); return; }
                if (g === "drop") return;
                send(encode(MsgType.Notification, id, { seq: e.seq, slot: e.slot, data: e.data }));
              },
              onEnd: () => { subs.delete(id); },
            };
            const num = (x: any) => (typeof x === "number" && Number.isFinite(x) ? x : undefined);
            const r = this.hub.subscribe(payload.type, payload.params, sub, { fromSeq: num(payload.fromSeq), fromSlot: num(payload.fromSlot) });
            subs.set(id, r.detach);
            // ack first, then replay, then live (all in this tick => strictly ordered)
            send(encode(MsgType.Response, id, { ok: true, topic: r.key, headSeq: r.headSeq, replayed: r.replay.length, lost: r.lost, firstSeq: r.replay[0]?.seq ?? r.headSeq + 1 }));
            for (const e of r.replay) sub.deliver(e);
          } catch (e: any) { send(encode(MsgType.Error, id, errMsg(e))); }
          return;
        }
        default: send(encode(MsgType.Error, id, `unexpected frame type ${type}`));
      }
    }, {
      maxFrame: this.o.maxFrame,
      onError: (e) => { this.stats.badFrames++; this.o.log("[binary] closing:", e.message); sock.destroy(); },
    });
    sock.on("close", () => { for (const d of subs.values()) d(); subs.clear(); });
    sock.on("data", (c: Buffer) => dec.push(c));
    dec.push(first);
  }

  // ---------- JSON-RPC 2.0 core (HTTP + WS) ----------
  private async rpcOne(r: any): Promise<JsonRpcReply | undefined> {
    const isObj = r !== null && typeof r === "object" && !Array.isArray(r);
    const idOk = isObj && (typeof r.id === "string" || typeof r.id === "number" || r.id === null);
    const id = idOk ? r.id : null;
    if (!isObj || r.jsonrpc !== "2.0" || typeof r.method !== "string" || ("id" in r && !idOk) ||
        (r.params !== undefined && (r.params === null || typeof r.params !== "object")))
      return rpcErr(id, -32600, "Invalid request");
    const notification = !("id" in r);
    try {
      const result = await this.dispatch(r.method, r.params);
      return notification ? undefined : { jsonrpc: "2.0", id, result: result === undefined ? null : result };
    } catch (e: any) {
      if (notification) return undefined;
      return e instanceof RpcError ? rpcErr(id, e.code, e.message, e.data) : rpcErr(id, -32603, `Internal error: ${e?.message ?? e}`);
    }
  }

  /** Returns reply JSON (single/batch) or undefined when nothing should be sent (all notifications). */
  async handleJsonRpc(text: string, one: (r: any) => Promise<JsonRpcReply | undefined> = (r) => this.rpcOne(r)) {
    let req: any;
    try { req = JSON.parse(text); } catch { return rpcErr(null, -32700, "Parse error"); }
    if (Array.isArray(req)) {
      if (!req.length) return rpcErr(null, -32600, "Invalid request");
      const out = (await Promise.all(req.map(one))).filter(Boolean);
      return out.length ? out : undefined;
    }
    return one(req);
  }

  // ---------- HTTP ----------
  private async onHttp(req: http.IncomingMessage, res: http.ServerResponse) {
    const url = new URL(req.url ?? "/", "http://x");
    res.setHeader("Access-Control-Allow-Origin", "*");
    if (req.method === "OPTIONS") {
      res.writeHead(204, { "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "content-type, solana-client" });
      return void res.end();
    }
    if (req.method === "GET" && url.pathname === "/health") { res.writeHead(200, { "content-type": "text/plain" }); return void res.end("ok"); }
    if (!this.authorized(req.url)) return this.json(res, 401, rpcErr(null, -32401, "Unauthorized: invalid or missing api-key"));
    if (req.method === "GET" && url.pathname === "/stream") return this.sse(req, res, url);
    if (req.method !== "POST") return this.json(res, 405, rpcErr(null, -32600, "Use POST for JSON-RPC"));
    const chunks: Buffer[] = []; let size = 0;
    for await (const c of req) {
      size += c.length;
      if (size > this.o.maxBodyBytes) { res.setHeader("connection", "close"); return this.json(res, 413, rpcErr(null, -32600, "Request too large")); }
      chunks.push(c);
    }
    const out = await this.handleJsonRpc(Buffer.concat(chunks).toString("utf8"));
    if (out === undefined) { res.writeHead(204); return void res.end(); }
    return this.json(res, 200, out);
  }

  private json(res: http.ServerResponse, status: number, body: any) {
    const b = Buffer.from(JSON.stringify(body));
    res.writeHead(status, { "content-type": "application/json", "content-length": b.length });
    res.end(b);
  }

  /** GET /stream?type=slot|root|account|program|logs|signature&pubkey|programId|mentions|signature&fromSeq&fromSlot  (SSE) */
  private sse(req: http.IncomingMessage, res: http.ServerResponse, url: URL) {
    const q = Object.fromEntries(url.searchParams);
    if (!this.hub) return this.json(res, 400, { error: "streaming not enabled" });
    const type = (q.type ?? "slot") as StreamType;
    const params: any = {};
    for (const k of ["pubkey", "programId", "mentions", "signature", "commitment", "encoding"]) if (q[k]) params[k] = q[k];
    const lastId = req.headers["last-event-id"];
    const n = (x: any) => (x != null && x !== "" && Number.isFinite(Number(x)) ? Number(x) : undefined);
    const sock = res.socket!;
    let r: ReturnType<StreamHub["subscribe"]>;
    const bp = gate(this.o, this.stats);
    const deliver = (e: Entry) => {
      const g = bp(sock.writableLength);
      if (g === "kill") { res.destroy(); return; }
      if (g === "drop") return;
      res.write(`id: ${e.seq}\nevent: ${type}\ndata: ${JSON.stringify({ seq: e.seq, slot: e.slot, data: e.data })}\n\n`);
    };
    try {
      r = this.hub.subscribe(type, params, { deliver, onEnd: () => res.end() }, { fromSeq: n(lastId) ?? n(q.fromSeq), fromSlot: n(q.fromSlot) });
    } catch (e: any) { return this.json(res, 400, { error: String(e?.message ?? e) }); }
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
    res.write(`: subscribed ${r.key} headSeq=${r.headSeq} replay=${r.replay.length} lost=${r.lost}\n\n`);
    for (const e of r.replay) deliver(e);
    const hb = setInterval(() => res.write(": ping\n\n"), 15_000);
    res.on("close", () => { clearInterval(hb); r.detach(); });
  }

  // ---------- Solana-compatible JSON-RPC WebSocket (Helius/Solana pubsub) ----------
  private onWs(ws: WebSocket) {
    const subs = new Map<number, { type: StreamType; detach: () => void }>();
    let alive = true;
    const bp = gate(this.o, this.stats);
    const ping = setInterval(() => { if (!alive) { ws.terminate(); return; } alive = false; ws.ping(); }, this.o.wsPingMs);
    ws.on("pong", () => (alive = true));
    const send = (o: any) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(o)); };

    const one = async (r: any): Promise<JsonRpcReply | undefined> => {
      const m = typeof r?.method === "string" ? SUB_RE.exec(r.method) : null;
      if (!m || r.jsonrpc !== "2.0") return this.rpcOne(r);
      const id = typeof r.id === "string" || typeof r.id === "number" ? r.id : null;
      const type = m[1] as StreamType;
      if (m[2] === "Unsubscribe") {
        const sid = Array.isArray(r.params) ? r.params[0] : undefined;
        const s = subs.get(sid);
        if (!s || s.type !== type) return rpcErr(id, -32602, "Invalid subscription id.");
        s.detach(); subs.delete(sid);
        return { jsonrpc: "2.0", id, result: true };
      }
      if (!this.hub) return rpcErr(id, -32601, "Method not found");
      const sid = this.nextWsSub++;
      try {
        const res = this.hub.subscribeRaw(type, r.params ?? [], {
          deliver: (e: Entry) => {
            const g = bp(ws.bufferedAmount);
            if (g === "kill") { ws.terminate(); return; }
            if (g === "drop") return;
            send({ jsonrpc: "2.0", method: `${type}Notification`, params: { result: e.raw, subscription: sid } });
          },
          onEnd: () => subs.delete(sid), // signatureSubscribe auto-cancels after first notification (Solana behaviour)
        });
        subs.set(sid, { type, detach: res.detach });
        return { jsonrpc: "2.0", id, result: sid };
      } catch (e: any) {
        return e instanceof RpcError ? rpcErr(id, e.code, e.message) : rpcErr(id, -32603, String(e?.message ?? e));
      }
    };

    ws.on("message", (data) => {
      this.handleJsonRpc(data.toString(), one).then((out) => { if (out !== undefined) send(out); }, () => {});
    });
    ws.on("close", () => { clearInterval(ping); for (const s of subs.values()) s.detach(); subs.clear(); });
    ws.on("error", () => ws.terminate());
  }
}
