import net from "node:net";
import { encode, FrameDecoder, MsgType } from "./protocol.js";
type Handler = (...args: any[]) => any | Promise<any>;
const HTTP_METHODS = ["POST", "GET ", "OPTI", "HEAD"];

export class RpcServer {
  private handlers = new Map<string, Handler>();
  private server = net.createServer((sock) => this.onConn(sock));
  register(name: string, fn: Handler) { this.handlers.set(name, fn); return this; }
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
    const dec = new FrameDecoder(async (type, id, [method, args]) => {
      if (type !== MsgType.Request) return;
      try { sock.write(encode(MsgType.Response, id, await this.dispatch(method, args))); }
      catch (e: any) { sock.write(encode(MsgType.Error, id, String(e?.message ?? e))); }
    });
    dec.push(first);
    sock.on("data", (c: Buffer) => dec.push(c));
  }

  /** Minimal HTTP/1.1 JSON-RPC 2.0 (single or batch). */
  private httpMode(sock: net.Socket, first: Buffer) {
    let buf = first;
    const tryParse = async () => {
      while (true) {
        const end = buf.indexOf("\r\n\r\n");
        if (end < 0) return;
        const headers = buf.subarray(0, end).toString("latin1");
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
    sock.on("data", (c: Buffer) => { buf = Buffer.concat([buf, c]); tryParse(); });
    tryParse();
  }
}
