import http from "node:http";
import net from "node:net";
import { WebSocketServer, WebSocket } from "ws";
import { encode, FrameDecoder, MsgType } from "../src/protocol.js";

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
export async function waitFor(cond: () => boolean, ms = 5000, what = "condition") {
  const end = Date.now() + ms;
  while (!cond()) { if (Date.now() > end) throw new Error(`timeout waiting for ${what}`); await sleep(10); }
}

/** Deterministic offline mock of a Solana RPC node (HTTP JSON-RPC + pubsub WS). */
export async function startMockSolana() {
  const subs = new Map<number, { ws: WebSocket; method: string; params: any[] }>();
  let nextSub = 1000;
  const calls: { method: string; params: any[] }[] = [];
  const st = { subscribeCalls: 0, connections: 0 };
  const responses: Record<string, (params: any[]) => any> = {
    getHealth: () => "ok",
    getSlot: () => 1234,
    getBalance: () => ({ context: { slot: 1234, apiVersion: "mock" }, value: 42 }),
    getRecentPrioritizationFees: () => [0, 10, 20, 30, 40, 50, 60, 70, 80, 1000].map((f, i) => ({ slot: 100 + i, prioritizationFee: f })),
    getProgramAccounts: () => Array.from({ length: 5 }, (_, i) => ({ pubkey: `P${i}`, account: {} })),
  };
  const server = http.createServer((req, res) => {
    let b = ""; req.on("data", (c) => (b += c));
    req.on("end", () => {
      const r = JSON.parse(b); calls.push({ method: r.method, params: r.params, path: req.url } as any);
      const f = responses[r.method];
      let out: any;
      try { out = f ? { jsonrpc: "2.0", id: r.id, result: f(r.params) } : { jsonrpc: "2.0", id: r.id, error: { code: -32601, message: "Method not found" } }; }
      catch (e: any) { out = { jsonrpc: "2.0", id: r.id, error: { code: -32002, message: String(e?.message ?? e) } }; }
      res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify(out));
    });
  });
  const wss = new WebSocketServer({ server });
  wss.on("connection", (ws) => {
    st.connections++;
    ws.on("message", (m) => {
      const r = JSON.parse(m.toString());
      if (r.method.endsWith("Unsubscribe")) { subs.delete(r.params[0]); ws.send(JSON.stringify({ jsonrpc: "2.0", id: r.id, result: true })); return; }
      if (r.method.endsWith("Subscribe")) {
        st.subscribeCalls++;
        const id = nextSub++; subs.set(id, { ws, method: r.method, params: r.params });
        ws.send(JSON.stringify({ jsonrpc: "2.0", id: r.id, result: id }));
      }
    });
    ws.on("close", () => { for (const [k, v] of subs) if (v.ws === ws) subs.delete(k); });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as net.AddressInfo).port;
  return {
    httpUrl: `http://127.0.0.1:${port}`, wsUrl: `ws://127.0.0.1:${port}`, responses, calls, st,
    activeSubs: (method?: string) => [...subs.values()].filter((s) => !method || s.method === method).length,
    emit(method: string, result: any) {
      for (const [id, s] of subs) if (s.method === method && s.ws.readyState === WebSocket.OPEN)
        s.ws.send(JSON.stringify({ jsonrpc: "2.0", method: method.replace("Subscribe", "Notification"), params: { result, subscription: id } }));
    },
    slot(n: number) { this.emit("slotSubscribe", { slot: n, parent: n - 1, root: n - 32 }); },
    dropAll() { for (const c of wss.clients) c.terminate(); },
    async close() { for (const c of wss.clients) c.terminate(); server.closeAllConnections(); await new Promise((r) => server.close(r)); },
  };
}

/** TCP proxy we can "unplug" to simulate a long network outage. */
export async function startProxy(targetPort: number) {
  const conns = new Set<net.Socket>();
  let server: net.Server | undefined;
  let port = 0;
  const up = () => new Promise<void>((r) => {
    server = net.createServer((c) => {
      const t = net.connect(targetPort, "127.0.0.1");
      conns.add(c); conns.add(t);
      c.pipe(t); t.pipe(c);
      const kill = () => { c.destroy(); t.destroy(); conns.delete(c); conns.delete(t); };
      c.on("error", kill); t.on("error", kill); c.on("close", kill); t.on("close", kill);
    });
    server.listen(port, "127.0.0.1", () => { port = (server!.address() as net.AddressInfo).port; r(); });
  });
  await up();
  return {
    get port() { return port; },
    async unplug() { for (const c of conns) c.destroy(); conns.clear(); await new Promise((r) => server!.close(r)); },
    plug: up,
  };
}

/** Raw binary-protocol socket for low-level tests. */
export async function rawClient(port: number) {
  const sock = net.connect(port, "127.0.0.1");
  await new Promise((r) => sock.once("connect", r));
  const frames: { type: number; id: number; payload: any }[] = [];
  const dec = new FrameDecoder((type, id, payload) => frames.push({ type, id, payload }), { onError: () => {} });
  sock.on("data", (d: Buffer) => dec.push(d));
  let closed = false; sock.on("close", () => (closed = true)); sock.on("error", () => {});
  return { sock, frames, get closed() { return closed; }, send: (t: MsgType, id: number, p: any) => sock.write(encode(t, id, p)) };
}

export async function httpPost(port: number, body: string, path = "/") {
  const res = await fetch(`http://127.0.0.1:${port}${path}`, { method: "POST", body, headers: { "content-type": "application/json" } });
  const text = await res.text();
  return { status: res.status, text, json: text ? JSON.parse(text) : undefined };
}
