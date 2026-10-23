import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { createFastRpc, RpcClient, RpcServer, MsgType, encode, FrameDecoder, SolanaWs } from "../src/index.js";
import { startMockSolana, startProxy, rawClient, sleep, waitFor } from "./helpers.js";

let mock: Awaited<ReturnType<typeof startMockSolana>>;
before(async () => { mock = await startMockSolana(); });
after(async () => { await mock.close(); });

test("client auto-reconnect after a long (3s) outage resumes from last seq: no gaps, no dups", async () => {
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl, lingerMs: 10_000 } });
  const port = await app.server.listen(0);
  const proxy = await startProxy(port);
  const c = await RpcClient.connect(proxy.port, "127.0.0.1", { minBackoffMs: 100, maxBackoffMs: 400, pingMs: 200 });
  const seqs: number[] = [], slots: number[] = [];
  await c.subscribe("slot", {}, (m) => { seqs.push(m.seq); slots.push(m.data.slot); });
  await waitFor(() => mock.activeSubs("slotSubscribe") === 1);
  let s = 1000;
  for (; s < 1005; s++) mock.slot(s);
  await waitFor(() => seqs.length === 5);

  await proxy.unplug();                        // network down
  const ticker = setInterval(() => mock.slot(s++), 50); // ~60 slots produced during outage
  await sleep(3000);
  assert.equal(c.connected, false);
  await proxy.plug();                          // network back
  await waitFor(() => c.connected, 3000, "reconnect");
  await sleep(300);
  clearInterval(ticker);
  const last = s - 1;
  await waitFor(() => slots.at(-1) === last, 3000, "caught up");
  assert.ok(c.reconnects >= 1);
  assert.ok(seqs.length > 50, `got ${seqs.length}`);
  assert.deepEqual(seqs, seqs.map((_, i) => i + 1), "contiguous seq 1..N");
  assert.deepEqual(slots, slots.map((_, i) => 1000 + i), "every slot exactly once");
  c.close(); await proxy.unplug(); await app.close();
});

test("ring overflow during outage reports lost count via onGap and continues", async () => {
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl, ringSize: 5, lingerMs: 10_000 } });
  const port = await app.server.listen(0);
  const proxy = await startProxy(port);
  const c = await RpcClient.connect(proxy.port, "127.0.0.1", { minBackoffMs: 50, maxBackoffMs: 100 });
  const seqs: number[] = []; let lost = 0;
  await c.subscribe("slot", {}, (m) => seqs.push(m.seq), { onGap: (n) => (lost += n) });
  await waitFor(() => mock.activeSubs("slotSubscribe") === 1);
  mock.slot(1); await waitFor(() => seqs.length === 1);
  // a second subscriber keeps the topic alive while c is offline
  const keep = await RpcClient.connect(port); await keep.subscribe("slot", {}, () => {});
  await proxy.unplug();
  for (let i = 2; i <= 20; i++) mock.slot(i);
  await sleep(100); await proxy.plug();
  await waitFor(() => seqs.at(-1) === 20, 3000);
  assert.equal(lost, 14);                     // seq 2..15 fell out of a 5-entry ring
  assert.deepEqual(seqs, [1, 16, 17, 18, 19, 20]);
  c.close(); keep.close(); await proxy.unplug(); await app.close();
});

test("backpressure: slow consumer gets drops, then is disconnected at hard limit; fast consumer unaffected", async () => {
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl, lingerMs: 50 }, server: { softLimit: 256 * 1024, hardLimit: 64 * 1024 * 1024, slowTimeoutMs: 300 } });
  const port = await app.server.listen(0);
  const slow = await rawClient(port);
  slow.send(MsgType.Subscribe, 1, { type: "account", params: { pubkey: "Big" } });
  await waitFor(() => slow.frames.length === 1 && mock.activeSubs("accountSubscribe") === 1);
  slow.sock.pause();                                  // stop reading -> kernel buffers fill -> writableLength grows
  const fast = await RpcClient.connect(port);
  let fastGot = 0; await fast.subscribe("account", { pubkey: "Big" }, () => fastGot++);
  const blob = Buffer.alloc(100 * 1024, 7).toString("base64");
  for (let i = 1; i <= 400; i++) {
    mock.emit("accountSubscribe", { context: { slot: i }, value: { lamports: i, data: [blob, "base64"] } });
    if (i % 10 === 0) await sleep(20);
    if (app.server.stats.slowDisconnects) break;
  }
  await waitFor(() => app.server.stats.slowDisconnects === 1, 10_000, "slow consumer disconnected");
  assert.ok(app.server.stats.dropped > 0, "some notifications dropped");
  slow.sock.resume();                                 // drain -> observe the server-side close
  await waitFor(() => slow.closed, 10_000, "socket closed");
  await waitFor(() => fastGot > 0);
  fast.close(); await app.close();
});

test("client resumes on seq gap (server-side drop) by re-subscribing with fromSeq", async () => {
  // fake server: sends seq 1,2 then 5; expects re-Subscribe with fromSeq=2, then replays 3,4,5
  const subs: any[] = [];
  const srv = net.createServer((s) => {
    const dec = new FrameDecoder((t, id, p) => {
      if (t === MsgType.Ping) return;
      if (t !== MsgType.Subscribe) return;
      subs.push(p);
      s.write(encode(MsgType.Response, id, { ok: true, lost: 0, firstSeq: (p.fromSeq ?? 0) + 1 }));
      if (subs.length === 1) for (const q of [1, 2, 5]) s.write(encode(MsgType.Notification, id, { seq: q, slot: q, data: q }));
      else for (const q of [3, 4, 5, 6]) s.write(encode(MsgType.Notification, id, { seq: q, slot: q, data: q }));
    });
    s.on("data", (d: Buffer) => dec.push(d)); s.on("error", () => {});
  });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const c = await RpcClient.connect((srv.address() as net.AddressInfo).port);
  const got: number[] = [];
  await c.subscribe("slot", {}, (m) => got.push(m.seq));
  await waitFor(() => got.length === 6);
  assert.deepEqual(got, [1, 2, 3, 4, 5, 6]);
  assert.equal(subs[1].fromSeq, 2);
  c.close(); srv.close();
});

test("upstream WS forced drop: reconnects, resubscribes all topics, dedups replayed slot", async () => {
  const logs: string[] = [];
  const ws = new SolanaWs(mock.wsUrl, (...a) => logs.push(a.join(" ")), { reqTimeoutMs: 2000, pingMs: 60_000 });
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { upstream: ws, lingerMs: 50 } });
  const port = await app.server.listen(0);
  const c = await RpcClient.connect(port);
  const slots: number[] = [], seqs: number[] = [], accs: number[] = [];
  await c.subscribe("slot", {}, (m) => { slots.push(m.data.slot); seqs.push(m.seq); });
  await c.subscribe("account", { pubkey: "A" }, (m) => accs.push(m.slot));
  await waitFor(() => mock.activeSubs() === 2);
  const before = mock.st.subscribeCalls;
  mock.slot(1); mock.slot(2);
  await waitFor(() => slots.length === 2);
  mock.dropAll();                                       // kill upstream socket
  await waitFor(() => ws.connects === 2 && mock.activeSubs() === 2, 5000, "resubscribed");
  assert.equal(mock.st.subscribeCalls - before, 2);
  mock.slot(2); mock.slot(3);                           // slot 2 re-sent after reconnect -> deduped
  mock.emit("accountSubscribe", { context: { slot: 3 }, value: { lamports: 1 } });
  await waitFor(() => slots.length === 3 && accs.length === 1);
  assert.deepEqual(slots, [1, 2, 3]); assert.deepEqual(seqs, [1, 2, 3]);
  assert.ok(logs.some((l) => l.includes("reconnecting")));
  c.close(); await app.close();
});

test("keepalive: client detects dead server (no pongs) and reconnects", async () => {
  // server that accepts but never answers pings
  let conns = 0;
  const srv = net.createServer((s) => { conns++; s.on("error", () => {}); });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const c = await RpcClient.connect((srv.address() as net.AddressInfo).port, "127.0.0.1", { pingMs: 100, minBackoffMs: 50 });
  await waitFor(() => conns >= 2, 3000, "reconnect after keepalive timeout");
  c.close(); srv.close();
});

test("binary Unsubscribe stops delivery and upstream sub is released after linger", async () => {
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl, lingerMs: 50 } });
  const port = await app.server.listen(0);
  const c = await RpcClient.connect(port);
  let n = 0;
  const unsub = await c.subscribe("logs", { mentions: "P" }, () => n++);
  await waitFor(() => mock.activeSubs("logsSubscribe") === 1);
  mock.emit("logsSubscribe", { context: { slot: 1 }, value: { signature: "a", err: null, logs: [] } });
  await waitFor(() => n === 1);
  await unsub();
  mock.emit("logsSubscribe", { context: { slot: 2 }, value: { signature: "b", err: null, logs: [] } });
  await waitFor(() => mock.activeSubs("logsSubscribe") === 0, 3000);
  assert.equal(n, 1);
  await assert.rejects(c.subscribe("account", {}, () => {}), /accountSubscribe/);
  c.close(); await app.close();
});
void RpcServer;
