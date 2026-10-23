import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { RpcServer, RpcClient, MsgType, RpcError } from "../src/index.js";
import { rawClient, sleep, waitFor } from "./helpers.js";

let server: RpcServer, port: number, client: RpcClient;
before(async () => {
  server = new RpcServer({ maxFrame: 64 * 1024 })
    .register("add", (a: number, b: number) => a + b)
    .register("echo", (x: any) => x)
    .register("boom", () => { throw new RpcError(-32010, "kaboom"); })
    .register("sleep", async (ms: number, v: any) => { await sleep(ms); return v; })
    .register("nothing", () => undefined);
  port = await server.listen(0);
  client = await RpcClient.connect(port, "127.0.0.1", { reconnect: false });
});
after(async () => { client.close(); await server.close(); });

test("call returns result; proxy works", async () => {
  assert.equal(await client.call("add", 2, 3), 5);
  const r = client.proxy<{ echo: (x: any) => any }>();
  assert.deepEqual(await r.echo({ a: [1, "b", null], u: new Uint8Array([1, 2]) }), { a: [1, "b", null], u: Buffer.from([1, 2]) });
  assert.equal(await client.call("nothing"), null);
});

test("errors: unknown method, thrown error", async () => {
  await assert.rejects(client.call("nope"), /Method not found/);
  await assert.rejects(client.call("boom"), /kaboom/);
});

test("multiplexing: out-of-order completion matched by id", async () => {
  const t = Date.now();
  const r = await Promise.all([client.call("sleep", 120, "a"), client.call("sleep", 10, "b"), client.call("sleep", 60, "c")]);
  assert.deepEqual(r, ["a", "b", "c"]);
  assert.ok(Date.now() - t < 300, "ran concurrently");
});

test("10k concurrent calls", async () => {
  const r = await Promise.all(Array.from({ length: 10_000 }, (_, i) => client.call("add", i, 1)));
  assert.equal(r[9999], 10_000);
});

test("malformed request payload -> error frame, connection stays usable", async () => {
  const c = await rawClient(port);
  c.send(MsgType.Request, 5, { not: "an array" });
  c.send(MsgType.Request, 6, ["add", [1, 1]]);
  await waitFor(() => c.frames.length === 2);
  assert.equal(c.frames.find((f) => f.id === 5)!.type, MsgType.Error);
  assert.equal(c.frames.find((f) => f.id === 6)!.payload, 2);
  c.sock.destroy();
});

test("garbage / oversized frame closes only that connection; server survives", async () => {
  const c = await rawClient(port);
  const b = Buffer.alloc(9); b.writeUInt32BE(1 << 30, 0); c.sock.write(b);
  await waitFor(() => c.closed);
  assert.ok(server.stats.badFrames >= 1);
  assert.equal(await client.call("add", 1, 1), 2);
});

test("tiny first chunk (<4 bytes) is sniffed correctly", async () => {
  const c = await rawClient(port);
  const { encode } = await import("../src/protocol.js");
  const f = encode(MsgType.Request, 9, ["add", [4, 5]]);
  c.sock.write(f.subarray(0, 2)); await sleep(20); c.sock.write(f.subarray(2));
  await waitFor(() => c.frames.length === 1);
  assert.equal(c.frames[0].payload, 9);
  c.sock.destroy();
});

test("pending calls reject when connection drops", async () => {
  const c = await RpcClient.connect(port, "127.0.0.1", { reconnect: false });
  const p = c.call("sleep", 500, 1);
  await sleep(20); c.simulateDrop();
  await assert.rejects(p, /connection closed/);
  c.close();
});
