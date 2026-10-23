// Offline Alpenglow simulation: fast slots (50ms), fast finality, no vote transactions, getAgGenesisCert.
import { test, after } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { WebSocket } from "ws";
import { createFastRpc, ChainState, b58encode } from "../src/index.js";
import { startMockSolana, httpPost, waitFor, sleep } from "./helpers.js";

const CERT = { block: { slot: 459652999, blockId: Array(32).fill(1) }, signature: { signature: Array(192).fill(2), bitmap: [255] } };
const mocks: Awaited<ReturnType<typeof startMockSolana>>[] = [];
const apps: ReturnType<typeof createFastRpc>[] = [];
after(async () => { for (const a of apps) await a.close(); for (const m of mocks) await m.close(); });

async function setup(cert: any, chain: any = {}) {
  const mock = await startMockSolana(); mocks.push(mock);
  if (cert !== undefined) mock.responses.getAgGenesisCert = () => cert;
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl, lingerMs: 50 }, webhooks: false, chain: { slotMs: 400, ...chain } });
  apps.push(app);
  const port = await app.server.listen(0);
  await app.ready;
  return { mock, app, port };
}
/** Alpenglow-like producer: a slot every `ms`, finalized immediately (root = slot, no 32-slot lag). */
function produce(mock: any, from: number, n: number, ms: number) {
  let i = 0;
  return new Promise<void>((res) => { const t = setInterval(() => { const s = from + i++; mock.emit("slotSubscribe", { slot: s, parent: s - 1, root: s }); if (i >= n) { clearInterval(t); res(); } }, ms); });
}

test("ChainState: EWMA slot rate from observations, ignores outages, slots(n)", () => {
  const c = new ChainState({ slotMs: 400 });
  assert.equal(c.slots(1), 400);
  let t = 0; for (let s = 100; s < 140; s++) { c.observe(s, t); t += 150; }
  assert.ok(Math.abs(c.slotMs - 150) < 1, String(c.slotMs));
  c.observe(200, t + 600_000); // long gap (outage) -> ignored for rate
  assert.ok(Math.abs(c.slotMs - 150) < 1);
  c.observe(150, t + 600_100); // older slot -> ignored
  assert.equal(c.slots(4), 600); assert.equal(c.slots(0.01, 50), 50);
});

test("detect: getAgGenesisCert object => alpenglow; null => towerbft; -32601 => unknown (old node)", async () => {
  const a = await setup(CERT);
  assert.deepEqual(a.app.chain.consensus, { consensus: "alpenglow", genesisSlot: 459652999 });
  const st = await (await fetch(`http://127.0.0.1:${a.port}/v0/chain`)).json();
  assert.equal(st.consensus, "alpenglow"); assert.equal(st.defaultCommitment, "finalized");
  const r = await httpPost(a.port, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getAgGenesisCert" }));
  assert.equal(r.json.result.block.slot, 459652999);
  const t = await setup(null);
  assert.deepEqual(t.app.chain.consensus, { consensus: "towerbft" }); assert.equal(t.app.chain.defaultCommitment, "confirmed");
  const u = await setup(undefined);
  assert.equal(u.app.chain.consensus.consensus, "unknown"); assert.match((u.app.chain.consensus as any).reason, /pre-Agave 4\.3/);
  assert.equal(u.app.chain.defaultCommitment, "confirmed");
});

test("observed fast slots drive cache TTL (no hardcoded 400ms)", async () => {
  const { mock, app, port } = await setup(CERT, { trackSlots: true });
  await waitFor(() => mock.activeSubs("slotSubscribe") >= 1);
  await produce(mock, 1000, 30, 40);
  await waitFor(() => app.chain.samples >= 20);
  const st = await (await fetch(`http://127.0.0.1:${port}/v0/chain`)).json();
  assert.ok(st.slotMs >= 25 && st.slotMs <= 90, `observed slotMs ${st.slotMs}`);
  const n = () => mock.calls.filter((c) => c.method === "getSlot").length;
  const before = n();
  const call = () => httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getSlot" }));
  await call(); await call();
  assert.equal(n() - before, 1, "cached within one slot");
  await sleep(Math.max(60, app.chain.slots(1)) + 30);
  await call();
  assert.equal(n() - before, 2, "expired after ~1 observed slot (would still be cached with a fixed 400ms TTL)");
});

test("Alpenglow defaults: internal streams use finalized; transactionSubscribe fast finality, no vote scan", async () => {
  const { mock, port } = await setup(CERT, { slotMs: 50 });
  const A = b58encode(randomBytes(32));
  const sig = "ag" + b58encode(randomBytes(8));
  let visibleAt = Infinity;
  mock.responses.getTransaction = (p) => {
    assert.equal(p[1].commitment, "finalized");
    return Date.now() >= visibleAt ? { slot: 5000, version: 0, transaction: { signatures: [sig], message: { accountKeys: [A] } }, meta: { err: null, loadedAddresses: { writable: [], readonly: [] } } } : null;
  };
  const ws = new WebSocket(`ws://127.0.0.1:${port}`);
  const msgs: any[] = []; ws.on("message", (m) => msgs.push(JSON.parse(m.toString())));
  await new Promise((r) => ws.once("open", r));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "transactionSubscribe", params: [{ accountInclude: [A] }] })); // no commitment => default
  await waitFor(() => mock.activeSubs("logsSubscribe") >= 1);
  const up = mock.calls; // upstream WS params are recorded by mock subs; check via activeSubs + emitted path
  const t0 = Date.now(); visibleAt = t0 + 120; // finalized ~2-3 fast slots later
  // a non-vote tx whose logs mention the Vote program id is NOT dropped under Alpenglow (heuristic disabled)
  mock.emit("logsSubscribe", { context: { slot: 5000 }, value: { signature: sig, err: null, logs: ["Program Vote111111111111111111111111111111111111111 mentioned"] } });
  await waitFor(() => msgs.some((m) => m.method === "transactionNotification"), 3000);
  const dt = Date.now() - t0;
  assert.ok(dt < 1000, `delivered in ${dt}ms (retry spacing follows slot rate)`);
  assert.equal(msgs.find((m) => m.method === "transactionNotification").params.result.signature, sig);
  ws.close(); void up;
});

test("confirmedAsFinalized: confirmed+finalized subscribers share one finalized upstream topic", async () => {
  const { mock, port, app } = await setup(CERT, { confirmedAsFinalized: true });
  const ws = new WebSocket(`ws://127.0.0.1:${port}`);
  const msgs: any[] = []; ws.on("message", (m) => msgs.push(JSON.parse(m.toString())));
  await new Promise((r) => ws.once("open", r));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "accountSubscribe", params: ["SysvarC1ock11111111111111111111111111111111", { commitment: "confirmed", encoding: "base64" }] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "accountSubscribe", params: ["SysvarC1ock11111111111111111111111111111111", { commitment: "finalized", encoding: "base64" }] }));
  await waitFor(() => msgs.filter((m) => m.id).length === 2);
  await waitFor(() => mock.activeSubs("accountSubscribe") >= 1);
  await sleep(50);
  assert.equal(mock.activeSubs("accountSubscribe"), 1);
  assert.equal(app.hub.stats().filter((t) => t.key.startsWith("account:")).length, 1);
  assert.match(app.hub.stats().find((t) => t.key.startsWith("account:"))!.key, /"finalized"/);
  // HTTP reads are rewritten too
  await httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 3, method: "getBalance", params: ["SysvarC1ock11111111111111111111111111111111", { commitment: "confirmed" }] }));
  assert.equal(mock.calls.filter((c) => c.method === "getBalance").at(-1)!.params[1].commitment, "finalized");
  ws.close();
});

test("rewrite is never applied without detected Alpenglow (towerbft/unknown keep confirmed)", async () => {
  const { mock, port } = await setup(null, { confirmedAsFinalized: true });
  await httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 3, method: "getBalance", params: ["SysvarC1ock11111111111111111111111111111111", { commitment: "confirmed" }] }));
  assert.equal(mock.calls.filter((c) => c.method === "getBalance").at(-1)!.params[1].commitment, "confirmed");
});

test("getPriorityFeeEstimate lookbackSlots: window in slots, validated", async () => {
  const { port } = await setup(CERT);
  const q = async (o: any) => (await httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getPriorityFeeEstimate", params: [{ options: o }] }))).json;
  // mock fees: slots 100..109 = [0,10,...,80,1000]; last 3 slots => [70,80,1000]
  assert.deepEqual((await q({ includeAllPriorityFeeLevels: true, lookbackSlots: 3 })).result.priorityFeeLevels.min, 70);
  assert.equal((await q({ includeAllPriorityFeeLevels: true })).result.priorityFeeLevels.min, 0);
  assert.equal((await q({ lookbackSlots: 0 })).error.code, -32602);
  assert.equal((await q({ lookbackSlots: 151 })).error.code, -32602);
});
