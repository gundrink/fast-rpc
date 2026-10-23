import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { WebSocket } from "ws";
import { createFastRpc } from "../src/index.js";
import { startMockSolana, httpPost, waitFor } from "./helpers.js";

let mock: Awaited<ReturnType<typeof startMockSolana>>, app: ReturnType<typeof createFastRpc>, port: number;
before(async () => {
  mock = await startMockSolana();
  app = createFastRpc({ api: { url: mock.httpUrl, gpaMaxResults: 3 }, hub: { wsUrl: mock.wsUrl, lingerMs: 50 }, server: { apiKeys: ["k1"] } });
  port = await app.server.listen(0);
});
after(async () => { await app.close(); await mock.close(); });

const rpc = (body: any, key = "k1") => httpPost(port, typeof body === "string" ? body : JSON.stringify(body), `/?api-key=${key}`);

test("api-key: missing/invalid -> 401, valid -> 200", async () => {
  const bad = await httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getSlot" }), "/");
  assert.equal(bad.status, 401);
  assert.equal((await rpc({ jsonrpc: "2.0", id: 1, method: "getSlot" }, "nope")).status, 401);
  const ok = await rpc({ jsonrpc: "2.0", id: 1, method: "getSlot" });
  assert.equal(ok.status, 200); assert.deepEqual(ok.json, { jsonrpc: "2.0", id: 1, result: 1234 });
});

test("id echo: number, string, null", async () => {
  assert.equal((await rpc({ jsonrpc: "2.0", id: "abc", method: "getHealth" })).json.id, "abc");
  assert.equal((await rpc({ jsonrpc: "2.0", id: 0, method: "getHealth" })).json.id, 0);
  assert.equal((await rpc({ jsonrpc: "2.0", id: null, method: "getHealth" })).json.id, null);
});

test("-32700 parse error", async () => {
  const r = await rpc("{not json");
  assert.deepEqual(r.json, { jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } });
});

test("-32600 invalid request variants", async () => {
  for (const b of [{ id: 1, method: "getSlot" }, { jsonrpc: "1.0", id: 1, method: "getSlot" }, { jsonrpc: "2.0", id: 1 }, { jsonrpc: "2.0", id: {}, method: "getSlot" }, { jsonrpc: "2.0", id: 1, method: "getSlot", params: 5 }, 42, []]) {
    const r = await rpc(b);
    assert.equal(r.json.error.code, -32600, JSON.stringify(b));
  }
  assert.equal((await rpc({ id: 7, method: "getSlot" })).json.id, 7);
});

test("-32601 method not found (id echoed)", async () => {
  const r = await rpc({ jsonrpc: "2.0", id: 9, method: "requestAirdrop", params: ["x", 1] });
  assert.deepEqual(r.json, { jsonrpc: "2.0", id: 9, error: { code: -32601, message: "Method not found" } });
});

test("-32602 invalid params from validators", async () => {
  const r = await rpc({ jsonrpc: "2.0", id: 1, method: "getBalance", params: [] });
  assert.equal(r.json.error.code, -32602);
  const m = await rpc({ jsonrpc: "2.0", id: 1, method: "getMultipleAccounts", params: [Array(101).fill("x")] });
  assert.equal(m.json.error.code, -32602);
});

test("batch: mixed results/errors, order kept, notifications omitted", async () => {
  const r = await rpc([
    { jsonrpc: "2.0", id: 1, method: "getSlot" },
    { jsonrpc: "2.0", method: "getSlot" },              // notification -> no entry
    { jsonrpc: "2.0", id: 2, method: "nope" },
    "bad",
    { jsonrpc: "2.0", id: 3, method: "getBalance", params: ["Abc"] },
  ]);
  assert.equal(r.json.length, 4);
  assert.deepEqual(r.json.map((x: any) => x.id), [1, 2, null, 3]);
  assert.equal(r.json[0].result, 1234);
  assert.equal(r.json[1].error.code, -32601);
  assert.equal(r.json[2].error.code, -32600);
  assert.equal(r.json[3].result.value, 42);
});

test("all-notification request -> 204 no body", async () => {
  const r = await rpc({ jsonrpc: "2.0", method: "getSlot" });
  assert.equal(r.status, 204); assert.equal(r.text, "");
});

test("upstream error codes are passed through", async () => {
  mock.responses.getBlock = () => { throw new Error("unused"); };
  delete mock.responses.getBlock; // mock answers -32601 for unknown -> our proxy forwards code
  const r = await rpc({ jsonrpc: "2.0", id: 1, method: "getBlock", params: [5] });
  assert.equal(r.json.error.code, -32601);
});

test("getProgramAccounts result cap -> -32005", async () => {
  const r = await rpc({ jsonrpc: "2.0", id: 1, method: "getProgramAccounts", params: ["Prog"] });
  assert.equal(r.json.error.code, -32005);
});

test("getPriorityFeeEstimate (Helius shape)", async () => {
  const all = await rpc({ jsonrpc: "2.0", id: 1, method: "getPriorityFeeEstimate", params: [{ accountKeys: ["A"], options: { includeAllPriorityFeeLevels: true } }] });
  assert.deepEqual(Object.keys(all.json.result.priorityFeeLevels), ["min", "low", "medium", "high", "veryHigh", "unsafeMax"]);
  assert.equal(all.json.result.priorityFeeLevels.unsafeMax, 1000);
  const one = await rpc({ jsonrpc: "2.0", id: 1, method: "getPriorityFeeEstimate", params: [{ options: { priorityLevel: "High" } }] });
  assert.equal(typeof one.json.result.priorityFeeEstimate, "number");
  assert.deepEqual(mock.calls.filter((c) => c.method === "getRecentPrioritizationFees").at(0)?.params, [["A"]]);
});

test("GET /health, wrong verb 405, keep-alive pipelining", async () => {
  assert.equal(await (await fetch(`http://127.0.0.1:${port}/health`)).text(), "ok");
  assert.equal((await fetch(`http://127.0.0.1:${port}/?api-key=k1`)).status, 405);
  const rs = await Promise.all(Array.from({ length: 50 }, (_, i) => rpc({ jsonrpc: "2.0", id: i, method: "getSlot" })));
  assert.deepEqual(rs.map((r) => r.json.id), [...Array(50).keys()]);
});

test("Solana-compatible WS on same port: slot/account subscribe, notification shape, unsubscribe", async () => {
  assert.equal(await new Promise((r) => { const w = new WebSocket(`ws://127.0.0.1:${port}/`); w.on("error", () => r("rejected")); w.on("open", () => r("open")); }), "rejected");
  const ws = new WebSocket(`ws://127.0.0.1:${port}/?api-key=k1`);
  const msgs: any[] = [];
  ws.on("message", (m) => msgs.push(JSON.parse(m.toString())));
  await new Promise((r) => ws.on("open", r));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "slotSubscribe" }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "accountSubscribe", params: ["Acc1", { encoding: "base64", commitment: "confirmed" }] }));
  await waitFor(() => msgs.length >= 2 && mock.activeSubs("accountSubscribe") === 1 && mock.activeSubs("slotSubscribe") === 1);
  const slotSid = msgs.find((m) => m.id === 1).result, accSid = msgs.find((m) => m.id === 2).result;
  assert.equal(typeof slotSid, "number");
  mock.slot(500);
  const acc = { context: { slot: 500 }, value: { lamports: 1, owner: "11111111111111111111111111111111", data: ["", "base64"], executable: false, rentEpoch: 0, space: 0 } };
  mock.emit("accountSubscribe", acc);
  await waitFor(() => msgs.length >= 4);
  assert.deepEqual(msgs.find((m) => m.method === "slotNotification"), { jsonrpc: "2.0", method: "slotNotification", params: { result: { slot: 500, parent: 499, root: 468 }, subscription: slotSid } });
  assert.deepEqual(msgs.find((m) => m.method === "accountNotification"), { jsonrpc: "2.0", method: "accountNotification", params: { result: acc, subscription: accSid } });
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 3, method: "slotUnsubscribe", params: [slotSid] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 4, method: "slotUnsubscribe", params: [slotSid] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 5, method: "accountSubscribe", params: [] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 6, method: "getSlot" }));
  await waitFor(() => msgs.some((m) => m.id === 6) && msgs.some((m) => m.id === 5) && msgs.some((m) => m.id === 4));
  assert.equal(msgs.find((m) => m.id === 3).result, true);
  assert.equal(msgs.find((m) => m.id === 4).error.code, -32602);
  assert.equal(msgs.find((m) => m.id === 5).error.code, -32602);
  assert.equal(msgs.find((m) => m.id === 6).result, 1234);
  ws.close();
  await waitFor(() => mock.activeSubs() === 0, 3000, "upstream unsubscribed after linger");
});

test("WS logs/program/signature subscribe; signature auto-cancels after first notification", async () => {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/?api-key=k1`);
  const msgs: any[] = []; ws.on("message", (m) => msgs.push(JSON.parse(m.toString())));
  await new Promise((r) => ws.on("open", r));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "logsSubscribe", params: [{ mentions: ["Prog1"] }, { commitment: "confirmed" }] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "programSubscribe", params: ["Prog1", { encoding: "base64" }] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 3, method: "signatureSubscribe", params: ["Sig1", { commitment: "confirmed" }] }));
  await waitFor(() => mock.activeSubs() === 3);
  const log = { context: { slot: 9 }, value: { signature: "S", err: null, logs: ["Program Prog1 invoke [1]"] } };
  mock.emit("logsSubscribe", log); mock.emit("logsSubscribe", log); // duplicate -> deduped
  mock.emit("programSubscribe", { context: { slot: 9 }, value: { pubkey: "X", account: { lamports: 1 } } });
  mock.emit("signatureSubscribe", { context: { slot: 9 }, value: { err: null } });
  await waitFor(() => msgs.filter((m) => m.method).length >= 3);
  await waitFor(() => mock.activeSubs("signatureSubscribe") === 0, 3000, "signature topic torn down");
  assert.equal(msgs.filter((m) => m.method === "logsNotification").length, 1);
  assert.deepEqual(msgs.find((m) => m.method === "logsNotification").params.result, log);
  assert.ok(msgs.find((m) => m.method === "programNotification"));
  assert.deepEqual(msgs.find((m) => m.method === "signatureNotification").params.result, { context: { slot: 9 }, value: { err: null } });
  ws.close();
});

test("SSE /stream with api-key and Last-Event-ID resume", async () => {
  const ctl = new AbortController();
  const res = await fetch(`http://127.0.0.1:${port}/stream?type=slot&api-key=k1`, { signal: ctl.signal });
  assert.equal(res.headers.get("content-type"), "text/event-stream");
  await waitFor(() => mock.activeSubs("slotSubscribe") === 1);
  for (let s = 600; s < 603; s++) mock.slot(s);
  const reader = res.body!.getReader(); let txt = "";
  while (!txt.includes("id: 3")) txt += new TextDecoder().decode((await reader.read()).value);
  assert.match(txt, /event: slot\ndata: \{"seq":1,"slot":600/);
  ctl.abort();
  const res2 = await fetch(`http://127.0.0.1:${port}/stream?type=slot&api-key=k1`, { headers: { "last-event-id": "1" } });
  const r2 = res2.body!.getReader(); let t2 = "";
  while (!t2.includes("id: 3")) t2 += new TextDecoder().decode((await r2.read()).value);
  assert.ok(!t2.includes("id: 1\n") && t2.includes("id: 2\n"), "replayed from seq 2");
  await r2.cancel();
});
