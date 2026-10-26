// ProLaunch upgrade: funded-by, auth/IP limits (+Redis), raced upstreams, TPU leaders, retry sender, smart fees, metrics.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs"; import os from "node:os"; import path from "node:path"; import net from "node:net";
import { randomBytes } from "node:crypto";
import { createFastRpc, b58encode, createFundedBy, IpLimiter, createRacedUpstream, createTpuSender, createSender, smartFees, ChainState, RpcError } from "../src/index.js";
import { startMockSolana, httpPost, sleep } from "./helpers.js";

const pk = () => b58encode(randomBytes(32)); const sigOf = () => b58encode(randomBytes(64));
const SYS = "11111111111111111111111111111111";
const ixData = (t: number, lamports: number, extra = 0) => { const d = Buffer.alloc(12 + extra); d.writeUInt32LE(t, 0); d.writeBigUInt64LE(BigInt(lamports), 4); return b58encode(d); };
function fundTx(from: string, to: string, lamports: number, type = 2, slot = 10) {
  return { slot, blockTime: 1_700_000_000 + slot, transaction: { signatures: [sigOf()], message: { accountKeys: [from, to, SYS], instructions: [{ programIdIndex: 2, accounts: [0, 1], data: ixData(type, lamports, type === 0 ? 40 : 0) }] } }, meta: { err: null, innerInstructions: [], loadedAddresses: { writable: [], readonly: [] } } };
}
// chain: W <- A1 (transfer 1.5 SOL) <- A2 (createAccount 2 SOL) <- CEX (labeled)
const W = pk(), A1 = pk(), A2 = pk(), CEX = pk(), DEEP = pk();
const hist = new Map<string, { sig: string; tx: any }[]>(); // newest first
const txs = new Map<string, any>();
function add(addr: string, tx: any) { const sig = sigOf(); txs.set(sig, tx); (hist.get(addr) ?? hist.set(addr, []).get(addr)!).unshift({ sig, tx }); }
add(W, fundTx(A1, W, 1_500_000_000)); add(W, fundTx(W, pk(), 1000)); // later outgoing
add(A1, fundTx(A2, A1, 2_000_000_000, 0)); add(A2, fundTx(CEX, A2, 9e9));
// DEEP: 25 sigs, only the oldest is an incoming transfer; one incoming in the newest 10 too
for (let i = 0; i < 25; i++) add(DEEP, i === 0 ? fundTx(A2, DEEP, 5e8) : i === 20 ? fundTx(A1, DEEP, 7e8) : fundTx(DEEP, pk(), 1));

let mock: Awaited<ReturnType<typeof startMockSolana>>; let app: ReturnType<typeof createFastRpc>; let port: number; let labelsFile: string;
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "frpc-"));
before(async () => {
  mock = await startMockSolana();
  mock.responses.getSignaturesForAddress = (p) => {
    let l = (hist.get(p[0]) ?? []).map((h) => ({ signature: h.sig, slot: h.tx.slot, err: null, blockTime: h.tx.blockTime }));
    if (p[1]?.before) l = l.slice(l.findIndex((s) => s.signature === p[1].before) + 1);
    return l.slice(0, p[1]?.limit ?? 1000);
  };
  mock.responses.getTransaction = (p) => txs.get(p[0]) ?? null;
  labelsFile = path.join(tmp, "labels.json");
  fs.writeFileSync(labelsFile, JSON.stringify({ labels: [{ address: CEX, name: "Test Exchange", type: "cex", source: "test fixture" }] }));
  app = createFastRpc({ api: { url: mock.httpUrl, fundedBy: { labelsFile } }, hub: { wsUrl: mock.wsUrl }, webhooks: false, chain: { detect: false } });
  port = await app.server.listen(0);
});
after(async () => { await app.close(); await mock.close(); });
const call = (m: string, p: any[]) => (app.api as any)[m](...p);

test("funded-by: default 2 hops, Helius-like fields, transfer + createAccount", async () => {
  const r = await (await fetch(`http://127.0.0.1:${port}/v0/addresses/${W}/funded-by`)).json();
  assert.equal(r.funder, A1); assert.equal(r.amount, 1.5); assert.equal(r.symbol, "SOL"); assert.equal(r.decimals, 9);
  assert.equal(r.hops.length, 2); assert.equal(r.hops[1].funder, A2); assert.equal(r.complete, true); assert.equal(r.truncated, false);
  assert.ok(r.signature && r.explorerUrl.includes(r.signature));
  const v1 = await (await fetch(`http://127.0.0.1:${port}/v1/wallet/${W}/funded-by?hops=1`)).json();
  assert.equal(v1.hops.length, 1);
});
test("funded-by: hop 3 stops at labeled CEX; JSON-RPC getFundedBy; cached", async () => {
  const j = (await httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getFundedBy", params: [W, { hops: 3 }] }))).json.result;
  assert.equal(j.hops.length, 3); assert.equal(j.hops[2].funder, CEX); assert.equal(j.hops[2].funderType, "cex"); assert.equal(j.stoppedAt, "labeled:cex");
  const again = (await httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getFundedBy", params: [W, { hops: 3 }] }))).json.result;
  assert.equal(again.cached, true);
});
test("funded-by: validation (hops cap 3, bad address) -> 400 / -32602", async () => {
  assert.equal((await fetch(`http://127.0.0.1:${port}/v0/addresses/${W}/funded-by?hops=4`)).status, 400);
  assert.equal((await fetch(`http://127.0.0.1:${port}/v0/addresses/notbase58!/funded-by`)).status, 400);
  assert.equal((await httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getFundedBy", params: [W, { hops: 0 }] }))).json.error.code, -32602);
});
test("funded-by: shallow pagination -> truncated:true with earliest incoming seen", async () => {
  const fb = createFundedBy(call, { labelsFile, pageSize: 10, maxPages: 1 });
  const r = await fb.fundedBy(DEEP, { hops: 1 });
  assert.equal(r.complete, false); assert.equal(r.truncated, true); assert.equal(r.funder, A1, "earliest incoming within the 10 newest");
  const full = await createFundedBy(call, { labelsFile, pageSize: 10, maxPages: 3, maxTxScan: 30 }).fundedBy(DEEP, { hops: 1 });
  assert.equal(full.complete, true); assert.equal(full.funder, A2, "true first funder once oldest reached");
});
test("funded-by: upstream error on first hop surfaces; on later hop returns partial", async () => {
  const bad = createFundedBy(async () => { throw new RpcError(-32603, "upstream timeout"); }, { labelsFile });
  await assert.rejects(bad.fundedBy(W), /upstream timeout/);
  let n = 0;
  const flaky = createFundedBy(async (m, p) => { if (m === "getSignaturesForAddress" && n++ > 0) throw new RpcError(-32603, "429"); return call(m, p); }, { labelsFile });
  const r = await flaky.fundedBy(W, { hops: 2 }); assert.equal(r.hops.length, 1); assert.equal(r.stoppedAt, "upstreamError"); assert.equal(r.truncated, true);
});
test("funded-by: file cache persists", async () => {
  const cacheFile = path.join(tmp, "fb.json");
  await createFundedBy(call, { labelsFile, cacheFile }).fundedBy(W, { hops: 1 });
  const r = await createFundedBy(async () => { throw new Error("no upstream"); }, { labelsFile, cacheFile }).fundedBy(W, { hops: 1 });
  assert.equal(r.cached, true); assert.equal(r.funder, A1);
});

test("auth: API_KEYS_FILE per-project keys, x-api-key header, per-key rps", async () => {
  const f = path.join(tmp, "keys.json");
  fs.writeFileSync(f, JSON.stringify({ keys: [{ key: "test-key-a", project: "prolaunch", rps: 2, burst: 2 }, { key: "test-key-b" }] }));
  process.env.API_KEYS_FILE = f;
  const a = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl }, webhooks: false, chain: { detect: false } });
  delete process.env.API_KEYS_FILE;
  const p = await a.server.listen(0);
  try {
    const body = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getSlot" });
    assert.equal((await httpPost(p, body)).status, 401);
    const h = (k: string) => fetch(`http://127.0.0.1:${p}/`, { method: "POST", headers: { "content-type": "application/json", "x-api-key": k }, body });
    assert.equal((await h("test-key-b")).status, 200);
    assert.equal((await h("test-key-a")).status, 200); assert.equal((await h("test-key-a")).status, 200);
    assert.equal((await h("test-key-a")).status, 429, "per-key rps");
    assert.equal((await fetch(`http://127.0.0.1:${p}/health`)).status, 200, "health stays open");
  } finally { await a.close(); }
});
test("IP limiter: sliding window in memory; server returns 429", async () => {
  const l = new IpLimiter(2, 200, undefined);
  assert.ok(await l.allow("1.1.1.1")); assert.ok(await l.allow("1.1.1.1")); assert.equal(await l.allow("1.1.1.1"), false);
  assert.ok(await l.allow("2.2.2.2")); await sleep(220); assert.ok(await l.allow("1.1.1.1"));
  const a = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl }, server: { ipLimiter: new IpLimiter(1, 10_000, undefined) }, webhooks: false, chain: { detect: false } });
  const p = await a.server.listen(0);
  try { const b = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getSlot" }); assert.equal((await httpPost(p, b)).status, 200); assert.equal((await httpPost(p, b)).status, 429); }
  finally { await a.close(); }
});
test("IP limiter: Redis fixed window (mock RESP server), falls back to memory when down", async () => {
  const store = new Map<string, number>(); const seen: string[] = [];
  const srv = net.createServer((s) => { let buf = ""; s.on("data", (d) => { buf += d; const parts = buf.split("\r\n"); if (!buf.endsWith("\r\n")) return; buf = ""; const words = parts.filter((x, i) => i % 2 === 0 && x && !x.startsWith("*") && !x.startsWith("$")); for (let i = 0; i < words.length;) { const cmd = words[i]; seen.push(cmd); if (cmd === "INCR") { const k = words[i + 1]; store.set(k, (store.get(k) ?? 0) + 1); s.write(`:${store.get(k)}\r\n`); i += 2; } else { s.write(":1\r\n"); i += 3; } } }); });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const l = new IpLimiter(2, 60_000, `redis://127.0.0.1:${(srv.address() as net.AddressInfo).port}`);
  assert.ok(await l.allow("9.9.9.9")); assert.ok(await l.allow("9.9.9.9")); assert.equal(await l.allow("9.9.9.9"), false);
  assert.ok(seen.includes("INCR") && seen.includes("PEXPIRE")); l.close(); srv.close();
  const down = new IpLimiter(1, 60_000, "redis://127.0.0.1:1"); assert.ok(await down.allow("x")); assert.equal(await down.allow("x"), false); down.close();
});

test("race: hedges to a faster upstream, fails over on errors, keeps answers (-32602) final", async () => {
  const mk = (delay: number, fail = false, code = -32603) => async () => { await sleep(delay); if (fail) throw new RpcError(code, "x"); return delay; };
  const r = createRacedUpstream(["slow", "fast"], { hedgeMs: 20, make: (u) => (u === "slow" ? mk(300) : mk(5)) as any });
  assert.equal(await r("getSlot"), 5, "hedged request won");
  const f = createRacedUpstream(["bad", "good"], { hedgeMs: 1000, make: (u) => (u === "bad" ? mk(1, true) : mk(2)) as any });
  assert.equal(await f("getSlot"), 2); assert.equal(f.stats().find((s: any) => s.url === "bad").healthy, false);
  const a = createRacedUpstream(["x", "y"], { hedgeMs: 1000, make: () => mk(1, true, -32602) as any });
  await assert.rejects(a("getSlot"), (e: any) => e.code === -32602);
  // via env-free api option
  const m2 = await startMockSolana();
  const ap = createFastRpc({ api: { urls: ["http://127.0.0.1:1", m2.httpUrl], race: { hedgeMs: 10 } }, hub: { wsUrl: m2.wsUrl }, webhooks: false, chain: { detect: false } });
  try { assert.equal(await (ap.api as any).getSlot(), 1234); assert.equal((await (ap.api as any).getUpstreamStats()).length, 2); } finally { await ap.close(); await m2.close(); }
});

test("TPU: resolves next leaders' tpuQuic, dedupes, sends via mock transport only", async () => {
  const L1 = pk(), L2 = pk(), L3 = pk();
  const c = async (m: string) => m === "getSlot" ? 500 : m === "getSlotLeaders" ? [L1, L1, L1, L1, L2, L2, L2, L2, L3] : [{ pubkey: L1, tpuQuic: "1.2.3.4:8009" }, { pubkey: L2, tpuQuic: "5.6.7.8:8009" }, { pubkey: L3, tpuQuic: null }];
  const sent: string[] = [];
  const t = createTpuSender(c as any, { fanoutLeaders: 3, transport: { send: async (a) => { sent.push(a); } } });
  assert.deepEqual((await t.leaderAddrs()).map((x) => x.addr), ["1.2.3.4:8009", "5.6.7.8:8009"]);
  const r = await t.send(Buffer.from([1])); assert.equal(r.leaders.length, 2); assert.deepEqual(sent, ["1.2.3.4:8009", "5.6.7.8:8009"]);
  await assert.rejects(createTpuSender(c as any).send(Buffer.alloc(1)), /not configured/);
});

function signedTx() { // legacy tx: 1 sig, 2 keys, blockhash, 0 ix (bytes only, never sent anywhere real)
  const bh = randomBytes(32);
  return { b64: Buffer.concat([Buffer.from([1]), randomBytes(64), Buffer.from([1, 0, 1, 2]), randomBytes(64), bh, Buffer.from([0])]).toString("base64"), bh: b58encode(bh) };
}
test("sender: rebroadcasts on all paths until target commitment (Alpenglow default finalized)", async () => {
  const chain = new ChainState(); (chain as any).consensus = { consensus: "alpenglow", genesisSlot: 1 };
  let polls = 0; const sends: string[] = []; const { b64, bh } = signedTx(); let bhSeen = "";
  const read = async (m: string, p: any[]) => {
    if (m === "getSignatureStatuses") { polls++; return { value: [polls < 3 ? null : { slot: 9, confirmationStatus: polls < 4 ? "confirmed" : "finalized", err: null }] }; }
    if (m === "isBlockhashValid") { bhSeen = p[0]; return { value: true }; }
  };
  const s = createSender({ read, senders: [async () => { sends.push("rpc1"); }, async () => { sends.push("rpc2"); throw new Error("down"); }], chain,
    jitoSend: async () => { sends.push("jito"); }, jitoCheck: async () => {}, tpu: { enabled: true, send: async () => { sends.push("tpu"); } } });
  const r = await s.sendWithRetry(b64, { jito: true, rebroadcastMs: 5 });
  assert.equal(r.confirmationStatus, "finalized"); assert.equal(r.rounds, 4); assert.equal(bhSeen, bh);
  assert.equal(r.paths.rpc, 4); assert.equal(r.paths.jito, 4); assert.equal(r.paths.tpu, 4);
});
test("sender: stops on blockhash expiry, on tx error, and surfaces preflight failure", async () => {
  const chain = new ChainState(); const { b64 } = signedTx();
  const base = (o: any) => createSender({ read: async (m) => (m === "getSignatureStatuses" ? { value: [o.st ?? null] } : { value: o.valid ?? true }), senders: [o.send ?? (async () => {})], chain });
  await assert.rejects(base({ valid: false }).sendWithRetry(b64, { rebroadcastMs: 1 }), (e: any) => e.code === -32003 && /expired/.test(e.message));
  const r = await base({ st: { slot: 1, confirmationStatus: "processed", err: { InstructionError: [0, "Custom"] } } }).sendWithRetry(b64);
  assert.deepEqual(r.err, { InstructionError: [0, "Custom"] });
  await assert.rejects(base({ send: async () => { throw new RpcError(-32002, "Transaction simulation failed"); } }).sendWithRetry(b64), (e: any) => e.code === -32002);
  await assert.rejects(base({}).sendWithRetry("@@notatx"), (e: any) => e.code === -32602);
});
test("sender via JSON-RPC sendTransactionWithRetry hits only the mock upstream", async () => {
  const { b64 } = signedTx(); let sent = 0;
  mock.responses.sendTransaction = () => { sent++; return "sig"; };
  mock.responses.getSignatureStatuses = () => ({ context: { slot: 1 }, value: [{ slot: 1, confirmationStatus: "finalized", err: null }] });
  mock.responses.isBlockhashValid = () => ({ context: { slot: 1 }, value: true });
  const j = (await httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "sendTransactionWithRetry", params: [b64, { tpu: false }] }))).json;
  assert.equal(j.result.confirmationStatus, "finalized"); assert.equal(sent, 1);
});
test("smart fees: priority levels + Jito tip floor (mock fetch) with fallback", async () => {
  const est = async () => ({ priorityFeeEstimate: 5, priorityFeeLevels: { min: 0, low: 1, medium: 5, high: 50, veryHigh: 90, unsafeMax: 100 } });
  const ok = await smartFees(est, { priorityLevel: "veryHigh" }, "mock", (async () => ({ json: async () => [{ landed_tips_50th_percentile: 0.00002 }] })) as any);
  assert.equal(ok.computeUnitPrice, 90); assert.equal(ok.jitoTip.lamports, 20_000);
  const fb = await smartFees(est, {}, "mock", (async () => { throw new Error("offline"); }) as any);
  assert.equal(fb.computeUnitPrice, 50); assert.match(fb.jitoTip.source, /fallback/);
});
test("health/metrics endpoints", async () => {
  const h = await (await fetch(`http://127.0.0.1:${port}/healthz`)).json(); assert.equal(h.ok, true);
  const m = await (await fetch(`http://127.0.0.1:${port}/metrics`)).text(); assert.match(m, /fastrpc_http \d+/);
});
