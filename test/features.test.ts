import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import net from "node:net";
import { randomBytes } from "node:crypto";
import { WebSocket } from "ws";
import {
  createFastRpc, b58encode, b58decode, findProgramAddress, decodeTransaction, buildLegacyTx, isPubkey, metadataPda, decodeMetadata,
  RpcClient, MsgType, StreamHub, Limiter, WebhookManager, matchesFilter, VOTE_PROGRAM,
} from "../src/index.js";
import { startMockSolana, httpPost, rawClient, waitFor, sleep } from "./helpers.js";

const pk = () => b58encode(randomBytes(32));
const A = pk(), B = pk(), C = pk(), OWNER = pk(), MINT = pk(), NFT = pk(), UA = pk();
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), "fastrpc-"));

let mock: Awaited<ReturnType<typeof startMockSolana>>, mock2: Awaited<ReturnType<typeof startMockSolana>>, dasMock: Awaited<ReturnType<typeof startMockSolana>>;
let app: ReturnType<typeof createFastRpc>, port: number, whFile: string;
const txdb = new Map<string, any>();

function encodeMetadata(m: { ua: string; mint: string; name: string; symbol: string; uri: string; fee: number; creators: string[]; ts?: number; coll?: string }) {
  const s = (x: string) => { const b = Buffer.from(x); const l = Buffer.alloc(4); l.writeUInt32LE(b.length); return Buffer.concat([l, b]); };
  const fee = Buffer.alloc(2); fee.writeUInt16LE(m.fee);
  const cr = m.creators.length ? Buffer.concat([Buffer.from([1]), (() => { const l = Buffer.alloc(4); l.writeUInt32LE(m.creators.length); return l; })(),
    ...m.creators.map((c) => Buffer.concat([b58decode(c), Buffer.from([1, 100 / m.creators.length])]))]) : Buffer.from([0]);
  return Buffer.concat([Buffer.from([4]), b58decode(m.ua), b58decode(m.mint), s(m.name), s(m.symbol), s(m.uri.padEnd(40, "\0")), fee, cr,
    Buffer.from([1, 1]), Buffer.from([1, 255]), m.ts != null ? Buffer.from([1, m.ts]) : Buffer.from([0]),
    m.coll ? Buffer.concat([Buffer.from([1, 1]), b58decode(m.coll)]) : Buffer.from([0])]);
}
const mintAcc = (supply: string, decimals: number) => ({ owner: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", data: { parsed: { type: "mint", info: { supply, decimals, mintAuthority: null, freezeAuthority: null, isInitialized: true } } } });
const COLL = pk();
const META: Record<string, string> = {
  [metadataPda(MINT)]: encodeMetadata({ ua: UA, mint: MINT, name: "Test Token", symbol: "TT", uri: "https://x/tt.json", fee: 0, creators: [], ts: 2 }).toString("base64"),
  [metadataPda(NFT)]: encodeMetadata({ ua: UA, mint: NFT, name: "Cool NFT #1", symbol: "COOL", uri: "https://x/1.json", fee: 500, creators: [UA], ts: 0, coll: COLL }).toString("base64"),
};

before(async () => {
  [mock, mock2, dasMock] = await Promise.all([startMockSolana(), startMockSolana(), startMockSolana()]);
  mock.responses.sendTransaction = (p) => decodeTransaction(p[0], p[1]?.encoding).signatures[0];
  mock2.responses.sendTransaction = (p) => decodeTransaction(p[0], p[1]?.encoding).signatures[0];
  mock.responses.simulateTransaction = (p) => ({ context: { slot: 1 }, value: { err: null, logs: ["Program log: mock"], unitsConsumed: 150, accounts: null } });
  mock.responses.getTransaction = (p) => txdb.get(p[0]) ?? null;
  mock.responses.getMultipleAccounts = (p) => ({ context: { slot: 1 }, value: p[0].map((k: string) =>
    k === MINT ? mintAcc("1000000000", 6) : k === NFT ? mintAcc("1", 0) : META[k] ? { owner: "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s", data: [META[k], "base64"] } : null) });
  mock.responses.getTokenAccountsByOwner = (p) => ({ context: { slot: 1 }, value: p[1].programId.startsWith("Tokenkeg") ? [
    { pubkey: pk(), account: { data: { parsed: { info: { mint: MINT, owner: p[0], state: "initialized", tokenAmount: { amount: "5000000" } } } } } },
    { pubkey: pk(), account: { data: { parsed: { info: { mint: NFT, owner: p[0], state: "frozen", tokenAmount: { amount: "1" } } } } } },
    { pubkey: pk(), account: { data: { parsed: { info: { mint: pk(), owner: p[0], state: "initialized", tokenAmount: { amount: "0" } } } } } },
  ] : [] });
  dasMock.responses.getAssetProof = (p) => ({ root: "R", proof: ["a"], node_index: 1, leaf: "L", tree_id: (p as any).id });
  whFile = path.join(tmp(), "webhooks.json");
  app = createFastRpc({ api: { url: mock.httpUrl, sendUrls: [mock2.httpUrl] }, hub: { wsUrl: mock.wsUrl, lingerMs: 50 }, webhooks: { file: whFile, baseDelayMs: 20, maxRetries: 3 } });
  port = await app.server.listen(0);
});
after(async () => { await app.close(); await Promise.all([mock.close(), mock2.close(), dasMock.close()]); });

let rid = 1;
const rpc = async (method: string, params?: any, p = port, q = "") => (await httpPost(p, JSON.stringify({ jsonrpc: "2.0", id: rid++, method, params }), "/" + q)).json;
const signedTx = (keys = [pk(), A, "11111111111111111111111111111111"]) => buildLegacyTx(keys, 1, [{ program: 2, accounts: [0, 1], data: Buffer.from([2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]) }]);

// ---------- codec ----------
test("codec: base58 round-trip incl. leading zeros, PDA, tx decode (base64/base58)", () => {
  for (let i = 0; i < 300; i++) { const b = randomBytes(i % 70); if (b.length > 3 && i % 5 === 0) b.fill(0, 0, 3); assert.ok(b58decode(b58encode(b)).equals(b)); }
  assert.equal(b58encode(Buffer.alloc(32)), "11111111111111111111111111111111");
  assert.equal(b58decode("11111111111111111111111111111111").length, 32);
  // known: wSOL metadata PDA
  assert.deepEqual(findProgramAddress([Buffer.from("metadata"), b58decode("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"), b58decode("So11111111111111111111111111111111111111112")], "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"),
    ["6dM4TqWyWJsbx7obrdLcviBkTafD5E8av61zfU6jq57X", 255]);
  const keys = [pk(), A, "11111111111111111111111111111111"];
  const raw = signedTx(keys);
  for (const [s, enc] of [[raw.toString("base64"), "base64"], [b58encode(raw), "base58"]] as const) {
    const d = decodeTransaction(s, enc);
    assert.equal(d.version, "legacy"); assert.deepEqual(d.accountKeys, keys); assert.equal(d.numRequiredSignatures, 1); assert.equal(d.signatures.length, 1);
  }
  assert.throws(() => decodeTransaction("AAAA", "base64"));
  assert.ok(isPubkey(A)); assert.ok(!isPubkey("abc")); assert.ok(!isPubkey(42));
});

// ---------- send relay ----------
test("sendTransaction: relays user-signed tx, fan-out first success, passes skipPreflight, validates", async () => {
  const tx = signedTx().toString("base64");
  const sig = decodeTransaction(tx, "base64").signatures[0];
  const r = await rpc("sendTransaction", [tx, { encoding: "base64", skipPreflight: true, maxRetries: 0 }]);
  assert.equal(r.result, sig);
  const c1 = mock.calls.filter((c) => c.method === "sendTransaction").at(-1)!, c2 = mock2.calls.filter((c) => c.method === "sendTransaction").at(-1)!;
  assert.deepEqual(c1.params[1], { encoding: "base64", skipPreflight: true, maxRetries: 0 });
  assert.equal(c2.params[0], tx, "fanned out to SEND upstream #2");
  // primary fails -> secondary wins
  mock.responses.sendTransaction = () => { throw new Error("x"); };
  const r2 = await rpc("sendTransaction", [tx, { encoding: "base64" }]);
  assert.equal(r2.result, sig);
  // invalid inputs never reach upstream
  const before = mock2.calls.length;
  assert.equal((await rpc("sendTransaction", ["%%%", { encoding: "base64" }])).error.code, -32602);
  assert.equal((await rpc("sendTransaction", [tx, { encoding: "hex" }])).error.code, -32602);
  assert.equal((await rpc("sendTransaction", [Buffer.alloc(1300, 1).toString("base64"), { encoding: "base64" }])).error.code, -32602);
  assert.equal((await rpc("sendTransaction", [])).error.code, -32602);
  assert.equal(mock2.calls.length, before);
});

test("sendTransaction: all upstreams fail -> upstream error surfaced", async () => {
  mock.responses.sendTransaction = () => { throw new Error("x"); };
  const saved = mock2.responses.sendTransaction; delete mock2.responses.sendTransaction;
  const r = await rpc("sendTransaction", [b58encode(signedTx())]);
  assert.ok(r.error, JSON.stringify(r)); assert.match(r.error.message, /x|Method not found/);
  mock2.responses.sendTransaction = saved;
});

test("simulateTransaction passthrough (no fan-out)", async () => {
  const tx = signedTx().toString("base64");
  const n2 = mock2.calls.length;
  const r = await rpc("simulateTransaction", [tx, { encoding: "base64", sigVerify: false, replaceRecentBlockhash: true }]);
  assert.equal(r.result.value.unitsConsumed, 150);
  assert.equal(mock2.calls.length, n2);
});

// ---------- priority fee w/ tx ----------
test("getPriorityFeeEstimate: decodes serialized transaction account keys", async () => {
  const keys = [pk(), B, C];
  const r = await rpc("getPriorityFeeEstimate", [{ transaction: b58encode(signedTx(keys)), options: { includeAllPriorityFeeLevels: true } }]);
  assert.ok(r.result.priorityFeeLevels, JSON.stringify(r));
  const call = mock.calls.filter((c) => c.method === "getRecentPrioritizationFees").at(-1)!;
  assert.deepEqual(call.params[0], keys);
  const r64 = await rpc("getPriorityFeeEstimate", [{ transaction: signedTx(keys).toString("base64"), options: { transactionEncoding: "Base64" } }]);
  assert.ok(r64.result.priorityFeeEstimate != null);
  assert.equal((await rpc("getPriorityFeeEstimate", [{ transaction: "zzz" }])).error.code, -32602);
});

// ---------- DAS ----------
test("DAS fallback: getAsset (fungible + NFT w/ Metaplex metadata)", async () => {
  const md = decodeMetadata(Buffer.from(META[metadataPda(NFT)], "base64"));
  assert.equal(md.name, "Cool NFT #1"); assert.equal(md.uri, "https://x/1.json"); assert.equal(md.sellerFeeBasisPoints, 500);
  const ft = (await rpc("getAsset", { id: MINT })).result;
  assert.equal(ft.interface, "FungibleToken"); assert.equal(ft.content.metadata.name, "Test Token"); assert.equal(ft.token_info.decimals, 6);
  const nft = (await rpc("getAsset", { id: NFT })).result;
  assert.equal(nft.interface, "V1_NFT"); assert.equal(nft.content.json_uri, "https://x/1.json"); assert.equal(nft.royalty.basis_points, 500);
  assert.deepEqual(nft.grouping, [{ group_key: "collection", group_value: COLL }]); assert.equal(nft.creators[0].address, UA);
  assert.equal((await rpc("getAsset", { id: pk() })).error.code, -32000);
  assert.equal((await rpc("getAsset", { id: "bad" })).error.code, -32602);
});

test("DAS fallback: getAssetsByOwner (hides zero balances, paging, ownership)", async () => {
  const r = (await rpc("getAssetsByOwner", { ownerAddress: OWNER, page: 1, limit: 10 })).result;
  assert.equal(r.total, 2); assert.equal(r.items.length, 2);
  const n = r.items.find((i: any) => i.id === NFT);
  assert.equal(n.ownership.owner, OWNER); assert.equal(n.ownership.frozen, true); assert.equal(n.ownership.ownership_model, "single");
  assert.equal((await rpc("getAssetsByOwner", { ownerAddress: OWNER, displayOptions: { showZeroBalance: true } })).result.total, 3);
  assert.equal((await rpc("getAssetsByOwner", { ownerAddress: OWNER, page: 2, limit: 1 })).result.items.length, 1);
  assert.equal((await rpc("getAssetsByOwner", {})).error.code, -32602);
});

test("DAS: methods w/o fallback need DAS_RPC_URL; DAS_RPC_URL proxies everything", async () => {
  for (const m of ["getSignaturesForAsset", "getAssetsByCreator"]) {
    const r = await rpc(m, { id: A });
    assert.equal(r.error.code, -32601); assert.match(r.error.message, /DAS_RPC_URL/);
  }
  // now served from the local index (empty here): proof not found, bad group params rejected
  assert.equal((await rpc("getAssetProof", { id: A })).error.code, -32000);
  assert.equal((await rpc("getAssetsByGroup", { groupKey: "creator", groupValue: A })).error.code, -32602);
  assert.equal((await rpc("searchAssets", { id: A })).error.code, -32602);
  const das = createFastRpc({ api: { url: mock.httpUrl, dasUrl: dasMock.httpUrl }, hub: { wsUrl: mock.wsUrl }, webhooks: false });
  const p = await das.server.listen(0);
  try {
    const r = await rpc("getAssetProof", { id: A }, p);
    assert.equal(r.result.tree_id, A);
    const call = dasMock.calls.at(-1)!;
    assert.equal(call.method, "getAssetProof"); assert.deepEqual(call.params, { id: A }, "object params forwarded as-is");
  } finally { await das.close(); }
});

// ---------- transactionSubscribe ----------
function mkTx(sig: string, keys: string[], opts: { err?: any; loaded?: string[] } = {}) {
  return { slot: 777, blockTime: 1, version: 0, transaction: { signatures: [sig], message: { accountKeys: keys, instructions: [] } },
    meta: { err: opts.err ?? null, fee: 5000, loadedAddresses: { writable: opts.loaded ?? [], readonly: [] } } };
}
const emitLog = (sig: string, err: any = null, logs: string[] = []) => mock.emit("logsSubscribe", { context: { slot: 777 }, value: { signature: sig, err, logs } });

test("matchesFilter semantics (include/exclude/required/vote/failed)", () => {
  assert.ok(matchesFilter({ accountInclude: [A] }, [A, B], false));
  assert.ok(!matchesFilter({ accountInclude: [C] }, [A, B], false));
  assert.ok(!matchesFilter({ accountInclude: [A], accountExclude: [B] }, [A, B], false));
  assert.ok(!matchesFilter({ accountRequired: [A, C] }, [A, B], false));
  assert.ok(matchesFilter({ accountRequired: [A, B] }, [A, B], false));
  assert.ok(!matchesFilter({ accountInclude: [A], failed: false }, [A], true));
  assert.ok(!matchesFilter({ accountInclude: [A] }, [A, VOTE_PROGRAM], false));
  assert.ok(matchesFilter({ accountInclude: [A], vote: true }, [A, VOTE_PROGRAM], false));
});

test("transactionSubscribe over WS: Helius notification shape + filters", async () => {
  const ws = new WebSocket(`ws://127.0.0.1:${port}`);
  const msgs: any[] = []; ws.on("message", (m) => msgs.push(JSON.parse(m.toString())));
  await new Promise((r) => ws.once("open", r));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "transactionSubscribe", params: [{ accountInclude: ["bad"] }] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "transactionSubscribe", params: [{}] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 3, method: "transactionSubscribe", params: [{ accountInclude: [A], accountExclude: [C], failed: false }, { commitment: "confirmed", encoding: "json", transactionDetails: "full", maxSupportedTransactionVersion: 0 }] }));
  await waitFor(() => msgs.length >= 3);
  assert.equal(msgs.find((m) => m.id === 1).error.code, -32602);
  assert.equal(msgs.find((m) => m.id === 2).error.code, -32602);
  const sid = msgs.find((m) => m.id === 3).result; assert.equal(typeof sid, "number");
  await waitFor(() => mock.activeSubs("logsSubscribe") >= 1, 3000, "upstream logs sub");
  const [s1, s2, s3, s4] = ["ok1", "excl", "fail", "lut"].map((x) => x + b58encode(randomBytes(8)));
  txdb.set(s1, mkTx(s1, [pk(), A])); txdb.set(s2, mkTx(s2, [A, C])); txdb.set(s3, mkTx(s3, [A], { err: { InstructionError: [0, "x"] } }));
  txdb.set(s4, mkTx(s4, [pk()], { loaded: [A] })); // A only via address lookup table
  emitLog(s1); emitLog(s2); emitLog(s3, { InstructionError: [0, "x"] }); emitLog(s4); emitLog(s1); // s1 duplicate
  await waitFor(() => msgs.filter((m) => m.method === "transactionNotification").length >= 2, 4000);
  await sleep(150);
  const notes = msgs.filter((m) => m.method === "transactionNotification");
  assert.deepEqual(notes.map((n) => n.params.result.signature).sort(), [s1, s4].sort());
  const n = notes.find((x) => x.params.result.signature === s1);
  assert.equal(n.params.subscription, sid);
  assert.deepEqual(Object.keys(n.params.result).sort(), ["signature", "slot", "transaction"]);
  assert.deepEqual(Object.keys(n.params.result.transaction).sort(), ["meta", "transaction", "version"]);
  assert.equal(n.params.result.slot, 777); assert.equal(n.params.result.transaction.transaction.signatures[0], s1);
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 9, method: "transactionUnsubscribe", params: [sid] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 10, method: "transactionUnsubscribe", params: [sid] }));
  await waitFor(() => msgs.some((m) => m.id === 10));
  assert.equal(msgs.find((m) => m.id === 9).result, true); assert.equal(msgs.find((m) => m.id === 10).error.code, -32602);
  ws.close();
});

// ---------- webhooks ----------
async function receiver(failFirst = 0) {
  const got: { body: any; auth?: string }[] = []; let fails = failFirst;
  const s = http.createServer((req, res) => {
    let b = ""; req.on("data", (c) => (b += c));
    req.on("end", () => { if (fails > 0) { fails--; res.writeHead(500); return res.end(); } got.push({ body: JSON.parse(b), auth: req.headers.authorization }); res.writeHead(200); res.end(); });
  });
  await new Promise<void>((r) => s.listen(0, "127.0.0.1", r));
  return { url: `http://127.0.0.1:${(s.address() as net.AddressInfo).port}/hook`, got, close: () => { s.closeAllConnections(); return new Promise((r) => s.close(r)); } };
}
const rest = async (method: string, p: string, body?: any, key = "") => {
  const r = await fetch(`http://127.0.0.1:${port}${p}${key ? `?api-key=${key}` : ""}`, { method, body: body ? JSON.stringify(body) : undefined, headers: { "content-type": "application/json" } });
  const t = await r.text(); return { status: r.status, json: t ? JSON.parse(t) : undefined };
};

test("webhooks REST: create/list/get/update/delete + validation + persistence", async () => {
  const rv = await receiver();
  try {
    assert.equal((await rest("POST", "/v0/webhooks", { webhookURL: "ftp://x", accountAddresses: [A] })).status, 400);
    assert.equal((await rest("POST", "/v0/webhooks", { webhookURL: rv.url, accountAddresses: [] })).status, 400);
    assert.equal((await rest("POST", "/v0/webhooks", { webhookURL: rv.url, accountAddresses: [A], webhookType: "bogus" })).status, 400);
    const c = await rest("POST", "/v0/webhooks", { webhookURL: rv.url, accountAddresses: [B, B], transactionTypes: ["ANY"], webhookType: "raw" });
    assert.equal(c.status, 200);
    const id = c.json.webhookID;
    assert.deepEqual(c.json.accountAddresses, [B]); assert.equal(c.json.webhookType, "raw");
    assert.equal((await rest("GET", "/v0/webhooks")).json.length, 1);
    assert.equal((await rest("GET", `/v0/webhooks/${id}`)).json.webhookURL, rv.url);
    assert.equal((await rest("PUT", `/v0/webhooks/${id}`, { accountAddresses: [B, C] })).json.accountAddresses.length, 2);
    // persisted
    const saved = JSON.parse(fs.readFileSync(whFile, "utf8"));
    assert.equal(saved[0].webhookID, id); assert.deepEqual(saved[0].accountAddresses, [B, C]);
    const hub2 = new StreamHub({ wsUrl: mock.wsUrl });
    const m2 = new WebhookManager(hub2, async () => null, { file: whFile });
    assert.equal(m2.list("anonymous")[0].webhookID, id, "reloaded from disk");
    m2.close(); hub2.close();
    assert.equal((await rest("DELETE", `/v0/webhooks/${id}`)).status, 200);
    assert.equal((await rest("GET", `/v0/webhooks/${id}`)).status, 404);
    assert.deepEqual(JSON.parse(fs.readFileSync(whFile, "utf8")), []);
  } finally { await rv.close(); }
});

test("webhooks delivery: POST raw tx array w/ authHeader, retries w/ backoff after 5xx", async () => {
  const rv = await receiver(2);
  const W = pk();
  const c = await rest("POST", "/v0/webhooks", { webhookURL: rv.url, accountAddresses: [W], webhookType: "raw", authHeader: "Bearer s3cret" });
  const id = c.json.webhookID;
  try {
    await waitFor(() => app.hub.stats().some((t) => t.key.includes(W)), 3000);
    await sleep(150);
    const sig = "wh" + b58encode(randomBytes(8));
    txdb.set(sig, mkTx(sig, [W, pk()]));
    mock.emit("logsSubscribe", { context: { slot: 900 }, value: { signature: sig, err: null, logs: [] } });
    await waitFor(() => rv.got.length >= 1, 5000, "webhook delivery");
    assert.equal(rv.got[0].auth, "Bearer s3cret");
    assert.ok(Array.isArray(rv.got[0].body)); assert.equal(rv.got[0].body[0].transaction.signatures[0], sig);
    await waitFor(() => app.webhooks!.stats.get(id)?.delivered === 1, 2000, "delivered stat");
    const st = app.webhooks!.stats.get(id)!;
    assert.equal(st.delivered, 1); assert.equal(st.retries, 2);
  } finally { await rest("DELETE", `/v0/webhooks/${id}`); await rv.close(); }
});

// ---------- persistent replay ----------
test("persistent ring: survives hub restart, seq continues, retention honoured", async () => {
  const dir = tmp();
  const mk = (retentionMs = 60_000) => new StreamHub({ wsUrl: mock.wsUrl, persistDir: dir, retentionMs, lingerMs: 10 });
  let h = mk();
  const got: any[] = [];
  let s = h.subscribe("slot", {}, { deliver: (e) => got.push(e) });
  await waitFor(() => mock.activeSubs("slotSubscribe") >= 1);
  for (const n of [5001, 5002, 5003]) mock.slot(n);
  await waitFor(() => got.length === 3);
  s.detach(); h.close();
  assert.equal(fs.readdirSync(dir).filter((f) => f.endsWith(".jsonl")).length, 1);

  h = mk(); // "restart"
  const got2: any[] = [];
  s = h.subscribe("slot", {}, { deliver: (e) => got2.push(e) }, { fromSeq: 1 });
  assert.deepEqual(s.replay.map((e) => [e.seq, e.data.slot]), [[2, 5002], [3, 5003]]);
  assert.equal(s.lost, 0); assert.equal(s.headSeq, 3);
  await waitFor(() => mock.activeSubs("slotSubscribe") >= 1);
  await sleep(50);
  mock.slot(5003); mock.slot(5004); // 5003 deduped from disk state
  await waitFor(() => got2.length >= 1);
  await sleep(50);
  assert.deepEqual(got2.map((e) => [e.seq, e.data.slot]), [[4, 5004]]);
  s.detach(); h.close();

  await sleep(30);
  h = mk(1); // everything older than 1ms expired, but seq high-water persists
  s = h.subscribe("slot", {}, { deliver: () => {} }, { fromSeq: 2 });
  assert.equal(s.replay.length, 0); assert.equal(s.headSeq, 4); assert.equal(s.lost, 2);
  s.detach(); h.close();
});

test("persistent ring: compaction keeps file bounded, torn line tolerated", async () => {
  const dir = tmp();
  let h = new StreamHub({ wsUrl: mock.wsUrl, persistDir: dir, ringSize: 5, lingerMs: 10 });
  const got: any[] = [];
  let s = h.subscribe("slot", {}, { deliver: (e) => got.push(e) });
  await waitFor(() => mock.activeSubs("slotSubscribe") >= 1);
  for (let i = 0; i < 40; i++) mock.slot(6000 + i);
  await waitFor(() => got.length === 40);
  s.detach(); h.close();
  const f = path.join(dir, fs.readdirSync(dir).find((x) => x.endsWith(".jsonl"))!);
  assert.ok(fs.readFileSync(f, "utf8").trim().split("\n").length <= 11);
  fs.appendFileSync(f, '{"seq":41,"slot":');
  h = new StreamHub({ wsUrl: mock.wsUrl, persistDir: dir, ringSize: 5, lingerMs: 10 });
  s = h.subscribe("slot", {}, { deliver: () => {} }, { fromSeq: 37 });
  assert.deepEqual(s.replay.map((e) => e.seq), [38, 39, 40]); assert.equal(s.headSeq, 40);
  s.detach(); h.close();
});

// ---------- binary auth + limits ----------
test("binary port: Auth frame required when API keys set; client apiKey option", async () => {
  const a = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl }, server: { apiKeys: ["k1"] }, webhooks: false });
  const p = await a.server.listen(0);
  try {
    const raw = await rawClient(p);
    raw.send(MsgType.Request, 1, ["getSlot", []]);
    await waitFor(() => raw.closed);
    assert.equal(raw.frames[0].type, MsgType.Error); assert.match(raw.frames[0].payload, /Auth/);
    const bad = await rawClient(p);
    bad.send(MsgType.Auth, 1, { apiKey: "nope" });
    await waitFor(() => bad.closed); assert.equal(bad.frames[0].type, MsgType.Error);
    await assert.rejects(RpcClient.connect(p, "127.0.0.1", { apiKey: "nope", reconnect: false }), /Unauthorized/);
    const c = await RpcClient.connect(p, "127.0.0.1", { apiKey: "k1" });
    assert.equal(await c.call("getSlot"), 1234);
    c.simulateDrop(); // re-auths on reconnect
    await waitFor(() => c.reconnects >= 1 && c.connected);
    assert.equal(await c.call("getSlot"), 1234);
    c.close();
  } finally { await a.close(); }
});

test("rate limits + credits per key (HTTP 429 / -32429, binary error, /v0/usage)", async () => {
  const limiter = new Limiter({ k1: { rps: 3, burst: 3 }, k2: { credits: 11 } });
  const a = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl }, server: { apiKeys: ["k1", "k2"], limiter }, webhooks: false });
  const p = await a.server.listen(0);
  try {
    const st: number[] = [];
    for (let i = 0; i < 5; i++) st.push((await httpPost(p, JSON.stringify({ jsonrpc: "2.0", id: i, method: "getSlot" }), "/?api-key=k1")).status);
    assert.deepEqual(st, [200, 200, 200, 429, 429]);
    const r = await httpPost(p, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getSlot" }), "/?api-key=k1");
    assert.equal(r.json.error.code, -32429);
    await sleep(400); // refill
    assert.equal((await httpPost(p, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getSlot" }), "/?api-key=k1")).status, 200);
    // credits: getProgramAccounts costs 10, getSlot 1
    const c = await RpcClient.connect(p, "127.0.0.1", { apiKey: "k2" });
    assert.equal(await c.call("getSlot"), 1234);
    await c.call("getProgramAccounts", "11111111111111111111111111111111").catch(() => {});
    await assert.rejects(c.call("getSlot"), /Credit limit/);
    c.close();
    const u = await (await fetch(`http://127.0.0.1:${p}/v0/usage?api-key=k2`)).json();
    assert.equal(u.creditsUsed, 11); assert.equal(u.creditsRemaining, 0); assert.equal(u.rateLimited, 1);
  } finally { await a.close(); }
});
