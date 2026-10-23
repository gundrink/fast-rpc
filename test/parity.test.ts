// Helius parity: response shapes are compared against examples copied from the Helius docs (test/fixtures/helius).
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { randomBytes } from "node:crypto";
import { WebSocket } from "ws";
import { createFastRpc, b58encode, b58decode, RpcClient, heliusTxResult, editionPda } from "../src/index.js";
import { startMockSolana, httpPost } from "./helpers.js";

const fx = (n: string) => JSON.parse(fs.readFileSync(new URL(`./fixtures/helius/${n}.json`, import.meta.url), "utf8"));
const pk = () => b58encode(randomBytes(32));
const SYS = "11111111111111111111111111111111";
const W = pk(), TO = pk(), MINT = pk(), TA = pk(), MASTER_MINT = pk();

/** every key of the Helius example exists in ours with the same JSON type (null matches anything) */
function sameShape(ours: any, ref: any, path = "$") {
  if (ref === null || ours === null) return;
  const t = (x: any) => (Array.isArray(x) ? "array" : typeof x);
  assert.equal(t(ours), t(ref), `${path}: type ${t(ours)} != Helius ${t(ref)}`);
  if (Array.isArray(ref)) { if (ref.length && ours.length) sameShape(ours[0], ref[0], `${path}[0]`); return; }
  if (typeof ref === "object") for (const k of Object.keys(ref)) {
    assert.ok(k in ours, `${path}.${k} missing (Helius field)`);
    sameShape(ours[k], ref[k], `${path}.${k}`);
  }
}

// --- mock chain: 5 SOL transfers W -> TO in slots 105..101 ---
const txs = new Map<string, any>();
const sigList: any[] = [];
function transferTx(sig: string, slot: number, lamports: number, err: any = null) {
  const d = Buffer.alloc(12); d.writeUInt32LE(2, 0); d.writeBigUInt64LE(BigInt(lamports), 4);
  return { slot, blockTime: 1_700_000_000 + slot, version: "legacy",
    transaction: { signatures: [sig], message: { header: { numRequiredSignatures: 1, numReadonlySignedAccounts: 0, numReadonlyUnsignedAccounts: 1 }, accountKeys: [W, TO, SYS], recentBlockhash: SYS,
      instructions: [{ programIdIndex: 2, accounts: [0, 1], data: b58encode(d) }] } },
    meta: { err, fee: 5000, preBalances: [10_000_000_000, 0, 1], postBalances: [10_000_000_000 - lamports - 5000, lamports, 1], innerInstructions: [], logMessages: [], preTokenBalances: [], postTokenBalances: [], loadedAddresses: { writable: [], readonly: [] } } };
}
for (let i = 0; i < 5; i++) {
  const sig = b58encode(randomBytes(64)); const slot = 105 - i; const err = i === 2 ? { InstructionError: [0, "Custom"] } : null;
  txs.set(sig, transferTx(sig, slot, 1_000_000 * (i + 1), err));
  sigList.push({ signature: sig, slot, err, memo: null, blockTime: 1_700_000_000 + slot, confirmationStatus: "finalized" });
}
function tokenAcc(mint: string, owner: string, amount: bigint) {
  const b = Buffer.alloc(165); Buffer.from(b58decode(mint)).copy(b, 0); Buffer.from(b58decode(owner)).copy(b, 32); b.writeBigUInt64LE(amount, 64); b[108] = 1;
  return b.toString("base64");
}

let mock: Awaited<ReturnType<typeof startMockSolana>>;
let app: ReturnType<typeof createFastRpc>, port: number;
const rpc = async (method: string, params: any, p = port) => (await httpPost(p, JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }))).json;

before(async () => {
  mock = await startMockSolana();
  mock.responses.getTransaction = (p) => txs.get(p[0]) ?? null;
  mock.responses.getSignaturesForAddress = (p) => {
    if (p[0] !== W) return [];
    let l = sigList; const o = p[1] ?? {};
    if (o.before) l = l.slice(l.findIndex((s) => s.signature === o.before) + 1);
    if (o.until) l = l.slice(0, l.findIndex((s) => s.signature === o.until));
    return l.slice(0, o.limit ?? 1000);
  };
  mock.responses.getTokenAccountsByOwner = (p) => ({ context: { slot: 1 }, value: p[1].programId?.startsWith("Tokenkeg") ? [{ pubkey: TA, account: { owner: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", data: [tokenAcc(MINT, W, 5n), "base64"] } }] : [] });
  mock.responses.getAccountInfo = (p) => {
    if (p[0] === editionPda(MASTER_MINT)) { const b = Buffer.alloc(18); b[0] = 6; b.writeBigUInt64LE(2n, 1); b[9] = 1; b.writeBigUInt64LE(10n, 10); return { context: { slot: 1 }, value: { data: [b.toString("base64"), "base64"] } }; }
    return { context: { slot: 1 }, value: null };
  };
  mock.responses.getProgramAccounts = (p) => {
    if (p[0] === "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s") return [2n, 1n].map((n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(n); return { pubkey: pk(), account: { data: [b.toString("base64"), "base64"] } }; });
    if (p[0].startsWith("Tokenkeg")) return [{ pubkey: TA, account: { data: [tokenAcc(MINT, W, 5n), "base64"] } }, { pubkey: pk(), account: { data: [tokenAcc(MINT, TO, 0n), "base64"] } }];
    return [];
  };
  app = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl }, webhooks: { file: undefined } as any, chain: { detect: false } });
  port = await app.server.listen(0);
});
after(async () => { await app.close(); await mock.close(); });

test("auth off by default: ?api-key= ignored on HTTP/WS/SSE/usage; binary needs no Auth frame", async () => {
  assert.equal((await httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getSlot" }), "/?api-key=whatever")).json.result, 1234);
  assert.equal((await httpPost(port, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getSlot" }))).json.result, 1234);
  const u = await (await fetch(`http://127.0.0.1:${port}/v0/usage?api-key=zzz`)).json();
  assert.equal(u.apiKey, "anonymous", "key ignored when auth is off");
  const ws = new WebSocket(`ws://127.0.0.1:${port}/?api-key=nope`);
  const msg = await new Promise<any>((r, j) => { ws.on("open", () => ws.send(JSON.stringify({ jsonrpc: "2.0", id: 7, method: "getSlot" }))); ws.on("message", (m) => r(JSON.parse(m.toString()))); ws.on("error", j); });
  assert.equal(msg.result, 1234); ws.close();
  const ac = new AbortController();
  const sse = await fetch(`http://127.0.0.1:${port}/stream?type=slot&api-key=nope`, { signal: ac.signal });
  assert.equal(sse.status, 200); ac.abort();
  const c = await RpcClient.connect(port, "127.0.0.1", { reconnect: false });
  assert.equal(await c.call("getSlot"), 1234); c.close();
  const c2 = await RpcClient.connect(port, "127.0.0.1", { reconnect: false, apiKey: "anything" });
  assert.equal(await c2.call("getSlot"), 1234); c2.close();
  // re-enable: API_KEYS / apiKeys -> 401 with Helius DAS-style -32001
  const locked = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl }, server: { apiKeys: ["k"] }, webhooks: false, chain: { detect: false } });
  const lp = await locked.server.listen(0);
  try {
    const r = await httpPost(lp, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getSlot" }));
    assert.equal(r.status, 401); assert.equal(r.json.error.code, -32001);
    assert.equal((await httpPost(lp, JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getSlot" }), "/?api-key=k")).json.result, 1234);
  } finally { await locked.close(); }
});

test("getPriorityFeeEstimate: Helius options + response shape", async () => {
  const all = (await rpc("getPriorityFeeEstimate", [{ accountKeys: [W], options: { includeAllPriorityFeeLevels: true } }])).result;
  sameShape(all, fx("priority-fee-all"));
  assert.deepEqual(Object.keys(all.priorityFeeLevels), Object.keys(fx("priority-fee-all").priorityFeeLevels));
  assert.equal((await rpc("getPriorityFeeEstimate", [{ options: { recommended: true } }])).result.priorityFeeEstimate, 10_000, "recommended floor");
  const noEmpty = (await rpc("getPriorityFeeEstimate", [{ options: { evaluateEmptySlotAsZero: false, includeAllPriorityFeeLevels: true } }])).result;
  assert.equal(noEmpty.priorityFeeLevels.min, 10, "zero-fee slots ignored");
  assert.ok((await rpc("getPriorityFeeEstimate", [{ options: { includeVote: true, priorityLevel: "VeryHigh" } }])).result.priorityFeeEstimate >= 0);
  assert.equal((await rpc("getPriorityFeeEstimate", [{ options: { priorityLevel: "Bogus" } }])).error.code, -32602);
});

test("Enhanced Transactions REST: POST /v0/transactions + GET /v0/addresses/:a/transactions", async () => {
  const sigs = sigList.map((s) => s.signature);
  const r = await httpPost(port, JSON.stringify({ transactions: sigs.slice(0, 2) }), "/v0/transactions?api-key=x");
  assert.equal(r.status, 200); assert.equal(r.json.length, 2);
  const ref = fx("enhanced-transactions")[0]; const ours = r.json[0];
  for (const k of ["description", "type", "source", "fee", "feePayer", "signature", "slot", "timestamp", "nativeTransfers", "tokenTransfers", "accountData", "instructions", "events"]) assert.ok(k in ours, k);
  const { events: _e, tokenTransfers: _t, instructions: _i, ...top } = ref;
  sameShape(ours, { ...top, accountData: [{ account: "", nativeBalanceChange: 0, tokenBalanceChanges: [] }] });
  assert.equal(ours.type, "TRANSFER"); assert.equal(ours.nativeTransfers[0].amount, 1_000_000); assert.ok("transactionError" in ours);
  assert.equal((await httpPost(port, JSON.stringify({ transactions: Array(101).fill(sigs[0]) }), "/v0/transactions")).status, 400);
  const bad = await httpPost(port, "nope", "/v0/transactions"); assert.equal(bad.status, 400); assert.ok(bad.json.error);

  const g = await (await fetch(`http://127.0.0.1:${port}/v0/addresses/${W}/transactions?limit=2`)).json();
  assert.deepEqual(g.map((x: any) => x.signature), sigs.slice(0, 2));
  const p2 = await (await fetch(`http://127.0.0.1:${port}/v0/addresses/${W}/transactions?before-signature=${sigs[1]}&limit=10`)).json();
  assert.deepEqual(p2.map((x: any) => x.signature), sigs.slice(2));
  const after = await (await fetch(`http://127.0.0.1:${port}/v0/addresses/${W}/transactions?after-signature=${sigs[2]}`)).json();
  assert.deepEqual(after.map((x: any) => x.signature), sigs.slice(0, 2));
  assert.equal((await (await fetch(`http://127.0.0.1:${port}/v0/addresses/${W}/transactions?type=SWAP`)).json()).length, 0);
  assert.equal((await fetch(`http://127.0.0.1:${port}/v0/addresses/${W}/transactions?limit=500`)).status, 400);
  assert.equal((await fetch(`http://127.0.0.1:${port}/v0/addresses/notakey/transactions`)).status, 400);
});

test("getTransactionsForAddress: signatures/full shapes, sortOrder, filters, paginationToken", async () => {
  const s = (await rpc("getTransactionsForAddress", [W, { limit: 2 }])).result;
  sameShape(s, fx("gtfa-signatures"));
  assert.deepEqual(s.data.map((x: any) => x.slot), [105, 104]); assert.match(s.paginationToken, /^\d+:\d+$/);
  const n = (await rpc("getTransactionsForAddress", [W, { limit: 2, paginationToken: s.paginationToken }])).result;
  assert.deepEqual(n.data.map((x: any) => x.slot), [103, 102]);
  const last = (await rpc("getTransactionsForAddress", [W, { limit: 2, paginationToken: n.paginationToken }])).result;
  assert.deepEqual(last.data.map((x: any) => x.slot), [101]); assert.equal(last.paginationToken, null);
  assert.deepEqual((await rpc("getTransactionsForAddress", [W, { sortOrder: "asc", limit: 3 }])).result.data.map((x: any) => x.slot), [101, 102, 103]);
  assert.deepEqual((await rpc("getTransactionsForAddress", [W, { filters: { status: "failed" } }])).result.data.map((x: any) => x.slot), [103]);
  assert.deepEqual((await rpc("getTransactionsForAddress", [W, { filters: { slot: { gte: 102, lt: 104 }, status: "succeeded" } }])).result.data.map((x: any) => x.slot), [102]);
  assert.deepEqual((await rpc("getTransactionsForAddress", [W, { filters: { blockTime: { gt: 1_700_000_103 } } }])).result.data.map((x: any) => x.slot), [105, 104]);
  assert.deepEqual((await rpc("getTransactionsForAddress", [W, { filters: { signature: { lt: sigList[3].signature } } }])).result.data.map((x: any) => x.slot), [101]);
  const f = (await rpc("getTransactionsForAddress", [W, { transactionDetails: "full", limit: 1 }])).result;
  sameShape(f, fx("gtfa-full")); assert.equal(f.data[0].meta.fee, 5000); assert.ok(!("signature" in f.data[0]));
  const ta = (await rpc("getTransactionsForAddress", [W, { filters: { tokenAccounts: "all" } }])).result;
  assert.equal(ta.data.length, 5);
  for (const bad of [{ sortOrder: "up" }, { transactionDetails: "x" }, { limit: 0 }, { paginationToken: "abc" }, { filters: { status: "ok" } }])
    assert.equal((await rpc("getTransactionsForAddress", [W, bad])).error.code, -32602, JSON.stringify(bad));
});

test("DAS: getTokenAccounts / getNftEditions / getSignaturesForAsset / getAssetsByOwner displayOptions + sortBy/cursor", async () => {
  const byOwner = (await rpc("getTokenAccounts", { owner: W })).result;
  assert.deepEqual(Object.keys(byOwner), ["total", "limit", "page", "token_accounts"]);
  assert.deepEqual(byOwner.token_accounts[0], { address: TA, mint: MINT, owner: W, amount: 5, delegated_amount: 0, frozen: false });
  const byMint = (await rpc("getTokenAccounts", { mint: MINT })).result;
  assert.equal(byMint.total, 1, "zero balances hidden");
  assert.equal((await rpc("getTokenAccounts", { mint: MINT, options: { showZeroBalance: true } })).result.total, 2);
  const cur = (await rpc("getTokenAccounts", { mint: MINT, options: { showZeroBalance: true }, limit: 1, cursor: "" })).result;
  assert.equal(cur.token_accounts.length, 1); assert.ok(cur.cursor);
  assert.equal((await rpc("getTokenAccounts", {})).error.code, -32602);
  const ed = (await rpc("getNftEditions", { mint: MASTER_MINT })).result;
  assert.equal(ed.master_edition_address, editionPda(MASTER_MINT)); assert.equal(ed.supply, 2); assert.equal(ed.max_supply, 10);
  assert.deepEqual(ed.editions.map((e: any) => e.edition), [1, 2]); assert.ok("edition_address" in ed.editions[0] && "mint" in ed.editions[0]);
  const sa = (await rpc("getSignaturesForAsset", { id: W, limit: 2 })).result;
  assert.deepEqual(Object.keys(sa), ["total", "limit", "page", "items"]); assert.deepEqual(sa.items[0], [sigList[0].signature, "Unknown"]);
  const own = (await rpc("getAssetsByOwner", { ownerAddress: W, displayOptions: { showNativeBalance: true, showGrandTotal: true } })).result;
  assert.equal(own.nativeBalance.lamports, 42); assert.equal(typeof own.grand_total, "number");
  assert.equal((await rpc("searchAssets", { ownerAddress: W, sortBy: { sortBy: "bogus" } })).error.code, -32602);
  assert.ok((await rpc("searchAssets", { ownerAddress: W, sortBy: { sortBy: "created", sortDirection: "asc" } })).result);
});

test("transactionSubscribe notification shape + transactionDetails accounts", () => {
  const tx = txs.get(sigList[0].signature);
  const ref = fx("transaction-notification").params.result;
  const ours = heliusTxResult({ signature: sigList[0].signature, slot: 105, tx, transactionIndex: 3 }, "full");
  sameShape(ours, ref); assert.equal(ours.transactionIndex, 3);
  const acc = heliusTxResult({ signature: sigList[0].signature, slot: 105, tx }, "accounts");
  assert.deepEqual(acc.transaction.transaction.accountKeys[0], { pubkey: W, signer: true, writable: true, source: "transaction" });
  assert.equal(acc.transaction.transaction.accountKeys[2].writable, false);
  assert.ok(!("logMessages" in acc.transaction.meta));
});

test("webhooks REST: Helius object fields + PATCH toggle", async () => {
  const body = { webhookURL: "http://127.0.0.1:9/hook", webhookType: "enhanced", accountAddresses: [W], transactionTypes: ["TRANSFER"], authHeader: "Bearer x" };
  const c = await httpPost(port, JSON.stringify(body), "/v0/webhooks?api-key=ignored");
  assert.equal(c.status, 200); sameShape(c.json, fx("webhook")); assert.equal(c.json.active, true);
  const r = await fetch(`http://127.0.0.1:${port}/v0/webhooks/${c.json.webhookID}`, { method: "PATCH", body: JSON.stringify({ active: false }) });
  assert.equal((await r.json()).active, false);
  assert.equal((await fetch(`http://127.0.0.1:${port}/v0/webhooks/${c.json.webhookID}`, { method: "PATCH", body: "{}" })).status, 400);
  assert.equal((await fetch(`http://127.0.0.1:${port}/v0/webhooks/${c.json.webhookID}`, { method: "DELETE" })).status, 200);
});
