import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { EventEmitter } from "node:events";
import { randomBytes } from "node:crypto";
import { WebSocket } from "ws";
import { keccak_256 } from "@noble/hashes/sha3.js";
import {
  createFastRpc, b58encode, b58decode, buildLegacyTx, SolanaWs, GeyserUpstream, StreamHub, CnftIndexer, MerkleTree, DISC, BUBBLEGUM, assetId,
  parseEnhanced, WebhookManager, tipLamports, decodeTransaction, SYSTEM_PROGRAM,
} from "../src/index.js";
import { startMockSolana, waitFor, sleep } from "./helpers.js";

const pk = () => b58encode(randomBytes(32));
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), "fastrpc-r-"));
let mock: Awaited<ReturnType<typeof startMockSolana>>;
before(async () => { mock = await startMockSolana(); });
after(async () => { await mock.close(); });

const post = async (port: number, method: string, params: any) => {
  const r = await fetch(`http://127.0.0.1:${port}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) });
  return r.json() as Promise<any>;
};
const signed = (keys: string[], ix: any[]) => { const b = buildLegacyTx(keys, 1, ix); randomBytes(64).copy(b, 1); return b; };
const transferIx = (from: number, to: number, lamports: number, program: number) => { const d = Buffer.alloc(12); d.writeUInt32LE(2); d.writeBigUInt64LE(BigInt(lamports), 4); return { program, accounts: [from, to], data: d }; };

// ---------------- Jito ----------------
test("jito: getTipAccounts/sendBundle/getBundleStatuses relay to JITO_URL with tip validation (mock only)", async () => {
  const jito = await startMockSolana();
  const TIPS = [pk(), pk()];
  jito.responses.getTipAccounts = () => TIPS;
  jito.responses.sendBundle = () => "bundle-id-1";
  jito.responses.getBundleStatuses = (p) => ({ context: { slot: 1 }, value: p[0].map((id: string) => ({ bundle_id: id, confirmation_status: "confirmed" })) });
  jito.responses.sendTransaction = () => "jito-sig";
  mock.responses.sendTransaction = () => "rpc-sig";
  const app = createFastRpc({ api: { url: mock.httpUrl, jito: { url: jito.httpUrl, minTipLamports: 1000 } }, hub: { wsUrl: mock.wsUrl }, webhooks: false, chain: { detect: false } });
  const port = await app.server.listen(0);
  try {
    assert.deepEqual((await post(port, "getTipAccounts", [])).result, TIPS);
    await post(port, "getTipAccounts", []);
    assert.equal(jito.calls.filter((c) => c.method === "getTipAccounts").length, 1, "tip accounts cached");
    assert.equal((jito.calls[0] as any).path, "/api/v1/bundles");
    const payer = pk();
    const good = signed([payer, TIPS[1], SYSTEM_PROGRAM], [transferIx(0, 1, 5000, 2)]).toString("base64");
    const noTip = signed([payer, pk(), SYSTEM_PROGRAM], [transferIx(0, 1, 5000, 2)]).toString("base64");
    const lowTip = signed([payer, TIPS[0], SYSTEM_PROGRAM], [transferIx(0, 1, 10, 2)]).toString("base64");
    const unsigned = buildLegacyTx([payer, TIPS[0], SYSTEM_PROGRAM], 1, [transferIx(0, 1, 5000, 2)]).toString("base64");
    assert.equal(tipLamports(decodeTransaction(good, "base64"), new Set(TIPS)), 5000);
    assert.equal((await post(port, "sendBundle", [[noTip], { encoding: "base64" }])).error.code, -32602);
    assert.match((await post(port, "sendBundle", [[lowTip], { encoding: "base64" }])).error.message, /tip/);
    assert.match((await post(port, "sendBundle", [[unsigned], { encoding: "base64" }])).error.message, /not signed/);
    assert.equal((await post(port, "sendBundle", [[good, good, good, good, good, good], { encoding: "base64" }])).error.code, -32602);
    assert.equal(jito.calls.filter((c) => c.method === "sendBundle").length, 0, "nothing sent for invalid bundles");
    assert.equal((await post(port, "sendBundle", [[noTip, good], { encoding: "base64" }])).result, "bundle-id-1");
    const sb = jito.calls.find((c) => c.method === "sendBundle")!;
    assert.deepEqual(sb.params, [[noTip, good], { encoding: "base64" }]);
    assert.equal((await post(port, "getBundleStatuses", [["bundle-id-1"]])).result.value[0].confirmation_status, "confirmed");
    assert.equal((await post(port, "getBundleStatuses", [[]])).error.code, -32602);
    // Sender-style dual send
    assert.match((await post(port, "sendTransaction", [good, { encoding: "base64", jito: true }])).error.message, /skipPreflight/);
    assert.match((await post(port, "sendTransaction", [noTip, { encoding: "base64", jito: true, skipPreflight: true }])).error.message, /tip/);
    assert.equal(mock.calls.filter((c) => c.method === "sendTransaction").length, 0, "invalid tip never reaches RPC");
    const r = await post(port, "sendTransaction", [good, { encoding: "base64", jito: true, skipPreflight: true }]);
    assert.ok(["jito-sig", "rpc-sig"].includes(r.result));
    await waitFor(() => jito.calls.some((c) => c.method === "sendTransaction") && mock.calls.some((c) => c.method === "sendTransaction"), 2000, "dual send");
    assert.equal((jito.calls.find((c) => c.method === "sendTransaction") as any).path, "/api/v1/transactions");
    assert.deepEqual(mock.calls.find((c) => c.method === "sendTransaction")!.params[1], { encoding: "base64", skipPreflight: true }, "jito flag stripped upstream");
  } finally { await app.close(); await jito.close(); delete mock.responses.sendTransaction; }
});

// ---------------- Geyser ----------------
class FakeStream extends EventEmitter { writes: any[] = []; write(r: any) { this.writes.push(r); return true; } end() { this.emit("end"); } destroy() {} }
const fakeClient = () => { const c = { streams: [] as FakeStream[], connects: 0, async connect() { c.connects++; }, async subscribe() { const s = new FakeStream(); c.streams.push(s); return s; } }; return c; };
const last = (s: FakeStream) => s.writes.filter((w) => !w.ping).at(-1);

test("geyser: slot/account/program via gRPC stream, others fall back to WS, reconnect re-sends filters", async () => {
  const c = fakeClient();
  const g = new GeyserUpstream({ client: c, fallback: new SolanaWs(mock.wsUrl), minBackoffMs: 20 });
  const hub = new StreamHub({ upstream: g, lingerMs: 0 });
  try {
    const slots: any[] = [], accs: any[] = [], progs: any[] = [];
    const ACC = pk(), PROG = pk();
    hub.subscribe("slot", {}, { deliver: (e) => slots.push(e) });
    hub.subscribe("account", { pubkey: ACC, commitment: "processed" }, { deliver: (e) => accs.push(e) });
    hub.subscribe("program", { programId: PROG, commitment: "processed", filters: [{ dataSize: 165 }, { memcmp: { offset: 32, bytes: ACC } }] }, { deliver: (e) => progs.push(e) });
    const logsBefore = mock.activeSubs("logsSubscribe");
    hub.subscribe("logs", { mentions: ACC }, { deliver: () => {} });
    await waitFor(() => c.streams.length >= 1 && !!last(c.streams[0]) && Object.keys(last(c.streams[0]).accounts).length === 2, 2000, "filters written");
    const s = c.streams[0]; const req = last(s);
    assert.equal(req.commitment, 0);
    const accName = Object.entries<any>(req.accounts).find(([, f]) => f.account[0] === ACC)![0];
    const progName = Object.entries<any>(req.accounts).find(([, f]) => f.owner[0] === PROG)![0];
    assert.deepEqual(req.accounts[progName].filters, [{ datasize: "165" }, { memcmp: { offset: "32", base58: ACC } }]);
    const slotName = Object.keys(req.slots)[0]; assert.ok(slotName);
    await waitFor(() => mock.activeSubs("logsSubscribe") > logsBefore, 2000, "logs via WS fallback");
    assert.equal(g.stats.fallbackSubs, 1);
    s.emit("data", { filters: [slotName], slot: { slot: "500", parent: "499", status: 2 } });
    s.emit("data", { filters: [slotName], slot: { slot: "501", parent: "500", status: 0 } });
    s.emit("data", { filters: [slotName], slot: { slot: "501", parent: "500", status: 1 } });
    const acct = { pubkey: b58decode(ACC), lamports: "42", owner: b58decode(PROG), executable: false, rentEpoch: "18446744073709551615", data: Buffer.from("hello"), writeVersion: "1" };
    s.emit("data", { filters: [accName], account: { slot: "501", isStartup: false, account: acct } });
    s.emit("data", { filters: [progName], account: { slot: "502", isStartup: false, account: acct } });
    await waitFor(() => slots.length === 1 && accs.length === 1 && progs.length === 1, 2000, "updates");
    assert.equal(slots[0].slot, 501); assert.equal(slots[0].raw?.root ?? 500, 500);
    assert.equal(accs[0].raw.value.lamports, 42); assert.deepEqual(accs[0].raw.value.data, [Buffer.from("hello").toString("base64"), "base64"]);
    assert.equal(progs[0].raw.value.pubkey, ACC);
    // stream drop -> reconnect with full filter set
    s.emit("error", new Error("boom"));
    await waitFor(() => c.streams.length === 2 && !!last(c.streams[1]), 2000, "reconnect");
    assert.equal(Object.keys(last(c.streams[1]).accounts).length, 2);
  } finally { hub.close(); }
});

test("geyser: native transactionSubscribe (no getTransaction), >100 accounts allowed", async () => {
  const c = fakeClient();
  const g = new GeyserUpstream({ client: c, fallback: new SolanaWs(mock.wsUrl) });
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { upstream: g }, webhooks: false, chain: { detect: false } });
  const port = await app.server.listen(0);
  const ws = new WebSocket(`ws://127.0.0.1:${port}`);
  try {
    await new Promise((r) => ws.once("open", r));
    const msgs: any[] = []; ws.on("message", (m) => msgs.push(JSON.parse(m.toString())));
    const accts = Array.from({ length: 1500 }, pk);
    ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "transactionSubscribe", params: [{ accountInclude: accts, failed: false }, { commitment: "confirmed" }] }));
    await waitFor(() => msgs.some((m) => m.id === 1), 2000, "sub ack");
    assert.ok(typeof msgs.find((m) => m.id === 1).result === "number", JSON.stringify(msgs));
    await waitFor(() => c.streams.length === 1 && !!last(c.streams[0]), 2000);
    const req = last(c.streams[0]); const name = Object.keys(req.transactions)[0];
    assert.equal(req.commitment, 1); assert.equal(req.transactions[name].accountInclude.length, 1500);
    assert.equal(req.transactions[name].vote, false); assert.equal(req.transactions[name].failed, false);
    const sig = randomBytes(64);
    const before = mock.calls.length;
    c.streams[0].emit("data", { filters: [name], transaction: { slot: "777", transaction: { signature: sig, isVote: false, index: "0",
      transaction: { signatures: [sig], message: { header: { numRequiredSignatures: 1, numReadonlySignedAccounts: 0, numReadonlyUnsignedAccounts: 1 }, accountKeys: [randomBytes(32), b58decode(accts[7])], recentBlockhash: randomBytes(32), instructions: [{ programIdIndex: 1, accounts: new Uint8Array([0]), data: Buffer.from([1, 2]) }], versioned: false, addressTableLookups: [] } },
      meta: { fee: "5000", preBalances: ["10", "0"], postBalances: ["5", "0"], innerInstructions: [], logMessages: ["Program log: hi"], preTokenBalances: [], postTokenBalances: [], loadedWritableAddresses: [], loadedReadonlyAddresses: [] } } } });
    await waitFor(() => msgs.some((m) => m.method === "transactionNotification"), 2000, "notification");
    const n = msgs.find((m) => m.method === "transactionNotification").params.result;
    assert.equal(n.signature, b58encode(sig)); assert.equal(n.slot, 777);
    assert.equal(n.transaction.meta.fee, 5000); assert.equal(n.transaction.transaction.message.accountKeys[1], accts[7]);
    assert.equal(n.transaction.transaction.message.instructions[0].data, b58encode(Buffer.from([1, 2])));
    assert.equal(mock.calls.slice(before).filter((x) => x.method === "getTransaction").length, 0, "no getTransaction round trip");
  } finally { ws.close(); await app.close(); }
});

// ---------------- Bubblegum ----------------
const borshStr = (s: string) => { const b = Buffer.from(s); const l = Buffer.alloc(4); l.writeUInt32LE(b.length); return Buffer.concat([l, b]); };
function metadataArgs(o: { name: string; symbol: string; uri: string; fee: number; collection?: string; creators: { address: string; verified: boolean; share: number }[] }) {
  const fee = Buffer.alloc(2); fee.writeUInt16LE(o.fee); const n = Buffer.alloc(4); n.writeUInt32LE(o.creators.length);
  return Buffer.concat([borshStr(o.name), borshStr(o.symbol), borshStr(o.uri), fee, Buffer.from([0, 1]), Buffer.from([0]), Buffer.from([1, 0]),
    o.collection ? Buffer.concat([Buffer.from([1, 0]), b58decode(o.collection)]) : Buffer.from([0]), Buffer.from([0]), Buffer.from([0]), n,
    ...o.creators.map((c) => Buffer.concat([b58decode(c.address), Buffer.from([c.verified ? 1 : 0, c.share])]))]);
}
const changeArgs = (nonce: number) => { const b = Buffer.alloc(108); randomBytes(96).copy(b); b.writeBigUInt64LE(BigInt(nonce), 96); b.writeUInt32LE(nonce, 104); return b; };
let slotN = 1000;
function bgTx(keys: string[], data: Buffer, accounts: number[], asInner = false) {
  const ix = { programIdIndex: keys.indexOf(BUBBLEGUM), accounts, data: b58encode(data) };
  return { slot: ++slotN, blockTime: 1, transaction: { signatures: [b58encode(randomBytes(64))], message: { accountKeys: keys, instructions: asInner ? [{ programIdIndex: 0, accounts: [], data: "" }] : [ix] } },
    meta: { err: null, fee: 5000, preBalances: keys.map(() => 0), postBalances: keys.map(() => 0), innerInstructions: asInner ? [{ index: 0, instructions: [ix] }] : [] } };
}
const keccak = (...p: Uint8Array[]) => Buffer.from(keccak_256(Buffer.concat(p)));
function naiveRoot(leaves: Buffer[], depth: number) {
  let level = Array.from({ length: 2 ** depth }, (_, i) => leaves[i] ?? Buffer.alloc(32));
  for (let d = 0; d < depth; d++) { const nx: Buffer[] = []; for (let i = 0; i < level.length; i += 2) nx.push(keccak(level[i], level[i + 1])); level = nx; }
  return level[0];
}

test("cnft: Bubblegum mint/transfer/burn -> leaves, merkle root/proofs, DAS getAsset/ByOwner/Proof/ByGroup/searchAssets, persistence", async () => {
  const TREE = pk(), AUTH = pk(), OWNER = pk(), OWNER2 = pk(), PAYER = pk(), COLL = pk(), CREATOR = pk();
  const file = path.join(tmp(), "cnft.json");
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl }, webhooks: false, chain: { detect: false }, cnft: { trees: [{ address: TREE, depth: 3 }], file } });
  const port = await app.server.listen(0);
  mock.responses.getTokenAccountsByOwner = () => ({ context: { slot: 1 }, value: [] });
  try {
    const idx = app.cnft;
    const md = metadataArgs({ name: "Leaf #0", symbol: "LEAF", uri: "https://x/0.json", fee: 250, creators: [{ address: CREATOR, verified: false, share: 100 }] });
    // mint_v1: tree_authority, leaf_owner, leaf_delegate, merkle_tree, payer, ...
    const k1 = [PAYER, AUTH, OWNER, TREE, BUBBLEGUM];
    idx.apply(bgTx(k1, Buffer.concat([DISC.mintV1, md]), [1, 2, 2, 3, 0]));
    // mint_to_collection_v1 via CPI (inner instruction); collection_mint at account 8
    const md1 = metadataArgs({ name: "Leaf #1", symbol: "LEAF", uri: "https://x/1.json", fee: 0, collection: COLL, creators: [] });
    const k2 = [PAYER, AUTH, OWNER, TREE, BUBBLEGUM, COLL];
    idx.apply(bgTx(k2, Buffer.concat([DISC.mintToCollectionV1, md1]), [1, 2, 2, 3, 0, 1, 1, 1, 5], true));
    const id0 = assetId(TREE, 0), id1 = assetId(TREE, 1);
    assert.equal(idx.assets.size, 2); assert.equal(idx.stats.mints, 2);
    const a0 = (await post(port, "getAsset", { id: id0 })).result;
    assert.equal(a0.content.metadata.name, "Leaf #0"); assert.equal(a0.compression.compressed, true); assert.equal(a0.compression.leaf_id, 0); assert.equal(a0.ownership.owner, OWNER);
    assert.equal(a0.royalty.basis_points, 250);
    // independent leaf hash + root
    const fee = Buffer.alloc(2); fee.writeUInt16LE(250);
    const dh = keccak(keccak(md), fee); assert.equal(a0.compression.data_hash, b58encode(dh));
    assert.equal(a0.compression.creator_hash, b58encode(keccak(b58decode(CREATOR), Buffer.from([0, 100]))));
    const leaf0 = keccak(Buffer.from([1]), b58decode(id0), b58decode(OWNER), b58decode(OWNER), Buffer.alloc(8), dh, keccak(b58decode(CREATOR), Buffer.from([0, 100])));
    const pr = (await post(port, "getAssetProof", { id: id0 })).result;
    assert.equal(pr.leaf, b58encode(leaf0)); assert.equal(pr.node_index, 8); assert.equal(pr.proof.length, 3); assert.equal(pr.tree_id, TREE);
    const leaf1 = b58decode((await post(port, "getAssetProof", { id: id1 })).result.leaf);
    assert.equal(pr.root, b58encode(naiveRoot([leaf0, leaf1], 3)), "root matches naive full-tree computation");
    assert.ok(MerkleTree.verify(leaf0, 0, pr.proof.map(b58decode), b58decode(pr.root)));
    // collection
    const g = (await post(port, "getAssetsByGroup", { groupKey: "collection", groupValue: COLL })).result;
    assert.equal(g.total, 1); assert.equal(g.items[0].id, id1); assert.deepEqual(g.items[0].grouping, [{ group_key: "collection", group_value: COLL }]);
    // transfer leaf 0 -> OWNER2: tree_authority, leaf_owner, leaf_delegate, new_leaf_owner, merkle_tree
    const k3 = [OWNER, AUTH, OWNER2, TREE, BUBBLEGUM];
    idx.apply(bgTx(k3, Buffer.concat([DISC.transfer, changeArgs(0)]), [1, 0, 0, 2, 3]));
    const by2 = (await post(port, "getAssetsByOwner", { ownerAddress: OWNER2 })).result;
    assert.equal(by2.total, 1); assert.equal(by2.items[0].id, id0);
    const pr2 = (await post(port, "getAssetProof", { id: id0 })).result;
    assert.notEqual(pr2.root, pr.root); assert.ok(MerkleTree.verify(b58decode(pr2.leaf), 0, pr2.proof.map(b58decode), b58decode(pr2.root)));
    const s = (await post(port, "searchAssets", { ownerAddress: OWNER, tokenType: "compressedNft" })).result;
    assert.deepEqual(s.items.map((x: any) => x.id), [id1]);
    assert.equal((await post(port, "searchAssets", { creatorAddress: CREATOR })).result.items[0].id, id0);
    // burn leaf 1: tree_authority, leaf_owner, leaf_delegate, merkle_tree
    idx.apply(bgTx([OWNER, AUTH, TREE, BUBBLEGUM], Buffer.concat([DISC.burn, changeArgs(1)]), [1, 0, 0, 2]));
    assert.equal((await post(port, "getAsset", { id: id1 })).result.burnt, true);
    assert.equal((await post(port, "searchAssets", { ownerAddress: OWNER, tokenType: "compressedNft" })).result.total, 0);
    assert.equal((await post(port, "searchAssets", { ownerAddress: OWNER, burnt: true })).result.total, 1);
    const pr3 = (await post(port, "getAssetProof", { id: id0 })).result;
    const leaf0b = b58decode(pr3.leaf);
    assert.equal(pr3.root, b58encode(naiveRoot([leaf0b], 3)), "burnt leaf is zeroed");
    // unknown tree ignored; failed tx ignored
    idx.apply(bgTx([PAYER, AUTH, OWNER, pk(), BUBBLEGUM], Buffer.concat([DISC.mintV1, md]), [1, 2, 2, 3, 0]));
    const failed = bgTx(k1, Buffer.concat([DISC.mintV1, md]), [1, 2, 2, 3, 0]); (failed.meta as any).err = { InstructionError: [0, "Custom"] }; idx.apply(failed);
    assert.equal(idx.assets.size, 2);
    // persistence
    idx.save();
    const re = new CnftIndexer({ trees: [{ address: TREE, depth: 3 }], file });
    assert.equal(re.assets.size, 2); assert.equal(re.proof(id0)!.root, pr3.root); assert.equal(re.trees.get(TREE)!.nextNonce, 2);
  } finally { await app.close(); delete mock.responses.getTokenAccountsByOwner; }
});

// ---------------- Enhanced ----------------
const USDC = "EPjFWyBX7rUaT5VFPhmHN8WYZRzkaVeuVwbNPcdDDDWE"; // shape only
const tok = (accountIndex: number, mint: string, owner: string, amount: string, decimals: number) => ({ accountIndex, mint, owner, uiTokenAmount: { amount, decimals, uiAmount: Number(amount) / 10 ** decimals, uiAmountString: "" } });
function jtx(o: { keys: string[]; ix?: any[]; pre?: number[]; post?: number[]; preT?: any[]; postT?: any[]; slot?: number; err?: any }) {
  return { slot: o.slot ?? 100, blockTime: 1700000000, transaction: { signatures: [b58encode(randomBytes(64))], message: { accountKeys: o.keys, instructions: o.ix ?? [] } },
    meta: { err: o.err ?? null, fee: 5000, preBalances: o.pre ?? o.keys.map(() => 0), postBalances: o.post ?? o.keys.map(() => 0), innerInstructions: [], preTokenBalances: o.preT ?? [], postTokenBalances: o.postT ?? [] } };
}
const sysXfer = (keys: string[], from: number, to: number, lamports: number) => { const d = Buffer.alloc(12); d.writeUInt32LE(2); d.writeBigUInt64LE(BigInt(lamports), 4); return { programIdIndex: keys.indexOf(SYSTEM_PROGRAM), accounts: [from, to], data: b58encode(d) }; };
const W1 = pk(), W2 = pk(), MINTX = pk(), NFTM = pk(), TA1 = pk(), TA2 = pk(), TA3 = pk(), TA4 = pk();
const transferTx = (slot = 100) => { const keys = [W1, W2, SYSTEM_PROGRAM]; return jtx({ slot, keys, ix: [sysXfer(keys, 0, 1, 500_000_000)], pre: [2e9, 0, 1], post: [2e9 - 5e8 - 5000, 5e8, 1] }); };
const swapTx = (slot = 101) => jtx({ slot, keys: [W1, TA1, TA2, TA3, TA4, "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"], ix: [{ programIdIndex: 5, accounts: [0, 1, 2], data: "" }],
  preT: [tok(1, USDC, W1, "10000000", 6), tok(2, MINTX, W1, "0", 9), tok(3, USDC, "pool", "0", 6), tok(4, MINTX, "pool", "5000000000", 9)],
  postT: [tok(1, USDC, W1, "0", 6), tok(2, MINTX, W1, "2000000000", 9), tok(3, USDC, "pool", "10000000", 6), tok(4, MINTX, "pool", "3000000000", 9)] });
const saleTx = () => jtx({ keys: [W2, W1, TA1, TA2, "M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K"], ix: [{ programIdIndex: 4, accounts: [0, 1], data: "" }],
  pre: [3e9, 1e9, 0, 0, 1], post: [3e9 - 1e9 - 5000, 2e9, 0, 0, 1],
  preT: [tok(2, NFTM, W1, "1", 0), tok(3, NFTM, W2, "0", 0)], postT: [tok(2, NFTM, W1, "0", 0), tok(3, NFTM, W2, "1", 0)] });

test("enhanced parser: TRANSFER, SWAP (heuristic), NFT_SALE, compressed + UNKNOWN", () => {
  const t = parseEnhanced(transferTx());
  assert.equal(t.type, "TRANSFER"); assert.equal(t.source, "SYSTEM_PROGRAM"); assert.deepEqual(t.nativeTransfers, [{ fromUserAccount: W1, toUserAccount: W2, amount: 500_000_000 }]);
  assert.match(t.description, /transferred 0.5 SOL/); assert.equal(t.feePayer, W1); assert.equal(t.accountData[1].nativeBalanceChange, 5e8);
  const s = parseEnhanced(swapTx());
  assert.equal(s.type, "SWAP"); assert.equal(s.source, "JUPITER");
  assert.deepEqual(s.events.swap.tokenInputs, [{ userAccount: W1, mint: USDC, tokenAmount: 10 }]); assert.deepEqual(s.events.swap.tokenOutputs, [{ userAccount: W1, mint: MINTX, tokenAmount: 2 }]);
  assert.equal(s.tokenTransfers.length, 2);
  const n = parseEnhanced(saleTx());
  assert.equal(n.type, "NFT_SALE"); assert.equal(n.source, "MAGIC_EDEN");
  assert.equal(n.events.nft.amount, 1e9); assert.equal(n.events.nft.buyer, W2); assert.equal(n.events.nft.seller, W1); assert.equal(n.events.nft.nfts[0].mint, NFTM);
  assert.equal(parseEnhanced(jtx({ keys: [W1, pk()], ix: [{ programIdIndex: 1, accounts: [0], data: "" }] })).type, "UNKNOWN");
  const keys = [W1, pk(), pk(), pk(), BUBBLEGUM];
  assert.equal(parseEnhanced(jtx({ keys, ix: [{ programIdIndex: 4, accounts: [1, 2, 2, 3], data: b58encode(Buffer.concat([DISC.mintV1, Buffer.alloc(4)])) }] })).type, "COMPRESSED_NFT_MINT");
});

async function receiver() {
  const got: any[] = [];
  const srv = http.createServer((req, res) => { let b = ""; req.on("data", (c) => (b += c)); req.on("end", () => { got.push(JSON.parse(b)); res.end("ok"); }); });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  return { got, url: `http://127.0.0.1:${(srv.address() as any).port}/hook`, close: () => new Promise((r) => srv.close(r)) };
}

test("enhanced webhooks: transactionTypes filter, enhanced payload, persisted stats + restart backfill from lastSlot", async () => {
  const rv = await receiver();
  const file = path.join(tmp(), "wh.json");
  const txs = new Map<string, any>();
  const hub = new StreamHub({ wsUrl: mock.wsUrl, lingerMs: 0 });
  const getTx = async (sig: string) => txs.get(sig) ?? null;
  const m = new WebhookManager(hub, getTx, { file, baseDelayMs: 10 });
  try {
    assert.throws(() => m.create("k", { webhookURL: rv.url, accountAddresses: [W1], webhookType: "enhanced", transactionTypes: ["NOPE"] }), /unsupported transactionTypes/);
    const h = m.create("k", { webhookURL: rv.url, accountAddresses: [W1], webhookType: "enhanced", transactionTypes: ["SWAP", "NFT_SALE"] });
    await waitFor(() => mock.activeSubs("logsSubscribe") > 0 && hub.stats().some((t) => t.key.includes(W1)), 3000); await sleep(100);
    const t1 = transferTx(200), t2 = swapTx(201);
    for (const t of [t1, t2]) { txs.set(t.transaction.signatures[0], t); mock.emit("logsSubscribe", { context: { slot: t.slot }, value: { signature: t.transaction.signatures[0], err: null, logs: [] } }); }
    await waitFor(() => m.stats.get(h.webhookID)?.delivered === 1, 3000, "delivery"); await sleep(150);
    assert.equal(rv.got.length, 1, "TRANSFER filtered out");
    const p = rv.got[0][0]; assert.equal(p.type, "SWAP"); assert.equal(p.signature, t2.transaction.signatures[0]); assert.ok(p.events.swap); assert.ok(Array.isArray(p.tokenTransfers));
    assert.equal(m.stats.get(h.webhookID)!.lastSlot, 201);
    m.close(); await sleep(50);
    const state = JSON.parse(fs.readFileSync(`${file}.state.json`, "utf8"));
    assert.equal(state[h.webhookID].delivered, 1); assert.equal(state[h.webhookID].lastSlot, 201);
    // restart: missed txs while down; rpc returns history newest-first
    const missed = [saleTx(), swapTx(250), transferTx(260)]; (missed[0] as any).slot = 240;
    const old = swapTx(150);
    const db = new Map([...missed, old, t2].map((t) => [t.transaction.signatures[0], t]));
    const rpcCalls: string[] = [];
    const rpc = async (method: string, params: any[]) => {
      rpcCalls.push(method);
      if (method === "getSignaturesForAddress") return [...db.values()].sort((a, b) => b.slot - a.slot).map((t) => ({ signature: t.transaction.signatures[0], slot: t.slot, err: null }));
      if (method === "getTransaction") return db.get(params[0]);
    };
    const m2 = new WebhookManager(hub, getTx, { file, rpc, baseDelayMs: 10 });
    await m2.ready; await m2.idle();
    assert.deepEqual(rv.got.slice(1).map((b) => [b[0].type, b[0].slot]), [["NFT_SALE", 240], ["SWAP", 250]], "only slot > lastSlot, filtered, oldest first");
    assert.equal(rpcCalls.filter((x) => x === "getTransaction").length, 3);
    const st = m2.stats.get(h.webhookID)!;
    assert.equal(st.delivered, 3, "stats carried across restart"); assert.equal(st.lastSlot, 250); assert.equal(st.backfilled, 2);
    m2.close();
  } finally { m.close(); hub.close(); await rv.close(); }
});
