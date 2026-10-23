import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { randomBytes } from "node:crypto";
import { keccak_256 } from "@noble/hashes/sha3.js";
import {
  b58encode, b58decode, GeyserUpstream, StreamHub, TxWatcher, CnftIndexer, MerkleTree, DISC, BUBBLEGUM, assetId, leafHash, hashCreators, parseEnhanced,
  WebhookManager, decodeTxError, encodeTxError, TxRecorder, JsonFetcher, CollectionIndex, collectionOffsets, createDas, treeConfigPda, SYSTEM_PROGRAM, isPrivateIp,
} from "../src/index.js";
import { startMockSolana, waitFor, sleep } from "./helpers.js";
import { startMockGeyser } from "./mock-geyser.js";

const pk = () => b58encode(randomBytes(32));
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), "fastrpc-c-"));
const keccak = (...p: Uint8Array[]) => Buffer.from(keccak_256(Buffer.concat(p)));
let mock: Awaited<ReturnType<typeof startMockSolana>>;
before(async () => { mock = await startMockSolana(); });
after(async () => { await mock.close(); });

// ---------------- tx errors ----------------
test("txerror: bincode TransactionError -> JSON-RPC shape (roundtrip)", () => {
  for (const e of ["AccountInUse", "BlockhashNotFound", { InstructionError: [2, { Custom: 6001 }] }, { InstructionError: [0, "InsufficientFunds"] },
    { DuplicateInstruction: 3 }, { InsufficientFundsForRent: { account_index: 4 } }, { ProgramExecutionTemporarilyRestricted: { account_index: 1 } }])
    assert.deepEqual(decodeTxError(encodeTxError(e)), e);
  assert.equal(decodeTxError(new Uint8Array()), null);
});

// ---------------- Geyser: real yellowstone client <-> local gRPC server ----------------
const u8 = (s: string) => b58decode(s);
function gTx(slot: number, keys: string[], err?: any) {
  return { slot: String(slot), transaction: { signature: randomBytes(64), isVote: false, index: "0",
    transaction: { signatures: [randomBytes(64)], message: { header: { numRequiredSignatures: 1, numReadonlySignedAccounts: 0, numReadonlyUnsignedAccounts: 1 }, accountKeys: keys.map(u8), recentBlockhash: randomBytes(32), instructions: [{ programIdIndex: keys.length - 1, accounts: new Uint8Array([0]), data: new Uint8Array([1, 2]) }], versioned: false, addressTableLookups: [] } },
    meta: { err: err ? { err: encodeTxError(err) } : undefined, fee: "5000", preBalances: keys.map(() => "1"), postBalances: keys.map(() => "1"), innerInstructions: [], innerInstructionsNone: false, logMessages: ["Program log: hi"], logMessagesNone: false, preTokenBalances: [], postTokenBalances: [], rewards: [], loadedWritableAddresses: [], loadedReadonlyAddresses: [], computeUnitsConsumed: "1234" } } };
}

test("geyser: real @triton-one/yellowstone-grpc client against local gRPC Geyser mock: x-token, filters, decoded err, blockTime, slots, reconnect", async () => {
  const srv = await startMockGeyser("secret");
  const g = new GeyserUpstream({ url: srv.url, token: "secret", fallback: { subscribe: () => () => {} } as any, blockTimeWaitMs: 300, minBackoffMs: 50 });
  try {
    const A = pk(), P = pk();
    const got: any[] = []; const slots: any[] = [];
    const un = g.subscribeTransactions({ accountInclude: [A] }, "confirmed", (r) => got.push(r));
    g.subscribe("slotSubscribe", [], (r) => slots.push(r));
    await waitFor(() => srv.calls.length >= 1 && srv.calls.some((c) => c.requests.some((r) => Object.keys(r.transactions ?? {}).length)), 8000, "subscribe request");
    const txCall = srv.calls.find((c) => c.requests.some((r) => Object.keys(r.transactions ?? {}).length))!;
    const req = txCall.requests.filter((r) => Object.keys(r.transactions ?? {}).length).at(-1);
    assert.deepEqual(srv.st.tokens.filter(Boolean).every((t) => t === "secret"), true, "x-token sent");
    const f = Object.values<any>(req.transactions)[0]; assert.deepEqual(f.accountInclude, [A]); assert.equal(f.vote, false); assert.equal(req.commitment, 1);
    assert.ok(Object.keys(req.blocksMeta).length, "blocksMeta requested for blockTime: " + JSON.stringify(txCall.requests));
    const fname = Object.keys(req.transactions)[0];
    // blockMeta first, then tx in that block -> immediate with blockTime
    txCall.call.write({ filters: ["bm"], blockMeta: { slot: "500", blockhash: "x", blockTime: { timestamp: "1700000500" }, parentSlot: "499", parentBlockhash: "y", executedTransactionCount: "1", entriesCount: "1" }, createdAt: new Date() });
    txCall.call.write({ filters: [fname], transaction: gTx(500, [A, P], { InstructionError: [0, { Custom: 6001 }] }), createdAt: new Date() });
    // tx whose blockMeta arrives later -> buffered then patched
    txCall.call.write({ filters: [fname], transaction: gTx(501, [A, P]), createdAt: new Date() });
    await sleep(50);
    txCall.call.write({ filters: ["bm"], blockMeta: { slot: "501", blockhash: "x", blockTime: { timestamp: "1700000501" }, parentSlot: "500", parentBlockhash: "y", executedTransactionCount: "1", entriesCount: "1" }, createdAt: new Date() });
    // tx with no blockMeta -> emitted with null after wait
    txCall.call.write({ filters: [fname], transaction: gTx(502, [A, P]), createdAt: new Date() });
    await waitFor(() => got.length === 3, 5000, "3 txs");
    assert.equal(got[0].blockTime, 1700000500); assert.deepEqual(got[0].meta.err, { InstructionError: [0, { Custom: 6001 }] }); assert.deepEqual(got[0].meta.status, { Err: { InstructionError: [0, { Custom: 6001 }] } });
    assert.equal(got[0].transaction.message.accountKeys[0], A); assert.equal(got[0].meta.computeUnitsConsumed, 1234); assert.deepEqual(got[0].meta.logMessages, ["Program log: hi"]);
    assert.equal(got[1].blockTime, 1700000501); assert.equal(got[1].meta.err, null);
    assert.equal(got[2].blockTime, null); assert.equal(got[2].slot, 502);
    // slots on their own (processed) stream
    const slotCall = srv.calls.find((c) => c.requests.some((r) => Object.keys(r.slots ?? {}).length))!;
    const sname = Object.keys(slotCall.requests.filter((r) => Object.keys(r.slots ?? {}).length).at(-1).slots)[0];
    slotCall.call.write({ filters: [sname], slot: { slot: "77", parent: "76", status: 0 }, createdAt: new Date() });
    await waitFor(() => slots.length === 1, 3000, "slot"); assert.deepEqual(slots[0], { slot: 77, parent: 76, root: 0 });
    // server kills streams -> client reconnects and re-sends filters
    srv.kill(); assert.equal(srv.calls.length, 0);
    await waitFor(() => srv.calls.some((c) => c.requests.some((r) => Object.keys(r.transactions ?? {}).length)) && srv.calls.some((c) => c.requests.some((r) => Object.keys(r.slots ?? {}).length)), 8000, "resubscribe");
    un();
  } finally { g.close(); await srv.close(); }
});

test("geyser: bad x-token is rejected by the server and retried (no crash)", async () => {
  const srv = await startMockGeyser("secret");
  const logs: string[] = [];
  const g = new GeyserUpstream({ url: srv.url, token: "wrong", fallback: { subscribe: () => () => {} } as any, minBackoffMs: 50, log: (...a) => logs.push(a.join(" ")) });
  try {
    g.subscribe("slotSubscribe", [], () => {});
    await waitFor(() => srv.st.tokens.length >= 2, 8000, "retries");
    assert.ok(srv.st.tokens.every((t) => t === "wrong")); assert.equal(srv.calls.length, 0);
  } finally { g.close(); await srv.close(); }
});

test("geyser recorder: hourly JSONL, 24h retention, transactionSubscribe fromSlot replay for any filter then live", async () => {
  const dir = tmp(); let now = Date.UTC(2026, 0, 1, 0, 0);
  const rec = new TxRecorder(dir, 24 * 3600_000, () => now);
  const A = pk(), B = pk();
  const mk = (slot: number, keys: string[], failed = false) => ({ slot, transaction: { signatures: [b58encode(randomBytes(64))], message: { accountKeys: keys } }, meta: { err: failed ? "X" : null, loadedAddresses: { writable: [], readonly: [] } } });
  const rows = [mk(10, [A]), mk(11, [B]), mk(12, [A, B], true), mk(13, [A])];
  for (const r of rows.slice(0, 2)) rec.record({ slot: r.slot, sig: r.transaction.signatures[0], keys: r.transaction.message.accountKeys, failed: !!r.meta.err, vote: false, tx: r });
  now += 2 * 3600_000;
  for (const r of rows.slice(2)) rec.record({ slot: r.slot, sig: r.transaction.signatures[0], keys: r.transaction.message.accountKeys, failed: !!r.meta.err, vote: false, tx: r });
  rec.record({ slot: 13, sig: rows[3].transaction.signatures[0], keys: [], failed: false, vote: false, tx: {} }); // dup ignored
  assert.equal(rec.segments().length, 2); assert.equal(rec.stats.written, 4);
  const all: number[] = []; for await (const r of rec.replay(0)) all.push(r.slot); assert.deepEqual(all, [10, 11, 12, 13]);
  // watcher over a hub whose upstream exposes subscribeTransactions + recorder
  const live: any[] = [];
  const up: any = { recorder: rec, subscribe: () => () => {}, subscribeTransactions: (_f: any, _c: any, cb: any) => { live.push(cb); return () => {}; } };
  const hub = new StreamHub({ upstream: up, lingerMs: 0 });
  const seen: number[] = [];
  const w = new TxWatcher(hub, async () => null, { accountInclude: [A], failed: false }, { fromSlot: 11 }, (e) => seen.push(e.slot));
  await waitFor(() => seen.length === 1, 3000);
  live[0]({ ...mk(14, [A]), isVote: false }); live[0]({ ...rows[3], isVote: false }); // live dup of replayed sig ignored
  await sleep(20); assert.deepEqual(seen, [13, 14], "replay (filtered: A, not failed, slot>=11) then live, deduped");
  w.stop();
  // retention: 25h later the first segment is pruned
  now += 23 * 3600_000; rec.prune(); assert.equal(rec.segments().length, 1);
  const left: number[] = []; for await (const r of rec.replay(0)) left.push(r.slot); assert.deepEqual(left, [12, 13]);
  rec.close(); hub.close();
});

// ---------------- Bubblegum: noop events ----------------
const NOOP = "noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV", CMP = "cmtDvXumGCrqC1Age74AVPhSRVXJMd8PJS91L8KbNCK";
const u32 = (n: number) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b; }; const u64 = (n: number) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b; };
const changelogEv = (tree: string, depth: number, index: number, leaf: Buffer, seq: number) => Buffer.concat([Buffer.from([0, 0]), b58decode(tree), u32(depth + 1),
  Buffer.concat([leaf, u32(2 ** depth + index)]), ...Array.from({ length: depth }, (_, l) => Buffer.concat([Buffer.alloc(32), u32((2 ** depth + index) >> (l + 1))])), u64(seq), u32(index)]);
function leafEv(o: { id: string; owner: string; delegate: string; nonce: number; dh: Buffer; ch: Buffer; v2?: { flags: number } }, lh: Buffer) {
  const d = Buffer.concat([Buffer.from([1, o.v2 ? 1 : 0, o.v2 ? 1 : 0]), b58decode(o.id), b58decode(o.owner), b58decode(o.delegate), u64(o.nonce), o.dh, o.ch,
    ...(o.v2 ? [Buffer.alloc(32, 7), Buffer.alloc(32, 8), Buffer.from([o.v2.flags])] : []), lh]);
  return Buffer.concat([Buffer.from([1, 0]), u32(d.length), d]);
}
const borshStr = (s: string) => Buffer.concat([u32(Buffer.byteLength(s)), Buffer.from(s)]);
function mdArgs(o: { name: string; uri: string; creators: { address: string; verified: boolean; share: number }[] }) {
  return Buffer.concat([borshStr(o.name), borshStr("SYM"), borshStr(o.uri), Buffer.from([100, 0]), Buffer.from([0, 1]), Buffer.from([0]), Buffer.from([1, 0]), Buffer.from([0]), Buffer.from([0]), Buffer.from([0]),
    u32(o.creators.length), ...o.creators.map((c) => Buffer.concat([b58decode(c.address), Buffer.from([c.verified ? 1 : 0, c.share])]))]);
}
let slotN = 5000;
/** top-level Bubblegum ix + inner noop events, like a real tx */
function evTx(keys: string[], data: Buffer, accounts: number[], events: Buffer[]) {
  const all = [...keys, NOOP, CMP].filter((k, i, a) => a.indexOf(k) === i);
  const ni = all.indexOf(NOOP), ci = all.indexOf(CMP);
  const inner = events.flatMap((e) => e[0] === 0 ? [{ programIdIndex: ci, accounts: [], data: "" }, { programIdIndex: ni, accounts: [], data: b58encode(e) }] : [{ programIdIndex: ni, accounts: [], data: b58encode(e) }]);
  return { slot: ++slotN, blockTime: 1, transaction: { signatures: [b58encode(randomBytes(64))], message: { accountKeys: all, instructions: [{ programIdIndex: all.indexOf(BUBBLEGUM), accounts, data: b58encode(data) }] } },
    meta: { err: null, fee: 5000, preBalances: all.map(() => 0), postBalances: all.map(() => 0), innerInstructions: [{ index: 0, instructions: inner }] } };
}
function naiveRoot(leaves: Map<number, Buffer>, depth: number) {
  let level = Array.from({ length: 2 ** depth }, (_, i) => leaves.get(i) ?? Buffer.alloc(32));
  for (let d = 0; d < depth; d++) { const nx: Buffer[] = []; for (let i = 0; i < level.length; i += 2) nx.push(keccak(level[i], level[i + 1])); level = nx; }
  return level[0];
}

test("cnft events: non-sequential mint, delegate, verify_creator, update_metadata, redeem/cancel_redeem, burn, V2 flags; real seq; syncTree", async () => {
  const TREE = pk(), AUTH = pk(), OWNER = pk(), DELEG = pk(), PAYER = pk(), CREATOR = pk(), D = 3;
  const idx = new CnftIndexer({ trees: [{ address: TREE, depth: D }] });
  const leaves = new Map<number, Buffer>();
  const md = mdArgs({ name: "A", uri: "https://x/a.json", creators: [{ address: CREATOR, verified: false, share: 100 }] });
  const id0 = assetId(TREE, 7); const dh = keccak(Buffer.from("dh0")); const ch = hashCreators([{ address: CREATOR, verified: false, share: 100 }]);
  let lh = leafHash(id0, OWNER, OWNER, 7, dh, ch);
  // mint: nonce 7 lands at leaf index 5, seq 40 (tree had prior history)
  idx.apply(evTx([PAYER, AUTH, OWNER, TREE, BUBBLEGUM], Buffer.concat([DISC.mintV1, md]), [1, 2, 2, 3, 0], [leafEv({ id: id0, owner: OWNER, delegate: OWNER, nonce: 7, dh, ch }, lh), changelogEv(TREE, D, 5, lh, 40)]));
  leaves.set(5, lh);
  let a = idx.assets.get(id0)!;
  assert.equal(a.leafIndex, 5); assert.equal(a.nonce, 7); assert.equal(a.seq, 40); assert.equal(idx.trees.get(TREE)!.seq, 40); assert.equal(a.metadata.name, "A");
  assert.equal(idx.proof(id0)!.node_index, 2 ** D + 5); assert.equal(idx.proof(id0)!.root, b58encode(naiveRoot(leaves, D)));
  // delegate (new delegate acc 3, tree acc 4)
  lh = leafHash(id0, OWNER, DELEG, 7, dh, ch);
  idx.apply(evTx([OWNER, AUTH, OWNER, DELEG, TREE, BUBBLEGUM], Buffer.concat([DISC.delegate, Buffer.alloc(108)]), [1, 0, 0, 3, 4], [leafEv({ id: id0, owner: OWNER, delegate: DELEG, nonce: 7, dh, ch }, lh), changelogEv(TREE, D, 5, lh, 41)]));
  leaves.set(5, lh); a = idx.assets.get(id0)!;
  assert.equal(a.delegate, DELEG); assert.equal(idx.toDas(a).ownership.delegated, true); assert.equal(idx.stats.delegates, 1);
  // verify_creator: creator at acc 5; args = 108 bytes + MetadataArgs
  const ch2 = hashCreators([{ address: CREATOR, verified: true, share: 100 }]); lh = leafHash(id0, OWNER, DELEG, 7, dh, ch2);
  idx.apply(evTx([AUTH, OWNER, DELEG, TREE, PAYER, CREATOR, BUBBLEGUM], Buffer.concat([DISC.verifyCreator, Buffer.alloc(108), md]), [0, 1, 2, 3, 4, 5], [leafEv({ id: id0, owner: OWNER, delegate: DELEG, nonce: 7, dh, ch: ch2 }, lh), changelogEv(TREE, D, 5, lh, 42)]));
  leaves.set(5, lh); a = idx.assets.get(id0)!;
  assert.equal(a.metadata.creators[0].verified, true); assert.equal(a.creatorHash, b58encode(ch2));
  // update_metadata: tree at acc 8; args = root(32)+nonce(8)+index(4) + current MetadataArgs + UpdateArgs(name=Some("B"), rest None)
  const md2 = mdArgs({ name: "A", uri: "https://x/a.json", creators: [{ address: CREATOR, verified: true, share: 100 }] });
  const upd = Buffer.concat([Buffer.from([1]), borshStr("B"), Buffer.from([0, 1]), borshStr("https://x/b.json"), Buffer.from([0, 0, 0, 0])]);
  const dh2 = keccak(Buffer.from("dh1")); lh = leafHash(id0, OWNER, DELEG, 7, dh2, ch2);
  idx.apply(evTx([AUTH, OWNER, DELEG, PAYER, TREE, BUBBLEGUM], Buffer.concat([DISC.updateMetadata, Buffer.alloc(44), md2, upd]), [0, 0, 0, 0, 0, 1, 2, 3, 4], [leafEv({ id: id0, owner: OWNER, delegate: DELEG, nonce: 7, dh: dh2, ch: ch2 }, lh), changelogEv(TREE, D, 5, lh, 43)]));
  leaves.set(5, lh); a = idx.assets.get(id0)!;
  assert.equal(a.metadata.name, "B"); assert.equal(a.metadata.uri, "https://x/b.json"); assert.equal(a.metadata.symbol, "SYM"); assert.equal(a.dataHash, b58encode(dh2));
  // redeem: leaf -> zero (no leaf event), then cancel_redeem restores it
  idx.apply(evTx([OWNER, TREE, BUBBLEGUM], Buffer.concat([DISC.redeem, Buffer.alloc(108)]), [0, 0, 0, 0, 1], [changelogEv(TREE, D, 5, Buffer.alloc(32), 44)]));
  a = idx.assets.get(id0)!; assert.equal(a.redeemed, true); assert.equal(a.burnt, false); assert.equal(idx.toDas(a).compression.asset_hash, b58encode(Buffer.alloc(32)));
  idx.apply(evTx([OWNER, TREE, BUBBLEGUM], Buffer.concat([DISC.cancelRedeem, Buffer.alloc(32)]), [0, 0, 1], [changelogEv(TREE, D, 5, lh, 45)]));
  a = idx.assets.get(id0)!; assert.equal(a.redeemed, false); assert.equal(idx.proof(id0)!.root, b58encode(naiveRoot(leaves, D)));
  // V2 mint at index 2 with frozen flag
  const id1 = assetId(TREE, 8); const lh1 = keccak(Buffer.from("v2leaf"));
  const md2v = Buffer.concat([borshStr("V2"), borshStr("S"), borshStr("u"), Buffer.from([0, 0]), Buffer.from([0, 1]), Buffer.from([0]), u32(0), Buffer.from([0])]);
  idx.apply(evTx([PAYER, AUTH, OWNER, TREE, BUBBLEGUM], Buffer.concat([DISC.mintV2, md2v]), [1, 2, 2, 3], [leafEv({ id: id1, owner: OWNER, delegate: OWNER, nonce: 8, dh, ch, v2: { flags: 1 } }, lh1), changelogEv(TREE, D, 2, lh1, 46)]));
  leaves.set(2, lh1);
  const d1 = idx.toDas(idx.assets.get(id1)!);
  assert.equal(d1.content.metadata.name, "V2"); assert.equal(d1.ownership.frozen, true); assert.equal((d1.compression as any).flags, 1); assert.ok((d1.compression as any).collection_hash);
  assert.equal(idx.proof(id1)!.root, b58encode(naiveRoot(leaves, D)));
  // burn
  idx.apply(evTx([OWNER, TREE, BUBBLEGUM], Buffer.concat([DISC.burn, Buffer.alloc(108)]), [0, 0, 0, 1], [changelogEv(TREE, D, 5, Buffer.alloc(32), 47)]));
  leaves.delete(5);
  assert.equal(idx.assets.get(id0)!.burnt, true); assert.equal(idx.proof(id1)!.root, b58encode(naiveRoot(leaves, D)));
  assert.equal(idx.stats.seqGaps, 0); assert.equal(idx.trees.get(TREE)!.seq, 47);
  // gap detection
  idx.apply(evTx([OWNER, TREE, BUBBLEGUM], Buffer.concat([DISC.burn, Buffer.alloc(108)]), [0, 0, 0, 1], [changelogEv(TREE, D, 2, Buffer.alloc(32), 50)]));
  assert.equal(idx.stats.seqGaps, 1); leaves.delete(2);
  // syncTree: on-chain header + active changelog root + TreeConfig.num_minted
  const maxBuf = 8, clSize = 32 + 32 * D + 8;
  const acct = Buffer.alloc(80 + maxBuf * clSize + 32 * D + 32 + 8);
  acct[0] = 1; acct.writeUInt32LE(maxBuf, 2); acct.writeUInt32LE(D, 6); acct.writeBigUInt64LE(50n, 56); acct.writeBigUInt64LE(3n, 64); acct.writeBigUInt64LE(8n, 72);
  naiveRoot(leaves, D).copy(acct, 80 + 3 * clSize);
  const cfg = Buffer.alloc(96); cfg.writeBigUInt64LE(9n, 80);
  const rpc = async (m: string, p: any[]) => ({ value: { data: [(p[0] === TREE ? acct : p[0] === treeConfigPda(TREE) ? cfg : Buffer.alloc(0)).toString("base64"), "base64"] } });
  const s = await idx.syncTree(rpc, TREE);
  assert.deepEqual([s.depth, s.chainSeq, s.localSeq, s.numMinted, s.inSync, s.behind], [3, 50, 50, 9, true, 0]);
  acct.writeBigUInt64LE(52n, 56); const s2 = await idx.syncTree(rpc, TREE); assert.equal(s2.inSync, false); assert.equal(s2.behind, 2);
});

// ---------------- enhanced parser additions ----------------
const TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", STAKE = "Stake11111111111111111111111111111111111111";
const anchorDisc = (n: string) => Buffer.from(require_sha(`global:${n}`)).subarray(0, 8);
import { createHash } from "node:crypto";
function require_sha(s: string) { return createHash("sha256").update(s).digest(); }
const tok = (accountIndex: number, mint: string, owner: string, amount: string, decimals: number) => ({ accountIndex, mint, owner, uiTokenAmount: { amount, decimals } });
function jtx(o: { keys: string[]; ix: any[]; inner?: any[]; pre?: number[]; post?: number[]; preT?: any[]; postT?: any[] }) {
  return { slot: 1, blockTime: 2, transaction: { signatures: [b58encode(randomBytes(64))], message: { accountKeys: o.keys, instructions: o.ix } },
    meta: { err: null, fee: 5000, preBalances: o.pre ?? o.keys.map(() => 0), postBalances: o.post ?? o.keys.map(() => 0), innerInstructions: o.inner ? [{ index: 0, instructions: o.inner }] : [], preTokenBalances: o.preT ?? [], postTokenBalances: o.postT ?? [] } };
}
const ix = (keys: string[], prog: string, accounts: number[], data: Buffer) => ({ programIdIndex: keys.indexOf(prog), accounts, data: b58encode(data) });

test("enhanced: TOKEN_MINT, BURN, STAKE_SOL/UNSTAKE_SOL/WITHDRAW, CREATE_ACCOUNT, NFT_LISTING/CANCEL/BID, Jupiter + Raydium swaps with token in/out", () => {
  const W = pk(), M = pk(), TA = pk(), TB = pk(), V = pk(), S = pk(), N = pk(), M2 = pk();
  // mint_to_checked 1000 (6 dp)
  let k = [W, M, TA, TOKEN];
  let e = parseEnhanced(jtx({ keys: k, ix: [ix(k, TOKEN, [1, 2, 0], Buffer.concat([Buffer.from([14]), u64(1_000_000_000), Buffer.from([6])]))], postT: [tok(2, M, W, "1000000000", 6)] }));
  assert.equal(e.type, "TOKEN_MINT"); assert.deepEqual(e.events.tokenMint[0], { mint: M, toTokenAccount: TA, toUserAccount: W, tokenAmount: 1000 });
  e = parseEnhanced(jtx({ keys: k, ix: [ix(k, TOKEN, [2, 1, 0], Buffer.concat([Buffer.from([8]), u64(5_000_000)]))], preT: [tok(2, M, W, "10000000", 6)], postT: [tok(2, M, W, "5000000", 6)] }));
  assert.equal(e.type, "BURN"); assert.equal(e.events.burn[0].tokenAmount, 5); assert.equal(e.events.burn[0].fromUserAccount, W);
  k = [W, S, V, STAKE];
  assert.equal(parseEnhanced(jtx({ keys: k, ix: [ix(k, STAKE, [1, 2], u32(2))] })).type, "STAKE_SOL");
  assert.equal(parseEnhanced(jtx({ keys: k, ix: [ix(k, STAKE, [1, 2], u32(2))] })).events.stake.voteAccount, V);
  assert.equal(parseEnhanced(jtx({ keys: k, ix: [ix(k, STAKE, [1], u32(5))] })).type, "UNSTAKE_SOL");
  const wd = parseEnhanced(jtx({ keys: k, ix: [ix(k, STAKE, [1, 0], Buffer.concat([u32(4), u64(2e9)]))] })); assert.equal(wd.type, "WITHDRAW"); assert.equal(wd.events.stake.amount, 2e9);
  k = [W, N, SYSTEM_PROGRAM];
  const ca = parseEnhanced(jtx({ keys: k, ix: [ix(k, SYSTEM_PROGRAM, [0, 1], Buffer.concat([u32(0), u64(2039280), u64(165), b58decode(TOKEN)]))] }));
  assert.equal(ca.type, "CREATE_ACCOUNT"); assert.deepEqual(ca.events.createAccount[0], { account: N, lamports: 2039280, space: 165, owner: TOKEN });
  // marketplaces
  const ME = "M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K", TBID = "TB1Dqt8JeKQh7RLDzfYDJsq8KS4fS2yt87avRjyRxMv";
  k = [W, M, ME, TBID];
  assert.equal(parseEnhanced(jtx({ keys: k, ix: [ix(k, ME, [0, 1], Buffer.concat([anchorDisc("sell"), u64(1)]))] })).type, "NFT_LISTING");
  assert.equal(parseEnhanced(jtx({ keys: k, ix: [ix(k, ME, [0, 1], anchorDisc("cancel_sell"))] })).type, "NFT_CANCEL_LISTING");
  const bid = parseEnhanced(jtx({ keys: k, ix: [ix(k, TBID, [0, 1], anchorDisc("bid"))] })); assert.equal(bid.type, "NFT_BID"); assert.equal(bid.source, "TENSOR");
  // Jupiter route with SwapEvent CPI events (2 hops)
  const JUP = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", RAY = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", WHIRL = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
  const swapEv = (amm: string, inM: string, inA: number, outM: string, outA: number) => Buffer.concat([Buffer.from("e445a52e51cb9a1d", "hex"), createHash("sha256").update("event:SwapEvent").digest().subarray(0, 8), b58decode(amm), b58decode(inM), u64(inA), b58decode(outM), u64(outA)]);
  k = [W, TA, TB, JUP, RAY, WHIRL, M, M2];
  const jt = jtx({ keys: k, ix: [ix(k, JUP, [0, 1, 2], anchorDisc("route"))], inner: [ix(k, RAY, [], Buffer.from([9])), ix(k, JUP, [], swapEv(RAY, M, 1_000_000, N, 5)), ix(k, WHIRL, [], Buffer.from([1])), ix(k, JUP, [], swapEv(WHIRL, N, 5, M2, 2_000_000_000))],
    preT: [tok(1, M, W, "1000000", 6), tok(2, M2, W, "0", 9)], postT: [tok(1, M, W, "0", 6), tok(2, M2, W, "2000000000", 9)] });
  const js = parseEnhanced(jt);
  assert.equal(js.type, "SWAP"); assert.equal(js.source, "JUPITER"); assert.equal(js.events.swap.innerSwaps.length, 2);
  assert.deepEqual(js.events.swap.innerSwaps.map((x: any) => x.programInfo.source), ["RAYDIUM", "ORCA"]);
  assert.deepEqual(js.events.swap.tokenIn, { mint: M, tokenAmount: 1, rawTokenAmount: "1000000" });
  assert.deepEqual(js.events.swap.tokenOut, { mint: M2, tokenAmount: 2, rawTokenAmount: "2000000000" });
  // newer Jupiter: one SwapsEvent (Vec<SwapEventV2>) attributed to DEX invocations in order
  const swapsEv = Buffer.concat([Buffer.from("e445a52e51cb9a1d", "hex"), createHash("sha256").update("event:SwapsEvent").digest().subarray(0, 8), u32(2),
    b58decode(M), u64(1_000_000), b58decode(N), u64(5), b58decode(N), u64(5), b58decode(M2), u64(2_000_000_000)]);
  const js2 = parseEnhanced(jtx({ keys: k, ix: [ix(k, "ComputeBudget111111111111111111111111111111" as any, [], Buffer.alloc(0)), ix(k, JUP, [0, 1, 2], anchorDisc("route"))].filter((x) => x.programIdIndex >= 0),
    inner: [ix(k, WHIRL, [], Buffer.from([1])), ix(k, RAY, [], Buffer.from([9])), ix(k, JUP, [], swapsEv)], preT: jt.meta.preTokenBalances, postT: jt.meta.postTokenBalances }));
  assert.equal(js2.source, "JUPITER"); assert.deepEqual(js2.events.swap.innerSwaps.map((x: any) => x.programInfo.source), ["ORCA", "RAYDIUM"]);
  assert.deepEqual(js2.events.swap.tokenOut, { mint: M2, tokenAmount: 2, rawTokenAmount: "2000000000" });
  // direct Raydium: inner SPL transfers user->vault, vault->user
  const PV1 = pk(), PV2 = pk(), POOL = pk();
  k = [W, TA, TB, PV1, PV2, RAY, TOKEN, POOL];
  const rt = jtx({ keys: k, ix: [ix(k, RAY, [1, 2, 3, 4], Buffer.from([9]))], inner: [ix(k, TOKEN, [1, 3, 0], Buffer.concat([Buffer.from([3]), u64(1_000_000)])), ix(k, TOKEN, [4, 2, 7], Buffer.concat([Buffer.from([3]), u64(3_000_000_000)]))],
    preT: [tok(1, M, W, "1000000", 6), tok(2, M2, W, "0", 9), tok(3, M, POOL, "0", 6), tok(4, M2, POOL, "9000000000", 9)],
    postT: [tok(1, M, W, "0", 6), tok(2, M2, W, "3000000000", 9), tok(3, M, POOL, "1000000", 6), tok(4, M2, POOL, "6000000000", 9)] });
  // Raydium as top-level ix: hop starts at the top-level DEX ix
  const rs = parseEnhanced(rt);
  assert.equal(rs.type, "SWAP"); assert.equal(rs.source, "RAYDIUM");
  assert.equal(rs.events.swap.tokenIn.mint, M); assert.equal(rs.events.swap.tokenIn.tokenAmount, 1); assert.equal(rs.events.swap.tokenIn.toUserAccount, POOL);
  assert.equal(rs.events.swap.tokenOut.mint, M2); assert.equal(rs.events.swap.tokenOut.tokenAmount, 3); assert.equal(rs.events.swap.tokenOut.toUserAccount, W);
  // cNFT V1 op types
  const T = pk(); k = [W, pk(), pk(), pk(), T, BUBBLEGUM];
  assert.equal(parseEnhanced(jtx({ keys: k, ix: [ix(k, BUBBLEGUM, [1, 0, 0, 3, 4], Buffer.concat([DISC.delegate, Buffer.alloc(108)]))] })).type, "COMPRESSED_NFT_DELEGATE");
  assert.equal(parseEnhanced(jtx({ keys: k, ix: [ix(k, BUBBLEGUM, [0, 0, 0, 0, 4], Buffer.concat([DISC.redeem, Buffer.alloc(108)]))] })).events.compressed[0].treeId, T);
});

// ---------------- webhooks: batching + paginated backfill ----------------
async function receiver() {
  const got: any[][] = [];
  const srv = http.createServer((req, res) => { let b = ""; req.on("data", (c) => (b += c)); req.on("end", () => { got.push(JSON.parse(b)); res.end("ok"); }); });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  return { got, url: `http://127.0.0.1:${(srv.address() as any).port}/hook`, close: () => new Promise<void>((r) => srv.close(() => r())) };
}
test("webhooks: batched POSTs (maxBatch) + backfill paginates getSignaturesForAddress with `before` until lastSlot", async () => {
  const rv = await receiver(); const file = path.join(tmp(), "wh.json"); const W = pk();
  const hub = new StreamHub({ wsUrl: mock.wsUrl, lingerMs: 0 });
  const m = new WebhookManager(hub, async () => null, { file, maxBatch: 2, flushMs: 30 });
  const h = m.create("k", { webhookURL: rv.url, accountAddresses: [W], webhookType: "raw" });
  m.close();
  // state: lastSlot 100; history: 2500 sigs at slots 101..2600 (newest first, pages of 1000)
  fs.writeFileSync(`${file}.state.json`, JSON.stringify({ [h.webhookID]: { delivered: 0, failed: 0, retries: 0, lastSlot: 100 } }));
  const hist = Array.from({ length: 2600 }, (_, i) => ({ signature: `s${i + 1}`, slot: i + 1, err: null })).reverse();
  const calls: any[] = [];
  const rpc = async (method: string, params: any[]) => {
    if (method === "getSignaturesForAddress") { calls.push(params[1]); const start = params[1].before ? hist.findIndex((x) => x.signature === params[1].before) + 1 : 0; return hist.slice(start, start + params[1].limit); }
    if (method === "getTransaction") return { slot: Number(params[0].slice(1)), transaction: { signatures: [params[0]], message: { accountKeys: [W] } }, meta: { err: null } };
  };
  const m2 = new WebhookManager(hub, async () => null, { file, rpc, maxBatch: 1000, flushMs: 10, backfillLimit: 5000 });
  try {
    await m2.ready; await m2.idle();
    assert.deepEqual(calls.map((c) => c.before ?? null), [null, "s1601", "s601"], "3 pages via before");
    const all = rv.got.flat(); assert.equal(all.length, 2500); assert.equal(all[0].slot, 101); assert.equal(all.at(-1).slot, 2600);
    assert.deepEqual(rv.got.map((b) => b.length), [1000, 1000, 500], "batched");
    assert.equal(m2.stats.get(h.webhookID)!.delivered, 2500); assert.equal((m2.stats.get(h.webhookID) as any).posts, 3);
  } finally { m2.close(); hub.close(); await rv.close(); }
});

// ---------------- DAS: off-chain json + collection index ----------------
test("das json: fetch json_uri with timeout/size cap/cache/private-host guard, fill content.files/links/description", async () => {
  let hits = 0;
  const srv = http.createServer((req, res) => {
    hits++;
    if (req.url === "/slow") return void setTimeout(() => res.end("{}"), 500);
    if (req.url === "/big") return void res.end(JSON.stringify({ x: "a".repeat(5000) }));
    if (req.url === "/bad") return void res.end("not json");
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ name: "Off", description: "desc", image: "https://img/x.png", external_url: "https://e", attributes: [{ trait_type: "a", value: 1 }], properties: { files: [{ uri: "https://img/x.png", type: "image/png" }, { uri: "https://v/x.mp4", type: "video/mp4" }] } }));
  });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r)); const base = `http://127.0.0.1:${(srv.address() as any).port}`;
  try {
    assert.equal(await new JsonFetcher().get(`${base}/ok`), null, "private host blocked by default");
    assert.ok(isPrivateIp("10.1.2.3") && isPrivateIp("::1") && isPrivateIp("169.254.169.254") && !isPrivateIp("8.8.8.8"));
    const f = new JsonFetcher({ allowPrivate: true, timeoutMs: 200, maxBytes: 1000 });
    assert.equal(f.rewrite("ipfs://Qm1/a.json"), "https://ipfs.io/ipfs/Qm1/a.json"); assert.equal(f.rewrite("ar://abc"), "https://arweave.net/abc");
    assert.equal(await f.get(`${base}/slow`), null); assert.equal(await f.get(`${base}/big`), null); assert.equal(f.stats.tooLarge, 1); assert.equal(await f.get(`${base}/bad`), null);
    const n0 = hits;
    const [x, y] = await Promise.all([f.get(`${base}/ok`), f.get(`${base}/ok`)]); assert.equal(x.name, "Off"); assert.equal(y, x);
    await f.get(`${base}/ok`); assert.equal(hits - n0, 1, "deduped + cached"); assert.ok(f.stats.hits >= 1);
    // through createDas getAsset (cnft asset with json_uri)
    const TREE = pk(); const idx = new CnftIndexer({ trees: [{ address: TREE, depth: 3 }] });
    const id = assetId(TREE, 0);
    idx.assets.set(id, { id, tree: TREE, nonce: 0, leafIndex: 0, owner: pk(), delegate: pk(), dataHash: b58encode(Buffer.alloc(32)), creatorHash: b58encode(Buffer.alloc(32)), burnt: false, seq: 1, slot: 1,
      metadata: { name: "On", symbol: "S", uri: `${base}/ok`, sellerFeeBasisPoints: 0, primarySaleHappened: false, isMutable: true, creators: [] } });
    const das: any = createDas(async () => { throw new Error("no rpc"); }, undefined, idx, { json: f });
    const a = await das.getAsset({ id });
    assert.deepEqual(a.content.files, [{ uri: "https://img/x.png", mime: "image/png" }, { uri: "https://v/x.mp4", mime: "video/mp4" }]);
    assert.equal(a.content.links.image, "https://img/x.png"); assert.equal(a.content.links.external_url, "https://e");
    assert.equal(a.content.metadata.description, "desc"); assert.equal(a.content.metadata.name, "On", "on-chain name wins"); assert.equal(a.content.metadata.attributes.length, 1);
    assert.deepEqual((await das.getAsset({ id, displayOptions: { showContent: false } })).content.files, []);
  } finally { await new Promise<void>((r) => srv.close(() => r())); }
});

test("das collections: getProgramAccounts memcmp index for DAS_COLLECTIONS feeds getAssetsByGroup (uncompressed)", async () => {
  const COLL = pk(); const mints = [pk(), pk(), pk()].sort();
  const offs = collectionOffsets(5); assert.ok(offs.includes(321 + 1 + 4 + 34 * 1 + 2 + 2 + 2), "1 creator, edition_nonce+token_standard Some");
  const calls: any[] = [];
  const up = async (method: string, params: any) => {
    calls.push({ method, params });
    if (method === "getProgramAccounts") {
      const off = params[1].filters[1].memcmp.offset; assert.equal(params[1].filters[1].memcmp.bytes, b58encode(Buffer.concat([Buffer.from([1, 1]), b58decode(COLL)])));
      assert.deepEqual(params[1].dataSlice, { offset: 33, length: 32 });
      const hit = off === offs[0] ? [mints[0]] : off === offs[5] ? [mints[1], mints[2]] : [];
      return hit.map((mm) => ({ pubkey: pk(), account: { data: [b58decode(mm).toString("base64"), "base64"] } }));
    }
    if (method === "getMultipleAccounts") return { value: params[0].map((x: string) => params[1].encoding === "jsonParsed" ? { owner: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", data: { parsed: { type: "mint", info: { decimals: 0, supply: "1" } } } } : null) };
  };
  const ci = new CollectionIndex([COLL], 5); await ci.build(up);
  assert.equal(ci.stats.calls, offs.length); assert.deepEqual(ci.list(COLL), mints);
  const das: any = createDas(up, undefined, undefined, { json: false, collections: ci });
  const g = await das.getAssetsByGroup({ groupKey: "collection", groupValue: COLL, limit: 2, page: 2 });
  assert.equal(g.total, 3); assert.deepEqual(g.items.map((x: any) => x.id), [mints[2]]); assert.equal(g.items[0].interface, "V1_NFT");
});
