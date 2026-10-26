// Mainnet v1 (SIMD-0385) transactions: wire decode, proxy version upgrade, v1 priority-fee sampling.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createFastRpc, decodeTransaction, b58encode } from "../src/index.js";
import { startMockSolana } from "./helpers.js";

// real mainnet v1 tx (Z4gaXHRr...), fetched read-only from getTransaction(encoding base64)
const HEX = "810100010f0000009508330fdb5e8bc1631c17caf01e01f2358910fc2a50d36d7cd8f9868949568b0103d6e1dd6123a01c87c415a4feffffffff5b16c1227c4f336bc389d6983712c98ad5945bca3c2c43f1343c57479959c7236bb60204ea43862e304492d79f713345096819f64a02f757247a1e27336863988d6ca813da311c27f95995276b9051e84401000000000000180000007103000002022100000162b2ee340000000065b3ee3400000000e3f9841be0470f004f78a278a1010000ff1ba67c6caed900f35fd1b259eaf0fd777e5357be58a6a946d2e8193d37cb7242508106afebf31d4f126cee82aaedc4b60aa146cb7425a6ccffac608b235f820a";

test("codec: decodes a real mainnet v1 transaction (matches getTransaction json)", () => {
  const d = decodeTransaction(Buffer.from(HEX, "hex").toString("base64"), "base64");
  assert.equal(d.version, 1);
  assert.deepEqual(d.accountKeys, ["FTp1BybZ51NiZKbnZH6MsrV3tUZNauhpQMbBcqYUEr5f", "FNj3pmEZFrvdmefpVJp3e6LdvQQqsHAmHcCGLKRWtp4t", "dijkbkCAKfFTCxQg3u1pg82gVU1jJGHBBRcteD11mBu"]);
  assert.equal(d.recentBlockhash, "B2kzvKzroYMT1J1oXQ44vhvjWYHFoGgpvWvX9H89FLpi");
  assert.equal(d.signatures[0], "Z4gaXHRr6e4rLbbj27F5aCnf67LAnySBFxshR9bmFDpQRuVM5kxVffi89RYCYuKQtYYtAuiKaqyeA4rcAGSLa8h");
  assert.deepEqual(d.config, { priorityFee: 324, computeUnitLimit: 24, loadedAccountsDataSizeLimit: 881 });
  assert.equal(d.instructions.length, 1); assert.deepEqual(d.instructions[0].accounts, [0, 1]); assert.equal(d.instructions[0].programIdIndex, 2);
  assert.equal(b58encode(d.instructions[0].data), "WKYWZTnear7w9DnyxqiYa1UxyrKRG2Y43fmVMtxeXY4sg");
  assert.throws(() => decodeTransaction(Buffer.from(HEX, "hex").subarray(0, 100).toString("base64"), "base64"), /truncated/);
});

test("proxy: getBlock/getTransaction always request maxSupportedTransactionVersion>=1", async () => {
  const m = await startMockSolana(); const seen: any[] = [];
  m.responses.getBlock = (p) => { seen.push(p); return { transactions: [] }; };
  m.responses.getTransaction = (p) => { seen.push(p); return null; };
  const a = createFastRpc({ api: { url: m.httpUrl }, hub: { wsUrl: m.wsUrl }, webhooks: false, chain: { detect: false } });
  try {
    const api = a.api as any;
    await api.getBlock(10); await api.getBlock(11, { maxSupportedTransactionVersion: 0, transactionDetails: "signatures" }); await api.getBlock(12, "json");
    await api.getTransaction("5x", { maxSupportedTransactionVersion: 0 }); await api.getTransaction("6x");
    assert.equal(seen.length, 5);
    for (const p of seen) assert.equal(p[1].maxSupportedTransactionVersion, 1);
    assert.equal(seen[1][1].transactionDetails, "signatures"); assert.equal(seen[2][1].encoding, "json");
  } finally { await a.close(); await m.close(); }
});

test("priority fee: v1 txs (transactionConfig.priorityFee, no ComputeBudget ix) are sampled", async () => {
  const m = await startMockSolana();
  m.responses.getRecentPrioritizationFees = () => Array.from({ length: 10 }, (_, i) => ({ slot: i, prioritizationFee: 0 }));
  m.responses.getBlock = () => ({ transactions: [[1000, 200_000], [5000, 100_000], [200, 1000]].map(([fee, cu]) => ({ version: 1, transaction: { message: { accountKeys: ["A"], instructions: [], transactionConfig: { priorityFee: fee, computeUnitLimit: cu } } }, meta: { err: null } })) });
  const a = createFastRpc({ api: { url: m.httpUrl }, hub: { wsUrl: m.wsUrl }, webhooks: false, chain: { detect: false } });
  try {
    const r = await (a.api as any).getPriorityFeeEstimate({ options: { includeAllPriorityFeeLevels: true, includeDetails: true } });
    assert.equal(r.source, "recentBlocks"); assert.equal(r.priorityFeeLevels.unsafeMax, 200_000); // 200 lamports / 1000 CU = 200k µL/CU
  } finally { await a.close(); await m.close(); }
});

test("race: provider-gated -32602 / -32601 fail over to the next upstream (no cooldown)", async () => {
  const { createRacedUpstream, RpcError } = await import("../src/index.js");
  const calls: string[] = [];
  const make = (u: string) => async (m: string) => { calls.push(u);
    if (u === "a") throw new RpcError(-32602, "Indexed requests require a personal token. Get one at: https://x");
    if (u === "b") throw new RpcError(-32601, "Method not found");
    return 42; };
  const r = createRacedUpstream(["a", "b", "c"], { make, hedgeMs: 10_000 });
  assert.equal(await r("getTokenSupply", ["x"]), 42);
  assert.ok(r.stats().every((s: any) => s.healthy)); // gated answers do not mark upstreams down
  const r2 = createRacedUpstream(["p"], { make: () => async () => { throw new RpcError(-32602, "Invalid param: bad pubkey"); } });
  await assert.rejects(r2("getBalance", ["x"]), /Invalid param/); // real param errors still surface
});

test("transactionSubscribe: processed commitment fetches with confirmed (getTransaction rejects processed) and v1", async () => {
  const { TxWatcher, ChainState } = await import("../src/index.js");
  const seen: any[] = [];
  const hub: any = { chain: new ChainState(), upstream: {}, subscribe: () => () => {} };
  const w = new TxWatcher(hub, async (_s: string, cfg: any) => { seen.push(cfg); return { slot: 1 }; }, { accountInclude: ["A"] }, { commitment: "processed", maxSupportedTransactionVersion: 0 }, () => {});
  await (w as any).fetch("sig");
  assert.equal(seen[0].commitment, "confirmed"); assert.equal(seen[0].maxSupportedTransactionVersion, 1);
  w.stop();
});
