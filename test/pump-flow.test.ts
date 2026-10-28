import { test } from "node:test";
import assert from "node:assert/strict";
import { PumpFlow, curvePda } from "../src/index.js";

const M = "3PwXa9BRA2sUdjtcGxs4bVLBau2dHTuZwtb6CbpTpdvA", M2 = "4F6ZbYX2ZRd7zV14zmY3dciTnm14hWD9prskde3dnPSa", DEV = "DbPzFfzmaFq1KHyqUk4SrfvQnXVokyui9sJg4a2JW8SK";
const stub = () => ({ hooks: [] as any[], subscribe: () => () => {}, curvePrice: async () => ({ creator: DEV }), poolInfo: async () => null }) as any;
let n = 0;
const sw = (o: any) => ({ type: "swap", signature: `S${n++}`, slot: 7, blockTime: 1_700_000_000, wallet: `W${n}`, mint: M, side: "buy", solAmount: 1, tokenAmount: 1000, price: 1e-8, priceValid: true, venue: "curve", program: "pump",
  marketCapSol: 10, liquiditySol: 2, progress: 12, receivedAt: 0, ...o });
const tick = () => new Promise((r) => setTimeout(r, 5));

test("flow windows: buy/sell ratio, net flow, unique buyers, price change + reasons", () => {
  let now = 1_000_000_000; const eng = stub(); const f = new PumpFlow(eng, async () => null, { now: () => now }).start(); const h = eng.hooks[0];
  h(sw({ create: true, wallet: DEV, price: 1e-8 })); now += 30_000;
  h(sw({ wallet: "A", solAmount: 2, price: 1.5e-8 })); h(sw({ wallet: "A", solAmount: 1, price: 2e-8 })); h(sw({ wallet: "B", side: "sell", solAmount: 1, price: 1.8e-8 }));
  const r: any = f.flow(M);
  const w = r.windows["1m"]; assert.equal(w.buySol, 4); assert.equal(w.sellSol, 1); assert.equal(w.netFlowSol, 3); assert.equal(w.buySellRatio, 4); assert.equal(w.uniqueBuyers, 2); assert.equal(w.txs, 4);
  assert.equal(w.priceChangePct, 80); // open 1e-8 -> last 1.8e-8
  assert.match(r.windows["5m"].priceChangeReason, /youngerThanWindow/); assert.equal(r.windows["5m"].complete, false);
  assert.equal(f.flow(M2).tracked, false); assert.equal(f.flow(M2).reason, "noTradesSinceStartup");
  assert.throws(() => f.flow("x"), /pumpFlow/);
  now += 120_000; assert.equal((f.flow(M) as any).windows["1m"].txs, 0); assert.equal((f.flow(M) as any).windows["1m"].buySellRatioReason, "noTrades");
});

test("trending: sort, limit, venue filter, enrichment (holders, top10 excl. curve, dev %), null reasons", async () => {
  let now = 1_000_000_000; const eng = stub(); const curve = curvePda(M);
  const rpc = async (m: string, p: any[]) => {
    if (m === "getTokenLargestAccounts") return { value: [{ address: "acc1", amount: "800000000000000" }, { address: "acc2", amount: "50000000000000" }, { address: "acc3", amount: "10000000000000" }], _meta: { holdersNonZero: 123 } };
    if (m === "getTokenSupply") return { value: { amount: "1000000000000000" } };
    if (m === "getMultipleAccounts") return { value: [curve, "H1", DEV].map((o) => ({ data: { parsed: { info: { owner: o } } } })) };
    if (m === "getTokenAccountsByOwner") return { value: [{ account: { data: { parsed: { info: { tokenAmount: { amount: "10000000000000" } } } } } }] };
    return null; };
  const f = new PumpFlow(eng, rpc, { now: () => now }).start(); const h = eng.hooks[0];
  for (let i = 0; i < 5; i++) h(sw({ mint: M, solAmount: 1 }));
  for (let i = 0; i < 2; i++) h(sw({ mint: M2, solAmount: 10, venue: "amm", program: "pumpswap", progress: 100 }));
  let r: any = await f.trending({ sort: "txs5m", limit: 1 });
  assert.equal(r.tokens.length, 1); assert.equal(r.tokens[0].mint, M); assert.equal(r.tokens[0].txs["5m"], 5);
  const t = r.tokens[0]; assert.equal(t.holderCount, 123); assert.equal(t.top10Pct, 6); assert.equal(t.curveOrPoolPct, 80); assert.equal(t.devHoldingPct, 1); assert.equal(t.bondingProgress, 12);
  assert.equal(t.buySol["1m"], 5); assert.equal(t.sellSol["5m"], 0); assert.equal(t.uniqueBuyers["1m"], 5); assert.equal(t.devSold, false); assert.equal(t.creator, DEV);
  h(sw({ mint: M, wallet: DEV, side: "sell", solAmount: 0.2 })); r = await f.trending({ sort: "txs5m", limit: 1 }); assert.equal(r.tokens[0].devSold, true); assert.equal(r.tokens[0].sellSol["1m"], 0.2);
  assert.equal(t.ageSec, null); assert.equal(t.ageReason, "createNotSeenSinceStartup");
  r = await f.trending({ sort: "volume5m", enrich: false }); assert.equal(r.tokens[0].mint, M2); assert.equal(r.tokens[0].holderCountReason, "notEnriched"); assert.equal(r.tokens[0].complete, true);
  r = await f.trending({ venue: "curve", enrich: false }); assert.deepEqual(r.tokens.map((x: any) => x.mint), [M]);
  await assert.rejects(f.trending({ sort: "bogus" }), /sort one of/); await assert.rejects(f.trending({ limit: 500 }), /limit/);
});

test("alerts: devSell + largeSell (% of position from tx balances), thresholds, budget reason", async () => {
  const eng = stub(); const pre = (w: string, a: number) => ({ owner: w, mint: M, uiTokenAmount: { amount: String(a) } });
  const rpc = async (m: string, p: any[]) => m === "getTransaction" ? ({ "S-big": { meta: { preTokenBalances: [pre("BIG", 1000)], postTokenBalances: [pre("BIG", 200)] } },
    "S-small": { meta: { preTokenBalances: [pre("SM", 1000)], postTokenBalances: [pre("SM", 900)] } }, "S-dev": { meta: { preTokenBalances: [pre(DEV, 500)], postTokenBalances: [pre(DEV, 0)] } } } as any)[p[0]] ?? null : null;
  const f = new PumpFlow(eng, rpc, { now: () => 1_000_000_000, txPerMin: 3 }).start(); const h = eng.hooks[0];
  const got: any[] = []; f.watch({ mints: [M], largeSellPct: 50, minSellSol: 0.5 }, (a) => got.push(a));
  h(sw({ signature: "S-big", wallet: "BIG", side: "sell", solAmount: 2 })); h(sw({ signature: "S-small", wallet: "SM", side: "sell", solAmount: 2 }));
  h(sw({ signature: "S-tiny", wallet: "T", side: "sell", solAmount: 0.1 })); h(sw({ signature: "S-dev", wallet: DEV, side: "sell", solAmount: 1 }));
  h(sw({ signature: "S-x", wallet: "X", side: "sell", solAmount: 1, mint: M2 })); // not watched
  await tick(); await tick();
  const big = got.find((a) => a.type === "largeSell" && a.wallet === "BIG"); assert.ok(big); assert.equal(big.wallet, "BIG"); assert.equal(big.pctOfPositionSold, 80); assert.equal(big.blockTime, 1_700_000_000); assert.equal(big.signature, "S-big");
  assert.ok(!got.some((a) => a.wallet === "SM" || a.wallet === "T" || a.wallet === "X"));
  const dev = got.find((a) => a.type === "devSell"); assert.ok(dev); assert.equal(dev.creator, DEV); assert.equal(dev.pctOfPositionSold, 100);
  h(sw({ signature: "S-over", wallet: "O", side: "sell", solAmount: 5 })); await tick();
  assert.equal(f.droppedChecks, 1);
  assert.throws(() => f.watch({ largeSellPct: 500 }, () => {}), /largeSellPct/);
});

test("upstream watchdog: reconnects when a logs sub goes silent; rotates URLs", async () => {
  const { WebSocketServer } = await import("ws"); const { SolanaWs } = await import("../src/upstream-ws.js");
  const wss = new WebSocketServer({ port: 0 }); let conns = 0;
  wss.on("connection", (c) => { conns++; c.on("message", (m) => { const j = JSON.parse(String(m)); c.send(JSON.stringify({ jsonrpc: "2.0", id: j.id, result: 7 })); }); });
  const port = (wss.address() as any).port;
  const up = new SolanaWs(`ws://127.0.0.1:1,ws://127.0.0.1:${port}`, () => {}, { reqTimeoutMs: 2000, pingMs: 60_000, handshakeMs: 1000, silentMs: 400, dogMs: 100 });
  const stop = up.subscribe("logsSubscribe", [{ mentions: ["x"] }], () => {});
  for (let i = 0; i < 100 && conns < 2; i++) await new Promise((r) => setTimeout(r, 100));
  assert.ok(conns >= 2, "rotated to working url, then watchdog reconnected the silent logs sub"); assert.ok(up.watchdogDrops >= 1);
  stop(); up.close(); wss.close();
});

test("curve screener: gPA holders, age from oldest signature, bundled flag, eta, score, filters", async () => {
  let now = 1_000_000_000; const eng = stub(); const curve = curvePda(M);
  const { b58decode } = await import("../src/codec.js");
  const acc = (owner: string, amt: number) => { const d = Buffer.alloc(40); b58decode(owner).copy(d, 0); d.writeBigUInt64LE(BigInt(amt), 32); return { pubkey: "x", account: { data: [d.toString("base64"), "base64"] } }; };
  const H = ["8JUjWjAyXTMB4ZXcV7nk3p6Gg1fWAAoSck7xekuyADKL", "BsjFnU6yWqZ6T4k2Gsz7jFU7dN6Pe3KcsnPBDoQ6q4kp", DEV];
  let calls: Record<string, number> = {};
  const rpc = async (m: string, p: any[]) => { calls[m] = (calls[m] ?? 0) + 1;
    if (m === "getAccountInfo") return { value: { owner: "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" } };
    if (m === "getProgramAccounts") return [acc(curve, 7e14), acc(H[0], 1e14), acc(H[1], 5e13), acc(DEV, 2e13), acc(H[0], 1e13), acc("11111111111111111111111111111111", 0)];
    if (m === "getSignaturesForAddress" && p[0] === M) return [{ signature: "z", slot: 105, blockTime: 999_000 }, ...Array.from({ length: 6 }, (_, i) => ({ signature: "b" + i, slot: 100 + (i % 2), blockTime: 999_400 })), { signature: "c", slot: 100, blockTime: 999_400 }];
    if (m === "getSignaturesForAddress") return [{ signature: "w", blockTime: 999_000 }];
    return null; };
  const f = new PumpFlow(eng, rpc, { now: () => now }).start(); const h = eng.hooks[0];
  for (let i = 0; i < 40; i++) { h(sw({ mint: M, wallet: "B" + i, solAmount: 0.5, progress: 20 + i * 0.5 })); if (i % 10 === 9) now += 10_000; }
  h(sw({ mint: M2, solAmount: 1, progress: 99 }));
  const r: any = await f.curveScreener({ minBonding: 10, maxBonding: 90, limit: 5 });
  assert.equal(r.tokens.length, 1); const t = r.tokens[0];
  assert.equal(t.mint, M); assert.equal(t.enrichSource, "gpaCurve"); assert.equal(t.holderCount, 3); assert.equal(t.top10Pct, 18); assert.equal(t.curveOrPoolPct, 70); assert.equal(t.devHoldingPct, 2);
  assert.equal(t.ageSec, 640); assert.equal(t.ageSource, "oldestSignature"); assert.equal(t.flags.earlyBuys3Slots, 6); assert.equal(t.flags.bundled, true); assert.equal(t.flags.freshWalletRatio, 1);
  assert.ok(t.bondingVelocityPctPerMin > 0 && t.bondingEtaSec > 0); assert.ok(t.score >= 0 && t.score <= 100); assert.match(r.scoreFormula, /uniqueBuyers5m/);
  assert.equal((await f.curveScreener({ maxTop10Pct: 10 })).tokens.length, 0);
  assert.equal((await f.curveScreener({ minAgeSec: 5000 })).tokens.length, 0);
  const n = calls.getProgramAccounts; await f.curveScreener({}); assert.equal(calls.getProgramAccounts, n, "enrichment cached");
  await assert.rejects(f.curveScreener({ maxBonding: 200 }), /maxBonding/); await assert.rejects(f.curveScreener({ sort: "x" }), /sort one of/);
});

test("largeSell solAmount: curve SOL / WSOL-quoted from event, non-SOL via solEquivalent or priceSol estimate, minSellSol applied, unknown only fires at minSellSol 0", async () => {
  const eng = stub(); const W = "So11111111111111111111111111111111111111112";
  const rpc = async (m: string, p: any[]) => m === "getTransaction" ? { meta: { preTokenBalances: [{ owner: String(p[0]).slice(2), mint: M, uiTokenAmount: { amount: "1000" } }], postTokenBalances: [] } } : null;
  const f = new PumpFlow(eng, rpc, { now: () => 1_000_000_000 }).start(); const h = eng.hooks[0];
  const got: any[] = [], got0: any[] = []; f.watch({ mints: [M], largeSellPct: 50, minSellSol: 1 }, (a) => got.push(a)); f.watch({ mints: [M], largeSellPct: 50, minSellSol: 0 }, (a) => got0.push(a));
  h(sw({ signature: "S-A", wallet: "A", side: "sell", solAmount: 2 }));                                            // plain curve
  h(sw({ signature: "S-B", wallet: "B", side: "sell", solAmount: 3, quoteMint: W }));                              // WSOL-tagged, no solEquivalent
  h(sw({ signature: "S-C", wallet: "C", side: "sell", solAmount: 0, quoteMint: "Q", solEquivalent: 4 }));         // non-SOL converted
  h(sw({ signature: "S-D", wallet: "D", side: "sell", solAmount: 0, quoteMint: "Q", priceSol: 0.002, tokenAmount: 1000 })); // estimate = 2 SOL
  h(sw({ signature: "S-E", wallet: "E", side: "sell", solAmount: 0, quoteMint: "Q", solEquivalentReason: "noSolRoute" })); // unknown
  h(sw({ signature: "S-F", wallet: "F", side: "sell", solAmount: 0.3 }));                                          // below minSellSol 1
  await tick(); await tick();
  const by = (w: string, l = got) => l.find((a) => a.wallet === w);
  assert.equal(by("A").solAmount, 2); assert.equal(by("A").solAmountSource, "event");
  assert.equal(by("B").solAmount, 3); assert.equal(by("B").solAmountSource, "event");
  assert.equal(by("C").solAmount, 4); assert.equal(by("C").solAmountSource, "solEquivalent");
  assert.equal(by("D").solAmount, 2); assert.equal(by("D").solAmountSource, "priceEstimate");
  assert.equal(by("E"), undefined); assert.equal(by("F"), undefined);
  const e0 = by("E", got0); assert.ok(e0); assert.equal(e0.solAmount, null); assert.equal(e0.solAmountReason, "noSolRoute"); assert.ok(by("F", got0));
});
