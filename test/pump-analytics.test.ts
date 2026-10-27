import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs"; import os from "node:os"; import path from "node:path";
import { PumpAnalytics, parseWalletsFile } from "../src/index.js";

const W = "4F6ZbYX2ZRd7zV14zmY3dciTnm14hWD9prskde3dnPSa", W2 = "DbPzFfzmaFq1KHyqUk4SrfvQnXVokyui9sJg4a2JW8SK", C = "3PwXa9BRA2sUdjtcGxs4bVLBau2dHTuZwtb6CbpTpdvA";
const stub = () => ({ hooks: [] as any[], subscribe: () => () => {}, tagOf: () => undefined, watched: () => false }) as any;
let n = 0;
const sw = (o: any) => ({ signature: `S${n++}`, slot: 100, wallet: W, mint: "M1", side: "buy", solAmount: 1, tokenAmount: 1e6, price: 1e-8, priceValid: true, venue: "curve", program: "pump", create: false, ...o });

test("parseWalletsFile", () => {
  const m = parseWalletsFile(`${W} DEV SAPI\n# x\n${W2} SAPI MAHKOTA\n`);
  assert.equal(m.get(W), "DEV SAPI"); assert.equal(m.size, 2);
});

test("wallet ranking: forward returns, win rate, realized PnL, tag filter, persistence", () => {
  let now = 1_000_000; const f = path.join(os.tmpdir(), `pa-${process.pid}.json`); fs.rmSync(f, { force: true });
  const a = new PumpAnalytics(stub(), async () => null, { now: () => now, dataFile: f, wallets: new Map([[W, "DEV SAPI"], [W2, "MAHKOTA"]]) });
  a.onSwap(sw({ mint: "M1", price: 1e-8 })); a.onSwap(sw({ mint: "M2", price: 2e-8 })); a.onSwap(sw({ wallet: W2, mint: "M3", price: 1e-8 }));
  a.onSwap(sw({ wallet: "Other111111111111111111111111111111111111111", mint: "M9" })); // untagged ignored
  a.onSwap(sw({ mint: "M4", priceValid: false, venue: "amm-other" })); // invalid entry: no forward return
  now += 61_000;
  a.onSwap(sw({ wallet: "X", mint: "M1", price: 1.5e-8 })); a.onSwap(sw({ wallet: "X", mint: "M2", price: 1e-8 })); a.onSwap(sw({ wallet: "X", mint: "M3", price: 3e-8 }));
  a.tick();
  now += 300_000; a.onSwap(sw({ mint: "M1", side: "sell", solAmount: 2, tokenAmount: 1e6, price: 2e-8 })); a.tick();
  const r = a.ranking({ minTrades: 1, horizon: 60 });
  assert.equal(r.wallets.length, 2); assert.equal(r.wallets[0].wallet, W2); assert.equal(r.wallets[0].avgReturn, 2);
  const w = r.wallets.find((x) => x.wallet === W)!;
  assert.equal(w.buys, 3); assert.equal(w.scored, 2); assert.equal(w.winRate, 0.5); assert.equal(w.avgReturn, 0); assert.equal(w.horizons["60s"].medianReturn, 0);
  assert.equal(w.realizedSol, 1); // sold for 2 SOL, cost 1 SOL
  assert.equal(a.ranking({ tag: "mahkota", minTrades: 1, horizon: 60 }).wallets.length, 1);
  assert.equal(a.ranking({ minTrades: 3, horizon: 60 }).wallets.length, 0);
  assert.throws(() => a.ranking({ horizon: 7 }), /horizon/);
  a.close();
  const b = new PumpAnalytics(stub(), async () => null, { now: () => now, dataFile: f });
  assert.equal(b.trades.length, a.trades.length); assert.equal(b.ranking({ minTrades: 1, horizon: 60 }).wallets.length, 2);
  fs.rmSync(f, { force: true });
});

test("launch risk: creator share, same-slot + early-slot bundle buys, score, launchRisk on later swaps, shared funder", async () => {
  const funders: Record<string, string> = { [W]: "F1", [W2]: "F1" };
  const a = new PumpAnalytics(stub(), async (m, p) => m === "getFundedBy" ? { funder: funders[p[0]] ?? null } : null, { earlySlots: 2 });
  a.onSwap(sw({ wallet: C, mint: "L1", create: true, slot: 500, tokenAmount: 50e6 })); // creator buys 5%
  a.onSwap(sw({ wallet: W, mint: "L1", slot: 500, tokenAmount: 30e6 }));
  a.onSwap(sw({ wallet: W2, mint: "L1", slot: 501, tokenAmount: 20e6 }));
  a.onSwap(sw({ wallet: "Late", mint: "L1", slot: 503, tokenAmount: 90e6 })); // outside early window
  const e = sw({ wallet: "Late", mint: "L1", slot: 504 }); a.onSwap(e);
  assert.deepEqual({ ...e.launchRisk, sharedFunder: undefined }, { creator: C, createSlot: 500, creatorPct: 5, bundleWallets: 2, bundlePct: 5, sameSlotBuys: 1, earlyBuyers: 2, sharedFunder: undefined, score: 22, final: true });
  const i = await a.launchInfo("L1", { funders: true });
  assert.equal(i.known, true); assert.deepEqual((i as any).sharedFunder, { funder: "F1", wallets: 2 }); assert.equal((i as any).score, 42);
  assert.equal((await a.launchInfo("nope")).known, false);
  const no = sw({ mint: "Z" }); a.onSwap(no); assert.equal(no.launchRisk, undefined); // no create seen
});
