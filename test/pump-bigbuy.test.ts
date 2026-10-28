import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { PumpBigBuys, PumpFlow } from "../src/index.js";

const M = "3PwXa9BRA2sUdjtcGxs4bVLBau2dHTuZwtb6CbpTpdvA", DEV = "DbPzFfzmaFq1KHyqUk4SrfvQnXVokyui9sJg4a2JW8SK", W = "8JUjWjAyXTMB4ZXcV7nk3p6Gg1fWAAoSck7xekuyADKL";
let n = 0;
const sw = (o: any) => ({ type: "swap", signature: `S${n++}`, slot: 7, blockTime: 1_700_000_000, wallet: W, mint: M, side: "buy", solAmount: 1, tokenAmount: 1000, price: 1e-3, priceValid: true, venue: "curve", program: "pump", marketCapSol: 30, progress: 40, receivedAt: 0, ...o });
const tick = () => new Promise((r) => setTimeout(r, 10));

test("big buys: notification insight, creator, wallet age, repeat, pnl, winrate, rankings, persistence", async () => {
  let now = 1_000_000_000_000; const file = `/tmp/bb-test-${process.pid}.json`; fs.rmSync(file, { force: true });
  const eng: any = { hooks: [], subscribe: () => () => {}, curvePrice: async () => ({ creator: DEV }), poolInfo: async () => null };
  const rpc = async (m: string, p: any[]) => m === "getSignaturesForAddress" ? (p[0] === W ? [{ blockTime: now / 1000 - 3600 }, { blockTime: now / 1000 - 7200 }] : Array.from({ length: 1000 }, () => ({ blockTime: 1 }))) : null;
  const flow = new PumpFlow(eng, rpc, { now: () => now }).start();
  const bb = new PumpBigBuys(eng, rpc, flow, { now: () => now, dataFile: file, evalSec: 60 }).start();
  const got: any[] = []; bb.subscribe({ minSol: 1 }, (x) => got.push(x));
  const h = (e: any) => { for (const f of eng.hooks) f(e); };
  h(sw({ solAmount: 0.6 })); // recorded (>=0.5) but below sub minSol
  h(sw({ solAmount: 2, tokenAmount: 2000, price: 1e-3 })); await tick();
  assert.equal(got.length, 1); const x = got[0];
  assert.equal(x.type, "bigBuy"); assert.equal(x.solAmount, 2); assert.equal(x.priceSol, 1e-3); assert.equal(x.bondingProgress, 40); assert.equal(x.isCreator, false);
  assert.equal(x.walletAgeSec, 7200); assert.equal(x.freshWallet, true); assert.equal(x.prior.pastBigBuys, 1); assert.equal(x.prior.priorBigBuysThisMint, 1);
  h(sw({ wallet: DEV, solAmount: 1.5 })); await tick(); assert.equal(got[1].isCreator, true); assert.equal(got[1].walletAgeReason, "moreThan1000Signatures"); assert.equal(got[1].freshWallet, false);
  // price doubles after evalSec -> W's prior buys settle as wins; W sells half: realized pnl
  now += 61_000; h(sw({ wallet: "BsjFnU6yWqZ6T4k2Gsz7jFU7dN6Pe3KcsnPBDoQ6q4kp", solAmount: 0.1, price: 2e-3 }));
  h(sw({ side: "sell", tokenAmount: 1300, solAmount: 2.6, price: 2e-3 }));
  const v: any = bb.walletView(W); assert.equal(v.wins, 2); assert.equal(v.winRate, 1); assert.equal(v.realizedPnlSol, 1.473333); assert.equal(v.unrealizedPnlSol, 1.926667); assert.equal(v.pastBigBuys, 2);
  const rk: any = bb.rank({ minSol: 0.5, windowSec: 3600, sort: "count" }); assert.equal(rk.top[0].wallet, W); assert.equal(rk.top[0].count, 2); assert.equal(rk.top[0].totalSol, 2.6);
  const rc: any = bb.recent({ minSol: 1 }); assert.equal(rc.total, 2); assert.equal(rc.buys[0].wallet, DEV);
  assert.equal(bb.recent({ minSol: 1, mint: M, venue: "amm" }).total, 0);
  bb.close(); const bb2 = new PumpBigBuys(eng, rpc, flow, { now: () => now, dataFile: file });
  assert.equal(bb2.recent({ minSol: 0.5 }).total, 3); assert.equal((bb2.walletView(W) as any).wins, 2);
  assert.throws(() => bb.subscribe({ minSol: 0.1 }, () => {}), /minSol/); assert.throws(() => bb.rank({ sort: "x" }), /sort/);
  fs.rmSync(file, { force: true });
});
