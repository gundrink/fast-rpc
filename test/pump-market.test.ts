import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { PumpMarket, addCandle, computeLevels, parsePumpLogs } from "../src/index.js";

const MINT = "3PwXa9BRA2sUdjtcGxs4bVLBau2dHTuZwtb6CbpTpdvA";
const stub = (o: any = {}) => ({ hooks: [] as any[], subscribe: () => () => {}, solUsd: 150, poolInfo: async () => null, curvePrice: async () => ({ complete: false }), decimals: async () => 8, quoteSol: async () => ({ sol: 0.01, via: "test" }), ...o }) as any;
let n = 0;
const sw = (o: any) => ({ type: "swap", signature: `S${n++}`, slot: 1, wallet: "W", mint: MINT, side: "buy", solAmount: 1, price: 1e-8, priceValid: true, venue: "curve", program: "pump", ...o });

test("addCandle: OHLCV buckets, out-of-order insert, cap", () => {
  const a: any[] = [];
  addCandle(a, 1000, 1000, 1, 1, 3); addCandle(a, 1000, 1500, 3, 2, 3); addCandle(a, 1000, 1900, 2, 1, 3);
  assert.deepEqual(a[0], { t: 1000, o: 1, h: 3, l: 1, c: 2, v: 4, n: 3 });
  addCandle(a, 1000, 3000, 5, 1, 3); addCandle(a, 1000, 2000, 4, 1, 3); // backfill insert in the middle
  assert.deepEqual(a.map((k) => k.t), [1000, 2000, 3000]);
  addCandle(a, 1000, 4000, 5, 1, 3); assert.equal(a.length, 3); assert.equal(a[0].t, 2000);
});

test("computeLevels: nearest pivot support/resistance with distance %", () => {
  const ps = [1, 2, 3, 2, 1, 1.5, 2.5, 4, 2.5, 1.5, 2, 3];
  const cs = ps.map((p, i) => ({ t: i, o: p, h: p * 1.01, l: p * 0.99, c: p, v: 1, n: 1 }));
  const l = computeLevels(cs, 2.8)!;
  assert.ok(l.resistance && l.resistance.price > 2.8 && l.resistance.distancePct > 0);
  assert.ok(l.support && l.support.price < 2.8 && l.support.distancePct < 0);
  assert.equal(computeLevels(cs.slice(0, 3), 2), null);
});

test("candles: live valid prices only, dust/invalid skipped, subscribe notifications", async () => {
  let now = 1_700_000_000_000; const eng = stub();
  const m = new PumpMarket(eng, async () => [], { now: () => now }).start();
  const got: any[] = []; m.subscribeCandles(MINT, "1s", (c) => got.push(c));
  const h = eng.hooks[0];
  h(sw({ price: 1e-8, blockTime: now / 1000 })); h(sw({ price: 2e-8, blockTime: now / 1000 }));
  h(sw({ price: 9e-8, priceValid: false, blockTime: now / 1000 })); h(sw({ price: 9e-8, dust: true, blockTime: now / 1000 }));
  h(sw({ price: 9e-8, venue: "amm-other", blockTime: now / 1000 }));
  const r = await m.candles(MINT, { interval: "1s", lookback: 10, backfill: false });
  assert.equal(r.candles.length, 1); assert.equal(r.candles[0].h, 2e-8); assert.equal(r.candles[0].n, 2); assert.equal(got.length, 2);
  await assert.rejects(m.candles(MINT, { interval: "2m" }), /interval/);
  m.close();
});

test("candles backfill: signatures -> getTransaction -> trade events, capped", async () => {
  const fx = JSON.parse(fs.readFileSync("test/fixtures/pump-logs.json", "utf8"));
  const logs = ["Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]", fx.pumpLog, "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P success"];
  const ev = parsePumpLogs(logs).events.find((e: any) => e.k === "pump") as any; assert.ok(ev, "fixture parses");
  const mint = ev.t.mint; const calls: Record<string, number> = {};
  const rpc = async (meth: string, p: any[]) => { calls[meth] = (calls[meth] ?? 0) + 1;
    if (meth === "getSignaturesForAddress") return Array.from({ length: p[1].limit }, (_, i) => ({ signature: `sig${i}`, blockTime: 1_700_000_000 + i, err: null }));
    return { blockTime: 1_700_000_000, meta: { logMessages: logs } }; };
  const m = new PumpMarket(stub(), rpc, { now: () => 1_700_000_100_000, backfillTxs: 12, backfillPerMin: 8 });
  const r = await m.candles(mint, { interval: "1m", lookback: 5 });
  assert.equal(calls.getTransaction, 8); assert.equal(r.backfill!.capped, true); assert.ok(r.candles.length >= 1);
  assert.equal(r.candles[0].n, 8); assert.equal(r.quoteMint, "Xs2yquAgsHByNzx68WJC55WHjHBvG9JsMB7CWjTLyPy"); assert.ok(r.candles[0].c > 0);
});

test("marketRegime: SOL change insufficient then computed, net flow windows, drawdown share", () => {
  let now = 1_000_000_000; const eng = stub();
  const m = new PumpMarket(eng, async () => [], { now: () => now }).start();
  const h = eng.hooks[0];
  let r: any = m.regime(); assert.equal(r.solChange["15m"].changePct, null); assert.equal(r.label, null); assert.equal(r.labelStatus, "warming");
  m.sample(); now += 10 * 60_000; eng.solUsd = 160; m.sample();
  r = m.regime(); assert.match(r.solChange["15m"].note, /insufficient/);
  now += 6 * 60_000; eng.solUsd = 165; m.sample(); r = m.regime(); assert.equal(r.solChange["15m"].changePct, 10); assert.equal(r.solChange["1h"].changePct, null);
  for (let i = 0; i < 10; i++) { const mint = `Mint${i}`; h(sw({ mint, create: true, price: 1e-8 })); h(sw({ mint, price: 2e-8 })); if (i < 4) h(sw({ mint, side: "sell", solAmount: 0.5, price: 1e-8 })); }
  r = m.regime(); assert.equal(r.netFlow["1m"].complete, false); assert.equal(r.netFlow["1m"].netSol, null); assert.equal(r.netFlow["1m"].partial.buySol, 20); assert.equal(r.netFlow["1m"].partial.sellSol, 2); assert.equal(r.netFlow["1m"].partial.netSol, 18);
  assert.equal(r.recentCurveTokens.count, 0); // too young
  now += 61_000; r = m.regime(); assert.equal(r.recentCurveTokens.count, 10); assert.equal(r.recentCurveTokens.drawdownOver30Pct, 4); assert.equal(r.recentCurveTokens.pct, 40);
  now += 300_000; h(sw({ mint: "Late", solAmount: 1 })); r = m.regime(); assert.equal(r.netFlow["5m"].complete, true); assert.equal(r.netFlow["1h"].complete, false); assert.equal(r.netFlow["1h"].netSol, null);
  assert.equal(r.labelStatus, "ready"); assert.ok(["risk-on", "neutral", "risk-off"].includes(r.label));
  m.close();
});

test("auto-warm: watched wallet / subscribed mint triggers one rate-limited backfill", async () => {
  const sigs: string[] = []; const eng = stub({ watched: (w: string) => w === "TAG", mintWatched: () => false, tagOf: () => undefined });
  const m = new PumpMarket(eng, async (meth: string, p: any[]) => { if (meth === "getSignaturesForAddress") sigs.push(p[0]); return []; }, { warmPerMin: 2 }).start();
  const h = eng.hooks[0];
  h(sw({ wallet: "TAG", mint: MINT })); h(sw({ wallet: "TAG", mint: MINT })); h(sw({ wallet: "X", mint: "So11111111111111111111111111111111111111112" }));
  h(sw({ wallet: "TAG", mint: "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY2NoAjW1dEYN6EM" })); h(sw({ wallet: "TAG", mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" }));
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(m.warmed, 2); assert.equal(sigs.length, 2); m.close();
});
