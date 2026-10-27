import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import WebSocket from "ws";
import { stepBasis, createFastRpc, decodePumpTrade, decodeAmmEvent, parsePumpLogs, decodeBondingCurve, curveMetrics, curvePda, PUMP_DISC, b58decode, WSOL, ammEvent, decodePythPrice } from "../src/index.js";
import { startMockSolana, waitFor } from "./helpers.js";

const fx = JSON.parse(fs.readFileSync(new URL("./fixtures/pump-logs.json", import.meta.url), "utf8"));
const MINT = "Hqni8pYDuwytkdysYautBGy71bPwpQstvMoUgpCApdEy", USER = "FqZC1pV1UU6V3Cer2MArPP1p9U4quPpsGyHC4TMT9Mg";
let mock: Awaited<ReturnType<typeof startMockSolana>>;
before(async () => { mock = await startMockSolana(); });
after(() => mock.close?.());

function tradeLog(o: { mint: string; user: string; buy: boolean; sol: bigint; tok: bigint; vsol: bigint; vtok: bigint; rtok: bigint }) {
  const b = Buffer.alloc(8 + 32 + 8 + 8 + 1 + 32 + 8 * 5);
  let p = 0; Buffer.from(PUMP_DISC.trade, "hex").copy(b, p); p += 8;
  b58decode(o.mint).copy(b, p); p += 32; b.writeBigUInt64LE(o.sol, p); p += 8; b.writeBigUInt64LE(o.tok, p); p += 8; b[p++] = o.buy ? 1 : 0;
  b58decode(o.user).copy(b, p); p += 32; b.writeBigInt64LE(1_793_000_000n, p); p += 8; b.writeBigUInt64LE(o.vsol, p); p += 8; b.writeBigUInt64LE(o.vtok, p); p += 8; p += 8; b.writeBigUInt64LE(o.rtok, p);
  return "Program data: " + b.toString("base64");
}

test("decodes real mainnet pump TradeEvent and PumpSwap SellEvent", () => {
  const t = decodePumpTrade(Buffer.from(fx.pumpLog.slice(14), "base64"));
  assert.equal(t.mint, MINT); assert.equal(t.user, USER); assert.equal(t.isBuy, false); assert.equal(t.tokenAmount, 81511598495n);
  const a = decodeAmmEvent(Buffer.from(fx.ammSellLog.slice(14), "base64"), false);
  assert.equal(a.pool, "8P66kdMCJynUgvsWRPkXES4Xn5zdgLcbJ1oo7nQUWRkb"); assert.equal(a.user, "4x5zKgYFcWKzDkLLADY7MRXXuMjB5qz3pEdkkcmNVWiY");
  assert.equal(a.baseAmount, 550989n); assert.equal(a.quoteAmount, 7303n);
  const r = parsePumpLogs([fx.pumpLog, fx.ammSellLog, "Program log: x"]);
  assert.equal(r.events.length, 2); assert.equal(r.create, false);
  // same discriminator emitted by some other program must be ignored
  const foreign = parsePumpLogs(["Program Other1111111111111111111111111111111111 invoke [1]", fx.ammSellLog, "Program Other1111111111111111111111111111111111 success",
    `Program ${"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"} invoke [1]`, fx.ammSellLog, "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA success"]);
  assert.equal(foreign.events.length, 1);
});

test("bonding curve decode + metrics + PDA", () => {
  const d = Buffer.alloc(8 + 40 + 1 + 32); Buffer.from(PUMP_DISC.curve, "hex").copy(d);
  d.writeBigUInt64LE(1_073_000_000_000_000n, 8); d.writeBigUInt64LE(30_000_000_000n, 16); d.writeBigUInt64LE(793_100_000_000_000n, 24); d.writeBigUInt64LE(0n, 32); d.writeBigUInt64LE(1_000_000_000_000_000n, 40); d[48] = 0;
  const c = decodeBondingCurve(d); const m = curveMetrics(c.virtualSolReserves, c.virtualTokenReserves, c.realTokenReserves);
  assert.equal(c.complete, false); assert.equal(m.progress, 0); assert.ok(Math.abs(m.price! - 2.8e-8) < 1e-9); assert.ok(Math.abs(m.marketCapSol! - 27.96) < 0.1);
  assert.equal(curvePda(MINT), curvePda(MINT)); assert.notEqual(curvePda(MINT), MINT);
  assert.throws(() => decodeBondingCurve(Buffer.alloc(81)));
});

test("WS pumpSwapSubscribe: processed event, wallet tag, pctOfPositionSold follow-up, confirmed follow-up, pumpCurvePrice", async () => {
  const W = "4F6ZbYX2ZRd7zV14zmY3dciTnm14hWD9prskde3dnPSa";
  mock.responses.getTokenAccountsByOwner = () => ({ context: { slot: 5 }, value: [{ account: { data: { parsed: { info: { tokenAmount: { amount: "300" } } } } } }] });
  mock.responses.getSignatureStatuses = (p: any) => ({ context: { slot: 6 }, value: p[0].map(() => ({ confirmationStatus: "confirmed", err: null })) });
  const curve = Buffer.alloc(81); Buffer.from(PUMP_DISC.curve, "hex").copy(curve); curve.writeBigUInt64LE(1_000_000_000_000_000n, 8); curve.writeBigUInt64LE(40_000_000_000n, 16); curve.writeBigUInt64LE(396_550_000_000_000n, 24); curve.writeBigUInt64LE(1_000_000_000_000_000n, 40);
  mock.responses.getAccountInfo = (p: any) => p[0] === curvePda(MINT) ? { context: { slot: 7 }, value: { data: [curve.toString("base64"), "base64"], owner: "x", lamports: 1, executable: false } } : { context: { slot: 7 }, value: null };
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl, lingerMs: 50 }, webhooks: false, chain: { detect: false } });
  const port = await app.server.listen(0);
  const ws = new WebSocket(`ws://127.0.0.1:${port}`); await new Promise((r) => ws.on("open", r));
  const got: any[] = []; ws.on("message", (d) => got.push(JSON.parse(String(d))));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ wallets: [W], tags: { [W]: "dump-leader" }, confirmations: true }] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "pumpSwapSubscribe", params: [{}] }));
  await waitFor(() => got.length >= 2 && mock.activeSubs("logsSubscribe") === 2);
  assert.equal(got.find((m) => m.id === 2).error.code, -32602);
  const sid = got.find((m) => m.id === 1).result;
  mock.emit("logsSubscribe", { context: { slot: 9 }, value: { signature: "S1", err: null, logs: [tradeLog({ mint: MINT, user: W, buy: false, sol: 1_000_000n, tok: 100n, vsol: 30_000_000_000n, vtok: 1_000_000_000_000_000n, rtok: 793_100_000_000_000n })] } });
  await waitFor(() => got.filter((m) => m.method === "pumpSwapNotification").length >= 3, 4000);
  const ev = got.filter((m) => m.method === "pumpSwapNotification").map((m) => m.params.result);
  const first = ev[0]; assert.equal(got.find((m) => m.method)!.params.subscription, sid);
  assert.equal(first.commitment, "processed"); assert.equal(first.tag, "dump-leader"); assert.equal(first.side, "sell"); assert.equal(first.mint, MINT); assert.equal(first.pctOfPositionSold, null);
  assert.equal(ev.find((e) => e.update === "position").pctOfPositionSold, 0.25); // 100 / (300 post + 100 sold)
  assert.equal(ev.find((e) => e.update === "confirmed").commitment, "confirmed");
  const r = await fetch(`http://127.0.0.1:${port}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 3, method: "pumpCurvePrice", params: [MINT] }) }).then((x) => x.json());
  assert.equal(r.result.progress, 50); assert.equal(r.result.complete, false); assert.ok(Math.abs(r.result.price - 4e-8) < 1e-12);
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 4, method: "pumpSwapUnsubscribe", params: [sid] }));
  await waitFor(() => got.some((m) => m.id === 4 && m.result === true));
  await waitFor(() => mock.activeSubs("logsSubscribe") === 0, 3000);
  ws.close(); await app.close();
});
void WSOL;

test("stepBasis: once per threshold, re-arm on new high, rebound", () => {
  const b = { high: 100, fired: new Set<number>() } as any; const A = [10, 20];
  assert.deepEqual(stepBasis(b, 95, A, 5), []);
  assert.deepEqual(stepBasis(b, 85, A, 5).map((x) => x.threshold), [10]);
  assert.deepEqual(stepBasis(b, 86, A, 5), []);
  assert.deepEqual(stepBasis(b, 75, A, 5).map((x) => x.threshold), [20]);
  assert.deepEqual(stepBasis(b, 70, A, 5), []);
  const r = stepBasis(b, 74, A, 5); assert.equal(r[0].kind, "rebound"); assert.equal(r[0].low, 70);
  assert.deepEqual(stepBasis(b, 74.5, A, 5), []); // rebound once per low
  assert.deepEqual(stepBasis(b, 110, A, 5), []); assert.equal(b.high, 110);
  assert.deepEqual(stepBasis(b, 98, A, 5).map((x) => x.threshold), [10]); // re-armed
});

test("WS drawdown tracking: high/drawdown fields, firstTag basis follows the mint, alerts", async () => {
  const W = "4F6ZbYX2ZRd7zV14zmY3dciTnm14hWD9prskde3dnPSa", O = "DbPzFfzmaFq1KHyqUk4SrfvQnXVokyui9sJg4a2JW8SK";
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl, lingerMs: 50 }, webhooks: false, chain: { detect: false } });
  const port = await app.server.listen(0);
  const ws = new WebSocket(`ws://127.0.0.1:${port}`); await new Promise((r) => ws.on("open", r));
  const got: any[] = []; ws.on("message", (d) => got.push(JSON.parse(String(d))));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ wallets: [W], tags: { [W]: "pump-confirm" }, drawdownAlerts: [10, 20], reboundPct: 5, position: false }] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "pumpSwapSubscribe", params: [{ allSwaps: true, drawdownAlerts: [200] }] }));
  await waitFor(() => got.filter((m) => m.id).length === 2 && mock.activeSubs("logsSubscribe") === 2);
  assert.equal(got.find((m) => m.id === 2).error.code, -32602);
  let i = 0;
  const trade = (user: string, vsolSol: number) => mock.emit("logsSubscribe", { context: { slot: 10 + i }, value: { signature: `D${i++}`, err: null,
    logs: [tradeLog({ mint: MINT, user, buy: true, sol: 1_000_000_000n, tok: 1n, vsol: BigInt(vsolSol * 1e9), vtok: 1_000_000_000_000_000n, rtok: 700_000_000_000_000n })] } });
  trade(W, 30); await waitFor(() => got.some((m) => m.params?.result?.signature === "D0"));
  for (const v of [26, 23, 24.5, 33, 29]) { trade(O, v); await waitFor(() => got.some((m) => m.params?.result?.signature === `D${i - 1}` && m.params.result.type === "swap")); }
  await new Promise((r) => setTimeout(r, 100));
  const res = got.filter((m) => m.method === "pumpSwapNotification").map((m) => m.params.result);
  const swaps = res.filter((e) => e.type === "swap"), al = res.filter((e) => e.type !== "swap");
  assert.equal(swaps.length, 6); // other wallet's swaps on the tagged mint are streamed
  assert.equal(swaps[0].tag, "pump-confirm"); assert.equal(swaps[0].drawdownSinceTagPct, 0);
  assert.ok(Math.abs(swaps[2].drawdownPct - 23.33) < 0.01); assert.ok(Math.abs(swaps[4].highSinceFirstTag - 3.3e-8) < 1e-15);
  const dd = al.filter((a) => a.type === "drawdown");
  assert.deepEqual(dd.map((a) => `${a.basis}:${a.threshold}`), ["subscribe:10", "firstTag:10", "subscribe:20", "firstTag:20", "subscribe:10", "firstTag:10"]);
  assert.equal(al.filter((a) => a.type === "rebound").length, 2);
  assert.equal(dd[0].mint, MINT); assert.ok(dd[0].high > dd[0].price);
  ws.close(); await app.close();
});

test("ammEvent: WSOL as quote vs base orientation, non-SOL / non-canonical / no-pool validity", () => {
  const T = "So11111111111111111111111111111111111111112".replace(/^So1+2$/, "TokenMint1111111111111111111111111111111111");
  const t = { user: "U", pool: "P", buy: true, timestamp: 1n, baseAmount: 1_000_000n, quoteAmount: 2_000_000_000n, baseAfter: 500_000_000_000n, quoteAfter: 50_000_000_000n } as any;
  const q = ammEvent(t, { base: T, quote: WSOL, baseDec: 6, quoteDec: 9, canonical: true }, "s", 1, 0);
  assert.equal(q.mint, T); assert.equal(q.side, "buy"); assert.equal(q.solAmount, 2); assert.equal(q.venue, "amm"); assert.equal(q.priceValid, true);
  assert.ok(Math.abs(q.price! - 50 / 500_000) < 1e-15);
  // WSOL as base: base side is SOL, buying base (SOL) == selling the token
  const b = ammEvent({ ...t, baseAfter: 50_000_000_000n, quoteAfter: 500_000_000_000n }, { base: WSOL, quote: T, baseDec: 9, quoteDec: 6, canonical: false }, "s", 1, 0);
  assert.equal(b.mint, T); assert.equal(b.side, "sell"); assert.equal(b.solAmount, 0.001); assert.equal(b.venue, "amm-other");
  assert.equal(b.priceValid, false); assert.equal(b.priceInvalidReason, "nonCanonicalPool"); assert.ok(Math.abs(b.price! - 50 / 500_000) < 1e-15);
  const n = ammEvent(t, { base: T, quote: "Other", baseDec: 6, quoteDec: 6, canonical: false }, "s", 1, 0);
  assert.equal(n.priceValid, false); assert.equal(n.priceInvalidReason, "nonSolPool"); assert.equal(n.price, null);
  assert.equal(ammEvent(t, null, "s", 1, 0).priceInvalidReason, "noPool");
  const d = ammEvent(t, { base: T, quote: WSOL, baseDec: null, quoteDec: 9, canonical: true }, "s", 1, 0);
  assert.equal(d.priceInvalidReason, "badReserves");
});

test("decodePythPrice: PriceUpdateV2 layout", () => {
  const b = Buffer.alloc(134); b[40] = 1; const o = 41 + 32;
  b.writeBigInt64LE(9915000000n, o); b.writeBigUInt64LE(5000000n, o + 8); b.writeInt32LE(-8, o + 16); b.writeBigInt64LE(1700000000n, o + 20);
  const v = decodePythPrice(b)!; assert.ok(Math.abs(v.price - 99.15) < 1e-9); assert.equal(v.publishTime, 1700000000);
});

test("outlier guard (rolling median, regime change) + dust flag", async () => {
  const app = createFastRpc({ api: { url: mock.httpUrl }, hub: { wsUrl: mock.wsUrl, lingerMs: 50 }, webhooks: false, chain: { detect: false } });
  const port = await app.server.listen(0);
  const ws = new WebSocket(`ws://127.0.0.1:${port}`); await new Promise((r) => ws.on("open", r));
  const got: any[] = []; ws.on("message", (d) => got.push(JSON.parse(String(d))));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ mints: [MINT], position: false }] }));
  await waitFor(() => got.some((m) => m.id === 1) && mock.activeSubs("logsSubscribe") >= 1);
  let i = 0; const U = "DbPzFfzmaFq1KHyqUk4SrfvQnXVokyui9sJg4a2JW8SK";
  const seq: [number, bigint][] = [[30, 10_000_000n], [30, 10_000_000n], [31, 10_000_000n], [300, 10_000_000n], [30, 1n], [3, 10_000_000n], [3.1, 10_000_000n], [3, 10_000_000n], [3.05, 10_000_000n]];
  for (const [v, sol] of seq) { const s = `O${i++}`; mock.emit("logsSubscribe", { context: { slot: 100 + i }, value: { signature: s, err: null,
    logs: [tradeLog({ mint: MINT, user: U, buy: true, sol, tok: 1n, vsol: BigInt(Math.round(v * 1e9)), vtok: 1_000_000_000_000_000n, rtok: 700_000_000_000_000n })] } });
    await waitFor(() => got.some((m) => m.params?.result?.signature === s)); }
  const sw = got.filter((m) => m.method === "pumpSwapNotification").map((m) => m.params.result).filter((e) => e.type === "swap");
  assert.deepEqual(sw.map((e) => e.priceValid), [true, true, true, false, true, false, false, true, true]);
  assert.equal(sw[3].priceInvalidReason, "outlier"); assert.equal(sw[4].dust, true); assert.equal(sw[0].venue, "curve"); assert.equal(sw[0].priceSource, "curveEventReserves");
  ws.close(); await app.close();
});
