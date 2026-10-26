// getTokenLargestAccounts fallback: streamed getProgramAccounts top-N (mocked upstream; no network)
import { test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { createTokenLargest, scanTopAmounts, b58encode } from "../src/index.js";

const pk = () => b58encode(randomBytes(32));
const amt = (n: bigint) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(n); return b.toString("base64"); };
function streamOf(s: string, chunk = 37) { const enc = new TextEncoder(); let i = 0; return new ReadableStream<Uint8Array>({ pull(c) { if (i >= s.length) return c.close(); c.enqueue(enc.encode(s.slice(i, i + chunk))); i += chunk; } }); }
const body = (items: [string, bigint][], ctx = true) => { const arr = items.map(([p, a]) => `{"pubkey":"${p}","account":{"lamports":2039280,"data":["${amt(a)}","base64"],"owner":"x","executable":false,"rentEpoch":0,"space":8}}`).join(",");
  return ctx ? `{"jsonrpc":"2.0","result":{"context":{"slot":5},"value":[${arr}]},"id":1}` : `{"jsonrpc":"2.0","result":[${arr}],"id":1}`; };

test("scanTopAmounts: top-N across tiny chunks, bigints, both shapes", async () => {
  const items: [string, bigint][] = Array.from({ length: 500 }, (_, i) => [pk(), BigInt(i) * 1_000_000_007n]);
  items.push([pk(), 18_000_000_000_000_000_000n]);
  for (const ctx of [true, false]) {
    const { best, scanned } = await scanTopAmounts(streamOf(body(items, ctx), 13), 20);
    assert.equal(scanned, 501); assert.equal(best.length, 20); assert.equal(best[0].raw, 18_000_000_000_000_000_000n); assert.equal(best[1].raw, 499n * 1_000_000_007n);
    for (let i = 1; i < best.length; i++) assert.ok(best[i - 1].raw >= best[i].raw);
  }
  await assert.rejects(scanTopAmounts(streamOf('{"jsonrpc":"2.0","error":{"code":-32602,"message":"Indexed requests require a personal token"},"id":1}'), 20), /personal token/);
});

test("createTokenLargest: Token vs Token-2022 filters, decimals, failover, cache", async () => {
  const MINT = pk(), M22 = pk(); const seen: any[] = []; let calls = 0;
  const rpc = async (m: string, p: any[]) => m === "getSlot" ? 99 : ({ value: { owner: p[0] === MINT ? "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" : "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", data: { parsed: { type: "mint", info: { decimals: 6 } } } } });
  const fetcher: any = async (url: string, init: any) => { calls++; const b = JSON.parse(init.body); seen.push({ url, b });
    if (url === "http://blocked") return new Response('{"jsonrpc":"2.0","error":{"code":410,"message":"disabled"},"id":1}');
    return new Response(streamOf(body([[pk(), 1_500_000n], [pk(), 25n], [pk(), 3_000_000_000n]])), { status: 200 }); };
  const f = createTokenLargest(rpc, { urls: ["http://blocked", "http://ok"], fetcher });
  const r = await f(MINT);
  assert.equal(r.context.slot, 99); assert.equal(r.value.length, 3);
  assert.deepEqual(r.value[0], { address: r.value[0].address, amount: "3000000000", decimals: 6, uiAmount: 3000, uiAmountString: "3000" });
  assert.equal(r.value[1].uiAmountString, "1.5"); assert.equal(r.value[2].uiAmountString, "0.000025");
  assert.equal(r._meta.upstream, "ok");
  const tokFilters = seen[1].b.params[1].filters; assert.deepEqual(tokFilters, [{ memcmp: { offset: 0, bytes: MINT } }, { dataSize: 165 }]);
  assert.deepEqual(seen[1].b.params[1].dataSlice, { offset: 64, length: 8 });
  const n = calls; await f(MINT); assert.equal(calls, n); // cached
  await f(M22); const last = seen.at(-1).b.params; assert.equal(last[0], "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"); assert.deepEqual(last[1].filters, [{ memcmp: { offset: 0, bytes: M22 } }]);
  await assert.rejects(f("not-a-key"), /base58/);
});
