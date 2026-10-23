import { test } from "node:test";
import assert from "node:assert/strict";
import { StreamHub, type Upstream, type Entry } from "../src/index.js";

function fakeUpstream() {
  const subs: { method: string; params: any[]; on: (r: any) => void; closed: boolean }[] = [];
  const up: Upstream = { subscribe(method, params, on) { const s = { method, params, on, closed: false }; subs.push(s); return () => { s.closed = true; }; } };
  return { up, subs, push: (r: any) => subs.filter((s) => !s.closed).forEach((s) => s.on(r)) };
}
const slot = (n: number) => ({ slot: n, parent: n - 1, root: n - 32 });

test("fan-out: many subscribers share one upstream sub; dedup + monotonically increasing seq", () => {
  const f = fakeUpstream(); const hub = new StreamHub({ upstream: f.up, lingerMs: 10 });
  const a: Entry[] = [], b: Entry[] = [];
  hub.subscribe("slot", {}, { deliver: (e) => a.push(e) });
  hub.subscribe("slot", {}, { deliver: (e) => b.push(e) });
  assert.equal(f.subs.length, 1);
  f.push(slot(10)); f.push(slot(11)); f.push(slot(11)); f.push(slot(10)); f.push(slot(12));
  assert.deepEqual(a.map((e) => e.seq), [1, 2, 3]);
  assert.deepEqual(a.map((e) => e.slot), [10, 11, 12]);
  assert.deepEqual(b, a);
  hub.close();
});

test("different params => different topics; malformed upstream messages ignored", () => {
  const f = fakeUpstream(); const hub = new StreamHub({ upstream: f.up });
  const got: Entry[] = [];
  hub.subscribe("account", { pubkey: "A" }, { deliver: (e) => got.push(e) });
  hub.subscribe("account", { pubkey: "B" }, { deliver: () => {} });
  assert.equal(f.subs.length, 2);
  assert.deepEqual(f.subs[0].params, ["A", { commitment: "confirmed", encoding: "base64" }]);
  f.subs[0].on({ garbage: true });
  f.subs[0].on({ context: { slot: 5 }, value: { lamports: 1 } });
  assert.equal(got.length, 1); assert.deepEqual(got[0].data, { lamports: 1 }); assert.equal(got[0].slot, 5);
  hub.close();
});

test("ring buffer: bounded, replay fromSeq/fromSlot, lost count when ring overflowed", () => {
  const f = fakeUpstream(); const hub = new StreamHub({ upstream: f.up, ringSize: 5 });
  hub.subscribe("slot", {}, { deliver: () => {} });
  for (let s = 100; s < 110; s++) f.push(slot(s)); // seq 1..10, ring keeps 6..10
  const r1 = hub.subscribe("slot", {}, { deliver: () => {} }, { fromSeq: 7 });
  assert.deepEqual(r1.replay.map((e) => e.seq), [8, 9, 10]); assert.equal(r1.lost, 0);
  const r2 = hub.subscribe("slot", {}, { deliver: () => {} }, { fromSeq: 2 });
  assert.deepEqual(r2.replay.map((e) => e.seq), [6, 7, 8, 9, 10]); assert.equal(r2.lost, 3);
  const r3 = hub.subscribe("slot", {}, { deliver: () => {} }, { fromSlot: 108 });
  assert.deepEqual(r3.replay.map((e) => e.slot), [108, 109]);
  const r4 = hub.subscribe("slot", {}, { deliver: () => {} }, { fromSeq: 10 });
  assert.equal(r4.replay.length, 0); assert.equal(r4.lost, 0);
  assert.equal(hub.stats()[0].ring, 5);
  hub.close();
});

test("validation errors are -32602", () => {
  const hub = new StreamHub({ upstream: fakeUpstream().up });
  for (const [t, p] of [["account", []], ["logs", [{ mentions: ["a", "b"] }]], ["program", [5]], ["signature", [null]]] as const)
    assert.throws(() => hub.subscribeRaw(t as any, p as any, { deliver() {} }), (e: any) => e.code === -32602);
  assert.throws(() => hub.subscribe("nope" as any, {}, { deliver() {} }), (e: any) => e.code === -32602);
});

test("linger: upstream unsubscribed only after last subscriber leaves + linger; rejoin cancels", async () => {
  const f = fakeUpstream(); const hub = new StreamHub({ upstream: f.up, lingerMs: 30 });
  const s1 = hub.subscribe("slot", {}, { deliver() {} });
  s1.detach();
  hub.subscribe("slot", {}, { deliver() {} }); // rejoin within linger
  await new Promise((r) => setTimeout(r, 60));
  assert.equal(f.subs[0].closed, false);
  hub.close();
  assert.equal(f.subs[0].closed, true);
});

test("signature topic is one-shot", () => {
  const f = fakeUpstream(); const hub = new StreamHub({ upstream: f.up });
  let ended = 0, got = 0;
  hub.subscribe("signature", { signature: "S" }, { deliver: () => got++, onEnd: () => ended++ });
  f.push({ context: { slot: 1 }, value: { err: null } });
  assert.equal(got, 1); assert.equal(ended, 1); assert.equal(f.subs[0].closed, true);
  assert.equal(hub.stats().length, 0);
});
