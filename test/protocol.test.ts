import { test } from "node:test";
import assert from "node:assert/strict";
import { encode, FrameDecoder, MsgType, HDR } from "../src/protocol.js";

const collect = (opts: any = {}) => {
  const out: any[] = []; const errs: Error[] = [];
  const d = new FrameDecoder((type, id, payload) => out.push({ type, id, payload }), { onError: (e) => errs.push(e), ...opts });
  return { d, out, errs };
};

test("roundtrip encode/decode", () => {
  const { d, out } = collect();
  d.push(encode(MsgType.Request, 7, ["add", [1, 2]]));
  assert.deepEqual(out, [{ type: MsgType.Request, id: 7, payload: ["add", [1, 2]] }]);
});

test("frames split byte-by-byte are reassembled", () => {
  const { d, out } = collect();
  const f = encode(MsgType.Response, 42, { big: "x".repeat(1000), n: [1, 2, 3] });
  for (const b of f) d.push(Buffer.from([b]));
  assert.equal(out.length, 1);
  assert.equal(out[0].payload.big.length, 1000);
});

test("multiple frames merged in one chunk + trailing partial", () => {
  const { d, out } = collect();
  const fs = Array.from({ length: 50 }, (_, i) => encode(MsgType.Notification, i, { seq: i }));
  const all = Buffer.concat(fs);
  d.push(all.subarray(0, all.length - 3));
  assert.equal(out.length, 49);
  d.push(all.subarray(all.length - 3));
  assert.equal(out.length, 50);
  assert.deepEqual(out.map((f) => f.id), [...Array(50).keys()]);
});

test("u32 ids wrap correctly", () => {
  const { d, out } = collect();
  d.push(encode(MsgType.Request, 0xffffffff, null));
  assert.equal(out[0].id, 0xffffffff);
});

test("oversized frame is rejected before buffering body", () => {
  const { d, out, errs } = collect({ maxFrame: 1024 });
  const h = Buffer.alloc(HDR); h.writeUInt32BE(10_000_000, 0);
  d.push(h);
  assert.equal(out.length, 0); assert.equal(errs.length, 1); assert.match(errs[0].message, /too large/);
  assert.equal(d.dead, true);
  d.push(encode(MsgType.Request, 1, null)); // ignored after failure
  assert.equal(out.length, 0);
});

test("undersized length, unknown type, malformed msgpack -> error", () => {
  const short = collect(); const b = Buffer.alloc(8); b.writeUInt32BE(2, 0); short.d.push(b);
  assert.match(short.errs[0].message, /too short/);

  const unk = collect(); const f = encode(MsgType.Request, 1, 1); f.writeUInt8(99, 4); unk.d.push(f);
  assert.match(unk.errs[0].message, /unknown frame type/);

  const bad = collect(); const g = Buffer.alloc(HDR + 3); g.writeUInt32BE(8, 0); g.writeUInt8(0, 4); g.writeUInt32BE(1, 5); g.set([0xdb, 0, 0], HDR); // truncated str32
  bad.d.push(g);
  assert.match(bad.errs[0].message, /bad msgpack/);
});

test("decoder without onError throws", () => {
  const d = new FrameDecoder(() => {});
  const b = Buffer.alloc(8); b.writeUInt32BE(1, 0);
  assert.throws(() => d.push(b));
});
