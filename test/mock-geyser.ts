// Local gRPC server implementing the Geyser.Subscribe (+Ping/GetVersion) subset, using the package's own protobuf codecs.
import * as grpc from "@grpc/grpc-js";
// @ts-ignore - generated ts-proto codecs shipped with the client package
import { SubscribeRequest, SubscribeUpdate, PingRequest, PongResponse, GetVersionRequest, GetVersionResponse } from "../node_modules/@triton-one/yellowstone-grpc/dist/esm/grpc/geyser.js";

// prost (used by the native client) omits empty map values; ts-proto's decoder then drops the key. Recover keys
// of map fields whose value message is empty (slots=2, blocks=4, blocks_meta=5, entry=9) by scanning the raw bytes.
const MAPS: Record<number, string> = { 2: "slots", 4: "blocks", 5: "blocksMeta", 9: "entry" };
function mapKeys(b: Buffer) {
  const out: Record<string, string[]> = {}; let o = 0;
  const varint = () => { let r = 0, sh = 0, x; do { x = b[o++]; r += (x & 0x7f) * 2 ** sh; sh += 7; } while (x & 0x80); return r; };
  while (o < b.length) {
    const tag = varint(); const f = Math.floor(tag / 8), wt = tag & 7;
    if (wt === 0) { varint(); continue; } if (wt === 1) { o += 8; continue; } if (wt === 5) { o += 4; continue; }
    const len = varint(); const sub = b.subarray(o, o + len); o += len;
    if (MAPS[f] && sub[0] === 0x0a) { const kl = sub[1]; (out[MAPS[f]] ??= []).push(sub.subarray(2, 2 + kl).toString()); }
  }
  return out;
}
const codec = (M: any) => ({ ser: (m: any) => Buffer.from(M.encode(M.fromPartial(m)).finish()), de: (b: Buffer) => M.decode(b) });
export async function startMockGeyser(token?: string) {
  const killed: any[] = [];
  const calls: { requests: any[]; call: grpc.ServerDuplexStream<any, any> }[] = [];
  const server = new grpc.Server();
  const rq0 = codec(SubscribeRequest), up = codec(SubscribeUpdate);
  const rq = { ser: rq0.ser, de: (b: Buffer) => { const m = rq0.de(b); for (const [k, keys] of Object.entries(mapKeys(b))) for (const key of keys) m[k][key] ??= {}; return m; } };
  const svc: grpc.ServiceDefinition = {
    subscribe: { path: "/geyser.Geyser/Subscribe", requestStream: true, responseStream: true, requestSerialize: rq.ser, requestDeserialize: rq.de, responseSerialize: up.ser, responseDeserialize: up.de } as any,
    ping: { path: "/geyser.Geyser/Ping", requestStream: false, responseStream: false, requestSerialize: codec(PingRequest).ser, requestDeserialize: codec(PingRequest).de, responseSerialize: codec(PongResponse).ser, responseDeserialize: codec(PongResponse).de } as any,
    getVersion: { path: "/geyser.Geyser/GetVersion", requestStream: false, responseStream: false, requestSerialize: codec(GetVersionRequest).ser, requestDeserialize: codec(GetVersionRequest).de, responseSerialize: codec(GetVersionResponse).ser, responseDeserialize: codec(GetVersionResponse).de } as any,
  };
  const st = { tokens: [] as (string | undefined)[] };
  server.addService(svc, {
    subscribe: (call: grpc.ServerDuplexStream<any, any>) => {
      st.tokens.push(call.metadata.get("x-token")[0]?.toString());
      if (token && call.metadata.get("x-token")[0]?.toString() !== token) { call.emit("error", { code: grpc.status.UNAUTHENTICATED, details: "bad token" }); return; }
      call.sendMetadata(new grpc.Metadata());
      const e = { requests: [] as any[], call }; calls.push(e);
      call.on("data", (r: any) => e.requests.push(r));
      call.on("error", () => {}); call.on("end", () => call.end());
    },
    ping: (c: any, cb: any) => cb(null, { count: c.request.count }),
    getVersion: (_c: any, cb: any) => cb(null, { version: "mock-geyser" }),
  });
  const port = await new Promise<number>((res, rej) => server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (e, p) => (e ? rej(e) : res(p))));
  return {
    url: `http://127.0.0.1:${port}`, calls, st,
    /** last non-ping request of the latest stream */
    lastRequest: () => calls.at(-1)?.requests.filter((r) => !r.ping || Object.keys(r.transactions ?? {}).length || Object.keys(r.slots ?? {}).length).at(-1),
    send: (u: any) => calls.at(-1)?.call.write({ createdAt: new Date(), ...u }),
    kill: () => { for (const c of calls.splice(0)) { killed.push(c); c.call.emit("error", { code: grpc.status.UNAVAILABLE, details: "killed" }); } },
    close: async () => { server.forceShutdown(); },
  };
}
