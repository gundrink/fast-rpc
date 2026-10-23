# fast-rpc

Fast TypeScript RPC over TCP (MessagePack, multiplexed) that fronts Solana **read-only** JSON-RPC. The same port also speaks HTTP JSON-RPC for `curl`.

## Endpoint layer (request lifecycle)

1. **TCP listen**: `RpcServer.listen(port)` uses `net.createServer`, with `setNoDelay` per socket.
2. **Sniff**: the first 4 bytes of a connection are checked. `POST`/`GET `/`HEAD`/`OPTI` means HTTP text mode. Anything else is binary mode.
3. **Parse / frame decode**
   - Binary: `FrameDecoder` (src/protocol.ts) reads `[u32 len][u8 type][u32 reqId][msgpack payload]` and handles split or coalesced TCP chunks. The payload is `[method, args[]]`.
   - HTTP: reads headers up to `\r\n\r\n`, then a `Content-Length` body, parsed as JSON-RPC 2.0 (single or batch, keep-alive supported).
4. **Dispatch**: `handlers.get(method)(...args)`. Requests run concurrently, so responses can return out of order and are matched by `reqId`. Unknown methods return `-32601`. Only whitelisted read methods are registered, so write calls like `sendTransaction` are rejected.
5. **Handler → upstream** (src/solana.ts): `fetch` POST to `SOLANA_RPC_URL` with `AbortSignal.timeout`, a short TTL cache, and in-flight dedupe (concurrent identical calls share one upstream request).
6. **Encode response**: binary sends a `Response`/`Error` frame with the same reqId. HTTP sends `200` + `application/json` `{jsonrpc,id,result|error}`.

Client (`src/client.ts`): one socket, a pending map keyed by reqId, writes batched per microtask, and `client.proxy<SolanaApi>()` so `await remote.getSlot()` reads like a local call.

## Streaming layer (LaserStream-style)

```
 clients (binary TCP / SSE)            fast-rpc server                         Solana
 ┌──────────┐ Subscribe{type,params,   ┌──────────────────────────────┐
 │ client A │──fromSeq|fromSlot}──────▶│ RpcServer (sniff: bin|HTTP)  │
 │ client B │◀─Notification{seq,slot}──│   └ per-conn subs, backpress.│
 │ curl SSE │◀─ id: seq / data: json ──│ StreamHub                    │
 └──────────┘   Ping/Pong keepalive    │   topic "slot:{}"  ─┐        │   one WS, auto-reconnect
                                       │   topic "account:{pubkey}" ─┼────▶│ wss://api.mainnet-beta…
                                       │   topic "logs:{mentions}"  ─┘    │ slot/account/logsSubscribe
                                       │   each: seq counter, dedup set,  │
                                       │         ring buffer (N entries)  │
                                       └──────────────────────────────┘
```

- **Frames** (src/protocol.ts): `Subscribe=3` (id=subId, `{type, params, fromSeq?, fromSlot?}`) → ack `Response {ok, topic, headSeq, replayed}`; `Notification=4` (id=subId, `{seq, slot, data}`); `Unsubscribe=5`; `Ping=6`/`Pong=7`.
- **Fan-out + dedup** (src/stream.ts): topic key = `type:params`. The first subscriber opens one upstream subscription and later ones share it. Upstream messages are deduped (slot number / account context slot / log signature), so an upstream reconnect doesn't double-deliver. Idle topics linger 10s so reconnecting clients can still replay.
- **Sequence numbers + ring buffer**: each topic stamps a monotonically increasing `seq` and keeps the last `STREAM_RING` (2000) entries. `fromSeq` replays `seq > fromSeq`; `fromSlot` replays `slot >= fromSlot`.
- **At-least-once + resume** (src/client.ts): the client tracks `lastSeq` per sub. On socket loss it reconnects with exponential backoff and re-sends `Subscribe{fromSeq:lastSeq}`. The server replays the gap, and the client drops `seq <= lastSeq` duplicates.
- **Keepalive**: the client pings every 5s. With no pong for 15s it kills the socket and reconnects. The upstream WS is pinged every 20s.
- **Backpressure**: when a socket buffers more than `STREAM_SOFT_LIMIT` (1MB), notifications to it are dropped. The client then sees a seq gap and resumes from the ring. Above `STREAM_HARD_LIMIT` (8MB) the slow consumer is disconnected.
- **Upstream** (src/upstream-ws.ts): one shared `ws` connection to `SOLANA_WS_URL` (default `wss://api.mainnet-beta.solana.com`). It resubscribes all handles after a reconnect.
- **Limitation**: seq/ring live in server memory, so replay doesn't survive a server restart.

Client API:
```ts
const c = await RpcClient.connect(7070);
const unsub = await c.subscribe("slot", {}, (m) => console.log(m.seq, m.data.slot));
await c.subscribe("account", { pubkey: "SysvarC1ock11111111111111111111111111111111" }, onMsg);
await c.subscribe("logs", { mentions: "<programId>" }, onMsg, { fromSlot: 123 });
for await (const m of c.stream("slot", {})) { /* async iterator */ }
await unsub();
```
SSE (text mode, same port): `curl -N 'localhost:7070/stream?type=slot'`. Also `type=account&pubkey=…`, `type=logs&mentions=…`, `fromSeq`/`fromSlot`, and `Last-Event-ID` for resume.

## Methods
`getHealth` (1s cache), `getSlot` (400ms), `getBlockHeight` (400ms), `getBalance(pubkey)` (2s), `getLatestBlockhash` (400ms), `getVersion` (60s).

## Run
```
npm install
npm run demo                 # starts server in-process + binary client against mainnet
npm run serve                # standalone server on :7070 (RPC + streams)
npm run stream               # streaming demo (slot, account, fan-out, reconnect+replay, fromSlot)
npm run typecheck
NO_SERVER=1 npm run demo     # binary client against running server
curl -X POST localhost:7070 -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}'
curl -X POST localhost:7070 -d '{"jsonrpc":"2.0","id":2,"method":"getBalance","params":["SysvarRent111111111111111111111111111111111"]}'
curl -N 'localhost:7070/stream?type=slot'
```
Env: `SOLANA_WS_URL`, `STREAM_RING`, `STREAM_SOFT_LIMIT`, `STREAM_HARD_LIMIT`, `DEMO_SECS`, `SOLANA_RPC_URL` (default `https://api.mainnet-beta.solana.com`), `SOLANA_TIMEOUT_MS` (5000), `PORT` (7070), `HOST` (127.0.0.1).
The public mainnet endpoint is rate-limited, so use your own RPC URL for heavy load.
