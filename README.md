# fast-rpc

Fast TypeScript RPC gateway for Solana, inspired by Helius RPC / LaserStream. A **single TCP port** speaks:

| Protocol | Detected by | Use |
|---|---|---|
| Binary MessagePack frames (primary) | first 4 bytes are not an HTTP verb | `RpcClient`: multiplexed calls + resumable streams |
| HTTP JSON-RPC 2.0 (`POST /?api-key=…`) | `POST`/`GET `/… | Helius/Solana-style RPC, `curl`, web3.js `Connection` |
| Solana pubsub WebSocket (`ws://host:port/?api-key=…`) | HTTP `Upgrade: websocket` | `accountSubscribe`, `slotSubscribe`, `logsSubscribe`, … (Solana/Helius-identical shapes) |
| SSE `GET /stream?type=…` | HTTP GET | `curl -N` streaming |

All upstream access is **read-only** (whitelisted methods; `sendTransaction` etc. → `-32601`).

## Architecture

```
 clients                                   fast-rpc (one port)                                   Solana node
 ┌─────────────┐  binary frames      ┌───────────────────────────────────────────┐
 │ RpcClient   │◀──────────────────▶ │ TCP listen ─▶ sniff first 4 bytes          │
 │ web3/curl   │  HTTP JSON-RPC      │   ├ binary: FrameDecoder ─▶ dispatch       │  HTTP JSON-RPC
 │ ws clients  │  WS pubsub          │   └ HTTP (node:http): JSON-RPC / WS / SSE  │ ───────────────▶ SOLANA_RPC_URL
 │ curl -N SSE │  SSE                │ handlers (src/solana.ts): validate, TTL    │  (timeout, cache,
 └─────────────┘                     │   cache, in-flight dedupe, limits          │   in-flight dedupe)
                                     │ StreamHub (src/stream.ts):                 │
                                     │   topic = type+params ─ one upstream sub   │  one shared WS
                                     │   dedup ─ seq ─ ring buffer ─ fan-out      │ ───────────────▶ SOLANA_WS_URL
                                     │   per-consumer backpressure gate           │  (auto-reconnect,
                                     └───────────────────────────────────────────┘   resubscribe, ping)
```

### Request lifecycle
1. **TCP listen**: `net.createServer`. Each socket is sniffed until ≥4 bytes have arrived, so tiny first chunks are handled.
2. **Parse**
   - Binary: `FrameDecoder` reads `[u32 len][u8 type][u32 id][msgpack]`. It handles split and merged chunks. Frames that are undersized, oversized (`MAX_FRAME_BYTES`, 16MB), of unknown type, or with undecodable msgpack close only that connection.
   - HTTP: the socket is handed to `node:http` (keep-alive, pipelining, header and body limits).
3. **Dispatch**: positional params go to the handler. JSON-RPC errors are spec-compliant: `-32700` parse, `-32600` invalid request (including an empty batch), `-32601` method not found, `-32602` invalid params, and upstream codes pass through unchanged. Batches keep their order and leave out notifications; a request made only of notifications gets `204`.
4. **Encode**: binary returns a `Response`/`Error` frame with the same id (out of order, multiplexed). HTTP returns a JSON body.

### Streaming (LaserStream-style)
- **Frames**: `Subscribe=3 {type, params, fromSeq?, fromSlot?}` → ack `{ok, topic, headSeq, replayed, lost, firstSeq}`; `Notification=4 {seq, slot, data}`; `Unsubscribe=5`; `Ping=6`/`Pong=7`.
- **Fan-out + dedup**: one upstream subscription per topic (type plus raw params), shared by every binary, WS and SSE consumer. Messages are deduped by slot, signature, or pubkey@slot, so an upstream reconnect never double-delivers. Idle topics linger 10s to keep the ring warm for clients that reconnect.
- **Seq + ring buffer**: each topic has a monotonically increasing `seq` and keeps the last `STREAM_RING` (2000) entries. `fromSeq` / `fromSlot` replay from that ring.
- **At-least-once + resume**: the server sends **ack → replay → live** in one tick, so they are strictly ordered. The client tracks `lastSeq`. After a reconnect (exponential backoff with jitter) it re-subscribes with `fromSeq`, drops duplicates, and treats a seq gap as a trigger to resume. If the gap has already fallen out of the ring, the ack reports `lost` and `onGap(lost)` fires.
- **Keepalive**: the client pings every 5s and reconnects after 3 missed intervals. The server pings WS clients every 30s. The upstream WS is pinged every 20s and terminated if a pong is missed.
- **Backpressure** (binary, WS, SSE alike): above `STREAM_SOFT_LIMIT` (1MB) buffered, notifications are dropped (the client resumes from the ring). A consumer that stays above the soft limit for `STREAM_SLOW_TIMEOUT_MS` (10s), or goes above `STREAM_HARD_LIMIT` (8MB), is disconnected.
- **Stream types**: `slot`, `root`, `account`, `program`, `logs`, `signature`. `signature` is one-shot, as in Solana.

Client API:
```ts
const c = await RpcClient.connect(7070);
const remote = c.proxy<SolanaApi>(); await remote.getSlot();
const unsub = await c.subscribe("slot", {}, (m) => console.log(m.seq, m.data.slot));
await c.subscribe("account", { pubkey: "SysvarC1ock11111111111111111111111111111111" }, onMsg);
await c.subscribe("logs", { mentions: "<programId>" }, onMsg, { fromSlot: 123, onGap: (n) => console.warn("lost", n) });
for await (const m of c.stream("slot", {})) { /* async iterator */ }
await unsub();
```

## Helius compatibility

| Feature | Status |
|---|---|
| URL `/?api-key=KEY` | ✅ HTTP, WS and SSE. Keys are checked against `API_KEYS` (comma list). With no keys set, the endpoint is open. Bad or missing key → `401`. |
| HTTP JSON-RPC 2.0 (single, batch, id echo, error codes) | ✅ |
| `GET /health` | ✅ |
| Read methods | ✅ `getHealth getVersion getSlot getBlockHeight getEpochInfo getLatestBlockhash getBalance getAccountInfo getMultipleAccounts(≤100) getTransaction getSignaturesForAddress(limit≤1000) getSignatureStatuses(≤256) getBlock getBlockTime getProgramAccounts(result cap GPA_MAX_RESULTS) getTokenAccountsByOwner getTokenAccountBalance getTokenSupply getMinimumBalanceForRentExemption getRecentPrioritizationFees` |
| `getPriorityFeeEstimate` | ⚠️ Same request/response shape (`priorityLevel`, `includeAllPriorityFeeLevels`, `priorityFeeLevels{min,low,medium,high,veryHigh,unsafeMax}`). Values are **approximated** from percentiles of `getRecentPrioritizationFees` for the given `accountKeys`. |
| Standard WS pubsub: `account/program/logs/signature/slot/root` `Subscribe`/`Unsubscribe` | ✅ Notification JSON is the exact upstream result (`{jsonrpc, method:"xNotification", params:{result, subscription}}`). An unknown subscription id → `-32602 "Invalid subscription id."`. |
| Notification shapes | ✅ Checked live against mainnet (`npm run live`): proxy and direct-mainnet responses have identical key/type shapes. |

### Honest differences vs Helius
- **No sending.** `sendTransaction`, `simulateTransaction` and `requestAirdrop` are not proxied. Helius staked connections / Sender are not replicated.
- **No DAS API** (`getAsset`, `searchAssets`, …), no Enhanced Transactions API, no webhooks, and no `getTransactionsForAddress`-style indexed history. Only what the upstream Solana node can answer is available.
- **Not Geyser.** Helius LaserStream reads Geyser/gRPC (Yellowstone) directly from validators. This project relays standard Solana WebSocket pubsub, so latency and completeness are bounded by the upstream node. There are no `transactionSubscribe` / Enhanced WebSocket filters (`accountInclude`/`accountRequired`); use `logsSubscribe` with `mentions` instead. Upstream `logsSubscribe` accepts only one `mentions` key.
- **Replay is in memory**: the ring is per process and lost on restart. Helius replays about 24h (≈216k slots).
- **Priority fee** is a heuristic (see above). A serialized `transaction` param is ignored; pass `accountKeys`.
- **gPA limits**: results above `GPA_MAX_RESULTS` return `-32005`. There is no Helius-style `getProgramAccountsV2` pagination. The public mainnet RPC also blocks gPA for large programs (`403`, passed through).
- **Rate limits / auth**: the API-key check is a simple allow-list. There are no per-key credits or rate limits. The binary protocol port has no key check, so bind it to localhost or a private network.
- **Commitment**: binary streams default to `confirmed`. WS clients pass their own config through unchanged.

## Run
```
npm install
npm test               # 43 offline tests (mock Solana HTTP+WS), ~9s
npm run typecheck
npm run serve          # :7070 → binary + HTTP JSON-RPC + WS pubsub + SSE
npm run demo           # binary client RPC calls against mainnet
npm run stream         # streaming demo: slot, account, fan-out, reconnect+replay, fromSlot
npm run live           # live shape check vs mainnet + 3s logsSubscribe (few calls)

curl -X POST 'localhost:7070/?api-key=KEY' -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}'
curl -N 'localhost:7070/stream?type=slot'
wscat -c 'ws://localhost:7070/?api-key=KEY' -x '{"jsonrpc":"2.0","id":1,"method":"slotSubscribe"}'
```
Env: `SOLANA_RPC_URL`, `SOLANA_WS_URL` (defaults mainnet-beta public), `SOLANA_TIMEOUT_MS` (10000), `API_KEYS`, `PORT` (7070), `HOST` (127.0.0.1), `STREAM_RING` (2000), `STREAM_SOFT_LIMIT`, `STREAM_HARD_LIMIT`, `STREAM_SLOW_TIMEOUT_MS`, `MAX_FRAME_BYTES`, `GPA_MAX_RESULTS` (10000).
The public mainnet endpoint is rate-limited; point `SOLANA_RPC_URL`/`SOLANA_WS_URL` at your own node or a provider for real load.

## Layout
```
src/protocol.ts     framing, FrameDecoder (limits, malformed handling)
src/server.ts       RpcServer: sniffing, binary, HTTP JSON-RPC, WS pubsub, SSE, backpressure gate, api-key
src/client.ts       RpcClient: multiplexing, proxy, subscribe/stream, reconnect+resume, keepalive
src/stream.ts       StreamHub: topics, dedup, seq, ring replay, linger
src/upstream-ws.ts  SolanaWs: shared upstream socket, resubscribe, timeouts
src/solana.ts       read-only handlers, cache, validation, getPriorityFeeEstimate
src/app.ts          createFastRpc() wiring
test/*.test.ts      node:test suites + mock Solana node + unpluggable TCP proxy
```
