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

## Methods
`getHealth` (1s cache), `getSlot` (400ms), `getBlockHeight` (400ms), `getBalance(pubkey)` (2s), `getLatestBlockhash` (400ms), `getVersion` (60s).

## Run
```
npm install
npm run demo                 # starts server in-process + binary client against mainnet
npm run serve                # standalone server on :7070
NO_SERVER=1 npm run demo     # binary client against running server
curl -X POST localhost:7070 -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}'
curl -X POST localhost:7070 -d '{"jsonrpc":"2.0","id":2,"method":"getBalance","params":["SysvarRent111111111111111111111111111111111"]}'
```
Env: `SOLANA_RPC_URL` (default `https://api.mainnet-beta.solana.com`), `SOLANA_TIMEOUT_MS` (5000), `PORT` (7070), `HOST` (127.0.0.1).
The public mainnet endpoint is rate-limited, so use your own RPC URL for heavy load.
