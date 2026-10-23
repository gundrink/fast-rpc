# fast-rpc

Fast TypeScript RPC gateway for Solana, inspired by Helius RPC / LaserStream. A **single TCP port** speaks:

| Protocol | Detected by | Use |
|---|---|---|
| Binary MessagePack frames (primary) | first 4 bytes are not an HTTP verb | `RpcClient`: multiplexed calls + resumable streams |
| HTTP JSON-RPC 2.0 (`POST /?api-key=…`) | `POST`/`GET `/… | Helius/Solana-style RPC, `curl`, web3.js `Connection` |
| Solana pubsub WebSocket (`ws://host:port/?api-key=…`) | HTTP `Upgrade: websocket` | `accountSubscribe`, `slotSubscribe`, `logsSubscribe`, … (Solana/Helius-identical shapes) |
| SSE `GET /stream?type=…` | HTTP GET | `curl -N` streaming |

Upstream access is **read-only** (whitelisted methods) except the **send relay**: `sendTransaction` / `simulateTransaction` forward *user-signed* transactions. fast-rpc never holds keys.

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
- **Frames**: `Subscribe=3 {type, params, fromSeq?, fromSlot?}` → ack `{ok, topic, headSeq, replayed, lost, firstSeq}`; `Notification=4 {seq, slot, data}`; `Unsubscribe=5`; `Ping=6`/`Pong=7`; `Auth=8 {apiKey}`.
- **Fan-out + dedup**: one upstream subscription per topic (type plus raw params), shared by every binary, WS and SSE consumer. Messages are deduped by slot, signature, or pubkey@slot, so an upstream reconnect never double-delivers. Idle topics linger 10s to keep the ring warm for clients that reconnect.
- **Seq + ring buffer**: each topic has a monotonically increasing `seq` and keeps the last `STREAM_RING` (2000) entries. `fromSeq` / `fromSlot` replay from that ring.
- **At-least-once + resume**: the server sends **ack → replay → live** in one tick, so they are strictly ordered. The client tracks `lastSeq`. After a reconnect (exponential backoff with jitter) it re-subscribes with `fromSeq`, drops duplicates, and treats a seq gap as a trigger to resume. If the gap has already fallen out of the ring, the ack reports `lost` and `onGap(lost)` fires.
- **Keepalive**: the client pings every 5s and reconnects after 3 missed intervals. The server pings WS clients every 30s. The upstream WS is pinged every 20s and terminated if a pong is missed.
- **Backpressure** (binary, WS, SSE alike): above `STREAM_SOFT_LIMIT` (1MB) buffered, notifications are dropped (the client resumes from the ring). A consumer that stays above the soft limit for `STREAM_SLOW_TIMEOUT_MS` (10s), or goes above `STREAM_HARD_LIMIT` (8MB), is disconnected.
- **Stream types**: `slot`, `root`, `account`, `program`, `logs`, `signature`. `signature` is one-shot, as in Solana.
- **Persistent replay (optional)**: set `STREAM_PERSIST_DIR` and each topic appends `{seq,slot,ts,k,raw}` JSON lines to `<dir>/<sha1(topic)>.jsonl`. When a topic is created (also after a process restart), the file is loaded: the ring, the dedup set and the **seq high-water mark** are restored, so `fromSeq` resume works across restarts. Entries older than `STREAM_RETENTION_MS` (default 1h) are dropped. The file is compacted to the ring once it grows past 2×`STREAM_RING` lines, and a torn last line is tolerated. Signature topics are not persisted.

### Send relay (Sender-like)
`sendTransaction` / `simulateTransaction` accept the standard Solana params (`[tx, {encoding, skipPreflight, preflightCommitment, maxRetries, minContextSlot, sigVerify, replaceRecentBlockhash}]`). The tx is **decoded locally first** (legacy + v0, base58/base64, ≤1232 bytes, ≥1 signer). Malformed input gets `-32602` and never reaches upstream. `sendTransaction` is fanned out in parallel to `SOLANA_RPC_URL` + every URL in `SEND_RPC_URLS` (comma list), and the first success (the signature) wins. If every upstream fails, the first upstream error is returned. `simulateTransaction` goes only to the primary.

### Jito relay + Sender-style dual send
- `sendBundle`, `getBundleStatuses`, `getInflightBundleStatuses` and `getTipAccounts` go to `JITO_URL` (default `https://mainnet.block-engine.jito.wtf`, `/api/v1/bundles`; optional `JITO_UUID`). `getTipAccounts` is cached for 10 minutes.
- `sendBundle` takes 1..5 signed txs (base58/base64). Each is decoded locally. The bundle is rejected with `-32602` **before anything is sent** unless the txs together carry ≥ `JITO_MIN_TIP_LAMPORTS` (default 1000). A tip is a top-level System transfer to a `getTipAccounts` address held as a static key.
- **Dual send:** `sendTransaction(tx, {skipPreflight:true, jito:true})`, or `SEND_JITO=1` for every send. It validates the tip, then races Jito `/api/v1/transactions` against the RPC fan-out (`SOLANA_RPC_URL` + `SEND_RPC_URLS`). The first signature wins. The `jito` flag is stripped before forwarding, and `skipPreflight:true` is required (as with Helius Sender).
- No key custody: only user-signed bytes are forwarded. Tests use mock block engines only. The only live check is the read-only `npm run live:jito` (`getTipAccounts`).

### Yellowstone gRPC upstream (optional)
Set `GEYSER_URL` (+ `GEYSER_TOKEN`) to use `@triton-one/yellowstone-grpc` as the stream source.
- `slotSubscribe`, `accountSubscribe` and `programSubscribe` (base64, memcmp/dataSize filters) become Geyser filters on one gRPC stream per commitment. The full filter set is rewritten on each change, re-sent on reconnect (exponential backoff), and pinged every 10s. Updates are converted to the exact Solana WS notification shapes.
- `transactionSubscribe` (and webhooks / the cNFT indexer) use native Geyser transaction filters (`accountInclude/Exclude/Required`, `vote`, `failed`). Full transactions arrive with **no `getTransaction` round trip**, and filters accept up to `TX_FILTER_MAX_ACCOUNTS_GEYSER` (50k) accounts. Results are converted to the `getTransaction(json)` shape.
- `logs`, `signature`, `root`, and non-base64/dataSlice account subs still go to the WS upstream (`SOLANA_WS_URL`). Without `GEYSER_URL`, everything stays on WS exactly as before.
- Tests drive `GeyserUpstream` through an injected fake client/duplex stream rather than a real gRPC server. The real client is constructed lazily (and was checked to load), but it has not been run against a live Geyser endpoint (no token).

### Compressed NFTs (best-effort Bubblegum indexer)
`CNFT_TREES="tree:maxDepth,…"` indexes those trees from confirmed transactions (live via the transactionSubscribe engine, sequentially for ordering). `CNFT_BACKFILL=1` replays history from tree creation via `getSignaturesForAddress` (≤ `CNFT_BACKFILL_MAX`, default 5000). `CNFT_INDEX_FILE` persists the index.
- It parses Bubblegum `mint_v1`, `mint_to_collection_v1`, `transfer` and `burn` instruction data, top-level and CPI (inner). It computes `data_hash`, `creator_hash`, the asset id (PDA) and the V1 leaf hash (keccak) exactly as Bubblegum does, and keeps a full concurrent-merkle-tree replica.
- Served locally (without `DAS_RPC_URL`): `getAsset` / `getAssetBatch` (compression fields, owner, delegate, collection, burnt), `getAssetsByOwner` (cNFTs + SPL), `getAssetProof` / `getAssetProofBatch` (root, full proof, `node_index`, leaf).
- **Limits:**
  - The index is correct only if it saw every change since tree creation.
  - Mint nonces are assumed sequential from 0.
  - `delegate`, `redeem`/`decompress`, `verify_creator`, `set_and_verify_collection`, `update_metadata` and Bubblegum V2 are not modelled. Assets touched by them will diverge.
  - `seq` is a local counter (no noop changelog parsing), and proofs are full depth (canopy ignored).
  - Untracked trees return "not found".

### DAS API
- With `DAS_RPC_URL` set (e.g. a Helius URL), all DAS methods (`getAsset getAssetBatch getAssetsByOwner getAssetsByGroup getAssetsByCreator getAssetsByAuthority searchAssets getAssetProof getAssetProofBatch getSignaturesForAsset getTokenAccounts getNftEditions`) are proxied as-is (object params).
- Without it, there is a **best-effort on-chain fallback** for `getAsset`, `getAssetBatch` and `getAssetsByOwner`. It reads the SPL mint (`jsonParsed`) plus the Metaplex Token Metadata PDA (borsh-decoded: name, symbol, uri, royalties, creators, token standard, verified collection) and returns the Helius asset shape. `getAssetsByOwner` scans Token + Token-2022 accounts (zero balances hidden unless `displayOptions.showZeroBalance`) with `page`/`limit`.
  - The fallback has **no** off-chain JSON (`content.files/links` are empty), no compressed NFTs, no editions/print supply, no `burnt` detection and no price info.
  - Token-2022 `tokenMetadata` extension names/symbols are used when there is no Metaplex metadata.
  - `getAssetsByGroup` (collection) and `searchAssets` (`ownerAddress`, `grouping`, `creatorAddress`, `creatorVerified`, `burnt`, `compressed`, `tokenType`, `page`/`limit`) answer **from the local index** (indexed cNFTs + the owner's SPL tokens). Uncompressed collection members and arbitrary searches without an owner/group/creator are not covered, and unknown filters give `-32602`.
  - `getSignaturesForAsset`, `getAssetsByCreator`, `getAssetsByAuthority`, `getTokenAccounts` and `getNftEditions` return `-32601 "… requires DAS_RPC_URL"`.

### transactionSubscribe (Helius Enhanced WS shape)
`{"method":"transactionSubscribe","params":[{"accountInclude":[…],"accountExclude":[…],"accountRequired":[…],"vote":false,"failed":false},{"commitment":"confirmed","encoding":"json|jsonParsed|base64","transactionDetails":"full|signatures|none","maxSupportedTransactionVersion":0}]}`
→ `transactionNotification {subscription, result:{transaction:{transaction, meta, version}, signature, slot}}`, plus `transactionUnsubscribe`.
- Implementation: one shared `logsSubscribe{mentions}` topic per account in `accountInclude ∪ accountRequired`. Each new signature (deduped) is fetched with `getTransaction` (bounded concurrency, retries) and then filtered on the full key set, including lookup-table `loadedAddresses`.
- At least one include/required account is needed (max `TX_FILTER_MAX_ACCOUNTS`, default 100; 50k with `GEYSER_URL`, where no getTransaction is needed).

### Webhooks (raw + enhanced)
REST, scoped to the api key: `POST /v0/webhooks`, `GET /v0/webhooks`, `GET|PUT|DELETE /v0/webhooks/:id`. The body uses the Helius shape `{webhookURL, accountAddresses, transactionTypes:["ANY"], webhookType:"raw", authHeader?, txnStatus?}`.
- Registrations persist to `WEBHOOKS_FILE` (atomic rewrite) and are re-armed at start-up.
- Matching transactions (via the transactionSubscribe engine) are POSTed as a JSON **array** of raw `getTransaction` results, with `Authorization: <authHeader>`.
- Failures (network or 5xx/408/429) are retried with exponential backoff (1s·2ⁿ, 5 retries). Other 4xx responses are permanent failures.
- **Enhanced:** `webhookType:"enhanced"` POSTs Helius-shaped enhanced transactions: `{description, type, source, fee, feePayer, signature, slot, timestamp, nativeTransfers, tokenTransfers, accountData, transactionError, instructions, events}`.
  - **Parsed types:**
    - `TRANSFER`: System/SPL transfers only.
    - `SWAP`: heuristic. The fee payer's net assets go out in one asset and come in in another. `source` comes from known DEX programs (Jupiter, Raydium, Orca, Meteora, Pump).
    - `NFT_SALE`: minimal. A 0-decimal amount-1 token moves seller→buyer while SOL or wSOL moves buyer→seller. `source` is Magic Eden / Tensor when known.
    - `COMPRESSED_NFT_MINT/TRANSFER/BURN`.
    - Otherwise `UNKNOWN`.
  - `transactionTypes` now **filters** (raw or enhanced). `ANY` matches everything, and unsupported type names give `400`.
- **Persistence/backfill:** delivery stats (`delivered/failed/retries/lastError/lastSlot/backfilled`) persist to `WEBHOOKS_FILE.state.json`. On restart, each webhook catches up on txs with slot > `lastSlot`: the newest `backfillLimit` (200) signatures per address, oldest first, deduped by signature.

### Auth, rate limits, credits
- **Binary port**: when `API_KEYS` is set, the first frame must be `Auth=8 {apiKey}`. Anything else (except Ping) gets an Error frame and the connection is closed. `RpcClient.connect(port, host, {apiKey})` re-authenticates on every reconnect, and a rejected key stops reconnecting.
- **Per-key limits** (all transports): token bucket `rps`/`burst` and a `credits` budget per key.
  - Configure with `KEY_LIMITS='{"KEY":{"rps":50,"credits":1000000}}'`, plus `DEFAULT_RPS` / `DEFAULT_CREDITS` for other keys.
  - Costs: 1 per call, 10 for gPA/DAS, 5 for getBlock, 2 for getTransaction/simulate, 1 per subscription.
  - When over the limit, JSON-RPC returns `-32429` (HTTP `429` if every reply in the batch was limited) and the binary port returns an Error frame.
  - `GET /v0/usage?api-key=…` → `{requests, creditsUsed, creditsRemaining, rateLimited, rps}`. Counters are in memory.

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
| `getPriorityFeeEstimate` | ⚠️ Same request/response shape (`priorityLevel`, `includeAllPriorityFeeLevels`, `priorityFeeLevels{min,low,medium,high,veryHigh,unsafeMax}`). Accepts `accountKeys` **or a serialized `transaction`** (base58 default, `options.transactionEncoding:"Base64"`), whose static keys are decoded locally. Values are **approximated** from percentiles of `getRecentPrioritizationFees`. |
| `sendTransaction` / `simulateTransaction` | ✅ relay + multi-upstream fan-out + Sender-style Jito dual send (tip validated); no staked connections |
| Jito `sendBundle` / `getBundleStatuses` / `getInflightBundleStatuses` / `getTipAccounts` | ✅ relayed to `JITO_URL`, bundles tip-validated |
| DAS | ✅ proxied with `DAS_RPC_URL`; ⚠️ otherwise on-chain fallback + local cNFT index (`CNFT_TREES`) + local `searchAssets`/`getAssetsByGroup` |
| `transactionSubscribe` | ✅ native with `GEYSER_URL` (≤50k accounts); ⚠️ logs+getTransaction backend otherwise |
| Webhooks | ⚠️ `raw` + `enhanced` (TRANSFER / SWAP / NFT_SALE / cNFT types only), restart backfill |
| Per-key rate limits / credits, `401`/`429` | ✅ (in-memory) |
| Standard WS pubsub: `account/program/logs/signature/slot/root` `Subscribe`/`Unsubscribe` | ✅ Notification JSON is the exact upstream result (`{jsonrpc, method:"xNotification", params:{result, subscription}}`). An unknown subscription id → `-32602 "Invalid subscription id."`. |
| Notification shapes | ✅ Checked live against mainnet (`npm run live`): proxy and direct-mainnet responses have identical key/type shapes. |

### Honest differences vs Helius
- **Sending:** still **no staked connections / SWQoS**. That needs a staked validator identity, and Helius's own stake, which a relay cannot provide.
  - Jito dual send helps landing, but it is still a public block-engine submission.
  - No server-side rebroadcast beyond upstream `maxRetries`.
  - Live checks only ever use `simulateTransaction` and the read-only `getTipAccounts`. Tests use mocks only.
- **DAS without `DAS_RPC_URL`:**
  - Off-chain JSON is opt-in (`DAS_FETCH_JSON=1`; `DAS_JSON_TIMEOUT_MS`, `DAS_JSON_MAX_BYTES`, `DAS_JSON_TTL_MS`, `DAS_JSON_CACHE`, `DAS_JSON_CONCURRENCY`, `DAS_IPFS_GATEWAY`, `DAS_ARWEAVE_GATEWAY`). Private IPs are blocked unless `DAS_JSON_ALLOW_PRIVATE=1`. There is no CDN or image cache and no price data.
  - The collection index covers only the collections listed in `DAS_COLLECTIONS` (gPA on Token Metadata, refreshed every `DAS_COLLECTIONS_REFRESH_MS`, `DAS_COLLECTION_MAX_CREATORS`). Its offsets assume padded name/symbol/uri strings, so metadata with unpadded strings can be missed. There is no global creator index.
  - cNFTs are indexed only for `CNFT_TREES` trees. `requestAirdrop` is not proxied.
- **Bubblegum:** all V1 and V2 instructions are recognised. When noop events are present (both noop programs), state comes from the real changelog `seq`/leaf index, non-sequential mints are handled, and `seqGaps` is counted. `syncTree` reads the on-chain seq and `num_minted`.
  - Metadata-changing V2 ops (collection/creator/data) are best effort, because the leaf only carries hashes.
  - The fallback path without events assumes sequential nonces.
  - There is no proof backfill from history. A tree's history before indexing starts is unknown.
- **Geyser** is optional and bring-your-own (`GEYSER_URL`/`GEYSER_TOKEN`). It is tested against a real gRPC mock server through the yellowstone client.
  - `meta.err` is decoded to JSON. `blockTime` comes from blocksMeta (txs wait up to `GEYSER_BLOCKTIME_WAIT_MS`, default 1500, then fall back to `null`).
  - Replay: `GEYSER_RECORD=<dir>` keeps hourly JSONL for 24h, and `transactionSubscribe` accepts `fromSlot`. Only txs matching active subscriptions or `GEYSER_RECORD_ACCOUNTS` are recorded, unlike LaserStream's full-chain replay.
  - Without Geyser, streaming is bounded by WS pubsub latency, and `transactionSubscribe` costs one `getTransaction` per signature.
- **Enhanced webhooks:**
  - Supported types: TRANSFER, SWAP, TOKEN_MINT, BURN, STAKE_SOL/UNSTAKE_SOL/WITHDRAW, CREATE_ACCOUNT, NFT_SALE/LISTING/CANCEL_LISTING/BID/BID_CANCELLED and all COMPRESSED_NFT_* types.
  - Swaps decode Jupiter `SwapEvent`/`SwapsEvent` plus Raydium/Orca hop transfers. Marketplace parsing covers only Magic Eden and Tensor. Helius covers far more types and sources.
  - Delivery is batched (`WEBHOOK_BATCH_MAX`, default 100; `WEBHOOK_FLUSH_MS`).
  - Restart backfill paginates with `before`, up to `WEBHOOK_BACKFILL_MAX` (default 5000) signatures per address.
- **Replay** for WS/SSE topics is in memory, or on disk with `STREAM_PERSIST_DIR`, only for topics that were active.
- **Priority fee** is a heuristic. For v0 transactions only static keys are decoded (lookup-table keys aren't resolved).
- **gPA limits**: results above `GPA_MAX_RESULTS` return `-32005`. There is no `getProgramAccountsV2` pagination. The public mainnet RPC also blocks gPA for large programs (`403`, passed through).
- **Limits/credits** are per process and in memory. Nothing is shared across instances and nothing is billed.
- **Commitment**: binary/SSE streams default to `finalized` once Alpenglow is detected (else `confirmed`). WS clients pass their own config through unchanged unless `ALPENGLOW_CONFIRMED_AS_FINALIZED=1`.

## Alpenglow readiness

Facts, all taken from the official page **https://solana.com/upgrades/alpenglow** (updated Oct 2026) unless noted otherwise:
- **Status:**
  - Votor (the voting protocol) is **live on mainnet** since epoch 1064 (2026-10-21, ~13:36 UTC). It shipped in Agave 4.3; mainnet runs v4.3.2.
  - The Alpenglow genesis block is slot 459,652,999, and Alpenglow consensus runs from slot 459,653,000.
  - Devnet activated in epoch 1167 (2026-09-25). The page also says Alpenglow is live on testnet, but gives no testnet date.
  - Rotor (the Turbine replacement) is planned and **not yet scheduled**.
  - Feature gate: `A1pengvuM6JEcyNuTnMqepBKhwHE3N6PmUrdATGawhJS`.
- **Timing:**
  - Finality is ~150ms (previously ~12.8s, the 32-slot lockout). This is "an expectation, not a constant".
  - Slot time was cut **separately** from 400ms to 200ms, in four stages completed in Oct 2026.
- **Commitment:**
  - `processed`, `confirmed` and `finalized` all still work.
  - `confirmed` and `finalized` now describe the same state.
  - New code should use `finalized`. `confirmed` will be deprecated and removed in a later release.
  - There is no new commitment level.
- **Votes:** vote transactions are no longer in blocks, so vote filtering is a no-op.
- **Detection:** call the new `getAgGenesisCert` RPC (Agave 4.3+, no params, reads the finalized bank).
  - `null` means TowerBFT; an object means the cluster has migrated (`block.slot` is the genesis slot).
  - `-32601` means the node is pre-4.3 and should be treated as unknown, not as TowerBFT.
  - The page says to gate behaviour on this call, not on a date or version string.
  - Verified live on 2026-10-23: mainnet returned `block.slot = 459652999`, and `getVersion` returned `solana-core 4.3.2`.
- **JSON-RPC / WebSocket are unchanged.** `bank_id` and multiple banks per slot exist only on Geyser/gRPC. fast-rpc relays standard pubsub, so it needs no `bank_id` handling.
- PoH ticks are gone, and `Clock.unix_timestamp` is now set by the leader. Neither affects this proxy.
- SIMDs listed by the page: 0326 (consensus), 0337 (fast leader handover markers), 0357 (VAT), 0384 (migration), 0387 (BLS pubkeys). I could not retrieve the SIMD-0326 text (GitHub fetch returned empty), so nothing here relies on SIMD-specific details.

What fast-rpc does (`src/chain.ts`, `GET /v0/chain`):
- **Slot rate is observed, not assumed.**
  - The initial guess is `SLOT_MS` (default 200). It is refined by an EWMA over slot notifications; gaps and outages are ignored.
  - With `CHAIN_TRACK_SLOTS=1`, an internal `slotSubscribe` keeps the estimate fresh. Otherwise any client slot stream feeds it.
  - Cache TTLs for slot-bound reads use N observed slots instead of fixed 400/1000ms: 1 slot for getSlot/BlockHeight/LatestBlockhash/AccountInfo/MultipleAccounts, 2 slots for balances, signatures, token accounts, epoch info and prioritization fees.
  - `transactionSubscribe` / webhook `getTransaction` retry spacing is 2 observed slots (min 100ms) instead of a fixed 400ms.
- **Detection:** `getAgGenesisCert` runs at start-up and every 10 min (`CHAIN_DETECT=0` disables it). The method is also proxied.
- **Default commitment:** internal subscriptions (binary/SSE streams without a commitment, `transactionSubscribe`, webhooks) use `finalized` once Alpenglow is detected, otherwise `confirmed`. Override with `DEFAULT_COMMITMENT`.
- **Optional `ALPENGLOW_CONFIRMED_AS_FINALIZED=1`:** client `confirmed` is rewritten to `finalized` (HTTP reads and WS/stream topics, which also merges duplicate upstream subscriptions). It is applied **only** while Alpenglow is detected, and is off by default.
- **Vote heuristic:** the Vote-program log scan in `transactionSubscribe` is skipped under Alpenglow. The account-key check stays, as a harmless no-op.
- **Priority fee:** `options.lookbackSlots` (1..150, as in Helius) is a window measured in **slots**. 150 slots is now ~30s rather than ~60s.

Unknowns (not implemented):
- There is no documented RPC field that exposes certificates. They are available only via Geyser `notify_block_footer`.
- Exact `rootSubscribe`/`slotSubscribe` semantics under Votor aren't specified beyond "JSON-RPC and WebSocket are unchanged", so fast-rpc passes them through as-is.
- No removal date has been given for `confirmed`.
- Rotor has no date.

`npm run live:alpenglow` checks detection and the observed slot rate against mainnet (1 RPC call plus 1 WS subscription). A sample run gave `{"consensus":"alpenglow","genesisSlot":459652999,"slotMs":269.1,...}`; the public node delivers slot notifications in bursts, so the observed figure sits above the nominal 200ms.

`npm run spl-tokens -- <wallet> [max]` lists a wallet's SPL + Token-2022 balances (name, symbol, decimals, uiAmount) through the binary `RpcClient`. Names come from Metaplex metadata or the Token-2022 metadata extension. The cost is 2 `getTokenAccountsByOwner` calls plus 2 `getMultipleAccounts` calls per 50 mints. A live run on `86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY` found 1091 non-zero accounts. The top 50 resolved names for SPL and Token-2022 mints (e.g. `BRUV`, `GUACA` "Solana Power Boost" [Token-2022], `TOLY`).

`npm run live:jito` makes one read-only `getTipAccounts` call through fast-rpc to the mainnet block engine. It returned the 8 tip accounts.

## Run
```
npm install
npm test               # 66 offline tests (incl. Alpenglow timing mock) (mock Solana HTTP+WS, mock send/DAS, local webhook receiver), ~10s
npm run typecheck
npm run serve          # :7070 → binary + HTTP JSON-RPC + WS pubsub + SSE
npm run demo           # binary client RPC calls against mainnet
npm run stream         # streaming demo: slot, account, fan-out, reconnect+replay, fromSlot
npm run live           # live shape check vs mainnet + 3s logsSubscribe (few calls)
npm run live:sim       # simulateTransaction (sigVerify:false), priority fee from tx, getAsset fallback; never sends

curl -X POST 'localhost:7070/?api-key=KEY' -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}'
curl -N 'localhost:7070/stream?type=slot'
wscat -c 'ws://localhost:7070/?api-key=KEY' -x '{"jsonrpc":"2.0","id":1,"method":"slotSubscribe"}'
```
Env: `SOLANA_RPC_URL`, `SOLANA_WS_URL` (defaults mainnet-beta public), `SOLANA_TIMEOUT_MS` (10000), `API_KEYS`, `PORT` (7070), `HOST` (127.0.0.1), `STREAM_RING` (2000), `STREAM_SOFT_LIMIT`, `STREAM_HARD_LIMIT`, `STREAM_SLOW_TIMEOUT_MS`, `MAX_FRAME_BYTES`, `GPA_MAX_RESULTS` (10000), `SEND_RPC_URLS`, `DAS_RPC_URL`, `WEBHOOKS_FILE`, `STREAM_PERSIST_DIR`, `STREAM_RETENTION_MS` (3600000), `KEY_LIMITS`, `DEFAULT_RPS`, `DEFAULT_CREDITS`, `TX_FILTER_MAX_ACCOUNTS` (100), `TX_FILTER_MAX_ACCOUNTS_GEYSER` (50000), `GEYSER_URL`, `GEYSER_TOKEN`, `JITO_URL`, `JITO_UUID`, `JITO_MIN_TIP_LAMPORTS` (1000), `SEND_JITO`, `CNFT_TREES`, `CNFT_INDEX_FILE`, `CNFT_BACKFILL`, `CNFT_BACKFILL_MAX` (5000), `SLOT_MS` (200), `CHAIN_TRACK_SLOTS`, `CHAIN_DETECT`, `DEFAULT_COMMITMENT`, `ALPENGLOW_CONFIRMED_AS_FINALIZED`.
The public mainnet endpoint is rate-limited; point `SOLANA_RPC_URL`/`SOLANA_WS_URL` at your own node or a provider for real load.

## Layout
```
src/protocol.ts     framing, FrameDecoder (limits, malformed handling)
src/server.ts       RpcServer: sniffing, binary, HTTP JSON-RPC, WS pubsub, SSE, backpressure gate, api-key
src/client.ts       RpcClient: multiplexing, proxy, subscribe/stream, reconnect+resume, keepalive
src/stream.ts       StreamHub: topics, dedup, seq, ring replay, linger
src/upstream-ws.ts  SolanaWs: shared upstream socket, resubscribe, timeouts
src/solana.ts       read handlers, send relay, cache, validation, getPriorityFeeEstimate
src/codec.ts        base58, PDA derivation (ed25519 on-curve check), legacy/v0 tx decoder
src/das.ts          DAS proxy + on-chain getAsset/getAssetsByOwner fallback (Metaplex borsh)
src/txstream.ts     TxWatcher: transactionSubscribe engine (logs topics + getTransaction + filters)
src/webhooks.ts     WebhookManager: REST CRUD, JSON persistence, delivery w/ retries
src/limits.ts       per-key token bucket + credits
src/chain.ts        ChainState: observed slot rate, Alpenglow detection, commitment defaults
src/jito.ts         Jito block-engine relay + tip validation
src/geyser.ts       GeyserUpstream: Yellowstone gRPC slot/account/program/tx streams, WS fallback
src/bubblegum.ts    CnftIndexer: Bubblegum parsing, leaf hashing, merkle replica, proofs, persistence
src/enhanced.ts     parseEnhanced: TRANSFER / SWAP / NFT_SALE / cNFT enhanced tx shape
src/app.ts          createFastRpc() wiring
test/*.test.ts      node:test suites + mock Solana node + unpluggable TCP proxy
```
