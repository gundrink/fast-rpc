// Comprehensive READ-ONLY mainnet check against a deployed fast-rpc.
// Usage: FRPC_URL=https://... FRPC_KEY=... npx tsx examples/live-mainnet-full.ts
// Never sends transactions or bundles; simulateTransaction uses an unsigned dummy with sigVerify:false.
import { WebSocket } from "ws";
import { b58encode, buildLegacyTx } from "../src/index.js";
const BASE = (process.env.FRPC_URL ?? "https://fast-rpc-production.up.railway.app").replace(/\/$/, "");
const KEY = process.env.FRPC_KEY ?? ""; if (!KEY) throw new Error("FRPC_KEY required");
const H = { "content-type": "application/json", "x-api-key": KEY };
const rows: { name: string; ok: boolean; ms: number; note: string }[] = [];
const short = (v: any) => { const s = typeof v === "string" ? v : JSON.stringify(v); return (s ?? "undefined").slice(0, 110); };
async function check(name: string, fn: () => Promise<string | void>) {
  const t = performance.now();
  try { const n = await fn(); rows.push({ name, ok: true, ms: performance.now() - t, note: n ?? "" }); }
  catch (e: any) { rows.push({ name, ok: false, ms: performance.now() - t, note: short(e?.message ?? e) }); }
}
let id = 0;
async function rpcRaw(method: string, params: any = []) {
  const r = await fetch(BASE, { method: "POST", headers: H, body: JSON.stringify({ jsonrpc: "2.0", id: ++id, method, params }) });
  return { status: r.status, j: await r.json().catch(() => null) as any };
}
async function rpc(method: string, params: any = []) { const { status, j } = await rpcRaw(method, params); if (j?.error) throw new Error(`${j.error.code} ${j.error.message}`); if (status !== 200) throw new Error(`HTTP ${status}`); return j.result; }
async function rest(method: string, path: string, body?: any) {
  const r = await fetch(BASE + path, { method, headers: H, body: body ? JSON.stringify(body) : undefined });
  const txt = await r.text(); let j: any; try { j = JSON.parse(txt); } catch { j = txt; }
  if (r.status >= 400) throw new Error(`HTTP ${r.status} ${short(j)}`); return j;
}
const must = (c: any, m: string) => { if (!c) throw new Error(m); };

// SPL probe mint (BONK). Earlier runs used a mistyped USDC address; real USDC is EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
const WSOL = "So11111111111111111111111111111111111111112", USDC = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";
const TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", VOTE = "Vote111111111111111111111111111111111111111";
const STAKE_POOL_OWNER = "GThUX1Atko4tqhN2NaiTazWSeFWMuiUvfFnyJyUghFMJ"; // well-known wallet (Raydium authority-ish); only read

// ---- bootstrap context from chain ----
const slot: number = await rpc("getSlot");
let block: any; let bslot = slot - 40;
for (let i = 0; i < 6 && !block; i++, bslot--) block = await rpc("getBlock", [bslot, { encoding: "json", transactionDetails: "full", rewards: false }]).catch(() => null);
const txs: any[] = block?.transactions ?? [];
const v1 = txs.find((t) => t.version === 1), v0 = txs.find((t) => t.version === 0), legacy = txs.find((t) => t.version === "legacy");
const nonVote = txs.filter((t) => !t.transaction.message.accountKeys.includes(VOTE));
const sample = nonVote[Math.floor(nonVote.length / 2)] ?? txs[0];
const sig: string = sample.transaction.signatures[0]; const payer: string = sample.transaction.message.accountKeys[0];
const young = nonVote.find((t) => t.meta.preBalances[0] < 50_000_000)?.transaction.message.accountKeys[0] ?? payer;
const usdcHolder = nonVote.flatMap((t) => t.meta.postTokenBalances ?? []).find((b: any) => b.mint === USDC)?.owner;
console.log(`context: slot=${slot} block=${bslot + 1} txs=${txs.length} versions: v1=${txs.filter((t) => t.version === 1).length} v0=${txs.filter((t) => t.version === 0).length} legacy=${txs.filter((t) => t.version === "legacy").length}`);

// ---- infra ----
await check("GET /health", async () => { const r = await fetch(BASE + "/health"); must(r.status === 200, `HTTP ${r.status}`); return await r.text(); });
await check("GET /healthz", async () => short(await rest("GET", "/healthz")));
await check("no key -> 401", async () => { const r = await fetch(BASE, { method: "POST", headers: { "content-type": "application/json" }, body: '{"jsonrpc":"2.0","id":1,"method":"getSlot"}' }); must(r.status === 401, `got ${r.status}`); return "401"; });
await check("wrong key -> 401", async () => { const r = await fetch(BASE, { method: "POST", headers: { "content-type": "application/json", "x-api-key": "nope" }, body: '{"jsonrpc":"2.0","id":1,"method":"getSlot"}' }); must(r.status === 401, `got ${r.status}`); return "401"; });
await check("GET /metrics", async () => { const r = await fetch(BASE + "/metrics", { headers: H }); const t = await r.text(); must(r.status === 200 && t.length > 50, `HTTP ${r.status}`); return `${t.split("\n").length} lines`; });
await check("Alpenglow detection (/v0/chain)", async () => { const c = await rest("GET", "/v0/chain"); must(c.consensus === "alpenglow", short(c)); return short(c); });
await check("JSON-RPC batch", async () => { const r = await fetch(BASE, { method: "POST", headers: H, body: JSON.stringify([{ jsonrpc: "2.0", id: 1, method: "getSlot" }, { jsonrpc: "2.0", id: 2, method: "getBlockHeight" }]) }); const j: any = await r.json(); must(Array.isArray(j) && j.length === 2 && j.every((x: any) => x.result != null), short(j)); return "2/2"; });

// ---- standard read methods ----
const std: [string, any[], (r: any) => any][] = [
  ["getAccountInfo", [WSOL, { encoding: "base64" }], (r) => r.value?.owner],
  ["getAccountInfo jsonParsed (BONK mint)", [USDC, { encoding: "jsonParsed" }], (r) => r.value?.data?.parsed?.type],
  ["getBalance", [payer], (r) => r.value >= 0],
  ["getBlock (no version param)", [bslot + 1, { transactionDetails: "signatures", rewards: false }], (r) => r.signatures?.length],
  ["getBlock (version 0 -> upgraded)", [bslot + 1, { transactionDetails: "accounts", rewards: false, maxSupportedTransactionVersion: 0 }], (r) => r.transactions?.length],
  ["getBlockCommitment", [bslot + 1], (r) => r.totalStake],
  ["getBlockHeight", [], (r) => r > 0],
  ["getBlockProduction", [], (r) => r.value?.range],
  ["getBlockTime", [bslot + 1], (r) => r > 0],
  ["getBlocks", [slot - 20, slot - 10], (r) => Array.isArray(r)],
  ["getBlocksWithLimit", [slot - 20, 5], (r) => r.length],
  ["getClusterNodes", [], (r) => r.length],
  ["getEpochInfo", [], (r) => r.epoch],
  ["getEpochSchedule", [], (r) => r.slotsPerEpoch],
  ["getFeeForMessage", [null], (r) => r?.context?.slot],
  ["getFirstAvailableBlock", [], (r) => r >= 0],
  ["getGenesisHash", [], (r) => r === "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"],
  ["getHealth", [], (r) => r === "ok"],
  ["getHighestSnapshotSlot", [], (r) => r.full],
  ["getIdentity", [], (r) => r.identity],
  ["getInflationGovernor", [], (r) => r.initial],
  ["getInflationRate", [], (r) => r.epoch],
  ["getInflationReward", [[VOTE]], (r) => Array.isArray(r)],
  ["getLargestAccounts", [], (r) => r.value?.length],
  ["getLatestBlockhash", [], (r) => r.value?.blockhash],
  ["getLeaderSchedule", [null, { identity: "7Np41oeYqPefeNQEHSv1UDhYrehxin3NStELsSKCT4K2" }], (r) => r !== undefined],
  ["getMaxRetransmitSlot", [], (r) => r > 0],
  ["getMaxShredInsertSlot", [], (r) => r > 0],
  ["getMinimumBalanceForRentExemption", [165], (r) => r > 0],
  ["getMultipleAccounts", [[WSOL, USDC], { encoding: "base64", dataSlice: { offset: 0, length: 8 } }], (r) => r.value?.length === 2],
  ["getProgramAccounts (filtered)", [TOKEN, { encoding: "base64", dataSlice: { offset: 0, length: 0 }, filters: [{ dataSize: 165 }, { memcmp: { offset: 32, bytes: payer } }] }], (r) => Array.isArray(r) ? r.length : r.value?.length],
  ["getRecentPerformanceSamples", [3], (r) => r.length],
  ["getRecentPrioritizationFees", [[USDC]], (r) => r.length],
  ["getSignatureStatuses", [[sig], { searchTransactionHistory: true }], (r) => r.value?.[0]?.confirmationStatus],
  ["getSignaturesForAddress", [payer, { limit: 3 }], (r) => r.length],
  ["getSlot", [], (r) => r > 0],
  ["getSlotLeader", [], (r) => r],
  ["getSlotLeaders", [slot, 4], (r) => r.length],
  ["getStakeMinimumDelegation", [], (r) => r.value >= 0],
  ["getSupply", [{ excludeNonCirculatingAccountsList: true }], (r) => r.value?.total],
  ["getTokenAccountBalance", [null], (r) => r.value?.uiAmountString],
  ["getTokenAccountsByOwner", [usdcHolder ?? payer, { programId: TOKEN }, { encoding: "jsonParsed" }], (r) => r.value?.length],
  ["getTokenLargestAccounts", [USDC], (r) => r.value?.length],
  ["getTokenSupply", [USDC], (r) => r.value?.uiAmountString],
  ["getTransaction (no version param)", [sig, { encoding: "json" }], (r) => r?.slot],
  ["getTransaction (version 0 -> upgraded)", [sig, { encoding: "json", maxSupportedTransactionVersion: 0 }], (r) => r?.slot],
  ["getTransaction jsonParsed v1", [v1?.transaction.signatures[0], { encoding: "jsonParsed" }], (r) => r?.version === 1 ? "v1" : r?.version],
  ["getTransaction base64 v0", [v0?.transaction.signatures[0], { encoding: "base64" }], (r) => r?.version === 0 ? "v0" : r?.version],
  ["getTransaction legacy", [legacy?.transaction.signatures[0], { encoding: "json" }], (r) => r?.version],
  ["getTransactionCount", [], (r) => r > 0],
  ["getVersion", [], (r) => r["solana-core"]],
  ["getVoteAccounts", [{ keepUnstakedDelinquents: false }], (r) => r.current?.length],
  ["isBlockhashValid", [null], (r) => typeof r.value === "boolean"],
  ["minimumLedgerSlot", [], (r) => r >= 0],
];
const lb = await rpc("getLatestBlockhash").catch(() => null);
const tokAcct = (await rpc("getTokenAccountsByOwner", [usdcHolder ?? payer, { programId: TOKEN }, { encoding: "base64", dataSlice: { offset: 0, length: 0 } }]).catch(() => null))?.value?.[0]?.pubkey;
for (const [m, p, f] of std) {
  if (m === "getTokenAccountBalance") p[0] = tokAcct; if (m === "isBlockhashValid") p[0] = lb?.value?.blockhash;
  if (m === "getFeeForMessage") { // legacy message: payer -> payer 0-lamport transfer, real blockhash (fee quote only, never sent)
    const { b58decode } = await import("../src/index.js"); const d = Buffer.alloc(12); d.writeUInt32LE(2, 0);
    p[0] = Buffer.concat([Buffer.from([1, 0, 1, 2]), b58decode(payer), Buffer.alloc(32), b58decode(lb.value.blockhash), Buffer.from([1, 1, 2, 0, 0, 12]), d]).toString("base64"); }
  await check(m, async () => { const r = await rpc(m.split(" ")[0], p); const v = f(r); must(v !== undefined && v !== false && v !== null, `unexpected ${short(r)}`); return short(v); });
}

// ---- simulate (unsigned dummy, sigVerify false) ----
await check("simulateTransaction (unsigned dummy, sigVerify:false)", async () => {
  const tx = buildLegacyTx([payer, "11111111111111111111111111111111"], 1, []).toString("base64");
  const r = await rpc("simulateTransaction", [tx, { encoding: "base64", sigVerify: false, replaceRecentBlockhash: true }]);
  must(r.value && "err" in r.value, short(r)); return `err=${short(r.value.err)} units=${r.value.unitsConsumed}`;
});

// ---- fees ----
await check("getPriorityFeeEstimate", async () => { const r = await rpc("getPriorityFeeEstimate", [{ accountKeys: [USDC], options: { includeAllPriorityFeeLevels: true } }]); must(r.priorityFeeLevels, short(r)); return short(r.priorityFeeLevels); });
await check("getPriorityFeeEstimate (v1 tx)", async () => { const r = await rpc("getPriorityFeeEstimate", [{ accountKeys: v1?.transaction.message.accountKeys ?? [payer], options: { recommended: true } }]); must(r.priorityFeeEstimate != null, short(r)); return short(r); });
await check("getPriorityFeeEstimate (global, no keys)", async () => { const r = await rpc("getPriorityFeeEstimate", [{ options: { includeAllPriorityFeeLevels: true, includeDetails: true } }]); must(r.priorityFeeLevels, short(r)); return short(r); });
await check("getSmartFees", async () => short(await rpc("getSmartFees", [{}])));

// ---- DAS ----
const BONK = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";
await check("getAsset (fungible BONK)", async () => { const r = await rpc("getAsset", { id: BONK }); must(r?.id === BONK, short(r)); return `${r.interface} ${r.content?.metadata?.symbol ?? ""}`; });
await check("getAsset (wSOL)", async () => { const r = await rpc("getAsset", { id: WSOL }); must(r?.id === WSOL, short(r)); return `${r.interface} ${r.token_info?.symbol ?? r.content?.metadata?.symbol ?? ""}`; });
const owner = usdcHolder ?? payer;
await check("getAssetsByOwner (showFungible)", async () => { const r = await rpc("getAssetsByOwner", { ownerAddress: owner, page: 1, limit: 20, displayOptions: { showFungible: true } }); must(Array.isArray(r?.items), short(r)); return `total=${r.total} items=${r.items.length} fungible=${r.items.filter((x: any) => /Fungible/.test(x.interface)).length}`; });
await check("searchAssets (owner, fungible)", async () => { const r = await rpc("searchAssets", { ownerAddress: owner, tokenType: "fungible", limit: 10 }); must(Array.isArray(r?.items), short(r)); return `items=${r.items.length}`; });
await check("getTokenAccounts (mint USDC)", async () => { const r = await rpc("getTokenAccounts", { mint: USDC, limit: 5 }); must(Array.isArray(r?.token_accounts), short(r)); return `n=${r.token_accounts.length}`; });
await check("getTokenAccounts (owner)", async () => { const r = await rpc("getTokenAccounts", { owner, limit: 5 }); must(Array.isArray(r?.token_accounts), short(r)); return `n=${r.token_accounts.length}`; });
// cNFT: find a Bubblegum mint/transfer in recent blocks -> asset id via getSignaturesForAsset is not possible, so search by tree
const BGUM = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY";
let cnft: string | undefined;
await check("find cNFT (searchAssets compressed)", async () => {
  const r = await rpc("searchAssets", { ownerAddress: owner, compressed: true, limit: 5 }).catch(() => null);
  cnft = r?.items?.[0]?.id;
  if (!cnft) { const sigs = await rpc("getSignaturesForAddress", [BGUM, { limit: 20 }]); for (const s of sigs) { const t = await rpc("getTransaction", [s.signature, { encoding: "json" }]).catch(() => null); const k = t?.transaction?.message?.accountKeys?.[0]; if (!k) continue; const a = await rpc("getAssetsByOwner", { ownerAddress: k, limit: 50 }).catch(() => null); cnft = a?.items?.find((x: any) => x.compression?.compressed)?.id; if (cnft) break; } }
  must(cnft, "no compressed asset discoverable (needs indexed trees)"); return cnft;
});
await check("getAssetProof (cNFT)", async () => { must(cnft, "no cNFT id"); const r = await rpc("getAssetProof", { id: cnft }); must(r?.root && Array.isArray(r.proof), short(r)); return `proof len ${r.proof.length}`; });

// ---- history ----
await check("getTransactionsForAddress", async () => { const r = await rpc("getTransactionsForAddress", [payer, { limit: 3, transactionDetails: "full" }]); const d = r.data ?? r; must(Array.isArray(d), short(r)); return `n=${d.length} versions=${d.map((x: any) => x.version ?? x.transaction?.version).join(",")}`; });
await check("POST /v0/transactions (v1+v0+legacy)", async () => { const s = [v1, v0, legacy].filter(Boolean).map((t: any) => t.transaction.signatures[0]); const r = await rest("POST", "/v0/transactions", { transactions: s }); must(Array.isArray(r) && r.length === s.length && r.every((x: any) => x?.signature), short(r)); return r.map((x: any) => x.type).join(","); });
await check("Enhanced: v1 SOL transfer -> TRANSFER", async () => {
  const SYS = "11111111111111111111111111111111";
  const isT = (x: any) => x.version === 1 && !x.meta.err && x.transaction.message.instructions.length === 1 && x.transaction.message.accountKeys[x.transaction.message.instructions[0].programIdIndex] === SYS && x.meta.preBalances[0] - x.meta.postBalances[0] > x.meta.fee;
  let t = txs.find(isT);
  for (let s = bslot; !t && s > bslot - 6; s--) t = ((await rpc("getBlock", [s, { encoding: "json", transactionDetails: "full", rewards: false }]).catch(() => null))?.transactions ?? []).find(isT);
  must(t, "no v1 transfer in sample block"); const r = await rest("POST", "/v0/transactions", { transactions: [t.transaction.signatures[0]] });
  must(r[0]?.type === "TRANSFER" && r[0].nativeTransfers?.length, short(r[0])); return `${r[0].type} ${r[0].nativeTransfers[0].amount} lamports`;
});
await check("GET /v0/addresses/:a/transactions", async () => { const r = await rest("GET", `/v0/addresses/${payer}/transactions?limit=3`); must(Array.isArray(r), short(r)); return `n=${r.length} types=${r.map((x: any) => x.type).join(",")}`; });
await check("getFundedBy (young wallet)", async () => { const r = await rpc("getFundedBy", [young]); must(r && "stoppedAt" in r, short(r)); return `${young.slice(0, 6)}… funder=${r.funder} stoppedAt=${r.stoppedAt}`; });
await check("GET /v0/addresses/:a/funded-by", async () => short(await rest("GET", `/v0/addresses/${young}/funded-by`)));

// ---- webhooks CRUD (watching a random unused key; never delivered) ----
await check("webhooks CRUD", async () => {
  const addr = b58encode(crypto.getRandomValues(new Uint8Array(32)));
  const c = await rest("POST", "/v0/webhooks", { webhookURL: "https://example.invalid/fast-rpc-check", webhookType: "enhanced", transactionTypes: ["ANY"], accountAddresses: [addr] });
  must(c.webhookID, short(c));
  const l = await rest("GET", "/v0/webhooks"); must(Array.isArray(l) && l.some((w: any) => w.webhookID === c.webhookID), "list");
  const g = await rest("GET", `/v0/webhooks/${c.webhookID}`); must(g.webhookID === c.webhookID, "get");
  const u = await rest("PUT", `/v0/webhooks/${c.webhookID}`, { transactionTypes: ["TRANSFER"] }); must(u.transactionTypes?.[0] === "TRANSFER", short(u));
  await rest("DELETE", `/v0/webhooks/${c.webhookID}`);
  const gone = await fetch(`${BASE}/v0/webhooks/${c.webhookID}`, { headers: H }); must(gone.status === 404, `after delete ${gone.status}`);
  return "create/list/get/update/delete ok";
});

// ---- Jito / TPU ----
await check("getTipAccounts (Jito)", async () => { const r = await rpc("getTipAccounts"); must(Array.isArray(r) && r.length, short(r)); return `n=${r.length}`; });
await check("Jito tip floor (public)", async () => { const r: any = await (await fetch("https://bundles.jito.wtf/api/v1/bundles/tip_floor")).json(); must(r?.[0]?.landed_tips_50th_percentile != null, short(r)); return `p50=${r[0].landed_tips_50th_percentile}`; });
await check("getTpuLeaders", async () => { const r = await rpc("getTpuLeaders"); must(r.length, short(r)); return `n=${r.length}`; });
await check("getTpuHandshake (QUIC from Railway)", async () => { const r = await rpc("getTpuHandshake"); const ok = r.filter((x: any) => x.ok); must(ok.length, short(r)); return `${ok.length}/${r.length} ok, ms=${r.map((x: any) => Math.round(x.ms)).join("/")}`; });

// ---- WebSocket pubsub ----
const wsUrl = BASE.replace(/^http/, "ws") + `/?api-key=${encodeURIComponent(KEY)}`;
function wsSub(method: string, params: any[], want = 1, ms = 20_000): Promise<string> {
  return new Promise((res, rej) => {
    const ws = new WebSocket(wsUrl); let n = 0, subId: any; const t = setTimeout(() => { ws.close(); rej(new Error(`timeout (${n} notifications, sub=${subId})`)); }, ms);
    ws.on("open", () => ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method, params })));
    ws.on("message", (d) => { const m = JSON.parse(d.toString()); if (m.id === 1) { if (m.error) { clearTimeout(t); ws.close(); rej(new Error(short(m.error))); } subId = m.result; return; }
      if (m.method) { n++; if (n >= want) { clearTimeout(t); ws.close(); res(`sub=${subId} got ${n}`); } } });
    ws.on("error", (e) => { clearTimeout(t); rej(e); });
  });
}
await check("WS slotSubscribe", () => wsSub("slotSubscribe", [], 3));
await check("WS accountSubscribe (Clock sysvar)", () => wsSub("accountSubscribe", ["SysvarC1ock11111111111111111111111111111111", { encoding: "base64" }], 1));
await check("WS logsSubscribe (Token program)", () => wsSub("logsSubscribe", [{ mentions: [TOKEN] }], 2));
await check("WS signatureSubscribe (already-confirmed sig)", async () => {
  // finalized signatures notify immediately or never; accept subscription id as pass, notification as bonus
  return await wsSub("signatureSubscribe", [sig, { commitment: "confirmed" }], 1, 6_000).catch((e) => { if (/sub=\d+/.test(e.message) && !/sub=undefined/.test(e.message)) return "subscribed (no notification for past sig: expected)"; throw e; });
});
await check("WS transactionSubscribe (Token program)", () => wsSub("transactionSubscribe", [{ accountInclude: [TOKEN], vote: false, failed: false }, { commitment: "confirmed", encoding: "jsonParsed", transactionDetails: "full", maxSupportedTransactionVersion: 0 }], 2, 45_000));
await check("WS transactionSubscribe processed (JUP)", () => wsSub("transactionSubscribe", [{ accountInclude: ["JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"], vote: false, failed: false }, { commitment: "processed", transactionDetails: "signatures" }], 2, 45_000));
await check("WS no key -> rejected", () => new Promise((res, rej) => { const ws = new WebSocket(BASE.replace(/^http/, "ws") + "/"); ws.on("open", () => { ws.close(); rej(new Error("opened without key")); }); ws.on("error", (e: any) => res(short(e.message))); ws.on("unexpected-response", (_q: any, r: any) => res(`HTTP ${r.statusCode}`)); }));

// ---- report ----
const pass = rows.filter((r) => r.ok).length;
console.log(`\n${pass}/${rows.length} passed  (${new Date().toISOString()}, ${BASE})\n`);
for (const r of rows) console.log(`${r.ok ? "PASS" : "FAIL"}  ${r.name.padEnd(52)} ${String(Math.round(r.ms)).padStart(6)}ms  ${r.note}`);
process.exit(0);
