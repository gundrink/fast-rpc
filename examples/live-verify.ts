// Reproducible READ-ONLY mainnet verification: funded-by on real recent fee payers + a busy address,
// TPU leader lookup, fee estimates. Never sends anything. Usage: npx tsx examples/live-verify.ts [n=5]
import { createSolanaApi, upstreamStats } from "../src/index.js";
const FREE = ["https://solana-rpc.publicnode.com", "https://api.mainnet-beta.solana.com", "https://api.mainnet.solana.com"];
const urls = process.env.SOLANA_RPC_URLS?.split(",") ?? FREE;
const api: any = createSolanaApi({ urls, race: { hedgeMs: 300 }, timeoutMs: 15_000, fundedBy: { timeoutMs: 60_000 } });
const N = Number(process.argv[2] ?? 5);
const timed = async (f: () => Promise<any>) => { const t0 = Date.now(); try { const v = await f(); return { ok: true, ms: Date.now() - t0, v }; } catch (e: any) { return { ok: false, ms: Date.now() - t0, err: `${e?.code ?? ""} ${e?.message ?? e}` }; } };
const slot = await api.getSlot({ commitment: "finalized" });
const block = await api.getBlock(slot - 5, { maxSupportedTransactionVersion: 1, transactionDetails: "accounts", rewards: false, commitment: "finalized" });
const VOTE = "Vote111111111111111111111111111111111111111";
const payers = [...new Set<string>(block.transactions.filter((x: any) => !x.meta?.err && !x.transaction.accountKeys.some((k: any) => k.pubkey === VOTE)).map((x: any) => x.transaction.accountKeys[0].pubkey))].slice(0, N);
const busy = process.env.BUSY_ADDRESS ?? "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"; // Jupiter v6 program
console.log(`block ${slot - 5}: ${block.transactions.length} txs, sampled ${payers.length} fee payers; upstreams: ${urls.join(", ")}`);
const results: any[] = [];
for (const a of [...payers, busy]) {
  const r = await timed(() => api.getFundedBy(a, { hops: 2 }));
  const row = r.ok ? { address: a, ms: r.ms, complete: r.v.complete, truncated: r.v.truncated, stoppedAt: r.v.stoppedAt, chain: r.v.hops.map((h: any) => `${h.funder} (${h.amount} SOL, slot ${h.slot}, sigsScanned ${h.signaturesScanned})`) } : { address: a, ms: r.ms, error: r.err };
  results.push(row); console.log(JSON.stringify(row));
}
const leaders = await timed(() => api.getTpuLeaders()); console.log("getTpuLeaders", JSON.stringify(leaders));
const pfe = await timed(() => api.getPriorityFeeEstimate({ accountKeys: [busy], options: { includeAllPriorityFeeLevels: true, includeDetails: true } })); console.log("getPriorityFeeEstimate", JSON.stringify(pfe));
const pfe0 = await timed(() => api.getPriorityFeeEstimate({ options: { includeAllPriorityFeeLevels: true, includeDetails: true } })); console.log("getPriorityFeeEstimate(global)", JSON.stringify(pfe0));
const fees = await timed(() => api.getSmartFees({ accountKeys: [busy], priorityLevel: "high" })); console.log("getSmartFees", JSON.stringify(fees));
console.log("upstreams", JSON.stringify(api.getUpstreamStats()), "429 retries", upstreamStats.retries429);
