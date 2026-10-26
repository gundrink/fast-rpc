// READ-ONLY: provoke public-RPC 429s with a burst of getSignaturesForAddress and show backoff/racing absorbing them.
import { createUpstream, createRacedUpstream, upstreamStats } from "../src/index.js";
const A = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", N = Number(process.argv[2] ?? 60);
const burst = async (name: string, up: any) => {
  const before = upstreamStats.retries429, t0 = Date.now();
  const r = await Promise.allSettled(Array.from({ length: N }, (_, i) => up("getSignaturesForAddress", [A, { limit: 1 + (i % 50) }])));
  const fail = r.filter((x) => x.status === "rejected") as PromiseRejectedResult[];
  console.log(name, { n: N, ok: N - fail.length, failed: fail.length, firstError: fail[0]?.reason?.message, retries429: upstreamStats.retries429 - before, ms: Date.now() - t0 });
};
await burst("single api.mainnet-beta, no retry", createUpstream("https://api.mainnet-beta.solana.com", 15000, 0));
await new Promise((r) => setTimeout(r, 12000));
await burst("single api.mainnet-beta, 429 backoff x4", createUpstream("https://api.mainnet-beta.solana.com", 15000, 4));
const raced = createRacedUpstream(["https://api.mainnet-beta.solana.com", "https://api.mainnet.solana.com", "https://solana-rpc.publicnode.com"], { hedgeMs: 300 });
await new Promise((r) => setTimeout(r, 12000));
await burst("raced 3 free RPCs", raced); console.log(JSON.stringify(raced.stats()));
