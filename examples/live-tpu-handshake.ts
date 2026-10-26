// READ-ONLY: QUIC handshakes (no stream, no tx) to real upcoming leaders via tpu-sidecar.
// Run the sidecar first (TPU_SIDECAR_ALLOW_SEND unset), then: TPU_SIDECAR_URL=http://127.0.0.1:7799 npx tsx examples/live-tpu-handshake.ts [rounds=5]
import { createSolanaApi, SidecarTransport } from "../src/index.js";
const t = new SidecarTransport(process.env.TPU_SIDECAR_URL ?? "http://127.0.0.1:7799", 15000);
const api: any = createSolanaApi({ urls: ["https://solana-rpc.publicnode.com", "https://api.mainnet-beta.solana.com", "https://api.mainnet.solana.com"], tpuTransport: t });
console.log("sidecar", JSON.stringify(await t.health()));
const rounds = Number(process.argv[2] ?? 5); const all: any[] = [];
for (let i = 0; i < rounds; i++) {
  const r = await api.getTpuHandshake(); all.push(...r.filter((x: any) => !x.reused));
  for (const x of r) console.log(`round ${i + 1} ${x.leader.slice(0, 8)}… ${x.addr} ${x.ok ? "OK" : "FAIL " + x.error} ${x.ms.toFixed(1)}ms${x.reused ? " (reused)" : ""}`);
  await new Promise((s) => setTimeout(s, 6000)); // ~ next leader group
}
const ok = all.filter((x) => x.ok).map((x) => x.ms).sort((a, b) => a - b);
console.log(JSON.stringify({ newHandshakes: all.length, ok: ok.length, failed: all.length - ok.length, p50ms: ok[Math.floor(ok.length / 2)]?.toFixed(1), maxMs: ok.at(-1)?.toFixed(1), errors: all.filter((x) => !x.ok).map((x) => x.error) }));
