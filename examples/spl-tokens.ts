// List a wallet's SPL + Token-2022 balances (name, symbol, decimals, uiAmount) through fast-rpc's binary client.
// usage: tsx examples/spl-tokens.ts <wallet> [maxTokens=50]   (FAST_RPC_PORT to use a running server; else starts one in-process)
import { createFastRpc, RpcClient, TOKEN_PROGRAM, TOKEN_2022 } from "../src/index.js";

const wallet = process.argv[2] ?? "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY";
const max = Number(process.argv[3] ?? 50);
let app: ReturnType<typeof createFastRpc> | undefined, port = Number(process.env.FAST_RPC_PORT ?? 0);
if (!port) { app = createFastRpc({ webhooks: false, chain: { detect: false } }); port = await app.server.listen(0); }
const c = await RpcClient.connect(port);
try {
  const rows = new Map<string, { mint: string; program: string; decimals: number; raw: bigint }>();
  for (const [programId, label] of [[TOKEN_PROGRAM, "spl"], [TOKEN_2022, "token-2022"]]) {
    const r: any = await c.call("getTokenAccountsByOwner", wallet, { programId }, { encoding: "jsonParsed", commitment: "confirmed" });
    for (const a of r.value) {
      const i = a.account.data.parsed.info; const amt = BigInt(i.tokenAmount.amount); if (amt === 0n) continue;
      const row = rows.get(i.mint) ?? { mint: i.mint, program: label, decimals: i.tokenAmount.decimals, raw: 0n };
      row.raw += amt; rows.set(i.mint, row);
    }
  }
  const ui = (r: { raw: bigint; decimals: number }) => Number(r.raw) / 10 ** r.decimals;
  const list = [...rows.values()].sort((a, b) => ui(b) - ui(a)).slice(0, max);
  // names/symbols: Metaplex metadata PDA or Token-2022 tokenMetadata extension (via getAssetBatch on-chain fallback)
  const assets: any[] = list.length ? await c.call("getAssetBatch", { ids: list.map((r) => r.mint) }) : [];
  console.log(`wallet ${wallet}: ${rows.size} non-zero token accounts (showing ${list.length})`);
  console.table(list.map((r, i) => ({ mint: r.mint, program: r.program, name: assets[i]?.content?.metadata?.name?.replace(/\0/g, "") || "?", symbol: assets[i]?.content?.metadata?.symbol?.replace(/\0/g, "") || "?", decimals: r.decimals, uiAmount: ui(r) })));
} finally { c.close(); await app?.close(); }
