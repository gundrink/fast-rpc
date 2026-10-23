// Live check for the new features using only read-only / simulate calls (NEVER sends a real tx).
import { createFastRpc, buildLegacyTx, b58encode, upstreamUrl } from "../src/index.js";
import { randomBytes } from "node:crypto";
const app = createFastRpc({ webhooks: false });
const port = await app.server.listen(0);
const post = async (method: string, params: any) => (await (await fetch(`http://127.0.0.1:${port}/`, { method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) })).json()) as any;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const payer = b58encode(randomBytes(32)), to = b58encode(randomBytes(32));
// unsigned (zero-signature) system transfer: only valid for simulate with sigVerify:false
const tx = buildLegacyTx([payer, to, "11111111111111111111111111111111"], 1, [{ program: 2, accounts: [0, 1], data: Buffer.from([2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]) }]).toString("base64");
const sim = await post("simulateTransaction", [tx, { encoding: "base64", sigVerify: false, replaceRecentBlockhash: true }]);
console.log("simulateTransaction:", JSON.stringify(sim.result ?? sim.error).slice(0, 300));
await sleep(300);
const pf = await post("getPriorityFeeEstimate", [{ transaction: tx, options: { transactionEncoding: "Base64", includeAllPriorityFeeLevels: true } }]);
console.log("getPriorityFeeEstimate(tx):", JSON.stringify(pf.result ?? pf.error));
await sleep(300);
const a = await post("getAsset", { id: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" });
console.log("getAsset(USDC, on-chain fallback):", JSON.stringify(a.error ?? { interface: a.result.interface, name: a.result.content.metadata.name, symbol: a.result.content.metadata.symbol, uri: a.result.content.json_uri, decimals: a.result.token_info.decimals, supply: a.result.token_info.supply }));
console.log("upstream:", upstreamUrl);
await app.close(); process.exit(0);
