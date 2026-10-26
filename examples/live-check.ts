// Brief live validation against mainnet: our proxy's responses vs direct upstream (shape/keys), plus WS logs stream.
import { WebSocket } from "ws";
import { createFastRpc, upstreamUrl } from "../src/index.js";
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const app = createFastRpc({ server: { apiKeys: ["demo-key"] } });
const port = await app.server.listen(Number(process.env.PORT ?? 7072));
const ours = `http://127.0.0.1:${port}/?api-key=demo-key`;
const post = async (url: string, method: string, params: any[] = []) => {
  const t = performance.now();
  const r = await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) });
  return { j: await r.json() as any, ms: performance.now() - t };
};
const shape = (v: any, d = 0): any => v === null ? "null" : Array.isArray(v) ? (v.length ? [shape(v[0], d + 1)] : []) :
  typeof v === "object" ? (d > 3 ? "object" : Object.fromEntries(Object.keys(v).sort().map((k) => [k, shape(v[k], d + 1)]))) : typeof v;

const WSOL = "So11111111111111111111111111111111111111112", TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const slot = (await post(ours, "getSlot")).j.result;
const blk = (await post(ours, "getBlock", [slot - 60, { transactionDetails: "signatures", rewards: false, maxSupportedTransactionVersion: 1 }])).j.result;
const sig = blk.signatures[Math.floor(blk.signatures.length / 2)];
const tx = (await post(ours, "getTransaction", [sig, { maxSupportedTransactionVersion: 1, encoding: "json" }])).j.result;
const payer = tx.transaction.message.accountKeys[0];
const cases: [string, any[]][] = [
  ["getHealth", []], ["getEpochInfo", []],
  ["getAccountInfo", [WSOL, { encoding: "base64" }]],
  ["getMultipleAccounts", [[WSOL, TOKEN], { encoding: "base64", dataSlice: { offset: 0, length: 8 } }]],
  ["getSignaturesForAddress", [payer, { limit: 2 }]],
  ["getTransaction", [sig, { maxSupportedTransactionVersion: 1, encoding: "json" }]],
  ["getBlock", [slot - 50, { transactionDetails: "none", rewards: false, maxSupportedTransactionVersion: 1 }]],
  ["getTokenSupply", [WSOL]],
];
for (const [m, p] of cases) {
  const a = await post(ours, m, p); await sleep(150);
  const b = await post(upstreamUrl, m, p); await sleep(150);
  const same = JSON.stringify(shape(a.j)) === JSON.stringify(shape(b.j));
  console.log(`${m.padEnd(26)} proxy ${a.ms.toFixed(0).padStart(4)}ms  shape==mainnet: ${same ? "yes" : "NO"}  ${a.j.error ? "error " + JSON.stringify(a.j.error) : JSON.stringify(a.j.result).slice(0, 90)}`);
}
const pf = await post(ours, "getPriorityFeeEstimate", [{ accountKeys: [TOKEN], options: { includeAllPriorityFeeLevels: true } }]);
console.log("getPriorityFeeEstimate    ", JSON.stringify(pf.j.result));
const pf2 = await post(ours, "getPriorityFeeEstimate", [{ accountKeys: [TOKEN] }]);
console.log("getPriorityFeeEstimate(M) ", JSON.stringify(pf2.j.result));
const gpa = await post(ours, "getProgramAccounts", [TOKEN, { dataSlice: { offset: 0, length: 0 } }]);
console.log("getProgramAccounts(Token) ", JSON.stringify(gpa.j.error ?? gpa.j.result).slice(0, 120));
const tok = await post(ours, "getTokenAccountsByOwner", [payer, { programId: TOKEN }, { encoding: "jsonParsed" }]);
console.log("getTokenAccountsByOwner   ", JSON.stringify(tok.j.error ?? { n: tok.j.result.value.length, first: tok.j.result.value[0]?.account?.data?.parsed?.info?.tokenAmount }).slice(0, 140));

// Solana-compatible WS: logsSubscribe (Token program) for ~3s, compare notification shape
const ws = new WebSocket(`ws://127.0.0.1:${port}/?api-key=demo-key`);
await new Promise((r) => ws.on("open", r));
let n = 0, first: any, subId: any;
ws.on("message", (raw) => { const m = JSON.parse(raw.toString()); if (m.id === 1) subId = m.result; else if (m.method === "logsNotification") { n++; first ??= m; } });
ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "logsSubscribe", params: [{ mentions: [TOKEN] }, { commitment: "confirmed" }] }));
await sleep(3000);
ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "logsUnsubscribe", params: [subId] }));
console.log(`logsSubscribe(Token) 3s: ${n} notifications, sub=${subId}; first shape:`, JSON.stringify(shape(first)));
console.log("  sample:", JSON.stringify({ slot: first?.params.result.context.slot, sig: first?.params.result.value.signature, err: first?.params.result.value.err, logs: first?.params.result.value.logs.slice(0, 2) }));
ws.close();
await sleep(200); await app.close(); process.exit(0);
