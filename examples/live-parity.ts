import { createFastRpc } from "../src/index.js";
const a = createFastRpc({ webhooks: false, chain: { detect: false } });
const p = await a.server.listen(0);
const post = async (b: any, path = "/") => (await fetch(`http://127.0.0.1:${p}${path}`, { method: "POST", body: JSON.stringify(b) })).json();
const W = "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY";
const g = await post({ jsonrpc: "2.0", id: 1, method: "getTransactionsForAddress", params: [W, { limit: 2 }] });
console.log("GTFA", JSON.stringify(g.result ?? g.error).slice(0, 400));
const sig = g.result?.data?.[0]?.signature;
if (sig) { const e = await post({ transactions: [sig] }, "/v0/transactions?api-key=ignored"); console.log("ENH", JSON.stringify(Array.isArray(e) ? { type: e[0]?.type, source: e[0]?.source, keys: Object.keys(e[0] ?? {}) } : e)); }
const f = await post({ jsonrpc: "2.0", id: 1, method: "getPriorityFeeEstimate", params: [{ accountKeys: ["JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"], options: { includeAllPriorityFeeLevels: true, recommended: true } }] });
console.log("PFE", JSON.stringify(f.result ?? f.error));
await a.close();
