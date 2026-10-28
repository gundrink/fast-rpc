// Live check: largeSell/devSell alerts carry solAmount (esp. curve tokens). usage: npx tsx examples/live-largesell-sol.ts <url> [secs=90]
import WebSocket from "ws"; import fs from "node:fs";
const url = process.argv[2] ?? "http://127.0.0.1:8899", secs = Number(process.argv[3] ?? 90);
const key = process.env.FAST_RPC_LOCAL_API_KEY; const q = key ? `?api-key=${key}` : "";
const ws = new WebSocket(url.replace(/^http/, "ws") + q); const a: any[] = []; const sw: any[] = [];
ws.on("open", () => { ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpFlowSubscribe", params: [{ allMints: true, largeSellPct: 30, minSellSol: 0, devSells: true, intervalSec: 0 }] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "pumpSwapSubscribe", params: [{ allSwaps: true, position: false }] })); });
ws.on("message", (b) => { const m = JSON.parse(String(b)); const e = m.params?.result; if (!e) return; if (m.method === "pumpFlowNotification" && e.type !== "flow") a.push(e); if (m.method === "pumpSwapNotification" && e.type === "swap" && e.side === "sell" && sw.length < 20000) sw.push(e); });
setTimeout(() => { const out: string[] = []; const log = (s: string) => { out.push(s); console.log(s); };
  for (const v of ["curve", "amm", "amm-other"]) { const x = a.filter((e) => e.venue === v); const n = x.filter((e) => e.solAmount == null);
    log(`${v}: alerts ${x.length}, solAmount null ${n.length} (${x.length ? (100 * n.length / x.length).toFixed(1) : "-"}%) reasons ${JSON.stringify(n.reduce((r, e) => (r[e.solAmountReason] = (r[e.solAmountReason] ?? 0) + 1, r), {}))}`); }
  const cs = sw.filter((e) => e.venue === "curve"); log(`curve sell swaps ${cs.length}; with quoteMint set ${cs.filter((e) => e.quoteMint).length}; solAmount>0 ${cs.filter((e) => e.solAmount > 0).length}`);
  const ex = a.find((e) => e.venue === "curve" && e.solAmount == null); if (ex) log("example null curve alert " + JSON.stringify(ex));
  fs.writeFileSync(new URL("./live-largesell-sol.last.txt", import.meta.url), out.join("\n") + "\n"); process.exit(0); }, secs * 1000);
