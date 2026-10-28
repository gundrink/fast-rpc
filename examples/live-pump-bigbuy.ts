// Live check of pumpBigBuySubscribe / pumpBigBuys / pumpBigBuyers (read-only mainnet).
// usage: npx tsx examples/live-pump-bigbuy.ts http://127.0.0.1:9911 [seconds=90]
import WebSocket from "ws"; import fs from "node:fs";
const url = process.argv[2] ?? "http://127.0.0.1:9911", secs = Number(process.argv[3] ?? 90);
const key = process.env.FAST_RPC_LOCAL_API_KEY; const q = key ? `?api-key=${key}` : "";
const call = async (method: string, params: any[]) => { const r = await fetch(url + q, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }); const j: any = await r.json(); if (j.error) throw new Error(method + ": " + j.error.message); return j.result; };
const out: string[] = []; const log = (...a: any[]) => { const s = a.map((x) => typeof x === "string" ? x : JSON.stringify(x)).join(" "); out.push(s); console.log(s); };
const ws = new WebSocket(url.replace(/^http/, "ws") + q); const notes: any[] = []; let err: any = null;
ws.on("open", () => ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpBigBuySubscribe", params: [{ minSol: 1 }] })));
ws.on("message", (b) => { const m = JSON.parse(String(b)); if (m.error) err = m.error; if (m.method === "pumpBigBuyNotification") notes.push(m.params.result); });
setTimeout(async () => {
  const c = (f: (x: any) => boolean) => notes.filter(f).length;
  log(`notifications ${notes.length} in ${secs}s; wsError ${JSON.stringify(err)}; below minSol ${c((x) => x.solAmount < 1)}; venues ${JSON.stringify(notes.reduce((a, x) => (a[x.venue] = (a[x.venue] ?? 0) + 1, a), {}))}`);
  log(`isCreator true/false/null ${c((x) => x.isCreator === true)}/${c((x) => x.isCreator === false)}/${c((x) => x.isCreator == null)}; walletAge known ${c((x) => x.walletAgeSec != null)}, fresh ${c((x) => x.freshWallet === true)}, ageReasons ${JSON.stringify(notes.reduce((a, x) => (x.walletAgeReason && (a[x.walletAgeReason] = (a[x.walletAgeReason] ?? 0) + 1), a), {}))}; repeat (prior big buys >1) ${c((x) => x.prior?.pastBigBuys > 1)}; missing sig/slot/blockTime ${c((x) => !x.signature || !x.slot || !x.blockTime)}`);
  for (const x of notes.slice(0, 3)) log("  sample", x);
  const rk = await call("pumpBigBuyers", [{ minSol: 1, windowSec: 3600, limit: 5, sort: "count" }]); log("pumpBigBuyers", { wallets: rk.wallets, buys: rk.buys }); for (const r of rk.top) log("  ", r);
  const rc = await call("pumpBigBuys", [{ minSol: 1, windowSec: 600, limit: 3 }]); log("pumpBigBuys", { total: rc.total, totalSol: rc.totalSol }, rc.buys);
  if (rk.top[0]) log("pumpBigBuyWallet", await call("pumpBigBuyWallet", [rk.top[0].wallet]));
  fs.writeFileSync(new URL("./live-pump-bigbuy.last.txt", import.meta.url), out.join("\n") + "\n"); ws.close(); process.exit(0);
}, secs * 1000);
