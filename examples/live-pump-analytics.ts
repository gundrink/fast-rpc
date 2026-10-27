// Live read-only: launch risk on fresh pump creates + wallet ranking. Usage: tsx examples/live-pump-analytics.ts [url] [seconds]
import WebSocket from "ws";
const base = process.argv[2] ?? "http://127.0.0.1:9911", secs = Number(process.argv[3] ?? 45);
const rpc = async (method: string, params: any[]) => (await (await fetch(base, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) })).json());
const creates = new Set<string>(); let withRisk = 0, swaps = 0;
const ws = new WebSocket(base.replace(/^http/, "ws"));
ws.on("open", () => { ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ allSwaps: true, position: false }] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "pumpRankingSubscribe", params: [{ minTrades: 0, windowSec: 604800, limit: 3, intervalSec: 10 }] })); });
let rk = 0;
ws.on("message", (d) => { const m = JSON.parse(String(d)); if (m.id) return; const r = m.params.result;
  if (m.method === "pumpRankingNotification") { if (!rk++) console.log("ranking WS event:", JSON.stringify(r).slice(0, 600)); return; }
  if (r.type !== "swap") return; swaps++; if (r.create && r.side === "buy") creates.add(r.mint); if (r.launchRisk) withRisk++; });
setTimeout(async () => {
  ws.close(); console.log(`swaps=${swaps} creates seen=${creates.size} swaps carrying launchRisk=${withRisk}`);
  const infos = (await Promise.all([...creates].slice(0, 40).map((m) => rpc("pumpLaunchInfo", [m])))).map((x) => x.result).filter((x) => x?.known);
  const sc = infos.map((i) => i.score).sort((a, b) => a - b);
  console.log(`launches scored=${infos.length} score p50=${sc[sc.length >> 1]} max=${sc.at(-1)} withSameSlotBuys=${infos.filter((i) => i.sameSlotBuys > 0).length} creatorPct p50=${infos.map((i) => i.creatorPct).sort((a, b) => a - b)[infos.length >> 1]}`);
  const top = infos.sort((a, b) => b.score - a.score)[0];
  if (top) { const f = (await rpc("pumpLaunchInfo", [top.mint, { funders: true }])).result; console.log("riskiest launch (with funders):", JSON.stringify({ ...f, early: f.early.slice(0, 5) })); }
  const r = (await rpc("pumpWalletRanking", [{ minTrades: 0, windowSec: 604800, limit: 5 }])).result;
  console.log("pumpWalletRanking:", JSON.stringify({ trackedTrades: r.trackedTrades, top: r.wallets.map((w: any) => ({ tag: w.tag, buys: w.buys, mints: w.mints, scored: w.scored, winRate: w.winRate, avgReturn: w.avgReturn, realizedSol: w.realizedSol })) }));
  process.exit(0);
}, secs * 1000);
