// Live check of pumpTrending / pumpFlow / pumpTrendingSubscribe / pumpFlowSubscribe (read-only mainnet).
// usage: npx tsx examples/live-pump-trending.ts http://127.0.0.1:9911 [seconds=120]
import WebSocket from "ws"; import fs from "node:fs";
const url = process.argv[2] ?? "http://127.0.0.1:9911", secs = Number(process.argv[3] ?? 120);
const key = process.env.FAST_RPC_LOCAL_API_KEY; const q = key ? `?api-key=${key}` : "";
const call = async (method: string, params: any[]) => { const r = await fetch(url + q, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }); const j: any = await r.json(); if (j.error) throw new Error(method + ": " + j.error.message); return j.result; };
const out: string[] = []; const log = (...a: any[]) => { const s = a.map((x) => typeof x === "string" ? x : JSON.stringify(x)).join(" "); out.push(s); console.log(s); };
const ws = new WebSocket(url.replace(/^http/, "ws") + q); const counts: Record<string, number> = {}; const alerts: any[] = []; let trendNote: any = null;
ws.on("open", () => { ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpFlowSubscribe", params: [{ allMints: true, largeSellPct: 50, minSellSol: 1, devSells: true }] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "pumpTrendingSubscribe", params: [{ sort: "txs5m", limit: 5, intervalSec: 30 }] })); });
ws.on("message", (b) => { const m = JSON.parse(String(b)); if (m.error) log("WS error", m.error); const e = m.params?.result; if (!e) return;
  const k = m.method + ":" + (e.type ?? "-"); counts[k] = (counts[k] ?? 0) + 1;
  if (m.method === "pumpFlowNotification" && e.type !== "flow") alerts.push(e); if (m.method === "pumpTrendingNotification") trendNote = e; });
setTimeout(async () => {
  log("ws counts", counts);
  for (const s of ["txs5m", "volume5m", "buyers5m"]) { const t0 = Date.now(); const r = await call("pumpTrending", [{ sort: s, limit: 5 }]); log(`pumpTrending sort=${s} ${Date.now() - t0}ms tokens=${r.tokens.length}`);
    for (const t of r.tokens) log("  ", { mint: t.mint, venue: t.venue, txs: t.txs, vol: t.volumeSol, buyers: t.uniqueBuyers, bsr: t.buySellRatio, chg: t.priceChangePct, mcapSol: t.marketCapSol, liq: t.liquiditySol, prog: t.bondingProgress, age: t.ageSec ?? t.ageReason, holders: t.holderCount ?? t.holderCountReason, top10: t.top10Pct ?? t.top10Reason, curvePool: t.curveOrPoolPct, dev: t.devHoldingPct ?? t.devHoldingReason }); }
  const top = (await call("pumpTrending", [{ sort: "txs5m", limit: 1, enrich: false }])).tokens[0];
  if (top) { const f = await call("pumpFlow", [top.mint]); log("pumpFlow", top.mint, { "1m": f.windows["1m"], "5m": f.windows["5m"] }, "alerts", f.recentAlerts.length); }
  log("trending WS note tokens", trendNote?.tokens?.length ?? null);
  log("alerts", alerts.length, alerts.slice(0, 5).map((a) => ({ type: a.type, mint: a.mint, wallet: a.wallet, sol: a.solAmount, pct: a.pctOfPositionSold, why: a.pctOfPositionSoldReason, sig: a.signature })));
  fs.writeFileSync(new URL("./live-pump-trending.last.txt", import.meta.url), out.join("\n") + "\n"); ws.close(); process.exit(0);
}, secs * 1000);
