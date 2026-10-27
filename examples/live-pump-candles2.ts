// Live check: deep candle backfill + disk cache, from/to, price units, alert blockTime, liquidity/mcap (read-only).
// usage: npx tsx examples/live-pump-candles2.ts http://127.0.0.1:9911 [wsSeconds=60]
import WebSocket from "ws"; import fs from "node:fs";
const url = process.argv[2] ?? "http://127.0.0.1:9911", secs = Number(process.argv[3] ?? 60);
const key = process.env.FAST_RPC_LOCAL_API_KEY; const q = key ? `?api-key=${key}` : "";
const call = async (method: string, params: any[]) => { const r = await fetch(url + q, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }); const j: any = await r.json(); if (j.error) throw new Error(method + ": " + j.error.message); return j.result; };
const out: string[] = []; const log = (...a: any[]) => { const s = a.map((x) => typeof x === "string" ? x : JSON.stringify(x)).join(" "); out.push(s); console.log(s); };
const ws = new WebSocket(url.replace(/^http/, "ws") + q);
const venues: Record<string, { n: number; liq: number; mcap: number; liqUsd: number; reasons: Record<string, number> }> = {}; const alerts: any[] = []; const cnt = new Map<string, { n: number; venue: string; quote?: string }>();
let liqBad = 0;
ws.on("open", () => ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ allSwaps: true, position: false, drawdownAlerts: [10, 25], reboundPct: 5 }] })));
ws.on("message", (b) => { const m = JSON.parse(String(b)); const e = m.params?.result; if (!e) return;
  if (e.type === "drawdown" || e.type === "rebound" || e.type === "confluence") { alerts.push(e); return; }
  if (e.type !== "swap" || !e.mint) return;
  const v = (venues[e.venue + (e.quoteMint ? "/nonSol" : "")] ??= { n: 0, liq: 0, mcap: 0, liqUsd: 0, reasons: {} }); v.n++;
  if (e.liquiditySol != null) { v.liq++; if (!(e.liquiditySol >= 0)) liqBad++; } else v.reasons[e.liquidityReason] = (v.reasons[e.liquidityReason] ?? 0) + 1;
  if (e.marketCapSol != null) v.mcap++; if (e.liquidityUsd != null) v.liqUsd++;
  const c = cnt.get(e.mint) ?? { n: 0, venue: e.venue, quote: e.quoteMint }; c.n++; cnt.set(e.mint, c); });
await new Promise((r) => setTimeout(r, secs * 1000));
log("liquidity/mcap coverage per venue", venues, `invalid liquidity ${liqBad}`);
log(`alerts ${alerts.length}; with blockTime ${alerts.filter((a) => typeof a.blockTime === "number").length}; with slot+signature ${alerts.filter((a) => a.slot && a.signature).length}`, alerts.slice(-1));
const amm = [...cnt.entries()].filter((x) => x[1].venue === "amm").sort((a, b) => b[1].n - a[1].n)[0]?.[0];
const nonSolCurve = [...cnt.entries()].filter((x) => x[1].venue === "curve" && x[1].quote).sort((a, b) => b[1].n - a[1].n)[0]?.[0];
for (const m of [amm, nonSolCurve].filter(Boolean)) {
  let t0 = Date.now(); const r = await call("pumpCandles", [m, { interval: "1m", lookback: 1440, maxTx: 2000, waitMs: 120000 }]);
  log(`deep ${m}: ${Date.now() - t0}ms venue=${r.venue} unit=${r.priceUnit} quote=${r.quoteMint} rate=${r.quoteSolRate} conv="${r.solConversion}" candles=${r.candles.length}`, r.coverage, "lastCandle", r.candles[r.candles.length - 1]);
  t0 = Date.now(); const r2 = await call("pumpCandles", [m, { interval: "1m", lookback: 1440, maxTx: 2000 }]);
  log(`repeat: ${Date.now() - t0}ms txsFetched ${r.coverage.txsFetched} -> ${r2.coverage.txsFetched} trades ${r2.coverage.trades}`);
  if (r.coverage.from) { const f = r.coverage.from + 60, to = f + 600; const r3 = await call("pumpCandles", [m, { interval: "1m", from: f, to, backfill: false }]);
    log(`range ${f}..${to}: candles ${r3.candles.length}, all inside: ${r3.candles.every((k: any) => k.t >= Math.floor(f / 60) * 60000 && k.t <= to * 1000)}`); }
}
log("disk cache files", fs.existsSync("/tmp/pc-9911") ? fs.readdirSync("/tmp/pc-9911").length : "n/a (8899 uses data/pump-candles)");
fs.writeFileSync(new URL("./live-pump-candles2.last.txt", import.meta.url), out.join("\n") + "\n"); process.exit(0);
