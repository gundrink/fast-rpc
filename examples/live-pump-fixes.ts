// Live check for donsol bug reports: regime warming, priceSolReason, levels coverage with auto-warm, non-SOL curve venue (read-only).
// usage: npx tsx examples/live-pump-fixes.ts http://127.0.0.1:9911 [seconds=330]
import WebSocket from "ws"; import fs from "node:fs";
const url = process.argv[2] ?? "http://127.0.0.1:9911", secs = Number(process.argv[3] ?? 330);
const key = process.env.FAST_RPC_LOCAL_API_KEY; const q = key ? `?api-key=${key}` : "";
const call = async (method: string, params: any[]) => { const r = await fetch(url + q, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }); const j: any = await r.json(); if (j.error) throw new Error(method + ": " + j.error.message); return j.result; };
const out: string[] = []; const log = (...a: any[]) => { const s = a.map((x) => typeof x === "string" ? x : JSON.stringify(x)).join(" "); out.push(s); console.log(s); };
const rg0 = await call("marketRegime", []); log("regime@start", { label: rg0.label, labelStatus: rg0.labelStatus, net5m: rg0.netFlow["5m"], net1h: rg0.netFlow["1h"].complete, sol15: rg0.solChange["15m"] });
const ws = new WebSocket(url.replace(/^http/, "ws") + q);
let nonSol = 0, nullNoReason = 0, ammOtherNonSol = 0, ammOtherNull = 0; const reasons: Record<string, number> = {}; const xsc: any[] = []; const curveNonSol = new Set<string>();
const cnt = new Map<string, number>(); let subMints: string[] = []; let mintSw = 0, mintLv = 0; const lvTimeline: string[] = [];
ws.on("open", () => ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ allSwaps: true, position: false }] })));
ws.on("message", (b) => { const m = JSON.parse(String(b)); const e = m.params?.result; if (!e || e.type !== "swap") return;
  if (m.method !== "pumpSwapNotification") return;
  if (m.params.subscription === subId) { mintSw++; if (e.levels) mintLv++; return; }
  if (e.priceSol == null && !e.priceSolReason) nullNoReason++;
  if (e.quoteMint) { nonSol++; if (e.priceSol == null) reasons[e.priceSolReason] = (reasons[e.priceSolReason] ?? 0) + 1;
    if (e.venue === "amm-other") { ammOtherNonSol++; if (e.priceSol == null) ammOtherNull++; }
    if (e.venue === "curve") curveNonSol.add(e.mint); }
  if (e.mint?.startsWith("Xsc9") || e.quoteMint?.startsWith("Xsc9")) xsc.push({ mint: e.mint, venue: e.venue, quoteMint: e.quoteMint, priceSol: e.priceSol, solEquivalent: e.solEquivalent, priceSolReason: e.priceSolReason });
  if (e.mint && e.priceValid) cnt.set(e.mint, (cnt.get(e.mint) ?? 0) + 1); });
let subId = -1;
ws.on("message", (b) => { const m = JSON.parse(String(b)); if (m.id === 2) subId = m.result; });
setTimeout(() => { subMints = [...cnt.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20).map((x) => x[0]);
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "pumpSwapSubscribe", params: [{ mints: subMints, levels: true, position: false }] })); }, 40_000);
for (const t of [70, 100, 150]) setTimeout(() => { lvTimeline.push(`${t}s: ${mintLv}/${mintSw}`); mintLv = 0; mintSw = 0; }, t * 1000);
setTimeout(async () => {
  log(`subscribed-mint levels coverage (window: levels/swaps): ${lvTimeline.join(", ")}`);
  log(`swaps priceSol null without priceSolReason: ${nullNoReason}; nonSol swaps ${nonSol}, reasons`, reasons, `amm-other nonSol ${ammOtherNonSol}, null priceSol ${ammOtherNull}`);
  log("Xsc9 samples", xsc.slice(-3));
  for (const m of [...curveNonSol].slice(0, 3)) { const c = await call("pumpCandles", [m, { interval: "1m", lookback: 30 }]); log(`non-SOL curve ${m} venue=${c.venue} quote=${c.quoteMint} candles=${c.candles.length}`); }
  for (const t of [0, 1]) { if (t) await new Promise((r) => setTimeout(r, Math.max(0, (secs - 160) * 1000))); const rg = await call("marketRegime", []);
    log(`regime@${t ? secs : 160}s`, { label: rg.label, labelStatus: rg.labelStatus, labelNote: rg.labelNote, net5m: rg.netFlow["5m"], net1h: { complete: rg.netFlow["1h"].complete, netSol: rg.netFlow["1h"].netSol }, sol15: rg.solChange["15m"], units: rg.units }); }
  fs.writeFileSync(new URL("./live-pump-fixes.last.txt", import.meta.url), out.join("\n") + "\n"); process.exit(0);
}, 160_000);
