// Live check of pumpCurveScreener (+ WS pumpCurveScreenerSubscribe) against a running fast-rpc (read-only mainnet).
// usage: npx tsx examples/live-pump-screener.ts http://127.0.0.1:9911 [warmupSec=120]
import WebSocket from "ws"; import fs from "node:fs";
const url = process.argv[2] ?? "http://127.0.0.1:9911", secs = Number(process.argv[3] ?? 120);
const key = process.env.FAST_RPC_LOCAL_API_KEY; const q = key ? `?api-key=${key}` : "";
const call = async (method: string, params: any[]) => { const r = await fetch(url + q, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }); const j: any = await r.json(); if (j.error) throw new Error(method + ": " + j.error.message); return j.result; };
const out: string[] = []; const log = (...a: any[]) => { const s = a.map((x) => typeof x === "string" ? x : JSON.stringify(x)).join(" "); out.push(s); console.log(s); };
const ws = new WebSocket(url.replace(/^http/, "ws") + q); let notes = 0, wsErr: any = null;
ws.on("open", () => ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpCurveScreenerSubscribe", params: [{ limit: 5, intervalSec: 30, minUniqueBuyers5m: 5 }] })));
ws.on("message", (b) => { const m = JSON.parse(String(b)); if (m.error) wsErr = m.error; if (m.method === "pumpCurveScreenerNotification") notes++; });
setTimeout(async () => {
  for (const p of [{ limit: 10 }, { limit: 10, minBonding: 30, maxBonding: 95, minUniqueBuyers5m: 10, excludeDevSold: true, maxTop10Pct: 40 }, { limit: 5, sort: "eta" }]) {
    const t0 = Date.now(); const r = await call("pumpCurveScreener", [p]); const toks = r.tokens;
    const nulls = (k: string) => toks.filter((t: any) => t[k] == null).length;
    log(`params=${JSON.stringify(p)} ${Date.now() - t0}ms candidates=${r.candidates} afterFilters=${r.afterFilters} returned=${toks.length} nulls holder=${nulls("holderCount")} top10=${nulls("top10Pct")} dev=${nulls("devHoldingPct")} age=${nulls("ageSec")} eta=${nulls("bondingEtaSec")} fresh=${toks.filter((t: any) => t.flags.freshWalletRatio == null).length}`);
    for (const t of toks.slice(0, 5)) log("  ", { mint: t.mint, score: t.score, prog: t.bondingProgress, vel: t.bondingVelocityPctPerMin, eta: t.bondingEtaSec ?? t.bondingEtaReason, age: t.ageSec ?? t.ageReason, ageSrc: t.ageSource, buyers5m: t.uniqueBuyers["5m"], net5m: t.netFlowSol["5m"], bsr5m: t.buySellRatio["5m"], holders: t.holderCount ?? t.holderCountReason, top10: t.top10Pct ?? t.top10Reason, dev: t.devHoldingPct ?? t.devHoldingReason, src: t.enrichSource ?? t.fastPathError, flags: t.flags });
  }
  log("ws notifications", notes, "wsError", wsErr);
  fs.writeFileSync(new URL("./live-pump-screener.last.txt", import.meta.url), out.join("\n") + "\n"); ws.close(); process.exit(0);
}, secs * 1000);
