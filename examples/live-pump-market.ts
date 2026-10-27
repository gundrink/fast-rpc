// Live check of pumpCandles / levels / marketRegime against a running fast-rpc (read-only).
// usage: npx tsx examples/live-pump-market.ts http://127.0.0.1:9911 [seconds=90]
import WebSocket from "ws"; import fs from "node:fs";
const url = process.argv[2] ?? "http://127.0.0.1:9911", secs = Number(process.argv[3] ?? 90);
const key = process.env.FAST_RPC_LOCAL_API_KEY; const q = key ? `?api-key=${key}` : "";
const call = async (method: string, params: any[]) => { const r = await fetch(url + q, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }); const j: any = await r.json(); if (j.error) throw new Error(method + ": " + j.error.message); return j.result; };
const ws = new WebSocket(url.replace(/^http/, "ws") + q);
const cnt = new Map<string, number>(); const okMints = new Set<string>(); let lv = 0, lvWith = 0, lvBad = 0; const candleNotes: any[] = [];
ws.on("open", () => ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ allSwaps: true, levels: true, position: false }] })));
ws.on("message", (b) => { const m = JSON.parse(String(b)); const e = m.params?.result; if (!e) return;
  if (m.method === "pumpSwapNotification" && e.type === "swap" && e.mint) { cnt.set(e.mint, (cnt.get(e.mint) ?? 0) + 1); lv++; if (e.priceValid && (e.venue === "curve" || e.venue === "amm")) okMints.add(e.mint);
    if (e.levels) { lvWith++; const s = e.levels.support, r = e.levels.resistance; if ((s && !(s.price < e.price)) || (r && !(r.price > e.price))) lvBad++; } }
  if (m.method === "pumpCandlesNotification") candleNotes.push(e); });
const out: string[] = []; const log = (...a: any[]) => { const s = a.map((x) => typeof x === "string" ? x : JSON.stringify(x)).join(" "); out.push(s); console.log(s); };
setTimeout(async () => {
  const top = [...cnt.entries()].filter((x) => okMints.has(x[0])).sort((a, b) => b[1] - a[1]).slice(0, 5).map((x) => x[0]);
  log(`swaps ${lv}; with levels ${lvWith}; levels on wrong side ${lvBad}`);
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "pumpCandlesSubscribe", params: [top[0], { interval: "1s" }] }));
  for (const m of top) {
    const t0 = Date.now(); const c = await call("pumpCandles", [m, { interval: "1m", lookback: 60 }]); const dt = Date.now() - t0;
    const last = c.candles[c.candles.length - 1]; let cp: any = null; try { cp = await call("pumpCurvePrice", [m]); } catch {}
    const bad = c.candles.filter((k: any) => !(k.l <= k.o && k.o <= k.h && k.l <= k.c && k.c <= k.h && k.v >= 0)).length;
    log(`${m} venue=${c.venue} quote=${c.quoteMint} candles=${c.candles.length} badOHLC=${bad} backfill=${JSON.stringify(c.backfill)} ${dt}ms lastClose=${last?.c} curve=${cp?.complete ? "migrated" : cp?.price} dev%=${cp?.price && last && !cp.complete ? ((last.c / cp.price - 1) * 100).toFixed(2) : "-"}`);
  }
  const c5 = await call("pumpCandles", [top[0], { interval: "5s", lookback: 24 }]); log("5s sample", c5.candles.slice(-3));
  await new Promise((r) => setTimeout(r, 15_000));
  log(`candle WS notifications in 15s: ${candleNotes.length}`, candleNotes.slice(-1));
  const rg = await call("marketRegime", []); log("regime", { solUsd: rg.solUsd, solChange: rg.solChange, net5m: rg.netFlow["5m"], net1h: rg.netFlow["1h"], recent: rg.recentCurveTokens, label: rg.label });
  fs.writeFileSync(new URL("./live-pump-market.last.txt", import.meta.url), out.join("\n") + "\n"); process.exit(0);
}, secs * 1000);
