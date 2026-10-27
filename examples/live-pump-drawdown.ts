// Live read-only: pick the busiest pump mint for 6s, then track high/drawdown with alerts. Usage: tsx examples/live-pump-drawdown.ts [url] [seconds]
import WebSocket from "ws";
const base = process.argv[2] ?? "http://127.0.0.1:9911"; const secs = Number(process.argv[3] ?? 45);
const ws = new WebSocket(base.replace(/^http/, "ws")); const cnt = new Map<string, number>(); let phase = 0, mint = "";
const out: any[] = [];
ws.on("open", () => ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ allSwaps: true }] })));
ws.on("message", (d) => {
  const m = JSON.parse(String(d)); if (m.id) { console.log("sub", m.id, m.result ?? m.error); return; }
  const r = m.params.result;
  if (m.params.subscription === pick && r.mint && r.price) cnt.set(r.mint, (cnt.get(r.mint) ?? 0) + 1);
  if (phase === 1 && m.params.subscription === track) { out.push(r); if (r.type !== "swap") console.log("ALERT", JSON.stringify(r)); }
});
let pick = 0, track = 0;
ws.on("message", (d) => { const m = JSON.parse(String(d)); if (m.id === 1) pick = m.result; if (m.id === 2) track = m.result; });
setTimeout(() => {
  const top = [...cnt.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8).map((x) => x[0]); mint = top[0]; console.log("busiest mints", top.join(","));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "pumpSwapSubscribe", params: [{ mints: top, drawdownAlerts: [0.5, 1, 3], reboundPct: 0.5 }] })); phase = 1;
}, 6000);
setTimeout(() => {
  const s = out.filter((e) => e.type === "swap");
  console.log("max drawdownPct seen:", Math.max(0, ...s.map((e) => e.drawdownPct ?? 0)).toFixed(2));
  console.log(`swaps=${s.length} alerts=${out.length - s.length} last=${JSON.stringify(s.at(-1) && { price: s.at(-1).price, highSinceSubscribe: s.at(-1).highSinceSubscribe, drawdownPct: s.at(-1).drawdownPct })}`);
  process.exit(0);
}, (secs + 6) * 1000);
