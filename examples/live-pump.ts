// Live read-only check: pumpSwapSubscribe (allSwaps + wallets), pumpCurveSubscribe, pumpCurvePrice. Usage: tsx examples/live-pump.ts [url] [seconds]
import fs from "node:fs"; import WebSocket from "ws";
const base = process.argv[2] ?? "http://127.0.0.1:9911"; const secs = Number(process.argv[3] ?? 40);
const wallets = fs.existsSync("/workspace/sapi/wallets.txt") ? fs.readFileSync("/workspace/sapi/wallets.txt", "utf8").split("\n").map((l) => l.trim().split(/\s+/)[0]).filter((x) => x && x.length >= 32) : [];
const ws = new WebSocket(base.replace(/^http/, "ws"));
const ev: any[] = []; const lat: number[] = []; let curveN = 0, wHits = 0, pos = 0, conf = 0; let firstMint = "";
const slotSeen = new Map<number, number>(); const slotLat: number[] = [];
ws.on("open", () => {
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 5, method: "slotSubscribe", params: [] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ allSwaps: true, confirmations: true }] }));
  ws.send(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "pumpSwapSubscribe", params: [{ wallets, tags: Object.fromEntries(wallets.map((w) => [w, "sapi"])) }] }));
});
ws.on("message", (d) => {
  const m = JSON.parse(String(d));
  if (m.id) { console.log("sub", m.id, m.result ?? m.error); return; }
  const r = m.params.result; const now = Date.now();
  if (m.method === "slotNotification") { if (!slotSeen.has(r.slot)) slotSeen.set(r.slot, now); return; }
  if (m.method === "pumpCurveNotification") { curveN++; if (curveN <= 3) console.log("curve", JSON.stringify(r)); return; }
  if (r.update === "position") { pos++; if (pos <= 3) console.log("position", r.side, r.mint, r.pctOfPositionSold); return; }
  if (r.update === "confirmed") { conf++; return; }
  if (r.tag) { wHits++; console.log("WALLET HIT", r.tag, r.wallet, r.side, r.mint, r.solAmount); return; }
  ev.push(r); lat.push(now - r.blockTime * 1000); if (slotSeen.has(r.slot)) slotLat.push(now - slotSeen.get(r.slot)!);
  if (ev.length <= 4) console.log("swap", JSON.stringify(r));
  if (!firstMint && r.program === "pump" && r.mint) { firstMint = r.mint; ws.send(JSON.stringify({ jsonrpc: "2.0", id: 3, method: "pumpCurveSubscribe", params: [firstMint] })); }
});
setTimeout(async () => {
  const s = [...lat].sort((a, b) => a - b); const q = (p: number) => s[Math.floor(p * (s.length - 1))];
  console.log(`events=${ev.length} pump=${ev.filter((e) => e.program === "pump").length} pumpswap=${ev.filter((e) => e.program === "pumpswap").length} creates=${ev.filter((e) => e.create).length} sells=${ev.filter((e) => e.side === "sell").length} positionFollowups=${pos} confirmedFollowups=${conf} walletHits=${wHits} curveNotifs=${curveN}`);
  console.log(`latency now-blockTime ms (blockTime has 1s resolution): p50=${q(0.5)} p90=${q(0.9)} min=${s[0]}`);
  const z = [...slotLat].sort((a, b) => a - b); const qz = (p: number) => z[Math.floor(p * (z.length - 1))];
  console.log(`latency vs first slotNotification of the same slot (ms, n=${z.length}): p50=${qz(0.5)} p90=${qz(0.9)} p99=${qz(0.99)}`);
  const t0 = Date.now();
  const r = await fetch(base, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 9, method: "pumpCurvePrice", params: [firstMint] }) }).then((x) => x.json());
  console.log("pumpCurvePrice", Date.now() - t0, "ms", JSON.stringify(r));
  process.exit(0);
}, secs * 1000);
