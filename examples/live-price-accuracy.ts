// Live read-only: compare streamed pump/PumpSwap event prices against on-chain post-tx account balances.
// AMM: pool-owned vault balances (WSOL vs token, decimals) -> price; curve: curve-PDA token account vs realTokenReserves.
// Usage: tsx examples/live-price-accuracy.ts [url] [seconds] [samplesPerVenue]
import WebSocket from "ws";
import { curvePda } from "../src/pump.js";
const base = process.argv[2] ?? "http://127.0.0.1:9911", secs = Number(process.argv[3] ?? 40), N = Number(process.argv[4] ?? 25);
const WSOL = "So11111111111111111111111111111111111111112";
const rpc = async (method: string, params: any[]) => (await (await fetch(base, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) })).json()).result;
const evs: any[] = []; const cnt: Record<string, number> = {};
const ws = new WebSocket(base.replace(/^http/, "ws"));
ws.on("open", () => ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ allSwaps: true }] })));
ws.on("message", (d) => { const m = JSON.parse(String(d)); const r = m.params?.result; if (!r || r.type !== "swap") return;
  const k = `${r.venue}:${r.priceValid ? "valid" : r.priceInvalidReason}`; cnt[k] = (cnt[k] ?? 0) + 1; if (r.priceValid && r.commitment === "processed") evs.push(r); if (r.priceInvalidReason === "outlier" && outl.length < 5) outl.push(r); });
const outl: any[] = [];
setTimeout(async () => {
  ws.close(); console.log("event counts by venue:validity", JSON.stringify(cnt));
  const pick = (v: string) => evs.filter((e) => e.venue === v).filter((_, i, a) => i % Math.max(1, Math.floor(a.length / N)) === 0).slice(0, N);
  const errs: Record<string, number[]> = { curve: [], amm: [] }; let miss = 0;
  for (const e of [...pick("curve"), ...pick("amm")]) {
    const tx = await rpc("getTransaction", [e.signature, { encoding: "json", maxSupportedTransactionVersion: 0, commitment: "confirmed" }]).catch(() => null);
    if (!tx?.meta) { miss++; continue; }
    const pb: any[] = tx.meta.postTokenBalances ?? [];
    if (e.venue === "amm") {
      const sol = pb.find((b) => b.owner === e.pool && b.mint === WSOL), tok = pb.find((b) => b.owner === e.pool && b.mint === e.mint);
      if (!sol || !tok) { miss++; continue; }
      const p = Number(sol.uiTokenAmount.amount) / 1e9 / (Number(tok.uiTokenAmount.amount) / 10 ** tok.uiTokenAmount.decimals);
      errs.amm.push(Math.abs(e.price / p - 1) * 100);
    } else {
      // curve token account balance should equal realTokenReserves after the LAST trade on this mint in the tx; use relative diff
      const pda = curvePda(e.mint), acct = pb.find((b) => b.mint === e.mint && b.owner === pda);
      const rt = Number(e.realTokenReserves); if (!acct || !rt) { miss++; continue; }
      // curve ATA holds realTokenReserves (+ any stray transfers); later trades in the same tx shift it, so this is an upper bound
      // the ATA also holds the 206.9M tokens reserved for migration (1B supply - 793.1M sellable)
      errs.curve.push(Math.abs((Number(acct.uiTokenAmount.amount) - 206_900_000e6) / rt - 1) * 100);
    }
  }
  const st = (a: number[]) => { const s = [...a].sort((x, y) => x - y); return a.length ? { n: a.length, within0_1pct: s.filter((x) => x <= 0.1).length, within1pct: s.filter((x) => x <= 1).length, p50: s[s.length >> 1].toFixed(4) + "%", max: s.at(-1)!.toFixed(4) + "%" } : { n: 0 }; };
  console.log("curve (event realTokenReserves vs curve token account):", JSON.stringify(st(errs.curve)));
  console.log("amm (event price vs pool vault balances):", JSON.stringify(st(errs.amm)));
  for (const o of outl) console.log("outlier sample", JSON.stringify({ sig: o.signature, mint: o.mint, price: o.price, sol: o.solAmount, side: o.side }));
  console.log("unverifiable samples:", miss);
  process.exit(0);
}, secs * 1000);
