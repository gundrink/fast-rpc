// Live read-only: non-SOL-quote pump swaps (buy_v2/sell_v2 curves + non-WSOL PumpSwap pools).
// Checks event quoteAmount vs the tx's quote-token balance change of the curve / pool vault, and reports SOL-equivalent coverage.
// Usage: tsx examples/live-pump-quote.ts [url] [seconds] [samples]
import WebSocket from "ws";
import { curvePda } from "../src/pump.js";
const base = process.argv[2] ?? "http://127.0.0.1:9911", secs = Number(process.argv[3] ?? 60), N = Number(process.argv[4] ?? 30);
const rpc = async (method: string, params: any[]) => (await (await fetch(base, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) })).json()).result;
const evs: any[] = []; const by: Record<string, number> = {}; const routes: Record<string, number> = {};
const ws = new WebSocket(base.replace(/^http/, "ws"));
ws.on("open", () => ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "pumpSwapSubscribe", params: [{ allSwaps: true, position: false }] })));
ws.on("message", (d) => { const r = JSON.parse(String(d)).params?.result; if (r?.type !== "swap" || !r.quoteMint || r.update) return;
  evs.push(r); const k = `${r.venue}:${r.solEquivalent != null ? "converted" : r.solEquivalentReason}`; by[k] = (by[k] ?? 0) + 1;
  const q = `${r.quoteMint.slice(0, 6)}→${(r.solEquivalentSource ?? r.solEquivalentReason ?? "").split(":")[0]}`; routes[q] = (routes[q] ?? 0) + 1; });
setTimeout(async () => {
  ws.close();
  console.log("non-SOL-quote swaps:", evs.length, JSON.stringify(by)); console.log("quote → route:", JSON.stringify(routes));
  const ex = evs.find((e) => e.solEquivalent != null); if (ex) console.log("example:", JSON.stringify({ sig: ex.signature, venue: ex.venue, side: ex.side, mint: ex.mint, quoteMint: ex.quoteMint, quoteAmount: ex.quoteAmount, quoteDecimals: ex.quoteDecimals, priceQuote: ex.priceQuote, quoteSolRate: ex.quoteSolRate, solEquivalent: ex.solEquivalent, priceSol: ex.priceSol, solEquivalentSource: ex.solEquivalentSource, priceValid: ex.priceValid, dust: ex.dust }));
  const pick = [...evs.filter((e) => e.venue === "curve").slice(0, N / 2), ...evs.filter((e) => e.venue !== "curve").slice(0, N / 2)];
  const err: Record<string, number[]> = { curve: [], amm: [] }; let miss = 0;
  for (const e of pick) {
    const tx = await rpc("getTransaction", [e.signature, { encoding: "json", maxSupportedTransactionVersion: 0, commitment: "confirmed" }]).catch(() => null);
    if (!tx?.meta) { miss++; continue; }
    const pre = new Map((tx.meta.preTokenBalances ?? []).map((b: any) => [b.accountIndex, b]));
    const owner = e.venue === "curve" ? curvePda(e.mint) : e.pool;
    const v = (tx.meta.postTokenBalances ?? []).find((b: any) => b.owner === owner && b.mint === e.quoteMint);
    if (!v) { miss++; continue; }
    const d = Math.abs(Number(BigInt(v.uiTokenAmount.amount) - BigInt((pre.get(v.accountIndex) as any)?.uiTokenAmount?.amount ?? 0)));
    const ud = (tx.meta.postTokenBalances ?? []).filter((b: any) => b.owner === e.wallet && b.mint === e.quoteMint).reduce((a: number, b: any) => a + Number(BigInt(b.uiTokenAmount.amount) - BigInt((pre.get(b.accountIndex) as any)?.uiTokenAmount?.amount ?? 0)), 0);
    // vault delta = net amount, or net ± fees when the program leaves fees in the vault (v3 curves, lp fee in pools)
    const q = Number(e.quoteAmountRaw), f = Number(e.quoteFeeRaw ?? 0);
    const x = Math.min(...[q, q + f, q - f].map((v) => Math.abs(v / d - 1) * 100));
    if (x > 0.01 && process.env.DEBUG) console.log("diff", e.venue, e.side, e.signature.slice(0, 10), "event", e.quoteAmountRaw, "vaultΔ", d, "userΔ", ud, (tx.meta.logMessages ?? []).find((l: string) => /Instruction: (Buy|Sell)/.test(l)));
    err[e.venue === "curve" ? "curve" : "amm"].push(x);
  }
  const st = (a: number[]) => { const s = [...a].sort((x, y) => x - y); return a.length ? { n: a.length, exact: s.filter((x) => x < 1e-9).length, within1pct: s.filter((x) => x <= 1).length, p50: s[s.length >> 1].toFixed(4) + "%", max: s.at(-1)!.toFixed(4) + "%" } : { n: 0 }; };
  console.log("curve quoteAmount vs curve quote-vault delta:", JSON.stringify(st(err.curve)));
  console.log("amm quoteAmount vs pool quote-vault delta:", JSON.stringify(st(err.amm)));
  console.log("unverifiable:", miss); process.exit(0);
}, secs * 1000);
