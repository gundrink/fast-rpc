// READ-ONLY: getTokenLargestAccounts via deployed fast-rpc on a fresh pump.fun Token-2022 mint + BONK; TPU status.
// FRPC_URL=... FRPC_KEY=... npx tsx examples/live-token-largest.ts
const BASE = (process.env.FRPC_URL ?? "https://fast-rpc-production.up.railway.app").replace(/\/$/, ""); const KEY = process.env.FRPC_KEY!;
let id = 0; const rows: string[] = [];
async function rpc(method: string, params: any = []) { const t = performance.now(); const r = await fetch(BASE, { method: "POST", headers: { "content-type": "application/json", "x-api-key": KEY }, body: JSON.stringify({ jsonrpc: "2.0", id: ++id, method, params }) }); const j: any = await r.json(); const ms = performance.now() - t; if (j.error) throw Object.assign(new Error(`${j.error.code} ${j.error.message}`), { ms }); return { v: j.result, ms }; }
const row = (ok: boolean, name: string, ms: number, note: string) => { rows.push(`${ok ? "PASS" : "FAIL"}  ${name.padEnd(46)} ${String(Math.round(ms)).padStart(6)}ms  ${note}`); };
const PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", T22 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
// fresh pump.fun Token-2022 mint from recent program txs
let pump: string | undefined;
const sigs = (await rpc("getSignaturesForAddress", [PUMP, { limit: 30 }])).v;
for (const s of sigs) { if (s.err) continue; const t = (await rpc("getTransaction", [s.signature, { encoding: "json" }]).catch(() => ({ v: null }))).v;
  pump = (t?.meta?.postTokenBalances ?? []).find((b: any) => b.programId === T22 && b.mint.endsWith("pump"))?.mint; if (pump) { rows.push(`info  pump.fun Token-2022 mint from tx ${s.signature.slice(0, 12)}…: ${pump}`); break; } }
for (const [label, mint] of [["pump.fun Token-2022", pump], ["BONK (classic SPL)", "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"]] as const) {
  if (!mint) { row(false, `getTokenLargestAccounts ${label}`, 0, "no mint found"); continue; }
  try {
    const { v, ms } = await rpc("getTokenLargestAccounts", [mint]);
    const ok = Array.isArray(v.value) && v.value.length > 0 && v.value.length <= 20 && v.context?.slot > 0 && v.value.every((x: any, i: number) => i === 0 || BigInt(v.value[i - 1].amount) >= BigInt(x.amount));
    row(ok, `getTokenLargestAccounts ${label}`, ms, `n=${v.value.length} top=${v.value[0]?.uiAmountString} dec=${v.value[0]?.decimals} via ${v._meta?.upstream ?? "native"} scanned=${v._meta?.holdersScanned}`);
    const c = await rpc("getTokenLargestAccounts", [mint]); row(c.ms < 2000, `  cached repeat`, c.ms, "");
    const b = (await rpc("getTokenAccountBalance", [v.value[0].address])).v.value; // cross-check top account
    row(b.amount === v.value[0].amount || BigInt(b.amount) > 0n, `  cross-check top via getTokenAccountBalance`, 0, `${b.amount} vs ${v.value[0].amount}${b.amount === v.value[0].amount ? " (equal)" : " (moved since scan)"}`);
  } catch (e: any) { row(false, `getTokenLargestAccounts ${label}`, e.ms ?? 0, e.message.slice(0, 140)); }
}
try { const { v, ms } = await rpc("getLargestAccounts", []); row(true, "getLargestAccounts", ms, `n=${v.value?.length}`); } catch (e: any) { row(false, "getLargestAccounts (best effort)", e.ms ?? 0, e.message.slice(0, 120)); }
try { const { v, ms } = await rpc("getTpuStatus"); row(v.enabled && v.allowSend, "getTpuStatus (allowSend on)", ms, JSON.stringify(v)); } catch (e: any) { row(false, "getTpuStatus", 0, e.message); }
try { const { v, ms } = await rpc("getTpuHandshake"); const ok = v.filter((x: any) => x.ok).length; row(ok > 0, "getTpuHandshake", ms, `${ok}/${v.length} ms=${v.map((x: any) => Math.round(x.ms)).join("/")}`); } catch (e: any) { row(false, "getTpuHandshake", 0, e.message); }
console.log(`${new Date().toISOString()} ${BASE}\n` + rows.join("\n")); process.exit(0);
