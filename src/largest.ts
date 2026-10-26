// getTokenLargestAccounts without the (publicly blocked) native method: stream getProgramAccounts over the
// mint's token accounts (dataSlice = amount only), keep a top-N heap. Constant memory even for millions of holders.
import { RpcError, INVALID_PARAMS } from "./errors.js";
import { isPubkey } from "./codec.js";

const TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
type Rpc = (method: string, params: any[]) => Promise<any>;
export interface LargestOptions { urls: string[]; fetcher?: typeof fetch; top?: number; ttlMs?: number; timeoutMs?: number; maxBytes?: number }

const ITEM = /"pubkey":"([1-9A-HJ-NP-Za-km-z]{32,44})","account":\{[^{}]*?"data":\["([A-Za-z0-9+/=]*)"/g;
const ITEM_REV = /"account":\{[^{}]*?"data":\["([A-Za-z0-9+/=]*)"[^{}]*\},"pubkey":"([1-9A-HJ-NP-Za-km-z]{32,44})"/g;

/** Scan a streamed gPA JSON body; returns top-N {address, raw} by amount and the count scanned. */
export async function scanTopAmounts(body: ReadableStream<Uint8Array>, top: number, maxBytes = Infinity) {
  const best: { address: string; raw: bigint }[] = []; let n = 0, bytes = 0, buf = "", head = "";
  const dec = new TextDecoder(); const reader = body.getReader();
  const push = (address: string, b64: string) => {
    const d = Buffer.from(b64, "base64"); if (d.length < 8) return; n++;
    const raw = d.readBigUInt64LE(0);
    if (best.length < top) { best.push({ address, raw }); best.sort((a, b) => (b.raw > a.raw ? 1 : b.raw < a.raw ? -1 : 0)); return; }
    if (raw <= best[best.length - 1].raw) return;
    best[best.length - 1] = { address, raw }; best.sort((a, b) => (b.raw > a.raw ? 1 : b.raw < a.raw ? -1 : 0));
  };
  for (;;) {
    const { done, value } = await reader.read(); if (done) break;
    bytes += value.length; if (bytes > maxBytes) { await reader.cancel(); throw new RpcError(-32005, `getTokenLargestAccounts: holder set exceeds ${maxBytes} bytes`); }
    buf += dec.decode(value, { stream: true }); if (!head) head = buf.slice(0, 512);
    let last = 0; ITEM.lastIndex = 0; let m: RegExpExecArray | null;
    while ((m = ITEM.exec(buf))) { push(m[1], m[2]); last = ITEM.lastIndex; }
    ITEM_REV.lastIndex = 0; while ((m = ITEM_REV.exec(buf))) { push(m[2], m[1]); last = Math.max(last, ITEM_REV.lastIndex); }
    buf = buf.slice(last); if (buf.length > 1 << 20) buf = buf.slice(-4096); // never grow unbounded
  }
  if (n === 0 && /"error"/.test(head + buf)) { const e = /"error":\s*\{[^}]*"code":\s*(-?\d+)[^}]*"message":\s*"([^"]*)"/.exec(head + buf); throw new RpcError(Number(e?.[1] ?? -32000), e?.[2] ?? "upstream error"); }
  return { best, scanned: n, bytes };
}

export function createTokenLargest(rpc: Rpc, o: LargestOptions) {
  const fetcher = o.fetcher ?? fetch; const top = o.top ?? 20; const ttl = o.ttlMs ?? Number(process.env.TOKEN_LARGEST_TTL_MS ?? 60_000);
  const cache = new Map<string, { at: number; v: any }>(); const inflight = new Map<string, Promise<any>>();
  async function compute(mint: string, commitment?: string) {
    const info = await rpc("getAccountInfo", [mint, { encoding: "jsonParsed", ...(commitment ? { commitment } : {}) }]);
    const acc = info?.value; if (!acc) throw INVALID_PARAMS("Invalid param: could not find mint");
    const program = acc.owner; const decimals = acc.data?.parsed?.info?.decimals;
    if ((program !== TOKEN_PROGRAM && program !== TOKEN_2022_PROGRAM) || acc.data?.parsed?.type !== "mint") throw INVALID_PARAMS("Invalid param: not a Token mint");
    // Token: fixed 165-byte accounts. Token-2022: accounts may carry extensions (size varies), so no dataSize;
    // the mint at offset 0 still identifies token accounts (mints start with a 4-byte COption tag, never the mint key)
    const filters: any[] = [{ memcmp: { offset: 0, bytes: mint } }]; if (program === TOKEN_PROGRAM) filters.push({ dataSize: 165 });
    const body = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getProgramAccounts", params: [program, { encoding: "base64", dataSlice: { offset: 64, length: 8 }, filters, withContext: true, ...(commitment ? { commitment } : {}) }] });
    const errors: string[] = [];
    for (const url of o.urls) {
      const t0 = Date.now();
      try {
        const r = await fetcher(url, { method: "POST", headers: { "content-type": "application/json" }, body, signal: AbortSignal.timeout(o.timeoutMs ?? Number(process.env.TOKEN_LARGEST_TIMEOUT_MS ?? 120_000)) });
        if (!r.ok || !r.body) { errors.push(`${host(url)}: HTTP ${r.status}`); continue; }
        const { best, scanned, bytes } = await scanTopAmounts(r.body, top, o.maxBytes ?? Number(process.env.TOKEN_LARGEST_MAX_BYTES ?? 2_000_000_000));
        const slot = (await rpc("getSlot", [])) as number;
        const div = 10 ** decimals;
        const value = best.map(({ address, raw }) => { const s = fmt(raw, decimals); return { address, amount: raw.toString(), decimals, uiAmount: Number(raw) / div, uiAmountString: s }; });
        return { context: { slot }, value, _meta: { source: "getProgramAccounts", upstream: host(url), program, holdersScanned: scanned, bytes, ms: Date.now() - t0 } };
      } catch (e: any) { errors.push(`${host(url)}: ${e?.message ?? e}`.slice(0, 200)); }
    }
    throw new RpcError(-32000, `getTokenLargestAccounts fallback failed on all upstreams: ${errors.join(" | ")}`);
  }
  return async (mint: any, cfg: any = {}) => {
    if (!isPubkey(mint)) throw INVALID_PARAMS("getTokenLargestAccounts: params[0] must be a base58 mint");
    const k = `${mint}:${cfg?.commitment ?? ""}`; const c = cache.get(k);
    if (c && Date.now() - c.at < ttl) return c.v;
    let p = inflight.get(k); if (!p) { p = compute(mint, cfg?.commitment).finally(() => inflight.delete(k)); inflight.set(k, p); }
    const v = await p; cache.set(k, { at: Date.now(), v }); if (cache.size > 500) cache.delete(cache.keys().next().value!);
    return v;
  };
}
const host = (u: string) => { try { return new URL(u).host; } catch { return u; } };
function fmt(raw: bigint, d: number) { if (!d) return raw.toString(); const s = raw.toString().padStart(d + 1, "0"); const i = s.slice(0, -d), f = s.slice(-d).replace(/0+$/, ""); return f ? `${i}.${f}` : i; }
