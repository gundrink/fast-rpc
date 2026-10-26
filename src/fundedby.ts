// Helius "funded-by" equivalent: first incoming SOL transfer to a wallet, walked back up to 3 hops.
// Shallow signature pagination (works on public non-archival RPC): if the oldest tx is not reached the
// result carries complete:false / truncated:true and reports the earliest incoming transfer that WAS seen.
import fs from "node:fs";
import { RpcError, INVALID_PARAMS } from "./errors.js";
import { b58decode } from "./codec.js";

export type LabelType = "cex" | "mixer" | "bridge" | "program" | "other";
export interface Label { address: string; name: string; type: LabelType; source?: string }
export type Call = (m: string, p: any[]) => Promise<any>;
export interface FundedByOptions {
  labelsFile?: string; cacheFile?: string; cacheTtlMs?: number;
  /** pages of 1000 signatures per hop (default FUNDED_BY_MAX_PAGES or 3) */
  maxPages?: number; pageSize?: number;
  /** oldest-first txs inspected per hop looking for the funding transfer (default 20) */
  maxTxScan?: number; timeoutMs?: number;
}
const SYS = "11111111111111111111111111111111";
const SOL_MINT = "So11111111111111111111111111111111111111112";

export function loadLabels(file?: string): Map<string, Label> {
  const m = new Map<string, Label>();
  if (!file || !fs.existsSync(file)) return m;
  const j = JSON.parse(fs.readFileSync(file, "utf8"));
  for (const l of j.labels ?? []) if (l?.address && l?.type) m.set(l.address, l);
  return m;
}

/** Incoming SOL transfers to `addr` in a getTransaction(json) result: System transfer / createAccount / transferWithSeed (top-level + inner). */
export function incomingSol(tx: any, addr: string): { from: string; lamports: number }[] {
  const msg = tx?.transaction?.message; if (!msg || tx.meta?.err) return [];
  const keys: string[] = [...(msg.accountKeys ?? []).map((k: any) => (typeof k === "string" ? k : k.pubkey)),
    ...(tx.meta?.loadedAddresses?.writable ?? []), ...(tx.meta?.loadedAddresses?.readonly ?? [])];
  const ixs = [...(msg.instructions ?? []), ...(tx.meta?.innerInstructions ?? []).flatMap((i: any) => i.instructions ?? [])];
  const out: { from: string; lamports: number }[] = [];
  for (const ix of ixs) {
    if (keys[ix.programIdIndex] !== SYS || typeof ix.data !== "string") continue;
    let d: Buffer; try { d = b58decode(ix.data); } catch { continue; }
    if (d.length < 12) continue;
    const t = d.readUInt32LE(0); const a = ix.accounts ?? [];
    const pick = (fi: number, ti: number) => { if (keys[a[ti]] === addr && keys[a[fi]] !== addr) out.push({ from: keys[a[fi]], lamports: Number(d.readBigUInt64LE(4)) }); };
    if (t === 0 || t === 2) pick(0, 1); else if (t === 11) pick(0, 2);
  }
  return out;
}

export function createFundedBy(call: Call, opts: FundedByOptions = {}) {
  const labelsFile = opts.labelsFile ?? process.env.LABELS_FILE ?? "data/labels.json";
  let labels = loadLabels(labelsFile);
  const cacheFile = opts.cacheFile ?? process.env.FUNDED_BY_CACHE_FILE;
  const ttl = opts.cacheTtlMs ?? Number(process.env.FUNDED_BY_CACHE_TTL_MS ?? 3_600_000);
  const maxPages = opts.maxPages ?? Number(process.env.FUNDED_BY_MAX_PAGES ?? 3);
  const pageSize = opts.pageSize ?? 1000;
  const maxTxScan = opts.maxTxScan ?? 20;
  const timeoutMs = opts.timeoutMs ?? Number(process.env.FUNDED_BY_TIMEOUT_MS ?? 30_000);
  const maxTxVersion = Number(process.env.FUNDED_BY_MAX_TX_VERSION ?? 1);
  const cache = new Map<string, { at: number; v: any }>();
  if (cacheFile && fs.existsSync(cacheFile)) try { for (const [k, v] of Object.entries(JSON.parse(fs.readFileSync(cacheFile, "utf8")))) cache.set(k, v as any); } catch {}
  const persist = () => { if (cacheFile) try { fs.writeFileSync(cacheFile, JSON.stringify(Object.fromEntries(cache))); } catch {} };

  async function oneHop(addr: string, deadline: number) {
    const sigs: any[] = []; let before: string | undefined; let reachedOldest = false;
    for (let p = 0; p < maxPages; p++) {
      if (Date.now() > deadline) break;
      const page: any[] = await call("getSignaturesForAddress", [addr, before ? { limit: pageSize, before } : { limit: pageSize }]);
      sigs.push(...page);
      if (page.length < pageSize) { reachedOldest = true; break; }
      before = page[page.length - 1].signature;
    }
    const ok = sigs.filter((s) => !s.err).reverse(); // oldest first
    for (const s of ok.slice(0, maxTxScan)) {
      if (Date.now() > deadline) break;
      let tx: any;
      try { tx = await call("getTransaction", [s.signature, { encoding: "json", maxSupportedTransactionVersion: maxTxVersion, commitment: "finalized" }]); }
      catch (e: any) { if (e instanceof RpcError && e.code === -32015) continue; throw e; } // unsupported tx version: skip it
      if (!tx) continue;
      const inc = incomingSol(tx, addr);
      if (inc.length) return { complete: reachedOldest, scanned: sigs.length, hit: { ...inc[0], signature: s.signature, slot: tx.slot ?? s.slot, timestamp: tx.blockTime ?? s.blockTime ?? null } };
    }
    return { complete: reachedOldest, scanned: sigs.length, hit: null };
  }

  async function fundedBy(address: string, o: { hops?: number } = {}) {
    if (typeof address !== "string") throw INVALID_PARAMS("funded-by: address must be a base58 string");
    try { if (b58decode(address).length !== 32) throw 0; } catch { throw INVALID_PARAMS("funded-by: invalid address"); }
    const hops = o.hops ?? 2;
    if (!Number.isInteger(hops) || hops < 1 || hops > 3) throw INVALID_PARAMS("funded-by: hops must be 1..3");
    const ck = `${address}:${hops}`; const c = cache.get(ck);
    if (c && Date.now() - c.at < ttl) return { ...c.v, cached: true };
    const deadline = Date.now() + timeoutMs;
    const chain: any[] = []; let cur = address; let stoppedAt: string | null = null; let complete = true;
    for (let h = 0; h < hops; h++) {
      let r;
      try { r = await oneHop(cur, deadline); }
      catch (e: any) { if (!chain.length) throw e instanceof RpcError ? e : new RpcError(-32603, String(e?.message ?? e)); stoppedAt = "upstreamError"; complete = false; break; }
      if (!r.complete) complete = false;
      if (!r.hit) { stoppedAt = r.complete ? "noIncomingSol" : "truncated"; break; }
      const l = labels.get(r.hit.from);
      chain.push({
        hop: h + 1, address: cur, funder: r.hit.from, funderName: l?.name ?? null, funderType: l?.type ?? null,
        mint: SOL_MINT, symbol: "SOL", amount: r.hit.lamports / 1e9, amountRaw: String(r.hit.lamports), decimals: 9,
        signature: r.hit.signature, slot: r.hit.slot, timestamp: r.hit.timestamp, date: r.hit.timestamp ? new Date(r.hit.timestamp * 1000).toISOString() : null,
        explorerUrl: `https://solscan.io/tx/${r.hit.signature}`, complete: r.complete, signaturesScanned: r.scanned,
      });
      if (l) { stoppedAt = `labeled:${l.type}`; break; }
      if (Date.now() > deadline) { stoppedAt = "timeout"; complete = false; break; }
      cur = r.hit.from;
    }
    const first = chain[0] ?? null;
    const v = { address, ...(first ? { funder: first.funder, funderName: first.funderName, funderType: first.funderType, mint: first.mint, symbol: first.symbol, amount: first.amount, amountRaw: first.amountRaw, decimals: 9, signature: first.signature, timestamp: first.timestamp, date: first.date, slot: first.slot, explorerUrl: first.explorerUrl } : { funder: null }),
      hops: chain, complete, truncated: !complete, stoppedAt };
    cache.set(ck, { at: Date.now(), v });
    if (cache.size > 50_000) cache.delete(cache.keys().next().value!);
    persist();
    return v;
  }
  return { fundedBy, reloadLabels: () => { labels = loadLabels(labelsFile); return labels.size; }, get labels() { return labels; } };
}
