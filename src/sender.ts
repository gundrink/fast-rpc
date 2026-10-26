// Multi-path sender: every rebroadcast round sends the SAME user-signed tx via all RPC send upstreams,
// Jito (optional) and direct TPU (optional, when a transport exists), then polls status until the target
// commitment, blockhash expiry or timeout. No key custody: only forwards already-signed transactions.
import { RpcError, INVALID_PARAMS } from "./errors.js";
import { decodeTransaction, b58encode, b58decode } from "./codec.js";
import type { ChainState } from "./chain.js";

type Up = (m: string, p: any[], ttl?: number) => Promise<any>;
export interface SenderDeps {
  read: Up; senders: Up[]; chain: ChainState;
  jitoSend?: (tx: string, cfg: any) => Promise<any>; jitoCheck?: (tx: string, cfg: any) => Promise<any>;
  tpu?: { enabled: boolean; send(wire: Buffer): Promise<any> };
}
const RANK: Record<string, number> = { processed: 0, confirmed: 1, finalized: 2 };

export function createSender(d: SenderDeps) {
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  /** sendTransactionWithRetry(tx, {encoding, skipPreflight, jito, tpu, commitment, rebroadcastMs, timeoutMs}) */
  async function sendWithRetry(tx: string, cfg: any = {}) {
    if (typeof tx !== "string") throw INVALID_PARAMS("sendTransactionWithRetry: params[0] must be a serialized transaction string");
    const enc = cfg.encoding ?? "base64";
    let dec; try { dec = decodeTransaction(tx, enc); } catch (e: any) { throw INVALID_PARAMS(`sendTransactionWithRetry: failed to deserialize transaction: ${e.message}`); }
    const sig = dec.signatures[0];
    if (!sig || /^1+$/.test(sig)) throw INVALID_PARAMS("sendTransactionWithRetry: transaction is not signed");
    const wire = enc === "base64" ? Buffer.from(tx, "base64") : b58decode(tx);
    const raw = dec.recentBlockhash ?? await rawBlockhash(wire);
    // Alpenglow: confirmed == finalized (~150ms), so default target is chain.defaultCommitment
    const target = d.chain.mapCommitment(cfg.commitment ?? d.chain.defaultCommitment);
    const every = cfg.rebroadcastMs ?? Math.max(100, Math.round(d.chain.slots(2)));
    const deadline = Date.now() + (cfg.timeoutMs ?? 60_000);
    const useJito = !!cfg.jito && !!d.jitoSend; const useTpu = cfg.tpu !== false && !!d.tpu?.enabled;
    if (useJito) await d.jitoCheck?.(tx, { encoding: enc });
    const sendCfg = { encoding: enc, skipPreflight: cfg.skipPreflight ?? true, maxRetries: 0, preflightCommitment: cfg.preflightCommitment };
    const paths = { rpc: 0, jito: 0, tpu: 0, errors: [] as string[] };
    let rounds = 0;
    // first round: preflight errors from upstream are real answers -> surface them
    while (true) {
      rounds++;
      const jobs: Promise<any>[] = d.senders.map((s) => s("sendTransaction", [tx, sendCfg], 0).then(() => paths.rpc++));
      if (useJito) jobs.push(d.jitoSend!(tx, { encoding: enc }).then(() => paths.jito++));
      if (useTpu) jobs.push(d.tpu!.send(wire).then(() => paths.tpu++));
      const r = await Promise.allSettled(jobs);
      const errs = r.filter((x): x is PromiseRejectedResult => x.status === "rejected").map((x) => x.reason);
      if (rounds === 1 && errs.length === r.length) {
        const pre = errs.find((e) => e instanceof RpcError && e.code === -32002); if (pre) throw pre;
      }
      for (const e of errs) if (paths.errors.length < 10) paths.errors.push(String(e?.message ?? e));
      const st = (await d.read("getSignatureStatuses", [[sig], { searchTransactionHistory: false }], 0).catch(() => null))?.value?.[0];
      if (st?.err) return { signature: sig, confirmationStatus: st.confirmationStatus, err: st.err, slot: st.slot, rounds, paths };
      if (st && RANK[st.confirmationStatus] >= RANK[target]) return { signature: sig, confirmationStatus: st.confirmationStatus, err: null, slot: st.slot, rounds, paths };
      const valid = await d.read("isBlockhashValid", [raw, { commitment: "processed" }], 0).then((v) => v?.value ?? v, () => true);
      if (valid === false) throw new RpcError(-32003, `sendTransactionWithRetry: blockhash expired before ${target} (signature ${sig})`, { signature: sig, rounds, paths });
      if (Date.now() > deadline) throw new RpcError(-32603, `sendTransactionWithRetry: timeout waiting for ${target}`, { signature: sig, rounds, paths });
      await sleep(every);
    }
  }
  return { sendWithRetry };
}
/** recentBlockhash from the wire message (after signatures + header + account keys). */
async function rawBlockhash(b: Buffer) {
  const cu = (o: number): [number, number] => { let v = 0, s = 0, i = o; for (;;) { const x = b[i++]; v |= (x & 0x7f) << s; if (!(x & 0x80)) break; s += 7; } return [v, i]; };
  let [n, o] = cu(0); o += n * 64; if (b[o] & 0x80) o++; o += 3;
  let k; [k, o] = cu(o); o += k * 32;
  return b58encode(b.subarray(o, o + 32));
}

/** Fee helper: priority fee (microlamports/CU) from our getPriorityFeeEstimate + Jito tip suggestion from the public tip floor. */
export async function smartFees(estimate: (req: any) => Promise<any>, req: any = {}, tipFloorUrl = process.env.JITO_TIP_FLOOR_URL ?? "https://bundles.jito.wtf/api/v1/bundles/tip_floor", fetcher: typeof fetch = fetch) {
  const fee = await estimate({ ...req, options: { ...(req.options ?? {}), includeAllPriorityFeeLevels: true } });
  let tip: any = null;
  if (req.jitoTip !== false) try {
    const r = await fetcher(tipFloorUrl, { signal: AbortSignal.timeout(3000) }); const j: any = await r.json(); const t = Array.isArray(j) ? j[0] : j;
    const pct = String(req.tipPercentile ?? "50");
    const sol = Number(t?.[`landed_tips_${pct}th_percentile`] ?? t?.landed_tips_50th_percentile);
    if (Number.isFinite(sol)) tip = { lamports: Math.max(Number(process.env.JITO_MIN_TIP_LAMPORTS ?? 1000), Math.ceil(sol * 1e9)), percentile: pct, source: tipFloorUrl };
  } catch { tip = { lamports: Number(process.env.JITO_MIN_TIP_LAMPORTS ?? 1000), percentile: null, source: "fallback:JITO_MIN_TIP_LAMPORTS" }; }
  const level = String(req.priorityLevel ?? "high");
  const lv = fee.priorityFeeLevels ?? {}; const key = Object.keys(lv).find((k) => k.toLowerCase() === level.toLowerCase()) ?? "high";
  return { computeUnitPrice: Math.max(lv[key] ?? 0, req.minMicroLamports ?? 0), priorityFeeLevels: lv, jitoTip: tip };
}
