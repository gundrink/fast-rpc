// Jito block-engine relay (no key custody: only forwards user-signed txs/bundles) + tip validation.
import { RpcError, INVALID_PARAMS } from "./errors.js";
import { createUpstream } from "./solana.js";
import { decodeTransaction, type DecodedTx } from "./codec.js";

export const SYSTEM_PROGRAM = "11111111111111111111111111111111";
export const jitoUrl = (process.env.JITO_URL ?? "https://mainnet.block-engine.jito.wtf").replace(/\/+$/, "");
export interface JitoOptions { url?: string; timeoutMs?: number; minTipLamports?: number; tipCacheMs?: number; uuid?: string }

/** Lamports transferred by top-level System transfers to any of `tipAccounts` (static keys only). */
export function tipLamports(tx: DecodedTx, tipAccounts: Set<string>): number {
  let total = 0;
  for (const ix of tx.instructions) {
    if (tx.accountKeys[ix.programIdIndex] !== SYSTEM_PROGRAM || ix.data.length < 12 || ix.data.readUInt32LE(0) !== 2) continue;
    const to = tx.accountKeys[ix.accounts[1]];
    if (to && tipAccounts.has(to)) total += Number(ix.data.readBigUInt64LE(4));
  }
  return total;
}

export function createJito(opts: JitoOptions = {}) {
  const base = (opts.url ?? jitoUrl).replace(/\/+$/, "");
  const q = opts.uuid ?? process.env.JITO_UUID ? `?uuid=${encodeURIComponent((opts.uuid ?? process.env.JITO_UUID)!)}` : "";
  const bundles = createUpstream(`${base}/api/v1/bundles${q}`, opts.timeoutMs);
  const txs = createUpstream(`${base}/api/v1/transactions${q}`, opts.timeoutMs);
  const minTip = opts.minTipLamports ?? Number(process.env.JITO_MIN_TIP_LAMPORTS ?? 1000);
  const tipCacheMs = opts.tipCacheMs ?? 600_000;

  const getTipAccounts = (): Promise<string[]> => bundles("getTipAccounts", [], tipCacheMs);
  const decode = (m: string, t: any, enc: string) => {
    if (typeof t !== "string") throw INVALID_PARAMS(`${m}: transactions must be serialized strings`);
    let d: DecodedTx; try { d = decodeTransaction(t, enc as any); } catch (e: any) { throw INVALID_PARAMS(`${m}: failed to deserialize transaction: ${e.message}`); }
    if (d.numRequiredSignatures < 1 || d.signatures.every((s) => s === "1111111111111111111111111111111111111111111111111111111111111111")) throw INVALID_PARAMS(`${m}: transaction is not signed`);
    return d;
  };
  const requireTip = async (m: string, decoded: DecodedTx[]) => {
    const tips = new Set(await getTipAccounts());
    const total = decoded.reduce((s, d) => s + tipLamports(d, tips), 0);
    if (total < minTip) throw INVALID_PARAMS(`${m}: missing Jito tip (System transfer >= ${minTip} lamports to a getTipAccounts address, static key)`);
    return total;
  };

  return {
    getTipAccounts: () => getTipAccounts(),
    sendBundle: async (list: any, cfg: any = {}) => {
      if (!Array.isArray(list) || list.length < 1 || list.length > 5) throw INVALID_PARAMS("sendBundle: params[0] must be 1..5 signed transactions");
      const enc = cfg?.encoding ?? "base58";
      if (enc !== "base58" && enc !== "base64") throw INVALID_PARAMS("sendBundle: encoding must be base58 or base64");
      await requireTip("sendBundle", list.map((t) => decode("sendBundle", t, enc)));
      return bundles("sendBundle", cfg?.encoding ? [list, { encoding: enc }] : [list], 0);
    },
    getBundleStatuses: (ids: any) => {
      if (!Array.isArray(ids) || !ids.length || ids.length > 5 || !ids.every((x) => typeof x === "string")) throw INVALID_PARAMS("getBundleStatuses: params[0] must be 1..5 bundle ids");
      return bundles("getBundleStatuses", [ids], 0);
    },
    getInflightBundleStatuses: (ids: any) => {
      if (!Array.isArray(ids) || !ids.length || ids.length > 5 || !ids.every((x) => typeof x === "string")) throw INVALID_PARAMS("getInflightBundleStatuses: params[0] must be 1..5 bundle ids");
      return bundles("getInflightBundleStatuses", [ids], 0);
    },
    /** Sender-style: validate tip, then send to Jito /transactions; caller races it with RPC. */
    sendTransaction: async (tx: string, cfg: any = {}) => {
      const enc = cfg?.encoding ?? "base58";
      await requireTip("sendTransaction", [decode("sendTransaction", tx, enc)]);
      return txs("sendTransaction", [tx, { encoding: enc }], 0);
    },
    /** validate a single tx's tip without sending */
    checkTip: async (tx: string, cfg: any = {}) => requireTip("sendTransaction", [decode("sendTransaction", tx, cfg?.encoding ?? "base58")]),
    minTip,
  };
}
export type Jito = ReturnType<typeof createJito>;
