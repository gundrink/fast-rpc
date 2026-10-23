// Read-only Solana JSON-RPC upstream with timeout + short TTL cache + in-flight dedupe.
const URL_ = process.env.SOLANA_RPC_URL ?? "https://api.mainnet-beta.solana.com";
const TIMEOUT_MS = Number(process.env.SOLANA_TIMEOUT_MS ?? 5000);
const cache = new Map<string, { exp: number; p: Promise<any> }>();
let seq = 0;

export async function upstream(method: string, params: any[] = [], ttlMs = 0): Promise<any> {
  const key = method + JSON.stringify(params);
  const hit = cache.get(key);
  if (hit && hit.exp > Date.now()) return hit.p;
  const p = (async () => {
    const res = await fetch(URL_, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: ++seq, method, params }),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (!res.ok) throw new Error(`upstream HTTP ${res.status}`);
    const j: any = await res.json();
    if (j.error) throw Object.assign(new Error(j.error.message), { code: j.error.code });
    return j.result;
  })();
  if (ttlMs > 0) { cache.set(key, { exp: Date.now() + ttlMs, p }); p.catch(() => cache.delete(key)); }
  return p;
}

export const solanaApi = {
  getHealth: () => upstream("getHealth", [], 1000),
  getSlot: (cfg?: object) => upstream("getSlot", cfg ? [cfg] : [], 400),
  getBlockHeight: () => upstream("getBlockHeight", [], 400),
  getBalance: (pubkey: string) => upstream("getBalance", [pubkey], 2000),
  getLatestBlockhash: () => upstream("getLatestBlockhash", [], 400),
  getVersion: () => upstream("getVersion", [], 60_000),
};
export type SolanaApi = typeof solanaApi;
export const upstreamUrl = URL_;
