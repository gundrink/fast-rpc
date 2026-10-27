import { RpcServer, type ServerOptions } from "./server.js";
import { StreamHub, type HubOptions } from "./stream.js";
import { createSolanaApi, createUpstream, upstreamUrl, type SolanaApiOptions } from "./solana.js";
import { WebhookManager, type WebhookOptions } from "./webhooks.js";
import { ChainState, type ChainOptions } from "./chain.js";
import { PumpEngine } from "./pump.js";
import { CnftIndexer, type IndexerOptions } from "./bubblegum.js";

export interface FastRpcOptions {
  server?: ServerOptions; hub?: HubOptions; api?: SolanaApiOptions; webhooks?: WebhookOptions | false;
  /** compressed-NFT indexer (CNFT_TREES); backfill from creation when CNFT_BACKFILL=1 */
  cnft?: IndexerOptions & { backfill?: boolean };
  chain?: ChainOptions & {
    /** call getAgGenesisCert at start + every redetectMs (default true; CHAIN_DETECT=0 disables) */
    detect?: boolean; redetectMs?: number;
    /** keep an internal slotSubscribe open so the slot rate is always observed (default CHAIN_TRACK_SLOTS=1) */
    trackSlots?: boolean;
  };
}

/** Wire everything: binary RPC + HTTP JSON-RPC + Solana-compatible WS (+transactionSubscribe) + SSE + webhooks on one port. */
export function createFastRpc(opts: FastRpcOptions = {}) {
  const co = opts.chain ?? {};
  const chain = new ChainState(co);
  const hub = new StreamHub({ ...opts.hub, chain });
  const cnft = new CnftIndexer(opts.cnft);
  const api = createSolanaApi({ ...opts.api, chain, cnft }) as Record<string, (...a: any[]) => any>;
  const getTx = (sig: string, cfg: any) => api.getTransaction(sig, cfg);
  const rpc = (m: string, p: any[]) => api[m](...p);
  const webhooks = opts.webhooks === false ? undefined : new WebhookManager(hub, getTx, { rpc, ...opts.webhooks });
  if (cnft.enabled) {
    cnft.start(hub, getTx);
    if (opts.cnft?.backfill ?? process.env.CNFT_BACKFILL === "1")
      for (const [tree, t] of cnft.trees) if (t.seq === 0) void cnft.backfill(rpc, tree).catch((e) => opts.cnft?.log?.("[cnft] backfill failed", tree, e?.message));
  }
  const server = new RpcServer(opts.server).attachStreams(hub, getTx).registerAll(api).attachChain(chain);
  const pump = new PumpEngine(hub, rpc as any);
  server.attachPump(pump);
  if (webhooks) server.attachWebhooks(webhooks);
  const timers: NodeJS.Timeout[] = [];
  const detect = co.detect ?? process.env.CHAIN_DETECT !== "0";
  const detectUp = createUpstream(opts.api?.url ?? upstreamUrl, opts.api?.timeoutMs);
  const ready = detect ? chain.detect(detectUp) : Promise.resolve(chain.consensus);
  if (detect) { const t = setInterval(() => void chain.detect(detectUp), co.redetectMs ?? 600_000); t.unref(); timers.push(t); }
  const track = (co.trackSlots ?? process.env.CHAIN_TRACK_SLOTS === "1") ? hub.subscribe("slot", {}, { deliver: () => {} }) : undefined;
  return {
    server, hub, api, pump, webhooks, chain, cnft, ready,
    close: async () => { for (const t of timers) clearInterval(t); track?.detach(); webhooks?.close(); cnft.stop(); await server.close(); hub.close(); },
  };
}
