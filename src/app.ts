import { RpcServer, type ServerOptions } from "./server.js";
import { StreamHub, type HubOptions } from "./stream.js";
import { createSolanaApi, type SolanaApiOptions } from "./solana.js";
import { WebhookManager, type WebhookOptions } from "./webhooks.js";

/** Wire everything: binary RPC + HTTP JSON-RPC + Solana-compatible WS (+transactionSubscribe) + SSE + webhooks on one port. */
export function createFastRpc(opts: { server?: ServerOptions; hub?: HubOptions; api?: SolanaApiOptions; webhooks?: WebhookOptions | false } = {}) {
  const hub = new StreamHub(opts.hub);
  const api = createSolanaApi(opts.api) as Record<string, (...a: any[]) => any>;
  const getTx = (sig: string, cfg: any) => api.getTransaction(sig, cfg);
  const webhooks = opts.webhooks === false ? undefined : new WebhookManager(hub, getTx, opts.webhooks);
  const server = new RpcServer(opts.server).attachStreams(hub, getTx).registerAll(api);
  if (webhooks) server.attachWebhooks(webhooks);
  return { server, hub, api, webhooks, close: async () => { webhooks?.close(); await server.close(); hub.close(); } };
}
