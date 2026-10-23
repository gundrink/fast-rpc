import { RpcServer, type ServerOptions } from "./server.js";
import { StreamHub, type HubOptions } from "./stream.js";
import { createSolanaApi, type SolanaApiOptions } from "./solana.js";

/** Wire everything: binary RPC + HTTP JSON-RPC + Solana-compatible WS + SSE on one port. */
export function createFastRpc(opts: { server?: ServerOptions; hub?: HubOptions; api?: SolanaApiOptions } = {}) {
  const hub = new StreamHub(opts.hub);
  const server = new RpcServer(opts.server).attachStreams(hub).registerAll(createSolanaApi(opts.api));
  return { server, hub, close: async () => { await server.close(); hub.close(); } };
}
