import { createFastRpc, upstreamUrl, wsUrl } from "../src/index.js";
const { server } = createFastRpc({ hub: { log: console.log }, server: { log: console.log } });
const port = await server.listen(Number(process.env.PORT ?? 7070), process.env.HOST ?? "127.0.0.1");
console.log(`fast-rpc on :${port} (binary + HTTP JSON-RPC + Solana WS + SSE /stream) -> ${upstreamUrl} | ${wsUrl}`);
