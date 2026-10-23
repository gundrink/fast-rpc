import { RpcServer, StreamHub, solanaApi, upstreamUrl, wsUrl } from "../src/index.js";
const s = new RpcServer().attachStreams(new StreamHub({ log: console.log }));
for (const [k, fn] of Object.entries(solanaApi)) s.register(k, fn);
const port = Number(process.env.PORT ?? 7070);
await s.listen(port, process.env.HOST ?? "127.0.0.1");
console.log(`fast-rpc on :${port} (binary + HTTP JSON-RPC + SSE /stream) -> ${upstreamUrl} | ${wsUrl}`);
