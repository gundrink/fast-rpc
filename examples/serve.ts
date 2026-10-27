import fs from "node:fs";
import { createFastRpc, upstreamUrl, wsUrl, parseWalletsFile } from "../src/index.js";
// default bind 127.0.0.1 (internal). On Railway (RAILWAY_ENVIRONMENT set) default 0.0.0.0 so the proxy can reach it.
const host = process.env.HOST ?? (process.env.RAILWAY_ENVIRONMENT ? "0.0.0.0" : "127.0.0.1");
const open = !process.env.API_KEYS && !process.env.API_KEYS_FILE;
if (host !== "127.0.0.1" && host !== "localhost" && open && process.env.ALLOW_PUBLIC_NO_AUTH !== "1") {
  console.error(`refusing to bind ${host} with auth OFF: set API_KEYS / API_KEYS_FILE, or ALLOW_PUBLIC_NO_AUTH=1 (private network only)`);
  process.exit(1);
}
// pump analytics (wallet ranking + launch risk): on unless PUMP_ANALYTICS=0; tagged wallets from PUMP_WALLETS_FILE ("<wallet> <label...>" per line)
const wf = process.env.PUMP_WALLETS_FILE ?? "/workspace/sapi/wallets.txt";
const wallets = fs.existsSync(wf) ? parseWalletsFile(fs.readFileSync(wf, "utf8")) : new Map<string, string>();
const pumpAnalytics = process.env.PUMP_ANALYTICS === "0" ? false as const : { dataFile: process.env.PUMP_DATA_FILE ?? "data/pump-analytics.json", wallets, backfill: process.env.PUMP_BACKFILL !== "0" };
const { server, close } = createFastRpc({ hub: { log: console.log }, server: { log: console.log }, pumpAnalytics });
const port = await server.listen(Number(process.env.PORT ?? 7070), host);
console.log(`fast-rpc on ${host}:${port} (binary + HTTP JSON-RPC + Solana WS + SSE /stream) -> ${process.env.SOLANA_RPC_URLS ?? upstreamUrl} | ${wsUrl} | auth ${open ? "OFF" : "ON"}`);
for (const s of ["SIGTERM", "SIGINT"] as const) process.on(s, () => void close().finally(() => process.exit(0)));
