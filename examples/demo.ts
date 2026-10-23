import { RpcServer, RpcClient, solanaApi, type SolanaApi } from "../src/index.js";
const port = Number(process.env.PORT ?? 7070);
let server: RpcServer | undefined;
if (!process.env.NO_SERVER) {
  server = new RpcServer();
  for (const [k, fn] of Object.entries(solanaApi)) server.register(k, fn);
  await server.listen(port);
}
const client = await RpcClient.connect(port);
const remote = client.proxy<SolanaApi>();
const timed = async (label: string, f: () => Promise<any>) => {
  const t = performance.now(); const r = await f();
  console.log(`${label.padEnd(34)} ${(performance.now() - t).toFixed(1).padStart(7)}ms ->`, JSON.stringify(r));
};
const PK = "SysvarRent111111111111111111111111111111111"; // well-known public sysvar account
await timed("getHealth()", () => remote.getHealth());
await timed("getSlot()", () => remote.getSlot());
await timed("getSlot() [cached]", () => remote.getSlot());
await timed(`getBalance(${PK.slice(0, 10)}…)`, () => remote.getBalance(PK));
await timed("getLatestBlockhash()", () => remote.getLatestBlockhash());
try { await client.call("sendTransaction", "x"); } catch (e: any) { console.log("write method blocked:", e.message); }
client.close(); server?.close();
