// LaserStream-style demo: slot stream, account stream, shared fan-out, reconnect + replay.
import { RpcServer, RpcClient, StreamHub, solanaApi } from "../src/index.js";
const port = Number(process.env.PORT ?? 7071);
const SECS = Number(process.env.DEMO_SECS ?? 4);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const t0 = Date.now(); const ts = () => `+${((Date.now() - t0) / 1000).toFixed(2)}s`;

const hub = new StreamHub({ log: (...a) => console.log(ts(), ...a) });
const server = new RpcServer().attachStreams(hub);
for (const [k, fn] of Object.entries(solanaApi)) server.register(k, fn);
await server.listen(port);

const a = await RpcClient.connect(port, "127.0.0.1", { log: (...m) => console.log(ts(), ...m) });
const b = await RpcClient.connect(port);

console.log(`\n== 1) slot stream (client A, iterator) + client B on same topic (fan-out, 1 upstream sub)`);
let bCount = 0;
const unsubB = await b.subscribe("slot", {}, () => bCount++);
const it = a.stream<{ slot: number; parent: number; root: number }>("slot", {});
const deadline = Date.now() + SECS * 1000;
for await (const m of it) {
  console.log(ts(), `A seq=${m.seq} slot=${m.data.slot} parent=${m.data.parent} root=${m.data.root}`);
  if (Date.now() > deadline) break;
}
console.log(ts(), `B received ${bCount} slot msgs via same topic; hub:`, JSON.stringify(hub.stats()));

console.log(`\n== 2) account stream: SysvarClock (changes every slot)`);
const CLOCK = "SysvarC1ock11111111111111111111111111111111";
let n = 0;
const unsubAcc = await a.subscribe("account", { pubkey: CLOCK, encoding: "base64" }, (m) => {
  if (n++ < 4) console.log(ts(), `account seq=${m.seq} slot=${m.slot} lamports=${m.data.lamports} owner=${m.data.owner} dataLen=${Buffer.from(m.data.data[0], "base64").length}`);
});
await sleep(2500);
await unsubAcc();
console.log(ts(), `account msgs: ${n}`);

console.log(`\n== 3) reconnect + replay: client C subscribes to slots, socket killed, resumes from last seq`);
const c = await RpcClient.connect(port, "127.0.0.1", { log: (...m) => console.log(ts(), ...m) });
const seqs: number[] = []; const slots: number[] = [];
await c.subscribe<{ slot: number }>("slot", {}, (m) => { seqs.push(m.seq); slots.push(m.data.slot); });
await sleep(1500);
const before = seqs.at(-1)!;
console.log(ts(), `C last seq before drop = ${before} (slot ${slots.at(-1)}); killing socket…`);
c.simulateDrop();
await sleep(2000); // slots keep flowing into server ring buffer while C is offline/reconnecting
const after = seqs.filter((s) => s > before);
await sleep(1000);
const all = seqs.filter((s) => s > before);
const gaps = seqs.slice(1).filter((s, i) => s !== seqs[i] + 1).length;
console.log(ts(), `C reconnects=${c.reconnects}; msgs after drop=${all.length} (first ${all[0]}…${all.at(-1)}), seq gaps=${gaps}, dups=${seqs.length - new Set(seqs).size}`);
console.log(ts(), `C seq timeline: ${seqs.join(",")}`);

console.log(`\n== 4) late joiner D with fromSlot replay from ring buffer`);
const d = await RpcClient.connect(port);
const from = slots[0];
const dSlots: number[] = [];
await d.subscribe<{ slot: number }>("slot", {}, (m) => dSlots.push(m.data.slot), { fromSlot: from });
console.log(ts(), `D asked fromSlot=${from}, got ${dSlots.length} replayed immediately: ${dSlots.slice(0, 6).join(",")}…${dSlots.at(-1)}`);

await unsubB();
for (const x of [a, b, c, d]) x.close();
await server.close(); hub.close();
setTimeout(() => process.exit(0), 200);
void after;
