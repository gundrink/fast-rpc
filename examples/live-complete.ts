// Read-only mainnet check of the new parsers (few calls): Jupiter swap -> enhanced SWAP with token in/out;
// recent Bubblegum tx -> noop events (real seq) + syncTree; off-chain JSON fetch for its metadata uri. No transactions sent.
import { parseEnhanced, CnftIndexer, BUBBLEGUM, parseNoopEvent, NOOP_PROGRAMS, b58decode, txAccountKeys, JsonFetcher, JUPITER } from "../src/index.js";
const URL_ = process.env.SOLANA_RPC_URL ?? "https://api.mainnet-beta.solana.com";
const rpc = async (method: string, params: any[]) => {
  const r: any = await (await fetch(URL_, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) })).json();
  if (r.error) throw new Error(`${method}: ${r.error.message}`); return r.result;
};
const getTx = (s: string) => rpc("getTransaction", [s, { encoding: "json", maxSupportedTransactionVersion: 1, commitment: "confirmed" }]).catch((e) => { console.log("skip", e.message.slice(0, 80)); return null; });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// 1) Jupiter
const js: any[] = await rpc("getSignaturesForAddress", [JUPITER, { limit: 8, commitment: "confirmed" }]);
for (const s of js.filter((x) => !x.err).slice(0, 3)) {
  await sleep(400); const t = await getTx(s.signature); if (!t) continue;
  const e: any = parseEnhanced(t);
  console.log("JUP", s.signature.slice(0, 16), e.type, e.source, "hops:", e.events.swap?.innerSwaps?.map((h: any) => h.programInfo.source).join(">"),
    "in:", JSON.stringify(e.events.swap?.tokenIn ?? e.events.swap?.nativeInput), "out:", JSON.stringify(e.events.swap?.tokenOut ?? e.events.swap?.nativeOutput));
  if (e.type === "SWAP") break;
}
// 2) Bubblegum
await sleep(400);
const bs: any[] = await rpc("getSignaturesForAddress", [BUBBLEGUM, { limit: 10, commitment: "confirmed" }]);
for (const s of bs.filter((x) => !x.err).slice(0, 4)) {
  await sleep(400); const t = await getTx(s.signature); if (!t) continue;
  const keys = txAccountKeys(t);
  const evs = (t.meta?.innerInstructions ?? []).flatMap((g: any) => g.instructions).filter((ix: any) => NOOP_PROGRAMS.has(keys[ix.programIdIndex])).map((ix: any) => parseNoopEvent(b58decode(ix.data))).filter(Boolean);
  const cl = evs.find((e: any) => e.changelog)?.changelog; if (!cl) continue;
  const leaf = evs.find((e: any) => e.leaf)?.leaf;
  const e: any = parseEnhanced(t);
  console.log("BGUM", s.signature.slice(0, 16), e.type, "tree", cl.tree, "seq", cl.seq, "index", cl.index, "depth", cl.path.length - 1, "leaf:", leaf ? `v${leaf.version} id=${leaf.id} nonce=${leaf.nonce} owner=${leaf.owner}` : "none");
  const idx = new CnftIndexer({ trees: [{ address: cl.tree, depth: cl.path.length - 1 }] }); idx.apply(t);
  const a = leaf && idx.assets.get(leaf.id);
  console.log("  indexer: events", idx.stats.events, "asset", a ? `${a.metadata.name || "(no md in ix)"} uri=${a.metadata.uri}` : "-");
  await sleep(400);
  const sync = await idx.syncTree(rpc, cl.tree);
  console.log("  syncTree:", JSON.stringify({ depth: sync.depth, maxBufferSize: sync.maxBufferSize, chainSeq: sync.chainSeq, eventSeq: cl.seq, numMinted: sync.numMinted }));
  if (a?.metadata.uri) { const j = await new JsonFetcher().get(a.metadata.uri); console.log("  json_uri:", j ? `name=${j.name} image=${String(j.image).slice(0, 60)}` : "fetch failed"); }
  break;
}
