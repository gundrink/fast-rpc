// Best-effort Bubblegum (compressed NFT) indexer for configured trees (CNFT_TREES="tree:depth,tree2:depth").
// Parses mint_v1 / mint_to_collection_v1 / transfer / burn instruction data (top-level + inner/CPI) from
// confirmed transactions and maintains leaves + a full concurrent-merkle-tree replica to serve proofs.
// Limits: must see every change since tree creation (backfill via getSignaturesForAddress or live from empty tree);
// delegate/redeem/decompress/verify_creator/set_collection/update_metadata/V2 instructions are not modelled.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { keccak_256 } from "@noble/hashes/sha3.js";
import { b58decode, b58encode, findProgramAddress, isPubkey } from "./codec.js";
import { TxWatcher, type GetTx, txAccountKeys } from "./txstream.js";
import type { StreamHub } from "./stream.js";

export const BUBBLEGUM = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY";
const disc = (n: string) => createHash("sha256").update(`global:${n}`).digest().subarray(0, 8);
export const DISC = { mintV1: disc("mint_v1"), mintToCollectionV1: disc("mint_to_collection_v1"), transfer: disc("transfer"), burn: disc("burn") };
const keccak = (...parts: Uint8Array[]) => Buffer.from(keccak_256(Buffer.concat(parts)));
const ZERO = Buffer.alloc(32);

export class MerkleTree {
  private levels: Map<number, Buffer>[]; private zeros: Buffer[] = [ZERO];
  constructor(readonly depth: number) {
    this.levels = Array.from({ length: depth + 1 }, () => new Map());
    for (let i = 1; i <= depth; i++) this.zeros.push(keccak(this.zeros[i - 1], this.zeros[i - 1]));
  }
  node(level: number, idx: number) { return this.levels[level].get(idx) ?? this.zeros[level]; }
  leaf(idx: number) { return this.node(0, idx); }
  set(idx: number, leaf: Buffer) {
    if (idx < 0 || idx >= 2 ** this.depth) throw new Error("leaf index out of range");
    let h = leaf, i = idx;
    for (let l = 0; l <= this.depth; l++) {
      if (h.equals(this.zeros[l])) this.levels[l].delete(i); else this.levels[l].set(i, h);
      if (l === this.depth) break;
      const sib = this.node(l, i ^ 1);
      h = i & 1 ? keccak(sib, h) : keccak(h, sib); i >>= 1;
    }
  }
  root() { return this.node(this.depth, 0); }
  proof(idx: number) { const out: Buffer[] = []; let i = idx; for (let l = 0; l < this.depth; l++) { out.push(this.node(l, i ^ 1)); i >>= 1; } return out; }
  static verify(leaf: Buffer, idx: number, proof: Buffer[], root: Buffer) {
    let h = leaf, i = idx; for (const s of proof) { h = i & 1 ? keccak(s, h) : keccak(h, s); i >>= 1; } return h.equals(root);
  }
}

export interface MetadataArgs { name: string; symbol: string; uri: string; sellerFeeBasisPoints: number; primarySaleHappened: boolean; isMutable: boolean; collection?: { verified: boolean; key: string }; creators: { address: string; verified: boolean; share: number }[]; tokenStandard?: number }

/** Borsh MetadataArgs; returns parsed args, byte length, and offset of collection.verified (if any). */
export function parseMetadataArgs(b: Buffer, o0 = 0) {
  let o = o0;
  const u8 = () => b[o++]; const u16 = () => { const v = b.readUInt16LE(o); o += 2; return v; };
  const u32 = () => { const v = b.readUInt32LE(o); o += 4; return v; };
  const str = () => { const n = u32(); const s = b.subarray(o, o + n).toString("utf8"); o += n; return s; };
  const pk = () => { const k = b58encode(b.subarray(o, o + 32)); o += 32; return k; };
  const m: MetadataArgs = { name: str(), symbol: str(), uri: str(), sellerFeeBasisPoints: u16(), primarySaleHappened: !!u8(), isMutable: !!u8(), creators: [] };
  if (u8()) o++;                       // edition_nonce
  if (u8()) m.tokenStandard = u8();
  let verifiedAt = -1;
  if (u8()) { verifiedAt = o; const verified = !!u8(); m.collection = { verified, key: pk() }; }
  if (u8()) o += 17;                   // uses
  o++;                                 // token_program_version
  const n = u32(); for (let i = 0; i < n; i++) m.creators.push({ address: pk(), verified: !!u8(), share: u8() });
  if (o > b.length) throw new Error("truncated MetadataArgs");
  return { args: m, len: o - o0, verifiedAt };
}

export function hashMetadata(raw: Buffer, sellerFee: number) { const f = Buffer.alloc(2); f.writeUInt16LE(sellerFee); return keccak(keccak(raw), f); }
export function hashCreators(c: MetadataArgs["creators"]) { return keccak(...c.map((x) => Buffer.concat([b58decode(x.address), Buffer.from([x.verified ? 1 : 0, x.share])]))); }
export function leafHash(id: string, owner: string, delegate: string, nonce: number, dataHash: Buffer, creatorHash: Buffer) {
  const n = Buffer.alloc(8); n.writeBigUInt64LE(BigInt(nonce));
  return keccak(Buffer.from([1]), b58decode(id), b58decode(owner), b58decode(delegate), n, dataHash, creatorHash);
}
export const assetId = (tree: string, nonce: number) => { const n = Buffer.alloc(8); n.writeBigUInt64LE(BigInt(nonce)); return findProgramAddress([Buffer.from("asset"), b58decode(tree), n], BUBBLEGUM)[0]; };

export interface CnftAsset { id: string; tree: string; nonce: number; owner: string; delegate: string; dataHash: string; creatorHash: string; metadata: MetadataArgs; burnt: boolean; seq: number; slot: number }
interface TreeState { depth: number; nextNonce: number; seq: number; lastSlot: number; tree: MerkleTree }

export function parseTrees(s = process.env.CNFT_TREES ?? ""): { address: string; depth: number }[] {
  return s.split(",").map((x) => x.trim()).filter(Boolean).map((x) => {
    const [address, d] = x.split(":"); const depth = Number(d ?? 14);
    if (!isPubkey(address) || !Number.isInteger(depth) || depth < 3 || depth > 30) throw new Error(`CNFT_TREES: bad entry ${x} (want tree:depth)`);
    return { address, depth };
  });
}

export interface IndexerOptions { trees?: { address: string; depth: number }[]; file?: string; log?: (...a: any[]) => void }

export class CnftIndexer {
  assets = new Map<string, CnftAsset>();
  trees = new Map<string, TreeState>();
  private seenSigs = new Set<string>();
  private watcher?: TxWatcher; private saveT?: NodeJS.Timeout;
  stats = { txs: 0, mints: 0, transfers: 0, burns: 0, unknownLeaf: 0 };
  constructor(private o: IndexerOptions = {}) {
    for (const t of o.trees ?? parseTrees()) this.trees.set(t.address, { depth: t.depth, nextNonce: 0, seq: 0, lastSlot: 0, tree: new MerkleTree(t.depth) });
    this.o.file ??= process.env.CNFT_INDEX_FILE;
    this.load();
  }
  get enabled() { return this.trees.size > 0; }
  has(id: string) { return this.assets.has(id); }

  /** Apply one getTransaction(json) result. Idempotent per signature. Process in chain order. */
  apply(r: any) {
    const sig = r?.transaction?.signatures?.[0];
    if (!r || r.meta?.err || (sig && this.seenSigs.has(sig))) return;
    if (sig) this.seenSigs.add(sig);
    const keys = txAccountKeys(r); const slot = r.slot ?? 0;
    const top = r.transaction?.message?.instructions ?? [];
    const inner = new Map<number, any[]>((r.meta?.innerInstructions ?? []).map((g: any) => [g.index, g.instructions]));
    const all: any[] = []; top.forEach((ix: any, i: number) => { all.push(ix); all.push(...(inner.get(i) ?? [])); });
    let touched = false;
    for (const ix of all) {
      if (keys[ix.programIdIndex] !== BUBBLEGUM) continue;
      let data: Buffer; try { data = b58decode(ix.data); } catch { continue; }
      const acc = (i: number) => keys[ix.accounts[i]];
      try { touched = this.applyIx(data, acc, slot) || touched; } catch (e: any) { this.o.log?.("[cnft] parse error", sig, e.message); }
    }
    if (touched) { this.stats.txs++; this.scheduleSave(); }
  }
  private applyIx(data: Buffer, acc: (i: number) => string, slot: number): boolean {
    const d = data.subarray(0, 8), body = data.subarray(8);
    const isMint = d.equals(DISC.mintV1), isColl = d.equals(DISC.mintToCollectionV1);
    if (isMint || isColl) {
      const tree = acc(3), t = this.trees.get(tree); if (!t) return false;
      const { args, len, verifiedAt } = parseMetadataArgs(body);
      let raw = Buffer.from(body.subarray(0, len));
      if (isColl) { if (verifiedAt < 0) return false; raw[verifiedAt] = 1; args.collection = { verified: true, key: acc(8) }; }
      const nonce = t.nextNonce++; const id = assetId(tree, nonce);
      const dataHash = hashMetadata(raw, args.sellerFeeBasisPoints), creatorHash = hashCreators(args.creators);
      const owner = acc(1), delegate = acc(2);
      t.tree.set(nonce, leafHash(id, owner, delegate, nonce, dataHash, creatorHash)); t.seq++; t.lastSlot = slot;
      this.assets.set(id, { id, tree, nonce, owner, delegate, dataHash: b58encode(dataHash), creatorHash: b58encode(creatorHash), metadata: args, burnt: false, seq: t.seq, slot });
      this.stats.mints++; return true;
    }
    const isXfer = d.equals(DISC.transfer), isBurn = d.equals(DISC.burn);
    if (!isXfer && !isBurn) return false;
    const tree = acc(isXfer ? 4 : 3), t = this.trees.get(tree); if (!t) return false;
    const nonce = Number(body.readBigUInt64LE(96)), index = body.readUInt32LE(104);
    const id = assetId(tree, nonce); const a = this.assets.get(id);
    if (!a) { this.stats.unknownLeaf++; return false; }
    t.seq++; t.lastSlot = slot; a.seq = t.seq; a.slot = slot;
    if (isXfer) {
      a.owner = acc(3); a.delegate = acc(3);
      t.tree.set(index, leafHash(id, a.owner, a.delegate, nonce, b58decode(a.dataHash), b58decode(a.creatorHash))); this.stats.transfers++;
    } else { a.burnt = true; t.tree.set(index, ZERO); this.stats.burns++; }
    return true;
  }

  /** Live: transactions mentioning the trees, fetched one at a time to keep chain order. */
  start(hub: StreamHub, getTx: GetTx) {
    if (!this.enabled || this.watcher) return;
    this.watcher = new TxWatcher(hub, getTx, { accountInclude: [...this.trees.keys()], failed: false }, { commitment: "confirmed" }, (e) => this.apply(e.tx), { concurrency: 1, maxQueue: 50_000, retries: 6 });
  }
  /** Backfill a tree from creation (oldest first). Only meaningful for small/young trees. */
  async backfill(rpc: (m: string, p: any[]) => Promise<any>, tree: string, max = Number(process.env.CNFT_BACKFILL_MAX ?? 5000)) {
    const sigs: string[] = []; let before: string | undefined;
    while (sigs.length < max) {
      const page: any[] = await rpc("getSignaturesForAddress", [tree, { limit: 1000, ...(before ? { before } : {}), commitment: "confirmed" }]);
      if (!page?.length) break;
      for (const s of page) if (!s.err) sigs.push(s.signature);
      before = page.at(-1).signature; if (page.length < 1000) break;
    }
    for (const s of sigs.reverse()) { const r = await rpc("getTransaction", [s, { encoding: "json", maxSupportedTransactionVersion: 0, commitment: "confirmed" }]); if (r) this.apply(r); }
    return sigs.length;
  }

  toDas(a: CnftAsset) {
    const t = this.trees.get(a.tree)!; const m = a.metadata;
    return {
      interface: "V1_NFT", id: a.id,
      content: { $schema: "https://schema.metaplex.com/nft1.0.json", json_uri: m.uri, files: [], metadata: { name: m.name, symbol: m.symbol }, links: {} },
      authorities: [],
      compression: { eligible: false, compressed: true, data_hash: a.dataHash, creator_hash: a.creatorHash, asset_hash: b58encode(a.burnt ? ZERO : t.tree.leaf(a.nonce)), tree: a.tree, seq: a.seq, leaf_id: a.nonce },
      grouping: m.collection?.verified ? [{ group_key: "collection", group_value: m.collection.key }] : [],
      royalty: { royalty_model: "creators", target: null, percent: m.sellerFeeBasisPoints / 10_000, basis_points: m.sellerFeeBasisPoints, primary_sale_happened: m.primarySaleHappened, locked: false },
      creators: m.creators,
      ownership: { frozen: false, delegated: a.delegate !== a.owner, delegate: a.delegate !== a.owner ? a.delegate : null, ownership_model: "single", owner: a.owner },
      supply: null, mutable: m.isMutable, burnt: a.burnt,
    };
  }
  proof(id: string) {
    const a = this.assets.get(id); if (!a) return null;
    const t = this.trees.get(a.tree)!;
    return { root: b58encode(t.tree.root()), proof: t.tree.proof(a.nonce).map(b58encode), node_index: 2 ** t.depth + a.nonce, leaf: b58encode(t.tree.leaf(a.nonce)), tree_id: a.tree };
  }
  list(pred: (a: CnftAsset) => boolean) { return [...this.assets.values()].filter(pred).sort((x, y) => x.id < y.id ? -1 : 1); }

  private scheduleSave() { if (!this.o.file || this.saveT) return; this.saveT = setTimeout(() => { this.saveT = undefined; this.save(); }, 500); this.saveT.unref(); }
  save() {
    if (!this.o.file) return;
    const trees = Object.fromEntries([...this.trees].map(([k, t]) => [k, { depth: t.depth, nextNonce: t.nextNonce, seq: t.seq, lastSlot: t.lastSlot }]));
    fs.mkdirSync(path.dirname(path.resolve(this.o.file)), { recursive: true });
    fs.writeFileSync(`${this.o.file}.tmp`, JSON.stringify({ trees, assets: [...this.assets.values()], sigs: [...this.seenSigs].slice(-50_000) }));
    fs.renameSync(`${this.o.file}.tmp`, this.o.file);
  }
  private load() {
    if (!this.o.file || !fs.existsSync(this.o.file)) return;
    try {
      const j = JSON.parse(fs.readFileSync(this.o.file, "utf8"));
      for (const [k, v] of Object.entries<any>(j.trees ?? {})) { const t = this.trees.get(k); if (t && t.depth === v.depth) Object.assign(t, { nextNonce: v.nextNonce, seq: v.seq, lastSlot: v.lastSlot }); }
      for (const a of j.assets ?? []) { const t = this.trees.get(a.tree); if (!t) continue; this.assets.set(a.id, a);
        t.tree.set(a.nonce, a.burnt ? ZERO : leafHash(a.id, a.owner, a.delegate, a.nonce, b58decode(a.dataHash), b58decode(a.creatorHash))); }
      for (const s of j.sigs ?? []) this.seenSigs.add(s);
    } catch (e: any) { this.o.log?.("[cnft] failed to load index", e.message); }
  }
  stop() { this.watcher?.stop(); this.watcher = undefined; if (this.saveT) { clearTimeout(this.saveT); this.saveT = undefined; this.save(); } }
}
