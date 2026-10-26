// Best-effort Bubblegum (compressed NFT) indexer for configured trees (CNFT_TREES="tree:depth,tree2:depth").
// Primary source: the account-compression noop CPI events (ChangeLog + Bubblegum LeafSchema V1/V2) which carry the
// real changelog seq, leaf index (non-sequential safe), owner/delegate/hashes for EVERY Bubblegum instruction
// (mint/transfer/burn/delegate/redeem/cancel_redeem/decompress/verify*/collection/update_metadata and all *_v2).
// Instruction data is used for metadata (mint, verify/unverify creator, (un)verify/set collection, update_metadata).
// Fallback (no noop events in tx, e.g. truncated inner ixs): instruction-only model of mint/transfer/burn/delegate.
// The tree account (seq, depth, root) and TreeConfig (num_minted) can be read to check sync (syncTree).
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { keccak_256 } from "@noble/hashes/sha3.js";
import { b58decode, b58encode, findProgramAddress, isPubkey, MAX_TX_VERSION } from "./codec.js";
import { TxWatcher, type GetTx, txAccountKeys } from "./txstream.js";
import type { StreamHub } from "./stream.js";

export const BUBBLEGUM = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY";
const disc = (n: string) => createHash("sha256").update(`global:${n}`).digest().subarray(0, 8);
export const DISC = Object.fromEntries(Object.entries({
  mintV1: "mint_v1", mintToCollectionV1: "mint_to_collection_v1", transfer: "transfer", burn: "burn", delegate: "delegate", redeem: "redeem", cancelRedeem: "cancel_redeem",
  decompressV1: "decompress_v1", verifyCreator: "verify_creator", unverifyCreator: "unverify_creator", verifyCollection: "verify_collection", unverifyCollection: "unverify_collection",
  setAndVerifyCollection: "set_and_verify_collection", updateMetadata: "update_metadata",
  mintV2: "mint_v2", transferV2: "transfer_v2", burnV2: "burn_v2", delegateV2: "delegate_v2", updateMetadataV2: "update_metadata_v2", setCollectionV2: "set_collection_v2",
  verifyCreatorV2: "verify_creator_v2", unverifyCreatorV2: "unverify_creator_v2", freezeV2: "freeze_v2", thawV2: "thaw_v2", delegateAndFreezeV2: "delegate_and_freeze_v2",
  thawAndRevokeV2: "thaw_and_revoke_v2", setNonTransferableV2: "set_non_transferable_v2", updateAssetDataV2: "update_asset_data_v2",
}).map(([k, n]) => [k, disc(n)])) as Record<string, Buffer>;
export const IX_NAMES = Object.keys(DISC);
export const NOOP_PROGRAMS = new Set(["noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV", "mnoopTCrg4p8ry25e4bcWA9XZjbNjMTfgYVGGEdRsf3"]);
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
/** Bubblegum V2 MetadataArgsV2 (name, symbol, uri, fee, primary, mutable, Option<token_standard>, creators, Option<collection>) */
export function parseMetadataArgsV2(b: Buffer, o0 = 0) {
  let o = o0;
  const u8 = () => b[o++]; const u32 = () => { const v = b.readUInt32LE(o); o += 4; return v; };
  const str = () => { const n = u32(); const s = b.subarray(o, o + n).toString("utf8"); o += n; return s; };
  const pk = () => { const k = b58encode(b.subarray(o, o + 32)); o += 32; return k; };
  const m: MetadataArgs = { name: str(), symbol: str(), uri: str(), sellerFeeBasisPoints: (o += 2, b.readUInt16LE(o - 2)), primarySaleHappened: !!u8(), isMutable: !!u8(), creators: [] };
  if (u8()) m.tokenStandard = u8();
  const n = u32(); for (let i = 0; i < n; i++) m.creators.push({ address: pk(), verified: !!u8(), share: u8() });
  if (u8()) m.collection = { verified: true, key: pk() };
  if (o > b.length) throw new Error("truncated MetadataArgsV2");
  return { args: m, len: o - o0 };
}

/** Bubblegum UpdateArgs: all Options (name, symbol, uri, creators, seller_fee, primary_sale, is_mutable) */
export function applyUpdateArgs(m: MetadataArgs, b: Buffer, o = 0): MetadataArgs {
  const r: MetadataArgs = structuredClone(m);
  const u32 = () => { const v = b.readUInt32LE(o); o += 4; return v; };
  const opt = <T>(f: () => T): T | undefined => (b[o++] ? f() : undefined);
  const str = () => { const n = u32(); const s = b.subarray(o, o + n).toString("utf8"); o += n; return s; };
  const name = opt(str), symbol = opt(str), uri = opt(str);
  const creators = opt(() => { const n = u32(); const c = []; for (let i = 0; i < n; i++) { c.push({ address: b58encode(b.subarray(o, o + 32)), verified: !!b[o + 32], share: b[o + 33] }); o += 34; } return c; });
  const fee = opt(() => { const v = b.readUInt16LE(o); o += 2; return v; });
  const psh = opt(() => !!b[o++]); const mut = opt(() => !!b[o++]);
  if (name != null) r.name = name; if (symbol != null) r.symbol = symbol; if (uri != null) r.uri = uri; if (creators) r.creators = creators;
  if (fee != null) r.sellerFeeBasisPoints = fee; if (psh != null) r.primarySaleHappened = psh; if (mut != null) r.isMutable = mut;
  return r;
}

/** Re-serialize MetadataArgs (V1 borsh) — used for data_hash after verify/update ops when no leaf event is present */
export function serializeMetadataArgs(m: MetadataArgs): Buffer {
  const parts: Buffer[] = []; const u8 = (v: number) => parts.push(Buffer.from([v]));
  const u16 = (v: number) => { const x = Buffer.alloc(2); x.writeUInt16LE(v); parts.push(x); }; const u32 = (v: number) => { const x = Buffer.alloc(4); x.writeUInt32LE(v); parts.push(x); };
  const str = (v: string) => { const x = Buffer.from(v, "utf8"); u32(x.length); parts.push(x); };
  str(m.name); str(m.symbol); str(m.uri); u16(m.sellerFeeBasisPoints); u8(m.primarySaleHappened ? 1 : 0); u8(m.isMutable ? 1 : 0);
  u8(0); if (m.tokenStandard != null) { u8(1); u8(m.tokenStandard); } else u8(0);
  if (m.collection) { u8(1); u8(m.collection.verified ? 1 : 0); parts.push(b58decode(m.collection.key)); } else u8(0);
  u8(0); u8(0); u32(m.creators.length);
  for (const c of m.creators) { parts.push(b58decode(c.address)); u8(c.verified ? 1 : 0); u8(c.share); }
  return Buffer.concat(parts);
}

export interface ChangeLogEvent { tree: string; path: { node: Buffer; index: number }[]; seq: number; index: number }
export interface LeafEvent { version: 1 | 2; id: string; owner: string; delegate: string; nonce: number; dataHash: Buffer; creatorHash: Buffer; collectionHash?: Buffer; assetDataHash?: Buffer; flags?: number; leafHash: Buffer }
/** Decode an account-compression noop payload: ChangeLog(V1) or ApplicationData(V1) carrying a Bubblegum LeafSchemaEvent. */
export function parseNoopEvent(b: Buffer): { changelog?: ChangeLogEvent; leaf?: LeafEvent } | null {
  try {
    if (b[0] === 0 && b[1] === 0) {
      let o = 2; const tree = b58encode(b.subarray(o, o + 32)); o += 32;
      const n = b.readUInt32LE(o); o += 4; const p = [];
      for (let i = 0; i < n; i++) { p.push({ node: Buffer.from(b.subarray(o, o + 32)), index: b.readUInt32LE(o + 32) }); o += 36; }
      const seq = Number(b.readBigUInt64LE(o)); o += 8; const index = b.readUInt32LE(o);
      return { changelog: { tree, path: p, seq, index } };
    }
    if (b[0] === 1 && b[1] === 0) {
      const len = b.readUInt32LE(2); const d = b.subarray(6, 6 + len);
      if (d[0] !== 1) return null;                          // BubblegumEventType::LeafSchemaEvent
      const tag = d[2]; let o = 3;                          // d[1] = Version, d[2] = LeafSchema variant
      const pk = () => { const k = b58encode(d.subarray(o, o + 32)); o += 32; return k; }; const h = () => { const x = Buffer.from(d.subarray(o, o + 32)); o += 32; return x; };
      const id = pk(), owner = pk(), delegate = pk(); const nonce = Number(d.readBigUInt64LE(o)); o += 8;
      const dataHash = h(), creatorHash = h();
      if (tag === 1) { const collectionHash = h(), assetDataHash = h(); const flags = d[o++]; return { leaf: { version: 2, id, owner, delegate, nonce, dataHash, creatorHash, collectionHash, assetDataHash, flags, leafHash: h() } }; }
      return { leaf: { version: 1, id, owner, delegate, nonce, dataHash, creatorHash, leafHash: h() } };
    }
  } catch {}
  return null;
}

/** Concurrent merkle tree account header + live seq/root (spl-account-compression layout). */
export function parseTreeAccount(data: Buffer) {
  const maxBufferSize = data.readUInt32LE(2), maxDepth = data.readUInt32LE(6);
  const authority = b58encode(data.subarray(10, 42)); const creationSlot = Number(data.readBigUInt64LE(42));
  const seq = Number(data.readBigUInt64LE(56)), activeIndex = Number(data.readBigUInt64LE(64)), bufferSize = Number(data.readBigUInt64LE(72));
  const clSize = 32 + 32 * maxDepth + 8; const clOff = 80 + activeIndex * clSize;
  const root = b58encode(data.subarray(clOff, clOff + 32));
  const rpOff = 80 + maxBufferSize * clSize; const rightmostIndex = data.readUInt32LE(rpOff + 32 * maxDepth + 32);
  return { maxBufferSize, maxDepth, authority, creationSlot, seq, activeIndex, bufferSize, root, rightmostIndex };
}
export const treeConfigPda = (tree: string) => findProgramAddress([b58decode(tree)], BUBBLEGUM)[0];
export const parseTreeConfig = (d: Buffer) => ({ creator: b58encode(d.subarray(8, 40)), delegate: b58encode(d.subarray(40, 72)), capacity: Number(d.readBigUInt64LE(72)), numMinted: Number(d.readBigUInt64LE(80)), isPublic: !!d[88] });

export function hashMetadata(raw: Buffer, sellerFee: number) { const f = Buffer.alloc(2); f.writeUInt16LE(sellerFee); return keccak(keccak(raw), f); }
export function hashCreators(c: MetadataArgs["creators"]) { return keccak(...c.map((x) => Buffer.concat([b58decode(x.address), Buffer.from([x.verified ? 1 : 0, x.share])]))); }
export function leafHash(id: string, owner: string, delegate: string, nonce: number, dataHash: Buffer, creatorHash: Buffer) {
  const n = Buffer.alloc(8); n.writeBigUInt64LE(BigInt(nonce));
  return keccak(Buffer.from([1]), b58decode(id), b58decode(owner), b58decode(delegate), n, dataHash, creatorHash);
}
export const assetId = (tree: string, nonce: number) => { const n = Buffer.alloc(8); n.writeBigUInt64LE(BigInt(nonce)); return findProgramAddress([Buffer.from("asset"), b58decode(tree), n], BUBBLEGUM)[0]; };

export interface CnftAsset {
  id: string; tree: string; nonce: number; leafIndex: number; owner: string; delegate: string; dataHash: string; creatorHash: string; metadata: MetadataArgs;
  burnt: boolean; redeemed?: boolean; seq: number; slot: number; version?: 1 | 2; collectionHash?: string; assetDataHash?: string; flags?: number; leafHash?: string;
}
interface TreeState { depth: number; nextNonce: number; seq: number; lastSlot: number; tree: MerkleTree; byIndex: Map<number, string>; chain?: { seq: number; root: string; numMinted?: number; checkedAt: number } }

export function parseTrees(s = process.env.CNFT_TREES ?? ""): { address: string; depth: number }[] {
  return s.split(",").map((x) => x.trim()).filter(Boolean).map((x) => {
    const [address, d] = x.split(":"); const depth = Number(d ?? 14);
    if (!isPubkey(address) || !Number.isInteger(depth) || depth < 3 || depth > 30) throw new Error(`CNFT_TREES: bad entry ${x} (want tree:depth)`);
    return { address, depth };
  });
}

export interface IndexerOptions { trees?: { address: string; depth: number }[]; file?: string; log?: (...a: any[]) => void }
const FLAG_FROZEN = 1, FLAG_PERM_FROZEN = 2, FLAG_NON_TRANSFERABLE = 4;

export class CnftIndexer {
  assets = new Map<string, CnftAsset>();
  trees = new Map<string, TreeState>();
  private seenSigs = new Set<string>();
  /** per-asset [signature, instruction type] history (newest last, capped) for getSignaturesForAsset */
  history = new Map<string, [string, string][]>();
  private curSig?: string;
  private note(id: string, name?: string) {
    if (!this.curSig) return;
    let h = this.history.get(id); if (!h) { h = []; this.history.set(id, h); }
    if (h.at(-1)?.[0] === this.curSig) return;
    h.push([this.curSig, name ? name[0].toUpperCase() + name.slice(1) : "Unknown"]); if (h.length > 1000) h.shift();
  }
  private watcher?: TxWatcher; private saveT?: NodeJS.Timeout;
  stats = { txs: 0, mints: 0, transfers: 0, burns: 0, unknownLeaf: 0, events: 0, updates: 0, redeems: 0, delegates: 0, seqGaps: 0 };
  constructor(private o: IndexerOptions = {}) {
    for (const t of o.trees ?? parseTrees()) this.addTree(t.address, t.depth);
    this.o.file ??= process.env.CNFT_INDEX_FILE;
    this.load();
  }
  addTree(address: string, depth: number) { if (!this.trees.has(address)) this.trees.set(address, { depth, nextNonce: 0, seq: 0, lastSlot: 0, tree: new MerkleTree(depth), byIndex: new Map() }); }
  get enabled() { return this.trees.size > 0; }
  has(id: string) { return this.assets.has(id); }

  /** Apply one getTransaction(json) result. Idempotent per signature. Process in chain order. */
  apply(r: any) {
    const sig = r?.transaction?.signatures?.[0];
    if (!r || r.meta?.err || (sig && this.seenSigs.has(sig))) return;
    if (sig) this.seenSigs.add(sig);
    this.curSig = sig;
    const keys = txAccountKeys(r); const slot = r.slot ?? 0;
    const top = r.transaction?.message?.instructions ?? [];
    const inner = new Map<number, any[]>((r.meta?.innerInstructions ?? []).map((g: any) => [g.index, g.instructions]));
    const all: any[] = []; top.forEach((ix: any, i: number) => { all.push(ix); all.push(...(inner.get(i) ?? [])); });
    const dec = (ix: any) => { try { return b58decode(ix.data); } catch { return null; } };
    const hasEvents = all.some((ix) => NOOP_PROGRAMS.has(keys[ix.programIdIndex]) && this.isTreeEvent(dec(ix)));
    let touched = false;
    let cur: { data: Buffer; acc: (i: number) => string } | undefined; let leaf: LeafEvent | undefined;
    for (const ix of all) {
      const prog = keys[ix.programIdIndex]; const data = dec(ix); if (!data) continue;
      const acc = (i: number) => keys[ix.accounts[i]];
      try {
        if (prog === BUBBLEGUM) {
          cur = { data, acc }; leaf = undefined;
          if (!hasEvents) touched = this.applyIx(data, acc, slot) || touched;
        } else if (hasEvents && NOOP_PROGRAMS.has(prog)) {
          const ev = parseNoopEvent(data); if (!ev) continue;
          if (ev.leaf) leaf = ev.leaf;
          else if (ev.changelog && this.trees.has(ev.changelog.tree)) { touched = this.applyEvent(ev.changelog, leaf, cur, slot) || touched; leaf = undefined; }
        }
      } catch (e: any) { this.o.log?.("[cnft] parse error", sig, e.message); }
    }
    if (touched) { this.stats.txs++; this.scheduleSave(); }
  }
  private isTreeEvent(d: Buffer | null) { return !!d && d[0] === 0 && d[1] === 0 && d.length >= 34 && this.trees.has(b58encode(d.subarray(2, 34))); }

  /** Event path: real seq + leaf index from the changelog; owner/delegate/hashes from the leaf schema. */
  private applyEvent(cl: ChangeLogEvent, leaf: LeafEvent | undefined, cur: { data: Buffer; acc: (i: number) => string } | undefined, slot: number): boolean {
    const t = this.trees.get(cl.tree)!;
    const node = cl.path[0]?.node ?? ZERO;
    if (t.seq && cl.seq !== t.seq + 1) this.stats.seqGaps++;
    t.seq = Math.max(t.seq, cl.seq); t.lastSlot = slot; this.stats.events++;
    t.tree.set(cl.index, node);
    const name = cur ? IX_NAMES.find((k) => cur.data.subarray(0, 8).equals(DISC[k])) : undefined;
    const body = cur?.data.subarray(8);
    if (leaf) {
      let a = this.assets.get(leaf.id);
      if (!a) {
        let md: MetadataArgs | undefined;
        if (body && (name === "mintV1" || name === "mintToCollectionV1")) { const p = parseMetadataArgs(body); md = p.args; if (name === "mintToCollectionV1") md.collection = { verified: true, key: cur!.acc(8) }; }
        else if (body && name === "mintV2") { md = parseMetadataArgsV2(body).args; }
        a = { id: leaf.id, tree: cl.tree, nonce: leaf.nonce, leafIndex: cl.index, owner: leaf.owner, delegate: leaf.delegate, dataHash: "", creatorHash: "", burnt: false, seq: cl.seq, slot,
          metadata: md ?? { name: "", symbol: "", uri: "", sellerFeeBasisPoints: 0, primarySaleHappened: false, isMutable: true, creators: [] } };
        this.assets.set(a.id, a); this.stats.mints++;
        t.nextNonce = Math.max(t.nextNonce, leaf.nonce + 1);
      } else {
        if (a.owner !== leaf.owner) this.stats.transfers++;
        if (a.delegate !== leaf.delegate) this.stats.delegates++;
        if (body) a.metadata = this.metadataAfter(name, body, cur!.acc, a.metadata);
      }
      if (a.leafIndex !== cl.index) t.byIndex.delete(a.leafIndex);
      Object.assign(a, { owner: leaf.owner, delegate: leaf.delegate, nonce: leaf.nonce, leafIndex: cl.index, dataHash: b58encode(leaf.dataHash), creatorHash: b58encode(leaf.creatorHash),
        seq: cl.seq, slot, burnt: false, redeemed: false, version: leaf.version, leafHash: b58encode(node) });
      if (leaf.version === 2) Object.assign(a, { collectionHash: b58encode(leaf.collectionHash!), assetDataHash: b58encode(leaf.assetDataHash!), flags: leaf.flags });
      t.byIndex.set(cl.index, a.id); this.note(a.id, name);
      return true;
    }
    // no leaf event: burn / redeem (leaf -> zero) or cancel_redeem (voucher leaf restored)
    const id = t.byIndex.get(cl.index); const a = id ? this.assets.get(id) : undefined;
    if (!a) { this.stats.unknownLeaf++; return true; }
    a.seq = cl.seq; a.slot = slot; a.leafHash = b58encode(node); this.note(a.id, name);
    if (node.equals(ZERO)) {
      if (name === "redeem") { a.redeemed = true; this.stats.redeems++; } else { a.burnt = true; this.stats.burns++; }
    } else { a.redeemed = false; a.burnt = false; }
    return true;
  }
  /** metadata after a metadata-changing instruction (V1 instructions carry the full current MetadataArgs) */
  private metadataAfter(name: string | undefined, body: Buffer, acc: (i: number) => string, prev: MetadataArgs): MetadataArgs {
    if (name === "verifyCreator" || name === "unverifyCreator") {
      const m = parseMetadataArgs(body, 108).args; const c = m.creators.find((x) => x.address === acc(5)); if (c) c.verified = name === "verifyCreator"; this.stats.updates++; return m;
    }
    if (name === "verifyCollection" || name === "unverifyCollection" || name === "setAndVerifyCollection") {
      const m = parseMetadataArgs(body, 108).args; m.collection = { verified: name !== "unverifyCollection", key: acc(8) }; this.stats.updates++; return m;
    }
    if (name === "updateMetadata") {
      const p = parseMetadataArgs(body, 44); this.stats.updates++; return applyUpdateArgs(p.args, body, 44 + p.len);
    }
    if (name === "verifyCreatorV2" || name === "unverifyCreatorV2") {
      const m = structuredClone(prev); const creator = acc(3); const c = m.creators.find((x) => x.address === creator); if (c) c.verified = name === "verifyCreatorV2"; return m;
    }
    return prev;
  }

  /** Fallback (no noop events): instruction-only model; nonce assumed sequential. */
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
      const lh = leafHash(id, owner, delegate, nonce, dataHash, creatorHash);
      t.tree.set(nonce, lh); t.seq++; t.lastSlot = slot; t.byIndex.set(nonce, id);
      this.assets.set(id, { id, tree, nonce, leafIndex: nonce, owner, delegate, dataHash: b58encode(dataHash), creatorHash: b58encode(creatorHash), metadata: args, burnt: false, seq: t.seq, slot, version: 1, leafHash: b58encode(lh) });
      this.stats.mints++; this.note(id, isColl ? "mintToCollectionV1" : "mintV1"); return true;
    }
    const isXfer = d.equals(DISC.transfer), isBurn = d.equals(DISC.burn), isDel = d.equals(DISC.delegate);
    if (!isXfer && !isBurn && !isDel) return false;
    const tree = acc(isBurn ? 3 : 4), t = this.trees.get(tree); if (!t) return false;
    const nonce = Number(body.readBigUInt64LE(96)), index = body.readUInt32LE(104);
    const id = assetId(tree, nonce); const a = this.assets.get(id);
    if (!a) { this.stats.unknownLeaf++; return false; }
    t.seq++; t.lastSlot = slot; a.seq = t.seq; a.slot = slot; this.note(id, isBurn ? "burn" : isXfer ? "transfer" : "delegate");
    if (isBurn) { a.burnt = true; t.tree.set(index, ZERO); a.leafHash = b58encode(ZERO); this.stats.burns++; return true; }
    if (isXfer) { a.owner = acc(3); a.delegate = acc(3); this.stats.transfers++; } else { a.delegate = acc(3); this.stats.delegates++; }
    const lh = leafHash(id, a.owner, a.delegate, nonce, b58decode(a.dataHash), b58decode(a.creatorHash));
    t.tree.set(index, lh); a.leafHash = b58encode(lh);
    return true;
  }

  /** Read the on-chain tree (seq/root/depth) + TreeConfig (num_minted) and compare with the replica. */
  async syncTree(rpc: (m: string, p: any[]) => Promise<any>, tree: string) {
    const [acc, cfg] = await Promise.all([
      rpc("getAccountInfo", [tree, { encoding: "base64", commitment: "confirmed" }]),
      rpc("getAccountInfo", [treeConfigPda(tree), { encoding: "base64", commitment: "confirmed" }]).catch(() => null),
    ]);
    const data = acc?.value?.data?.[0]; if (!data) throw new Error(`tree ${tree} not found`);
    const h = parseTreeAccount(Buffer.from(data, "base64"));
    const numMinted = cfg?.value?.data?.[0] ? parseTreeConfig(Buffer.from(cfg.value.data[0], "base64")).numMinted : undefined;
    this.addTree(tree, h.maxDepth);
    const t = this.trees.get(tree)!;
    t.chain = { seq: h.seq, root: h.root, numMinted, checkedAt: Date.now() };
    const local = b58encode(t.tree.root());
    return { tree, depth: h.maxDepth, maxBufferSize: h.maxBufferSize, chainSeq: h.seq, localSeq: t.seq, chainRoot: h.root, localRoot: local, numMinted, inSync: h.seq === t.seq && h.root === local, behind: Math.max(0, h.seq - t.seq) };
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
    for (const s of sigs.reverse()) { const r = await rpc("getTransaction", [s, { encoding: "json", maxSupportedTransactionVersion: MAX_TX_VERSION, commitment: "confirmed" }]); if (r) this.apply(r); }
    return sigs.length;
  }

  toDas(a: CnftAsset) {
    const t = this.trees.get(a.tree)!; const m = a.metadata;
    return {
      interface: "V1_NFT", id: a.id,
      content: { $schema: "https://schema.metaplex.com/nft1.0.json", json_uri: m.uri, files: [], metadata: { name: m.name, symbol: m.symbol }, links: {} },
      authorities: [],
      compression: { eligible: false, compressed: true, data_hash: a.dataHash, creator_hash: a.creatorHash, asset_hash: b58encode(a.burnt || a.redeemed ? ZERO : t.tree.leaf(a.leafIndex ?? a.nonce)), tree: a.tree, seq: a.seq, leaf_id: a.nonce,
        ...(a.version === 2 ? { collection_hash: a.collectionHash, asset_data_hash: a.assetDataHash, flags: a.flags } : {}) },
      grouping: m.collection?.verified ? [{ group_key: "collection", group_value: m.collection.key }] : [],
      royalty: { royalty_model: "creators", target: null, percent: m.sellerFeeBasisPoints / 10_000, basis_points: m.sellerFeeBasisPoints, primary_sale_happened: m.primarySaleHappened, locked: false },
      creators: m.creators,
      ownership: { frozen: !!((a.flags ?? 0) & (FLAG_FROZEN | FLAG_PERM_FROZEN)), non_transferable: !!((a.flags ?? 0) & FLAG_NON_TRANSFERABLE), delegated: a.delegate !== a.owner, delegate: a.delegate !== a.owner ? a.delegate : null, ownership_model: "single", owner: a.owner },
      supply: null, mutable: m.isMutable, burnt: a.burnt,
    };
  }
  proof(id: string) {
    const a = this.assets.get(id); if (!a) return null;
    const t = this.trees.get(a.tree)!;
    const i = a.leafIndex ?? a.nonce;
    return { root: b58encode(t.tree.root()), proof: t.tree.proof(i).map(b58encode), node_index: 2 ** t.depth + i, leaf: b58encode(t.tree.leaf(i)), tree_id: a.tree };
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
        a.leafIndex ??= a.nonce; t.byIndex.set(a.leafIndex, a.id);
        t.tree.set(a.leafIndex, a.burnt || a.redeemed ? ZERO : a.leafHash ? b58decode(a.leafHash) : leafHash(a.id, a.owner, a.delegate, a.nonce, b58decode(a.dataHash), b58decode(a.creatorHash))); }
      for (const s of j.sigs ?? []) this.seenSigs.add(s);
    } catch (e: any) { this.o.log?.("[cnft] failed to load index", e.message); }
  }
  stop() { this.watcher?.stop(); this.watcher = undefined; if (this.saveT) { clearTimeout(this.saveT); this.saveT = undefined; this.save(); } }
}
