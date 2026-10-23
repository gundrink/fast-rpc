// DAS (Digital Asset Standard) API. Proxies to DAS_RPC_URL when set; otherwise best-effort on-chain
// implementation of getAsset / getAssetsByOwner for SPL mints (+ Metaplex Token Metadata decoding).
import type { CnftIndexer, CnftAsset } from "./bubblegum.js";
import { JsonFetcher } from "./offchain.js";
import { CollectionIndex } from "./collections.js";
import { RpcError, INVALID_PARAMS } from "./errors.js";
import { b58decode, b58encode, findProgramAddress, isPubkey } from "./codec.js";

type Up = (method: string, params: any, ttl?: number) => Promise<any>;
export const METADATA_PROGRAM = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";
export const TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
export const TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
export const DAS_METHODS = ["getAsset", "getAssetBatch", "getAssetsByOwner", "getAssetsByGroup", "getAssetsByCreator", "getAssetsByAuthority", "searchAssets", "getAssetProof", "getAssetProofBatch", "getSignaturesForAsset", "getTokenAccounts", "getNftEditions"] as const;

export const metadataPda = (mint: string) =>
  findProgramAddress([Buffer.from("metadata"), b58decode(METADATA_PROGRAM), b58decode(mint)], METADATA_PROGRAM)[0];

export interface Metadata { updateAuthority: string; mint: string; name: string; symbol: string; uri: string; sellerFeeBasisPoints: number;
  creators: { address: string; verified: boolean; share: number }[]; primarySaleHappened: boolean; isMutable: boolean; tokenStandard?: number; collection?: { verified: boolean; key: string } }

/** Borsh-decode a Metaplex Token Metadata account (v1 layout; trailing optional fields best-effort). */
export function decodeMetadata(data: Buffer): Metadata {
  let o = 0;
  const u8 = () => data[o++];
  const pk = () => { const k = b58encode(data.subarray(o, o + 32)); o += 32; return k; };
  const str = () => { const n = data.readUInt32LE(o); o += 4; if (n > 1000) throw new Error("bad string"); const s = data.subarray(o, o + n).toString("utf8"); o += n; return s.replace(/\0+$/g, "").trim(); };
  if (u8() !== 4) throw new Error("not a MetadataV1 account");
  const updateAuthority = pk(), mint = pk(), name = str(), symbol = str(), uri = str();
  const sellerFeeBasisPoints = data.readUInt16LE(o); o += 2;
  const creators: Metadata["creators"] = [];
  if (u8()) { const n = data.readUInt32LE(o); o += 4; for (let i = 0; i < n; i++) creators.push({ address: pk(), verified: !!u8(), share: u8() }); }
  const m: Metadata = { updateAuthority, mint, name, symbol, uri, sellerFeeBasisPoints, creators, primarySaleHappened: !!u8(), isMutable: !!u8() };
  try {
    if (u8()) o += 1;                         // edition_nonce
    if (u8()) m.tokenStandard = u8();         // token_standard
    if (u8()) { const verified = !!u8(); m.collection = { verified, key: pk() }; }
  } catch {}
  return m;
}

const TS = ["NonFungible", "FungibleAsset", "Fungible", "NonFungibleEdition", "ProgrammableNonFungible", "ProgrammableNonFungibleEdition"];

function toAsset(mint: string, mintInfo: any, tokenProgram: string, md0?: Metadata, owner?: { owner: string; frozen: boolean; amount?: string }) {
  // Token-2022 metadata extension (jsonParsed) when there's no Metaplex metadata account
  const ext = (mintInfo?.extensions ?? []).find((e: any) => e.extension === "tokenMetadata")?.state;
  const md: Metadata | undefined = md0 ?? (ext ? { updateAuthority: ext.updateAuthority ?? "", mint, name: ext.name ?? "", symbol: ext.symbol ?? "", uri: ext.uri ?? "", sellerFeeBasisPoints: 0, creators: [], primarySaleHappened: false, isMutable: !!ext.updateAuthority } as any : undefined);
  const decimals = mintInfo?.decimals ?? 0;
  const supply = Number(mintInfo?.supply ?? 0);
  const nft = decimals === 0 && supply === 1;
  const iface = md?.tokenStandard != null ? (TS[md.tokenStandard]?.startsWith("Programmable") ? "ProgrammableNFT" : TS[md.tokenStandard] === "Fungible" || TS[md.tokenStandard] === "FungibleAsset" ? "FungibleToken" : "V1_NFT")
    : nft ? "V1_NFT" : "FungibleToken";
  return {
    interface: iface, id: mint,
    content: { $schema: "https://schema.metaplex.com/nft1.0.json", json_uri: md?.uri ?? "", files: [], metadata: { name: md?.name ?? "", symbol: md?.symbol ?? "" }, links: {} },
    authorities: md ? [{ address: md.updateAuthority, scopes: ["full"] }] : [],
    compression: { eligible: false, compressed: false, data_hash: "", creator_hash: "", asset_hash: "", tree: "", seq: 0, leaf_id: 0 },
    grouping: md?.collection?.verified ? [{ group_key: "collection", group_value: md.collection.key }] : [],
    royalty: { royalty_model: "creators", target: null, percent: (md?.sellerFeeBasisPoints ?? 0) / 10_000, basis_points: md?.sellerFeeBasisPoints ?? 0, primary_sale_happened: md?.primarySaleHappened ?? false, locked: false },
    creators: md?.creators ?? [],
    ownership: { frozen: owner?.frozen ?? false, delegated: false, delegate: null, ownership_model: nft ? "single" : "token", owner: owner?.owner ?? "" },
    supply: nft ? { print_max_supply: 0, print_current_supply: 0, edition_nonce: null } : null,
    mutable: md?.isMutable ?? (mintInfo?.mintAuthority != null),
    burnt: false,
    token_info: { supply, decimals, token_program: tokenProgram, mint_authority: mintInfo?.mintAuthority ?? null, freeze_authority: mintInfo?.freezeAuthority ?? null,
      ...(owner?.amount != null ? { balance: Number(owner.amount) } : {}) },
  };
}

export interface DasExtras { json?: JsonFetcher | false; collections?: CollectionIndex }
export function createDas(up: Up, dasUp?: Up, cnft?: CnftIndexer, extras: DasExtras = {}) {
  const proxied = Object.fromEntries(DAS_METHODS.map((m) => [m, (p: any) => {
    if (!dasUp) throw new RpcError(-32601, `${m} requires DAS_RPC_URL (no on-chain fallback for this method)`);
    return dasUp(m, p ?? {}, 0);
  }]));
  if (dasUp) return proxied;
  // off-chain json: on when DAS_FETCH_JSON=1 (or a fetcher is injected); per request displayOptions.showContent=false skips it
  const json = extras.json === false ? undefined : extras.json ?? (process.env.DAS_FETCH_JSON === "1" ? new JsonFetcher() : undefined);
  const collections = extras.collections ?? (process.env.DAS_COLLECTIONS ? new CollectionIndex() : undefined);
  if (collections?.enabled && !extras.collections) collections.start(up);
  const enrich = async <T>(x: T, p?: any): Promise<T> => {
    if (!json || p?.displayOptions?.showContent === false) return x;
    const list = (Array.isArray(x) ? x : (x as any)?.items ?? [x]).filter(Boolean);
    await Promise.all(list.map((a: any) => json.enrich(a)));
    return x;
  };

  const fetchAssets = async (mints: string[], owners: Map<string, { owner: string; frozen: boolean; amount?: string }> = new Map()) => {
    const out: any[] = [];
    for (let i = 0; i < mints.length; i += 50) {
      const chunk = mints.slice(i, i + 50);
      const pdas = chunk.map(metadataPda);
      const [mintRes, mdRes] = await Promise.all([
        up("getMultipleAccounts", [chunk, { encoding: "jsonParsed" }], 1000),
        up("getMultipleAccounts", [pdas, { encoding: "base64" }], 1000),
      ]);
      chunk.forEach((mint, j) => {
        const acc = mintRes?.value?.[j];
        if (!acc || acc.data?.parsed?.type !== "mint") return out.push(null);
        let md: Metadata | undefined;
        const raw = mdRes?.value?.[j]?.data?.[0];
        if (raw) { try { md = decodeMetadata(Buffer.from(raw, "base64")); } catch {} }
        out.push(toAsset(mint, acc.data.parsed.info, acc.owner, md, owners.get(mint)));
      });
    }
    return out;
  };

  const paging = (p: any) => ({ limit: Math.min(Math.max(Number(p?.limit ?? 100), 1), 1000), page: Math.max(Number(p?.page ?? 1), 1) });
  const cnftOf = (pred: (a: CnftAsset) => boolean) => cnft?.enabled ? cnft.list(pred).map((a) => cnft.toDas(a)) : [];
  const page = (all: any[], p: any) => { const { limit, page } = paging(p); return { total: all.length, limit, page, items: all.slice((page - 1) * limit, page * limit) }; };
  const proof = (id: any) => {
    if (!isPubkey(id)) throw INVALID_PARAMS("getAssetProof: params.id must be an asset id");
    const r = cnft?.proof(id); if (!r) throw new RpcError(-32000, "Asset proof not found (only CNFT_TREES-indexed compressed assets)"); return r;
  };
  const ownerTokens = async (owner: string, showZero: boolean) => {
    const [t1, t2] = await Promise.all([TOKEN_PROGRAM, TOKEN_2022].map((programId) =>
      up("getTokenAccountsByOwner", [owner, { programId }, { encoding: "jsonParsed" }], 1000).catch(() => ({ value: [] }))));
    const owners = new Map<string, { owner: string; frozen: boolean; amount?: string }>();
    for (const a of [...(t1?.value ?? []), ...(t2?.value ?? [])]) {
      const info = a.account?.data?.parsed?.info; if (!info) continue;
      if (!showZero && info.tokenAmount?.amount === "0") continue;
      owners.set(info.mint, { owner, frozen: info.state === "frozen", amount: info.tokenAmount?.amount });
    }
    return owners;
  };

  return {
    ...proxied,
    getAssetProof: async (p: any) => proof(p?.id),
    getAssetProofBatch: async (p: any) => {
      if (!Array.isArray(p?.ids) || !p.ids.length || p.ids.length > 1000) throw INVALID_PARAMS("getAssetProofBatch: params.ids must be 1..1000 ids");
      return Object.fromEntries(p.ids.map((id: string) => { try { return [id, proof(id)]; } catch { return [id, null]; } }));
    },
    getAssetsByGroup: async (p: any) => {
      if (p?.groupKey !== "collection" || !isPubkey(p?.groupValue)) throw INVALID_PARAMS("getAssetsByGroup: groupKey must be 'collection' and groupValue a collection mint");
      // local indexes: compressed assets (CNFT_TREES) + uncompressed members of DAS_COLLECTIONS (gPA index)
      const c = cnftOf((a) => a.metadata.collection?.verified === true && a.metadata.collection.key === p.groupValue);
      const mints = collections?.has(p.groupValue) ? collections.list(p.groupValue) : [];
      const { limit, page: pg } = paging(p); const start = (pg - 1) * limit;
      const cs = c.slice(start, start + limit);
      const ms = mints.slice(Math.max(0, start - c.length), Math.max(0, start - c.length) + (limit - cs.length));
      const items = [...cs, ...(ms.length ? (await fetchAssets(ms)).filter(Boolean) : [])];
      return enrich({ total: c.length + mints.length, limit, page: pg, items }, p);
    },
    searchAssets: async (p: any = {}) => {
      const known = new Set(["ownerAddress", "grouping", "creatorAddress", "creatorVerified", "compressed", "burnt", "tokenType", "page", "limit", "sortBy", "displayOptions", "options", "interface"]);
      const unk = Object.keys(p ?? {}).filter((k) => !known.has(k));
      if (unk.length) throw INVALID_PARAMS(`searchAssets: unsupported filters on local index: ${unk.join(",")}`);
      if (p.ownerAddress !== undefined && !isPubkey(p.ownerAddress)) throw INVALID_PARAMS("searchAssets: ownerAddress must be a pubkey");
      if (!p.ownerAddress && !p.grouping && !p.creatorAddress) throw INVALID_PARAMS("searchAssets: local index needs ownerAddress, grouping or creatorAddress");
      const tt = p.tokenType ?? (p.ownerAddress ? "all" : "compressedNft");
      const grp = Array.isArray(p.grouping) ? { k: p.grouping[0], v: p.grouping[1] } : undefined;
      let items: any[] = cnftOf((a) => (p.ownerAddress === undefined || a.owner === p.ownerAddress)
        && (p.burnt === undefined ? !a.burnt : a.burnt === !!p.burnt)
        && (!grp || (grp.k === "collection" && a.metadata.collection?.verified === true && a.metadata.collection.key === grp.v))
        && (!p.creatorAddress || a.metadata.creators.some((c) => c.address === p.creatorAddress && (p.creatorVerified === undefined || c.verified === !!p.creatorVerified))));
      if (!["all", "compressedNft", "fungible", "nonFungible", "regularNft"].includes(tt)) throw INVALID_PARAMS("searchAssets: bad tokenType");
      if (tt === "fungible" || tt === "regularNft" || p.compressed === false) items = [];
      if (p.ownerAddress && tt !== "compressedNft" && p.compressed !== true && !grp && !p.creatorAddress && !p.burnt) {
        const owners = await ownerTokens(p.ownerAddress, !!p.displayOptions?.showZeroBalance);
        let toks = (await fetchAssets([...owners.keys()].sort(), owners)).filter(Boolean);
        if (tt === "fungible") toks = toks.filter((a) => a.interface === "FungibleToken");
        if (tt === "nonFungible" || tt === "regularNft") toks = toks.filter((a) => a.interface !== "FungibleToken");
        items = [...items, ...toks];
      }
      return enrich(page(items, p), p);
    },
    getAsset: async (p: any) => {
      const id = p?.id;
      if (!isPubkey(id)) throw INVALID_PARAMS("getAsset: params.id must be a mint address");
      if (cnft?.has(id)) return enrich(cnft.toDas(cnft.assets.get(id)!), p);
      const [a] = await fetchAssets([id]);
      if (!a) throw new RpcError(-32000, "Asset not found (on-chain fallback supports SPL mints only)");
      return enrich(a, p);
    },
    getAssetBatch: async (p: any) => {
      const ids: string[] = p?.ids;
      if (!Array.isArray(ids) || !ids.length || ids.length > 1000 || !ids.every(isPubkey)) throw INVALID_PARAMS("getAssetBatch: params.ids must be 1..1000 addresses");
      const local = new Map(ids.filter((i) => cnft?.has(i)).map((i) => [i, cnft!.toDas(cnft!.assets.get(i)!)]));
      const rest = ids.filter((i) => !local.has(i)); const fetched = rest.length ? await fetchAssets(rest) : [];
      const m = new Map(rest.map((i, j) => [i, fetched[j]]));
      return enrich(ids.map((i) => local.get(i) ?? m.get(i) ?? null), p);
    },
    getAssetsByOwner: async (p: any) => {
      const owner = p?.ownerAddress;
      if (!isPubkey(owner)) throw INVALID_PARAMS("getAssetsByOwner: params.ownerAddress required");
      const { limit, page } = paging(p);
      const owners = await ownerTokens(owner, !!p.displayOptions?.showZeroBalance);
      const compressed = cnftOf((a) => a.owner === owner && !a.burnt);
      const all = [...owners.keys()].sort();
      const start = (page - 1) * limit;
      const c = compressed.slice(start, start + limit);
      const pageMints = all.slice(Math.max(0, start - compressed.length), Math.max(0, start - compressed.length) + (limit - c.length));
      const items = [...c, ...(await fetchAssets(pageMints, owners)).filter(Boolean)];
      return enrich({ total: all.length + compressed.length, limit, page, items }, p);
    },
  };
}
