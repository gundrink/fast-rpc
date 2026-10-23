// DAS (Digital Asset Standard) API. Proxies to DAS_RPC_URL when set; otherwise best-effort on-chain
// implementation of getAsset / getAssetsByOwner for SPL mints (+ Metaplex Token Metadata decoding).
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

function toAsset(mint: string, mintInfo: any, tokenProgram: string, md?: Metadata, owner?: { owner: string; frozen: boolean; amount?: string }) {
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

export function createDas(up: Up, dasUp?: Up) {
  const proxied = Object.fromEntries(DAS_METHODS.map((m) => [m, (p: any) => {
    if (!dasUp) throw new RpcError(-32601, `${m} requires DAS_RPC_URL (no on-chain fallback for this method)`);
    return dasUp(m, p ?? {}, 0);
  }]));
  if (dasUp) return proxied;

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

  return {
    ...proxied,
    getAsset: async (p: any) => {
      const id = p?.id;
      if (!isPubkey(id)) throw INVALID_PARAMS("getAsset: params.id must be a mint address");
      const [a] = await fetchAssets([id]);
      if (!a) throw new RpcError(-32000, "Asset not found (on-chain fallback supports SPL mints only)");
      return a;
    },
    getAssetBatch: async (p: any) => {
      const ids: string[] = p?.ids;
      if (!Array.isArray(ids) || !ids.length || ids.length > 1000 || !ids.every(isPubkey)) throw INVALID_PARAMS("getAssetBatch: params.ids must be 1..1000 addresses");
      return fetchAssets(ids);
    },
    getAssetsByOwner: async (p: any) => {
      const owner = p?.ownerAddress;
      if (!isPubkey(owner)) throw INVALID_PARAMS("getAssetsByOwner: params.ownerAddress required");
      const limit = Math.min(Math.max(Number(p.limit ?? 100), 1), 1000), page = Math.max(Number(p.page ?? 1), 1);
      const showZero = !!p.displayOptions?.showZeroBalance;
      const [t1, t2] = await Promise.all([TOKEN_PROGRAM, TOKEN_2022].map((programId) =>
        up("getTokenAccountsByOwner", [owner, { programId }, { encoding: "jsonParsed" }], 1000).catch(() => ({ value: [] }))));
      const owners = new Map<string, { owner: string; frozen: boolean; amount?: string }>();
      for (const a of [...(t1?.value ?? []), ...(t2?.value ?? [])]) {
        const info = a.account?.data?.parsed?.info; if (!info) continue;
        if (!showZero && info.tokenAmount?.amount === "0") continue;
        owners.set(info.mint, { owner, frozen: info.state === "frozen", amount: info.tokenAmount?.amount });
      }
      const all = [...owners.keys()].sort();
      const pageMints = all.slice((page - 1) * limit, page * limit);
      const items = (await fetchAssets(pageMints, owners)).filter(Boolean);
      return { total: all.length, limit, page, items };
    },
  };
}
