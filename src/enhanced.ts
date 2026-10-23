// Best-effort Helius "enhanced transaction" parser. Input: getTransaction(encoding:"json").
// Types: TRANSFER, SWAP (Jupiter SwapEvent CPI + per-hop Raydium/Orca/Meteora/Pump inner transfers, user deltas),
// NFT_SALE, NFT_LISTING, NFT_CANCEL_LISTING, NFT_BID, NFT_BID_CANCELLED (Magic Eden M2 / Tensor), TOKEN_MINT, BURN,
// STAKE_SOL, UNSTAKE_SOL, WITHDRAW, CREATE_ACCOUNT, COMPRESSED_NFT_* (all Bubblegum V1/V2 ops); else UNKNOWN.
import { createHash } from "node:crypto";
import { b58decode, b58encode } from "./codec.js";
import { txAccountKeys } from "./txstream.js";
import { BUBBLEGUM, DISC } from "./bubblegum.js";

export const TX_TYPES = ["ANY", "UNKNOWN", "TRANSFER", "SWAP", "NFT_SALE", "NFT_LISTING", "NFT_CANCEL_LISTING", "NFT_BID", "NFT_BID_CANCELLED", "TOKEN_MINT", "BURN",
  "STAKE_SOL", "UNSTAKE_SOL", "WITHDRAW", "CREATE_ACCOUNT", "COMPRESSED_NFT_MINT", "COMPRESSED_NFT_TRANSFER", "COMPRESSED_NFT_BURN", "COMPRESSED_NFT_DELEGATE",
  "COMPRESSED_NFT_REDEEM", "COMPRESSED_NFT_CANCEL_REDEEM", "COMPRESSED_NFT_DECOMPRESS", "COMPRESSED_NFT_VERIFY_CREATOR", "COMPRESSED_NFT_UNVERIFY_CREATOR",
  "COMPRESSED_NFT_VERIFY_COLLECTION", "COMPRESSED_NFT_UNVERIFY_COLLECTION", "COMPRESSED_NFT_SET_VERIFY_COLLECTION", "COMPRESSED_NFT_UPDATE_METADATA",
  "COMPRESSED_NFT_FREEZE", "COMPRESSED_NFT_THAW", "COMPRESSED_NFT_UPDATE"] as const;
const TOKEN_PROGRAMS = new Set(["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"]);
export const STAKE_PROGRAM = "Stake11111111111111111111111111111111111111";
const anchor = (n: string) => createHash("sha256").update(`global:${n}`).digest().subarray(0, 8);
const ME = "M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K", TCOMP = "TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp", TBID = "TB1Dqt8JeKQh7RLDzfYDJsq8KS4fS2yt87avRjyRxMv", TAMM = "TAMM6ub33ij1mbetoMyVBLeKY5iP41i4UPUJQGkhfsg";
const MARKET: [string[], string, string[]][] = [
  [[ME], "NFT_LISTING", ["sell", "mip1_sell", "ocp_sell", "core_sell"]], [[ME], "NFT_CANCEL_LISTING", ["cancel_sell", "mip1_cancel_sell", "ocp_cancel_sell", "core_cancel_sell"]],
  [[ME], "NFT_BID", ["buy", "buy_v2"]], [[ME], "NFT_BID_CANCELLED", ["cancel_buy"]],
  [[TCOMP], "NFT_LISTING", ["list", "list_legacy", "list_core", "list_t22", "list_wns"]], [[TCOMP], "NFT_CANCEL_LISTING", ["delist", "delist_legacy", "delist_core", "delist_t22", "delist_wns"]],
  [[TCOMP, TBID], "NFT_BID", ["bid"]], [[TCOMP, TBID], "NFT_BID_CANCELLED", ["cancel_bid", "close_expired_bid"]],
  [[TAMM], "NFT_LISTING", ["deposit_nft", "list"]], [[TAMM], "NFT_CANCEL_LISTING", ["withdraw_nft", "delist"]],
];
const MARKET_IX = MARKET.flatMap(([progs, type, names]) => names.map((n) => ({ progs, type, name: n, d: anchor(n) })));
const CNFT_TYPES: Record<string, string> = {
  mintV1: "COMPRESSED_NFT_MINT", mintToCollectionV1: "COMPRESSED_NFT_MINT", mintV2: "COMPRESSED_NFT_MINT", transfer: "COMPRESSED_NFT_TRANSFER", transferV2: "COMPRESSED_NFT_TRANSFER",
  burn: "COMPRESSED_NFT_BURN", burnV2: "COMPRESSED_NFT_BURN", delegate: "COMPRESSED_NFT_DELEGATE", delegateV2: "COMPRESSED_NFT_DELEGATE", redeem: "COMPRESSED_NFT_REDEEM",
  cancelRedeem: "COMPRESSED_NFT_CANCEL_REDEEM", decompressV1: "COMPRESSED_NFT_DECOMPRESS", verifyCreator: "COMPRESSED_NFT_VERIFY_CREATOR", verifyCreatorV2: "COMPRESSED_NFT_VERIFY_CREATOR",
  unverifyCreator: "COMPRESSED_NFT_UNVERIFY_CREATOR", unverifyCreatorV2: "COMPRESSED_NFT_UNVERIFY_CREATOR", verifyCollection: "COMPRESSED_NFT_VERIFY_COLLECTION",
  unverifyCollection: "COMPRESSED_NFT_UNVERIFY_COLLECTION", setAndVerifyCollection: "COMPRESSED_NFT_SET_VERIFY_COLLECTION", setCollectionV2: "COMPRESSED_NFT_SET_VERIFY_COLLECTION",
  updateMetadata: "COMPRESSED_NFT_UPDATE_METADATA", updateMetadataV2: "COMPRESSED_NFT_UPDATE_METADATA", freezeV2: "COMPRESSED_NFT_FREEZE", delegateAndFreezeV2: "COMPRESSED_NFT_FREEZE",
  thawV2: "COMPRESSED_NFT_THAW", thawAndRevokeV2: "COMPRESSED_NFT_THAW", setNonTransferableV2: "COMPRESSED_NFT_UPDATE", updateAssetDataV2: "COMPRESSED_NFT_UPDATE",
};
// [tree account index, new owner account index | null] per op (V1 layouts; V2 best effort)
const CNFT_ACC: Record<string, [number, number | null]> = { mintV1: [3, 1], mintToCollectionV1: [3, 1], transfer: [4, 3], burn: [3, null], delegate: [4, 1], redeem: [4, 1], cancelRedeem: [2, 1],
  verifyCreator: [3, 1], unverifyCreator: [3, 1], verifyCollection: [3, 1], unverifyCollection: [3, 1], setAndVerifyCollection: [3, 1], updateMetadata: [8, 5] };
export const JUPITER = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const EVENT_IX_TAG = Buffer.from("e445a52e51cb9a1d", "hex");
const JUP_SWAP_EVENT = createHash("sha256").update("event:SwapEvent").digest().subarray(0, 8);
const JUP_SWAPS_EVENT = createHash("sha256").update("event:SwapsEvent").digest().subarray(0, 8); // Vec<SwapEventV2 { input_mint, input_amount, output_mint, output_amount }>
const DEX = new Set(["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
  "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
  "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"]);
const SYSTEM = "11111111111111111111111111111111";
const BENIGN = new Set([SYSTEM, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
  "ComputeBudget111111111111111111111111111111", "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr", "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo"]);
const SOURCES: Record<string, string> = {
  JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4: "JUPITER", "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "RAYDIUM", CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK: "RAYDIUM",
  CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C: "RAYDIUM", whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc: "ORCA", "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP": "ORCA", Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB: "METEORA", LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo: "METEORA",
  pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA: "PUMP_AMM", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "PUMP_FUN",
  M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K: "MAGIC_EDEN", TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN: "TENSOR", TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp: "TENSOR", TB1Dqt8JeKQh7RLDzfYDJsq8KS4fS2yt87avRjyRxMv: "TENSOR", TAMM6ub33ij1mbetoMyVBLeKY5iP41i4UPUJQGkhfsg: "TENSOR",
  [STAKE_PROGRAM]: "STAKE_PROGRAM",
  [BUBBLEGUM]: "BUBBLEGUM", [SYSTEM]: "SYSTEM_PROGRAM",
};
const SOL = 1e9;

export function parseEnhanced(r: any) {
  const keys = txAccountKeys(r); const meta = r.meta ?? {};
  const sig = r.transaction?.signatures?.[0] ?? ""; const feePayer = keys[0] ?? "";
  const top = r.transaction?.message?.instructions ?? [];
  const inner = new Map<number, any[]>((meta.innerInstructions ?? []).map((g: any) => [g.index, g.instructions]));
  const flat: any[] = []; top.forEach((ix: any, i: number) => { flat.push(ix); flat.push(...(inner.get(i) ?? [])); });
  const programs = new Set(top.map((ix: any) => keys[ix.programIdIndex]));
  const data = (ix: any) => { try { return b58decode(ix.data ?? ""); } catch { return Buffer.alloc(0); } };

  const nativeTransfers: any[] = [];
  for (const ix of flat) {
    if (keys[ix.programIdIndex] !== SYSTEM) continue;
    const d = data(ix); if (d.length >= 12 && d.readUInt32LE(0) === 2) nativeTransfers.push({ fromUserAccount: keys[ix.accounts[0]], toUserAccount: keys[ix.accounts[1]], amount: Number(d.readBigUInt64LE(4)) });
  }
  // token balance deltas per token account
  type TB = { idx: number; mint: string; owner: string; decimals: number; amt: bigint };
  const tb = (l: any[]): Map<number, TB> => new Map((l ?? []).map((b: any) => [b.accountIndex, { idx: b.accountIndex, mint: b.mint, owner: b.owner ?? "", decimals: b.uiTokenAmount?.decimals ?? 0, amt: BigInt(b.uiTokenAmount?.amount ?? "0") }]));
  const pre = tb(meta.preTokenBalances), post = tb(meta.postTokenBalances);
  const deltas: (TB & { delta: bigint })[] = [];
  for (const i of new Set([...pre.keys(), ...post.keys()])) {
    const a = pre.get(i), b = post.get(i); const base = (b ?? a)!;
    const delta = (b?.amt ?? 0n) - (a?.amt ?? 0n); if (delta !== 0n) deltas.push({ ...base, delta });
  }
  const ui = (v: bigint, dec: number) => Number(v) / 10 ** dec;
  const tokenTransfers: any[] = [];
  for (const mint of new Set(deltas.map((d) => d.mint))) {
    const outs = deltas.filter((d) => d.mint === mint && d.delta < 0).map((d) => ({ ...d, left: -d.delta }));
    const ins = deltas.filter((d) => d.mint === mint && d.delta > 0).map((d) => ({ ...d, left: d.delta }));
    for (const i of ins) for (const o of outs) {
      if (i.left === 0n) break; if (o.left === 0n) continue;
      const amt = i.left < o.left ? i.left : o.left; i.left -= amt; o.left -= amt;
      tokenTransfers.push({ fromUserAccount: o.owner, toUserAccount: i.owner, fromTokenAccount: keys[o.idx], toTokenAccount: keys[i.idx], tokenAmount: ui(amt, i.decimals), mint,
        tokenStandard: i.decimals === 0 && amt === 1n ? "NonFungible" : "Fungible" });
    }
  }
  const accountData = keys.map((account, i) => ({ account, nativeBalanceChange: Number(meta.postBalances?.[i] ?? 0) - Number(meta.preBalances?.[i] ?? 0),
    tokenBalanceChanges: deltas.filter((d) => d.idx === i).map((d) => ({ userAccount: d.owner, tokenAccount: account, mint: d.mint, rawTokenAmount: { tokenAmount: d.delta.toString(), decimals: d.decimals } })) }));

  let type = "UNKNOWN"; let source = "UNKNOWN"; const events: any = {}; let description = "";
  for (const p of programs) if (SOURCES[p as string] && p !== SYSTEM) { source = SOURCES[p as string]; break; }
  // compressed NFTs (all Bubblegum V1/V2 ops)
  const cnftEvents: any[] = [];
  for (const ix of flat) if (keys[ix.programIdIndex] === BUBBLEGUM) {
    const d = data(ix).subarray(0, 8); const name = Object.keys(DISC).find((k) => d.equals(DISC[k])); const t = name && CNFT_TYPES[name];
    if (!t) continue;
    const [ti, oi] = CNFT_ACC[name!] ?? [-1, null];
    cnftEvents.push({ type: t, treeId: ti >= 0 ? keys[ix.accounts[ti]] : null, newLeafOwner: oi == null ? null : keys[ix.accounts[oi]], instruction: name });
  }
  if (cnftEvents.length) { type = cnftEvents[0].type; source = "BUBBLEGUM"; events.compressed = cnftEvents; }
  // token-program / stake / system instruction classification
  const tokIx = (ix: any) => TOKEN_PROGRAMS.has(keys[ix.programIdIndex]) ? data(ix) : null;
  const mints: any[] = [], burns: any[] = [];
  const tbByIdx = new Map<number, TB>([...pre, ...post]);
  const acctInfo = (k: string) => { const i = keys.indexOf(k); return tbByIdx.get(i); };
  for (const ix of flat) {
    const d = tokIx(ix); if (!d || !d.length) continue;
    const amt = d.length >= 9 ? d.readBigUInt64LE(1) : 0n;
    if (d[0] === 7 || d[0] === 14) { const mint = keys[ix.accounts[0]], to = keys[ix.accounts[1]]; const dec = d[0] === 14 ? d[9] : acctInfo(to)?.decimals ?? 0; mints.push({ mint, toTokenAccount: to, toUserAccount: acctInfo(to)?.owner ?? "", tokenAmount: ui(amt, dec) }); }
    if (d[0] === 8 || d[0] === 15) { const from = keys[ix.accounts[0]], mint = keys[ix.accounts[1]]; const dec = d[0] === 15 ? d[9] : acctInfo(from)?.decimals ?? 0; burns.push({ mint, fromTokenAccount: from, fromUserAccount: acctInfo(from)?.owner ?? "", tokenAmount: ui(amt, dec) }); }
  }
  if (type === "UNKNOWN") for (const ix of top) {
    const prog = keys[ix.programIdIndex]; const d = data(ix);
    const m = MARKET_IX.find((x) => x.progs.includes(prog) && d.subarray(0, 8).equals(x.d));
    if (m) { type = m.type; source = SOURCES[prog]; events.nft = { type: m.type, source, instruction: m.name, feePayer, signature: sig, slot: r.slot, timestamp: r.blockTime ?? null, accounts: (ix.accounts ?? []).map((a: number) => keys[a]) }; description = `${feePayer} ${m.type.toLowerCase().replace(/_/g, " ")} on ${source}.`; break; }
    if (prog === STAKE_PROGRAM && d.length >= 4) {
      const v = d.readUInt32LE(0);
      const st = v === 2 ? "STAKE_SOL" : v === 5 ? "UNSTAKE_SOL" : v === 4 ? "WITHDRAW" : "";
      if (st) { type = st; source = "STAKE_PROGRAM"; const stake = keys[ix.accounts[0]];
        description = st === "STAKE_SOL" ? `${feePayer} staked ${stake} to ${keys[ix.accounts[1]]}.` : st === "UNSTAKE_SOL" ? `${feePayer} deactivated stake ${stake}.` : `${feePayer} withdrew ${Number(d.readBigUInt64LE(4)) / SOL} SOL from ${stake}.`;
        events.stake = { type: st, stakeAccount: stake, ...(st === "STAKE_SOL" ? { voteAccount: keys[ix.accounts[1]] } : {}), ...(st === "WITHDRAW" ? { amount: Number(d.readBigUInt64LE(4)), to: keys[ix.accounts[1]] } : {}) }; break; }
    }
  }
  const nft = tokenTransfers.find((t) => t.tokenStandard === "NonFungible");
  const nativeDelta = (a: string) => accountData.find((x) => x.account === a)?.nativeBalanceChange ?? 0;
  if (type === "UNKNOWN" && nft) {
    const buyer = nft.toUserAccount, seller = nft.fromUserAccount;
    const paid = -(nativeDelta(buyer) + (buyer === feePayer ? meta.fee ?? 0 : 0));
    const wsol = tokenTransfers.find((t) => t.mint === "So11111111111111111111111111111111111111112" && t.fromUserAccount === buyer && t.toUserAccount === seller);
    const amount = wsol ? Math.round(wsol.tokenAmount * SOL) : paid;
    if (amount > 0 && (nativeDelta(seller) > 0 || wsol)) {
      type = "NFT_SALE";
      events.nft = { type: "NFT_SALE", source, amount, fee: meta.fee ?? 0, feePayer, signature: sig, slot: r.slot, timestamp: r.blockTime ?? null, saleType: "INSTANT_SALE", buyer, seller, nfts: [{ mint: nft.mint, tokenStandard: "NonFungible" }] };
      description = `${seller} sold ${nft.mint} to ${buyer} for ${amount / SOL} SOL on ${source}.`;
    }
  }
  if (type === "UNKNOWN" && mints.length && !programs.has(JUPITER) && ![...programs].some((p) => DEX.has(p as string))) {
    type = "TOKEN_MINT"; events.tokenMint = mints; source = [...programs].map((p) => SOURCES[p as string]).find((x) => x && x !== "SYSTEM_PROGRAM") ?? "SPL_TOKEN";
    description = `${feePayer} minted ${mints.map((m) => `${m.tokenAmount} ${m.mint}`).join(", ")}.`;
  }
  if (type === "UNKNOWN" && burns.length && !mints.length && ![...programs].some((p) => DEX.has(p as string))) {
    type = "BURN"; events.burn = burns; source = "SPL_TOKEN";
    description = `${burns[0].fromUserAccount} burned ${burns.map((b) => `${b.tokenAmount} ${b.mint}`).join(", ")}.`;
  }
  const innerSwaps = parseInnerSwaps(r, keys, flat, tbByIdx, data);
  if (type === "UNKNOWN") {
    // swap: the fee payer's (or signer's) net SOL / token changes go out in one asset and in in another
    const user = feePayer;
    const userDeltas = new Map<string, number>();
    for (const d of deltas) if (d.owner === user) userDeltas.set(d.mint, (userDeltas.get(d.mint) ?? 0) + ui(d.delta, d.decimals));
    const sol = nativeDelta(user) + (meta.fee ?? 0);
    const outs = [...userDeltas].filter(([, v]) => v < 0), ins = [...userDeltas].filter(([, v]) => v > 0);
    const solOut = sol < -10_000 ? -sol : 0, solIn = sol > 10_000 ? sol : 0; // ignore rent dust
    const hasOut = outs.length || solOut, hasIn = ins.length || solIn;
    if (innerSwaps.length || (hasOut && hasIn && (ins.length + outs.length >= 1) && (outs.some(([m]) => !ins.some(([n]) => n === m)) || solOut || solIn))) {
      type = "SWAP";
      events.swap = { nativeInput: solOut ? { account: user, amount: String(solOut) } : null, nativeOutput: solIn ? { account: user, amount: String(solIn) } : null,
        tokenInputs: outs.map(([mint, v]) => ({ userAccount: user, mint, tokenAmount: -v })), tokenOutputs: ins.map(([mint, v]) => ({ userAccount: user, mint, tokenAmount: v })), tokenFees: [], nativeFees: [], innerSwaps };
      if (innerSwaps.length) { const f = innerSwaps[0], l = innerSwaps.at(-1); events.swap.tokenIn = f.tokenInputs[0] ?? null; events.swap.tokenOut = l.tokenOutputs[0] ?? null; }
      if (flat.some((ix) => keys[ix.programIdIndex] === JUPITER)) source = "JUPITER";
      description = `${user} swapped ${solOut ? `${solOut / SOL} SOL` : outs.map(([m, v]) => `${-v} ${m}`).join(", ")} for ${solIn ? `${solIn / SOL} SOL` : ins.map(([m, v]) => `${v} ${m}`).join(", ")}${source !== "UNKNOWN" ? ` on ${source}` : ""}.`;
    }
  }
  if (type === "UNKNOWN" && [...programs].every((p) => BENIGN.has(p as string))) {
    const created: any[] = [];
    for (const ix of flat) {
      const prog = keys[ix.programIdIndex]; const d = data(ix);
      if (prog === SYSTEM && d.length === 52 && d.readUInt32LE(0) === 0) created.push({ account: keys[ix.accounts[1]], lamports: Number(d.readBigUInt64LE(4)), space: Number(d.readBigUInt64LE(12)), owner: b58encode(d.subarray(20, 52)) });
      else if (prog === "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL" && (d.length === 0 || d[0] <= 1)) created.push({ account: keys[ix.accounts[1]], owner: keys[ix.accounts[2]], mint: keys[ix.accounts[3]], associatedTokenAccount: true });
    }
    const onlyCreate = created.length && !tokenTransfers.length && nativeTransfers.every((t) => created.some((c) => c.account === t.toUserAccount));
    if (onlyCreate) { type = "CREATE_ACCOUNT"; source = "SYSTEM_PROGRAM"; events.createAccount = created; description = `${feePayer} created ${created.map((c) => c.account).join(", ")}.`; }
  }
  if (type === "UNKNOWN" && (nativeTransfers.length || tokenTransfers.length) && [...programs].every((p) => BENIGN.has(p as string))) {
    type = "TRANSFER"; source = "SYSTEM_PROGRAM";
    const t = nativeTransfers[0]; const k = tokenTransfers[0];
    description = t ? `${t.fromUserAccount} transferred ${t.amount / SOL} SOL to ${t.toUserAccount}.` : `${k.fromUserAccount} transferred ${k.tokenAmount} ${k.mint} to ${k.toUserAccount}.`;
  }
  return {
    description, type, source, fee: meta.fee ?? 0, feePayer, signature: sig, slot: r.slot, timestamp: r.blockTime ?? null,
    nativeTransfers, tokenTransfers, accountData, transactionError: meta.err ?? null,
    instructions: top.map((ix: any, i: number) => ({ programId: keys[ix.programIdIndex], accounts: (ix.accounts ?? []).map((a: number) => keys[a]), data: ix.data,
      innerInstructions: (inner.get(i) ?? []).map((x: any) => ({ programId: keys[x.programIdIndex], accounts: (x.accounts ?? []).map((a: number) => keys[a]), data: x.data })) })),
    events,
  };
}

/** Per-hop swaps: Jupiter SwapEvent CPI events (exact), else each DEX invocation's inner SPL transfers (first = in, last = out). */
function parseInnerSwaps(r: any, keys: string[], flat: any[], tb: Map<number, { mint: string; owner: string; decimals: number }>, data: (ix: any) => Buffer) {
  const out: any[] = [];
  const tbk = (k: string) => tb.get(keys.indexOf(k));
  const dec = (mint: string) => [...tb.values()].find((x) => x.mint === mint)?.decimals;
  const amt = (v: bigint, m: string) => { const x = dec(m); return x == null ? { rawTokenAmount: v.toString() } : { tokenAmount: Number(v) / 10 ** x, rawTokenAmount: v.toString() }; };
  const dexSeq = flat.map((ix) => keys[ix.programIdIndex]).filter((p) => DEX.has(p));
  for (const ix of flat) {
    if (keys[ix.programIdIndex] !== JUPITER) continue;
    const d = data(ix);
    if (d.length >= 20 && d.subarray(0, 8).equals(EVENT_IX_TAG) && d.subarray(8, 16).equals(JUP_SWAPS_EVENT)) {
      const n = d.readUInt32LE(16); let o = 20;
      for (let i = 0; i < n && o + 80 <= d.length; i++) {
        const inMint = b58encode(d.subarray(o, o + 32)), inAmt = d.readBigUInt64LE(o + 32), outMint = b58encode(d.subarray(o + 40, o + 72)), outAmt = d.readBigUInt64LE(o + 72); o += 80;
        const prog = dexSeq[out.length];
        out.push({ tokenInputs: [{ mint: inMint, ...amt(inAmt, inMint) }], tokenOutputs: [{ mint: outMint, ...amt(outAmt, outMint) }], tokenFees: [], nativeFees: [],
          programInfo: { source: (prog && SOURCES[prog]) ?? "JUPITER", account: prog ?? JUPITER, programName: (prog && SOURCES[prog]) ?? "UNKNOWN", instructionName: "SwapsEvent" } });
      }
      continue;
    }
    if (d.length >= 16 + 32 * 3 + 16 && d.subarray(0, 8).equals(EVENT_IX_TAG) && d.subarray(8, 16).equals(JUP_SWAP_EVENT)) {
      let o = 16; const pk = () => { const k = b58encode(d.subarray(o, o + 32)); o += 32; return k; };
      const amm = pk(), inMint = pk(); const inAmt = d.readBigUInt64LE(o); o += 8; const outMint = pk(); const outAmt = d.readBigUInt64LE(o);
      out.push({ tokenInputs: [{ mint: inMint, ...amt(inAmt, inMint) }], tokenOutputs: [{ mint: outMint, ...amt(outAmt, outMint) }], tokenFees: [], nativeFees: [],
        programInfo: { source: SOURCES[amm] ?? "JUPITER", account: amm, programName: SOURCES[amm] ?? "UNKNOWN", instructionName: "SwapEvent" } });
    }
  }
  if (out.length) return out;
  let hop: any;
  for (const ix of flat) {
    const prog = keys[ix.programIdIndex];
    if (DEX.has(prog)) { hop = { prog, transfers: [] as any[] }; out.push(hop); continue; }
    if (!hop || !TOKEN_PROGRAMS.has(prog)) continue;
    const d = data(ix); if (!(d[0] === 3 || d[0] === 12) || d.length < 9) continue;
    const from = keys[ix.accounts[0]], to = keys[ix.accounts[d[0] === 3 ? 1 : 2]]; const amount = d.readBigUInt64LE(1);
    const fi = tbk(from), ti = tbk(to); const mint = d[0] === 12 ? keys[ix.accounts[1]] : fi?.mint ?? ti?.mint ?? ""; const decimals = d[0] === 12 ? d[9] : fi?.decimals ?? ti?.decimals ?? 0;
    hop.transfers.push({ fromUserAccount: fi?.owner ?? "", toUserAccount: ti?.owner ?? "", fromTokenAccount: from, toTokenAccount: to, tokenAmount: Number(amount) / 10 ** decimals, rawTokenAmount: amount.toString(), mint });
  }
  return out.filter((h) => h.transfers?.length >= 2).map((h) => ({ tokenInputs: [h.transfers[0]], tokenOutputs: [h.transfers.at(-1)], tokenFees: [], nativeFees: [],
    programInfo: { source: SOURCES[h.prog] ?? "UNKNOWN", account: h.prog, programName: SOURCES[h.prog] ?? "UNKNOWN", instructionName: "swap" } }));
}
