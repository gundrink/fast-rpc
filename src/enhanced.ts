// Best-effort Helius "enhanced transaction" parser: TRANSFER, SWAP (heuristic), NFT_SALE (minimal),
// COMPRESSED_NFT_MINT/TRANSFER/BURN; everything else UNKNOWN. Input: getTransaction(encoding:"json").
import { b58decode } from "./codec.js";
import { txAccountKeys } from "./txstream.js";
import { BUBBLEGUM, DISC } from "./bubblegum.js";

export const TX_TYPES = ["ANY", "UNKNOWN", "TRANSFER", "SWAP", "NFT_SALE", "COMPRESSED_NFT_MINT", "COMPRESSED_NFT_TRANSFER", "COMPRESSED_NFT_BURN"] as const;
const SYSTEM = "11111111111111111111111111111111";
const BENIGN = new Set([SYSTEM, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
  "ComputeBudget111111111111111111111111111111", "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr", "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo"]);
const SOURCES: Record<string, string> = {
  JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4: "JUPITER", "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "RAYDIUM", CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK: "RAYDIUM",
  CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C: "RAYDIUM", whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc: "ORCA", LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo: "METEORA",
  pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA: "PUMP_AMM", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "PUMP_FUN",
  M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K: "MAGIC_EDEN", TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN: "TENSOR", TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp: "TENSOR",
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
  // compressed NFTs
  for (const ix of flat) if (keys[ix.programIdIndex] === BUBBLEGUM) {
    const d = data(ix).subarray(0, 8);
    const t = d.equals(DISC.mintV1) || d.equals(DISC.mintToCollectionV1) ? "COMPRESSED_NFT_MINT" : d.equals(DISC.transfer) ? "COMPRESSED_NFT_TRANSFER" : d.equals(DISC.burn) ? "COMPRESSED_NFT_BURN" : "";
    if (t) { type = t; source = "BUBBLEGUM"; events.compressed = [{ type: t, treeId: keys[ix.accounts[t === "COMPRESSED_NFT_TRANSFER" ? 4 : 3]], newLeafOwner: t === "COMPRESSED_NFT_BURN" ? null : keys[ix.accounts[t === "COMPRESSED_NFT_TRANSFER" ? 3 : 1]] }]; break; }
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
  if (type === "UNKNOWN") {
    // swap: the fee payer's (or signer's) net SOL / token changes go out in one asset and in in another
    const user = feePayer;
    const userDeltas = new Map<string, number>();
    for (const d of deltas) if (d.owner === user) userDeltas.set(d.mint, (userDeltas.get(d.mint) ?? 0) + ui(d.delta, d.decimals));
    const sol = nativeDelta(user) + (meta.fee ?? 0);
    const outs = [...userDeltas].filter(([, v]) => v < 0), ins = [...userDeltas].filter(([, v]) => v > 0);
    const solOut = sol < -10_000 ? -sol : 0, solIn = sol > 10_000 ? sol : 0; // ignore rent dust
    const hasOut = outs.length || solOut, hasIn = ins.length || solIn;
    if (hasOut && hasIn && (ins.length + outs.length >= 1) && (outs.some(([m]) => !ins.some(([n]) => n === m)) || solOut || solIn)) {
      type = "SWAP";
      events.swap = { nativeInput: solOut ? { account: user, amount: String(solOut) } : null, nativeOutput: solIn ? { account: user, amount: String(solIn) } : null,
        tokenInputs: outs.map(([mint, v]) => ({ userAccount: user, mint, tokenAmount: -v })), tokenOutputs: ins.map(([mint, v]) => ({ userAccount: user, mint, tokenAmount: v })), tokenFees: [], nativeFees: [], innerSwaps: [] };
      description = `${user} swapped ${solOut ? `${solOut / SOL} SOL` : outs.map(([m, v]) => `${-v} ${m}`).join(", ")} for ${solIn ? `${solIn / SOL} SOL` : ins.map(([m, v]) => `${v} ${m}`).join(", ")}${source !== "UNKNOWN" ? ` on ${source}` : ""}.`;
    }
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
