// Chain timing + consensus awareness (Alpenglow-ready). Nothing here assumes 400ms slots or 32-slot finality:
// slot duration is configured (SLOT_MS) and then refined from observed slot notifications; Alpenglow is
// detected via the official getAgGenesisCert RPC (Agave 4.3+), never by date or version string.
// Source: https://solana.com/upgrades/alpenglow
import { RpcError } from "./errors.js";

export type Commitment = "processed" | "confirmed" | "finalized";
export type ConsensusState = { consensus: "alpenglow"; genesisSlot: number | null } | { consensus: "towerbft" } | { consensus: "unknown"; reason: string };

export interface ChainOptions {
  /** initial slot duration guess; refined from observed slots. Default SLOT_MS env or 200 (mainnet after the Oct 2026 slot-time reduction). */
  slotMs?: number;
  /** clamp for the observed estimate */
  minSlotMs?: number; maxSlotMs?: number;
  /** default commitment for internal subscriptions/fetches. Default DEFAULT_COMMITMENT env, else "finalized" once Alpenglow is detected, else "confirmed". */
  defaultCommitment?: Commitment;
  /** rewrite client "confirmed" to "finalized" (they describe the same state under Alpenglow). Default ALPENGLOW_CONFIRMED_AS_FINALIZED=1, off otherwise. */
  confirmedAsFinalized?: boolean;
}

export class ChainState {
  slotMs: number;
  samples = 0;
  consensus: ConsensusState = { consensus: "unknown", reason: "not checked" };
  private last?: { slot: number; t: number };
  private readonly o: Required<Omit<ChainOptions, "defaultCommitment">> & { defaultCommitment?: Commitment };
  constructor(opts: ChainOptions = {}) {
    const env = process.env;
    this.o = {
      slotMs: opts.slotMs ?? Number(env.SLOT_MS ?? 200),
      minSlotMs: opts.minSlotMs ?? 20, maxSlotMs: opts.maxSlotMs ?? 2000,
      defaultCommitment: opts.defaultCommitment ?? (env.DEFAULT_COMMITMENT as Commitment | undefined),
      confirmedAsFinalized: opts.confirmedAsFinalized ?? env.ALPENGLOW_CONFIRMED_AS_FINALIZED === "1",
    };
    this.slotMs = this.o.slotMs;
  }
  /** Feed a slot observation (from slotSubscribe or any slot-bearing response). EWMA over per-slot deltas. */
  observe(slot: number, t = Date.now()) {
    if (!Number.isFinite(slot)) return;
    const prev = this.last;
    if (!prev || slot > prev.slot) this.last = { slot, t };
    if (!prev || slot <= prev.slot) return;
    const per = (t - prev.t) / (slot - prev.slot);
    if (per < this.o.minSlotMs || per > this.o.maxSlotMs) return; // gaps/outages/bursts: ignore
    this.slotMs = this.samples ? this.slotMs * 0.9 + per * 0.1 : per;
    this.samples++;
  }
  /** Duration of n slots (ms), floored at minMs. Use for TTLs/retries instead of hardcoded 400ms. */
  slots(n: number, minMs = 50) { return Math.max(minMs, Math.round(n * this.slotMs)); }
  get alpenglow() { return this.consensus.consensus === "alpenglow"; }
  get defaultCommitment(): Commitment { return this.o.defaultCommitment ?? (this.alpenglow ? "finalized" : "confirmed"); }
  /** Optional rewrite of a client commitment (only when explicitly enabled AND Alpenglow is confirmed active). */
  mapCommitment<T extends string | undefined>(c: T): T | "finalized" {
    return c === "confirmed" && this.o.confirmedAsFinalized && this.alpenglow ? "finalized" : c;
  }
  /** Detect via getAgGenesisCert: null => TowerBFT, object => Alpenglow, -32601 => node too old (unknown). */
  async detect(up: (m: string, p: any[], ttl?: number) => Promise<any>) {
    try {
      const cert = await up("getAgGenesisCert", [], 0);
      this.consensus = cert == null ? { consensus: "towerbft" }
        : { consensus: "alpenglow", genesisSlot: Number.isFinite(cert?.block?.slot) ? cert.block.slot : null };
    } catch (e: any) {
      this.consensus = { consensus: "unknown", reason: e instanceof RpcError && e.code === -32601 ? "upstream lacks getAgGenesisCert (pre-Agave 4.3)" : `detect failed: ${e?.message ?? e}` };
    }
    return this.consensus;
  }
  status() { return { ...this.consensus, slotMs: Math.round(this.slotMs * 10) / 10, slotSamples: this.samples, defaultCommitment: this.defaultCommitment, confirmedAsFinalized: this.o.confirmedAsFinalized }; }
}
