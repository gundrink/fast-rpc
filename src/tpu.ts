// Direct-to-leader send: resolve the next leaders' TPU-QUIC addresses (getSlot + getSlotLeaders + getClusterNodes).
// Node has no stable built-in QUIC, so the actual wire transport is pluggable (TpuTransport). Without one this
// path is disabled and reported as such; tests use a mock transport. Unstaked QUIC is deprioritized by leaders (SWQoS).
type Call = (m: string, p: any[], ttl?: number) => Promise<any>;
export interface TpuTransport { send(addr: string, wire: Buffer): Promise<void> }
export interface TpuOptions { fanoutLeaders?: number; transport?: TpuTransport; nodesTtlMs?: number }

export function createTpuSender(call: Call, o: TpuOptions = {}) {
  const fanout = o.fanoutLeaders ?? Number(process.env.TPU_FANOUT ?? 4);
  let nodes: { at: number; map: Map<string, string> } | undefined;
  async function nodeMap() {
    if (nodes && Date.now() - nodes.at < (o.nodesTtlMs ?? 60_000)) return nodes.map;
    const list: any[] = await call("getClusterNodes", [], 0);
    const map = new Map<string, string>(); for (const n of list) if (n?.pubkey && n.tpuQuic) map.set(n.pubkey, n.tpuQuic);
    nodes = { at: Date.now(), map }; return map;
  }
  async function leaderAddrs() {
    const slot: number = await call("getSlot", [{ commitment: "processed" }], 0);
    const leaders: string[] = await call("getSlotLeaders", [slot, fanout * 4], 0); // 4 consecutive slots per leader
    const m = await nodeMap(); const seen = new Set<string>(); const out: { leader: string; addr: string }[] = [];
    for (const l of leaders) { if (seen.has(l)) continue; seen.add(l); const a = m.get(l); if (a) out.push({ leader: l, addr: a }); if (seen.size >= fanout) break; }
    return out;
  }
  return {
    enabled: !!o.transport, leaderAddrs,
    async send(wire: Buffer) {
      if (!o.transport) throw new Error("TPU transport not configured (no QUIC client in Node runtime)");
      const t = await leaderAddrs();
      const r = await Promise.allSettled(t.map((x) => o.transport!.send(x.addr, wire)));
      return { leaders: t.map((x, i) => ({ ...x, ok: r[i].status === "fulfilled" })) };
    },
  };
}
