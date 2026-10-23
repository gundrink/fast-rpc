// Uncompressed collection index: getProgramAccounts(Token Metadata) memcmp on the verified collection field for the
// configured collections (DAS_COLLECTIONS). Offsets assume fixed-padded name/symbol/uri (Metaplex default:
// 32/10/200) and enumerate creators count x edition_nonce/token_standard Option combos.
import { b58decode, b58encode, isPubkey } from "./codec.js";
import { METADATA_PROGRAM } from "./das.js";

type Up = (method: string, params: any, ttl?: number) => Promise<any>;
const BASE = 1 + 32 + 32 + (4 + 32) + (4 + 10) + (4 + 200) + 2; // 321: creators Option tag

/** offsets of the collection Option tag for each creators/edition_nonce/token_standard layout */
export function collectionOffsets(maxCreators = 5) {
  const out: number[] = [];
  for (const creators of [-1, ...Array.from({ length: maxCreators + 1 }, (_, i) => i)]) {
    const afterCreators = BASE + 1 + (creators < 0 ? 0 : 4 + 34 * creators);
    for (const en of [1, 2]) for (const ts of [1, 2]) out.push(afterCreators + 2 + en + ts);
  }
  return [...new Set(out)].sort((a, b) => a - b);
}

export class CollectionIndex {
  members = new Map<string, Set<string>>();
  stats = { calls: 0, builtAt: 0 };
  private t?: NodeJS.Timeout;
  constructor(readonly collections = (process.env.DAS_COLLECTIONS ?? "").split(",").map((s) => s.trim()).filter(Boolean), private maxCreators = Number(process.env.DAS_COLLECTION_MAX_CREATORS ?? 5)) {
    for (const c of collections) if (!isPubkey(c)) throw new Error(`DAS_COLLECTIONS: bad mint ${c}`);
  }
  get enabled() { return this.collections.length > 0; }
  has(c: string) { return this.members.has(c); }
  async build(up: Up) {
    for (const c of this.collections) {
      const set = new Set<string>();
      const bytes = b58encode(Buffer.concat([Buffer.from([1, 1]), b58decode(c)]));
      for (const offset of collectionOffsets(this.maxCreators)) {
        this.stats.calls++;
        const r = await up("getProgramAccounts", [METADATA_PROGRAM, { encoding: "base64", dataSlice: { offset: 33, length: 32 }, filters: [{ memcmp: { offset: 0, bytes: "5" } }, { memcmp: { offset, bytes } }] }], 0);
        for (const a of (Array.isArray(r) ? r : r?.value) ?? []) { const d = a.account?.data?.[0]; if (d) set.add(b58encode(Buffer.from(d, "base64"))); }
      }
      this.members.set(c, set);
    }
    this.stats.builtAt = Date.now();
  }
  start(up: Up, refreshMs = Number(process.env.DAS_COLLECTIONS_REFRESH_MS ?? 3600_000), log?: (...a: any[]) => void) {
    if (!this.enabled || this.t) return;
    const run = () => this.build(up).catch((e) => log?.("[das] collection index failed", e?.message));
    void run(); this.t = setInterval(run, refreshMs); this.t.unref();
  }
  list(c: string) { return [...(this.members.get(c) ?? [])].sort(); }
  stop() { clearInterval(this.t); this.t = undefined; }
}
