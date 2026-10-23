// Off-chain metadata (json_uri) fetcher for DAS content: timeout, size cap, LRU+TTL cache, ipfs/ar gateway rewrite,
// SSRF guard (private/loopback hosts blocked unless allowPrivate), bounded concurrency.
import dns from "node:dns/promises";
import net from "node:net";

export interface JsonFetchOptions { timeoutMs?: number; maxBytes?: number; ttlMs?: number; maxEntries?: number; concurrency?: number; allowPrivate?: boolean; ipfsGateway?: string; arweaveGateway?: string; fetch?: typeof fetch }

export function isPrivateIp(ip: string): boolean {
  if (net.isIPv4(ip)) {
    const [a, b] = ip.split(".").map(Number);
    return a === 10 || a === 127 || a === 0 || (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || (a === 100 && b >= 64 && b <= 127) || a >= 224;
  }
  const l = ip.toLowerCase();
  if (l.startsWith("::ffff:")) return isPrivateIp(l.slice(7));
  return l === "::1" || l === "::" || l.startsWith("fc") || l.startsWith("fd") || l.startsWith("fe80");
}

export class JsonFetcher {
  private cache = new Map<string, { at: number; v: any }>();
  private inflight = new Map<string, Promise<any>>();
  private active = 0; private waiters: (() => void)[] = [];
  readonly o: Required<Omit<JsonFetchOptions, "fetch">> & { fetch: typeof fetch };
  stats = { hits: 0, fetched: 0, failed: 0, blocked: 0, tooLarge: 0 };
  constructor(o: JsonFetchOptions = {}) {
    const env = process.env;
    this.o = { timeoutMs: Number(env.DAS_JSON_TIMEOUT_MS ?? 3000), maxBytes: Number(env.DAS_JSON_MAX_BYTES ?? 512 * 1024), ttlMs: Number(env.DAS_JSON_TTL_MS ?? 3600_000),
      maxEntries: Number(env.DAS_JSON_CACHE ?? 5000), concurrency: Number(env.DAS_JSON_CONCURRENCY ?? 8), allowPrivate: env.DAS_JSON_ALLOW_PRIVATE === "1",
      ipfsGateway: env.DAS_IPFS_GATEWAY ?? "https://ipfs.io/ipfs/", arweaveGateway: env.DAS_ARWEAVE_GATEWAY ?? "https://arweave.net/", fetch: globalThis.fetch, ...o } as any;
  }
  rewrite(uri: string) {
    const u = uri.trim().replace(/\0/g, "");
    if (u.startsWith("ipfs://")) return this.o.ipfsGateway + u.slice(7).replace(/^ipfs\//, "");
    if (u.startsWith("ar://")) return this.o.arweaveGateway + u.slice(5);
    return u;
  }
  private async guard(url: URL) {
    if (!/^https?:$/.test(url.protocol)) throw new Error("unsupported scheme");
    if (this.o.allowPrivate) return;
    const host = url.hostname.replace(/^\[|\]$/g, "");
    const ips = net.isIP(host) ? [host] : (await dns.lookup(host, { all: true })).map((a) => a.address);
    if (ips.some(isPrivateIp)) { this.stats.blocked++; throw new Error("private address blocked"); }
  }
  private async slot() { if (this.active < this.o.concurrency) { this.active++; return; } await new Promise<void>((r) => this.waiters.push(r)); this.active++; }
  private release() { this.active--; this.waiters.shift()?.(); }
  /** JSON object or null (never throws) */
  async get(uri: string): Promise<any> {
    if (!uri) return null;
    const key = this.rewrite(uri);
    const c = this.cache.get(key);
    if (c && Date.now() - c.at < this.o.ttlMs) { this.stats.hits++; this.cache.delete(key); this.cache.set(key, c); return c.v; }
    let p = this.inflight.get(key);
    if (!p) { p = this.load(key).finally(() => this.inflight.delete(key)); this.inflight.set(key, p); }
    return p;
  }
  private async load(key: string) {
    await this.slot();
    let v: any = null;
    try {
      const url = new URL(key); await this.guard(url);
      const r = await this.o.fetch(url, { signal: AbortSignal.timeout(this.o.timeoutMs), redirect: "follow", headers: { accept: "application/json" } });
      if (!r.ok || !r.body) throw new Error(`HTTP ${r.status}`);
      if (Number(r.headers.get("content-length") ?? 0) > this.o.maxBytes) { this.stats.tooLarge++; throw new Error("too large"); }
      const chunks: Buffer[] = []; let n = 0;
      for await (const ch of r.body as any) { n += ch.length; if (n > this.o.maxBytes) { this.stats.tooLarge++; throw new Error("too large"); } chunks.push(Buffer.from(ch)); }
      v = JSON.parse(Buffer.concat(chunks).toString("utf8")); if (typeof v !== "object" || v === null) v = null;
      this.stats.fetched++;
    } catch { this.stats.failed++; v = null; }
    finally { this.release(); }
    this.cache.set(key, { at: Date.now(), v });
    while (this.cache.size > this.o.maxEntries) this.cache.delete(this.cache.keys().next().value!);
    return v;
  }
  /** fill DAS content (files, links.image, metadata.description/attributes) in place */
  async enrich(asset: any) {
    const uri = asset?.content?.json_uri; if (!uri) return asset;
    const j = await this.get(uri); if (!j) return asset;
    const c = asset.content;
    const image = typeof j.image === "string" ? j.image : undefined;
    const files = Array.isArray(j.properties?.files) ? j.properties.files.filter((f: any) => f && typeof f.uri === "string").map((f: any) => ({ uri: f.uri, mime: f.type ?? f.mime })) : [];
    if (image && !files.some((f: any) => f.uri === image)) files.unshift({ uri: image, mime: undefined });
    c.files = files.map((f: any) => (f.mime ? f : { uri: f.uri }));
    c.links = { ...c.links, ...(image ? { image } : {}), ...(typeof j.external_url === "string" ? { external_url: j.external_url } : {}), ...(typeof j.animation_url === "string" ? { animation_url: j.animation_url } : {}) };
    c.metadata = { ...c.metadata, ...(typeof j.description === "string" ? { description: j.description } : {}), ...(Array.isArray(j.attributes) ? { attributes: j.attributes } : {}),
      ...(j.name && !c.metadata?.name ? { name: j.name } : {}), ...(j.symbol && !c.metadata?.symbol ? { symbol: j.symbol } : {}) };
    return asset;
  }
}
