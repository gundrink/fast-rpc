// Helius-like webhooks (raw + enhanced): REST CRUD, persisted to JSON, backed by TxWatcher, POST delivery w/ retries.
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { StreamHub } from "./stream.js";
import { TxWatcher, type GetTx } from "./txstream.js";
import { isPubkey } from "./codec.js";
import { parseEnhanced, TX_TYPES } from "./enhanced.js";

export interface Webhook { webhookID: string; wallet: string; webhookURL: string; transactionTypes: string[]; accountAddresses: string[]; webhookType: "raw" | "rawDevnet" | "enhanced" | "enhancedDevnet"; authHeader?: string; txnStatus?: "all" | "success" | "failed" }
export interface WebhookOptions {
  /** JSON-RPC caller used for restart backfill (getSignaturesForAddress + getTransaction) */
  rpc?: (method: string, params: any[]) => Promise<any>;
  /** max signatures per address fetched on restart backfill, paginated with `before` (default 5000, WEBHOOK_BACKFILL_MAX) */
  backfillLimit?: number;
  /** batching: max txs per POST (default 100, WEBHOOK_BATCH_MAX) and flush delay (default 0 = next tick, WEBHOOK_FLUSH_MS) */
  maxBatch?: number; flushMs?: number;
  file?: string; maxRetries?: number; baseDelayMs?: number; timeoutMs?: number; maxPerKey?: number; log?: (...a: any[]) => void }

export class WebhookError extends Error { constructor(public status: number, msg: string) { super(msg); } }

export class WebhookManager {
  private hooks = new Map<string, Webhook>();
  private watchers = new Map<string, TxWatcher>();
  stats = new Map<string, { delivered: number; failed: number; retries: number; lastError?: string; lastSlot?: number; backfilled?: number }>();
  private o: Required<Omit<WebhookOptions, "file" | "log" | "rpc">> & { file?: string; log: (...a: any[]) => void; rpc?: WebhookOptions["rpc"] };
  private stateT?: NodeJS.Timeout;
  private delivered = new Map<string, Set<string>>();
  /** resolves when restart backfill is done */
  ready: Promise<void> = Promise.resolve();
  private pending = new Set<Promise<void>>();
  private batches = new Map<string, { items: any[]; t?: NodeJS.Timeout }>();

  constructor(private hub: StreamHub, private getTx: GetTx, opts: WebhookOptions = {}) {
    this.o = { file: opts.file ?? process.env.WEBHOOKS_FILE, maxRetries: opts.maxRetries ?? 5, baseDelayMs: opts.baseDelayMs ?? 1000,
      timeoutMs: opts.timeoutMs ?? 10_000, maxPerKey: opts.maxPerKey ?? 50, log: opts.log ?? (() => {}), rpc: opts.rpc,
      backfillLimit: opts.backfillLimit ?? Number(process.env.WEBHOOK_BACKFILL_MAX ?? 5000), maxBatch: opts.maxBatch ?? Number(process.env.WEBHOOK_BATCH_MAX ?? 100), flushMs: opts.flushMs ?? Number(process.env.WEBHOOK_FLUSH_MS ?? 0) };
    if (this.o.file && fs.existsSync(this.o.file)) {
      try {
        for (const h of JSON.parse(fs.readFileSync(this.o.file, "utf8")) as Webhook[]) this.hooks.set(h.webhookID, h);
        const sf = `${this.o.file}.state.json`;
        if (fs.existsSync(sf)) for (const [id, st] of Object.entries<any>(JSON.parse(fs.readFileSync(sf, "utf8")))) if (this.hooks.has(id)) this.stats.set(id, st);
        for (const h of this.hooks.values()) this.start(h);
        this.ready = Promise.all([...this.hooks.values()].map((h) => this.backfill(h).catch((e) => this.o.log("[webhooks] backfill failed", h.webhookID, e?.message)))).then(() => {});
      }
      catch (e: any) { this.o.log("[webhooks] failed to load", e.message); }
    }
  }

  private saveState() {
    if (!this.o.file || this.stateT) return;
    this.stateT = setTimeout(() => { this.stateT = undefined; this.writeState(); }, 200); this.stateT.unref();
  }
  private writeState() {
    if (!this.o.file) return;
    fs.mkdirSync(path.dirname(path.resolve(this.o.file)), { recursive: true });
    const f = `${this.o.file}.state.json`;
    fs.writeFileSync(`${f}.tmp`, JSON.stringify(Object.fromEntries(this.stats))); fs.renameSync(`${f}.tmp`, f);
  }
  private st(id: string) { let s = this.stats.get(id); if (!s) { s = { delivered: 0, failed: 0, retries: 0 }; this.stats.set(id, s); } return s; }

  /** Restart catch-up: txs for the hook's addresses with slot > lastSlot, oldest first (bounded by backfillLimit/address). */
  private async backfill(h: Webhook) {
    const st = this.stats.get(h.webhookID); const from = st?.lastSlot; const rpc = this.o.rpc;
    if (!rpc || !from) return;
    const sigs = new Map<string, number>();
    for (const a of h.accountAddresses) {
      let before: string | undefined; let got = 0;
      while (got < this.o.backfillLimit) {
        const limit = Math.min(1000, this.o.backfillLimit - got);
        const page: any[] = await rpc("getSignaturesForAddress", [a, { limit, commitment: "confirmed", ...(before ? { before } : {}) }]);
        if (!page?.length) break;
        got += page.length; let reached = false;
        for (const s of page) {
          if (s.slot <= from) { reached = true; continue; }
          if (!(h.txnStatus === "success" && s.err) && !(h.txnStatus === "failed" && !s.err)) sigs.set(s.signature, s.slot);
        }
        if (reached || page.length < limit) break;
        before = page.at(-1).signature;
      }
    }
    const ordered = [...sigs].sort((x, y) => x[1] - y[1]);
    let n = 0;
    for (const [sig] of ordered) {
      const r = await rpc("getTransaction", [sig, { encoding: "json", maxSupportedTransactionVersion: 0, commitment: "confirmed" }]).catch(() => null);
      if (r && this.handle(h, r)) n++;
    }
    this.st(h.webhookID).backfilled = (this.st(h.webhookID).backfilled ?? 0) + n; this.saveState();
  }

  /** filter + deliver one tx; returns true if queued for delivery */
  private handle(h: Webhook, tx: any): boolean {
    if (h.txnStatus === "failed" && !tx.meta?.err) return false;
    if (h.txnStatus === "success" && tx.meta?.err) return false;
    const sig = tx.transaction?.signatures?.[0];
    let seen = this.delivered.get(h.webhookID); if (!seen) { seen = new Set(); this.delivered.set(h.webhookID, seen); }
    if (sig) { if (seen.has(sig)) return false; seen.add(sig); if (seen.size > 10_000) seen.delete(seen.values().next().value!); }
    const enhanced = h.webhookType.startsWith("enhanced");
    const types = h.transactionTypes ?? ["ANY"];
    let payload: any = tx;
    if (enhanced || !types.includes("ANY")) {
      const e = parseEnhanced(tx);
      if (!types.includes("ANY") && !types.includes(e.type)) return false;
      if (enhanced) payload = e;
    }
    const s = this.st(h.webhookID); s.lastSlot = Math.max(s.lastSlot ?? 0, tx.slot ?? 0); this.saveState();
    this.enqueue(h, payload);
    return true;
  }
  private enqueue(h: Webhook, payload: any) {
    let b = this.batches.get(h.webhookID); if (!b) { b = { items: [] }; this.batches.set(h.webhookID, b); }
    b.items.push(payload);
    if (b.items.length >= this.o.maxBatch) return this.flush(h);
    if (!b.t) { b.t = setTimeout(() => this.flush(h), this.o.flushMs); }
  }
  private flush(h: Webhook) {
    const b = this.batches.get(h.webhookID); if (!b) return;
    if (b.t) { clearTimeout(b.t); b.t = undefined; }
    while (b.items.length) {
      const items = b.items.splice(0, this.o.maxBatch);
      const p = this.deliver(h, items); this.pending.add(p); p.finally(() => this.pending.delete(p));
    }
  }

  private save() {
    if (!this.o.file) return;
    fs.mkdirSync(path.dirname(path.resolve(this.o.file)), { recursive: true });
    const tmp = `${this.o.file}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify([...this.hooks.values()], null, 2));
    fs.renameSync(tmp, this.o.file);
  }

  private validate(b: any, partial = false): Partial<Webhook> {
    if (!b || typeof b !== "object") throw new WebhookError(400, "body must be a JSON object");
    const out: Partial<Webhook> = {};
    if (b.webhookURL !== undefined || !partial) {
      let u: URL; try { u = new URL(b.webhookURL); } catch { throw new WebhookError(400, "webhookURL must be a valid http(s) URL"); }
      if (!/^https?:$/.test(u.protocol)) throw new WebhookError(400, "webhookURL must be http(s)");
      out.webhookURL = b.webhookURL;
    }
    if (b.accountAddresses !== undefined || !partial) {
      if (!Array.isArray(b.accountAddresses) || !b.accountAddresses.length || b.accountAddresses.length > 100 || !b.accountAddresses.every(isPubkey))
        throw new WebhookError(400, "accountAddresses must be 1..100 base58 addresses");
      out.accountAddresses = [...new Set(b.accountAddresses as string[])];
    }
    if (b.webhookType !== undefined || !partial) {
      if (!["raw", "rawDevnet", "enhanced", "enhancedDevnet"].includes(b.webhookType ?? "raw")) throw new WebhookError(400, "webhookType must be raw|rawDevnet|enhanced|enhancedDevnet");
      out.webhookType = b.webhookType ?? "raw";
    }
    if (b.transactionTypes !== undefined || !partial) {
      const t = b.transactionTypes ?? ["ANY"];
      if (!Array.isArray(t) || !t.length || !t.every((x) => typeof x === "string")) throw new WebhookError(400, "transactionTypes must be a non-empty string array");
      const bad = t.filter((x: string) => !(TX_TYPES as readonly string[]).includes(x));
      if (bad.length) throw new WebhookError(400, `unsupported transactionTypes: ${bad.join(",")} (supported: ${TX_TYPES.join(",")})`);
      out.transactionTypes = t;
    }
    if (b.authHeader !== undefined) { if (typeof b.authHeader !== "string") throw new WebhookError(400, "authHeader must be a string"); out.authHeader = b.authHeader; }
    if (b.txnStatus !== undefined) { if (!["all", "success", "failed"].includes(b.txnStatus)) throw new WebhookError(400, "txnStatus must be all|success|failed"); out.txnStatus = b.txnStatus; }
    return out;
  }

  create(wallet: string, body: any): Webhook {
    if ([...this.hooks.values()].filter((h) => h.wallet === wallet).length >= this.o.maxPerKey) throw new WebhookError(429, "webhook limit reached");
    const h = { webhookID: randomUUID(), wallet, ...this.validate(body) } as Webhook;
    this.hooks.set(h.webhookID, h); this.save(); this.start(h);
    return h;
  }
  list(wallet: string) { return [...this.hooks.values()].filter((h) => h.wallet === wallet); }
  get(wallet: string, id: string) { const h = this.hooks.get(id); if (!h || h.wallet !== wallet) throw new WebhookError(404, "webhook not found"); return h; }
  update(wallet: string, id: string, body: any) {
    const h = { ...this.get(wallet, id), ...this.validate(body, true) };
    this.hooks.set(id, h); this.save(); this.stop(id); this.start(h);
    return h;
  }
  delete(wallet: string, id: string) { this.get(wallet, id); this.stop(id); this.hooks.delete(id); this.stats.delete(id); this.save(); this.saveState(); }

  private start(h: Webhook) {
    const failed = h.txnStatus === "success" ? false : undefined;
    const w = new TxWatcher(this.hub, this.getTx, { accountInclude: h.accountAddresses, failed, vote: false }, { encoding: "json" }, (e) => { this.handle(h, e.tx); });
    this.watchers.set(h.webhookID, w);
  }
  private stop(id: string) { this.watchers.get(id)?.stop(); this.watchers.delete(id); }

  /** POST payload (Helius raw webhooks send an array of transactions); exponential backoff on failure/non-2xx. */
  private async deliver(h: Webhook, payload: any[]) {
    const st = this.st(h.webhookID);
    const body = JSON.stringify(payload);
    for (let attempt = 0; attempt <= this.o.maxRetries; attempt++) {
      if (!this.hooks.has(h.webhookID)) return;
      try {
        const r = await fetch(h.webhookURL, { method: "POST", body, signal: AbortSignal.timeout(this.o.timeoutMs),
          headers: { "content-type": "application/json", ...(h.authHeader ? { authorization: h.authHeader } : {}) } });
        await r.arrayBuffer().catch(() => {});
        if (r.ok) { st.delivered += payload.length; (st as any).posts = ((st as any).posts ?? 0) + 1; this.saveState(); return; }
        st.lastError = `HTTP ${r.status}`;
        if (r.status >= 400 && r.status < 500 && r.status !== 408 && r.status !== 429) break; // permanent
      } catch (e: any) { st.lastError = e?.message ?? String(e); }
      if (attempt < this.o.maxRetries) { st.retries++; await new Promise((res) => setTimeout(res, this.o.baseDelayMs * 2 ** attempt)); }
    }
    st.failed += payload.length; this.saveState(); this.o.log(`[webhooks] delivery failed ${h.webhookID}: ${st.lastError}`);
  }
  async idle() { for (const id of this.batches.keys()) { const h = this.hooks.get(id); if (h) this.flush(h); } await Promise.all([...this.pending]); }
  close() { for (const b of this.batches.values()) if (b.t) clearTimeout(b.t); for (const id of [...this.watchers.keys()]) this.stop(id); if (this.stateT) { clearTimeout(this.stateT); this.stateT = undefined; this.writeState(); } }
}
