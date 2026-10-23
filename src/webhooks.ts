// Helius-like webhooks (raw): REST CRUD, persisted to JSON, backed by TxWatcher, POST delivery w/ retries.
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { StreamHub } from "./stream.js";
import { TxWatcher, type GetTx } from "./txstream.js";
import { isPubkey } from "./codec.js";

export interface Webhook { webhookID: string; wallet: string; webhookURL: string; transactionTypes: string[]; accountAddresses: string[]; webhookType: "raw" | "rawDevnet"; authHeader?: string; txnStatus?: "all" | "success" | "failed" }
export interface WebhookOptions { file?: string; maxRetries?: number; baseDelayMs?: number; timeoutMs?: number; maxPerKey?: number; log?: (...a: any[]) => void }

export class WebhookError extends Error { constructor(public status: number, msg: string) { super(msg); } }

export class WebhookManager {
  private hooks = new Map<string, Webhook>();
  private watchers = new Map<string, TxWatcher>();
  stats = new Map<string, { delivered: number; failed: number; retries: number; lastError?: string }>();
  private o: Required<Omit<WebhookOptions, "file" | "log">> & { file?: string; log: (...a: any[]) => void };
  private pending = new Set<Promise<void>>();

  constructor(private hub: StreamHub, private getTx: GetTx, opts: WebhookOptions = {}) {
    this.o = { file: opts.file ?? process.env.WEBHOOKS_FILE, maxRetries: opts.maxRetries ?? 5, baseDelayMs: opts.baseDelayMs ?? 1000,
      timeoutMs: opts.timeoutMs ?? 10_000, maxPerKey: opts.maxPerKey ?? 50, log: opts.log ?? (() => {}) };
    if (this.o.file && fs.existsSync(this.o.file)) {
      try { for (const h of JSON.parse(fs.readFileSync(this.o.file, "utf8")) as Webhook[]) { this.hooks.set(h.webhookID, h); this.start(h); } }
      catch (e: any) { this.o.log("[webhooks] failed to load", e.message); }
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
      if (!["raw", "rawDevnet"].includes(b.webhookType ?? "raw")) throw new WebhookError(400, "only webhookType raw/rawDevnet is supported (no enhanced parsing)");
      out.webhookType = b.webhookType ?? "raw";
    }
    if (b.transactionTypes !== undefined || !partial) {
      const t = b.transactionTypes ?? ["ANY"];
      if (!Array.isArray(t) || !t.every((x) => typeof x === "string")) throw new WebhookError(400, "transactionTypes must be a string array");
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
  delete(wallet: string, id: string) { this.get(wallet, id); this.stop(id); this.hooks.delete(id); this.stats.delete(id); this.save(); }

  private start(h: Webhook) {
    const failed = h.txnStatus === "success" ? false : undefined;
    const w = new TxWatcher(this.hub, this.getTx, { accountInclude: h.accountAddresses, failed, vote: false }, { encoding: "json" }, (e) => {
      if (h.txnStatus === "failed" && !e.tx.meta?.err) return;
      const p = this.deliver(h, [e.tx]); this.pending.add(p); p.finally(() => this.pending.delete(p));
    });
    this.watchers.set(h.webhookID, w);
  }
  private stop(id: string) { this.watchers.get(id)?.stop(); this.watchers.delete(id); }

  /** POST payload (Helius raw webhooks send an array of transactions); exponential backoff on failure/non-2xx. */
  private async deliver(h: Webhook, payload: any[]) {
    const st = this.stats.get(h.webhookID) ?? { delivered: 0, failed: 0, retries: 0 }; this.stats.set(h.webhookID, st);
    const body = JSON.stringify(payload);
    for (let attempt = 0; attempt <= this.o.maxRetries; attempt++) {
      if (!this.hooks.has(h.webhookID)) return;
      try {
        const r = await fetch(h.webhookURL, { method: "POST", body, signal: AbortSignal.timeout(this.o.timeoutMs),
          headers: { "content-type": "application/json", ...(h.authHeader ? { authorization: h.authHeader } : {}) } });
        await r.arrayBuffer().catch(() => {});
        if (r.ok) { st.delivered++; return; }
        st.lastError = `HTTP ${r.status}`;
        if (r.status >= 400 && r.status < 500 && r.status !== 408 && r.status !== 429) break; // permanent
      } catch (e: any) { st.lastError = e?.message ?? String(e); }
      if (attempt < this.o.maxRetries) { st.retries++; await new Promise((res) => setTimeout(res, this.o.baseDelayMs * 2 ** attempt)); }
    }
    st.failed++; this.o.log(`[webhooks] delivery failed ${h.webhookID}: ${st.lastError}`);
  }
  async idle() { await Promise.all([...this.pending]); }
  close() { for (const id of [...this.watchers.keys()]) this.stop(id); }
}
