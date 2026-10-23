// Disk-backed transaction recorder: hourly JSONL segments, retention window (default 24h).
// Feeds transactionSubscribe {fromSlot} replay for any filter (matching is done at replay time).
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

export interface RecordedTx { t: number; slot: number; sig: string; keys: string[]; failed: boolean; vote: boolean; tx: any }
export class TxRecorder {
  private cur?: { hour: number; fd: number }; private seen = new Set<string>(); private seenQ: string[] = [];
  stats = { written: 0, pruned: 0 };
  constructor(readonly dir: string, readonly retentionMs = 24 * 3600_000, private now = () => Date.now()) { fs.mkdirSync(dir, { recursive: true }); this.prune(); }
  private file(hour: number) { return path.join(this.dir, `tx-${hour}.jsonl`); }
  record(r: Omit<RecordedTx, "t">) {
    if (this.seen.has(r.sig)) return;
    this.seen.add(r.sig); this.seenQ.push(r.sig); if (this.seenQ.length > 50_000) this.seen.delete(this.seenQ.shift()!);
    const t = this.now(); const hour = Math.floor(t / 3600_000);
    if (this.cur?.hour !== hour) { if (this.cur) fs.closeSync(this.cur.fd); this.cur = { hour, fd: fs.openSync(this.file(hour), "a") }; this.prune(); }
    fs.writeSync(this.cur.fd, JSON.stringify({ t, ...r }) + "\n"); this.stats.written++;
  }
  segments() {
    return fs.readdirSync(this.dir).map((f) => /^tx-(\d+)\.jsonl$/.exec(f)).filter(Boolean).map((m) => Number(m![1])).sort((a, b) => a - b);
  }
  prune() {
    const min = Math.floor((this.now() - this.retentionMs) / 3600_000);
    for (const h of this.segments()) if (h < min) { try { fs.unlinkSync(this.file(h)); this.stats.pruned++; } catch {} }
  }
  /** stream records with slot >= fromSlot (and within retention), oldest first */
  async *replay(fromSlot: number): AsyncGenerator<RecordedTx> {
    const cutoff = this.now() - this.retentionMs;
    for (const h of this.segments()) {
      const rl = readline.createInterface({ input: fs.createReadStream(this.file(h)), crlfDelay: Infinity });
      for await (const line of rl) {
        if (!line) continue; let r: RecordedTx; try { r = JSON.parse(line); } catch { continue; }
        if (r.slot >= fromSlot && r.t >= cutoff) yield r;
      }
    }
  }
  close() { if (this.cur) fs.closeSync(this.cur.fd); this.cur = undefined; }
}
