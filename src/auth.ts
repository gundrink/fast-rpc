// Optional per-project API keys (API_KEYS / API_KEYS_FILE) + per-IP sliding-window limit, memory or Redis (REDIS_URL).
import fs from "node:fs";
import net from "node:net";
import type { KeyLimit } from "./limits.js";

export interface Project { key: string; project?: string; rps?: number; burst?: number; credits?: number }
/** API_KEYS_FILE: {"keys":[{"key":"...","project":"prolaunch","rps":50,"credits":1000000}]} */
export function loadKeys(file = process.env.API_KEYS_FILE): { keys: string[]; limits: Record<string, KeyLimit>; projects: Record<string, string> } {
  const out = { keys: [] as string[], limits: {} as Record<string, KeyLimit>, projects: {} as Record<string, string> };
  if (!file) return out;
  const j = JSON.parse(fs.readFileSync(file, "utf8"));
  for (const p of (j.keys ?? []) as Project[]) {
    if (!p?.key) continue;
    out.keys.push(p.key); out.projects[p.key] = p.project ?? p.key.slice(0, 6);
    if (p.rps != null || p.credits != null) out.limits[p.key] = { rps: p.rps, burst: p.burst, credits: p.credits };
  }
  return out;
}

/** Minimal RESP client: only what a fixed-window counter needs (INCR + PEXPIRE). */
export class MiniRedis {
  private sock?: net.Socket; private q: ((v: any) => void)[] = []; private buf = "";
  constructor(private url: string) {}
  private conn() {
    if (this.sock && !this.sock.destroyed) return this.sock;
    const u = new URL(this.url);
    const s = net.connect(Number(u.port || 6379), u.hostname);
    s.setNoDelay(true);
    s.on("data", (d) => { this.buf += d.toString(); let m; while ((m = /^([:+\-])([^\r]*)\r\n/.exec(this.buf))) { this.buf = this.buf.slice(m[0].length); this.q.shift()?.(m[1] === "-" ? new Error(m[2]) : m[1] === ":" ? Number(m[2]) : m[2]); } });
    s.on("error", () => { for (const r of this.q.splice(0)) r(new Error("redis error")); s.destroy(); });
    if (u.password) this.raw(["AUTH", ...(u.username ? [decodeURIComponent(u.username)] : []), decodeURIComponent(u.password)], s);
    return (this.sock = s);
  }
  private raw(args: string[], s = this.conn()) {
    return new Promise<any>((res, rej) => {
      this.q.push((v) => (v instanceof Error ? rej(v) : res(v)));
      s.write(`*${args.length}\r\n` + args.map((a) => `$${Buffer.byteLength(a)}\r\n${a}\r\n`).join(""));
    });
  }
  cmd(...args: string[]) { return Promise.race([this.raw(args), new Promise((_, j) => setTimeout(() => j(new Error("redis timeout")), 500).unref())]); }
  close() { this.sock?.destroy(); }
}

/** Per-IP limit. Memory: true sliding window (timestamp log). Redis: fixed window (shared across replicas). Off when limit=0. */
export class IpLimiter {
  private hits = new Map<string, number[]>();
  private redis?: MiniRedis;
  constructor(readonly limit = Number(process.env.IP_LIMIT ?? 0), readonly windowMs = Number(process.env.IP_WINDOW_MS ?? 1000), redisUrl = process.env.REDIS_URL) {
    if (redisUrl && limit > 0) this.redis = new MiniRedis(redisUrl);
  }
  async allow(ip: string): Promise<boolean> {
    if (!this.limit) return true;
    if (this.redis) {
      try {
        const k = `frpc:ip:${ip}:${Math.floor(Date.now() / this.windowMs)}`;
        const n = await this.redis.cmd("INCR", k);
        if (n === 1) await this.redis.cmd("PEXPIRE", k, String(this.windowMs * 2));
        return n <= this.limit;
      } catch { /* redis down: fall back to local memory */ }
    }
    const now = Date.now(); const arr = (this.hits.get(ip) ?? []).filter((t) => t > now - this.windowMs);
    if (arr.length >= this.limit) { this.hits.set(ip, arr); return false; }
    arr.push(now); this.hits.set(ip, arr);
    if (this.hits.size > 100_000) for (const [k, v] of this.hits) if (!v.length || v[v.length - 1] < now - this.windowMs) this.hits.delete(k);
    return true;
  }
  close() { this.redis?.close(); }
}
