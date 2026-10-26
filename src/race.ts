// Multi-upstream racing for reads: pick the fastest healthy upstream (EWMA latency), hedge to the next one
// if no answer within hedgeMs, first success wins. Errors / 429 mark an upstream unhealthy for a cooldown.
import { createUpstream, isRateLimit, backoffMs } from "./solana.js";
import { RpcError } from "./errors.js";

export type RaceUpstream = (method: string, params?: any[], ttlMs?: number) => Promise<any>;
export interface RaceOptions { rounds?: number; hedgeMs?: number; cooldownMs?: number; fanout?: number; timeoutMs?: number; make?: (url: string) => RaceUpstream }
/** JSON-RPC error codes that are a real answer (not an upstream fault) — do not retry elsewhere. */
const ANSWER = new Set([-32602, -32601, -32600, -32002, -32003, -32004, -32007, -32009, -32014, -32016]);

export function createRacedUpstream(urls: string[], o: RaceOptions = {}) {
  const hedgeMs = o.hedgeMs ?? Number(process.env.RACE_HEDGE_MS ?? 150);
  const cooldown = o.cooldownMs ?? 30_000;
  const fanout = o.fanout ?? Math.min(3, urls.length);
  // inner upstreams fail fast on 429 (no inner retry): the racer fails over, then backs off and retries the round
  const make = o.make ?? ((u: string) => createUpstream(u, o.timeoutMs, 0));
  const rounds = o.rounds ?? Number(process.env.RACE_429_ROUNDS ?? 3);
  const ups = urls.map((url) => ({ url, call: make(url), ewma: 200, ok: 0, fail: 0, downUntil: 0 }));
  const ranked = () => { const now = Date.now(); return [...ups].sort((a, b) => (+(a.downUntil > now) - +(b.downUntil > now)) || a.ewma - b.ewma); };
  const round = (method: string, params: any[], ttl: number) => new Promise<any>((resolve, reject) => {
      const order = ranked().slice(0, Math.max(1, fanout)); let i = 0, pending = 0, done = false; let lastErr: any;
      const timers: NodeJS.Timeout[] = [];
      const launch = () => {
        if (done || i >= order.length) return; const u = order[i++]; pending++; const t0 = Date.now();
        if (i < order.length) timers.push(setTimeout(launch, hedgeMs));
        u.call(method, params, ttl).then((v) => {
          u.ewma = u.ewma * 0.8 + (Date.now() - t0) * 0.2; u.ok++;
          if (!done) { done = true; timers.forEach(clearTimeout); resolve(v); }
        }, (e) => {
          pending--;
          if (e instanceof RpcError && ANSWER.has(e.code)) { if (!done) { done = true; timers.forEach(clearTimeout); reject(e); } return; }
          u.fail++; u.downUntil = Date.now() + cooldown; lastErr = e;
          if (!done && i < order.length) { timers.forEach(clearTimeout); launch(); }
          else if (!done && pending === 0 && i >= order.length) { done = true; reject(lastErr); }
        });
      };
      launch();
    });
  const fn: RaceUpstream & { stats: () => any } = Object.assign(
    async (method: string, params: any[] = [], ttl = 0) => {
      for (let n = 0; ; n++) {
        try { return await round(method, params, ttl); }
        catch (e: any) { if (n >= rounds || !isRateLimit(e)) throw e; for (const u of ups) u.downUntil = 0; await new Promise((r) => setTimeout(r, backoffMs(n, e.retryAfter))); }
      }
    },
    { stats: () => ups.map(({ url, ewma, ok, fail, downUntil }) => ({ url: url.replace(/([?&]api-key=)[^&]+/, "$1***"), latencyMs: Math.round(ewma), ok, fail, healthy: downUntil <= Date.now() })) },
  );
  return fn;
}
