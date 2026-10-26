// TpuTransport backed by tpu-sidecar (Rust, Agave solana-tls-utils + Anza quinn). Off unless TPU_SIDECAR_URL is set.
import type { TpuTransport } from "./tpu.js";
export interface HandshakeResult { addr: string; ok: boolean; ms: number; reused: boolean; error?: string }
export class SidecarTransport implements TpuTransport {
  constructor(readonly url = process.env.TPU_SIDECAR_URL ?? "http://127.0.0.1:7799", private timeoutMs = 5000) {}
  private async post(path: string, body: any) {
    const r = await fetch(this.url.replace(/\/+$/, "") + path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), signal: AbortSignal.timeout(this.timeoutMs) });
    if (!r.ok) throw new Error(`tpu-sidecar ${path}: HTTP ${r.status} ${await r.text()}`);
    return r.json() as Promise<HandshakeResult[]>;
  }
  /** QUIC connect only (no stream, nothing sent) */
  handshake(addrs: string[]) { return this.post("/handshake", { addrs }); }
  async send(addr: string, wire: Buffer) {
    const [r] = await this.post("/send", { wire_b64: wire.toString("base64"), addrs: [addr] });
    if (!r?.ok) throw new Error(`tpu send ${addr}: ${r?.error ?? "failed"}`);
  }
  async health() { const r = await fetch(this.url.replace(/\/+$/, "") + "/health", { signal: AbortSignal.timeout(this.timeoutMs) }); return r.json(); }
}
export const sidecarFromEnv = () => (process.env.TPU_SIDECAR_URL ? new SidecarTransport(process.env.TPU_SIDECAR_URL) : undefined);
