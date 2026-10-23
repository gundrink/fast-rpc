import net from "node:net";
import { encode, FrameDecoder, MsgType } from "./protocol.js";

type Pending = { resolve: (v: any) => void; reject: (e: Error) => void };

export class RpcClient {
  private sock!: net.Socket;
  private nextId = 1;
  private pending = new Map<number, Pending>();
  private queue: Buffer[] = [];
  private flushScheduled = false;

  static async connect(port: number, host = "127.0.0.1") {
    const c = new RpcClient();
    await new Promise<void>((res, rej) => {
      c.sock = net.connect(port, host, res).once("error", rej);
    });
    c.sock.setNoDelay(true);
    const dec = new FrameDecoder((type, id, payload) => {
      const p = c.pending.get(id); if (!p) return;
      c.pending.delete(id);
      type === MsgType.Error ? p.reject(new Error(payload)) : p.resolve(payload);
    });
    c.sock.on("data", (d: Buffer) => dec.push(d));
    c.sock.on("close", () => { for (const p of c.pending.values()) p.reject(new Error("closed")); c.pending.clear(); });
    return c;
  }

  /** Multiplexed call: many in flight on one socket, matched by request id. */
  call<T = any>(method: string, ...args: any[]): Promise<T> {
    const id = this.nextId = (this.nextId % 0xffffffff) + 1;
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.queue.push(encode(MsgType.Request, id, [method, args]));
      if (!this.flushScheduled) { // batch writes per tick
        this.flushScheduled = true;
        queueMicrotask(() => {
          this.flushScheduled = false;
          const q = this.queue; this.queue = [];
          this.sock.write(q.length === 1 ? q[0] : Buffer.concat(q));
        });
      }
    });
  }

  /** Typed proxy: `api.add(1,2)` feels like a local call. */
  proxy<T extends Record<string, (...a: any[]) => any>>(): { [K in keyof T]: (...a: Parameters<T[K]>) => Promise<Awaited<ReturnType<T[K]>>> } {
    return new Proxy({} as any, { get: (_, name: string) => (...a: any[]) => this.call(name, ...a) });
  }
  close() { this.sock.end(); }
}
