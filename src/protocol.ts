// Frame: [u32 length][u8 type][u32 reqId][msgpack payload]   (length = 5 + payload bytes)
import { Packr, Unpackr } from "msgpackr";
export const enum MsgType {
  Request = 0, Response = 1, Error = 2,
  // streaming
  Subscribe = 3,     // id=subId, payload {type, params, fromSeq?, fromSlot?}
  Notification = 4,  // id=subId, payload {seq, slot, data}
  Unsubscribe = 5,   // id=subId
  Ping = 6, Pong = 7,
}
const MAX_TYPE = 7;
const packr = new Packr({ useRecords: false });
const unpackr = new Unpackr({ useRecords: false });
export const HDR = 9;
export const DEFAULT_MAX_FRAME = Number(process.env.MAX_FRAME_BYTES ?? 16 << 20);

export class FrameError extends Error {}

export function encode(type: MsgType, id: number, payload: unknown): Buffer {
  const body = packr.pack(payload);
  const buf = Buffer.allocUnsafe(HDR + body.length);
  buf.writeUInt32BE(5 + body.length, 0);
  buf.writeUInt8(type, 4);
  buf.writeUInt32BE(id >>> 0, 5);
  body.copy(buf, HDR);
  return buf;
}

/**
 * Incremental decoder: handles split/coalesced TCP chunks. Oversized, undersized,
 * unknown-type or undecodable frames call onError once and the decoder stops (caller should close).
 */
export class FrameDecoder {
  private chunks: Buffer[] = [];
  private size = 0;
  dead = false;
  constructor(
    private onFrame: (type: MsgType, id: number, payload: any) => void,
    private opts: { maxFrame?: number; onError?: (e: Error) => void } = {},
  ) {}

  private fail(msg: string) {
    this.dead = true; this.chunks = []; this.size = 0;
    const e = new FrameError(msg);
    if (this.opts.onError) this.opts.onError(e); else throw e;
  }

  push(chunk: Buffer) {
    if (this.dead) return;
    this.chunks.push(chunk); this.size += chunk.length;
    if (this.size < 4) return;
    let buf = this.chunks.length === 1 ? this.chunks[0] : Buffer.concat(this.chunks, this.size);
    let off = 0;
    const max = this.opts.maxFrame ?? DEFAULT_MAX_FRAME;
    while (buf.length - off >= 4) {
      const len = buf.readUInt32BE(off);
      if (len < 5) return this.fail(`frame too short (${len})`);
      if (len > max) return this.fail(`frame too large (${len} > ${max})`);
      if (buf.length - off - 4 < len) break;
      const type = buf.readUInt8(off + 4);
      if (type > MAX_TYPE) return this.fail(`unknown frame type ${type}`);
      const id = buf.readUInt32BE(off + 5);
      let payload: any;
      try { payload = unpackr.unpack(buf.subarray(off + HDR, off + 4 + len)); }
      catch (e: any) { return this.fail(`bad msgpack payload: ${e?.message ?? e}`); }
      off += 4 + len;
      this.onFrame(type, id, payload);
      if (this.dead) return;
    }
    const rest = off === 0 ? buf : buf.subarray(off);
    this.chunks = rest.length ? [rest] : []; this.size = rest.length;
  }
}
