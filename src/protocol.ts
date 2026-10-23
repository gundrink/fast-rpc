// Frame: [u32 length][u8 type][u32 reqId][msgpack payload]
import { Packr, Unpackr } from "msgpackr";
export const enum MsgType { Request = 0, Response = 1, Error = 2 }
const packr = new Packr({ useRecords: false });
const unpackr = new Unpackr({ useRecords: false });
const HDR = 9;

export function encode(type: MsgType, id: number, payload: unknown): Buffer {
  const body = packr.pack(payload);
  const buf = Buffer.allocUnsafe(HDR + body.length);
  buf.writeUInt32BE(5 + body.length, 0);
  buf.writeUInt8(type, 4);
  buf.writeUInt32BE(id, 5);
  body.copy(buf, HDR);
  return buf;
}

/** Incremental decoder: handles partial/coalesced TCP chunks. */
export class FrameDecoder {
  private buf: Buffer = Buffer.alloc(0);
  constructor(private onFrame: (type: MsgType, id: number, payload: any) => void) {}
  push(chunk: Buffer) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    let off = 0;
    while (this.buf.length - off >= 4) {
      const len = this.buf.readUInt32BE(off);
      if (this.buf.length - off - 4 < len) break;
      const type = this.buf.readUInt8(off + 4);
      const id = this.buf.readUInt32BE(off + 5);
      const payload = unpackr.unpack(this.buf.subarray(off + HDR, off + 4 + len));
      off += 4 + len;
      this.onFrame(type, id, payload);
    }
    this.buf = off === this.buf.length ? Buffer.alloc(0) : this.buf.subarray(off);
  }
}
