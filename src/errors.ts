export class RpcError extends Error {
  constructor(public code: number, message: string, public data?: unknown) { super(message); }
}
export const INVALID_PARAMS = (m: string) => new RpcError(-32602, m);
