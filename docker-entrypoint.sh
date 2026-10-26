#!/bin/sh
# TPU_SIDECAR=1 starts the QUIC sidecar on loopback and points fast-rpc at it (off by default).
if [ "$TPU_SIDECAR" = "1" ]; then
  export TPU_SIDECAR_LISTEN="${TPU_SIDECAR_LISTEN:-127.0.0.1:7799}"
  export TPU_SIDECAR_URL="${TPU_SIDECAR_URL:-http://$TPU_SIDECAR_LISTEN}"
  tpu-sidecar &
fi
exec npx tsx examples/serve.ts
