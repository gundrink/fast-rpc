# --- stage 1: tpu-sidecar (Rust; Agave solana-tls-utils + Anza quinn) ---
FROM rust:1-slim AS sidecar
WORKDIR /b
COPY tpu-sidecar/Cargo.toml tpu-sidecar/Cargo.lock* ./
COPY tpu-sidecar/src ./src
RUN cargo build --release --locked 2>/dev/null || cargo build --release

# --- stage 2: fast-rpc ---
FROM node:24-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY src ./src
COPY examples ./examples
COPY data ./data
COPY docker-entrypoint.sh ./
COPY --from=sidecar /b/target/release/tpu-sidecar /usr/local/bin/tpu-sidecar
ENV NODE_ENV=production PORT=7070
EXPOSE 7070
HEALTHCHECK --interval=30s --timeout=3s CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||7070)+'/health').then(r=>process.exit(r.ok?0:1),()=>process.exit(1))"
CMD ["sh", "./docker-entrypoint.sh"]
