FROM node:24-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY src ./src
COPY examples ./examples
COPY data ./data
ENV NODE_ENV=production PORT=7070
EXPOSE 7070
HEALTHCHECK --interval=30s --timeout=3s CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||7070)+'/health').then(r=>process.exit(r.ok?0:1),()=>process.exit(1))"
CMD ["npx", "tsx", "examples/serve.ts"]
