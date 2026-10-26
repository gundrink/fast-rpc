import { createFastRpc } from "../src/index.js";
const app = createFastRpc({ webhooks: false } as any); const api = app.api as any;
for (const m of ["GKNH1bfeKt9ErKjSNw2ZTMjeXUu8fdzzb23uvNd4pump", "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"]) {
  const t = Date.now(); try { const r = await api.getTokenLargestAccounts(m); console.log(m.slice(0, 6), Date.now() - t, "ms", JSON.stringify(r._meta), JSON.stringify(r.value.slice(0, 2)), "rssMB", Math.round(process.memoryUsage().rss / 1e6)); } catch (e: any) { console.log(m, "ERR", e.message); } }
await app.close(); process.exit(0);
