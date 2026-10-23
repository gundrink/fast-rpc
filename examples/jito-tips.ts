// Read-only live check: getTipAccounts via fast-rpc -> JITO_URL. Never sends bundles/transactions.
import { createFastRpc } from "../src/index.js";
const app = createFastRpc({ webhooks: false, chain: { detect: false } });
const port = await app.server.listen(0);
const r = await fetch(`http://127.0.0.1:${port}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getTipAccounts", params: [] }) });
console.log(JSON.stringify(await r.json(), null, 1));
await app.close();
