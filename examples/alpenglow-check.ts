// Live: detect Alpenglow via getAgGenesisCert and measure slot rate from slotSubscribe for ~5s (1 call + 1 WS sub).
import { createFastRpc } from "../src/index.js";
const app = createFastRpc({ webhooks: false, chain: { trackSlots: true } });
const port = await app.server.listen(0);
await app.ready;
await new Promise((r) => setTimeout(r, 5000));
console.log(JSON.stringify(await (await fetch(`http://127.0.0.1:${port}/v0/chain`)).json()));
await app.close(); process.exit(0);
