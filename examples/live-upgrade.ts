import { createSolanaApi } from "../src/index.js";
const a: any = createSolanaApi();
const l = await a.getTpuLeaders(); console.log("leaders", l.length, l[0]?.addr);
const r = await a.getFundedBy("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", { hops: 1 }).catch((e: any) => e.message);
console.log(JSON.stringify(r).slice(0, 400));
