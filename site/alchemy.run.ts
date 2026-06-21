import alchemy from "alchemy";
import { Website } from "alchemy/cloudflare";

const app = await alchemy("streamsy-docs");

const site = await Website("streamsy-docs", {
  build: "bun run build",
  entrypoint: "dist/_worker.js/index.js",
  assets: "dist",
  compatibility: "node",
  spa: false,
});

console.log({ url: site.url });
await app.finalize();
