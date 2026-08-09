import alchemy from "alchemy";
import { DurableObjectNamespace, Website } from "alchemy/cloudflare";

const app = await alchemy("streamsy-risk");
const publishedDomain = "hexdomination.gandrew.com";

const game = DurableObjectNamespace("game", {
  className: "GameDurableObject",
  sqlite: true,
});

const site = await Website("demo", {
  build: "bun run build:cloudflare",
  entrypoint: "./server/cloudflare/worker.ts",
  assets: "./dist",
  spa: true,
  compatibilityDate: "2026-07-26",
  bindings: { GAME: game },
  domains: [{ domainName: publishedDomain, adopt: true }],
  url: true,
});

console.log({
  stage: app.stage,
  url: `https://${publishedDomain}`,
  workersDevUrl: site.url,
});
await app.finalize();
