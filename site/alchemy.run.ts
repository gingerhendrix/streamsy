import alchemy from "alchemy";
import { Website } from "alchemy/cloudflare";
import { docsDeploymentTarget } from "../scripts/docs-deployment-targets.ts";

const target = docsDeploymentTarget(process.env.STREAMSY_DOCS_DEPLOYMENT);

// Static documentation hosting configuration; this does not host protocol storage.
const app = await alchemy(target.appId);

// Nitro emits a Workers module plus static assets. Upload the built modules as-is.
const site = await Website(target.resourceId, {
  name: target.name,
  build: "bun run build",
  entrypoint: ".output/server/index.mjs",
  assets: ".output/public",
  compatibility: "node",
  compatibilityDate: "2026-06-27",
  noBundle: true,
  spa: false,
  domains: target.domains,
});

console.log({ url: site.url });
await app.finalize();
