import alchemy from "alchemy";
import { Website } from "alchemy/cloudflare";
import { CloudflareStateStore } from "alchemy/state";
import { docsDeploymentTarget } from "../scripts/docs-deployment-targets.ts";

const target = docsDeploymentTarget(process.env.STREAMSY_DOCS_DEPLOYMENT);

// Shared targets (production, next) pin their stage and keep state in the
// Cloudflare state store, so CI and local deploys see one state. The store
// needs ALCHEMY_STATE_TOKEN. The experimental target keeps local state.
const app =
  target.stage === undefined
    ? await alchemy(target.appId)
    : await alchemy(target.appId, {
        stage: target.stage,
        stateStore: (scope) => new CloudflareStateStore(scope),
      });

// Static documentation hosting configuration; this does not host protocol storage.
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
  domains: [...target.domains],
});

console.log({ url: site.url });

await app.finalize();
