import alchemy from "alchemy";
import { Website } from "alchemy/cloudflare";

const deployment = process.env.STREAMSY_DOCS_DEPLOYMENT ?? "production";
if (deployment !== "production") {
  throw new Error(`Unknown STREAMSY_DOCS_DEPLOYMENT value: ${deployment}`);
}

// Static documentation hosting configuration; this does not host protocol storage.
const app = await alchemy("streamsy-docs");

// Nitro emits a Workers module plus static assets. Upload the built modules as-is.
const site = await Website("streamsy-docs", {
  build: "bun run build",
  entrypoint: ".output/server/index.mjs",
  assets: ".output/public",
  compatibility: "node",
  compatibilityDate: "2026-06-27",
  noBundle: true,
  spa: false,
  domains: ["streamsy.gandrew.com", "streamsy.dev"],
});

console.log({ url: site.url });
await app.finalize();
