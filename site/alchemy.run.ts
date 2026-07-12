import alchemy from "alchemy";
import { Website } from "alchemy/cloudflare";

const app = await alchemy("streamsy-docs");

// The docs site builds with the nitro `cloudflare_module` preset (see vite.config.ts),
// which emits a Workers module worker + a separate static-assets dir:
//   .output/server/index.mjs   -> worker entry (with sibling chunks + Takumi WASM under server/wasm)
//   .output/public             -> static assets (served via the ASSETS binding)
//
// `noBundle: true` is required: nitro has already bundled the worker, so Alchemy uploads
// the entry plus all sibling modules as-is. The default noBundle globs
// (**/*.js, **/*.mjs, **/*.wasm) pick up the Takumi `*.wasm` module too, which is needed
// for og:image rendering on Cloudflare's workerd runtime.
const site = await Website("streamsy-docs", {
  build: "bun run build",
  entrypoint: ".output/server/index.mjs",
  assets: ".output/public",
  compatibility: "node",
  compatibilityDate: "2026-06-27",
  noBundle: true,
  spa: false,
  domains: ["streamsy.gandrew.com"],
});

console.log({ url: site.url });
await app.finalize();
