/* oxlint-disable effecttsgo/node-builtin-import -- This Bun build script owns its output folder. */
/**
 * Build the local workerd host: the SPA into `dist/web` (the only assets
 * directory) and the edge Worker plus game object into `dist/worker`.
 */
import { rmSync } from "node:fs";

const root = `${import.meta.dir}/..`;
rmSync(`${root}/dist`, { recursive: true, force: true });

const web = await Bun.build({
  entrypoints: [`${root}/public/index.html`],
  outdir: `${root}/dist/web`,
  // Invite links such as /games/<id> are deep SPA routes, so assets need absolute URLs.
  publicPath: "/",
  minify: true,
});
if (!web.success) throw new AggregateError(web.logs, "SPA build failed");

const worker = await Bun.build({
  entrypoints: [`${root}/server/cloudflare/worker.ts`],
  outdir: `${root}/dist/worker`,
  naming: "[name].[ext]",
  target: "browser",
  format: "esm",
  external: ["cloudflare:workers"],
});
if (!worker.success) throw new AggregateError(worker.logs, "Worker build failed");

// The Worker must not carry the Bun storage host or its process adapters.
const source = await Bun.file(`${root}/dist/worker/worker.js`).text();
for (const forbidden of ["bun:sqlite", "@streamsy/storage/bun", "node:child_process"]) {
  if (source.includes(forbidden)) throw new Error(`Worker bundle imports ${forbidden}`);
}
