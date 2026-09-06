/* oxlint-disable effecttsgo/global-console, effecttsgo/node-builtin-import -- This one-shot Bun build executable resolves its output paths with the Node-compatible filesystem and path APIs and reports bundle results directly to the invoking terminal. */
/**
 * Build the browser bundle and prove the Worker entry bundles for the
 * Cloudflare runtime. Alchemy bundles the Worker again at deploy time; this
 * step keeps bundling failures inside the normal check loop.
 *
 * Browser assets land in `dist/assets`, which is what both the local host and
 * the Alchemy `Assets` resource serve.
 */
import { rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const outDir = join(packageDir, "dist");

await rm(outDir, { recursive: true, force: true });

const app = await Bun.build({
  entrypoints: [join(packageDir, "src/index.html")],
  outdir: join(outDir, "assets"),
  target: "browser",
  format: "esm",
  minify: true,
  sourcemap: "linked",
  naming: { entry: "[name].[ext]", chunk: "[name]-[hash].[ext]", asset: "[name]-[hash].[ext]" },
  define: { "process.env.NODE_ENV": JSON.stringify("production") },
});

if (!app.success) {
  for (const log of app.logs) console.error(log);
  throw new Error("Browser bundle failed");
}

const worker = await Bun.build({
  entrypoints: [join(packageDir, "server/worker.ts")],
  outdir: join(outDir, "worker"),
  target: "browser",
  format: "esm",
  external: ["cloudflare:workers"],
});

if (!worker.success) {
  for (const log of worker.logs) console.error(log);
  throw new Error("Worker bundle failed");
}

const appBytes = app.outputs.reduce((total, output) => total + output.size, 0);
const workerBytes = worker.outputs.reduce((total, output) => total + output.size, 0);
console.log(
  `assets → dist/assets (${appBytes} bytes, ${app.outputs.length} files), worker bundle → dist/worker (${workerBytes} bytes)`,
);
