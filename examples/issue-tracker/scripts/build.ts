/**
 * Build the browser bundle.
 *
 * Assets land in `dist/assets`, which is what the local host serves.
 */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- This Bun build executable removes and recreates its package-owned output directory through the runtime filesystem API.
import { rm } from "node:fs/promises";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- This Bun build executable resolves package-owned input and output paths before invoking Bun.build.
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
  // oxlint-disable-next-line effecttsgo/global-console -- The executable forwards Bun build diagnostics to its invoking terminal before failing.
  for (const log of app.logs) console.error(log);
  throw new Error("Browser bundle failed");
}

const bytes = app.outputs.reduce((total, output) => total + output.size, 0);
// oxlint-disable-next-line effecttsgo/global-console -- The build command's stdout contract reports output size and file count to its caller.
console.log(`assets → dist/assets (${bytes} bytes, ${app.outputs.length} files)`);
