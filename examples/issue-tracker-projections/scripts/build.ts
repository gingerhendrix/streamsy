/**
 * Build the browser assets and prove the Worker entry bundles for the
 * Cloudflare runtime. Alchemy bundles the Worker again at deploy time; this
 * step keeps bundling failures inside the normal check loop.
 */
import { cp, mkdir, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const outDir = join(packageDir, "dist");

await rm(outDir, { recursive: true, force: true });
await mkdir(outDir, { recursive: true });
await cp(join(packageDir, "public"), join(outDir, "assets"), { recursive: true });

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

const bytes = worker.outputs.reduce((total, output) => total + output.size, 0);
console.log(`assets → dist/assets, worker bundle → dist/worker (${bytes} bytes)`);
