/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, effecttsgo/global-fetch -- This executable example owns the local Miniflare, temporary I/O, and HTTP boundaries. */
import { mkdtemp, rm } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const sourceDirectory = dirname(fileURLToPath(import.meta.url));
const root = resolve(sourceDirectory, "../../..");
const entrypoint = resolve(root, "packages/serve/test/cloudflare/example-worker.ts");
const ownedRoot = await mkdtemp(join(sourceDirectory, ".streamsy-cloudflare-example-"));
let miniflare: Miniflare | undefined;

try {
  const outputRoot = join(ownedRoot, "worker");
  const build = await Bun.build({
    entrypoints: [entrypoint],
    outdir: outputRoot,
    target: "browser",
    format: "esm",
    minify: false,
    sourcemap: "none",
    external: ["cloudflare:workers"],
  });
  if (!build.success) throw new Error("Cloudflare example Worker build failed");
  const worker = build.outputs.find((output) => output.path.endsWith(".js"));
  if (worker === undefined) throw new Error("Cloudflare example Worker emitted no JavaScript");

  miniflare = new Miniflare({
    scriptPath: worker.path,
    modules: true,
    compatibilityDate: "2026-07-30",
    compatibilityFlags: ["nodejs_compat"],
    host: "127.0.0.1",
    port: 0,
    cf: false,
    durableObjects: { STREAMS: { className: "StreamsObject", useSQLite: true } },
    durableObjectsPersist: join(ownedRoot, "state"),
  });
  const origin = (await miniflare.ready).origin;
  const response = await fetch(new URL("/streams/events", origin), {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({}),
  });
  await response.arrayBuffer();
  if (response.status !== 201)
    throw new Error(`Cloudflare example create failed: ${response.status}`);
} finally {
  try {
    await miniflare?.dispose();
  } finally {
    await rm(ownedRoot, { recursive: true, force: true });
  }
}
