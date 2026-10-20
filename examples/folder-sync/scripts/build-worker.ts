/* oxlint-disable effecttsgo/async-function -- Bun bundler executable boundary. */
const result = await Bun.build({
  // The test entry adds test switches. Deploys use index.js, which has none.
  entrypoints: [
    `${import.meta.dir}/../worker/index.ts`,
    `${import.meta.dir}/../test/worker/test-worker.ts`,
  ],
  outdir: `${import.meta.dir}/../public/worker`,
  naming: "[name].[ext]",
  target: "browser",
  format: "esm",
  external: ["cloudflare:workers"],
});

if (!result.success) throw new AggregateError(result.logs, "Worker build failed");
