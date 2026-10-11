/* oxlint-disable effecttsgo/async-function -- Bun bundler executable boundary. */
const result = await Bun.build({
  entrypoints: [`${import.meta.dir}/../worker/index.ts`],
  outdir: `${import.meta.dir}/../public/worker`,
  target: "browser",
  format: "esm",
  external: ["cloudflare:workers"],
});

if (!result.success) throw new AggregateError(result.logs, "Worker build failed");
