const result = await Bun.build({
  entrypoints: [`${import.meta.dir}/../worker/index.ts`],
  outdir: `${import.meta.dir}/../public/worker`,
  naming: "[name].[ext]",
  target: "browser",
  format: "esm",
  external: ["cloudflare:workers"],
});

if (!result.success) throw new AggregateError(result.logs, "Worker build failed");
