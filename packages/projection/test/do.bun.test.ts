import { mkdtempSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Config, Effect, Schema } from "effect";
import { Miniflare } from "miniflare";

const scratch = Effect.runSync(
  Config.String("STREAMSY_PROJECTION_SCRATCH").pipe(Config.withDefault("/tmp")),
);

test("same-object Durable Object SQLite runs the fused composition", async () => {
  const root = mkdtempSync(join(scratch, "projection-workerd-"));
  const built = await Bun.build({
    entrypoints: [join(import.meta.dir, "do-worker.test.ts")],
    outdir: join(root, "bundle"),
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });
  expect(built.success).toBe(true);
  const output = built.outputs[0];
  if (output === undefined) throw new Error("No workerd bundle");
  const miniflare = new Miniflare({
    rootPath: root,
    modulesRoot: join(root, "bundle"),
    scriptPath: output.path,
    modules: true,
    compatibilityDate: "2026-08-06",
    durableObjects: { PROJECTION: { className: "ProjectionObject", useSQLite: true } },
    durableObjectsPersist: join(root, "state"),
  });
  try {
    const response = await miniflare.dispatchFetch("http://projection.test/proof");
    expect(response.status).toBe(200);
    const progress = Schema.Struct({ status: Schema.String, items: Schema.Finite });
    const result = Schema.decodeUnknownSync(
      Schema.Struct({
        first: progress,
        before: Schema.Unknown,
        after: Schema.Unknown,
        failed: Schema.String,
        final: progress,
        restart: progress,
        stored: Schema.Struct({
          output: Schema.Array(Schema.Finite),
          loaded: Schema.Struct({ token: Schema.String }),
        }),
      }),
    )(await response.json());
    expect(result.first.status).toBe("limit-reached");
    expect(result.first.items).toBe(1);
    expect(result.failed).toBe("Failure");
    expect(result.after).toEqual(result.before);
    expect(result.final.status).toBe("caught-up");
    expect(result.final.items).toBe(2);
    expect(result.restart.items).toBe(0);
    expect(result.stored.output).toEqual([1, 3]);
    expect(result.stored.loaded.token).toBe("2");
  } finally {
    await miniflare.dispose();
  }
});
