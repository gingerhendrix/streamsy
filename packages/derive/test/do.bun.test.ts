/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- Bun owns the local workerd build and process lifetime. */
import { mkdtempSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Config, Effect, Schema } from "effect";
import { Miniflare } from "miniflare";

const scratch = Effect.runSync(
  Config.String("STREAMSY_DERIVE_SCRATCH").pipe(Config.withDefault("/tmp")),
);

test("same-object Durable Object SQLite runs the fused Derive composition", async () => {
  const root = mkdtempSync(join(scratch, "derive-workerd-"));
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
    durableObjects: { DERIVE: { className: "DeriveObject", useSQLite: true } },
    durableObjectsPersist: join(root, "state"),
  });
  try {
    const response = await miniflare.dispatchFetch("http://derive.test/proof");
    expect(response.status).toBe(200);
    const result = Schema.decodeUnknownSync(
      Schema.Struct({
        before: Schema.Unknown,
        after: Schema.Unknown,
        failed: Schema.String,
        stored: Schema.Struct({
          output: Schema.Array(Schema.Finite),
          state: Schema.Struct({ encoded: Schema.String }),
        }),
        restart: Schema.Struct({ items: Schema.Finite }),
      }),
    )(await response.json());
    expect(result.after).toEqual(result.before);
    expect(result.failed).toBe("Failure");
    expect(result.stored.output).toEqual([1, 3]);
    expect(result.stored.state.encoded).toBe("3");
    expect(result.restart.items).toBe(0);
  } finally {
    await miniflare.dispose();
  }
});
