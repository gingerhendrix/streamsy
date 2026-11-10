/* oxlint-disable effecttsgo/node-builtin-import -- This test owns the local workerd bundle and scratch directory. */
import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Config, Effect, FileSystem, Path, Schema } from "effect";
import { Miniflare } from "miniflare";

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect(
    "FoldByKey loads more than 50 previous keys on Durable Object SQLite",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;

        const scratch = yield* Config.String("STREAMSY_TEST_SCRATCH").pipe(
          Config.withDefault(path.join(import.meta.dirname, "../../../build/projection-tests")),
        );

        yield* fs.makeDirectory(scratch, { recursive: true });

        const root = yield* fs.makeTempDirectoryScoped({
          directory: scratch,
          prefix: "fold-workerd-",
        });

        const built = yield* Effect.promise(() =>
          Bun.build({
            entrypoints: [path.join(import.meta.dirname, "support/fold-by-key-worker.ts")],
            outdir: path.join(root, "bundle"),
            target: "browser",
            format: "esm",
            external: ["cloudflare:workers"],
          }),
        );

        expect(built.success).toBe(true);
        const output = built.outputs[0];

        if (output === undefined) throw new Error("No fold workerd bundle");

        const miniflare = yield* Effect.acquireRelease(
          Effect.sync(
            () =>
              new Miniflare({
                rootPath: root,
                modulesRoot: path.join(root, "bundle"),
                scriptPath: output.path,
                modules: true,
                compatibilityDate: "2026-08-06",
                cf: false,
                durableObjects: { FOLD: { className: "FoldObject", useSQLite: true } },
                durableObjectsPersist: path.join(root, "state"),
              }),
          ),
          (instance) => Effect.promise(() => instance.dispose()),
        );

        const response = yield* Effect.promise(() =>
          miniflare.dispatchFetch("http://fold.test/proof"),
        );

        expect(response.status).toBe(200);

        const result = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            lagging: Schema.Finite,
            rows: Schema.Array(Schema.Struct({ id: Schema.String, count: Schema.Finite })),
            outputCount: Schema.Finite,
          }),
        )(yield* Effect.promise(() => response.json()));

        expect(result.lagging).toBe(0);
        expect(result.rows).toEqual(
          Array.makeBy(113, (index) => ({ id: String(index).padStart(3, "0"), count: 2 })),
        );
        expect(result.outputCount).toBe(226);
      }),
    30000,
  );
});
