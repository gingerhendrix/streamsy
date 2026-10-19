import { BunServices } from "@effect/platform-bun";

import { expect, layer } from "@effect/vitest";
import { Effect, FileSystem, Path, Schema } from "effect";
import { Miniflare } from "miniflare";

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect("same-object Durable Object SQLite runs the transactional composition", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "projection-workerd-" });

      const built = yield* Effect.promise(() =>
        Bun.build({
          entrypoints: [path.join(import.meta.dirname, "support/do-worker.ts")],
          outdir: path.join(root, "bundle"),
          target: "browser",
          format: "esm",
          external: ["cloudflare:workers"],
        }),
      );

      expect(built.success).toBe(true);
      const output = built.outputs[0];

      if (output === undefined) throw new Error("No workerd bundle");

      const miniflare = yield* Effect.acquireRelease(
        Effect.sync(
          () =>
            new Miniflare({
              rootPath: root,
              modulesRoot: path.join(root, "bundle"),
              scriptPath: output.path,
              modules: true,
              compatibilityDate: "2026-08-06",
              durableObjects: { PROJECTION: { className: "ProjectionObject", useSQLite: true } },
              durableObjectsPersist: path.join(root, "state"),
            }),
        ),
        (instance) => Effect.promise(() => instance.dispose()),
      );

      const response = yield* Effect.promise(() =>
        miniflare.dispatchFetch("http://projection.test/proof"),
      );

      expect(response.status).toBe(200);
      const progress = Schema.Struct({ status: Schema.String, items: Schema.Finite });

      const result = yield* Schema.decodeUnknownEffect(
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
      )(yield* Effect.promise(() => response.json()));

      expect(result.first.status).toBe("limit-reached");
      expect(result.first.items).toBe(1);
      expect(result.failed).toBe("Failure");
      expect(result.after).toEqual(result.before);
      expect(result.final.status).toBe("caught-up");
      expect(result.final.items).toBe(2);
      expect(result.restart.items).toBe(0);
      expect(result.stored.output).toEqual([1, 3]);
      expect(result.stored.loaded.token).toBe("3");
    }),
  );
});
