import { BunServices } from "@effect/platform-bun";

import { expect, layer } from "@effect/vitest";
import { Array, Effect, FileSystem, Path, Schema } from "effect";
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

      const offsetsResponse = yield* Effect.promise(() =>
        miniflare.dispatchFetch("http://projection.test/offsets"),
      );

      expect(offsetsResponse.status).toBe(200);

      const sliceSchema = Schema.Struct({
        items: Schema.Array(Schema.Finite),
        offsets: Schema.Array(Schema.String),
        nextOffset: Schema.String,
      });

      const offsets = yield* Schema.decodeUnknownEffect(
        Schema.Struct({
          expected: Schema.Array(Schema.String),
          skippedOffsets: Schema.Array(Schema.Array(Schema.String)),
          slices: Schema.Array(sliceSchema),
          entries: Schema.Array(
            Schema.Struct({ item: Schema.Finite, offset: Schema.String, index: Schema.Finite }),
          ),
          keys: Schema.Array(Schema.String),
          first: Schema.String,
          replay: Schema.Finite,
          rest: Schema.Finite,
        }),
      )(yield* Effect.promise(() => offsetsResponse.json()));

      expect(offsets.first).toBe("Failure");
      expect(offsets.skippedOffsets).toEqual([[], [], []]);
      expect(offsets.replay).toBe(3);
      expect(offsets.rest).toBe(2);
      expect(offsets.slices[1]).toEqual(offsets.slices[0]);
      expect(offsets.keys[1]).toBe(offsets.keys[0]);
      expect(offsets.entries.slice(3, 6)).toEqual(offsets.entries.slice(0, 3));
      expect(
        Array.flatMap([offsets.slices[0]!, offsets.slices[2]!], (slice) => slice.offsets),
      ).toEqual(offsets.expected);
      expect(
        Array.map(
          [...offsets.entries.slice(0, 3), ...offsets.entries.slice(6)],
          (entry) => entry.offset,
        ),
      ).toEqual(offsets.expected);

      for (const slice of offsets.slices) {
        expect(slice.offsets).toHaveLength(slice.items.length);
        expect(slice.offsets.at(-1)).toBe(slice.nextOffset);
      }

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
