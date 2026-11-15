/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, effecttsgo/strict-effect-provide -- This test owns a real workerd process and client runtime. */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { Config, Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/http";
import { Streamsy } from "@streamsy/core";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";
import { readSlice } from "../src/read.ts";
import { RemoteStreams } from "@streamsy/core/remote";
import { Miniflare } from "miniflare";
import { Accepted, Source, group } from "./support/accepted-streams.ts";

const scratch = Effect.runSync(
  Config.String("STREAMSY_PROJECTION_SCRATCH").pipe(Config.withDefault(tmpdir())),
);

test("ProjectionWakeup commits accepted rows which a remote writer can await", async () => {
  const root = mkdtempSync(join(scratch, "accepted-workerd-"));
  let mf: Miniflare | undefined;

  try {
    const built = await Bun.build({
      entrypoints: [join(import.meta.dirname, "support/accepted-worker.ts")],
      outdir: join(root, "bundle"),
      target: "browser",
      format: "esm",
      external: ["cloudflare:workers"],
    });

    if (!built.success || built.outputs[0] === undefined)
      throw new Error("No accepted workerd bundle");
    mf = new Miniflare({
      rootPath: root,
      modulesRoot: join(root, "bundle"),
      scriptPath: built.outputs[0].path,
      modules: true,
      compatibilityDate: "2026-08-06",
      cf: false,
      host: "127.0.0.1",
      port: 0,
      durableObjects: { OBJECT: { className: "AcceptedObject", useSQLite: true } },
      durableObjectsPersist: join(root, "state"),
    });
    const url = await mf.ready;
    await Effect.runPromise(
      Effect.gen(function* () {
        const source = yield* Source;
        yield* source.create({ id: "one" });
        yield* (yield* Accepted).create({ id: "one" });
        const receipt = yield* source.append({ id: "one" }, [5]);
        yield* Streamsy.awaitWake(
          Accepted,
          { id: "one" },
          { stream: Source.at({ id: "one" }).id, offset: receipt.offset },
        ).pipe(Effect.timeout("10 seconds"));
        expect((yield* (yield* Accepted).collect({ id: "one" })).items).toMatchObject([
          { value: { offset: receipt.offset } },
        ]);
        const input = Source.at({ id: "one" });

        const projection = Projection.process({
          id: "http-entry-offsets",
          params: {},
          inputs: { input },
          commit: Commit.atLeastOnce,
          process: (batch) =>
            Effect.sync(() => {
              expect(batch.input.items).toEqual([5]);
              expect(batch.input.offsets).toBeUndefined();
              expect(batch.input).not.toHaveProperty("offsets");
              expect(Projection.items(batch)).toEqual([{ input: "input", item: 5, index: 0 }]);
              expect(Projection.items(batch)[0]).not.toHaveProperty("offset");
            }),
        }).at({});

        expect(
          (yield* Projection.run(projection).pipe(Effect.provide(CheckpointStore.layerMemory)))
            .items,
        ).toBe(1);
        expect((yield* readSlice("input", input, receipt.offset)).slice.offsets).toEqual([]);
      }).pipe(
        Effect.provide(
          RemoteStreams.layer(group, { baseUrl: url.origin }).pipe(
            Layer.provide(FetchHttpClient.layer),
          ),
        ),
      ),
    );
  } finally {
    await mf?.dispose();
    rmSync(root, { recursive: true, force: true });
  }
}, 60_000);
