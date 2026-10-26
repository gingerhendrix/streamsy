import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Effect, Layer, Schedule, Schema } from "effect";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { Files } from "../../server/streams.ts";
import { search } from "../../server/search-index.ts";
import { filesWake } from "../../server/wake.ts";
import { FileIndex, FileIndexLive } from "../../server/file-index.ts";
import { IndexLive } from "../../server/index-live.ts";
import { FolderStore, FolderStoreLive, Initialise } from "../../server/folder-store.ts";
import { sqliteHost } from "../../server/storage.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { storedText } from "../content.ts";

class Waiting extends Schema.TaggedError<Waiting, { readonly brand: unique symbol }>()(
  "Waiting",
  {},
) {}

const graph = (filename: string) =>
  Layer.merge(FolderStoreLive, FileIndexLive).pipe(
    Layer.provideMerge(IndexLive),
    Layer.provideMerge(Initialise),
    Layer.provideMerge(sqliteHost(filename)),
  );

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect("startup indexes a durable dirty wake row without another source write", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("index-dirty-restart-");
      const storage = sqliteHost(`${directory}/index.sqlite`);
      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(
            Layer.mergeAll(Initialise, FolderStoreLive).pipe(Layer.provideMerge(storage)),
          );

          yield* Effect.gen(function* () {
            const store = yield* FolderStore;
            yield* store.create("dirty", "Dirty", "create");
            yield* store.commit("dirty", "a", {
              ...(yield* storedText("dirtyneedle")),
              txid: "dirty",
              author: "test",
              parent: null,
            });
            yield* Projection.run(filesWake.at({ folderId: "dirty" }));
          }).pipe(Effect.provideContext(context));
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(graph(`${directory}/index.sqlite`));

          const rows = yield* FileIndex.pipe(
            Effect.flatMap((index) => index.search("dirtyneedle")),
            Effect.provideContext(context),
          );

          expect(rows).toHaveLength(1);
        }),
      );
      // Remove while attachment is stopped, leaving the old search rows behind.
      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(storage);
          yield* Effect.gen(function* () {
            yield* (yield* Files).remove({ folderId: "dirty" });
            yield* Projection.run(filesWake.at({ folderId: "dirty" }));
          }).pipe(Effect.provideContext(context));
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(graph(`${directory}/index.sqlite`));
          yield* Effect.gen(function* () {
            expect(yield* (yield* FileIndex).list()).toEqual([]);
            expect(
              (yield* (yield* CheckpointStore).load(search.at({ folderId: "dirty" }))).token,
            ).toBe("0");
          }).pipe(Effect.provideContext(context));
        }),
      );
    }),
  );

  it.effect(
    "index restart resumes source cursors, restores known folders and discovers new ones",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("index-restart-");

        for (const run of [1, 2, 3])
          yield* Effect.scoped(
            Effect.gen(function* () {
              const context = yield* Layer.build(graph(`${directory}/index.sqlite`));
              yield* Effect.gen(function* () {
                const store = yield* FolderStore;
                const index = yield* FileIndex;

                if (run === 1) yield* store.create("old/folder", "Old", "create");
                yield* store.commit("old/folder", "a", {
                  ...(yield* storedText(`restartneedle ${run}`)),
                  txid: `old-${run}`,
                  author: "test",
                  parent: null,
                });
                yield* store.create(`new/${run}`, "New", `folder-${run}`);
                yield* store.commit(`new/${run}`, "b", {
                  ...(yield* storedText("restartneedle new")),
                  txid: `new-${run}`,
                  author: "test",
                  parent: null,
                });

                const listed = yield* index.list().pipe(
                  Effect.filterOrFail(
                    (rows) =>
                      rows.length === run + 1 &&
                      Array.some(
                        rows,
                        (row) => row.folderId === "old/folder" && row.version === run,
                      ),
                    () => new Waiting(),
                  ),
                  Effect.retry({
                    while: Schema.is(Waiting),
                    schedule: Schedule.spaced("10 millis").pipe(Schedule.upTo({ times: 200 })),
                  }),
                );

                expect(listed).toHaveLength(run + 1);
                expect(yield* index.search("restartneedle")).toHaveLength(run + 1);
              }).pipe(Effect.provideContext(context));
            }),
          );
      }),
  );
});
