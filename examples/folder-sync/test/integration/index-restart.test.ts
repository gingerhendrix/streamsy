import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer, Schedule, Schema } from "effect";
import { FileIndex, FileIndexLive } from "../../server/file-index.ts";
import { IndexLive } from "../../server/index-followers.ts";
import { FolderStore, FolderStoreLive, Initialise } from "../../server/folder-store.ts";
import { sqliteHost } from "../../server/storage.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { storedText } from "../content.ts";

class Waiting extends Schema.TaggedError<Waiting>()("Waiting", {}) {}

const graph = (filename: string) =>
  Layer.merge(FolderStoreLive, FileIndexLive).pipe(
    Layer.provideMerge(IndexLive),
    Layer.provideMerge(Initialise),
    Layer.provideMerge(sqliteHost(filename)),
  );

layer(BunServices.layer, { excludeTestServices: true })((it) => {
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
                });
                yield* store.create(`new/${run}`, "New", `folder-${run}`);
                yield* store.commit(`new/${run}`, "b", {
                  ...(yield* storedText("restartneedle new")),
                  txid: `new-${run}`,
                  author: "test",
                });

                const listed = yield* index.list().pipe(
                  Effect.filterOrFail(
                    (rows) =>
                      rows.length === run + 1 &&
                      rows.some((row) => row.folderId === "old/folder" && row.version === run),
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
