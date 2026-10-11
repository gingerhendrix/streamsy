import { BunServices } from "@effect/platform-bun";
import { expect, layer } from "@streamsy/effect-test";
import { ConfigProvider, Effect, Layer, Schedule, Schema } from "effect";
import { FileIndex, FileIndexLive, IndexLive } from "../../server/file-index.ts";
import {
  FolderStore,
  FolderStoreLive,
  Initialise,
  NoWriteHook,
} from "../../server/folder-store.ts";
import { Changes } from "../../server/streams.ts";
import { StorageLive } from "../../server/storage.ts";
import { testDirectory } from "../../scripts/test-directory.ts";

class Waiting extends Schema.TaggedError<Waiting>()("Waiting", {}) {}

const metadata = (opId: string) => ({ base: null, opId, author: "test", origin: "restart" });

const graph = (filename: string) =>
  Layer.mergeAll(FolderStoreLive, FileIndexLive).pipe(
    Layer.provideMerge(IndexLive),
    Layer.provideMerge(Initialise),
    Layer.provide(NoWriteHook),
    Layer.provideMerge(StorageLive),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ FOLDER_SYNC_DB: filename }))),
  );

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect("FOLDER_SYNC_DB restarts restore known folder followers and discover new folders", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("index-restart-");
      const filename = `${directory}/files.sqlite`;

      for (const run of [1, 2, 3]) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(graph(filename));
            yield* Effect.gen(function* () {
              const store = yield* FolderStore;
              const index = yield* FileIndex;
              const changes = yield* Changes;

              if (run === 1) yield* store.create("daily/old", "Old");

              const written = yield* store.write(
                "daily/old",
                "old.md",
                metadata(`old-${run}`),
                new TextEncoder().encode(`restartneedle round ${run}`),
              );

              yield* store.create(`new/${run}`, "New");
              yield* store.write(
                `new/${run}`,
                "new.md",
                metadata(`new-${run}`),
                new TextEncoder().encode("restartneedle new"),
              );

              const rows = yield* Effect.gen(function* () {
                const hits = yield* index.search("restartneedle");
                const manifest = yield* index.list();
                const feed = yield* changes.collect({});

                if (
                  hits.length !== run + 1 ||
                  !hits.some(
                    (row) => row.version === written.version && row.folderId === "daily/old",
                  ) ||
                  manifest.length !== run + 1 ||
                  !manifest.some(
                    (row) => row.folderId === "daily/old" && row.version === written.version,
                  ) ||
                  feed.items.length !== 2 * run
                )
                  return yield* new Waiting();

                return { hits, manifest, feed };
              }).pipe(
                Effect.retry({
                  while: Schema.is(Waiting),
                  schedule: Schedule.spaced("10 millis").pipe(Schedule.upTo({ times: 300 })),
                }),
              );

              expect(rows.hits).toHaveLength(run + 1);
              expect(rows.manifest).toHaveLength(run + 1);
              expect(rows.feed.items).toHaveLength(2 * run);
            }).pipe(Effect.provideContext(context));
          }),
        );
      }
    }),
  );
});
