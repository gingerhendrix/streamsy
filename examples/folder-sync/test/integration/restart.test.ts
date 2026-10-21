import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer } from "effect";
import { FolderStore, FolderStoreLive, Initialise } from "../../server/folder-store.ts";
import { sqliteHost } from "../../server/storage.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { storedText } from "../content.ts";

const graph = (filename: string) =>
  Layer.merge(Initialise, FolderStoreLive).pipe(Layer.provideMerge(sqliteHost(filename)));

layer(BunServices.layer)((it) => {
  it.effect("versions keep rising across restarts after an upsert and after a tombstone", () =>
    Effect.gen(function* () {
      const directory = yield* testDirectory("counter-restart-");

      for (const deleteBeforeRestart of [false, true]) {
        const filename = `${directory}/${deleteBeforeRestart}.sqlite`;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(graph(filename));
            yield* Effect.gen(function* () {
              const store = yield* FolderStore;
              yield* store.create("daily/demo", "Docs", "create");
              const content = yield* storedText("one");
              expect(
                (yield* store.commit("daily/demo", "a", {
                  ...content,
                  txid: "first",
                  author: "test",
                  parent: null,
                })).version,
              ).toBe(1);

              if (deleteBeforeRestart)
                yield* store.remove("daily/demo", "a", {
                  txid: "delete",
                  author: "test",
                  parent: 1,
                });
            }).pipe(Effect.provideContext(context));
          }),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(graph(filename));
            yield* Effect.gen(function* () {
              const store = yield* FolderStore;
              const content = yield* storedText("two");
              expect(
                (yield* store.commit("daily/demo", "a", {
                  ...content,
                  txid: "second",
                  author: "test",
                  parent: null,
                })).version,
              ).toBe(deleteBeforeRestart ? 3 : 2);
              // The restarted owner keeps counting from the stream, not from a reset counter.
              expect(
                (yield* store.commit("daily/demo", "b", {
                  ...content,
                  txid: "third",
                  author: "test",
                  parent: null,
                })).version,
              ).toBe(deleteBeforeRestart ? 4 : 3);
            }).pipe(Effect.provideContext(context));
          }),
        );
      }
    }),
  );
});
