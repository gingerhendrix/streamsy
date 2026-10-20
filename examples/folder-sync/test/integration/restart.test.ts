import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer } from "effect";
import { FolderStore, FolderStoreLive, Initialise } from "../../server/folder-store.ts";
import { prepareContent } from "../../server/domain.ts";
import { sqliteHost } from "../../server/storage.ts";
import { testDirectory } from "../../scripts/test-directory.ts";

const graph = (filename: string) =>
  Layer.merge(Initialise, FolderStoreLive).pipe(Layer.provide(sqliteHost(filename)));

layer(BunServices.layer)((it) => {
  it.effect("versions keep rising across restarts after an upsert and after a delete", () =>
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
              const content = yield* prepareContent(new TextEncoder().encode("one"));
              expect(
                (yield* store.commit("daily/demo", "a", {
                  ...content,
                  txid: "first",
                  author: "test",
                })).version,
              ).toBe(1);

              if (deleteBeforeRestart)
                yield* store.remove("daily/demo", "a", { txid: "delete", author: "test" });
            }).pipe(Effect.provideContext(context));
          }),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(graph(filename));
            yield* Effect.gen(function* () {
              const store = yield* FolderStore;
              const content = yield* prepareContent(new TextEncoder().encode("two"));
              expect(
                (yield* store.commit("daily/demo", "a", {
                  ...content,
                  txid: "second",
                  author: "test",
                })).version,
              ).toBe(2);
              // The restarted owner keeps counting from the stream, not from a reset counter.
              expect(
                (yield* store.commit("daily/demo", "b", {
                  ...content,
                  txid: "third",
                  author: "test",
                })).version,
              ).toBe(3);
            }).pipe(Effect.provideContext(context));
          }),
        );
      }
    }),
  );
});
