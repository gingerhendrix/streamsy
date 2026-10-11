import { BunServices } from "@effect/platform-bun";
import { expect, layer } from "@streamsy/effect-test";
import { Effect, Layer } from "effect";
import {
  FolderStore,
  FolderStoreLive,
  Initialise,
  NoWriteHook,
} from "../../server/folder-store.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { sqliteHost } from "../../server/storage.ts";

const metadata = { base: null, opId: "persisted", author: "test", origin: "test" };

const graph = (filename: string) =>
  Layer.merge(Initialise, FolderStoreLive).pipe(
    Layer.provide(NoWriteHook),
    Layer.provide(sqliteHost(filename)),
  );

layer(BunServices.layer)((it) => {
  it.effect(
    "SQLite recovers folder registry, head, blobs and current-head opId after restart",
    () =>
      Effect.gen(function* () {
        const directory = yield* testDirectory("restart-");
        const filename = `${directory}/files.sqlite`;
        const bytes = new Uint8Array([0, 255, 1]);

        const version = yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(graph(filename));

            return yield* Effect.gen(function* () {
              const store = yield* FolderStore;
              yield* store.create("daily/demo", "Persisted");

              return (yield* store.write("daily/demo", "binary", metadata, bytes)).version;
            }).pipe(Effect.provideContext(context));
          }),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(graph(filename));
            yield* Effect.gen(function* () {
              const store = yield* FolderStore;
              expect((yield* store.create("daily/demo", "New title")).title).toBe("Persisted");
              const head = yield* store.read("daily/demo", "binary");
              expect(head.version).toBe(version);
              expect(head.bytes).toEqual(bytes);
              expect((yield* store.write("daily/demo", "binary", metadata, bytes)).version).toBe(
                version,
              );
              expect(yield* store.history("daily/demo", "binary")).toHaveLength(1);
            }).pipe(Effect.provideContext(context));
          }),
        );
      }),
  );
});
