import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { DurableState, CommitBoundary, StorageFault } from "@streamsy/core";
import { OwnerRecords } from "@streamsy/core/internal";
import { Effect, Layer } from "effect";
import {
  FolderConflict,
  InvalidFile,
  MissingFile,
  PreconditionFailed,
  prepareContent,
} from "../server/domain.ts";
import { FolderStore, FolderStoreLive, Initialise } from "../server/folder-store.ts";
import { Files } from "../server/streams.ts";
import { memoryHost, sqliteHost } from "../server/storage.ts";

for (const [name, storage] of [
  ["memory", memoryHost],
  ["sqlite", sqliteHost(":memory:")],
] as const) {
  const host = Layer.merge(Initialise, FolderStoreLive).pipe(Layer.provideMerge(storage));
  layer(host)(name, (it) => {
    it.effect(
      "versions rise across paths; delete takes no number; recreation rejects stale versions",
      () =>
        Effect.gen(function* () {
          const store = yield* FolderStore;
          const files = yield* Files;
          const folder = "opaque/雪%";
          yield* store.create(folder, "Docs", "create");
          expect(yield* store.create(folder, "Docs", "repeat")).toEqual({
            folderId: folder,
            title: "Docs",
          });
          expect(yield* Effect.flip(store.create(folder, "Other", "conflict"))).toBeInstanceOf(
            FolderConflict,
          );

          const one = {
            ...(yield* prepareContent(new TextEncoder().encode("one"))),
            author: "test",
            txid: "one",
          };

          const two = {
            ...(yield* prepareContent(new TextEncoder().encode("two"))),
            author: "test",
            txid: "two",
          };

          expect((yield* store.commit(folder, "a", one, { absent: true })).version).toBe(1);
          expect((yield* store.commit(folder, "b", two)).version).toBe(2);
          yield* store.remove(folder, "a", { txid: "delete", author: "test" }, { version: 1 });
          expect(yield* Effect.flip(store.read(folder, "a"))).toBeInstanceOf(MissingFile);
          expect((yield* store.commit(folder, "a", two, { absent: true })).version).toBe(3);
          expect(yield* Effect.flip(store.commit(folder, "a", one, { version: 1 }))).toBeInstanceOf(
            PreconditionFailed,
          );
          expect(
            yield* Effect.flip(store.commit(folder, "a", one, { absent: true })),
          ).toBeInstanceOf(PreconditionFailed);
          expect(
            yield* Effect.flip(
              store.remove(folder, "a", { txid: "bad", author: "test" }, { absent: true }),
            ),
          ).toBeInstanceOf(InvalidFile);
          const changes = (yield* files.collect({ folderId: folder })).items;
          expect(
            changes.map((change) => ("value" in change ? change.value.version : null)),
          ).toEqual([1, 2, null, 3]);
          expect(changes[2]).not.toHaveProperty("value");
          // Replaying one event per append into another owner changes storage positions, never versions.
          yield* files.create({ folderId: "copy" });

          for (const change of changes) yield* files.append({ folderId: "copy" }, [change]);
          expect((yield* files.collect({ folderId: "copy" })).items).toEqual(changes);

          // A fresh storage owner has no source offsets or owner records to consult.
          // Replay the wire changes, including their value versions, unchanged.
          const fresh = yield* Layer.build(Layer.fresh(storage));
          yield* Effect.gen(function* () {
            const destination = yield* Files;
            yield* destination.create({ folderId: folder });

            for (const change of changes) yield* destination.append({ folderId: folder }, [change]);
            const replayed = (yield* destination.collect({ folderId: folder })).items;
            expect(replayed).toEqual(changes);
            expect(
              replayed.map((change) => ("value" in change ? change.value.version : null)),
            ).toEqual([1, 2, null, 3]);
          }).pipe(Effect.provideContext(fresh));
        }),
    );
    it.effect(
      "same-hash no-op returns first txid before checking precondition; moved retry gets 412",
      () =>
        Effect.gen(function* () {
          const store = yield* FolderStore;
          const files = yield* Files;
          yield* store.create("retry", "Retry", "create-retry");

          const one = {
            ...(yield* prepareContent(new TextEncoder().encode("one"))),
            author: "test",
            txid: "first",
          };

          const accepted = yield* store.commit("retry", "a", one, { absent: true });
          const tail = (yield* files.head({ folderId: "retry" })).nextOffset;
          expect(
            yield* store.commit("retry", "a", { ...one, txid: "second" }, { absent: true }),
          ).toEqual(accepted);
          expect((yield* files.head({ folderId: "retry" })).nextOffset).toBe(tail);

          const two = {
            ...(yield* prepareContent(new TextEncoder().encode("two"))),
            author: "test",
            txid: "moved",
          };

          yield* store.commit("retry", "a", two, { version: 1 });
          expect(
            yield* Effect.flip(store.commit("retry", "a", one, { absent: true })),
          ).toBeInstanceOf(PreconditionFailed);
          yield* store.remove("retry", "a", { author: "test", txid: "delete" });
          const deletedTail = (yield* files.head({ folderId: "retry" })).nextOffset;
          expect(
            yield* store.remove("retry", "a", { author: "test", txid: "repeat" }, { version: 1 }),
          ).toEqual({});
          expect((yield* files.head({ folderId: "retry" })).nextOffset).toBe(deletedTail);
        }),
    );
    it.effect("failed head persistence rolls back append and counter", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        const records = yield* OwnerRecords;
        const files = yield* Files;
        yield* store.create("rollback", "Rollback", "create");

        const body = {
          ...(yield* prepareContent(new TextEncoder().encode("one"))),
          author: "test",
          txid: "first",
        };

        const before = (yield* files.head({ folderId: "rollback" })).nextOffset;

        const context = yield* Layer.build(Layer.fresh(FolderStoreLive)).pipe(
          Effect.provideService(OwnerRecords, {
            ...records,
            write: () =>
              Effect.fail(
                new StorageFault({ operation: "test", message: "disk failed", retryable: false }),
              ),
          }),
        );

        yield* Effect.gen(function* () {
          const failing = yield* FolderStore;
          yield* Effect.flip(failing.commit("rollback", "a", body));
        }).pipe(Effect.provideContext(context));
        expect((yield* files.head({ folderId: "rollback" })).nextOffset).toBe(before);
        expect((yield* store.commit("rollback", "a", body)).version).toBe(1);
        // An arbitrary failed transaction cannot consume a number either.
        const boundary = yield* CommitBoundary;
        yield* Effect.flip(
          boundary.withTransaction(
            store.commit("rollback", "b", body).pipe(Effect.andThen(Effect.fail("abort"))),
          ),
        );
        expect((yield* store.commit("rollback", "b", body)).version).toBe(2);

        const append = DurableState.changes(Files.at({ folderId: "rollback" }), {}, [
          DurableState.remove("file", "missing"),
        ]);

        expect(append[0]?.headers.operation).toBe("delete");
      }),
    );
  });
}
