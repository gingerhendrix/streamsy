import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { DurableState, CommitBoundary } from "@streamsy/core";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { Deferred, Effect, Layer, Result } from "effect";
import {
  BlobMissing,
  FolderConflict,
  InvalidFile,
  MissingFile,
  Precondition,
  PreconditionFailed,
  sha256,
} from "../server/domain.ts";
import {
  CHECKPOINT_EVERY,
  FolderStore,
  FolderStoreLive,
  Initialise,
} from "../server/folder-store.ts";
import { folderView } from "../server/folder-view.ts";
import { Files } from "../server/streams.ts";
import { memoryHost, sqliteHost } from "../server/storage.ts";
import { storedText } from "./content.ts";

const body = (text: string, txid: string) =>
  Effect.map(storedText(text), (content) => ({
    ...content,
    author: "test",
    txid,
  }));

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
            ...(yield* storedText("one")),
            author: "test",
            txid: "one",
          };

          const two = {
            ...(yield* storedText("two")),
            author: "test",
            txid: "two",
          };

          expect((yield* store.commit(folder, "a", one, Precondition.IfAbsent())).version).toBe(1);
          expect((yield* store.commit(folder, "b", two)).version).toBe(2);
          yield* store.remove(
            folder,
            "a",
            { txid: "delete", author: "test" },
            Precondition.IfMatch({ version: 1 }),
          );
          expect(yield* Effect.flip(store.read(folder, "a"))).toBeInstanceOf(MissingFile);
          expect((yield* store.commit(folder, "a", two, Precondition.IfAbsent())).version).toBe(3);
          expect(
            yield* Effect.flip(
              store.commit(folder, "a", one, Precondition.IfMatch({ version: 1 })),
            ),
          ).toBeInstanceOf(PreconditionFailed);
          expect(
            yield* Effect.flip(store.commit(folder, "a", one, Precondition.IfAbsent())),
          ).toBeInstanceOf(PreconditionFailed);
          expect(
            yield* Effect.flip(
              store.remove(folder, "a", { txid: "bad", author: "test" }, Precondition.IfAbsent()),
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
            ...(yield* storedText("one")),
            author: "test",
            txid: "first",
          };

          const accepted = yield* store.commit("retry", "a", one, Precondition.IfAbsent());
          const tail = (yield* files.head({ folderId: "retry" })).nextOffset;
          expect(
            yield* store.commit("retry", "a", { ...one, txid: "second" }, Precondition.IfAbsent()),
          ).toEqual(accepted);
          expect((yield* files.head({ folderId: "retry" })).nextOffset).toBe(tail);

          const two = {
            ...(yield* storedText("two")),
            author: "test",
            txid: "moved",
          };

          yield* store.commit("retry", "a", two, Precondition.IfMatch({ version: 1 }));
          expect(
            yield* Effect.flip(store.commit("retry", "a", one, Precondition.IfAbsent())),
          ).toBeInstanceOf(PreconditionFailed);
          yield* store.remove("retry", "a", { author: "test", txid: "delete" });
          const deletedTail = (yield* files.head({ folderId: "retry" })).nextOffset;
          expect(
            yield* store.remove(
              "retry",
              "a",
              { author: "test", txid: "repeat" },
              Precondition.IfMatch({ version: 1 }),
            ),
          ).toEqual({});
          expect((yield* files.head({ folderId: "retry" })).nextOffset).toBe(deletedTail);
        }),
    );
    it.effect("the view rebuilds from the stream after its checkpoint is removed", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        const checkpoints = yield* CheckpointStore;
        yield* store.create("rebuild", "Rebuild", "create");
        const one = yield* body("one", "first");
        const two = yield* body("two", "second");
        yield* store.commit("rebuild", "a", one);
        yield* store.commit("rebuild", "b", two);
        yield* store.remove("rebuild", "b", { txid: "delete", author: "test" });
        const before = yield* store.read("rebuild", "a");
        const view = folderView.at({ folderId: "rebuild" });
        yield* Projection.run(view);
        expect((yield* Projection.read(view)).state.last).toBe(2);

        yield* checkpoints.remove(Projection.key(view));
        expect((yield* Projection.read(view)).state).toEqual({ last: 0, files: new Map() });
        expect(yield* store.read("rebuild", "a")).toEqual(before);
        expect(yield* Effect.flip(store.read("rebuild", "b"))).toBeInstanceOf(MissingFile);
        // The deleted version 2 is still in the retained stream, so the next write gets 3.
        expect((yield* store.commit("rebuild", "b", one)).version).toBe(3);
      }),
    );
    it.effect("root paths named after Object.prototype members are ordinary files", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        yield* store.create("proto", "Proto", "create");
        const names = ["constructor", "toString", "__proto__", "hasOwnProperty", "valueOf"];

        for (const [index, path] of names.entries()) {
          expect(yield* Effect.flip(store.read("proto", path))).toBeInstanceOf(MissingFile);
          const first = yield* body(`first ${path}`, `create-${index}`);
          const created = yield* store.commit("proto", path, first, Precondition.IfAbsent());
          const second = yield* body(`second ${path}`, `update-${index}`);
          yield* store.commit(
            "proto",
            path,
            second,
            Precondition.IfMatch({ version: created.version }),
          );
          expect((yield* store.read("proto", path)).value.hash).toBe(second.hash);
        }

        // The saved checkpoint decodes every name as a map entry, not as an object member.
        const view = folderView.at({ folderId: "proto" });
        yield* Projection.run(view);
        const saved = (yield* Projection.read(view)).state;
        expect([...saved.files.keys()].toSorted()).toEqual(names.toSorted());
        expect(saved.files.get("__proto__")?.version).toBe(names.length * 2 - 4);
        expect((yield* store.read("proto", "__proto__")).value.hash).toBe(
          yield* sha256(new TextEncoder().encode("second __proto__")),
        );
        yield* store.remove("proto", "constructor", { txid: "delete", author: "test" });
        expect(yield* Effect.flip(store.read("proto", "constructor"))).toBeInstanceOf(MissingFile);
      }),
    );
    it.effect("the view and the stream hold metadata only; a read names the blob", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        const files = yield* Files;
        yield* store.create("meta", "Meta", "create");
        yield* store.commit("meta", "a", yield* body("alpha", "a1"));
        yield* store.commit("meta", "b", yield* body("beta", "b1"));
        const two = yield* body("alpha two", "a2");
        yield* store.commit("meta", "a", two);

        const view = folderView.at({ folderId: "meta" });
        yield* Projection.run(view);
        const saved = (yield* Projection.read(view)).state.files.get("a");
        expect(saved).toEqual({ txid: "a2", version: 3, hash: two.hash, size: 9, author: "test" });
        expect(yield* store.read("meta", "a")).toEqual({
          head: { version: 3, hash: two.hash, txid: "a2" },
          value: { path: "a", version: 3, hash: two.hash, size: 9, author: "test" },
        });

        // No change on the stream carries content.
        for (const change of (yield* files.collect({ folderId: "meta" })).items)
          expect(change).not.toHaveProperty("value.text");

        // A copy of the stream reads the same, because a read does not use stream offsets.
        const changes = (yield* files.collect({ folderId: "meta" })).items;
        yield* files.create({ folderId: "meta-copy" });

        for (const change of [changes[2], changes[1]])
          if (change !== undefined) yield* files.append({ folderId: "meta-copy" }, [change]);
        expect((yield* store.read("meta-copy", "a")).value.hash).toBe(two.hash);
      }),
    );
    it.effect("a commit names stored content: a missing or wrong-size blob gets BlobMissing", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        yield* store.create("blobs", "Blobs", "create");
        const bytes = new TextEncoder().encode("never uploaded");
        const hash = yield* sha256(bytes);
        const missing = { hash, size: bytes.length, author: "test", txid: "missing" };
        expect(yield* Effect.flip(store.commit("blobs", "a", missing))).toBeInstanceOf(BlobMissing);

        const stored = yield* body("stored", "stored");
        expect(
          yield* Effect.flip(store.commit("blobs", "a", { ...stored, size: stored.size + 1 })),
        ).toBeInstanceOf(BlobMissing);
        expect((yield* store.commit("blobs", "a", stored)).version).toBe(1);
      }),
    );
    it.effect("a failing checkpoint save runs once per CHECKPOINT_EVERY writes", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        const checkpoints = yield* CheckpointStore;
        yield* store.create("unsaved", "Unsaved", "create");
        let saves = 0;

        const failing = CheckpointStore.of({
          ...checkpoints,
          compareAndSet: () =>
            Effect.suspend(() => {
              saves++;

              return Effect.fail(
                new ProjectionFault({
                  phase: "checkpoint",
                  reason: "storage-failure",
                  message: "row too large",
                }),
              );
            }),
        });

        const context = yield* Layer.build(Layer.fresh(FolderStoreLive)).pipe(
          Effect.provideService(CheckpointStore, failing),
        );

        const writes = CHECKPOINT_EVERY * 2 + 1;

        yield* Effect.gen(function* () {
          const writer = yield* FolderStore;

          for (let index = 1; index <= writes; index++)
            yield* writer.commit("unsaved", `f${index}`, yield* body(`text ${index}`, `t${index}`));
        }).pipe(Effect.provideContext(context));

        // The first write, write CHECKPOINT_EVERY + 1 and write 2 * CHECKPOINT_EVERY + 1.
        expect(saves).toBe(3);
        expect((yield* store.read("unsaved", `f${writes}`)).head.version).toBe(writes);
      }),
    );
    it.effect("a checkpoint that lags the tail still gives the tail's heads", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        const files = yield* Files;
        yield* store.create("lag", "Lag", "create");
        const view = folderView.at({ folderId: "lag" });
        const writes = CHECKPOINT_EVERY + 5;

        for (let index = 1; index <= writes; index++)
          yield* store.commit("lag", `f${index}`, yield* body(`text ${index}`, `t${index}`));
        const saved = yield* Projection.read(view);
        const tail = (yield* files.head({ folderId: "lag" })).nextOffset;
        expect(saved.offsets.files).not.toBe(tail);
        expect(saved.state.last).toBeLessThan(writes);
        expect((yield* store.read("lag", `f${writes}`)).head).toMatchObject({
          version: writes,
          txid: `t${writes}`,
        });
        expect((yield* store.commit("lag", "next", yield* body("next", "next"))).version).toBe(
          writes + 1,
        );
      }),
    );
    it.effect(
      "two writes with the same If-Match: one lands, the loser folds again and gets 412",
      () =>
        Effect.gen(function* () {
          const store = yield* FolderStore;
          const files = yield* Files;
          const checkpoints = yield* CheckpointStore;
          yield* store.create("race", "Race", "create");
          yield* store.commit("race", "a", yield* body("base", "base"));
          const left = yield* body("left", "left");
          const right = yield* body("right", "right");

          // Hold the first two view loads until both have started, so both commits decide from version 1.
          let loads = 0;
          const bothRead = yield* Deferred.make<void>();

          const racing = CheckpointStore.of({
            ...checkpoints,
            load: (key) =>
              Effect.gen(function* () {
                loads++;

                if (loads === 2) yield* Deferred.succeed(bothRead, undefined);

                if (loads <= 2) yield* Deferred.await(bothRead);

                return yield* checkpoints.load(key);
              }),
          });

          const context = yield* Layer.build(Layer.fresh(FolderStoreLive)).pipe(
            Effect.provideService(CheckpointStore, racing),
          );

          const results = yield* Effect.gen(function* () {
            const racers = yield* FolderStore;

            return yield* Effect.all(
              [
                Effect.result(
                  racers.commit("race", "a", left, Precondition.IfMatch({ version: 1 })),
                ),
                Effect.result(
                  racers.commit("race", "a", right, Precondition.IfMatch({ version: 1 })),
                ),
              ],
              { concurrency: "unbounded" },
            );
          }).pipe(Effect.provideContext(context));

          const landed = results.filter(Result.isSuccess);
          const refused = results.filter(Result.isFailure);
          expect(landed).toHaveLength(1);
          expect(refused).toHaveLength(1);
          expect(refused[0]?.failure).toBeInstanceOf(PreconditionFailed);
          expect(landed[0]?.success.version).toBe(2);
          // Two racing loads, the winner's first-write checkpoint, and the loser's second fold.
          expect(loads).toBe(4);
          expect((yield* files.collect({ folderId: "race" })).items).toHaveLength(2);
        }),
    );
    it.effect("an aborted transaction cannot consume a version", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        const files = yield* Files;
        yield* store.create("rollback", "Rollback", "create");
        const one = yield* body("one", "first");
        expect((yield* store.commit("rollback", "a", one)).version).toBe(1);
        const before = (yield* files.head({ folderId: "rollback" })).nextOffset;
        const boundary = yield* CommitBoundary;
        yield* Effect.flip(
          boundary.withTransaction(
            store.commit("rollback", "b", one).pipe(Effect.andThen(Effect.fail("abort"))),
          ),
        );
        expect((yield* files.head({ folderId: "rollback" })).nextOffset).toBe(before);
        expect((yield* store.commit("rollback", "b", one)).version).toBe(2);

        const append = DurableState.changes(Files.at({ folderId: "rollback" }), {}, [
          DurableState.remove("file", "missing"),
        ]);

        expect(append[0]).toMatchObject({ headers: { operation: "delete" } });
      }),
    );
  });
}
