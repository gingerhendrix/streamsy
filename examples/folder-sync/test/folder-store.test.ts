import { layer } from "@effect/vitest";
import { expect, test } from "vitest";
import { DurableState, CommitBoundary } from "@streamsy/core";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { Array, Deferred, Effect, Layer, Option, Order } from "effect";
import {
  BlobMissing,
  DeletedFile,
  FolderConflict,
  InvalidFile,
  MissingFile,
  sha256,
} from "../server/domain.ts";
import {
  CHECKPOINT_EVERY,
  FolderStore,
  FolderStoreLive,
  Initialise,
} from "../server/folder-store.ts";
import { applyFileMessage, emptyView, folderView, RETRY_MEMORY } from "../server/folder-view.ts";
import { Files } from "../server/streams.ts";
import { memoryHost, sqliteHost } from "../server/storage.ts";
import { storedText } from "./content.ts";

const body = (text: string, txid: string, parent: number | null = null) =>
  Effect.map(storedText(text), (content) => ({
    ...content,
    author: "test",
    txid,
    parent,
  }));

/** A file upsert as the owner writes it, for folds of the view. */
const upsert = (path: string, version: number, txid: string) =>
  DurableState.changes(Files.at({ folderId: "fold" }), {}, [
    DurableState.upsert(
      "file",
      { path, version, parent: null, hash: "a".repeat(64), size: 1, author: "test" },
      { headers: { txid } },
    ),
  ])[0];

test("the retry memory keeps the last RETRY_MEMORY changes of a folder, oldest out first", () => {
  let view = emptyView();

  for (let version = 1; version <= RETRY_MEMORY + 1; version++) {
    const change = upsert(`f${version % 7}`, version, `t${version}`);

    if (change !== undefined) view = applyFileMessage(view, change);
  }

  expect(view.recent.size).toBe(RETRY_MEMORY);
  expect(view.recent.has("t1")).toBe(false);
  expect(view.recent.get("t2")).toEqual({
    path: "f2",
    version: 2,
    parent: null,
    hash: "a".repeat(64),
  });
  expect([...view.recent.keys()].at(-1)).toBe(`t${RETRY_MEMORY + 1}`);
  // A reset clears the paths and keeps the memory, so a retry from before it does not land again.
  view = applyFileMessage(view, { headers: { control: "reset" } });
  expect(view.files.size).toBe(0);
  expect(view.recent.size).toBe(RETRY_MEMORY);
});

for (const [name, storage] of [
  ["memory", memoryHost],
  ["sqlite", sqliteHost(":memory:")],
] as const) {
  const host = Layer.merge(Initialise, FolderStoreLive).pipe(Layer.provideMerge(storage));
  layer(host)(name, (it) => {
    it.effect(
      "versions rise across paths; a delete is a tombstone with a version and a parent",
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

          const one = yield* body("one", "one");
          const two = yield* body("two", "two");

          expect((yield* store.commit(folder, "a", one)).version).toBe(1);
          expect((yield* store.commit(folder, "b", two)).version).toBe(2);
          expect(
            yield* store.remove(folder, "a", { txid: "delete", author: "test", parent: 1 }),
          ).toEqual({ version: 3, parent: 1, txid: "delete" });
          expect(yield* Effect.flip(store.read(folder, "a"))).toEqual(
            new DeletedFile({ version: 3 }),
          );
          // A file made again names the delete as its parent.
          expect(yield* store.commit(folder, "a", { ...two, txid: "again", parent: 3 })).toEqual({
            version: 4,
            parent: 3,
            hash: two.hash,
            txid: "again",
          });
          // A parent must be a version of this folder.
          expect(
            yield* Effect.flip(store.commit(folder, "a", { ...one, txid: "future", parent: 9 })),
          ).toBeInstanceOf(InvalidFile);
          const changes = (yield* files.collect({ folderId: folder })).items;
          expect(
            Array.map(changes, (change) => ("value" in change ? change.value.version : null)),
          ).toEqual([1, 2, 3, 4]);
          expect(changes[2]).toMatchObject({
            headers: { operation: "upsert", txid: "delete" },
            value: { path: "a", version: 3, parent: 1, author: "test", deleted: true },
          });
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
              Array.map(replayed, (change) => ("value" in change ? change.value.version : null)),
            ).toEqual([1, 2, 3, 4]);
          }).pipe(Effect.provideContext(fresh));
        }),
    );
    it.effect(
      "a retry gets its first answer after it is overtaken; a txid for another change gets 400",
      () =>
        Effect.gen(function* () {
          const store = yield* FolderStore;
          const files = yield* Files;
          yield* store.create("retry", "Retry", "create-retry");

          const tail = () =>
            Effect.map(files.head({ folderId: "retry" }), (head) => head.nextOffset);

          const one = yield* body("one", "first");
          const accepted = yield* store.commit("retry", "a", one);
          const start = yield* tail();
          // Rule 1: the head already has this content.
          expect(yield* store.commit("retry", "a", { ...one, txid: "second" })).toEqual(accepted);
          expect(yield* tail()).toBe(start);

          // B lands from the same parent as a late retry of A. The retry does not hide B.
          const two = yield* body("two", "moved", null);
          expect((yield* store.commit("retry", "a", two)).version).toBe(2);
          const moved = yield* tail();
          expect(yield* store.commit("retry", "a", one)).toEqual(accepted);
          expect(yield* tail()).toBe(moved);
          expect((yield* store.read("retry", "a")).head.txid).toBe("moved");

          // The memory checks the path and the kind of change.
          expect(yield* Effect.flip(store.commit("retry", "b", one))).toBeInstanceOf(InvalidFile);
          expect(
            yield* Effect.flip(
              store.remove("retry", "a", { author: "test", txid: "first", parent: 1 }),
            ),
          ).toBeInstanceOf(InvalidFile);

          // A delete retry comes before rule 1, so it gets its version after the file is gone.
          const deleted = { author: "test", txid: "delete", parent: 2 };
          expect(yield* store.remove("retry", "a", deleted)).toEqual({
            version: 3,
            parent: 2,
            txid: "delete",
          });
          const deletedTail = yield* tail();
          expect(yield* store.remove("retry", "a", deleted)).toEqual({
            version: 3,
            parent: 2,
            txid: "delete",
          });
          expect(
            yield* store.remove("retry", "a", { author: "test", txid: "repeat", parent: 2 }),
          ).toEqual({});
          expect(yield* tail()).toBe(deletedTail);
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
        yield* store.remove("rebuild", "b", { txid: "delete", author: "test", parent: 2 });
        const before = yield* store.read("rebuild", "a");
        const view = folderView.at({ folderId: "rebuild" });
        yield* Projection.run(view);
        const saved = (yield* Projection.read(view)).state;
        expect(saved.last).toBe(3);

        yield* checkpoints.remove(Projection.key(view));
        expect((yield* Projection.read(view)).state).toEqual(emptyView());
        expect(yield* store.read("rebuild", "a")).toEqual(before);
        expect(yield* Effect.flip(store.read("rebuild", "b"))).toEqual(
          new DeletedFile({ version: 3 }),
        );
        // A cold fold gives the same view, the retry memory too.
        expect((yield* Projection.current(view)).state).toEqual(saved);
        expect((yield* store.commit("rebuild", "b", yield* body("one", "third", 3))).version).toBe(
          4,
        );
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
          const created = yield* store.commit("proto", path, first);
          const second = yield* body(`second ${path}`, `update-${index}`, created.version);
          yield* store.commit("proto", path, second);
          expect((yield* store.read("proto", path)).value.hash).toBe(second.hash);
        }

        // The saved checkpoint decodes every name as a map entry, not as an object member.
        const view = folderView.at({ folderId: "proto" });
        yield* Projection.run(view);
        const saved = (yield* Projection.read(view)).state;
        expect(Array.sort([...saved.files.keys()], Order.String)).toEqual(
          Array.sort(names, Order.String),
        );
        expect(saved.files.get("__proto__")?.version).toBe(names.length * 2 - 4);
        expect((yield* store.read("proto", "__proto__")).value.hash).toBe(
          yield* sha256(new TextEncoder().encode("second __proto__")),
        );
        yield* store.remove("proto", "constructor", { txid: "delete", author: "test", parent: 2 });
        expect(yield* Effect.flip(store.read("proto", "constructor"))).toBeInstanceOf(DeletedFile);
      }),
    );
    it.effect("the view and the stream hold metadata only; a read names the blob", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        const files = yield* Files;
        yield* store.create("meta", "Meta", "create");
        yield* store.commit("meta", "a", yield* body("alpha", "a1"));
        yield* store.commit("meta", "b", yield* body("beta", "b1"));
        const two = yield* body("alpha two", "a2", 1);
        yield* store.commit("meta", "a", two);

        const view = folderView.at({ folderId: "meta" });
        yield* Projection.run(view);
        const saved = (yield* Projection.read(view)).state.files.get("a");
        expect(saved).toEqual({
          txid: "a2",
          version: 3,
          parent: 1,
          hash: two.hash,
          size: 9,
          author: "test",
        });
        expect(yield* store.read("meta", "a")).toEqual({
          head: { version: 3, parent: 1, hash: two.hash, txid: "a2" },
          value: { path: "a", version: 3, parent: 1, hash: two.hash, size: 9, author: "test" },
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
        const missing = { hash, size: bytes.length, author: "test", txid: "missing", parent: null };
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
      "two writes from one parent both land; the loser of the append race folds again and is the head",
      () =>
        Effect.gen(function* () {
          const store = yield* FolderStore;
          const files = yield* Files;
          const checkpoints = yield* CheckpointStore;
          yield* store.create("race", "Race", "create");
          yield* store.commit("race", "a", yield* body("base", "base"));
          const left = yield* body("left", "left", 1);
          const right = yield* body("right", "right", 1);

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
              [racers.commit("race", "a", left), racers.commit("race", "a", right)],
              { concurrency: "unbounded" },
            );
          }).pipe(Effect.provideContext(context));

          expect(
            Array.sort(
              Array.map(results, (head) => head.version),
              Order.Number,
            ),
          ).toEqual([2, 3]);
          expect(Array.map(results, (head) => head.parent)).toEqual([1, 1]);

          const last = Option.getOrUndefined(
            Array.findFirst(results, (head) => head.version === 3),
          );

          expect((yield* store.read("race", "a")).head).toEqual(last);
          // Two racing loads, the winner's first-write checkpoint, and the loser's second fold.
          expect(loads).toBe(4);
          expect((yield* files.collect({ folderId: "race" })).items).toHaveLength(3);
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
            store
              .commit("rollback", "b", { ...one, txid: "aborted" })
              .pipe(Effect.andThen(Effect.fail("abort"))),
          ),
        );
        expect((yield* files.head({ folderId: "rollback" })).nextOffset).toBe(before);
        // The aborted txid did not land, so the retry memory does not know it.
        expect((yield* store.commit("rollback", "b", { ...one, txid: "aborted" })).version).toBe(2);

        const append = DurableState.changes(Files.at({ folderId: "rollback" }), {}, [
          DurableState.remove("file", "missing"),
        ]);

        expect(append[0]).toMatchObject({ headers: { operation: "delete" } });
      }),
    );
  });
}
