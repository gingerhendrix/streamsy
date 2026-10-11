import { expect, layer } from "@streamsy/effect-test";
import { ZERO_OFFSET, StorageFault } from "@streamsy/core";
import { Deferred, Effect, Fiber, Layer } from "effect";
import { InvalidFile, MissingFile } from "../server/domain.ts";
import {
  FolderStore,
  FolderStoreLive,
  Initialise,
  NoWriteHook,
  WriteHook,
  WriteHookFailure,
} from "../server/folder-store.ts";
import { Files, Folders } from "../server/streams.ts";
import { memoryHost, sqliteHost } from "../server/storage.ts";

const metadata = (opId: string, base: string | null = null) => ({
  opId,
  base,
  author: "test",
  origin: "test",
});

const bytes = (text: string) => new TextEncoder().encode(text);

for (const [name, storage] of [
  ["memory", memoryHost],
  ["sqlite", sqliteHost(":memory:")],
] as const) {
  const host = Layer.merge(Initialise, FolderStoreLive).pipe(
    Layer.provideMerge(NoWriteHook),
    Layer.provideMerge(storage),
  );

  layer(host)(name, (it) => {
    it.effect("opaque folder ids round trip and folder creation is idempotent", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        const folderId = "2026-10-10/a?b#c%/雪";
        const first = yield* store.create(folderId, "Opaque");
        expect(yield* store.create(folderId, "New title")).toEqual(first);
        const folders = yield* Folders;
        expect(
          (yield* folders.collect({})).items.filter((folder) => folder.folderId === folderId),
        ).toHaveLength(1);

        const written = yield* store.write(
          folderId,
          "constructor",
          metadata("opaque"),
          bytes("opaque"),
        );

        expect((yield* store.read(folderId, "constructor")).version).toBe(written.version);
        expect(yield* Effect.flip(store.read(folderId, "__proto__"))).toBeInstanceOf(MissingFile);
      }),
    );
    it.effect("stale writes mark the exact unseen head and opId replay appends nothing", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        yield* store.create("lww", "LWW");
        const first = yield* store.write("lww", "a.md", metadata("first"), bytes("one"));

        const second = yield* store.write(
          "lww",
          "a.md",
          metadata("second", first.version),
          bytes("two"),
        );

        expect(second.overwrote).toBeUndefined();

        const third = yield* store.write(
          "lww",
          "a.md",
          metadata("third", first.version),
          bytes("three"),
        );

        expect(third.overwrote).toBe(second.version);
        expect(
          yield* store.write("lww", "a.md", metadata("third", first.version), bytes("three")),
        ).toEqual(third);
        expect(yield* store.history("lww", "a.md")).toHaveLength(3);
        expect(new TextDecoder().decode((yield* store.read("lww", "a.md")).bytes)).toBe("three");
        expect(
          new TextDecoder().decode((yield* store.read("lww", "a.md", { at: first.version })).bytes),
        ).toBe("one");
        expect(
          yield* Effect.flip(
            store.write(
              "lww",
              "a.md",
              metadata("future", "9999999999999999_0000000000000000"),
              bytes("bad"),
            ),
          ),
        ).toBeInstanceOf(InvalidFile);
      }),
    );
    it.effect(
      "replay after another writer moves the head returns its original receipt without reverting",
      () =>
        Effect.gen(function* () {
          const store = yield* FolderStore;
          const files = yield* Files;
          yield* store.create("replay", "Replay");
          const initial = yield* store.write("replay", "a", metadata("initial"), bytes("initial"));
          const a1 = yield* store.write("replay", "a", metadata("__proto__"), bytes("A1"));
          expect(a1.overwrote).toBe(initial.version);

          const b1 = yield* store.write(
            "replay",
            "a",
            metadata("constructor", a1.version),
            bytes("B1"),
          );

          const tail = (yield* files.head({ folderId: "replay" })).nextOffset;
          expect(yield* store.write("replay", "./a", metadata("__proto__"), bytes("A1"))).toEqual(
            a1,
          );
          expect((yield* files.head({ folderId: "replay" })).nextOffset).toBe(tail);
          expect((yield* store.read("replay", "a")).bytes).toEqual(bytes("B1"));
          expect((yield* store.read("replay", "a")).version).toBe(b1.version);
          expect(yield* store.history("replay", "a")).toHaveLength(3);

          const reused = yield* Effect.flip(
            store.write("replay", "other", metadata("__proto__"), bytes("bad")),
          );

          expect(reused).toMatchObject({ message: "opId already used" });
          expect((yield* files.head({ folderId: "replay" })).nextOffset).toBe(tail);
          expect(yield* store.history("replay", "other")).toEqual([]);
        }),
    );
    it.effect("history has exact per-entry offsets across paths, deletes and recreation", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        yield* store.create("history", "History");
        const first = yield* store.write("history", "a", metadata("first"), bytes("a"));
        const other = yield* store.write("history", "b", metadata("other"), bytes("b"));
        const deleted = yield* store.write("history", "a", metadata("delete", first.version));
        expect(deleted.overwrote).toBeUndefined();
        expect(yield* store.write("history", "a", metadata("delete", first.version))).toEqual(
          deleted,
        );
        expect(yield* Effect.flip(store.read("history", "a"))).toBeInstanceOf(MissingFile);
        expect(
          yield* Effect.flip(store.read("history", "a", { at: other.version })),
        ).toBeInstanceOf(MissingFile);

        const restored = yield* store.write(
          "history",
          "a",
          metadata("restore", first.version),
          bytes("restored"),
        );

        expect(restored.overwrote).toBe(deleted.version);
        expect((yield* store.history("history", "a")).map((row) => row.version)).toEqual([
          first.version,
          deleted.version,
          restored.version,
        ]);
        expect((yield* store.history("history", "b"))[0]?.version).toBe(other.version);
        expect((yield* store.read("history", "a", { at: first.version })).bytes).toEqual(
          bytes("a"),
        );
      }),
    );
    it.effect("delete requires a live head, while known delete replays never append again", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        const files = yield* Files;
        yield* store.create("delete", "Delete");
        expect(
          yield* Effect.flip(store.write("delete", "missing", metadata("missing-delete"))),
        ).toBeInstanceOf(MissingFile);
        expect((yield* files.head({ folderId: "delete" })).nextOffset).toBe(ZERO_OFFSET);
        expect(yield* store.history("delete", "missing")).toEqual([]);
        const put = yield* store.write("delete", "a", metadata("put"), bytes("first"));
        const deleted = yield* store.write("delete", "a", metadata("delete", put.version));
        expect(
          yield* Effect.flip(store.write("delete", "a", metadata("delete-again", deleted.version))),
        ).toBeInstanceOf(MissingFile);
        expect((yield* files.head({ folderId: "delete" })).nextOffset).toBe(deleted.version);
        expect(yield* store.write("delete", "a", metadata("delete", put.version))).toEqual(deleted);

        const restored = yield* store.write(
          "delete",
          "a",
          metadata("restore", deleted.version),
          bytes("restored"),
        );

        expect(yield* store.write("delete", "a", metadata("delete", put.version))).toEqual(deleted);
        expect((yield* files.head({ folderId: "delete" })).nextOffset).toBe(restored.version);
        expect((yield* store.read("delete", "a")).bytes).toEqual(bytes("restored"));
        expect(yield* store.history("delete", "a")).toHaveLength(3);
      }),
    );
    it.effect("concurrent stale edits commit in log order and expose the replaced versions", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        yield* store.create("race", "Race");
        const first = yield* store.write("race", "a", metadata("initial"), bytes("initial"));

        const results = yield* Effect.all(
          [
            store.write("race", "a", metadata("left", first.version), bytes("left")),
            store.write("race", "a", metadata("right", first.version), bytes("right")),
          ],
          { concurrency: 2 },
        );

        const sorted = results.toSorted((a, b) => a.version.localeCompare(b.version));
        expect(sorted[0]?.overwrote).toBeUndefined();
        expect(sorted[1]?.overwrote).toBe(sorted[0]?.version);
        const latest = sorted[1];

        if (latest === undefined) throw new Error("Expected two committed edits");
        expect((yield* store.read("race", "a")).version).toBe(latest.version);
      }),
    );
    it.effect("minVersion waits for the folder log while allowing writes to proceed", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        yield* store.create("wait", "Wait");
        const first = yield* store.write("wait", "a", metadata("first"), bytes("a"));
        const expected = "0000000000000002_0000000000000000";
        const files = yield* Files;
        const parked = yield* Deferred.make<void>();

        const context = yield* Layer.build(Layer.fresh(FolderStoreLive)).pipe(
          Effect.provideService(Files, {
            ...files,
            wire: {
              ...files.wire,
              readNext: (id, options) =>
                Deferred.succeed(parked, undefined).pipe(
                  Effect.andThen(files.wire.readNext(id, options)),
                ),
            },
          }),
        );

        const reader = yield* Effect.gen(function* () {
          const service = yield* FolderStore;

          return yield* service.read("wait", "a", { minVersion: expected });
        }).pipe(Effect.provideContext(context), Effect.forkChild);

        yield* Deferred.await(parked);
        const second = yield* store.write("wait", "b", metadata("second"), bytes("b"));
        expect(second.version).toBe(expected);
        expect((yield* Fiber.join(reader)).version).toBe(first.version);
      }),
    );
    it.effect(
      "hook storage failures retain their cause and roll back both same-owner appends",
      () =>
        Effect.gen(function* () {
          const store = yield* FolderStore;
          const files = yield* Files;
          const folders = yield* Folders;
          yield* store.create("storage-hook", "Hook storage failure");
          const before = (yield* folders.collect({})).items.length;

          const fault = new StorageFault({
            operation: "hook.append",
            message: "disk unavailable",
            retryable: true,
          });

          const context = yield* Layer.build(Layer.fresh(FolderStoreLive)).pipe(
            Effect.provideService(WriteHook, {
              committed: () =>
                folders
                  .append({}, [
                    { folderId: "rolled-back-storage-hook", title: "rollback", created: "test" },
                  ])
                  .pipe(Effect.andThen(Effect.fail(fault))),
            }),
          );

          const failure = yield* Effect.gen(function* () {
            const service = yield* FolderStore;

            return yield* Effect.flip(
              service.write("storage-hook", "a", metadata("hook"), bytes("bad")),
            );
          }).pipe(Effect.provideContext(context));

          expect(failure).toBeInstanceOf(WriteHookFailure);
          expect(failure).toMatchObject({ cause: fault });
          expect((yield* files.head({ folderId: "storage-hook" })).nextOffset).toBe(ZERO_OFFSET);
          expect((yield* folders.collect({})).items).toHaveLength(before);
          const retried = yield* store.write("storage-hook", "a", metadata("hook"), bytes("good"));
          expect((yield* store.read("storage-hook", "a")).version).toBe(retried.version);
          expect(yield* store.history("storage-hook", "a")).toHaveLength(1);
        }),
    );
    it.effect("a failing same-commit hook rolls back the file append and its checkpoint", () =>
      Effect.gen(function* () {
        const store = yield* FolderStore;
        yield* store.create("hook", "Hook");
        const files = yield* Files;
        const folders = yield* Folders;
        const before = (yield* folders.collect({})).items.length;

        const context = yield* Layer.build(Layer.fresh(FolderStoreLive)).pipe(
          Effect.provideService(WriteHook, {
            committed: () =>
              folders
                .append({}, [{ folderId: "hook-side-effect", title: "rollback", created: "test" }])
                .pipe(Effect.andThen(Effect.fail(new InvalidFile({ message: "Hook rejected" })))),
          }),
        );

        const failure = yield* Effect.gen(function* () {
          const service = yield* FolderStore;

          return yield* Effect.flip(service.write("hook", "a", metadata("hook"), bytes("bad")));
        }).pipe(Effect.provideContext(context));

        expect(failure).toBeInstanceOf(InvalidFile);
        expect((yield* files.head({ folderId: "hook" })).nextOffset).toBe(ZERO_OFFSET);
        expect((yield* folders.collect({})).items).toHaveLength(before);
        expect(yield* store.history("hook", "a")).toEqual([]);
        const committed = yield* store.write("hook", "a", metadata("good"), bytes("good"));
        expect((yield* store.read("hook", "a")).version).toBe(committed.version);
      }),
    );
  });
}
