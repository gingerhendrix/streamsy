import { expect, layer } from "@streamsy/effect-test";
import { ZERO_OFFSET } from "@streamsy/core";
import { Deferred, Effect, Fiber, Layer } from "effect";
import { InvalidFile, MissingFile } from "../server/domain.ts";
import {
  FolderStore,
  FolderStoreLive,
  Initialise,
  NoWriteHook,
  WriteHook,
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
                .pipe(
                  Effect.mapError(() => new InvalidFile({ message: "Hook append failed" })),
                  Effect.andThen(Effect.fail(new InvalidFile({ message: "Hook rejected" }))),
                ),
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
