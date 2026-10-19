import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import {
  Clock,
  Console,
  Effect,
  Exit,
  Fiber,
  Layer,
  Logger,
  Result,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import { SqlClient } from "effect/sql";
import { FolderStore, FolderStoreLive, Initialise, NoWriteHook } from "../server/folder-store.ts";
import { FileIndex, FileIndexLive, IndexLive, manifestRows } from "../server/file-index.ts";
import {
  folderTriggers,
  indexProjections,
  initialiseSearch,
  manifest,
  search,
} from "../server/projections.ts";
import { BlobStore } from "../server/blob-store.ts";
import { Changes, Files, Manifest, folderHead } from "../server/streams.ts";
import { sqliteHost } from "../server/storage.ts";

class NotReady extends Schema.TaggedError<NotReady>()("NotReady", {}) {}

const until = <A, E, R>(read: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  read.pipe(
    Effect.filterOrFail(ready, () => new NotReady()),
    Effect.retry({
      while: Schema.is(NotReady),
      schedule: Schedule.spaced("10 millis").pipe(Schedule.upTo({ times: 300 })),
    }),
  );

const metadata = (opId: string, base: number | null = null) => ({
  opId,
  base,
  author: "Zoë",
  origin: "test",
});

const bytes = (value: string) => new TextEncoder().encode(value);

const graph = Layer.mergeAll(Initialise, FolderStoreLive, FileIndexLive).pipe(
  Layer.provide(NoWriteHook),
  Layer.provideMerge(sqliteHost(":memory:")),
  Layer.provideMerge(BunServices.layer),
);

layer(Layer.empty, { excludeTestServices: true })((it) => {
  it.effect("portable globs preserve Bun's POSIX negated character classes", () =>
    Effect.gen(function* () {
      yield* Layer.build(IndexLive);
      const store = yield* FolderStore;
      const index = yield* FileIndex;
      yield* store.create("glob", "Glob");
      const paths = ["ab.md", "b.md"];

      for (const path of paths) yield* store.write("glob", path, metadata(path), bytes(path));
      yield* until(index.list({ folder: "glob" }), (rows) => rows.length === 2);
      const actual = yield* index.list({ folder: "glob", glob: "[!a]*.md" });
      const expected = paths.filter((path) => new Bun.Glob("[!a]*.md").match(path));
      expect(expected).toEqual(["b.md"]);
      expect(actual.map((row) => row.path)).toEqual(expected);
    }).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Isolated scoped SQL runtime exercises the list API against the previous Bun matcher.
      Effect.provide(graph),
    ),
  );
  it.effect("a failed search follower logs and resumes its checkpoint", () =>
    Effect.gen(function* () {
      const blobs = yield* BlobStore;
      let reads = 0;
      const messages: unknown[] = [];
      const logger = Logger.make(({ message }) => messages.push(message));
      yield* Layer.build(IndexLive).pipe(
        Effect.provideService(BlobStore, {
          ...blobs,
          get: (hash) => blobs.get(++reads === 1 ? "0".repeat(64) : hash),
        }),
        Effect.provideService(Logger.CurrentLoggers, new Set([logger])),
      );
      const store = yield* FolderStore;
      const index = yield* FileIndex;
      yield* store.create("supervised", "Supervised");
      yield* store.write(
        "supervised",
        "large.md",
        metadata("supervised"),
        bytes("text ".repeat(14000) + "recoveryneedle"),
      );
      yield* until(index.search("recoveryneedle"), (hits) => hits.length === 1);
      expect(reads).toBeGreaterThanOrEqual(2);
      expect(
        messages.some((message) => String(message).includes("search failed; restarting")),
      ).toBe(true);
    }).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Isolated scoped SQL runtime with an injected missing blob.
      Effect.provide(graph),
    ),
  );

  it.effect(
    "search ranks relevant text before alphabetic paths and returns safe snippet parts",
    () =>
      Effect.gen(function* () {
        yield* initialiseSearch;
        const sql = yield* SqlClient.SqlClient;
        const index = yield* FileIndex;
        yield* sql`INSERT INTO folder_sync_search VALUES ('docs', 'a.md', 1, ${"needle " + "filler ".repeat(100)})`;
        yield* sql`INSERT INTO folder_sync_search VALUES ('docs', 'z.md', 2, ${"<img src=x onerror=alert(1)> needle needle needle"})`;
        const hits = yield* index.search("needle");
        expect(hits.map((hit) => hit.path)).toEqual(["z.md", "a.md"]);
        expect(hits[0]?.snippet.some((part) => part.match && part.text.includes("needle"))).toBe(
          true,
        );
        expect(hits[0]?.snippet.map((part) => part.text).join("")).toContain("<img");
      }).pipe(
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Isolated scoped SQL test runtime.
        Effect.provide(graph),
      ),
  );

  it.effect("a manifest snapshot plus its feed cursor misses no burst writes", () =>
    Effect.gen(function* () {
      const store = yield* FolderStore;
      const index = yield* FileIndex;
      const changes = yield* Changes;
      const state = yield* Manifest;
      yield* changes.create({});
      yield* state.create({});

      yield* store.create("burst", "Burst");

      const write = (n: number) =>
        store.write("burst", `file-${n % 20}.md`, metadata(`burst-${n}`), bytes(`value-${n}`));

      const pipeline = indexProjections();
      yield* Effect.forEach(
        Array.from({ length: 10 }, (_, n) => n),
        write,
      );
      yield* Projection.run(pipeline.folderChanges.at({ folderId: "burst" }));
      yield* Projection.run(pipeline.manifest.at({}));
      // Persist ten heads, then let only Changes catch up with the remaining burst.
      yield* Effect.forEach(
        Array.from({ length: 30 }, (_, n) => n + 10),
        write,
      );
      yield* Projection.run(pipeline.folderChanges.at({ folderId: "burst" }));
      const feedTailBeforeBootstrap = (yield* changes.head({})).nextOffset;

      const writer = yield* Effect.forEach(
        Array.from({ length: 40 }, (_, n) => n + 40),
        write,
      ).pipe(Effect.forkScoped);

      const snapshot = yield* index.snapshot();
      expect(snapshot.files).toHaveLength(10);
      expect(snapshot.cursor < feedTailBeforeBootstrap).toBe(true);
      yield* Fiber.join(writer);
      yield* Projection.run(pipeline.folderChanges.at({ folderId: "burst" }));
      expect((yield* changes.collect({})).items).toHaveLength(80);
      const heads = new Map(snapshot.files.map((row) => [row.path, row.version]));
      yield* changes.read({}, { offset: snapshot.cursor }).pipe(
        Stream.runForEach((page) =>
          Effect.sync(() => {
            for (const change of page.items) {
              const known = heads.get(change.path);

              if (known === undefined || change.version > known)
                heads.set(change.path, change.version);
            }
          }),
        ),
      );

      yield* Projection.run(pipeline.manifest.at({}));
      const final = yield* index.list();
      expect(final).toHaveLength(20);

      expect(new Map(final.map((row) => [row.path, row.version]))).toEqual(heads);
    }).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Isolated scoped SQL test runtime.
      Effect.provide(graph),
    ),
  );

  it.effect(
    "live outputs handle slash ids, same-second writes, overwrites, deletes and text blobs",
    () =>
      Effect.gen(function* () {
        yield* Layer.build(IndexLive);
        const store = yield* FolderStore;
        const index = yield* FileIndex;
        const changes = yield* Changes;
        const files = yield* Files;
        yield* store.create("daily/demo", "Demo");
        yield* store.create("other", "Other");
        // Same timestamp is explicit and independent of how fast the test machine runs.
        yield* files.append({ folderId: "daily/demo" }, [
          {
            type: "put",
            path: "same-one.md",
            version: 1,
            hash: "a".repeat(64),
            size: 12,
            inline: "same instant",
            ...metadata("same-one"),
            updatedAt: "2026-10-11T00:00:00.000Z",
          },
        ]);
        yield* files.append({ folderId: "daily/demo" }, [
          {
            type: "put",
            path: "same-two.md",
            version: 2,
            hash: "b".repeat(64),
            size: 12,
            inline: "same instant",
            ...metadata("same-two"),
            updatedAt: "2026-10-11T00:00:00.000Z",
          },
        ]);

        const first = yield* store.write(
          "daily/demo",
          "docs/a.md",
          metadata("one"),
          bytes("oldneedle"),
        );

        const second = yield* store.write(
          "daily/demo",
          "docs/a.md",
          metadata("two", first.version),
          bytes("intermediate"),
        );

        const stale = yield* store.write(
          "daily/demo",
          "docs/a.md",
          metadata("three", first.version),
          bytes("newneedle"),
        );

        const binary = yield* store.write(
          "daily/demo",
          "binary",
          metadata("text"),
          bytes("binaryneedle"),
        );

        yield* store.write(
          "daily/demo",
          "binary",
          metadata("binary", binary.version),
          new Uint8Array([0, 255]),
        );

        const deleted = yield* store.write(
          "daily/demo",
          "gone.md",
          metadata("gone"),
          bytes("deletedneedle"),
        );

        yield* store.write("daily/demo", "gone.md", metadata("delete", deleted.version));
        yield* store.write(
          "daily/demo",
          "large.txt",
          metadata("large"),
          bytes(`largeneedle ${"word ".repeat(14000)}`),
        );
        yield* store.write("other", "docs/a.md", metadata("other"), bytes("newneedle"));
        const result = yield* until(index.search("newneedle"), (rows) => rows.length === 2);
        expect(result.map((row) => row.folderId)).toEqual(["daily/demo", "other"]);
        expect((yield* index.search("newneedle", "daily/demo"))[0]?.version).toBe(stale.version);
        expect(yield* until(index.search("same"), (rows) => rows.length === 2)).toHaveLength(2);
        yield* until(index.search("largeneedle"), (rows) => rows.length === 1);
        expect(yield* index.search("oldneedle OR binaryneedle OR deletedneedle")).toEqual([]);
        yield* until(index.list(), (rows) => rows.length === 6);
        expect(
          (yield* index.list({ folder: "daily/demo", prefix: "docs/", glob: "**/*.md" })).map(
            (row) => row.path,
          ),
        ).toEqual(["docs/a.md"]);
        expect((yield* index.list()).some((row) => row.path === "gone.md")).toBe(false);
        const feed = (yield* changes.collect({})).items;
        expect(feed).toHaveLength(11);
        expect(
          feed.find((row) => row.version === stale.version && row.folderId === "daily/demo")
            ?.overwrote,
        ).toBe(second.version);
        expect(feed.filter((row) => row.updatedAt === "2026-10-11T00:00:00.000Z")).toHaveLength(2);
        expect((yield* index.list({ folder: "daily/demo", prefix: "docs/" }))[0]?.author).toBe(
          "Zoë",
        );
        expect(yield* Effect.flip(index.search('"unterminated'))).toMatchObject({
          message: "Invalid FTS5 query",
        });
      }).pipe(
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Each test is a scoped runtime boundary with isolated SQLite storage.
        Effect.provide(graph, { local: true }),
        Effect.scoped,
      ),
  );

  it.effect("new generations rebuild all index projections from logs to equal live results", () =>
    Effect.gen(function* () {
      const store = yield* FolderStore;
      const index = yield* FileIndex;
      const changes = yield* Changes;
      const state = yield* Manifest;
      const sql = yield* SqlClient.SqlClient;

      const scope = yield* Effect.acquireRelease(Scope.make(), (ownedScope) =>
        Scope.close(ownedScope, Exit.void),
      );

      yield* Layer.build(IndexLive).pipe(Effect.provideService(Scope.Scope, scope));

      for (const folderId of ["section/one", "section/two"]) {
        yield* store.create(folderId, folderId);

        for (let n = 0; n < 16; n++) {
          const first = yield* store.write(
            folderId,
            `docs/${n}.md`,
            metadata(`${n}-first`),
            bytes(`durable corpus ${n} ${"content ".repeat(850)}`),
          );

          if (n % 4 === 0)
            yield* store.write(
              folderId,
              `docs/${n}.md`,
              metadata(`${n}-second`, first.version),
              bytes(`durable updated ${n}`),
            );

          if (n % 8 === 0) yield* store.write(folderId, `docs/${n}.md`, metadata(`${n}-delete`));
        }
      }

      const live = yield* until(index.list(), (rows) => rows.length === 28);
      yield* until(index.search("durable"), (rows) => rows.length === 28);
      // Stop every live runner before deleting outputs and resetting the FTS sink.
      yield* Scope.close(scope, Exit.void);
      const liveSearch = yield* index.search("durable");
      const originalFeed = (yield* changes.collect({})).items;
      yield* changes.remove({});
      yield* state.remove({});
      yield* sql`DELETE FROM folder_sync_search`;
      yield* changes.create({});
      yield* state.create({});
      const rebuilt = indexProjections(2);
      const start = yield* Clock.currentTimeMillis;
      yield* Projection.run(
        folderTriggers(
          (folderId) =>
            Effect.gen(function* () {
              yield* Projection.run({ ...folderHead.at({ folderId }), generation: 3 });
              yield* Projection.run(rebuilt.folderChanges.at({ folderId }));
            }),
          2,
        ).at({}),
      );
      yield* Projection.run(rebuilt.manifest.at({}));
      yield* Projection.run(rebuilt.search.at({}));
      const elapsed = (yield* Clock.currentTimeMillis) - start;
      yield* Console.log(
        `folder-sync rebuild: 44 commits, 32 original files, ~213 KiB text, ${elapsed} ms, readLimit=1000`,
      );
      expect(yield* manifestRows).toEqual(live);
      expect(yield* index.search("durable")).toEqual(liveSearch);
      const rebuiltFeed = (yield* changes.collect({})).items;

      const order = (rows: typeof rebuiltFeed) =>
        rows.toSorted((a, b) => a.folderId.localeCompare(b.folderId) || a.version - b.version);

      expect(order(rebuiltFeed)).toEqual(order(originalFeed));
    }).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Each test is a scoped runtime boundary with isolated SQLite storage.
      Effect.provide(graph, { local: true }),
      Effect.scoped,
    ),
  );

  it.effect("manifest and FTS mutations roll back with their failed checkpoint", () =>
    Effect.gen(function* () {
      yield* initialiseSearch;
      const store = yield* FolderStore;
      const changes = yield* Changes;
      const state = yield* Manifest;
      const checkpoints = yield* CheckpointStore;
      const index = yield* FileIndex;
      yield* changes.create({});
      yield* state.create({});

      yield* store.create("rollback", "Rollback");
      yield* store.write("rollback", "a.md", metadata("rollback"), bytes("rollbackneedle"));
      yield* Projection.run(indexProjections().folderChanges.at({ folderId: "rollback" }));

      const broken = {
        ...checkpoints,
        compareAndSet: () =>
          Effect.fail(
            new ProjectionFault({
              phase: "checkpoint",
              reason: "storage-failure",
              message: "injected",
            }),
          ),
      };

      const manifestFailed = yield* Projection.run(manifest.at({})).pipe(
        Effect.provideService(CheckpointStore, broken),
        Effect.result,
      );

      const searchFailed = yield* Projection.run(search.at({})).pipe(
        Effect.provideService(CheckpointStore, broken),
        Effect.result,
      );

      expect(Result.isFailure(manifestFailed)).toBe(true);
      expect(Result.isFailure(searchFailed)).toBe(true);
      expect(yield* manifestRows).toEqual([]);
      expect(yield* index.search("rollbackneedle")).toEqual([]);
      yield* Projection.run(manifest.at({}));
      yield* Projection.run(search.at({}));
      expect(yield* manifestRows).toHaveLength(1);
      expect(yield* index.search("rollbackneedle")).toHaveLength(1);
    }).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Each test is a scoped runtime boundary with isolated SQLite storage.
      Effect.provide(graph, { local: true }),
      Effect.scoped,
    ),
  );
});
