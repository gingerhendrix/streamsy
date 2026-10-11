import { BunServices } from "@effect/platform-bun";
import { expect, layer } from "@streamsy/effect-test";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { Clock, Console, Effect, Exit, Layer, Result, Schedule, Schema, Scope } from "effect";
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

const metadata = (opId: string, base: string | null = null) => ({
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
      const scope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
        Scope.close(scope, Exit.void),
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
        `folder-sync rebuild: 44 commits, 32 original files, ~213 KiB text, ${elapsed} ms, readLimit=1`,
      );
      expect(yield* manifestRows).toEqual(live);
      expect(yield* index.search("durable")).toEqual(liveSearch);
      const rebuiltFeed = (yield* changes.collect({})).items;

      const order = (rows: typeof rebuiltFeed) =>
        rows.toSorted(
          (a, b) => a.folderId.localeCompare(b.folderId) || a.version.localeCompare(b.version),
        );

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
