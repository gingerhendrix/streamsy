import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { Effect, Layer, Logger, Result, Schedule, Schema, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { FolderStore, FolderStoreLive, Initialise } from "../server/folder-store.ts";
import { FileIndex, FileIndexLive, IndexLive } from "../server/file-index.ts";
import { initialiseSearch, search, SEARCH_LIMIT } from "../server/projections.ts";
import { BlobStore } from "../server/blob-store.ts";
import { prepareContent } from "../server/domain.ts";
import { sqliteHost } from "../server/storage.ts";

class NotReady extends Schema.TaggedError<NotReady>()("NotReady", {}) {}

const until = <A, E, R>(read: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  read.pipe(
    Effect.filterOrFail(ready, () => new NotReady()),
    Effect.retry({
      while: Schema.is(NotReady),
      schedule: Schedule.spaced("10 millis").pipe(Schedule.upTo({ times: 200 })),
    }),
  );

const graph = Layer.mergeAll(Initialise, FolderStoreLive, FileIndexLive).pipe(
  Layer.provideMerge(sqliteHost(":memory:")),
);

layer(graph, { excludeTestServices: true })((it) => {
  it.effect(
    "search ranks relevant text before alphabetic paths and returns safe snippet parts",
    () =>
      Effect.gen(function* () {
        yield* initialiseSearch;
        const sql = yield* SqlClient.SqlClient;
        const index = yield* FileIndex;
        yield* sql`INSERT INTO folder_sync_search VALUES ('ranking', 'a.md', 1, ${"rankingneedle " + "filler ".repeat(100)})`;
        yield* sql`INSERT INTO folder_sync_search VALUES ('ranking', 'z.md', 2, ${"<img src=x onerror=alert(1)> rankingneedle rankingneedle rankingneedle"})`;
        const hits = yield* index.search("rankingneedle", "ranking");
        expect(hits.map((hit) => hit.path)).toEqual(["z.md", "a.md"]);
        expect(
          hits[0]?.snippet.some((part) => part.match && part.text.includes("rankingneedle")),
        ).toBe(true);
        expect(hits[0]?.snippet.map((part) => part.text).join("")).toContain("<img");
      }),
  );

  it.effect("portable globs preserve Bun's POSIX negated character classes", () =>
    Effect.gen(function* () {
      yield* Layer.build(IndexLive);
      const store = yield* FolderStore;
      const index = yield* FileIndex;
      yield* store.create("glob", "Glob", "glob");
      const paths = ["ab.md", "b.md"];

      for (const path of paths)
        yield* store.commit("glob", path, {
          ...(yield* prepareContent(new TextEncoder().encode(path))),
          txid: path,
          author: "test",
        });
      yield* until(index.list({ folder: "glob" }), (rows) => rows.length === 2);
      const actual = yield* index.list({ folder: "glob", glob: "[!a]*.md" });
      const expected = paths.filter((path) => new Bun.Glob("[!a]*.md").match(path));
      expect(expected).toEqual(["b.md"]);
      expect(actual.map((row) => row.path)).toEqual(expected);
    }),
  );

  it.effect("a missing blob logs a warning, keeps metadata and advances the folder cursor", () =>
    Effect.gen(function* () {
      yield* initialiseSearch;
      const store = yield* FolderStore;
      const blobs = yield* BlobStore;
      const index = yield* FileIndex;
      yield* store.create("lost-blob", "Lost blob", "lost-blob");
      const bytes = new TextEncoder().encode("lostneedle " + "x".repeat(65536));
      const content = yield* prepareContent(bytes);
      yield* blobs.put(content.hash, Stream.succeed(bytes));
      yield* store.commit("lost-blob", "lost", { ...content, author: "test", txid: "lost" });
      const warnings: unknown[] = [];
      const logger = Logger.make(({ message }) => warnings.push(message));
      const projection = search.at({ folderId: "lost-blob" });
      yield* Projection.run(projection).pipe(
        Effect.provideService(BlobStore, {
          ...blobs,
          get: () => Effect.void.pipe(Effect.as(undefined)),
        }),
        Effect.provideService(Logger.CurrentLoggers, new Set([logger])),
      );
      expect(yield* index.list({ folder: "lost-blob" })).toMatchObject([{ path: "lost" }]);
      expect(yield* index.search("lostneedle", "lost-blob")).toEqual([]);
      const before = (yield* Projection.read(projection)).offsets.input;
      expect(before).toBeDefined();
      expect(warnings.some((message) => String(message).includes(content.hash))).toBe(true);
      yield* store.commit("lost-blob", "next", {
        ...(yield* prepareContent(new TextEncoder().encode("nextneedle"))),
        author: "test",
        txid: "next",
      });
      yield* Projection.run(projection).pipe(
        Effect.provideService(BlobStore, {
          ...blobs,
          get: () => Effect.die("Missing blob must not be read again"),
        }),
      );
      expect(yield* index.list({ folder: "lost-blob" })).toHaveLength(2);
      expect(yield* index.search("nextneedle", "lost-blob")).toHaveLength(1);
      expect((yield* Projection.read(projection)).offsets.input).not.toBe(before);
    }),
  );

  it.effect("ls and FTS consume folder streams after writes, replacements and deletes", () =>
    Effect.gen(function* () {
      yield* Layer.build(IndexLive);
      const store = yield* FolderStore;
      const index = yield* FileIndex;
      yield* store.create("opaque/id", "Docs", "folder");

      const body = {
        ...(yield* prepareContent(new TextEncoder().encode("oldneedle"))),
        txid: "first",
        author: "Zoë",
      };

      yield* store.commit("opaque/id", "docs/a.md", body);
      yield* until(index.search("oldneedle"), (rows) => rows.length === 1);
      yield* store.commit(
        "opaque/id",
        "docs/a.md",
        {
          ...(yield* prepareContent(new TextEncoder().encode("newneedle"))),
          txid: "second",
          author: "Zoë",
        },
        { version: 1 },
      );
      yield* until(index.search("newneedle"), (rows) => rows.length === 1);
      expect(yield* index.search("oldneedle")).toEqual([]);
      expect(
        (yield* index.list({ folder: "opaque/id", prefix: "docs/", glob: "**/*.md" }))[0],
      ).toMatchObject({ path: "docs/a.md", version: 2, author: "Zoë" });
      yield* store.remove("opaque/id", "docs/a.md", { txid: "delete", author: "test" });
      yield* until(index.list({ folder: "opaque/id" }), (rows) => rows.length === 0);
      expect(yield* index.search("newneedle")).toEqual([]);
      expect(yield* Effect.flip(index.search('"unterminated'))).toMatchObject({
        message: "Invalid FTS5 query",
      });
      expect(yield* Effect.flip(index.list({ glob: "[" }))).toMatchObject({
        message: "Invalid glob pattern",
      });
    }),
  );
  it.effect("blob indexing stops at 1 MiB, tolerates a split UTF-8 tail, and skips binary", () =>
    Effect.gen(function* () {
      yield* Layer.build(IndexLive);
      const store = yield* FolderStore;
      const blobs = yield* BlobStore;
      const index = yield* FileIndex;
      yield* store.create("cap", "Cap", "create-cap");
      const prefix = "prefixneedle " + "x".repeat(SEARCH_LIMIT - 14);

      for (const [path, bytes] of [
        ["large", new TextEncoder().encode(`${prefix}é suffixneedle`)],
        ["binary", new Uint8Array([0, 255, 1])],
      ] as const) {
        const content = yield* prepareContent(bytes);
        yield* blobs.put(content.hash, Stream.succeed(bytes));
        yield* store.commit("cap", path, {
          hash: content.hash,
          size: content.size,
          author: "test",
          txid: path,
        });
      }

      yield* until(index.list({ folder: "cap" }), (rows) => rows.length === 2);
      expect(yield* index.search("prefixneedle", "cap")).toHaveLength(1);
      expect(yield* index.search("suffixneedle", "cap")).toEqual([]);
    }),
  );
  it.effect("manifest, FTS and the source cursor roll back together", () =>
    Effect.gen(function* () {
      yield* initialiseSearch;
      const store = yield* FolderStore;
      const index = yield* FileIndex;
      const checkpoints = yield* CheckpointStore;
      yield* store.create("rollback-index", "Rollback", "folder");
      yield* store.commit("rollback-index", "a", {
        ...(yield* prepareContent(new TextEncoder().encode("rollbackneedle"))),
        txid: "first",
        author: "test",
      });
      const projection = search.at({ folderId: "rollback-index" });

      const failed = yield* Projection.run(projection).pipe(
        Effect.provideService(CheckpointStore, {
          ...checkpoints,
          compareAndSet: () =>
            Effect.fail(
              new ProjectionFault({
                phase: "checkpoint",
                reason: "storage-failure",
                message: "injected",
              }),
            ),
        }),
        Effect.result,
      );

      expect(Result.isFailure(failed)).toBe(true);
      expect(yield* index.list({ folder: "rollback-index" })).toEqual([]);
      expect(yield* index.search("rollbackneedle")).toEqual([]);
      yield* Projection.run(projection);
      expect(yield* index.list({ folder: "rollback-index" })).toHaveLength(1);
      expect((yield* Projection.read(projection)).offsets.input).not.toBeUndefined();
    }),
  );
});
