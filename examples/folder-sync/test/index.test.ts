import { StorageFault, StreamAddress, StreamGone, ZERO_OFFSET } from "@streamsy/core";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { Effect, Layer, Logger, Result, Schedule, Schema, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { FolderStore, FolderStoreLive, Initialise } from "../server/folder-store.ts";
import { FileIndex, FileIndexLive } from "../server/file-index.ts";
import { IndexLive, runIndex } from "../server/index-followers.ts";
import {
  applyIndexChanges,
  initialiseSearch,
  prepareIndexChanges,
  search,
  SEARCH_LIMIT,
} from "../server/search-index.ts";
import type { FileMessage } from "../server/messages.ts";
import { BlobStore } from "../server/blob-store.ts";
import { Precondition, prepareContent } from "../server/domain.ts";
import { Files, Folders } from "../server/streams.ts";
import { IndexRunner, IndexRunnerLive } from "../worker/index-runner.ts";
import { registerFolder } from "../server/folder-registry.ts";
import { sqliteHost } from "../server/storage.ts";
import { storedText } from "./content.ts";

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

const fileMessage = (path: string, version = 1): FileMessage => ({
  type: "file",
  key: path,
  value: { path, version, hash: "0".repeat(64), size: 6, author: "test" },
  headers: { operation: "upsert", txid: path },
});

const reset: FileMessage = { headers: { control: "reset" } };

layer(Layer.fresh(graph), { excludeTestServices: true })((it) => {
  it.effect("Bun discovery consumes a stored registry reset without replaying it", () =>
    Effect.gen(function* () {
      const store = yield* FolderStore;
      const folders = yield* Folders;
      const index = yield* FileIndex;
      yield* store.create("discarded", "Discarded", "discarded");
      yield* store.create("retained", "Retained", "retained");

      for (const folderId of ["discarded", "retained"])
        yield* store.commit(folderId, "a", {
          ...(yield* storedText("needle")),
          txid: folderId,
          author: "test",
        });

      const retained = (yield* folders.collect({})).items.find(
        (change) => StreamAddress.isChange(change) && change.key === "retained",
      );

      const data = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.Unknown)))([
        reset,
        retained,
      ]);

      yield* folders.wire.append(Folders.at({}).id, {
        contentType: "application/json",
        data: new TextEncoder().encode(data),
      });
      yield* Layer.build(IndexLive);
      yield* until(index.list({ folder: "retained" }), (rows) => rows.length === 1);
      expect(yield* index.list({ folder: "discarded" })).toEqual([]);
    }),
  );
});

layer(graph, { excludeTestServices: true })((it) => {
  it.effect("a gone bootstrap folder does not block registry discovery of a healthy folder", () =>
    Effect.gen(function* () {
      const store = yield* FolderStore;
      const files = yield* Files;
      const index = yield* FileIndex;
      yield* store.create("gone-bootstrap", "Gone", "gone-bootstrap");
      yield* store.create("healthy-bootstrap", "Healthy", "healthy-bootstrap");

      for (const folderId of ["gone-bootstrap", "healthy-bootstrap"])
        yield* store.commit(folderId, "a", {
          ...(yield* storedText("needle")),
          txid: folderId,
          author: "test",
        });
      const goneId = Files.at({ folderId: "gone-bootstrap" }).id;
      const messages: unknown[] = [];
      const logger = Logger.make(({ message }) => messages.push(message));
      yield* Layer.build(IndexLive).pipe(
        Effect.provideService(Files, {
          ...files,
          wire: {
            ...files.wire,
            read: (id, options) =>
              id === goneId ? Effect.fail(new StreamGone({ id })) : files.wire.read(id, options),
          },
        }),
        Effect.provideService(Logger.CurrentLoggers, new Set([logger])),
      );
      yield* until(index.list({ folder: "healthy-bootstrap" }), (rows) => rows.length === 1);
      expect(yield* index.list({ folder: "gone-bootstrap" })).toEqual([]);
      expect(
        messages.filter((entry) =>
          String(entry).includes("gone-bootstrap history is unavailable at bootstrap"),
        ),
      ).toHaveLength(1);
      expect(
        messages.some((entry) => String(entry).includes("Index follower folders failed")),
      ).toBe(false);
    }),
  );

  it.effect("Bun indexes a stored reset and following changes once, keeping its cursor", () =>
    Effect.gen(function* () {
      yield* initialiseSearch;
      const store = yield* FolderStore;
      const files = yield* Files;
      const index = yield* FileIndex;
      yield* store.create("stored-reset", "Reset", "folder");
      yield* files.wire.append(Files.at({ folderId: "stored-reset" }).id, {
        contentType: "application/json",
        data: new TextEncoder().encode(
          yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.Unknown)))([
            fileMessage("before"),
            reset,
            fileMessage("after", 2),
          ]),
        ),
      });
      yield* runIndex("stored-reset");
      expect((yield* index.list({ folder: "stored-reset" })).map((row) => row.path)).toEqual([
        "after",
      ]);

      const before = (yield* Projection.read(search.at({ folderId: "stored-reset" }))).offsets
        .input;

      yield* runIndex("stored-reset");
      expect((yield* Projection.read(search.at({ folderId: "stored-reset" }))).offsets.input).toBe(
        before,
      );
    }),
  );

  it.effect("Bun rebuilds a gone cursor from the beginning and rejects a gone bootstrap", () =>
    Effect.gen(function* () {
      yield* initialiseSearch;
      const store = yield* FolderStore;
      const files = yield* Files;
      yield* store.create("gone-index", "Gone", "folder");
      yield* store.commit("gone-index", "a", {
        ...(yield* storedText("needle")),
        txid: "one",
        author: "test",
      });
      yield* runIndex("gone-index");
      const reads: string[] = [];
      let gone = false;

      const wrapped = {
        ...files,
        wire: {
          ...files.wire,
          read: (
            id: Parameters<typeof files.wire.read>[0],
            options?: Parameters<typeof files.wire.read>[1],
          ) => {
            reads.push(options?.offset ?? ZERO_OFFSET);

            if (!gone) {
              gone = true;

              return Effect.fail(new StreamGone({ id }));
            }

            return files.wire.read(id, options);
          },
        },
      };

      yield* runIndex("gone-index").pipe(Effect.provideService(Files, wrapped));
      expect(reads[0]).not.toBe(ZERO_OFFSET);
      expect(reads.slice(1)).toContain(ZERO_OFFSET);
      let attempts = 0;

      const failed = yield* runIndex("gone-index").pipe(
        Effect.provideService(Files, {
          ...files,
          wire: {
            ...files.wire,
            read: (id) => {
              attempts++;

              return Effect.fail(new StreamGone({ id }));
            },
          },
        }),
        Effect.result,
      );

      expect(Result.isFailure(failed)).toBe(true);
      expect(attempts).toBe(2);
    }),
  );

  it.effect("Worker runner indexes registered folders and skips one with no stream yet", () =>
    Effect.gen(function* () {
      yield* initialiseSearch;
      const store = yield* FolderStore;
      const index = yield* FileIndex;
      const content = yield* storedText("needle");
      yield* store.create("runner-a", "A", "runner-a");
      // The Worker registers a folder before its object creates the stream.
      yield* registerFolder({ folderId: "runner-pending", title: "Pending", txid: "pending" });
      yield* store.commit("runner-a", "a.md", { ...content, author: "test", txid: "a" });

      // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Each test builds one runner over the shared test graph.
      const runner = yield* IndexRunner.pipe(Effect.provide(IndexRunnerLive));
      yield* runner.catchup;

      const paths = index
        .list({ folder: "runner-a" })
        .pipe(Effect.map((rows) => rows.map((row) => row.path)));

      expect(yield* paths).toEqual(["a.md"]);

      // A ping folds on from the saved checkpoint.
      yield* store.commit("runner-a", "b.md", { ...content, author: "test", txid: "b" });
      yield* runner.moved("runner-a");
      expect(yield* paths).toEqual(["a.md", "b.md"]);
      yield* runner.moved("runner-pending");
    }),
  );

  it.effect("Worker catch-up reports a failing folder and still indexes the others", () =>
    Effect.gen(function* () {
      yield* initialiseSearch;
      const store = yield* FolderStore;
      const index = yield* FileIndex;
      const files = yield* Files;
      const content = yield* storedText("needle");
      yield* store.create("runner-broken", "Broken", "runner-broken");
      yield* store.create("runner-healthy", "Healthy", "runner-healthy");
      yield* store.commit("runner-healthy", "h.md", { ...content, author: "test", txid: "h" });
      const broken = Files.at({ folderId: "runner-broken" }).id;

      const runner = yield* IndexRunner.pipe(
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- The runner reads through a Files service with one failing folder.
        Effect.provide(IndexRunnerLive),
        Effect.provideService(Files, {
          ...files,
          wire: {
            ...files.wire,
            read: (id, options) =>
              id === broken
                ? Effect.fail(
                    new StorageFault({ operation: "read", message: "down", retryable: true }),
                  )
                : files.wire.read(id, options),
          },
        }),
      );

      expect(Result.isFailure(yield* Effect.result(runner.catchup))).toBe(true);
      expect(yield* index.list({ folder: "runner-healthy" })).toHaveLength(1);
    }),
  );

  it.effect("blob page preparation stages each prefix before fetching the next", () =>
    Effect.gen(function* () {
      yield* initialiseSearch;
      const sql = yield* SqlClient.SqlClient;
      const blobs = yield* BlobStore;
      const bytes = new TextEncoder().encode("needle " + "x".repeat(SEARCH_LIMIT));

      const changes = Array.from({ length: 12 }, (_, i): FileMessage => {
        const row = fileMessage(`large-${i}`);

        if (!("value" in row)) throw new Error("Expected upsert");

        return { ...row, value: { ...row.value, size: bytes.length } };
      });

      let fetched = 0;

      const prepared = yield* prepareIndexChanges("staged", changes).pipe(
        Effect.provideService(BlobStore, {
          ...blobs,
          get: () =>
            Effect.gen(function* () {
              const rows = yield* sql<{
                count: number;
              }>`SELECT count(*) AS count FROM folder_sync_index_text WHERE folderId = 'staged'`.pipe(
                Effect.orDie,
              );

              expect(rows[0]?.count).toBe(fetched++);

              return { size: bytes.length, body: Stream.succeed(bytes) };
            }),
        }),
      );

      expect(prepared).toEqual(changes);
      yield* sql.withTransaction(applyIndexChanges("staged", prepared));
      expect(
        (yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM folder_sync_search WHERE folderId = 'staged'`)[0]?.count,
      ).toBe(12);
      expect(yield* sql`SELECT * FROM folder_sync_index_text`).toEqual([]);
    }),
  );

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
          ...(yield* storedText(path)),
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
        ...(yield* storedText("nextneedle")),
        author: "test",
        txid: "next",
      });
      yield* Projection.run(projection).pipe(
        Effect.provideService(BlobStore, {
          ...blobs,
          get: (hash) =>
            hash === content.hash
              ? Effect.die("Missing blob must not be read again")
              : blobs.get(hash),
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
        ...(yield* storedText("oldneedle")),
        txid: "first",
        author: "Zoë",
      };

      yield* store.commit("opaque/id", "docs/a.md", body);
      yield* until(index.search("oldneedle"), (rows) => rows.length === 1);
      yield* store.commit(
        "opaque/id",
        "docs/a.md",
        {
          ...(yield* storedText("newneedle")),
          txid: "second",
          author: "Zoë",
        },
        Precondition.IfMatch({ version: 1 }),
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
        ...(yield* storedText("rollbackneedle")),
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
