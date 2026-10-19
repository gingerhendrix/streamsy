import { Projection } from "@streamsy/projection";
import { Cause, Context, Effect, Fiber, Layer, Schema, Schedule, Stream } from "effect";
import { SqlClient } from "effect/sql";
import picomatch from "picomatch";
import { InvalidFile } from "./domain.ts";
import { Folders, ListedFile } from "./streams.ts";
import { initialiseSearch, search } from "./projections.ts";
import { SearchResult, snippetParts } from "./search-result.ts";

export { SearchResult } from "./search-result.ts";

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const list = Effect.fn("FileIndex.list")(function* (
    options: { folder?: string; prefix?: string; glob?: string } = {},
  ) {
    const glob =
      options.glob === undefined
        ? undefined
        : yield* Effect.try({
            try: () =>
              picomatch(options.glob ?? "", { dot: true, strictBrackets: true, posix: true }),
            catch: () => new InvalidFile({ message: "Invalid glob pattern" }),
          });

    const rows = yield* Schema.decodeUnknownEffect(Schema.Array(ListedFile))(
      yield* sql`SELECT * FROM folder_sync_manifest ORDER BY folderId, path`,
    );

    return rows.filter(
      (row) =>
        (options.folder === undefined || row.folderId === options.folder) &&
        (options.prefix === undefined || row.path.startsWith(options.prefix)) &&
        (glob === undefined || glob(row.path)),
    );
  });

  const find = Effect.fn("FileIndex.search")(function* (q: string, folder?: string) {
    if (q.trim() === "") return [];

    const rows = yield* sql
      .unsafe<{ folderId: string; path: string; version: number; snippet: string }>(
        `SELECT folderId, path, version, snippet(folder_sync_search, 3, char(1), char(2), '…', 24) AS snippet
       FROM folder_sync_search WHERE folder_sync_search MATCH ?
       ${folder === undefined ? "" : "AND folderId = ?"} ORDER BY rank, folderId, path LIMIT 100`,
        folder === undefined ? [q] : [q, folder],
      )
      .pipe(
        Effect.catchIf(
          (error) =>
            /fts5:|unterminated string|no such column|syntax error/i.test(
              String(error.reason.cause),
            ),
          () => Effect.fail(new InvalidFile({ message: "Invalid FTS5 query" })),
        ),
      );

    return yield* Schema.decodeEffect(Schema.Array(SearchResult))(
      rows.map((row) => ({ ...row, snippet: snippetParts(row.snippet) })),
    );
  });

  return { list, search: find };
});

export class FileIndex extends Context.Service<FileIndex, Effect.Success<typeof make>>()(
  "FolderSync/FileIndex",
) {}

export const FileIndexLive = Layer.effect(FileIndex, make);

class FollowerFailed extends Schema.TaggedError<FollowerFailed>()("FollowerFailed", {}) {}

/** A stopped binding is logged and restarted, including defects from its handler. */
export const supervise = <A, E, R>(name: string, work: Effect.Effect<A, E, R>) =>
  work.pipe(
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterruptsOnly(cause),
      (cause) =>
        Effect.logError(`Index follower ${name} failed; restarting in one second`, cause).pipe(
          Effect.andThen(Effect.fail(new FollowerFailed())),
        ),
    ),
    Effect.andThen(
      Effect.logWarning(`Index follower ${name} stopped; restarting in one second`).pipe(
        Effect.andThen(Effect.fail(new FollowerFailed())),
      ),
    ),
    Effect.retry({ schedule: Schedule.spaced("1 second") }),
  );

/** Folder discovery owns one direct source follower per folder. No derived feed. */
export const IndexLive = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* initialiseSearch;
    const folders = yield* Folders;
    const scope = yield* Effect.scope;
    const started = new Set<string>();

    const start = Effect.fn("Index.start")(function* (folderId: string) {
      if (started.has(folderId)) return;
      const bound = search.at({ folderId });
      yield* Projection.run(bound);
      yield* supervise(folderId, Projection.follow(bound).pipe(Effect.flatMap(Fiber.join))).pipe(
        Effect.forkIn(scope),
      );
      started.add(folderId);
    });

    // Replay the small registry on every runtime start to restore all folder bindings.
    yield* supervise(
      "folders",
      folders
        .follow({})
        .pipe(
          Stream.runForEach((batch) =>
            Effect.forEach(
              batch.items,
              (change) => ("value" in change ? start(change.value.folderId) : Effect.void),
              { discard: true },
            ),
          ),
        ),
    ).pipe(Effect.forkIn(scope));
  }),
);
