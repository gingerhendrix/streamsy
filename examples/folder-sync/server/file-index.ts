import { Array, Context, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import picomatch from "picomatch";
import { InvalidFile } from "./domain.ts";
import { ListedFile, SearchResult, snippetParts } from "./index-rows.ts";

/** Queries over the index tables. Attached projections fill them. */
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

    return Array.filter(
      rows,
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
      Array.map(rows, (row) => ({ ...row, snippet: snippetParts(row.snippet) })),
    );
  });

  return { list, search: find };
});

export class FileIndex extends Context.Service<FileIndex, Effect.Success<typeof make>>()(
  "FolderSync/FileIndex",
) {}

export const FileIndexLive = Layer.effect(FileIndex, make);
