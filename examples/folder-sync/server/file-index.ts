import { CommitBoundary, ZERO_OFFSET } from "@streamsy/core";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { Cause, Context, Effect, Fiber, Layer, Schema, Schedule, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { InvalidFile } from "./domain.ts";
import { Changes, Files, Folders, Manifest, ManifestFile, folderHead } from "./streams.ts";
import {
  folderChanges,
  folderTriggers,
  initialiseSearch,
  manifest,
  search,
} from "./projections.ts";

import picomatch from "picomatch";
import { feedEpoch } from "./feed-epoch.ts";

import { SearchResult, snippetParts } from "./search-result.ts";

export { SearchResult } from "./search-result.ts";

/** Replays Durable State events into their current live rows. No SQL shadow manifest. */
export const manifestRows = Effect.gen(function* () {
  const stream = yield* Manifest;

  const rows = yield* stream.fold(
    {},
    () => new Map<string, ManifestFile>(),
    (state, event) => {
      if ("value" in event) state.set(event.key, event.value);
      else state.delete(event.key);

      return state;
    },
  );

  return [...rows.state.values()].toSorted(
    (a, b) => a.folderId.localeCompare(b.folderId) || a.path.localeCompare(b.path),
  );
});

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const stream = yield* Manifest;
  const boundary = yield* CommitBoundary;
  const checkpoints = yield* CheckpointStore;

  const list = Effect.fn("FileIndex.list")(function* (
    options: {
      folder?: string;
      prefix?: string;
      glob?: string;
    } = {},
  ) {
    const glob =
      options.glob === undefined
        ? undefined
        : yield* Effect.try({
            try: () =>
              picomatch(options.glob ?? "", { dot: true, strictBrackets: true, posix: true }),
            catch: () => new InvalidFile({ message: "Invalid glob pattern" }),
          });

    const rows = yield* manifestRows.pipe(Effect.provideService(Manifest, stream));

    return rows.filter(
      (row) =>
        (options.folder === undefined || row.folderId === options.folder) &&
        (options.prefix === undefined || row.path.startsWith(options.prefix)) &&
        (glob === undefined || glob(row.path)),
    );
  });

  /** Rows and their consumed feed offset share the manifest writer's SQL boundary. */
  const snapshot = Effect.fn("FileIndex.snapshot")(function* (
    options: { folder?: string; prefix?: string; glob?: string } = {},
  ) {
    return yield* boundary.withTransaction(
      Effect.gen(function* () {
        const files = yield* list(options);

        const progress = yield* Projection.read(manifest.at({})).pipe(
          Effect.provideService(CheckpointStore, checkpoints),
        );

        return { files, cursor: progress.offsets.input ?? ZERO_OFFSET, epoch: yield* feedEpoch };
      }),
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

  return { list, snapshot, search: find };
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

/** Each runtime restores all known bindings before tailing the persisted registry checkpoint. */
export const IndexLive = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* initialiseSearch;
    const changes = yield* Changes;
    const state = yield* Manifest;
    const registry = yield* Folders;
    const files = yield* Files;
    const boundary = yield* CommitBoundary;
    yield* changes.create({});
    yield* state.create({});
    const scope = yield* Effect.scope;
    const started = new Set<string>();

    const start = Effect.fn("FolderTriggers.start")(function* (folderId: string) {
      if (started.has(folderId)) return;
      const head = folderHead.at({ folderId });
      // Decide owns the SQL boundary before the projection permit. Use that same lock order.
      yield* boundary.withTransaction(Projection.run(head));
      const bound = folderChanges.at({ folderId });
      // Finish both catch-up runs before forking, so a failed registry attempt leaves no partial bindings.
      yield* Projection.run(bound);
      yield* supervise(
        `head:${folderId}`,
        Effect.gen(function* () {
          // Read again on every restart, after the previous attempt's durable progress.
          const progress = yield* boundary.withTransaction(Projection.read(head));

          yield* files
            .follow({ folderId }, { offset: progress.offsets.input })
            .pipe(Stream.runForEach(() => boundary.withTransaction(Projection.run(head))));
        }),
      ).pipe(Effect.forkIn(scope));
      yield* supervise(
        `changes:${folderId}`,
        Projection.follow(bound).pipe(Effect.flatMap(Fiber.join)),
      ).pipe(Effect.forkIn(scope));
      started.add(folderId);
    });

    for (const folder of (yield* registry.collect({})).items) yield* start(folder.folderId);

    const triggers = folderTriggers(start).at({});
    yield* Projection.run(triggers);
    yield* supervise("registry", Projection.follow(triggers).pipe(Effect.flatMap(Fiber.join))).pipe(
      Effect.forkIn(scope),
    );
    yield* Projection.run(manifest.at({}));
    yield* Projection.run(search.at({}));
    yield* supervise(
      "manifest",
      Projection.follow(manifest.at({})).pipe(Effect.flatMap(Fiber.join)),
    ).pipe(Effect.forkIn(scope));
    yield* supervise(
      "search",
      Projection.follow(search.at({})).pipe(Effect.flatMap(Fiber.join)),
    ).pipe(Effect.forkIn(scope));
  }),
);
