/**
 * The SQLite maintained-state store.
 *
 * One `bun:sqlite` database holds the maintained rows, the reducer state, the
 * source checkpoint, the published position, and the command receipts. The
 * advance is one transaction: rows, reducer state and the checkpoint commit
 * together, so a crash can leave the view behind the source but never ahead of
 * it, and never half-folded.
 *
 * The Durable Streams themselves live in their own storage adapter. This
 * database is the *view's* durable state, not the log's.
 */
import { Database } from "bun:sqlite";
import { Effect, Layer } from "effect";
import type { IssueRow } from "../domain/issue.ts";
import { decodeCatalogRow, type CatalogRow } from "../domain/catalog.ts";
import { StoreRestorePoison, StoreUnavailable } from "./errors.ts";
import {
  IssueStore,
  restoreRow,
  type CommandReceipt,
  type CommitInput,
  type IssueStoreService,
} from "./store.ts";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS view_rows (
  workspace_id TEXT NOT NULL,
  row_key      TEXT NOT NULL,
  value        TEXT NOT NULL,
  PRIMARY KEY (workspace_id, row_key)
);
CREATE TABLE IF NOT EXISTS reducer_state (
  workspace_id TEXT NOT NULL,
  row_key      TEXT NOT NULL,
  value        TEXT NOT NULL,
  PRIMARY KEY (workspace_id, row_key)
);
CREATE TABLE IF NOT EXISTS view_progress (
  workspace_id  TEXT PRIMARY KEY,
  checkpoint    TEXT,
  published     TEXT,
  next_sequence INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS command_receipts (
  command_id   TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  issue_id     TEXT NOT NULL,
  offset_token TEXT NOT NULL,
  event_id     TEXT NOT NULL,
  sequence     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS source_state_rows (
  source_id    TEXT NOT NULL,
  partition_id TEXT NOT NULL,
  row_key      TEXT NOT NULL,
  value        TEXT NOT NULL,
  PRIMARY KEY (source_id, partition_id, row_key)
);
CREATE TABLE IF NOT EXISTS source_progress (
  source_id    TEXT NOT NULL,
  partition_id TEXT NOT NULL,
  checkpoint   TEXT,
  PRIMARY KEY (source_id, partition_id)
);
`;

interface ValueRow {
  readonly row_key: string;
  readonly value: string;
}

interface ProgressRow {
  readonly checkpoint: string | null;
  readonly published: string | null;
  readonly next_sequence: number;
}

interface ReceiptRow {
  readonly command_id: string;
  readonly workspace_id: string;
  readonly issue_id: string;
  readonly offset_token: string;
  readonly event_id: string;
  readonly sequence: number;
}

interface StateProgressRow {
  readonly checkpoint: string | null;
}

/** Wrap one synchronous SQLite operation as a typed failure rather than a throw. */
const sqlite = <A>(operation: string, run: () => A): Effect.Effect<A, StoreUnavailable> =>
  Effect.try({
    try: run,
    catch: (cause) =>
      new StoreUnavailable({
        operation,
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });

export interface SqliteStoreOptions {
  readonly filename: string;
}

/**
 * Open the database for the lifetime of the layer's scope and close it on
 * release, so a host that disposes its runtime releases the file handle too.
 */
export const sqliteLayer = (options: SqliteStoreOptions): Layer.Layer<IssueStore> =>
  Layer.effect(
    IssueStore,
    Effect.acquireRelease(
      Effect.sync(() => {
        const database = new Database(options.filename, { create: true });
        database.exec("PRAGMA journal_mode = WAL");
        database.exec("PRAGMA foreign_keys = ON");
        database.exec(SCHEMA);
        return database;
      }),
      (database) => Effect.sync(() => database.close(false)),
    ).pipe(Effect.map(service)),
  );

function service(database: Database): IssueStoreService {
  const selectProgress = database.query<ProgressRow, [string]>(
    "SELECT checkpoint, published, next_sequence FROM view_progress WHERE workspace_id = ?",
  );
  const selectState = database.query<ValueRow, [string, string]>(
    "SELECT row_key, value FROM reducer_state WHERE workspace_id = ? AND row_key = ?",
  );
  const selectRows = database.query<ValueRow, [string]>(
    "SELECT row_key, value FROM view_rows WHERE workspace_id = ? ORDER BY row_key",
  );
  const upsertRow = database.query<never, [string, string, string]>(
    "INSERT INTO view_rows (workspace_id, row_key, value) VALUES (?, ?, ?)" +
      " ON CONFLICT (workspace_id, row_key) DO UPDATE SET value = excluded.value",
  );
  const upsertState = database.query<never, [string, string, string]>(
    "INSERT INTO reducer_state (workspace_id, row_key, value) VALUES (?, ?, ?)" +
      " ON CONFLICT (workspace_id, row_key) DO UPDATE SET value = excluded.value",
  );
  const upsertCheckpoint = database.query<never, [string, string, number]>(
    "INSERT INTO view_progress (workspace_id, checkpoint, next_sequence) VALUES (?, ?, ?)" +
      " ON CONFLICT (workspace_id) DO UPDATE SET checkpoint = excluded.checkpoint," +
      " next_sequence = MAX(view_progress.next_sequence, excluded.next_sequence)",
  );
  const upsertPublished = database.query<never, [string, string]>(
    "INSERT INTO view_progress (workspace_id, published) VALUES (?, ?)" +
      " ON CONFLICT (workspace_id) DO UPDATE SET published = excluded.published",
  );
  const selectReceipt = database.query<ReceiptRow, [string]>(
    "SELECT * FROM command_receipts WHERE command_id = ?",
  );
  const insertReceipt = database.query<never, [string, string, string, string, string, number]>(
    "INSERT INTO command_receipts" +
      " (command_id, workspace_id, issue_id, offset_token, event_id, sequence)" +
      " VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (command_id) DO NOTHING",
  );
  const selectStateProgress = database.query<StateProgressRow, [string, string]>(
    "SELECT checkpoint FROM source_progress WHERE source_id = ? AND partition_id = ?",
  );
  const selectSourceRows = database.query<ValueRow, [string, string]>(
    "SELECT row_key, value FROM source_state_rows" +
      " WHERE source_id = ? AND partition_id = ? ORDER BY row_key",
  );
  const upsertSourceRow = database.query<never, [string, string, string, string]>(
    "INSERT INTO source_state_rows (source_id, partition_id, row_key, value)" +
      " VALUES (?, ?, ?, ?) ON CONFLICT (source_id, partition_id, row_key)" +
      " DO UPDATE SET value = excluded.value",
  );
  const upsertSourceProgress = database.query<never, [string, string, string]>(
    "INSERT INTO source_progress (source_id, partition_id, checkpoint) VALUES (?, ?, ?)" +
      " ON CONFLICT (source_id, partition_id) DO UPDATE SET checkpoint = excluded.checkpoint",
  );

  /**
   * The atomic advance. `db.transaction` rolls back on a throw, so a failed
   * row write cannot leave the checkpoint claiming work that did not land.
   */
  const commitTransaction = database.transaction((workspaceId: string, input: CommitInput) => {
    for (const [key, row] of input.rows) {
      const value = JSON.stringify(row);
      upsertRow.run(workspaceId, key, value);
      upsertState.run(workspaceId, key, value);
    }
    upsertCheckpoint.run(workspaceId, input.checkpoint, input.nextSequence);
  });
  const commitStateTransaction = database.transaction(
    (
      sourceId: string,
      partitionId: string,
      checkpoint: string,
      rows: ReadonlyMap<string, CatalogRow>,
    ) => {
      for (const [key, row] of rows) {
        upsertSourceRow.run(sourceId, partitionId, key, JSON.stringify(row));
      }
      upsertSourceProgress.run(sourceId, partitionId, checkpoint);
    },
  );

  return IssueStore.of({
    progress: Effect.fn("IssueStore.progress")(function* (workspaceId: string) {
      const row = yield* sqlite("progress", () => selectProgress.get(workspaceId));
      return {
        checkpoint: row?.checkpoint ?? undefined,
        published: row?.published ?? undefined,
      };
    }),
    reducerStates: Effect.fn("IssueStore.reducerStates")(function* (
      workspaceId: string,
      keys: readonly string[],
    ) {
      const restored = new Map<string, IssueRow>();
      for (const key of keys) {
        const row = yield* sqlite("reducerStates", () => selectState.get(workspaceId, key));
        if (row === null || row === undefined) continue;
        restored.set(key, yield* restoreRow("reducer_state", key, row.value));
      }
      return restored;
    }),
    rows: Effect.fn("IssueStore.rows")(function* (workspaceId: string) {
      const found = yield* sqlite("rows", () => selectRows.all(workspaceId));
      const restored: IssueRow[] = [];
      for (const row of found) {
        restored.push(yield* restoreRow("view_rows", row.row_key, row.value));
      }
      return restored;
    }),
    commit: Effect.fn("IssueStore.commit")(function* (workspaceId: string, input: CommitInput) {
      yield* sqlite("commit", () => commitTransaction(workspaceId, input));
    }),
    markPublished: Effect.fn("IssueStore.markPublished")(function* (
      workspaceId: string,
      position: string,
    ) {
      yield* sqlite("markPublished", () => upsertPublished.run(workspaceId, position));
    }),
    receipt: Effect.fn("IssueStore.receipt")(function* (commandId: string) {
      const row = yield* sqlite("receipt", () => selectReceipt.get(commandId));
      if (row === null || row === undefined) return undefined;
      return {
        commandId: row.command_id,
        workspaceId: row.workspace_id,
        issueId: row.issue_id,
        offset: row.offset_token,
        eventId: row.event_id,
        sequence: row.sequence,
      } satisfies CommandReceipt;
    }),
    recordReceipt: Effect.fn("IssueStore.recordReceipt")(function* (receipt: CommandReceipt) {
      yield* sqlite("recordReceipt", () =>
        insertReceipt.run(
          receipt.commandId,
          receipt.workspaceId,
          receipt.issueId,
          receipt.offset,
          receipt.eventId,
          receipt.sequence,
        ),
      );
    }),
    nextSequence: Effect.fn("IssueStore.nextSequence")(function* (workspaceId: string) {
      const row = yield* sqlite("nextSequence", () => selectProgress.get(workspaceId));
      return row?.next_sequence ?? 0;
    }),
    stateCheckpoint: Effect.fn("IssueStore.stateCheckpoint")(function* (
      sourceId: string,
      partitionId: string,
    ) {
      const row = yield* sqlite("stateCheckpoint", () =>
        selectStateProgress.get(sourceId, partitionId),
      );
      return row?.checkpoint ?? undefined;
    }),
    stateRows: Effect.fn("IssueStore.stateRows")(function* (sourceId, collection, partitionId) {
      const found = yield* sqlite("stateRows", () => selectSourceRows.all(sourceId, partitionId));
      const restored: CatalogRow[] = [];
      for (const row of found) {
        const decoded = yield* Effect.try({
          try: () => decodeCatalogRow(collection, JSON.parse(row.value)).row,
          catch: (cause) =>
            new StoreRestorePoison({
              table: "source_state_rows",
              key: row.row_key,
              detail: cause instanceof Error ? cause.message : String(cause),
            }),
        });
        restored.push(decoded);
      }
      return restored;
    }),
    commitState: Effect.fn("IssueStore.commitState")(function* (sourceId, partitionId, input) {
      yield* sqlite("commitState", () =>
        commitStateTransaction(sourceId, partitionId, input.checkpoint, input.rows),
      );
    }),
  });
}
