/** SQLite application boundary over the generic maintained-view store. */
import { Database } from "bun:sqlite";
import {
  OutboxStore,
  outboxStore,
  type OutboxBacking,
  type OutboxDraft,
} from "@streamsy/effect-sink";
import { createSqliteOutboxBacking } from "@streamsy/effect-sink/sqlite";
import {
  importLegacyIssueStore,
  migrateViewStore,
  sqliteService,
} from "@streamsy/views-store/sqlite";
import { Context, Effect, Layer } from "effect";
import { planHash } from "@streamsy/views";
import { decodeCatalogRow, type CatalogRow } from "../domain/catalog.ts";
import { issueLifecycle, issues } from "../domain/declaration.ts";
import type { CommandKind } from "./commands.ts";
import { CommandIdConflict, StoreRestorePoison, StoreUnavailable } from "./errors.ts";
import {
  IssueStore,
  issueStoreAdapter,
  type CommandReceipt,
  type IssueStoreBoundary,
} from "./store.ts";

const COMMAND_RECEIPTS_SCHEMA = `CREATE TABLE IF NOT EXISTS command_receipts (
  workspace_id TEXT NOT NULL,
  command_id   TEXT NOT NULL,
  command_kind TEXT NOT NULL,
  target_id    TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  event_id     TEXT NOT NULL,
  event_sequence INTEGER NOT NULL,
  event_offset TEXT NOT NULL,
  PRIMARY KEY (workspace_id, command_id)
);`;

/** Slice 1 tables stay present so migration is recoverable and receipts remain app-owned. */
const APPLICATION_SCHEMA = `
CREATE TABLE IF NOT EXISTS view_rows (
  workspace_id TEXT NOT NULL, row_key TEXT NOT NULL, value TEXT NOT NULL,
  PRIMARY KEY (workspace_id, row_key)
);
CREATE TABLE IF NOT EXISTS reducer_state (
  workspace_id TEXT NOT NULL, row_key TEXT NOT NULL, value TEXT NOT NULL,
  PRIMARY KEY (workspace_id, row_key)
);
CREATE TABLE IF NOT EXISTS view_progress (
  workspace_id TEXT PRIMARY KEY, checkpoint TEXT, published TEXT,
  next_sequence INTEGER NOT NULL DEFAULT 0
);
${COMMAND_RECEIPTS_SCHEMA}
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

interface ProgressRow {
  readonly published: string | null;
  readonly next_sequence: number;
}
interface ReceiptRow {
  readonly command_id: string;
  readonly workspace_id: string;
  readonly command_kind: CommandKind;
  readonly target_id: string;
  readonly request_hash: string;
  readonly event_id: string;
  readonly event_sequence: number;
  readonly event_offset: string;
}

interface StateProgressRow {
  readonly checkpoint: string | null;
}

interface ValueRow {
  readonly row_key: string;
  readonly value: string;
}

interface TableInfoRow {
  readonly name: string;
}

/**
 * Slice 1 receipts lack canonical intent. Replace only that application table;
 * accepted commands remain recoverable from the canonical issue-event source.
 */
function migrateCommandReceipts(database: Database): void {
  const columns = database.query<TableInfoRow, []>("PRAGMA table_info(command_receipts)").all();
  if (columns.some((column) => column.name === "request_hash")) return;
  database.transaction(() => {
    database.exec("DROP TABLE command_receipts");
    database.exec(COMMAND_RECEIPTS_SCHEMA);
  })();
}

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
 * The open database, as its own scoped service.
 *
 * The maintained-state store and the effect sink's outbox are two surfaces over
 * one file, and they must be: an atomic receipt-and-enqueue is only possible if
 * both run on the same connection. Naming the connection is what lets two
 * layers share it without either one owning the other.
 */
class IssueDatabase extends Context.Service<IssueDatabase, Database>()(
  "issue-tracker/IssueDatabase",
) {}

const databaseLayer = (options: SqliteStoreOptions): Layer.Layer<IssueDatabase> =>
  Layer.effect(
    IssueDatabase,
    Effect.acquireRelease(
      Effect.sync(() => {
        const database = new Database(options.filename, { create: true });
        database.exec("PRAGMA journal_mode = WAL");
        database.exec("PRAGMA foreign_keys = ON");
        database.exec(APPLICATION_SCHEMA);
        migrateCommandReceipts(database);
        migrateViewStore(database);
        importLegacyIssueStore(database, {
          planName: issues.name,
          planHash: planHash(issues.plan),
          partition: "main",
          sourceId: "issue-tracker.issue-events",
          relationId: issues.name,
          reducerId: issueLifecycle.ref.name,
          reducerVersion: issueLifecycle.ref.version,
        });
        return database;
      }),
      (database) => Effect.sync(() => database.close(false)),
    ),
  );

export const sqliteLayer = (options: SqliteStoreOptions): Layer.Layer<IssueStore | OutboxStore> =>
  Layer.merge(
    Layer.effect(
      IssueStore,
      Effect.gen(function* () {
        const database = yield* IssueDatabase;
        return issueStoreAdapter(
          sqliteService(database),
          boundary(database, createSqliteOutboxBacking(database)),
        );
      }),
    ),
    Layer.effect(
      OutboxStore,
      Effect.gen(function* () {
        const database = yield* IssueDatabase;
        return outboxStore(createSqliteOutboxBacking(database));
      }),
    ),
  ).pipe(Layer.provide(databaseLayer(options)));

function boundary(database: Database, outbox: OutboxBacking): IssueStoreBoundary {
  const selectProgress = database.query<ProgressRow, [string]>(
    "SELECT published, next_sequence FROM view_progress WHERE workspace_id = ?",
  );
  const upsertPublished = database.query<never, [string, string]>(
    "INSERT INTO view_progress (workspace_id, published) VALUES (?, ?)" +
      " ON CONFLICT (workspace_id) DO UPDATE SET published = excluded.published",
  );
  const upsertSequence = database.query<never, [string, number]>(
    "INSERT INTO view_progress (workspace_id, next_sequence) VALUES (?, ?)" +
      " ON CONFLICT (workspace_id) DO UPDATE SET next_sequence = MAX(view_progress.next_sequence, excluded.next_sequence)",
  );
  const selectReceipt = database.query<ReceiptRow, [string, string]>(
    "SELECT * FROM command_receipts WHERE workspace_id = ? AND command_id = ?",
  );
  const insertReceipt = database.query<
    never,
    [string, string, string, string, string, string, number, string]
  >(
    "INSERT INTO command_receipts" +
      " (workspace_id, command_id, command_kind, target_id, request_hash," +
      " event_id, event_sequence, event_offset) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" +
      " ON CONFLICT (workspace_id, command_id) DO NOTHING",
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
   * The receipt and the deliveries it implies, in one transaction.
   *
   * `ON CONFLICT DO NOTHING` on both writes makes the whole step idempotent, so
   * a retried command that reaches here again re-derives the same rows and
   * changes nothing.
   */
  const commitReceipt = database.transaction(
    (receipt: CommandReceipt, deliveries: readonly OutboxDraft[]) => {
      insertReceipt.run(
        receipt.workspaceId,
        receipt.commandId,
        receipt.commandKind,
        receipt.targetId,
        receipt.requestHash,
        receipt.eventId,
        receipt.eventSequence,
        receipt.eventOffset,
      );
      if (deliveries.length > 0) outbox.enqueue(deliveries);
    },
  );

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

  return {
    progress: (workspaceId) =>
      sqlite("progress", () => {
        const row = selectProgress.get(workspaceId);
        return { published: row?.published ?? undefined, nextSequence: row?.next_sequence ?? 0 };
      }),
    markPublished: (workspaceId, position) =>
      sqlite("markPublished", () => {
        upsertPublished.run(workspaceId, position);
      }),
    updateNextSequence: (workspaceId, nextSequence) =>
      sqlite("updateNextSequence", () => {
        upsertSequence.run(workspaceId, nextSequence);
      }),
    receipt: (workspaceId, commandId) =>
      sqlite("receipt", () => {
        const row = selectReceipt.get(workspaceId, commandId);
        if (row === null) return undefined;
        return {
          commandId: row.command_id,
          workspaceId: row.workspace_id,
          commandKind: row.command_kind,
          targetId: row.target_id,
          requestHash: row.request_hash,
          eventId: row.event_id,
          eventSequence: row.event_sequence,
          eventOffset: row.event_offset,
        } satisfies CommandReceipt;
      }),
    recordReceipt: (receipt, deliveries) =>
      Effect.gen(function* () {
        yield* sqlite("recordReceipt", () => commitReceipt(receipt, deliveries));
        const stored = yield* sqlite("recordReceipt.verify", () =>
          selectReceipt.get(receipt.workspaceId, receipt.commandId),
        );
        if (
          stored === null ||
          stored === undefined ||
          stored.command_kind !== receipt.commandKind ||
          stored.target_id !== receipt.targetId ||
          stored.request_hash !== receipt.requestHash ||
          stored.event_id !== receipt.eventId ||
          stored.event_sequence !== receipt.eventSequence ||
          stored.event_offset !== receipt.eventOffset
        ) {
          return yield* new CommandIdConflict({
            workspaceId: receipt.workspaceId,
            commandId: receipt.commandId,
          });
        }
        return undefined;
      }),
    stateCheckpoint: (sourceId, partitionId) =>
      sqlite(
        "stateCheckpoint",
        () => selectStateProgress.get(sourceId, partitionId)?.checkpoint ?? undefined,
      ),
    stateRows: (sourceId, collection, partitionId) =>
      Effect.gen(function* () {
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
    commitState: (sourceId, partitionId, input) =>
      sqlite("commitState", () =>
        commitStateTransaction(sourceId, partitionId, input.checkpoint, input.rows),
      ),
  };
}
