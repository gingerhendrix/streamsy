/** SQLite application boundary over the generic maintained-view store. */
import { SqliteClient } from "@effect/sql-sqlite-bun";
import {
  OutboxUnavailable,
  OutboxStore,
  outboxStore,
  type OutboxBacking,
} from "@streamsy/effect-sink";
import { OUTBOX_SCHEMA, createSqliteOutboxBacking } from "@streamsy/effect-sink/sqlite";
import {
  importLegacyIssueStore,
  migrateViewStore,
  sqliteService,
} from "@streamsy/views-store/sqlite";
import { Database } from "bun:sqlite";
import { Context, Effect, Layer, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
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
CREATE TABLE IF NOT EXISTS graph_publication (
  workspace_id TEXT NOT NULL,
  product      TEXT NOT NULL,
  revision     TEXT NOT NULL,
  PRIMARY KEY (workspace_id, product)
);
CREATE TABLE IF NOT EXISTS transition_progress (
  workspace_id      TEXT PRIMARY KEY,
  history_epoch     INTEGER,
  history_sequence  INTEGER,
  producer_sequence INTEGER NOT NULL DEFAULT 0
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

interface TransitionProgressRow {
  readonly history_epoch: number | null;
  readonly history_sequence: number | null;
  readonly producer_sequence: number;
}

interface ValueRow {
  readonly row_key: string;
  readonly value: string;
}

interface TableInfoRow {
  readonly name: string;
}

const JsonString = Schema.fromJsonString(Schema.Unknown);
const encodeJsonString = Schema.encodeUnknownSync(JsonString);

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

const isCommandIdConflict = Schema.is(CommandIdConflict);
const sqliteUnavailable = <A>(
  operation: string,
  effect: Effect.Effect<A, OutboxUnavailable | SqlError>,
): Effect.Effect<A, StoreUnavailable> =>
  effect.pipe(
    Effect.mapError(
      (cause) =>
        new StoreUnavailable({
          operation,
          detail: cause instanceof Error ? cause.message : String(cause),
        }),
    ),
  );

const first = <A>(rows: ReadonlyArray<A>): A | undefined => rows[0];
const sameReceipt = (left: CommandReceipt, right: CommandReceipt): boolean =>
  left.commandId === right.commandId &&
  left.workspaceId === right.workspaceId &&
  left.commandKind === right.commandKind &&
  left.targetId === right.targetId &&
  left.requestHash === right.requestHash &&
  left.eventId === right.eventId &&
  left.eventSequence === right.eventSequence &&
  left.eventOffset === right.eventOffset;

export interface SqliteStoreOptions {
  readonly filename: string;
}

export const prepareSqliteStore = (options: SqliteStoreOptions): void => {
  const database = new Database(options.filename, { create: true });
  try {
    database.exec("PRAGMA journal_mode = WAL");
    database.exec("PRAGMA foreign_keys = ON");
    database.exec(APPLICATION_SCHEMA);
    database.exec(OUTBOX_SCHEMA);
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
  } finally {
    database.close(false);
  }
};

export const sqliteClientLayer = (options: SqliteStoreOptions) =>
  Layer.effectContext(
    Effect.gen(function* () {
      yield* Effect.sync(() => prepareSqliteStore(options));
      const client = yield* SqliteClient.make({ filename: options.filename, create: true });
      yield* client.unsafe<Record<string, never>>("PRAGMA foreign_keys = ON").pipe(Effect.asVoid);
      return Context.empty().pipe(
        Context.add(SqliteClient.SqliteClient, client),
        Context.add(SqlClient.SqlClient, client),
      );
    }),
  ).pipe(Layer.provide(Reactivity.layer));

export const sqliteLayer = (options: SqliteStoreOptions): Layer.Layer<IssueStore | OutboxStore> =>
  Layer.merge(
    Layer.effect(
      IssueStore,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        return issueStoreAdapter(sqliteService(sql), createSqliteIssueStoreBoundary(sql));
      }),
    ),
    Layer.effect(
      OutboxStore,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        return outboxStore(createSqliteOutboxBacking(sql));
      }),
    ),
  ).pipe(Layer.provide(sqliteClientLayer(options)));

export function createSqliteIssueStoreBoundary(
  sql: SqlClient.SqlClient,
  outbox: OutboxBacking = createSqliteOutboxBacking(sql),
): IssueStoreBoundary {
  const queryAll = <A extends object>(statement: string, params: ReadonlyArray<unknown> = []) =>
    sql.unsafe<A>(statement, params);
  const queryFirst = <A extends object>(statement: string, params: ReadonlyArray<unknown> = []) =>
    queryAll<A>(statement, params).pipe(Effect.map(first));
  const execute = (statement: string, params: ReadonlyArray<unknown> = []) =>
    queryAll<Record<string, never>>(statement, params).pipe(Effect.asVoid);
  const loadReceipt = (workspaceId: string, commandId: string) =>
    queryFirst<ReceiptRow>(
      "SELECT * FROM command_receipts WHERE workspace_id = ? AND command_id = ?",
      [workspaceId, commandId],
    ).pipe(
      Effect.map((row) =>
        row === undefined
          ? undefined
          : ({
              commandId: row.command_id,
              workspaceId: row.workspace_id,
              commandKind: row.command_kind,
              targetId: row.target_id,
              requestHash: row.request_hash,
              eventId: row.event_id,
              eventSequence: row.event_sequence,
              eventOffset: row.event_offset,
            } satisfies CommandReceipt),
      ),
    );

  return {
    progress: (workspaceId) =>
      sqliteUnavailable(
        "progress",
        queryFirst<ProgressRow>(
          "SELECT published, next_sequence FROM view_progress WHERE workspace_id = ?",
          [workspaceId],
        ).pipe(
          Effect.map((row) => ({
            published: row?.published ?? undefined,
            nextSequence: row?.next_sequence ?? 0,
          })),
        ),
      ),
    markPublished: (workspaceId, position) =>
      sqliteUnavailable(
        "markPublished",
        execute(
          "INSERT INTO view_progress (workspace_id, published) VALUES (?, ?)" +
            " ON CONFLICT (workspace_id) DO UPDATE SET published = excluded.published",
          [workspaceId, position],
        ),
      ),
    updateNextSequence: (workspaceId, nextSequence) =>
      sqliteUnavailable(
        "updateNextSequence",
        execute(
          "INSERT INTO view_progress (workspace_id, next_sequence) VALUES (?, ?)" +
            " ON CONFLICT (workspace_id) DO UPDATE SET" +
            " next_sequence = MAX(view_progress.next_sequence, excluded.next_sequence)",
          [workspaceId, nextSequence],
        ),
      ),
    receipt: (workspaceId, commandId) =>
      sqliteUnavailable("receipt", loadReceipt(workspaceId, commandId)),
    recordReceipt: (receipt, deliveries) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const inserted = yield* queryFirst<{ readonly command_id: string }>(
              "INSERT INTO command_receipts" +
                " (workspace_id, command_id, command_kind, target_id, request_hash," +
                " event_id, event_sequence, event_offset) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" +
                " ON CONFLICT (workspace_id, command_id) DO NOTHING" +
                " RETURNING command_id",
              [
                receipt.workspaceId,
                receipt.commandId,
                receipt.commandKind,
                receipt.targetId,
                receipt.requestHash,
                receipt.eventId,
                receipt.eventSequence,
                receipt.eventOffset,
              ],
            );
            if (inserted === undefined) {
              const stored = yield* loadReceipt(receipt.workspaceId, receipt.commandId);
              if (stored === undefined || !sameReceipt(stored, receipt))
                return yield* new CommandIdConflict({
                  workspaceId: receipt.workspaceId,
                  commandId: receipt.commandId,
                });
              return undefined;
            }
            if (deliveries.length > 0) yield* outbox.enqueue(deliveries);
            return undefined;
          }),
        )
        .pipe(
          Effect.mapError((cause): CommandIdConflict | StoreUnavailable =>
            isCommandIdConflict(cause)
              ? cause
              : new StoreUnavailable({
                  operation: "recordReceipt",
                  detail: cause instanceof Error ? cause.message : String(cause),
                }),
          ),
        ),
    graphPublished: (workspaceId, product) =>
      sqliteUnavailable(
        "graphPublished",
        queryFirst<{ readonly revision: string }>(
          "SELECT revision FROM graph_publication WHERE workspace_id = ? AND product = ?",
          [workspaceId, product],
        ).pipe(Effect.map((row) => row?.revision ?? undefined)),
      ),
    markGraphPublished: (workspaceId, product, revision) =>
      sqliteUnavailable(
        "markGraphPublished",
        execute(
          "INSERT INTO graph_publication (workspace_id, product, revision) VALUES (?, ?, ?)" +
            " ON CONFLICT (workspace_id, product) DO UPDATE SET revision = excluded.revision",
          [workspaceId, product, revision],
        ),
      ),
    transitionProgress: (workspaceId) =>
      sqliteUnavailable(
        "transitionProgress",
        queryFirst<TransitionProgressRow>(
          "SELECT history_epoch, history_sequence, producer_sequence" +
            " FROM transition_progress WHERE workspace_id = ?",
          [workspaceId],
        ).pipe(
          Effect.map((row) => {
            if (row === undefined) return { position: undefined, sequence: 0 };
            const position =
              row.history_epoch === null || row.history_sequence === null
                ? undefined
                : { epoch: row.history_epoch, sequence: row.history_sequence };
            return { position, sequence: row.producer_sequence };
          }),
        ),
      ),
    markTransitionsPublished: (workspaceId, progress) =>
      sqliteUnavailable(
        "markTransitionsPublished",
        execute(
          "INSERT INTO transition_progress" +
            " (workspace_id, history_epoch, history_sequence, producer_sequence)" +
            " VALUES (?, ?, ?, ?) ON CONFLICT (workspace_id) DO UPDATE SET" +
            " history_epoch = excluded.history_epoch," +
            " history_sequence = excluded.history_sequence," +
            " producer_sequence = excluded.producer_sequence",
          [
            workspaceId,
            progress.position?.epoch ?? null,
            progress.position?.sequence ?? null,
            progress.sequence,
          ],
        ),
      ),
    stateCheckpoint: (sourceId, partitionId) =>
      sqliteUnavailable(
        "stateCheckpoint",
        queryFirst<StateProgressRow>(
          "SELECT checkpoint FROM source_progress WHERE source_id = ? AND partition_id = ?",
          [sourceId, partitionId],
        ).pipe(Effect.map((row) => row?.checkpoint ?? undefined)),
      ),
    stateRows: (sourceId, collection, partitionId) =>
      Effect.gen(function* () {
        const found = yield* sqliteUnavailable(
          "stateRows",
          queryAll<ValueRow>(
            "SELECT row_key, value FROM source_state_rows" +
              " WHERE source_id = ? AND partition_id = ? ORDER BY row_key",
            [sourceId, partitionId],
          ),
        );
        const restored: CatalogRow[] = [];
        for (const row of found) {
          const value = yield* Schema.decodeEffect(JsonString)(row.value).pipe(
            Effect.mapError(
              (cause) =>
                new StoreRestorePoison({
                  table: "source_state_rows",
                  key: row.row_key,
                  detail: String(cause),
                }),
            ),
          );
          const decoded = yield* Effect.try({
            try: () => decodeCatalogRow(collection, value).row,
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
      sqliteUnavailable(
        "commitState",
        sql.withTransaction(
          Effect.gen(function* () {
            for (const [key, row] of input.rows) {
              yield* execute(
                "INSERT INTO source_state_rows (source_id, partition_id, row_key, value)" +
                  " VALUES (?, ?, ?, ?) ON CONFLICT (source_id, partition_id, row_key)" +
                  " DO UPDATE SET value = excluded.value",
                [sourceId, partitionId, key, encodeJsonString(row)],
              );
            }
            yield* execute(
              "INSERT INTO source_progress (source_id, partition_id, checkpoint) VALUES (?, ?, ?)" +
                " ON CONFLICT (source_id, partition_id) DO UPDATE SET checkpoint = excluded.checkpoint",
              [sourceId, partitionId, input.checkpoint],
            );
          }),
        ),
      ),
  };
}
