/** One user's inbox, backed by the placement's shared Effect SQL client. */
import { Context, Effect, Layer, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import { compareInboxRows, InboxRow } from "../../domain/inbox.ts";
import { InboxRestorePoison, InboxUnavailable } from "./domain-errors.ts";

export interface InboxBatch {
  readonly operationId: string;
  readonly payloadHash: string;
  readonly rows: readonly InboxRow[];
}

export interface InboxBatchResult {
  readonly operationId: string;
  readonly payloadHash: string;
  readonly applied: number;
}

export interface InboxStoreService {
  readonly upsert: (userId: string, rows: readonly InboxRow[]) => Effect.Effect<number, InboxUnavailable>;
  readonly applyBatch: (
    userId: string,
    batch: InboxBatch,
  ) => Effect.Effect<InboxBatchResult, InboxUnavailable>;
  readonly rows: (userId: string) => Effect.Effect<readonly InboxRow[], InboxUnavailable | InboxRestorePoison>;
}

export class InboxStore extends Context.Service<InboxStore, InboxStoreService>()(
  "issue-tracker/InboxStore",
) {}

export const INBOX_SCHEMA = `CREATE TABLE IF NOT EXISTS inbox_rows (
  user_id TEXT NOT NULL, inbox_id TEXT NOT NULL, value TEXT NOT NULL,
  PRIMARY KEY (user_id, inbox_id)
);
CREATE TABLE IF NOT EXISTS exchange_batch_receipts (
  operation_id TEXT PRIMARY KEY,
  payload_hash TEXT NOT NULL,
  applied INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL
);`;

interface InboxValueRow { readonly inbox_id: string; readonly value: string }
interface BatchReceiptRow { readonly payload_hash: string; readonly applied: number }

const InboxRowJson = Schema.fromJsonString(InboxRow);
const encodeInboxRow = Schema.encodeUnknownSync(InboxRowJson);
const misplaced = (userId: string, row: InboxRow): InboxUnavailable =>
  new InboxUnavailable({ operation: "upsert", detail: `row ${row.inboxId} is placed at user ${row.userId}, not ${userId}` });
const unavailable = (operation: string, cause: unknown) =>
  new InboxUnavailable({ operation, detail: cause instanceof Error ? cause.message : String(cause) });
const first = <A>(rows: ReadonlyArray<A>): A | undefined => rows[0];

const validateRows = (userId: string, rows: readonly InboxRow[]) => {
  for (const row of rows) if (row.userId !== userId) return Effect.fail(misplaced(userId, row));
  return Effect.void;
};

/** The in-memory inbox. Same placement and receipt conflict rules as SQL. */
// oxlint-disable-next-line effecttsgo/lazy-effect -- Each user partition needs distinct mutable backing.
export const inboxMemoryLayer = (): Layer.Layer<InboxStore> =>
  Layer.sync(InboxStore, () => {
    const stored = new Map<string, string>();
    const receipts = new Map<string, BatchReceiptRow>();
    const upsert = Effect.fn("InboxStore.upsert")(function* (userId: string, rows: readonly InboxRow[]) {
      yield* validateRows(userId, rows);
      for (const row of rows) stored.set(`${userId}\u0000${row.inboxId}`, encodeInboxRow(row));
      return rows.length;
    });
    return InboxStore.of({
      upsert,
      applyBatch: Effect.fn("InboxStore.applyBatch")(function* (userId, batch) {
        const receipt = receipts.get(batch.operationId);
        if (receipt !== undefined) {
          if (receipt.payload_hash !== batch.payloadHash) {
            return yield* unavailable("applyBatch", `operation ${batch.operationId} payload conflict`);
          }
          return { operationId: batch.operationId, payloadHash: receipt.payload_hash, applied: receipt.applied };
        }
        const applied = yield* upsert(userId, batch.rows);
        receipts.set(batch.operationId, { payload_hash: batch.payloadHash, applied });
        return { operationId: batch.operationId, payloadHash: batch.payloadHash, applied };
      }),
      rows: Effect.fn("InboxStore.rows")(function* (userId: string) {
        const prefix = `${userId}\u0000`;
        const restored: InboxRow[] = [];
        for (const [key, value] of stored) {
          if (key.startsWith(prefix)) restored.push(yield* restore(userId, key.slice(prefix.length), value));
        }
        return restored.toSorted(compareInboxRows);
      }),
    });
  });

export const migrateInboxStore = Effect.fn("InboxStore.migrate")(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const statement of INBOX_SCHEMA.split(";").map((value) => value.trim()).filter(Boolean)) {
    yield* sql.unsafe<Record<string, never>>(statement).pipe(Effect.asVoid);
  }
});

export const inboxSqlLayer: Layer.Layer<InboxStore, never, SqlClient.SqlClient> = Layer.effect(
  InboxStore,
  Effect.map(SqlClient.SqlClient, inboxService),
);

export const migratedInboxSqlLayer: Layer.Layer<InboxStore, SqlError, SqlClient.SqlClient> =
  Layer.effect(
    InboxStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migrateInboxStore();
      return inboxService(sql);
    }),
  );

export function inboxService(sql: SqlClient.SqlClient): InboxStoreService {
  const execute = (statement: string, params: ReadonlyArray<unknown> = []) =>
    sql.unsafe<Record<string, never>>(statement, params).pipe(Effect.asVoid);
  const upsertRows = (userId: string, rows: readonly InboxRow[]) =>
    Effect.forEach(rows, (row) => execute(
      "INSERT INTO inbox_rows (user_id, inbox_id, value) VALUES (?, ?, ?)" +
        " ON CONFLICT (user_id, inbox_id) DO UPDATE SET value = excluded.value",
      [userId, row.inboxId, encodeInboxRow(row)],
    ), { discard: true });
  const apply = (userId: string, rows: readonly InboxRow[]) =>
    validateRows(userId, rows).pipe(
      Effect.andThen(sql.withTransaction(upsertRows(userId, rows))),
      Effect.as(rows.length),
      Effect.mapError((cause) => unavailable("upsert", cause)),
    );
  return InboxStore.of({
    upsert: apply,
    applyBatch: (userId, batch) =>
      validateRows(userId, batch.rows).pipe(
        Effect.andThen(sql.withTransaction(Effect.gen(function* () {
          const receipt = first(yield* sql.unsafe<BatchReceiptRow>(
            "SELECT payload_hash, applied FROM exchange_batch_receipts WHERE operation_id = ?",
            [batch.operationId],
          ));
          if (receipt !== undefined) {
            if (receipt.payload_hash !== batch.payloadHash) {
              return yield* Effect.fail(`operation ${batch.operationId} payload conflict`);
            }
            return { operationId: batch.operationId, payloadHash: receipt.payload_hash, applied: receipt.applied };
          }
          yield* upsertRows(userId, batch.rows);
          yield* execute(
            "INSERT INTO exchange_batch_receipts (operation_id, payload_hash, applied, created_at_ms) VALUES (?, ?, ?, ?)",
            [batch.operationId, batch.payloadHash, batch.rows.length, Date.now()],
          );
          return { operationId: batch.operationId, payloadHash: batch.payloadHash, applied: batch.rows.length };
        }))),
        Effect.mapError((cause) => unavailable("applyBatch", cause)),
      ),
    rows: (userId) =>
      sql.unsafe<InboxValueRow>(
        "SELECT inbox_id, value FROM inbox_rows WHERE user_id = ? ORDER BY inbox_id",
        [userId],
      ).pipe(
        Effect.mapError((cause) => unavailable("rows", cause)),
        Effect.flatMap((found) => Effect.forEach(found, (row) => restore(userId, row.inbox_id, row.value))),
        Effect.map((rows) => rows.toSorted(compareInboxRows)),
      ),
  });
}

const restore = (userId: string, key: string, value: string) =>
  Schema.decodeEffect(InboxRowJson)(value).pipe(
    Effect.mapError((cause) => new InboxRestorePoison({ userId, key, detail: String(cause) })),
  );
