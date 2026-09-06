import { Effect, Schema } from "effect";
import type { SqlClient } from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import type { DeadLetterReason } from "./errors.ts";
import { OutboxUnavailable } from "./errors.ts";
import type {
  OutboxBacking,
  OutboxDraft,
  OutboxEnqueueReport,
  OutboxEntry,
  OutboxEntryState,
} from "./outbox.ts";

export const OUTBOX_SCHEMA = `CREATE TABLE IF NOT EXISTS streamsy_effect_outbox (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  sink               TEXT NOT NULL,
  partition_id       TEXT NOT NULL,
  idempotency_key    TEXT NOT NULL,
  payload            TEXT NOT NULL,
  state              TEXT NOT NULL,
  attempts           INTEGER NOT NULL DEFAULT 0,
  next_attempt_at_ms INTEGER NOT NULL,
  last_error         TEXT,
  dead_letter_reason TEXT,
  enqueued_at_ms     INTEGER NOT NULL,
  settled_at_ms      INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS streamsy_effect_outbox_identity
  ON streamsy_effect_outbox (sink, idempotency_key);
CREATE INDEX IF NOT EXISTS streamsy_effect_outbox_due
  ON streamsy_effect_outbox (sink, state, next_attempt_at_ms, id);
`;
const OUTBOX_STATEMENTS = [
  `CREATE TABLE IF NOT EXISTS streamsy_effect_outbox (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  sink               TEXT NOT NULL,
  partition_id       TEXT NOT NULL,
  idempotency_key    TEXT NOT NULL,
  payload            TEXT NOT NULL,
  state              TEXT NOT NULL,
  attempts           INTEGER NOT NULL DEFAULT 0,
  next_attempt_at_ms INTEGER NOT NULL,
  last_error         TEXT,
  dead_letter_reason TEXT,
  enqueued_at_ms     INTEGER NOT NULL,
  settled_at_ms      INTEGER
)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS streamsy_effect_outbox_identity
  ON streamsy_effect_outbox (sink, idempotency_key)`,
  `CREATE INDEX IF NOT EXISTS streamsy_effect_outbox_due
  ON streamsy_effect_outbox (sink, state, next_attempt_at_ms, id)`,
] as const;

interface OutboxRow {
  readonly id: number;
  readonly sink: string;
  readonly partition_id: string;
  readonly idempotency_key: string;
  readonly payload: string;
  readonly state: string;
  readonly attempts: number;
  readonly next_attempt_at_ms: number;
  readonly last_error: string | null;
  readonly dead_letter_reason: string | null;
  readonly enqueued_at_ms: number;
  readonly settled_at_ms: number | null;
}

const isOutboxUnavailable = Schema.is(OutboxUnavailable);
const attempt = <A>(
  operation: string,
  effect: Effect.Effect<A, OutboxUnavailable | SqlError>,
): Effect.Effect<A, OutboxUnavailable> =>
  effect.pipe(
    Effect.mapError((cause) =>
      isOutboxUnavailable(cause)
        ? cause
        : new OutboxUnavailable({
            operation,
            detail: cause instanceof Error ? cause.message : String(cause),
          }),
    ),
  );

export function migrateOutbox(sql: SqlClient): Effect.Effect<void, OutboxUnavailable> {
  return attempt(
    "migrateOutbox",
    Effect.forEach(OUTBOX_STATEMENTS, (statement) =>
      sql.unsafe<Record<string, never>>(statement).pipe(Effect.asVoid),
    ).pipe(Effect.asVoid),
  );
}

export function createSqliteOutboxBacking(sql: SqlClient): OutboxBacking {
  // Readiness deliberately is not cached: migration can run inside a caller's
  // transaction, and publishing that transaction-local result after rollback
  // would leave this backing unable to recover. The idempotent statements make
  // a retry safe; placements may still migrate eagerly before publishing stores.
  const ready: Effect.Effect<void, OutboxUnavailable> = Effect.suspend(() => migrateOutbox(sql));
  const queryAll = <A extends object>(statement: string, params: ReadonlyArray<unknown> = []) =>
    Effect.flatMap(ready, () => sql.unsafe<A>(statement, params));
  const execute = (operation: string, statement: string, params: ReadonlyArray<unknown> = []) =>
    attempt(operation, queryAll<Record<string, never>>(statement, params).pipe(Effect.asVoid));

  return {
    enqueue: (
      drafts: readonly OutboxDraft[],
    ): Effect.Effect<OutboxEnqueueReport, OutboxUnavailable> =>
      attempt(
        "enqueue",
        Effect.gen(function* () {
          let enqueued = 0;
          for (const draft of drafts) {
            const inserted = yield* queryAll<{ readonly inserted: number }>(
              "INSERT INTO streamsy_effect_outbox" +
                " (sink, partition_id, idempotency_key, payload, state, attempts," +
                " next_attempt_at_ms, enqueued_at_ms)" +
                " VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)" +
                " ON CONFLICT (sink, idempotency_key) DO NOTHING" +
                " RETURNING 1 AS inserted",
              [
                draft.sink,
                draft.partitionId,
                draft.idempotencyKey,
                draft.payload,
                draft.enqueuedAtMs,
                draft.enqueuedAtMs,
              ],
            );
            if (inserted.length > 0) enqueued += 1;
          }
          return { enqueued, absorbed: drafts.length - enqueued };
        }),
      ),
    claimDue: (sink, partitionId, nowMs, limit) =>
      attempt(
        "claimDue",
        queryAll<OutboxRow>(
          partitionId === undefined
            ? "SELECT * FROM streamsy_effect_outbox" +
                " WHERE sink = ? AND state = 'pending' AND next_attempt_at_ms <= ?" +
                " ORDER BY id LIMIT ?"
            : "SELECT * FROM streamsy_effect_outbox" +
                " WHERE sink = ? AND partition_id = ? AND state = 'pending'" +
                " AND next_attempt_at_ms <= ? ORDER BY id LIMIT ?",
          partitionId === undefined ? [sink, nowMs, limit] : [sink, partitionId, nowMs, limit],
        ).pipe(Effect.map((rows) => rows.map(decodeRow))),
      ),
    markDelivered: (id, attempts, atMs) =>
      execute(
        "markDelivered",
        "UPDATE streamsy_effect_outbox SET state = 'delivered', attempts = ?," +
          " last_error = NULL, settled_at_ms = ? WHERE id = ?",
        [attempts, atMs, id],
      ),
    reschedule: (id, attempts, nextAttemptAtMs, detail) =>
      execute(
        "reschedule",
        "UPDATE streamsy_effect_outbox SET state = 'pending', attempts = ?," +
          " next_attempt_at_ms = ?, last_error = ? WHERE id = ?",
        [attempts, nextAttemptAtMs, detail, id],
      ),
    deadLetter: (id, attempts, reason, detail, atMs) =>
      execute(
        "deadLetter",
        "UPDATE streamsy_effect_outbox SET state = 'dead', attempts = ?," +
          " dead_letter_reason = ?, last_error = ?, settled_at_ms = ? WHERE id = ?",
        [attempts, reason, detail, atMs, id],
      ),
    list: (sink, partitionId) =>
      attempt(
        "list",
        queryAll<OutboxRow>(
          partitionId === undefined
            ? "SELECT * FROM streamsy_effect_outbox WHERE sink = ? ORDER BY id"
            : "SELECT * FROM streamsy_effect_outbox WHERE sink = ? AND partition_id = ? ORDER BY id",
          partitionId === undefined ? [sink] : [sink, partitionId],
        ).pipe(Effect.map((rows) => rows.map(decodeRow))),
      ),
  };
}

function decodeRow(row: OutboxRow): OutboxEntry {
  return {
    id: row.id,
    sink: row.sink,
    partitionId: row.partition_id,
    idempotencyKey: row.idempotency_key,
    payload: row.payload,
    state: decodeState(row.state),
    attempts: row.attempts,
    nextAttemptAtMs: row.next_attempt_at_ms,
    lastError: row.last_error ?? undefined,
    deadLetterReason: decodeReason(row.dead_letter_reason),
    enqueuedAtMs: row.enqueued_at_ms,
    settledAtMs: row.settled_at_ms ?? undefined,
  };
}

/** A stored state this build does not know is a schema fault, never a silent "pending". */
function decodeState(value: string): OutboxEntryState {
  if (value === "pending" || value === "delivered" || value === "dead") return value;
  throw new TypeError(`unknown outbox state ${value}`);
}

function decodeReason(value: string | null): DeadLetterReason | undefined {
  if (value === null) return undefined;
  if (value === "attempts-exhausted" || value === "permanent" || value === "payload-poison") {
    return value;
  }
  throw new TypeError(`unknown outbox dead-letter reason ${value}`);
}
