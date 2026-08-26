/**
 * The SQLite outbox backing.
 *
 * One table, one unique index, and no background state. The unique index on
 * `(sink, idempotency_key)` is the durable half of the idempotency contract: an
 * `INSERT ... ON CONFLICT DO NOTHING` makes a repeated enqueue a no-op at the
 * storage layer, which is the only place that can decide it without a race.
 *
 * Every function here is synchronous on the caller's `Database`, so an
 * application can call `enqueue` from inside its own `database.transaction` and
 * commit the cause of a delivery and the decision to deliver it together.
 */
import { Database } from "bun:sqlite";
import type { DeadLetterReason } from "./errors.ts";
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

/** Create the outbox table. Idempotent, so a host may call it on every open. */
export function migrateOutbox(database: Database): void {
  database.exec(OUTBOX_SCHEMA);
}

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

export function createSqliteOutboxBacking(database: Database): OutboxBacking {
  migrateOutbox(database);

  const insert = database.query<never, [string, string, string, string, number, number]>(
    "INSERT INTO streamsy_effect_outbox" +
      " (sink, partition_id, idempotency_key, payload, state, attempts," +
      " next_attempt_at_ms, enqueued_at_ms)" +
      " VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)" +
      " ON CONFLICT (sink, idempotency_key) DO NOTHING",
  );
  const selectDue = database.query<OutboxRow, [string, number, number]>(
    "SELECT * FROM streamsy_effect_outbox" +
      " WHERE sink = ? AND state = 'pending' AND next_attempt_at_ms <= ?" +
      " ORDER BY id LIMIT ?",
  );
  const selectDueInPartition = database.query<OutboxRow, [string, string, number, number]>(
    "SELECT * FROM streamsy_effect_outbox" +
      " WHERE sink = ? AND partition_id = ? AND state = 'pending' AND next_attempt_at_ms <= ?" +
      " ORDER BY id LIMIT ?",
  );
  const selectAll = database.query<OutboxRow, [string]>(
    "SELECT * FROM streamsy_effect_outbox WHERE sink = ? ORDER BY id",
  );
  const selectPartition = database.query<OutboxRow, [string, string]>(
    "SELECT * FROM streamsy_effect_outbox WHERE sink = ? AND partition_id = ? ORDER BY id",
  );
  const settleDelivered = database.query<never, [number, number, number]>(
    "UPDATE streamsy_effect_outbox SET state = 'delivered', attempts = ?," +
      " last_error = NULL, settled_at_ms = ? WHERE id = ?",
  );
  const settleRetry = database.query<never, [number, number, string, number]>(
    "UPDATE streamsy_effect_outbox SET state = 'pending', attempts = ?," +
      " next_attempt_at_ms = ?, last_error = ? WHERE id = ?",
  );
  const settleDead = database.query<never, [number, string, string, number, number]>(
    "UPDATE streamsy_effect_outbox SET state = 'dead', attempts = ?," +
      " dead_letter_reason = ?, last_error = ?, settled_at_ms = ? WHERE id = ?",
  );

  return {
    enqueue: (drafts: readonly OutboxDraft[]): OutboxEnqueueReport => {
      let enqueued = 0;
      for (const draft of drafts) {
        // The insert writes no row exactly when the unique index absorbed a
        // repeated enqueue, so the row count *is* the idempotency decision.
        const written = insert.run(
          draft.sink,
          draft.partitionId,
          draft.idempotencyKey,
          draft.payload,
          draft.enqueuedAtMs,
          draft.enqueuedAtMs,
        );
        if (written.changes > 0) enqueued += 1;
      }
      return { enqueued, absorbed: drafts.length - enqueued };
    },
    claimDue: (sink, partitionId, nowMs, limit) =>
      (partitionId === undefined
        ? selectDue.all(sink, nowMs, limit)
        : selectDueInPartition.all(sink, partitionId, nowMs, limit)
      ).map(decodeRow),
    markDelivered: (id, attempts, atMs) => {
      settleDelivered.run(attempts, atMs, id);
    },
    reschedule: (id, attempts, nextAttemptAtMs, detail) => {
      settleRetry.run(attempts, nextAttemptAtMs, detail, id);
    },
    deadLetter: (id, attempts, reason, detail, atMs) => {
      settleDead.run(attempts, reason, detail, atMs, id);
    },
    list: (sink, partitionId) =>
      (partitionId === undefined
        ? selectAll.all(sink)
        : selectPartition.all(sink, partitionId)
      ).map(decodeRow),
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
