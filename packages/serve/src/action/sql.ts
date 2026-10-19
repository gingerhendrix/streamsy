import type { SqlClient, SqlError } from "effect/sql";
import { Effect, Option, Predicate, Schema } from "effect";
import type { DeadLetterReason } from "./errors.ts";
import { OutboxUnavailable } from "./errors.ts";
import type {
  OutboxBacking,
  OutboxDraft,
  OutboxEnqueueReport,
  OutboxEntry,
  OutboxEntryState,
} from "./outbox.ts";

const TABLE = "streamsy_effect_outbox";

/** MySQL key columns. Their sum fits the 3072-byte InnoDB index limit at 4 bytes a character. */
const MYSQL_SINK_LENGTH = 255;

const MYSQL_PARTITION_LENGTH = 255;

const MYSQL_KEY_LENGTH = 512;

// NO PAD, so keys that differ only by trailing spaces stay distinct, as on SQLite and Postgres.
const MYSQL_KEY_TYPE = "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL";

const CREATE_TABLE = {
  sqlite: `CREATE TABLE IF NOT EXISTS ${TABLE} (
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
  pg: `CREATE TABLE IF NOT EXISTS ${TABLE} (
  id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sink               TEXT NOT NULL,
  partition_id       TEXT NOT NULL,
  idempotency_key    TEXT NOT NULL,
  payload            TEXT NOT NULL,
  state              TEXT NOT NULL,
  attempts           INTEGER NOT NULL DEFAULT 0,
  next_attempt_at_ms BIGINT NOT NULL,
  last_error         TEXT,
  dead_letter_reason TEXT,
  enqueued_at_ms     BIGINT NOT NULL,
  settled_at_ms      BIGINT
)`,
  mysql: `CREATE TABLE IF NOT EXISTS ${TABLE} (
  id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
  sink               VARCHAR(${MYSQL_SINK_LENGTH}) ${MYSQL_KEY_TYPE},
  partition_id       VARCHAR(${MYSQL_PARTITION_LENGTH}) ${MYSQL_KEY_TYPE},
  idempotency_key    VARCHAR(${MYSQL_KEY_LENGTH}) ${MYSQL_KEY_TYPE},
  payload            LONGTEXT NOT NULL,
  state              VARCHAR(16) NOT NULL,
  attempts           INT NOT NULL DEFAULT 0,
  next_attempt_at_ms BIGINT NOT NULL,
  last_error         LONGTEXT,
  dead_letter_reason VARCHAR(32),
  enqueued_at_ms     BIGINT NOT NULL,
  settled_at_ms      BIGINT
)`,
} as const;

const INDEXES = [
  { name: `${TABLE}_identity`, unique: true, columns: "(sink, idempotency_key)" },
  { name: `${TABLE}_due`, unique: false, columns: "(sink, state, next_attempt_at_ms, id)" },
  // The due index sorts by retry time; ordered passes need the oldest pending id without a sort.
  { name: `${TABLE}_order`, unique: false, columns: "(sink, state, id)" },
] as const;

type Index = (typeof INDEXES)[number];

const createIndex = (index: Index, ifNotExists: boolean) =>
  `CREATE ${index.unique ? "UNIQUE " : ""}INDEX ${ifNotExists ? "IF NOT EXISTS " : ""}${
    index.name
  } ON ${TABLE} ${index.columns}`;

interface OutboxRow {
  readonly id: number | bigint;
  readonly sink: string;
  readonly partition_id: string;
  readonly idempotency_key: string;
  readonly payload: string;
  readonly state: string;
  readonly attempts: number;
  readonly next_attempt_at_ms: number | bigint;
  readonly last_error: string | null;
  readonly dead_letter_reason: string | null;
  readonly enqueued_at_ms: number | bigint;
  readonly settled_at_ms: number | bigint | null;
}

const isOutboxUnavailable = Schema.is(OutboxUnavailable);

const attempt = <A>(
  operation: string,
  effect: Effect.Effect<A, OutboxUnavailable | SqlError.SqlError>,
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

const unsupported = (operation: string) =>
  Effect.fail(
    new OutboxUnavailable({
      operation,
      detail: "Unsupported SQL dialect; the SQL outbox supports sqlite, pg and mysql",
    }),
  );

/** The names of the outbox indexes that exist in the current MySQL database. */
const mysqlIndexes = (sql: SqlClient.SqlClient) =>
  sql<{
    readonly name: string;
  }>`SELECT DISTINCT index_name AS name FROM information_schema.statistics
 WHERE table_schema = DATABASE() AND table_name = ${TABLE}`.pipe(
    Effect.map((rows) => new Set(rows.map(({ name }) => name))),
  );

/** MySQL has no `CREATE INDEX IF NOT EXISTS`. A concurrent migration may win the create. */
const migrateMysql = (
  sql: SqlClient.SqlClient,
): Effect.Effect<void, OutboxUnavailable | SqlError.SqlError> =>
  Effect.gen(function* () {
    // MySQL DDL commits the open transaction, so it must run outside one.
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new OutboxUnavailable({
        operation: "migrateOutbox",
        detail: "On MySQL, run migrateOutbox outside a transaction, at placement start",
      });
    yield* sql.unsafe<Record<string, never>>(CREATE_TABLE.mysql);

    for (const index of INDEXES) {
      if ((yield* mysqlIndexes(sql)).has(index.name)) continue;
      yield* sql
        .unsafe<Record<string, never>>(createIndex(index, false))
        .pipe(
          Effect.catch((error) =>
            Effect.flatMap(mysqlIndexes(sql), (names) =>
              names.has(index.name) ? Effect.void : Effect.fail(error),
            ),
          ),
        );
    }

    return undefined;
  });

export function migrateOutbox(sql: SqlClient.SqlClient): Effect.Effect<void, OutboxUnavailable> {
  const statements = (dialect: "sqlite" | "pg"): Effect.Effect<void, SqlError.SqlError> =>
    Effect.forEach(
      [CREATE_TABLE[dialect], ...INDEXES.map((index) => createIndex(index, true))],
      (statement) => sql.unsafe<Record<string, never>>(statement),
      { discard: true },
    );

  return attempt(
    "migrateOutbox",
    sql.onDialectOrElse({
      sqlite: () => statements("sqlite"),
      pg: () => statements("pg"),
      mysql: () => migrateMysql(sql),
      orElse: () => unsupported("migrateOutbox"),
    }),
  );
}

/**
 * MySQL readiness only reads the catalog. The backing may run inside a caller's
 * transaction, and DDL there would commit it early.
 */
const mysqlReady = (sql: SqlClient.SqlClient) =>
  Effect.flatMap(mysqlIndexes(sql), (names) =>
    INDEXES.every((index) => names.has(index.name))
      ? Effect.void
      : Effect.fail(
          new OutboxUnavailable({
            operation: "ready",
            detail: `${TABLE} is not migrated; on MySQL, run migrateOutbox at placement start`,
          }),
        ),
  );

/** MySQL `INSERT IGNORE` would truncate an over-long key, so refuse it first. */
const mysqlKeyFault = (draft: OutboxDraft): OutboxUnavailable | undefined => {
  const limits = [
    ["sink", draft.sink, MYSQL_SINK_LENGTH],
    ["partition id", draft.partitionId, MYSQL_PARTITION_LENGTH],
    ["idempotency key", draft.idempotencyKey, MYSQL_KEY_LENGTH],
  ] as const;

  for (const [label, value, limit] of limits) {
    // MySQL VARCHAR counts code points, which is what the spread yields.
    // oxlint-disable-next-line typescript/no-misused-spread -- code points are the intended unit
    const length = [...value].length;

    if (length > limit)
      return new OutboxUnavailable({
        operation: "enqueue",
        detail: `The ${label} has ${length} characters; the MySQL outbox limit is ${limit}`,
      });
  }

  return undefined;
};

export function createSqlOutboxBacking(sql: SqlClient.SqlClient): OutboxBacking {
  // Readiness deliberately is not cached: on SQLite and Postgres, migration can
  // run inside a caller's transaction, and publishing that transaction-local
  // result after rollback would leave this backing unable to recover. The
  // idempotent statements make a retry safe; placements may still migrate
  // eagerly before publishing stores. MySQL only checks, so it must migrate eagerly.
  const ready: Effect.Effect<void, OutboxUnavailable | SqlError.SqlError> = Effect.suspend(() =>
    sql.onDialectOrElse({
      mysql: () => mysqlReady(sql),
      orElse: () => migrateOutbox(sql),
    }),
  );

  const run = <A>(
    operation: string,
    statement: Effect.Effect<A, SqlError.SqlError | OutboxUnavailable>,
  ) => attempt(operation, Effect.andThen(ready, statement));

  const execute = (operation: string, statement: Effect.Effect<unknown, SqlError.SqlError>) =>
    run(operation, statement).pipe(Effect.asVoid);

  const rows = (
    operation: string,
    statement: Effect.Effect<ReadonlyArray<OutboxRow>, SqlError.SqlError>,
  ) => run(operation, statement).pipe(Effect.map((found) => found.map(decodeRow)));

  /** Whether the draft was new. A draft with a known sink and key changes nothing. */
  const insert = (draft: OutboxDraft) => {
    const row = sql.insert({
      sink: draft.sink,
      partition_id: draft.partitionId,
      idempotency_key: draft.idempotencyKey,
      payload: draft.payload,
      state: "pending",
      attempts: 0,
      next_attempt_at_ms: draft.enqueuedAtMs,
      enqueued_at_ms: draft.enqueuedAtMs,
    });

    return sql.onDialectOrElse({
      // MySQL has no RETURNING. An ignored duplicate affects no rows.
      mysql: () => {
        const fault = mysqlKeyFault(draft);

        if (fault !== undefined) return Effect.fail(fault);

        return sql`INSERT IGNORE INTO streamsy_effect_outbox ${row}`.raw.pipe(
          Effect.map(
            (result) =>
              Predicate.hasProperty(result, "affectedRows") && Number(result.affectedRows) > 0,
          ),
        );
      },
      orElse: () =>
        sql`INSERT INTO streamsy_effect_outbox ${row}
 ON CONFLICT (sink, idempotency_key) DO NOTHING RETURNING 1 AS inserted`.pipe(
          Effect.map((inserted) => inserted.length > 0),
        ),
    });
  };

  return {
    enqueue: (
      drafts: readonly OutboxDraft[],
    ): Effect.Effect<OutboxEnqueueReport, OutboxUnavailable> =>
      run(
        "enqueue",
        Effect.gen(function* () {
          let enqueued = 0;

          for (const draft of drafts) if (yield* insert(draft)) enqueued += 1;

          return { enqueued, absorbed: drafts.length - enqueued };
        }),
      ),
    nextDue: (sink, partitionId, ordered = false) =>
      run(
        "nextDue",
        sql<{ readonly at: number | bigint }>`SELECT next_attempt_at_ms AS at
 FROM streamsy_effect_outbox WHERE ${sql.and([
   sql`sink = ${sink}`,
   sql`state = 'pending'`,
   ...(partitionId === undefined ? [] : [sql`partition_id = ${partitionId}`]),
 ])} ORDER BY ${ordered ? sql`id` : sql`next_attempt_at_ms, id`} LIMIT 1`,
      ).pipe(
        Effect.map((found) =>
          Option.fromUndefinedOr(found[0]).pipe(Option.map((row) => Number(row.at))),
        ),
      ),
    claimDue: (sink, partitionId, nowMs, limit, ordered = false) =>
      rows(
        "claimDue",
        sql<OutboxRow>`SELECT * FROM streamsy_effect_outbox WHERE ${sql.and([
          sql`sink = ${sink}`,
          ...(partitionId === undefined ? [] : [sql`partition_id = ${partitionId}`]),
          sql`state = 'pending'`,
          ...(ordered ? [] : [sql`next_attempt_at_ms <= ${nowMs}`]),
        ])} ORDER BY id LIMIT ${limit}`,
      ),
    markDelivered: (id, attempts, atMs) =>
      execute(
        "markDelivered",
        sql`UPDATE streamsy_effect_outbox SET state = 'delivered', attempts = ${attempts},
 last_error = NULL, settled_at_ms = ${atMs} WHERE id = ${id}`,
      ),
    reschedule: (id, attempts, nextAttemptAtMs, detail) =>
      execute(
        "reschedule",
        sql`UPDATE streamsy_effect_outbox SET state = 'pending', attempts = ${attempts},
 next_attempt_at_ms = ${nextAttemptAtMs}, last_error = ${detail} WHERE id = ${id}`,
      ),
    deadLetter: (id, attempts, reason, detail, atMs) =>
      execute(
        "deadLetter",
        sql`UPDATE streamsy_effect_outbox SET state = 'dead', attempts = ${attempts},
 dead_letter_reason = ${reason}, last_error = ${detail}, settled_at_ms = ${atMs} WHERE id = ${id}`,
      ),
    list: (sink, partitionId) =>
      rows(
        "list",
        sql<OutboxRow>`SELECT * FROM streamsy_effect_outbox WHERE ${sql.and([
          sql`sink = ${sink}`,
          ...(partitionId === undefined ? [] : [sql`partition_id = ${partitionId}`]),
        ])} ORDER BY id`,
      ),
  };
}

/** Postgres returns BIGINT as a `bigint`; SQLite and MySQL return a `number`. */
const toNumber = (value: number | bigint): number => Number(value);

function decodeRow(row: OutboxRow): OutboxEntry {
  return {
    id: toNumber(row.id),
    sink: row.sink,
    partitionId: row.partition_id,
    idempotencyKey: row.idempotency_key,
    payload: row.payload,
    state: decodeState(row.state),
    attempts: row.attempts,
    nextAttemptAtMs: toNumber(row.next_attempt_at_ms),
    lastError: row.last_error ?? undefined,
    deadLetterReason: decodeReason(row.dead_letter_reason),
    enqueuedAtMs: toNumber(row.enqueued_at_ms),
    settledAtMs: row.settled_at_ms === null ? undefined : toNumber(row.settled_at_ms),
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
