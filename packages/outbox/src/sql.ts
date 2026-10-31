import { SqlClient, type SqlError } from "effect/sql";
import { Array, Effect, Layer, Option, Predicate, Schema, SchemaTransformation } from "effect";
import { InvalidStoredRecord, OutboxUnavailable, type StoreFailure } from "./errors.ts";
import {
  DeadLetterReason,
  EntryState,
  OutboxStore,
  Settlement,
  type OutboxBacking,
  type OutboxDraft,
  type EnqueueReport,
  type OutboxEntry,
} from "./store.ts";

const TABLE = "streamsy_outbox";

/** MySQL key columns. Their sum fits the 3072-byte InnoDB index limit at 4 bytes a character. */
const MYSQL_NAME_LENGTH = 255;

const MYSQL_PARTITION_LENGTH = 255;

const MYSQL_KEY_LENGTH = 512;

// NO PAD, so keys that differ only by trailing spaces stay distinct, as on SQLite and Postgres.
const MYSQL_KEY_TYPE = "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL";

const CREATE_TABLE = {
  sqlite: `CREATE TABLE IF NOT EXISTS ${TABLE} (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  name               TEXT NOT NULL,
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
  name               TEXT NOT NULL,
  partition_id       TEXT NOT NULL,
  idempotency_key    TEXT NOT NULL,
  payload            TEXT NOT NULL,
  state              TEXT NOT NULL,
  attempts           BIGINT NOT NULL DEFAULT 0,
  next_attempt_at_ms BIGINT NOT NULL,
  last_error         TEXT,
  dead_letter_reason TEXT,
  enqueued_at_ms     BIGINT NOT NULL,
  settled_at_ms      BIGINT
)`,
  mysql: `CREATE TABLE IF NOT EXISTS ${TABLE} (
  id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
  name               VARCHAR(${MYSQL_NAME_LENGTH}) ${MYSQL_KEY_TYPE},
  partition_id       VARCHAR(${MYSQL_PARTITION_LENGTH}) ${MYSQL_KEY_TYPE},
  idempotency_key    VARCHAR(${MYSQL_KEY_LENGTH}) ${MYSQL_KEY_TYPE},
  payload            LONGTEXT NOT NULL,
  state              VARCHAR(16) NOT NULL,
  attempts           BIGINT NOT NULL DEFAULT 0,
  next_attempt_at_ms BIGINT NOT NULL,
  last_error         LONGTEXT,
  dead_letter_reason VARCHAR(32),
  enqueued_at_ms     BIGINT NOT NULL,
  settled_at_ms      BIGINT
)`,
} as const;

const INDEXES = [
  { name: `${TABLE}_identity`, unique: true, columns: "(name, idempotency_key)" },
  { name: `${TABLE}_due`, unique: false, columns: "(name, state, next_attempt_at_ms, id)" },
  // The due index sorts by retry time; ordered passes need the oldest pending id without a sort.
  { name: `${TABLE}_order`, unique: false, columns: "(name, state, id)" },
] as const;

type Index = (typeof INDEXES)[number];

const createIndex = (index: Index, ifNotExists: boolean) =>
  `CREATE ${index.unique ? "UNIQUE " : ""}INDEX ${ifNotExists ? "IF NOT EXISTS " : ""}${
    index.name
  } ON ${TABLE} ${index.columns}`;

interface OutboxRow {
  readonly id: number | bigint;
  readonly name: string;
  readonly partition_id: string;
  readonly idempotency_key: string;
  readonly payload: string;
  readonly state: string;
  readonly attempts: number | bigint;
  readonly next_attempt_at_ms: number | bigint;
  readonly last_error: string | null;
  readonly dead_letter_reason: string | null;
  readonly enqueued_at_ms: number | bigint;
  readonly settled_at_ms: number | bigint | null;
}

/** Map driver failures to the operational outbox error, keeping the operation label. */
const unavailable = <A>(
  operation: string,
  effect: Effect.Effect<A, StoreFailure | SqlError.SqlError>,
): Effect.Effect<A, StoreFailure> =>
  Effect.catchTag(effect, "SqlError", (cause: SqlError.SqlError) =>
    Effect.fail(new OutboxUnavailable({ operation, detail: cause.message })),
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
    Effect.map((rows) => new Set(Array.map(rows, ({ name }) => name))),
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

export function migrateOutbox(sql: SqlClient.SqlClient): Effect.Effect<void, StoreFailure> {
  const statements = (dialect: "sqlite" | "pg"): Effect.Effect<void, SqlError.SqlError> =>
    Effect.forEach(
      [CREATE_TABLE[dialect], ...Array.map(INDEXES, (index) => createIndex(index, true))],
      (statement) => sql.unsafe<Record<string, never>>(statement),
      { discard: true },
    );

  return unavailable(
    "migrateOutbox",
    sql.onDialectOrElse({
      sqlite: () => statements("sqlite"),
      pg: () => statements("pg"),
      mysql: () => migrateMysql(sql),
      orElse: () => unsupported("migrateOutbox"),
    }),
  );
}

/** MySQL `INSERT IGNORE` would truncate an over-long key, so refuse it first. */
const mysqlKeyFault = (draft: OutboxDraft): OutboxUnavailable | undefined => {
  const limits = [
    ["name", draft.name, MYSQL_NAME_LENGTH],
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

const execute = (operation: string, statement: Effect.Effect<unknown, SqlError.SqlError>) =>
  unavailable(operation, statement).pipe(Effect.asVoid);

const rows = (
  operation: string,
  statement: Effect.Effect<ReadonlyArray<OutboxRow>, SqlError.SqlError>,
) =>
  unavailable(operation, statement).pipe(
    Effect.flatMap((found) => Effect.forEach(found, (row) => decodeRow(row))),
  );

function makeStore(sql: SqlClient.SqlClient): OutboxBacking {
  /** The scope filter. An omitted partition selects every partition. */
  const inScope = (name: string, partitionId: string | undefined) => [
    sql`name = ${name}`,
    ...(partitionId === undefined ? [] : [sql`partition_id = ${partitionId}`]),
  ];

  const update = Settlement.$match({
    Delivered: ({ attempts, atMs }) =>
      sql`state = 'delivered', attempts = ${attempts}, last_error = NULL, settled_at_ms = ${atMs}`,
    Retry: ({ attempts, nextAttemptAtMs, detail }) =>
      sql`state = 'pending', attempts = ${attempts}, next_attempt_at_ms = ${nextAttemptAtMs},
 last_error = ${detail}`,
    Dead: ({ attempts, reason, detail, atMs }) =>
      sql`state = 'dead', attempts = ${attempts}, dead_letter_reason = ${reason},
 last_error = ${detail}, settled_at_ms = ${atMs}`,
  });

  /** Whether the draft was new. A draft with a known name and key changes nothing. */
  const insert = (draft: OutboxDraft) => {
    const row = sql.insert({
      name: draft.name,
      partition_id: draft.partitionId,
      idempotency_key: draft.idempotencyKey,
      payload: draft.encodedPayload,
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

        return sql`INSERT IGNORE INTO streamsy_outbox ${row}`.raw.pipe(
          Effect.map(
            (result) =>
              Predicate.hasProperty(result, "affectedRows") && Number(result.affectedRows) > 0,
          ),
        );
      },
      orElse: () =>
        sql`INSERT INTO streamsy_outbox ${row}
 ON CONFLICT (name, idempotency_key) DO NOTHING RETURNING 1 AS inserted`.pipe(
          Effect.map((inserted) => inserted.length > 0),
        ),
    });
  };

  return {
    enqueue: (drafts: readonly OutboxDraft[]): Effect.Effect<EnqueueReport, StoreFailure> =>
      unavailable(
        "enqueue",
        Effect.gen(function* () {
          let enqueued = 0;

          for (const draft of drafts) if (yield* insert(draft)) enqueued += 1;

          return { enqueued, absorbed: drafts.length - enqueued };
        }),
      ),
    nextDue: ({ name, partitionId, ordered = false }) =>
      unavailable(
        "nextDue",
        sql<{ readonly at: number | bigint }>`SELECT next_attempt_at_ms AS at
 FROM streamsy_outbox WHERE ${sql.and([...inScope(name, partitionId), sql`state = 'pending'`])}
 ORDER BY ${ordered ? sql`id` : sql`next_attempt_at_ms, id`} LIMIT 1`,
      ).pipe(
        Effect.flatMap((found) =>
          found[0] === undefined
            ? Effect.succeed(Option.none<number>())
            : Schema.decodeEffect(StoredInt)(found[0].at).pipe(
                storedFailure("nextDue"),
                Effect.asSome,
              ),
        ),
      ),
    readDue: ({ name, partitionId, ordered = false }, nowMs, limit) =>
      rows(
        "readDue",
        sql<OutboxRow>`SELECT * FROM streamsy_outbox WHERE ${sql.and([
          ...inScope(name, partitionId),
          sql`state = 'pending'`,
          ...(ordered ? [] : [sql`next_attempt_at_ms <= ${nowMs}`]),
        ])} ORDER BY id LIMIT ${limit}`,
      ),
    settle: (id, settlement) =>
      execute("settle", sql`UPDATE streamsy_outbox SET ${update(settlement)} WHERE id = ${id}`),
    list: ({ name, partitionId }) =>
      rows(
        "list",
        sql<OutboxRow>`SELECT * FROM streamsy_outbox WHERE ${sql.and(
          inScope(name, partitionId),
        )} ORDER BY id`,
      ),
  };
}

/** A BIGINT may arrive as a number, a bigint or a digit string. Unsafe values are refused. */
const StoredInt = Schema.Union([
  Schema.Finite,
  Schema.BigInt,
  Schema.String.check(Schema.isPattern(/^-?\d+$/)),
]).pipe(
  Schema.decodeTo(
    Schema.Int,
    SchemaTransformation.transform<number, number | bigint | string>({
      decode: Number,
      encode: (value) => value,
    }),
  ),
);

const StoredRow = Schema.Struct({
  id: StoredInt,
  name: Schema.String,
  partition_id: Schema.String,
  idempotency_key: Schema.String,
  payload: Schema.String,
  state: EntryState,
  attempts: StoredInt,
  next_attempt_at_ms: StoredInt,
  last_error: Schema.NullOr(Schema.String),
  dead_letter_reason: Schema.NullOr(DeadLetterReason),
  enqueued_at_ms: StoredInt,
  settled_at_ms: Schema.NullOr(StoredInt),
});

const storedFailure = (operation: string) =>
  Effect.mapError(
    (cause: Schema.SchemaError) => new InvalidStoredRecord({ operation, detail: cause.message }),
  );

/** Decode structure independently of payload validity, then check state consistency. */
const decodeRow = (input: OutboxRow): Effect.Effect<OutboxEntry, InvalidStoredRecord> =>
  Effect.flatMap(
    Schema.decodeUnknownEffect(StoredRow)(input).pipe(storedFailure("decodeRow")),
    (row) => {
      const settled = row.settled_at_ms !== null;

      const consistent =
        row.id > 0 &&
        row.attempts >= 0 &&
        (row.state === "pending" ? !settled : settled && row.attempts > 0) &&
        (row.state === "dead") === (row.dead_letter_reason !== null);

      return consistent
        ? Effect.succeed({
            id: row.id,
            name: row.name,
            partitionId: row.partition_id,
            idempotencyKey: row.idempotency_key,
            encodedPayload: row.payload,
            state: row.state,
            attempts: row.attempts,
            nextAttemptAtMs: row.next_attempt_at_ms,
            enqueuedAtMs: row.enqueued_at_ms,
            settledAtMs: row.settled_at_ms ?? undefined,
            lastError: row.last_error ?? undefined,
            deadLetterReason: row.dead_letter_reason ?? undefined,
          })
        : Effect.fail(
            new InvalidStoredRecord({ operation: "decodeRow", detail: "Inconsistent outbox row" }),
          );
    },
  );

/**
 * The SQL store over the context's `SqlClient`. Run `migrateOutbox` at startup first.
 * Operations use that client's active transaction, so enqueue commits with causal writes.
 */
export const layerSql: Layer.Layer<OutboxStore, never, SqlClient.SqlClient> = Layer.effect(
  OutboxStore,
  Effect.map(SqlClient.SqlClient, makeStore),
);
