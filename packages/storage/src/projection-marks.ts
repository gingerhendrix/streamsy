import { Array, Effect, Option } from "effect";
import type { SqlClient, SqlError } from "effect/sql";
import { StorageFault } from "@streamsy/core";
import {
  ProjectionMarks,
  fitKey,
  type ProjectionMarksApi,
  type PendingMark,
} from "@streamsy/core/internal";
import { MYSQL_PROJECTION_PARAMS_LENGTH } from "./schema.ts";

const TABLE = "streamsy_projection_pending";

/**
 * Durable Object SQL accepts at most 100 bound parameters in one statement. A
 * marked row binds five values and a params lookup binds one, so both chunk.
 */
const MARK_CHUNK = 16;

const READ_CHUNK = 64;

/** BIGINT columns come back as strings on Postgres and sometimes on MySQL. */
type SqlNumber = number | string | bigint;

interface MarkRow {
  readonly params: string;
  readonly seq: SqlNumber;
  readonly attempts: SqlNumber;
  readonly due_at: SqlNumber;
  readonly wake_at: SqlNumber | null;
  readonly stopped: SqlNumber;
  readonly failed_at: SqlNumber | null;
  readonly failure_reason: string | null;
  readonly failure_message: string | null;
}

const fault = (operation: string) => (error: SqlError.SqlError) =>
  new StorageFault({
    operation: `marks.${operation}`,
    message: error.message,
    retryable: error.isRetryable,
    cause: error,
  });

const pendingMark = (row: MarkRow): PendingMark => ({
  params: row.params,
  seq: Number(row.seq),
  attempts: Number(row.attempts),
  dueAt: Number(row.due_at),
  wakeAt: row.wake_at === null ? Option.none() : Option.some(Number(row.wake_at)),
  stopped: Number(row.stopped) !== 0,
  failure:
    row.failed_at === null
      ? Option.none()
      : Option.some({
          at: Number(row.failed_at),
          reason: Option.fromNullishOr(row.failure_reason),
          message: row.failure_message ?? "",
        }),
});

/**
 * Pending marks in `streamsy_projection_pending`. Statements join the boundary
 * transaction when one is open, so a mark written by a commit hook commits or
 * rolls back with the write that caused it.
 */
export const sqlProjectionMarks = (sql: SqlClient.SqlClient) => {
  const table = sql(TABLE);
  const mysql = sql.onDialectOrElse({ mysql: () => true, orElse: () => false });
  const readParams = sql.literal(mysql ? "COALESCE(full_params, params) AS params" : "params");

  const storedParams = (params: string) =>
    mysql ? fitKey(params, MYSQL_PROJECTION_PARAMS_LENGTH) : Effect.succeed(params);

  const writeParams = (params: string) =>
    mysql
      ? Effect.map(storedParams(params), (fitted) => ({ params: fitted, full_params: params }))
      : Effect.succeed({ params });

  const mark = (projection: string, params: ReadonlyArray<string>) =>
    Effect.forEach(
      Array.chunksOf(params, MARK_CHUNK),
      Effect.fnUntraced(function* (chunk) {
        const values = yield* Effect.forEach(chunk, writeParams);

        const rows = sql.insert(
          Array.map(values, (keys) => ({
            projection,
            ...keys,
            seq: 1,
            attempts: 0,
            due_at: 0,
          })),
        );

        yield* sql.onDialectOrElse({
          mysql: () => sql`INSERT INTO ${table} ${rows} ON DUPLICATE KEY UPDATE seq = seq + 1,
 full_params = VALUES(full_params), due_at = CASE WHEN wake_at IS NOT NULL THEN 0 ELSE due_at END, wake_at = NULL`,
          orElse: () =>
            sql`INSERT INTO ${table} ${rows}
 ON CONFLICT (projection, params) DO UPDATE SET seq = ${table}.seq + 1,
 due_at = CASE WHEN ${table}.wake_at IS NOT NULL THEN 0 ELSE ${table}.due_at END, wake_at = NULL`,
        });
      }),
      { discard: true },
    );

  const nextDue = (projection: string) =>
    sql<{
      readonly at: SqlNumber | null;
    }>`SELECT MIN(due_at) AS at FROM ${table} WHERE projection = ${projection} AND stopped = 0`.pipe(
      Effect.map((rows) => {
        const at = rows[0]?.at;

        return at === null || at === undefined ? Option.none<number>() : Option.some(Number(at));
      }),
      Effect.mapError(fault("nextDue")),
    );

  const summary = (projection: string, now: number) =>
    sql<{
      readonly fresh: SqlNumber | null;
      readonly sleeping: SqlNumber | null;
      readonly retry_due: SqlNumber | null;
      readonly backing_off: SqlNumber | null;
      readonly stopped: SqlNumber | null;
      readonly next_due_at: SqlNumber | null;
    }>`SELECT
 SUM(CASE WHEN stopped = 0 AND attempts = 0 AND wake_at IS NULL THEN 1 ELSE 0 END) AS fresh,
 SUM(CASE WHEN stopped = 0 AND wake_at IS NOT NULL THEN 1 ELSE 0 END) AS sleeping,
 SUM(CASE WHEN stopped = 0 AND attempts > 0 AND due_at <= ${now} THEN 1 ELSE 0 END) AS retry_due,
 SUM(CASE WHEN stopped = 0 AND attempts > 0 AND due_at > ${now} THEN 1 ELSE 0 END) AS backing_off,
 SUM(CASE WHEN stopped <> 0 THEN 1 ELSE 0 END) AS stopped,
 MIN(CASE WHEN stopped = 0 THEN due_at ELSE NULL END) AS next_due_at FROM ${table} WHERE projection = ${projection}`.pipe(
      Effect.map((rows) => {
        const row = rows[0];
        const at = row?.next_due_at;

        return {
          fresh: Number(row?.fresh ?? 0),
          sleeping: Number(row?.sleeping ?? 0),
          retryDue: Number(row?.retry_due ?? 0),
          backingOff: Number(row?.backing_off ?? 0),
          stopped: Number(row?.stopped ?? 0),
          nextDueAt:
            at === null || at === undefined ? Option.none<number>() : Option.some(Number(at)),
        };
      }),
      Effect.mapError(fault("summary")),
    );

  // MySQL can use the due index in deadline order. Adding params to the
  // order requires a filesort even when it refers to the underlying column.
  const dueOrder = sql.literal(mysql ? "due_at" : "due_at, params");

  const due = (projection: string, now: number, limit: number) =>
    sql<MarkRow>`SELECT ${readParams}, seq, attempts, due_at, wake_at, stopped, failed_at, failure_reason, failure_message FROM ${table}
 WHERE projection = ${projection} AND stopped = 0 AND due_at <= ${now}
 ORDER BY ${dueOrder} LIMIT ${limit}`.pipe(
      Effect.map(Array.map(pendingMark)),
      Effect.mapError(fault("due")),
    );

  const read = (projection: string, params: ReadonlyArray<string>) =>
    Effect.forEach(
      Array.chunksOf(params, READ_CHUNK),
      Effect.fnUntraced(function* (chunk) {
        const keys = yield* Effect.forEach(chunk, storedParams);

        return yield* sql<MarkRow>`SELECT ${readParams}, seq, attempts, due_at, wake_at, stopped, failed_at, failure_reason, failure_message FROM ${table}
 WHERE projection = ${projection} AND ${sql.in("params", keys)}`;
      }),
    ).pipe(
      Effect.map(
        (chunks) => new Map(Array.map(chunks.flat(), (row) => [row.params, pendingMark(row)])),
      ),
      Effect.mapError(fault("read")),
    );

  const clear = (projection: string, params: string, seq: number) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const key = yield* storedParams(params);
          yield* sql`DELETE FROM ${table} WHERE projection = ${projection} AND params = ${key} AND seq = ${seq}`;
          yield* sql`UPDATE ${table} SET attempts = 0, due_at = 0, wake_at = NULL, stopped = 0,
 failed_at = NULL, failure_reason = NULL, failure_message = NULL
 WHERE projection = ${projection} AND params = ${key} AND seq <> ${seq}`;
        }),
      )
      .pipe(Effect.mapError(fault("clear")));

  const sleep = (projection: string, params: string, seq: number, wakeAt: number) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const row = sql.insert({
            projection,
            ...(yield* writeParams(params)),
            seq: 1,
            attempts: 0,
            due_at: wakeAt,
            wake_at: wakeAt,
          });

          // Insert first so an absent row sleeps too. On conflict keep seq unchanged.
          // MySQL evaluates assignments left to right; neither deadline changes seq.
          yield* sql.onDialectOrElse({
            mysql: () => sql`INSERT INTO ${table} ${row} ON DUPLICATE KEY UPDATE
 full_params = VALUES(full_params),
 due_at = CASE WHEN seq = ${seq} THEN ${wakeAt} ELSE 0 END,
 wake_at = CASE WHEN seq = ${seq} THEN ${wakeAt} ELSE NULL END,
 attempts = 0, stopped = 0, failed_at = NULL, failure_reason = NULL, failure_message = NULL`,
            orElse:
              () => sql`INSERT INTO ${table} ${row} ON CONFLICT (projection, params) DO UPDATE SET
 due_at = CASE WHEN ${table}.seq = ${seq} THEN ${wakeAt} ELSE 0 END,
 wake_at = CASE WHEN ${table}.seq = ${seq} THEN ${wakeAt} ELSE NULL END,
 attempts = 0, stopped = 0, failed_at = NULL, failure_reason = NULL, failure_message = NULL`,
          });
        }),
      )
      .pipe(Effect.mapError(fault("sleep")));

  const remove = (projection: string, params: string) =>
    storedParams(params).pipe(
      Effect.flatMap(
        (key) => sql`DELETE FROM ${table} WHERE projection = ${projection} AND params = ${key}`,
      ),
      Effect.asVoid,
      Effect.mapError(fault("remove")),
    );

  const fail = Effect.fnUntraced(function* (
    projection: string,
    params: string,
    options: Parameters<ProjectionMarksApi["fail"]>[2],
  ) {
    const row = sql.insert({
      projection,
      ...(yield* writeParams(params)),
      seq: 1,
      attempts: options.attempts,
      due_at: Option.getOrElse(options.dueAt, () => 0),
      wake_at: null,
      stopped: Option.isNone(options.dueAt) ? 1 : 0,
      failed_at: options.failure.at,
      failure_reason: Option.getOrNull(options.failure.reason),
      failure_message: options.failure.message.slice(0, 1_000),
    });

    yield* sql
      .onDialectOrElse({
        mysql: () =>
          sql`INSERT INTO ${table} ${row}
 ON DUPLICATE KEY UPDATE full_params = VALUES(full_params), attempts = VALUES(attempts), due_at = VALUES(due_at), wake_at = NULL,
 stopped = VALUES(stopped), failed_at = VALUES(failed_at),
 failure_reason = VALUES(failure_reason), failure_message = VALUES(failure_message)`,
        orElse: () =>
          sql`INSERT INTO ${table} ${row}
 ON CONFLICT (projection, params) DO UPDATE SET attempts = excluded.attempts, due_at = excluded.due_at, wake_at = NULL,
 stopped = excluded.stopped, failed_at = excluded.failed_at,
 failure_reason = excluded.failure_reason, failure_message = excluded.failure_message`,
      })
      .pipe(Effect.asVoid, Effect.mapError(fault("fail")));
  });

  const failed = (projection: string, limit: number) =>
    sql<MarkRow>`SELECT ${readParams}, seq, attempts, due_at, wake_at, stopped, failed_at, failure_reason, failure_message FROM ${table}
 WHERE projection = ${projection} AND attempts > 0
 ORDER BY stopped DESC, due_at, params LIMIT ${limit}`.pipe(
      Effect.map(Array.map(pendingMark)),
      Effect.mapError(fault("failed")),
    );

  const resume = (projection: string) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql<{ readonly count: SqlNumber }>`SELECT COUNT(*) AS count
 FROM ${table} WHERE projection = ${projection} AND stopped <> 0`;

          yield* sql`UPDATE ${table} SET due_at = 0, stopped = 0 WHERE projection = ${projection} AND stopped <> 0`;

          return Number(rows[0]?.count ?? 0);
        }),
      )
      .pipe(Effect.mapError(fault("resume")));

  const retry = (projection: string, params?: string) => {
    // Count and update in one transaction on all dialects, including MySQL,
    // which has no UPDATE RETURNING. Neither attempts nor seq is reset.
    return sql
      .withTransaction(
        Effect.gen(function* () {
          const key = params === undefined ? undefined : yield* storedParams(params);

          const selected = sql`projection = ${projection} AND (stopped <> 0 OR (attempts > 0 AND due_at <> 0))
            ${key === undefined ? sql`` : sql`AND params = ${key}`}`;

          const rows = yield* sql<{ readonly count: SqlNumber }>`SELECT COUNT(*) AS count
          FROM ${table} WHERE ${selected}`;

          yield* sql`UPDATE ${table} SET due_at = 0, stopped = 0 WHERE ${selected}`;

          return Number(rows[0]?.count ?? 0);
        }),
      )
      .pipe(Effect.mapError(fault("retry")));
  };

  return ProjectionMarks.of({
    mark,
    nextDue,
    summary,
    due,
    read,
    clear,
    sleep,
    remove,
    fail,
    failed,
    resume,
    retry,
  });
};
