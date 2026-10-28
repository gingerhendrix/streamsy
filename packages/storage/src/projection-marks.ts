import { Array, Effect, Option } from "effect";
import type { SqlClient, SqlError } from "effect/sql";
import { StorageFault } from "@streamsy/core";
import { ProjectionMarks, type PendingMark } from "@streamsy/core/internal";

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
});

/**
 * Pending marks in `streamsy_projection_pending`. Statements join the boundary
 * transaction when one is open, so a mark written by a commit hook commits or
 * rolls back with the write that caused it.
 */
export const sqlProjectionMarks = (sql: SqlClient.SqlClient) => {
  const table = sql(TABLE);

  const mark = (projection: string, params: ReadonlyArray<string>) =>
    Effect.forEach(
      Array.chunksOf(params, MARK_CHUNK),
      (chunk) => {
        const rows = sql.insert(
          Array.map(chunk, (value) => ({
            projection,
            params: value,
            seq: 1,
            attempts: 0,
            due_at: 0,
          })),
        );

        return sql.onDialectOrElse({
          mysql: () => sql`INSERT INTO ${table} ${rows} ON DUPLICATE KEY UPDATE seq = seq + 1`,
          orElse: () =>
            sql`INSERT INTO ${table} ${rows}
 ON CONFLICT (projection, params) DO UPDATE SET seq = ${table}.seq + 1`,
        });
      },
      { discard: true },
    );

  const nextDue = (projection: string) =>
    sql<{
      readonly at: SqlNumber | null;
    }>`SELECT MIN(due_at) AS at FROM ${table} WHERE projection = ${projection}`.pipe(
      Effect.map((rows) => {
        const at = rows[0]?.at;

        return at === null || at === undefined ? Option.none<number>() : Option.some(Number(at));
      }),
      Effect.mapError(fault("nextDue")),
    );

  const due = (projection: string, now: number, limit: number) =>
    sql<MarkRow>`SELECT params, seq, attempts, due_at FROM ${table}
 WHERE projection = ${projection} AND due_at <= ${now}
 ORDER BY due_at, params LIMIT ${limit}`.pipe(
      Effect.map(Array.map(pendingMark)),
      Effect.mapError(fault("due")),
    );

  const read = (projection: string, params: ReadonlyArray<string>) =>
    Effect.forEach(
      Array.chunksOf(params, READ_CHUNK),
      (chunk) =>
        sql<MarkRow>`SELECT params, seq, attempts, due_at FROM ${table}
 WHERE projection = ${projection} AND ${sql.in("params", chunk)}`,
    ).pipe(
      Effect.map(
        (chunks) => new Map(Array.map(chunks.flat(), (row) => [row.params, pendingMark(row)])),
      ),
      Effect.mapError(fault("read")),
    );

  const clear = (projection: string, params: string, seq: number) =>
    sql`DELETE FROM ${table} WHERE projection = ${projection} AND params = ${params} AND seq = ${seq}`.pipe(
      Effect.asVoid,
      Effect.mapError(fault("clear")),
    );

  const remove = (projection: string, params: string) =>
    sql`DELETE FROM ${table} WHERE projection = ${projection} AND params = ${params}`.pipe(
      Effect.asVoid,
      Effect.mapError(fault("remove")),
    );

  const defer = (projection: string, params: string, attempts: number, dueAt: number) => {
    const row = sql.insert({ projection, params, seq: 1, attempts, due_at: dueAt });

    return sql
      .onDialectOrElse({
        mysql: () =>
          sql`INSERT INTO ${table} ${row}
 ON DUPLICATE KEY UPDATE attempts = VALUES(attempts), due_at = VALUES(due_at)`,
        orElse: () =>
          sql`INSERT INTO ${table} ${row}
 ON CONFLICT (projection, params) DO UPDATE SET attempts = excluded.attempts, due_at = excluded.due_at`,
      })
      .pipe(Effect.asVoid, Effect.mapError(fault("defer")));
  };

  const deferred = (projection: string) =>
    sql<{
      readonly count: SqlNumber;
    }>`SELECT COUNT(*) AS count FROM ${table} WHERE projection = ${projection} AND attempts > 0`.pipe(
      Effect.map((rows) => Number(rows[0]?.count ?? 0)),
      Effect.mapError(fault("deferred")),
    );

  return ProjectionMarks.of({ mark, nextDue, due, read, clear, remove, defer, deferred });
};
