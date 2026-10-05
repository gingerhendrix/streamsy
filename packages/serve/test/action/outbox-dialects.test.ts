/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
/**
 * The SQL outbox on Postgres and MySQL. Each test runs in a fresh database and
 * skips when its URL is absent. MySQL needs the eager migration, so every case
 * migrates first, as a placement does at start.
 */
import { describe, expect, test } from "bun:test";
import { Context, Effect, Exit, Layer, Schema, type Scope } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import { OutboxUnavailable } from "@streamsy/serve/action/errors";
import type { OutboxBacking, OutboxDraft } from "@streamsy/serve/action/outbox";
import { createSqlOutboxBacking, migrateOutbox } from "@streamsy/serve/action/sql";
import {
  dialectClient,
  freshDatabase,
  sqlDialects,
  type SqlDialect,
} from "../../../storage/test/dialects/support.ts";

const dialects: ReadonlyArray<SqlDialect> = [sqlDialects.pg, sqlDialects.mysql];
const TIMEOUT_MS = 60_000;
const isOutboxUnavailable = Schema.is(OutboxUnavailable);

const draft = (key: string, partitionId = "main", enqueuedAtMs = 1_000): OutboxDraft => ({
  sink: "test.sink",
  partitionId,
  idempotencyKey: key,
  payload: JSON.stringify({ key }),
  enqueuedAtMs,
});

/** Run `use` with a client on a fresh database of `dialect`. */
const withClient = <A, E>(
  dialect: SqlDialect,
  baseUrl: string,
  use: (sql: SqlClient.SqlClient) => Effect.Effect<A, E, Scope.Scope>,
) =>
  Effect.runPromise(
    freshDatabase(dialect, baseUrl).pipe(
      Effect.flatMap((url) =>
        Layer.build(dialectClient(dialect, url)).pipe(
          Effect.flatMap((context) => use(Context.get(context, SqlClient.SqlClient))),
          Effect.scoped,
        ),
      ),
      Effect.scoped,
    ),
  );

/** A migrated outbox, as a placement builds it at start. */
const withOutbox = <A, E>(
  dialect: SqlDialect,
  baseUrl: string,
  use: (outbox: OutboxBacking, sql: SqlClient.SqlClient) => Effect.Effect<A, E, Scope.Scope>,
) =>
  withClient(dialect, baseUrl, (sql) =>
    migrateOutbox(sql).pipe(Effect.andThen(use(createSqlOutboxBacking(sql), sql))),
  );

for (const dialect of dialects) {
  const url = dialect.url;
  describe.skipIf(url === undefined)(`the ${dialect.name} outbox`, () => {
    test(
      "absorbs a repeated idempotency key instead of enqueuing twice",
      async () => {
        if (url === undefined) return;
        const result = await withOutbox(dialect, url, (outbox) =>
          Effect.gen(function* () {
            const first = yield* outbox.enqueue([draft("a"), draft("b")]);
            const second = yield* outbox.enqueue([draft("a"), draft("c")]);
            // Keys compare exactly: case and trailing spaces make another key.
            const third = yield* outbox.enqueue([draft("A"), draft("a "), draft("a")]);
            const keys = (yield* outbox.list("test.sink", undefined)).map(
              (entry) => entry.idempotencyKey,
            );
            return { first, second, third, keys };
          }),
        );
        expect(result).toEqual({
          first: { enqueued: 2, absorbed: 0 },
          second: { enqueued: 1, absorbed: 1 },
          third: { enqueued: 2, absorbed: 1 },
          keys: ["a", "b", "c", "A", "a "],
        });
      },
      TIMEOUT_MS,
    );

    test(
      "claims due work in enqueue order and settles it",
      async () => {
        if (url === undefined) return;
        const result = await withOutbox(dialect, url, (outbox) =>
          Effect.gen(function* () {
            yield* outbox.enqueue([draft("a"), draft("b"), draft("c"), draft("d", "other")]);
            const firstIds = (yield* outbox.claimDue("test.sink", "main", 1_000, 2)).map(
              (entry) => entry.id,
            );
            yield* outbox.reschedule(1, 1, 5_000, "not yet");
            const due = (yield* outbox.claimDue("test.sink", undefined, 1_000, 10)).map(
              (entry) => entry.idempotencyKey,
            );
            yield* outbox.markDelivered(2, 1, 2_000);
            yield* outbox.deadLetter(3, 3, "attempts-exhausted", "handler down", 2_100);
            const later = (yield* outbox.claimDue("test.sink", undefined, 9_000, 10)).map(
              (entry) => entry.idempotencyKey,
            );
            return { firstIds, due, later, entries: yield* outbox.list("test.sink", "main") };
          }),
        );
        expect(result.firstIds).toEqual([1, 2]);
        expect(result.due).toEqual(["b", "c", "d"]);
        expect(result.later).toEqual(["a", "d"]);
        expect(result.entries).toMatchObject([
          { idempotencyKey: "a", state: "pending", attempts: 1, lastError: "not yet" },
          { idempotencyKey: "b", state: "delivered", attempts: 1, settledAtMs: 2_000 },
          {
            idempotencyKey: "c",
            state: "dead",
            attempts: 3,
            deadLetterReason: "attempts-exhausted",
            lastError: "handler down",
            settledAtMs: 2_100,
          },
        ]);
      },
      TIMEOUT_MS,
    );

    test(
      "decodes BIGINT ids and times as numbers",
      async () => {
        if (url === undefined) return;
        // Past 2^31, so a 4-byte column could not hold it.
        const now = 1_790_000_000_000;
        const [entry] = await withOutbox(dialect, url, (outbox) =>
          Effect.gen(function* () {
            yield* outbox.enqueue([draft("a", "main", now)]);
            yield* outbox.markDelivered(1, 1, now + 1);
            return yield* outbox.list("test.sink", undefined);
          }),
        );
        expect(entry).toMatchObject({
          id: 1,
          nextAttemptAtMs: now,
          enqueuedAtMs: now,
          settledAtMs: now + 1,
        });
        for (const value of [
          entry?.id,
          entry?.nextAttemptAtMs,
          entry?.enqueuedAtMs,
          entry?.settledAtMs,
        ])
          expect(value).toBeTypeOf("number");
      },
      TIMEOUT_MS,
    );

    test(
      "an enqueue rolls back with the caller's transaction",
      async () => {
        if (url === undefined) return;
        const result = await withOutbox(dialect, url, (outbox, sql) =>
          Effect.gen(function* () {
            const exit = yield* Effect.exit(
              sql.withTransaction(
                Effect.gen(function* () {
                  yield* outbox.enqueue([draft("a")]);
                  return yield* new OutboxUnavailable({
                    operation: "testRollback",
                    detail: "the caller's own write failed",
                  });
                }),
              ),
            );
            const afterRollback = yield* outbox.list("test.sink", undefined);
            const retry = yield* outbox.enqueue([draft("a")]);
            return { failed: Exit.isFailure(exit), afterRollback, retry };
          }),
        );
        expect(result).toEqual({
          failed: true,
          afterRollback: [],
          retry: { enqueued: 1, absorbed: 0 },
        });
      },
      TIMEOUT_MS,
    );

    test(
      "migrating twice is safe",
      async () => {
        if (url === undefined) return;
        const result = await withOutbox(dialect, url, (outbox, sql) =>
          Effect.gen(function* () {
            yield* outbox.enqueue([draft("a")]);
            yield* migrateOutbox(sql);
            return yield* outbox.enqueue([draft("a"), draft("b")]);
          }),
        );
        expect(result).toEqual({ enqueued: 1, absorbed: 1 });
      },
      TIMEOUT_MS,
    );
  });
}

const mysqlUrl = sqlDialects.mysql.url;
describe.skipIf(mysqlUrl === undefined)("the mysql outbox without the eager migration", () => {
  test(
    "the backing reports the missing tables and creates nothing",
    async () => {
      if (mysqlUrl === undefined) return;
      const result = await withClient(sqlDialects.mysql, mysqlUrl, (sql) =>
        Effect.gen(function* () {
          const outbox = createSqlOutboxBacking(sql);
          const enqueue = yield* outbox.enqueue([draft("a")]).pipe(Effect.flip);
          const tables = yield* sql.unsafe<{ readonly name: string }>(sqlDialects.mysql.tables);
          return { operation: enqueue.operation, tables: tables.map(({ name }) => name) };
        }),
      );
      expect(result).toEqual({ operation: "ready", tables: [] });
    },
    TIMEOUT_MS,
  );

  test(
    "migrateOutbox refuses to run inside a transaction",
    async () => {
      if (mysqlUrl === undefined) return;
      const result = await withClient(sqlDialects.mysql, mysqlUrl, (sql) =>
        Effect.gen(function* () {
          const failure = yield* sql.withTransaction(migrateOutbox(sql)).pipe(Effect.flip);
          const tables = yield* sql.unsafe<{ readonly name: string }>(sqlDialects.mysql.tables);
          return {
            operation: isOutboxUnavailable(failure) ? failure.operation : String(failure),
            tables: tables.map(({ name }) => name),
          };
        }),
      );
      expect(result).toEqual({ operation: "migrateOutbox", tables: [] });
    },
    TIMEOUT_MS,
  );

  test(
    "an over-long key is refused, not truncated",
    async () => {
      if (mysqlUrl === undefined) return;
      const result = await withClient(sqlDialects.mysql, mysqlUrl, (sql) =>
        Effect.gen(function* () {
          yield* migrateOutbox(sql);
          const outbox = createSqlOutboxBacking(sql);
          const failure = yield* outbox.enqueue([draft("k".repeat(513))]).pipe(Effect.flip);
          return { operation: failure.operation, rows: yield* outbox.list("test.sink", undefined) };
        }),
      );
      expect(result).toEqual({ operation: "enqueue", rows: [] });
    },
    TIMEOUT_MS,
  );
});
