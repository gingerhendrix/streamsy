/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
/* oxlint-disable effecttsgo/strict-effect-provide -- Each test entry point builds its own store over a fresh database. */
/**
 * The SQL outbox on Postgres and MySQL. Each test runs in a fresh database and
 * skips when its URL is absent. Every case migrates first, as a placement does at start.
 */
import { describe, expect, test } from "vitest";
import { Array, Context, Effect, Exit, Layer, Option, Schema, type Scope } from "effect";
import { SqlClient } from "effect/sql";
import { InvalidStoredRecord, OutboxUnavailable } from "../src/errors.ts";
import { OutboxStore, Settlement, type OutboxBacking, type OutboxDraft } from "../src/store.ts";
import { layerSql, migrateOutbox } from "../src/sql.ts";
import { dialectClient, freshDatabase, sqlDialects, type SqlDialect } from "./dialect-support.ts";

const dialects: ReadonlyArray<SqlDialect> = [sqlDialects.pg, sqlDialects.mysql];

const TIMEOUT_MS = 60_000;

const isOutboxUnavailable = Schema.is(OutboxUnavailable);

const draft = (key: string, partitionId = "main", enqueuedAtMs = 1_000): OutboxDraft => ({
  name: "test.name",
  partitionId,
  idempotencyKey: key,
  encodedPayload: JSON.stringify({ key }),
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

/** The SQL store over `sql`, built through its public layer. */
const sqlStore = (sql: SqlClient.SqlClient): Effect.Effect<OutboxBacking> =>
  OutboxStore.use(Effect.succeed).pipe(
    Effect.provide(layerSql),
    Effect.provideService(SqlClient.SqlClient, sql),
  );

/** A migrated outbox, as a placement builds it at start. */
const withOutbox = <A, E>(
  dialect: SqlDialect,
  baseUrl: string,
  use: (outbox: OutboxBacking, sql: SqlClient.SqlClient) => Effect.Effect<A, E, Scope.Scope>,
) =>
  withClient(dialect, baseUrl, (sql) =>
    migrateOutbox(sql).pipe(
      Effect.andThen(sqlStore(sql)),
      Effect.flatMap((outbox) => use(outbox, sql)),
    ),
  );

for (const dialect of dialects) {
  const url = dialect.url;
  describe.skipIf(url === undefined)(`the ${dialect.name} outbox`, () => {
    test(
      "reads pending deadlines, including ordered and partition scopes",
      async () => {
        if (url === undefined) return;
        await withOutbox(dialect, url, (outbox) =>
          Effect.gen(function* () {
            expect(yield* outbox.nextDue({ name: "test.name" })).toEqual(Option.none());
            yield* outbox.enqueue([draft("a"), draft("b", "other", 2_000)]);
            yield* outbox.settle(
              1,
              Settlement.Retry({ attempts: 1, nextAttemptAtMs: 5_000, detail: "retry" }),
            );
            expect(yield* outbox.nextDue({ name: "test.name" })).toEqual(Option.some(2_000));
            expect(yield* outbox.nextDue({ name: "test.name", partitionId: "main" })).toEqual(
              Option.some(5_000),
            );
            expect(yield* outbox.nextDue({ name: "test.name", ordered: true })).toEqual(
              Option.some(5_000),
            );
            yield* outbox.settle(1, Settlement.Delivered({ attempts: 2, atMs: 5_000 }));
            yield* outbox.settle(
              2,
              Settlement.Dead({ attempts: 1, reason: "permanent", detail: "gone", atMs: 5_000 }),
            );
            expect(yield* outbox.nextDue({ name: "test.name" })).toEqual(Option.none());
          }),
        );
      },
      TIMEOUT_MS,
    );

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

            const keys = Array.map(
              yield* outbox.list({ name: "test.name" }),
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

            const firstIds = Array.map(
              yield* outbox.readDue({ name: "test.name", partitionId: "main" }, 1_000, 2),
              (entry) => entry.id,
            );

            yield* outbox.settle(
              1,
              Settlement.Retry({ attempts: 1, nextAttemptAtMs: 5_000, detail: "not yet" }),
            );

            const due = Array.map(
              yield* outbox.readDue({ name: "test.name" }, 1_000, 10),
              (entry) => entry.idempotencyKey,
            );

            yield* outbox.settle(2, Settlement.Delivered({ attempts: 1, atMs: 2_000 }));
            yield* outbox.settle(
              3,
              Settlement.Dead({
                attempts: 3,
                reason: "attempts-exhausted",
                detail: "handler down",
                atMs: 2_100,
              }),
            );

            const later = Array.map(
              yield* outbox.readDue({ name: "test.name" }, 9_000, 10),
              (entry) => entry.idempotencyKey,
            );

            return {
              firstIds,
              due,
              later,
              entries: yield* outbox.list({ name: "test.name", partitionId: "main" }),
            };
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
            yield* outbox.settle(1, Settlement.Delivered({ attempts: 1, atMs: now + 1 }));

            return yield* outbox.list({ name: "test.name" });
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

            const afterRollback = yield* outbox.list({ name: "test.name" });
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
      "persists safe attempt counts beyond 32-bit integer limits",
      async () => {
        if (url === undefined) return;
        await withOutbox(dialect, url, (outbox) =>
          Effect.gen(function* () {
            yield* outbox.enqueue([draft("a")]);
            yield* outbox.settle(
              1,
              Settlement.Retry({
                attempts: Number.MAX_SAFE_INTEGER - 1,
                nextAttemptAtMs: 1_000,
                detail: "retry",
              }),
            );
            expect((yield* outbox.readDue({ name: "test.name" }, 1_000, 1))[0]?.attempts).toBe(
              Number.MAX_SAFE_INTEGER - 1,
            );
            yield* outbox.settle(
              1,
              Settlement.Delivered({ attempts: Number.MAX_SAFE_INTEGER, atMs: 1_000 }),
            );
            expect((yield* outbox.list({ name: "test.name" }))[0]?.attempts).toBe(
              Number.MAX_SAFE_INTEGER,
            );
          }),
        );
      },
      TIMEOUT_MS,
    );

    test(
      "rejects unsafe BIGINT values before converting them to public numbers",
      async () => {
        if (url === undefined) return;
        await withOutbox(dialect, url, (outbox, sql) =>
          Effect.gen(function* () {
            yield* outbox.enqueue([draft("a")]);
            yield* sql`UPDATE streamsy_outbox SET next_attempt_at_ms = 9007199254740993`;
            expect(yield* outbox.nextDue({ name: "test.name" }).pipe(Effect.flip)).toBeInstanceOf(
              InvalidStoredRecord,
            );
            expect(yield* outbox.list({ name: "test.name" }).pipe(Effect.flip)).toBeInstanceOf(
              InvalidStoredRecord,
            );
          }),
        );
      },
      TIMEOUT_MS,
    );

    test(
      "reports structural corruption separately from payload poison",
      async () => {
        if (url === undefined) return;
        await withOutbox(dialect, url, (outbox, sql) =>
          Effect.gen(function* () {
            yield* outbox.enqueue([draft("a")]);
            yield* sql`UPDATE streamsy_outbox SET state = 'unknown'`;
            expect(yield* outbox.list({ name: "test.name" }).pipe(Effect.flip)).toBeInstanceOf(
              InvalidStoredRecord,
            );
          }),
        );
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
    "the backing reports the missing table and creates nothing",
    async () => {
      if (mysqlUrl === undefined) return;

      const result = await withClient(sqlDialects.mysql, mysqlUrl, (sql) =>
        Effect.gen(function* () {
          const outbox = yield* sqlStore(sql);
          const enqueue = yield* outbox.enqueue([draft("a")]).pipe(Effect.flip);
          const tables = yield* sql.unsafe<{ readonly name: string }>(sqlDialects.mysql.tables);

          return {
            operation: isOutboxUnavailable(enqueue) ? enqueue.operation : String(enqueue),
            tables: Array.map(tables, ({ name }) => name),
          };
        }),
      );

      expect(result).toEqual({ operation: "enqueue", tables: [] });
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
            tables: Array.map(tables, ({ name }) => name),
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
          const outbox = yield* sqlStore(sql);
          const failure = yield* outbox.enqueue([draft("k".repeat(513))]).pipe(Effect.flip);

          return {
            operation: isOutboxUnavailable(failure) ? failure.operation : String(failure),
            rows: yield* outbox.list({ name: "test.name" }),
          };
        }),
      );

      expect(result).toEqual({ operation: "enqueue", rows: [] });
    },
    TIMEOUT_MS,
  );
});
