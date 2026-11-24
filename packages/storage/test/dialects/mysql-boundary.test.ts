import { SqliteClient } from "@effect/sql-sqlite-bun";
import { it } from "@effect/vitest";
import { CommitBoundary } from "@streamsy/core";
import { Context, Effect, Layer, Option } from "effect";
import { SqlClient } from "effect/sql";
import { describe, expect } from "vitest";
import { storageLayer } from "../../src/storage.ts";
import { dialectClient, forkWaiting, freshDatabase, sqlDialects } from "./support.ts";

const mysql = sqlDialects.mysql;

const fixture = Effect.gen(function* () {
  if (mysql.url === undefined) return yield* Effect.die(new Error("Missing MySQL test URL"));
  const url = yield* freshDatabase(mysql, mysql.url);
  const client = dialectClient(mysql, url, { maxConnections: 1 });
  const host = yield* Layer.build(storageLayer().pipe(Layer.provideMerge(client)));
  const sql = Context.get(host, SqlClient.SqlClient);
  const boundary = Context.get(host, CommitBoundary);

  yield* sql`SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ`;
  yield* sql`CREATE TABLE boundary_probe (id INT PRIMARY KEY) ENGINE=InnoDB`;

  return { sql, boundary, client };
});

const readRows = (sql: SqlClient.SqlClient) =>
  sql<{ readonly id: number }>`SELECT id FROM boundary_probe ORDER BY id`;

describe.skipIf(mysql.url === undefined)("MySQL commit boundary", () => {
  it.live("sees a writer's intervening commit in the second plain read", () =>
    Effect.gen(function* () {
      const { sql, boundary, client } = yield* fixture;
      const writer = Context.get(yield* Layer.build(client), SqlClient.SqlClient);

      yield* sql`SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE`;

      const write = yield* forkWaiting(
        writer.withTransaction(writer`INSERT INTO boundary_probe VALUES (1)`),
      );

      yield* boundary.withTransaction(
        Effect.gen(function* () {
          expect(yield* readRows(sql)).toEqual([]);
          yield* write;
          expect(yield* readRows(sql)).toEqual([{ id: 1 }]);
        }),
      );
    }),
  );

  it.live("preserves the same session default after commit, rollback and a body defect", () =>
    Effect.gen(function* () {
      const { sql, boundary, client } = yield* fixture;
      const writer = Context.get(yield* Layer.build(client), SqlClient.SqlClient);

      const session = sql<{ readonly id: number; readonly isolation: string }>`SELECT
        CONNECTION_ID() AS id, @@SESSION.transaction_isolation AS isolation`;

      const before = yield* session;
      expect(before[0]?.isolation).toBe("REPEATABLE-READ");

      const checkRawSnapshot = (id: number) =>
        Effect.gen(function* () {
          const write = yield* forkWaiting(
            writer.withTransaction(writer`INSERT INTO boundary_probe VALUES (${id})`),
          );

          // Open the next transaction on this session before any other SQL can use
          // a pending one-shot isolation setting.
          yield* sql.withTransaction(
            Effect.gen(function* () {
              const initial = yield* readRows(sql);
              yield* write;
              expect(yield* readRows(sql)).toEqual(initial);
            }),
          );
          expect(yield* readRows(sql)).toContainEqual({ id });
          yield* writer`DELETE FROM boundary_probe WHERE id = ${id}`;
        });

      yield* boundary.withTransaction(sql`INSERT INTO boundary_probe VALUES (1)`);
      yield* checkRawSnapshot(10);
      expect(yield* session).toEqual(before);

      const rollback = boundary.withTransaction(
        sql`INSERT INTO boundary_probe VALUES (2)`.pipe(Effect.andThen(Effect.fail("rollback"))),
      );

      expect(yield* Effect.flip(rollback)).toBe("rollback");
      yield* checkRawSnapshot(11);
      expect(yield* session).toEqual(before);
      expect(yield* readRows(sql)).toEqual([{ id: 1 }]);

      const defect = boundary.withTransaction(
        sql`INSERT INTO boundary_probe VALUES (3)`.pipe(Effect.andThen(Effect.die("body defect"))),
      );

      expect(yield* Effect.exit(defect)).toMatchObject({ _tag: "Failure" });
      yield* checkRawSnapshot(12);
      expect(yield* session).toEqual(before);
      expect(yield* readRows(sql)).toEqual([{ id: 1 }]);
    }),
  );

  it.live("joins the same transaction on a nested boundary call", () =>
    Effect.gen(function* () {
      const { sql, boundary } = yield* fixture;

      const nested = boundary.withTransaction(
        Effect.gen(function* () {
          const transaction = yield* Effect.serviceOption(sql.transactionService);
          yield* boundary.withTransaction(
            Effect.gen(function* () {
              const joined = yield* Effect.serviceOption(sql.transactionService);
              expect(Option.getOrThrow(joined)).toBe(Option.getOrThrow(transaction));
              yield* sql`INSERT INTO boundary_probe VALUES (1)`;
            }),
          );
          expect(yield* readRows(sql)).toEqual([{ id: 1 }]);

          return yield* Effect.fail("rollback");
        }),
      );

      expect(yield* Effect.flip(nested)).toBe("rollback");
      expect(yield* readRows(sql)).toEqual([]);
    }),
  );

  it.live("lets a second SQL client use its own begin statement inside the body", () =>
    Effect.gen(function* () {
      const { boundary } = yield* fixture;

      const sqlite = Context.get(
        yield* Layer.build(SqliteClient.layer({ filename: ":memory:" })),
        SqlClient.SqlClient,
      );

      const rows = yield* boundary.withTransaction(
        sqlite.withTransaction(sqlite`SELECT 1 AS value`),
      );

      expect(rows).toEqual([{ value: 1 }]);
    }),
  );

  it.live("opens a SQLite transaction from a MySQL afterCommit callback", () =>
    Effect.gen(function* () {
      const { sql, boundary } = yield* fixture;

      const sqlite = Context.get(
        yield* Layer.build(SqliteClient.layer({ filename: ":memory:" })),
        SqlClient.SqlClient,
      );

      yield* sqlite`CREATE TABLE callback_probe (id INT PRIMARY KEY)`;

      const write = Effect.asVoid(
        Effect.orDie(sqlite.withTransaction(sqlite`INSERT INTO callback_probe VALUES (1)`)),
      );

      yield* boundary.withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO boundary_probe VALUES (1)`;
          yield* sql.afterCommit(write);
          expect(yield* sqlite`SELECT id FROM callback_probe`).toEqual([]);
        }),
      );

      expect(yield* sqlite`SELECT id FROM callback_probe`).toEqual([{ id: 1 }]);
    }),
  );

  it.live("uses the caller's begin statement and preserves it inside the body", () =>
    Effect.gen(function* () {
      const { sql, boundary } = yield* fixture;
      const statement = "START TRANSACTION READ ONLY";

      const failure = yield* boundary
        .withTransaction(
          Effect.gen(function* () {
            expect(yield* SqlClient.BeginTransaction).toBe(statement);

            return yield* sql`INSERT INTO boundary_probe VALUES (1)`;
          }),
        )
        .pipe(Effect.provideService(SqlClient.BeginTransaction, statement), Effect.flip);

      expect(failure).toMatchObject({ _tag: "SqlError", reason: { cause: { errno: 1792 } } });
      expect(yield* readRows(sql)).toEqual([]);
    }),
  );
});
