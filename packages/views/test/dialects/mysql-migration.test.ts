import { describe, expect, it } from "@effect/vitest";
import { Context, Deferred, Effect, Fiber, Layer, Result } from "effect";
import { SqlClient, Statement } from "effect/sql";
import { NonRetryable } from "@streamsy/projection";
import { dialectClient, sqlDialects } from "../../../storage/test/dialects/support.ts";
import { View, ViewFault } from "../../src/index.ts";
import { mysql } from "../../src/sql/mysql.ts";
import { fixtureDatabase } from "../support/backends.ts";

const dialect = sqlDialects.mysql;

const twoPools = Effect.gen(function* () {
  const url = yield* fixtureDatabase(dialect);

  return [
    Context.get(
      yield* Layer.build(dialectClient(dialect, url, { maxConnections: 1 })),
      SqlClient.SqlClient,
    ),
    Context.get(
      yield* Layer.build(dialectClient(dialect, url, { maxConnections: 1 })),
      SqlClient.SqlClient,
    ),
  ] as const;
});

const sessionId = (sql: SqlClient.SqlClient) =>
  Effect.map(sql<{ id: number }>`SELECT CONNECTION_ID() AS id`, (rows) => rows[0]?.id);

const unlockedAndReusable = (
  first: SqlClient.SqlClient,
  second: SqlClient.SqlClient,
  id: number | undefined,
) =>
  Effect.gen(function* () {
    expect(yield* sessionId(first)).toBe(id);

    const [lock] = yield* second<{
      owner: number | null;
    }>`SELECT IS_USED_LOCK('streamsy_view_layout') AS owner`;

    // Other fixture databases can migrate in parallel under the server-wide name.
    expect(lock?.owner).not.toBe(id);
  });

const layout = (sql: SqlClient.SqlClient) =>
  sql`SELECT id, version FROM streamsy_view_layout ORDER BY id`;

describe.skipIf(dialect.url === undefined)("MySQL Views migration", () => {
  it.live("serializes two hosts and returns the lock and both pooled connections", () =>
    Effect.gen(function* () {
      const [first, second] = yield* twoPools;
      const id = yield* sessionId(first);
      const other = yield* sessionId(second);
      expect(id).not.toBe(other);
      yield* Effect.all([View.migrate(first), View.migrate(second)], { concurrency: 2 });
      expect(yield* layout(first)).toEqual([{ id: 1, version: 1 }]);
      expect(yield* first.unsafe(dialect.tables)).toHaveLength(mysql.ddl.length);
      yield* unlockedAndReusable(first, second, id);
      yield* unlockedAndReusable(second, first, other);
    }),
  );

  it.live("rejects a newer version before any DDL and releases the session lock", () =>
    Effect.gen(function* () {
      const [first, second] = yield* twoPools;
      const id = yield* sessionId(first);
      yield* first.unsafe(mysql.ddl[0]!);
      yield* first`INSERT INTO streamsy_view_layout (id, version) VALUES (1, 2)`;

      const fault = yield* View.migrate(first).pipe(
        Effect.provideService(Statement.CurrentTransformer, (statement) => {
          expect(statement.compile()[0]).not.toMatch(/^(CREATE|ALTER|INSERT|UPDATE|DELETE)\b/u);

          return Effect.succeed(statement);
        }),
        Effect.flip,
      );

      expect(fault).toBeInstanceOf(ViewFault);
      expect(fault).toMatchObject({
        [NonRetryable]: true,
        message: expect.stringContaining("version 2"),
      });
      expect(yield* first.unsafe(dialect.tables)).toEqual([{ name: "streamsy_view_layout" }]);
      expect(yield* layout(first)).toEqual([{ id: 1, version: 2 }]);
      yield* unlockedAndReusable(first, second, id);
    }),
  );

  for (let prefix = 0; prefix <= mysql.ddl.length; prefix++) {
    it.live(`recovers after failure with ${prefix} DDL statements committed`, () =>
      Effect.gen(function* () {
        const [first, second] = yield* twoPools;
        const id = yield* sessionId(first);

        const stopAt = mysql.ddl[prefix] ?? "INSERT INTO streamsy_view_layout";

        const attempt = yield* View.migrate(first).pipe(
          Effect.provideService(Statement.CurrentTransformer, (statement, sql) =>
            Effect.succeed(
              statement.compile()[0].startsWith(stopAt)
                ? sql.unsafe("INVALID STATEMENT")
                : statement,
            ),
          ),
          Effect.result,
        );

        expect(Result.isFailure(attempt)).toBe(true);
        yield* unlockedAndReusable(first, second, id);
        expect(yield* first.unsafe(dialect.tables)).toHaveLength(prefix);

        if (prefix > 0) expect(yield* layout(first)).toEqual([]);

        yield* View.migrate(second);
        expect(yield* first.unsafe(dialect.tables)).toHaveLength(mysql.ddl.length);
        expect(yield* layout(first)).toEqual([{ id: 1, version: 1 }]);
      }),
    );
  }

  it.live(
    "releases the named lock and reuses the session after interrupting a running statement",
    () =>
      Effect.gen(function* () {
        const [first, second] = yield* twoPools;
        const id = yield* sessionId(first);

        const replacing = yield* Deferred.make<void>();

        const migration = yield* View.migrate(first).pipe(
          Effect.provideService(Statement.CurrentTransformer, (statement, sql) =>
            statement.compile()[0] === mysql.ddl[2]
              ? Deferred.succeed(replacing, undefined).pipe(
                  Effect.as(sql`SELECT SLEEP(30) AS vb4_pause`),
                )
              : Effect.succeed(statement),
          ),
          Effect.forkScoped,
        );

        yield* Deferred.await(replacing);
        yield* Effect.gen(function* () {
          while (true) {
            const [process] = yield* second<{
              info: string | null;
            }>`SELECT INFO AS info FROM information_schema.PROCESSLIST WHERE ID = ${id}`;

            if (process?.info?.includes("vb4_pause")) return;
            yield* Effect.sleep("10 millis");
          }
        }).pipe(Effect.timeout("5 seconds"));
        expect(yield* second`SELECT IS_USED_LOCK('streamsy_view_layout') AS owner`).toEqual([
          { owner: id },
        ]);
        yield* Fiber.interrupt(migration);

        yield* unlockedAndReusable(first, second, id);
        expect(yield* layout(first)).toEqual([]);
        yield* View.migrate(second);
        expect(yield* layout(first)).toEqual([{ id: 1, version: 1 }]);
      }),
    15_000,
  );
});
