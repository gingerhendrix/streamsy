import { describe, expect, it } from "@effect/vitest";
import { Context, Deferred, Effect, Fiber, Layer, Result } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState } from "@streamsy/core";
import { NonRetryable, Projection } from "@streamsy/projection";
import { dialectClient, sqlDialects } from "../../../storage/test/dialects/support.ts";
import { View, ViewFault } from "../../src/index.ts";
import { circuit, group, Results, Shop } from "../circuit/support/shop.ts";
import { fixtureDatabase, hostAt } from "../support/backends.ts";

const pg = sqlDialects.pg;

const testLock = 23017;

const twoClients = Effect.gen(function* () {
  const url = yield* fixtureDatabase(pg);

  return [
    Context.get(yield* Layer.build(dialectClient(pg, url)), SqlClient.SqlClient),
    Context.get(yield* Layer.build(dialectClient(pg, url)), SqlClient.SqlClient),
  ] as const;
});

// Block already-running statements in real transactions until the test releases them.
const holdGate = Effect.fnUntraced(function* (sql: SqlClient.SqlClient) {
  const locked = yield* Deferred.make<void>();
  const release = yield* Deferred.make<void>();

  const fiber = yield* sql
    .withTransaction(
      sql`SELECT pg_advisory_xact_lock(${testLock}::bigint)`.pipe(
        Effect.andThen(Deferred.succeed(locked, undefined)),
        Effect.andThen(Deferred.await(release)),
      ),
    )
    .pipe(Effect.forkScoped);

  yield* Deferred.await(locked);

  return {
    waitFor: (sessions: number) =>
      Effect.gen(function* () {
        while (true) {
          const [row] = yield* sql<{ count: string }>`SELECT count(*) FROM pg_locks
            WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND classid = 0 AND objid = ${testLock} AND objsubid = 1 AND NOT granted`;

          if (Number(row?.count) === sessions) return;
          yield* Effect.sleep("10 millis");
        }
      }).pipe(Effect.timeout("10 seconds")),
    release: Deferred.succeed(release, undefined).pipe(Effect.andThen(Fiber.join(fiber))),
  };
});

const installGate = Effect.fnUntraced(function* (sql: SqlClient.SqlClient, table: string) {
  yield* sql`CREATE FUNCTION views_test_gate() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN PERFORM pg_advisory_xact_lock(${sql.literal(String(testLock))}); RETURN NEW; END; $$`;
  yield* sql`CREATE TRIGGER gate BEFORE INSERT ON ${sql(table)}
    FOR EACH ROW EXECUTE FUNCTION views_test_gate()`;
});

const seed = (shop: string) =>
  Effect.gen(function* () {
    const input = yield* Shop;
    yield* input.create({ shop });
    yield* input.append(
      { shop },
      DurableState.changes(Shop.at({ shop }), {}, [
        DurableState.upsert("order", { orderId: "o", customerId: "c", total: 1 }),
      ]),
    );
  });

const twoHosts = Effect.gen(function* () {
  const url = yield* fixtureDatabase(pg);

  return [
    yield* Layer.build(hostAt(pg, group, url)),
    yield* Layer.build(hostAt(pg, group, url)),
  ] as const;
});

describe.skipIf(pg.url === undefined)("Postgres Views concurrency", () => {
  it.live("migrates concurrently from separate pools and records one layout version", () =>
    Effect.gen(function* () {
      const [first, second] = yield* twoClients;
      yield* Effect.all([View.migrate(first), View.migrate(second)], { concurrency: 2 });
      expect(yield* first`SELECT id, version FROM streamsy_view_layout`).toEqual([
        { id: 1, version: 1 },
      ]);
      expect(yield* first.unsafe(pg.tables)).toHaveLength(6);
    }),
  );

  it.live("rejects a newer layout without writing any tables or version", () =>
    Effect.gen(function* () {
      const [sql] = yield* twoClients;
      yield* sql`CREATE TABLE streamsy_view_layout (id SMALLINT PRIMARY KEY, version INTEGER NOT NULL)`;
      yield* sql`INSERT INTO streamsy_view_layout (id, version) VALUES (1, 2)`;
      const fault = yield* View.migrate(sql).pipe(Effect.flip);
      expect(fault).toBeInstanceOf(ViewFault);
      expect(fault).toMatchObject({
        message: "Unsupported Views layout version 2. Supported version is 1",
        [NonRetryable]: true,
      });
      expect(yield* sql.unsafe(pg.tables)).toEqual([{ name: "streamsy_view_layout" }]);
      expect(yield* sql`SELECT id, version FROM streamsy_view_layout`).toEqual([
        { id: 1, version: 2 },
      ]);
    }),
  );

  it.live(
    "races two first units, returns a retryable duplicate and succeeds on retry",
    () =>
      Effect.gen(function* () {
        const [left, right] = yield* twoHosts;
        const sql = Context.get(left, SqlClient.SqlClient);
        yield* View.migrate(sql);
        yield* seed("a").pipe(Effect.provide(left));
        // Empty before either runner so output creation does not serialize the race.
        yield* Results.use((output) => output.create({ shop: "a", generation: 1 })).pipe(
          Effect.provide(left),
        );
        yield* installGate(sql, "streamsy_view_circuits");
        const gate = yield* holdGate(sql);
        const binding = circuit().at({ shop: "a" });
        const run = Projection.run(binding, { limit: 1 });

        const [first, second] = yield* Effect.all(
          [
            run.pipe(Effect.provide(left), Effect.result),
            run.pipe(Effect.provide(right), Effect.result),
            gate.waitFor(2).pipe(Effect.andThen(gate.release)),
          ],
          { concurrency: 3 },
        );

        const winner = Result.isSuccess(first) ? first : second;
        const loser = Result.isFailure(first) ? first : second;
        expect(winner).toMatchObject({ _tag: "Success", success: { units: 1 } });
        expect(loser).toMatchObject({
          _tag: "Failure",
          failure: {
            phase: "process",
            reason: "storage-failure",
            retryable: true,
            cause: { reason: { _tag: "UniqueViolation" } },
          },
        });

        const retry = yield* run.pipe(Effect.provide(Result.isFailure(first) ? left : right));
        expect(retry.units).toBe(0);
        expect(yield* sql`SELECT plan_hash FROM streamsy_view_circuits`).toHaveLength(1);
      }),
    30_000,
  );

  it.live(
    "retires a binding while a neighbour is updating its operator rows",
    () =>
      Effect.gen(function* () {
        const [left, right] = yield* twoHosts;
        const sql = Context.get(left, SqlClient.SqlClient);
        yield* View.migrate(sql);
        const declaration = circuit();
        const retired = declaration.at({ shop: "a" });
        const neighbour = declaration.at({ shop: "b" });
        yield* Effect.gen(function* () {
          yield* seed("a");
          yield* seed("b");
          yield* Projection.run(retired);
          yield* Projection.run(neighbour);
          const input = yield* Shop;
          const params = { shop: "b" };
          yield* input.append(
            params,
            DurableState.changes(Shop.at(params), {}, [
              DurableState.upsert("order", { orderId: "o", customerId: "c", total: 2 }),
            ]),
          );
        }).pipe(Effect.provide(left));

        yield* installGate(sql, "streamsy_view_integrals");
        const gate = yield* holdGate(sql);

        const inFlight = yield* Projection.run(neighbour, { limit: 1 }).pipe(
          Effect.provide(right),
          Effect.forkScoped,
        );

        yield* gate.waitFor(1);

        const didRetire = yield* Projection.retire(
          declaration,
          { shop: "a" },
          { generation: 1 },
        ).pipe(Effect.provide(left), Effect.timeout("5 seconds"));

        expect(didRetire.record).toBe("retired");
        yield* gate.release;
        expect((yield* Fiber.join(inFlight)).units).toBe(1);
        expect(yield* sql`SELECT namespace FROM streamsy_view_circuits`).toEqual([
          { namespace: expect.stringContaining('\\"shop\\":\\"b\\"') },
        ]);
      }),
    30_000,
  );
});
