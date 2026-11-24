import { describe, expect, it } from "@effect/vitest";
import { DurableState } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Array, Context, Deferred, Effect, Fiber, Layer } from "effect";
import { SqlClient, Statement } from "effect/sql";
import { sqlDialects, withRepeatableRead } from "../../../storage/test/dialects/support.ts";
import { View } from "../../src/index.ts";
import { circuit, group, Results, Shop } from "../circuit/support/shop.ts";
import { fixtureDatabase, hostAt } from "../support/backends.ts";

const levels = [
  { name: "Postgres", dialect: sqlDialects.pg, repeatableRead: false },
  { name: "MySQL default", dialect: sqlDialects.mysql, repeatableRead: false },
  { name: "MySQL REPEATABLE READ", dialect: sqlDialects.mysql, repeatableRead: true },
] as const;

const params = { shop: "race" };

const append = (orderId: string, total: number) =>
  Shop.use((input) =>
    input.append(
      params,
      DurableState.changes(Shop.at(params), {}, [
        DurableState.upsert("order", { orderId, customerId: "c", total }),
      ]),
    ),
  );

const isCheckpointLock = (query: string) =>
  query.startsWith("SELECT owner_value FROM streamsy_owner_records") &&
  query.endsWith(" FOR UPDATE");

const isOperatorStatement = (query: string) => query.includes("streamsy_view_");

for (const { name, dialect, repeatableRead } of levels)
  describe.skipIf(dialect.url === undefined)(`${name} Views lock order`, () => {
    it.live(
      "a unit locks its checkpoint before it reads operator rows",
      () =>
        Effect.gen(function* () {
          const url = yield* fixtureDatabase(dialect);

          const open = Layer.build(
            hostAt(dialect, group, url, { readLimit: 1, maxConnections: 1 }),
          );

          const writer = yield* open;
          const reader = yield* open;
          const sql = Context.get(reader, SqlClient.SqlClient);
          yield* View.migrate(sql);

          yield* Context.get(writer, Shop).create(params);
          const run = Projection.run(circuit().at(params), { limit: 1 });
          yield* append("first", 1).pipe(Effect.provide(writer));
          yield* run.pipe(Effect.provide(writer));
          yield* append("second", 2).pipe(Effect.provide(writer));
          yield* append("third", 3).pipe(Effect.provide(writer));

          const statements: string[] = [];
          const beforeLock = yield* Deferred.make<void>();
          const proceed = yield* Deferred.make<void>();

          const onReader = (repeatableRead ? withRepeatableRead(sql, run) : run).pipe(
            Effect.provide(reader),
            Effect.provideService(Statement.CurrentTransformer, (statement) => {
              const [query] = statement.compile();
              statements.push(query);

              return isCheckpointLock(query)
                ? Deferred.succeed(beforeLock, undefined).pipe(
                    Effect.andThen(Deferred.await(proceed)),
                    Effect.as(statement),
                  )
                : Effect.succeed(statement);
            }),
          );

          // The plain checkpoint read has taken the snapshot at REPEATABLE READ.
          const raced = yield* onReader.pipe(Effect.flip, Effect.forkScoped);
          yield* Deferred.await(beforeLock);
          expect((yield* run.pipe(Effect.provide(writer))).units).toBe(1);
          yield* Deferred.succeed(proceed, undefined);
          expect(yield* Fiber.join(raced)).toMatchObject({
            reason: "token-conflict",
            retryable: true,
          });
          expect(Array.filter(statements, isOperatorStatement)).toEqual([]);

          statements.length = 0;
          expect((yield* onReader).units).toBe(1);

          const order = Array.filter(
            statements,
            (query) => isCheckpointLock(query) || isOperatorStatement(query),
          );

          expect(order[0]).toSatisfy(isCheckpointLock);
          expect(order[1]).toSatisfy(isOperatorStatement);

          const { items } = yield* Context.get(writer, Results).collect({
            ...params,
            generation: 1,
          });

          expect(
            DurableState.materialize(DurableState.materialized(Results), items).rows.customerTotals,
          ).toEqual(new Map([["c", { customerId: "c", orders: 3, revenue: 6, largest: 3 }]]));
        }),
      15_000,
    );
  });
