import { describe, expect, it } from "@effect/vitest";
import { CommitBoundary, DurableState, StreamAddress, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { Projection, ProjectionFault } from "@streamsy/projection";
import { Array, Context, Deferred, Effect, Fiber, Layer, Result, Schedule, Schema } from "effect";
import { SqlClient, Statement } from "effect/sql";
import { sqlDialects } from "../../../storage/test/dialects/support.ts";
import { View } from "../../src/index.ts";
import { keyExpression } from "../../src/expression.ts";
import { compilePlan } from "../../src/relation.ts";
import { canonicalJson } from "../../src/runtime/key.ts";
import {
  circuit,
  customerTotals,
  group,
  Order,
  orders,
  Results,
  Shop,
  views,
} from "../circuit/support/shop.ts";
import { fixtureDatabase, hostAt } from "../support/backends.ts";
import { fullRecompute } from "../support/full-recompute.ts";

const mysql = sqlDialects.mysql;

class Routed extends Streamsy.state<Routed>()(
  "ViewsTest/FirstUnit",
  "first-unit/:shop/:customerId/g/:generation",
  {
    params: {
      ...Shop.paramCodecs,
      customerId: Schema.String,
      generation: Schema.FiniteFromString,
    },
    collections: { order: { schema: Order, key: "orderId" } },
  },
) {}

const routedView = View.circuit({
  id: "mysql-first-unit",
  params: Shop.paramCodecs,
  output: Projection.perGeneration(Routed, "generation", { routeBy: ["customerId"] }),
  views: {
    order: View.routed(orders, {
      by: (row) => ({ customerId: row.customerId }),
      row: (row) => row,
    }),
  },
});

const routedGroup = Streamsy.group(Shop, Routed);

const raceParams = { shop: "race" };

const sideParams = { shop: "side" };

const sideCircuit = View.circuit({
  id: "mysql-side-circuit",
  params: Shop.paramCodecs,
  output: Projection.perGeneration(Results, "generation"),
  views,
}).at(sideParams);

const order = (orderId: string, total: number) => ({ orderId, customerId: "c", total });

const totalsPlan = compilePlan("customerTotals", {
  kind: "key",
  input: customerTotals.expression,
  key: keyExpression(Results.collections.customerTotals.key),
});

const checkTotals = (params: typeof raceParams, rows: ReadonlyArray<ReturnType<typeof order>>) =>
  Results.use((output) =>
    Effect.gen(function* () {
      const { items } = yield* output.collect({ ...params, generation: 1 });
      const actual = DurableState.materialize(DurableState.materialized(Results), items);

      const expected = fullRecompute(
        {
          plan: totalsPlan,
          sources: {
            [canonicalJson([Shop.key, "order"])]: Array.map(rows, (row) => ({
              key: row.orderId,
              row,
            })),
          },
        },
        true,
      );

      expect([...actual.rows.customerTotals.values()]).toEqual(
        Array.map(expected.rows, ({ row }) => row),
      );
    }),
  );

const append = (row: ReturnType<typeof order>, params = raceParams) =>
  Shop.use((input) =>
    input.append(
      params,
      DurableState.changes(Shop.at(params), {}, [DurableState.upsert("order", row)]),
    ),
  );

const twoHosts = <const Keys extends readonly Streamsy.Any[]>(
  streams: Streamsy.Group<Keys>,
  readLimit: number,
) =>
  Effect.gen(function* () {
    const url = yield* fixtureDatabase(mysql);
    const left = yield* Layer.build(hostAt(mysql, streams, url, { readLimit, maxConnections: 1 }));
    const right = yield* Layer.build(hostAt(mysql, streams, url, { readLimit, maxConnections: 1 }));
    yield* View.migrate(Context.get(left, SqlClient.SqlClient));

    return [left, right] as const;
  });

const retryProjection = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.retry({
      times: 8,
      schedule: Schedule.spaced("10 millis"),
      while: (error) => Schema.is(ProjectionFault)(error) && error.retryable,
    }),
  );

describe.skipIf(mysql.url === undefined)("MySQL Views concurrency", () => {
  it.live(
    "converges after racing the first unit of one binding from two pools",
    () =>
      Effect.gen(function* () {
        const [left, right] = yield* twoHosts(routedGroup, 200);
        const input = Context.get(left, Shop);
        yield* input.create(raceParams);
        const row = order("a", 1);
        yield* append(row).pipe(Effect.provide(left));
        const both = yield* Deferred.make<void>();
        let arrivals = 0;
        const binding = routedView.at(raceParams);
        const run = Projection.run(binding, { limit: 1 });

        const results = yield* Effect.forEach(
          [left, right],
          (host) =>
            run.pipe(
              Effect.provide(host),
              Effect.provideService(Statement.CurrentTransformer, (statement) => {
                if (!statement.compile()[0].startsWith("INSERT INTO streamsy_view_circuits"))
                  return Effect.succeed(statement);

                arrivals++;

                return (
                  arrivals === 2 ? Deferred.succeed(both, undefined) : Deferred.await(both)
                ).pipe(Effect.as(statement));
              }),
              Effect.result,
            ),
          { concurrency: 2 },
        );

        expect(arrivals).toBe(2);
        expect(
          Array.flatMap(results, (result) =>
            Result.isSuccess(result) ? [result.success.units] : [],
          ),
        ).toEqual([1]);

        for (const [index, result] of results.entries()) {
          if (Result.isSuccess(result)) continue;
          expect(result.failure).toMatchObject({ reason: "storage-failure", retryable: true });
          expect((yield* run.pipe(Effect.provide(index === 0 ? left : right))).units).toBe(0);
        }

        const { items } = yield* Context.get(left, Routed).collect({
          ...raceParams,
          customerId: row.customerId,
          generation: 1,
        });

        expect(
          DurableState.materialize(DurableState.materialized(Routed), items).rows.order,
        ).toEqual(new Map([[row.orderId, row]]));
      }),
    15_000,
  );

  it.live(
    "rejects a raced checkpoint in a unit that opens its own transaction",
    () =>
      Effect.gen(function* () {
        const [writer, reader] = yield* twoHosts(group, 1);
        const input = Context.get(writer, Shop);
        yield* input.create(raceParams);
        const binding = circuit().at(raceParams);
        const run = Projection.run(binding, { limit: 1 });

        yield* append(order("first", 1)).pipe(Effect.provide(writer));
        yield* run.pipe(Effect.provide(writer));
        yield* append(order("second", 2)).pipe(Effect.provide(writer));
        yield* append(order("third", 3)).pipe(Effect.provide(writer));

        const beforeLock = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();

        const waiting = yield* run.pipe(
          Effect.provide(reader),
          Effect.provideService(Statement.CurrentTransformer, (statement) => {
            const [query] = statement.compile();

            const checkpointLock =
              query.startsWith("SELECT owner_value FROM streamsy_owner_records") &&
              query.endsWith(" FOR UPDATE");

            return checkpointLock
              ? Deferred.succeed(beforeLock, undefined).pipe(
                  Effect.andThen(Deferred.await(proceed)),
                  Effect.as(statement),
                )
              : Effect.succeed(statement);
          }),
          Effect.result,
          Effect.forkScoped,
        );

        yield* Deferred.await(beforeLock);
        expect((yield* run.pipe(Effect.provide(writer))).units).toBe(1);
        yield* Deferred.succeed(proceed, undefined);
        expect(yield* Fiber.join(waiting)).toMatchObject({
          _tag: "Failure",
          failure: { reason: "token-conflict", retryable: true },
        });
        expect((yield* run.pipe(Effect.provide(reader))).units).toBe(1);

        const { items } = yield* Context.get(writer, Results).collect({
          ...raceParams,
          generation: 1,
        });

        expect(
          DurableState.materialize(DurableState.materialized(Results), items).rows.customerTotals,
        ).toEqual(new Map([["c", { customerId: "c", orders: 3, revenue: 6, largest: 3 }]]));
      }),
    15_000,
  );

  for (const prior of ["an earlier read", "no earlier read", "another circuit"] as const) {
    it.live(
      `commits oracle results in a host transaction with ${prior}`,
      () =>
        Effect.gen(function* () {
          const [writer, reader] = yield* twoHosts(group, 1);
          const sql = Context.get(reader, SqlClient.SqlClient);
          const input = Context.get(writer, Shop);
          const boundary = Context.get(reader, CommitBoundary);
          const binding = circuit().at(raceParams);
          const run = Projection.run(binding, { limit: 1 });
          const first = order("first", 1);
          const second = order("second", 2);
          const third = order("third", 3);

          yield* sql`SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ`;
          yield* input.create(raceParams);
          yield* append(first).pipe(Effect.provide(writer));
          yield* run.pipe(Effect.provide(writer));
          yield* append(second).pipe(Effect.provide(writer));
          yield* append(third).pipe(Effect.provide(writer));

          if (prior === "another circuit") {
            yield* input.create(sideParams);
            yield* append(first, sideParams).pipe(Effect.provide(writer));
          }

          const aboutToReadCheckpoint = yield* Deferred.make<void>();
          const writerCommitted = yield* Deferred.make<void>();

          const pending = yield* boundary
            .withTransaction(
              Effect.gen(function* () {
                if (prior === "an earlier read")
                  yield* sql`SELECT value FROM streamsy_view_accumulators`;

                if (prior === "another circuit")
                  expect((yield* Projection.run(sideCircuit, { limit: 1 })).units).toBe(1);

                let paused = false;

                return yield* run.pipe(
                  Effect.provideService(Statement.CurrentTransformer, (statement) => {
                    const [query] = statement.compile();

                    if (
                      paused ||
                      !query.startsWith("SELECT owner_value FROM streamsy_owner_records")
                    )
                      return Effect.succeed(statement);

                    // Without a host read, let Projection's first checkpoint read establish the snapshot.
                    if (prior === "no earlier read" && !query.endsWith(" FOR UPDATE"))
                      return Effect.succeed(statement);

                    paused = true;

                    return Deferred.succeed(aboutToReadCheckpoint, undefined).pipe(
                      Effect.andThen(Deferred.await(writerCommitted)),
                      Effect.as(statement),
                    );
                  }),
                );
              }).pipe(Effect.provide(reader)),
            )
            .pipe(Effect.forkScoped);

          yield* Deferred.await(aboutToReadCheckpoint);
          expect((yield* run.pipe(Effect.provide(writer))).units).toBe(1);
          yield* Deferred.succeed(writerCommitted, undefined);
          expect((yield* Fiber.join(pending)).units).toBe(1);
          expect((yield* run.pipe(Effect.provide(writer))).units).toBe(0);

          yield* checkTotals(raceParams, [first, second, third]).pipe(Effect.provide(writer));

          if (prior === "another circuit")
            yield* checkTotals(sideParams, [first]).pipe(Effect.provide(writer));
        }),
      15_000,
    );
  }

  it.live(
    "converges across new and populated snapshots and neighbouring lifecycle operations",
    () =>
      Effect.gen(function* () {
        const [left, right] = yield* twoHosts(routedGroup, 200);
        const input = Context.get(left, Shop);
        const output = Context.get(left, Routed);
        const bindings = Array.makeBy(8, (index) => ({ shop: `snapshot-${index}` }));

        const snapshot = (params: (typeof bindings)[number], row: ReturnType<typeof order>) =>
          input.append(
            params,
            DurableState.changes(Shop.at(params), {}, [
              StreamAddress.snapshotStart(),
              DurableState.upsert("order", row),
              StreamAddress.snapshotEnd(),
            ]),
          );

        const run = (params: (typeof bindings)[number], host: typeof left) =>
          Projection.run(routedView.at(params), { limit: 1 }).pipe(
            Effect.provide(host),
            retryProjection,
          );

        const check = (params: (typeof bindings)[number], row: ReturnType<typeof order>) =>
          Effect.gen(function* () {
            const { items } = yield* output.collect({
              ...params,
              customerId: row.customerId,
              generation: 1,
            });

            expect(
              DurableState.materialize(DurableState.materialized(Routed), items).rows.order,
            ).toEqual(new Map([[row.orderId, row]]));
          });

        for (const params of bindings) yield* input.create(params);

        for (const row of [order("before", 1), order("after", 2)]) {
          for (const params of bindings) yield* snapshot(params, row);

          for (const pair of Array.chunksOf(bindings, 2)) {
            yield* Effect.forEach(
              pair,
              (params, index) => run(params, index === 0 ? left : right),
              { concurrency: 2 },
            );

            for (const params of pair) yield* check(params, row);
          }
        }

        const [retired, forgotten, neighbour] = bindings;
        const latest = order("last", 3);
        yield* snapshot(neighbour!, latest);
        yield* Effect.all(
          [
            Projection.retire(routedView, retired, { generation: 1 }).pipe(
              Effect.provide(left),
              retryProjection,
            ),
            run(neighbour!, right),
          ],
          { concurrency: 2 },
        );
        expect(
          yield* output.head({ ...retired, customerId: "c", generation: 1 }).pipe(Effect.flip),
        ).toMatchObject({ _tag: "StreamNotFound" });
        yield* check(neighbour!, latest);

        const again = order("again", 4);
        yield* snapshot(neighbour!, again);
        yield* Effect.all(
          [
            Projection.forget(routedView.at(forgotten!)).pipe(
              Effect.provide(left),
              retryProjection,
            ),
            run(neighbour!, right),
          ],
          { concurrency: 2 },
        );

        const checkpoint = yield* Projection.read(routedView.at(forgotten!)).pipe(
          Effect.provide(left),
        );

        expect(checkpoint.offsets).toEqual({ [Shop.key]: ZERO_OFFSET });
        yield* check(forgotten!, order("after", 2));
        yield* check(neighbour!, again);
      }),
    30_000,
  );
});
