/* oxlint-disable effecttsgo/strict-effect-provide -- The smoke composes its complete local host. */
import { Array, Effect, Equal, Formatter, Layer, Predicate } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection, OwnerTransaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { View, ViewBackend } from "@streamsy/views";
import { Orders, ShopViews, shopCircuit } from "./streams.ts";

const host = Layer.mergeAll(
  CheckpointStore.layer,
  OwnerTransaction.layer,
  ViewBackend.layerSql,
).pipe(
  Layer.provideMerge(
    BunStorage.layer(Streamsy.group(Orders, ShopViews), { client: { filename: ":memory:" } }),
  ),
);

export const smoke = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* View.migrate(sql);

  const params = { shopId: "main" };
  const input = yield* Orders;
  const output = yield* ShopViews;
  yield* input.create(params);

  const append = (
    ...entries: Array.NonEmptyReadonlyArray<DurableState.Entry<typeof Orders.collections>>
  ) => input.append(params, DurableState.changes(Orders.at(params), {}, entries));

  const checkViews = (
    step: string,
    generation: number,
    expected: DurableState.Rows<typeof ShopViews.collections>,
  ) =>
    Effect.gen(function* () {
      const { items } = yield* output.collect({ ...params, generation });
      const { rows } = DurableState.materialize(DurableState.materialized(ShopViews), items);

      if (!Equal.equals(rows, expected)) {
        return yield* Effect.die(
          new Error(
            `${step}: expected ${Formatter.format(expected)}, got ${Formatter.format(rows)}`,
          ),
        );
      }

      return undefined;
    });

  const initial = shopCircuit(1).at(params);
  const a1 = { orderId: "a1", customerId: "ava", total: 30 };
  const a2 = { orderId: "a2", customerId: "ava", total: 20 };
  const b1 = { orderId: "b1", customerId: "ben", total: 10 };
  const benTotal = { customerId: "ben", orders: 1, revenue: 10 };

  yield* append(
    DurableState.upsert("order", a1),
    DurableState.upsert("order", a2),
    DurableState.upsert("order", b1),
  );
  yield* Projection.run(initial);
  yield* checkViews("initial orders", initial.generation, {
    customerTotals: new Map([
      ["ava", { customerId: "ava", orders: 2, revenue: 50 }],
      ["ben", benTotal],
    ]),
    topOrder: new Map([
      ["a1", a1],
      ["b1", b1],
    ]),
  });

  const edited = { ...a2, total: 40 };
  yield* append(DurableState.upsert("order", edited));
  yield* Projection.run(initial);
  yield* checkViews("edited order", initial.generation, {
    customerTotals: new Map([
      ["ava", { customerId: "ava", orders: 2, revenue: 70 }],
      ["ben", benTotal],
    ]),
    topOrder: new Map([
      ["a2", edited],
      ["b1", b1],
    ]),
  });

  const avaTotal = { customerId: "ava", orders: 1, revenue: 30 };

  const afterRemove = {
    customerTotals: new Map([
      ["ava", avaTotal],
      ["ben", benTotal],
    ]),
    topOrder: new Map([
      ["a1", a1],
      ["b1", b1],
    ]),
  };

  yield* append(DurableState.remove("order", a2.orderId));
  yield* Projection.run(initial);
  yield* checkViews("removed order", initial.generation, afterRemove);

  // The new plan excludes orders below 25 and replays into a fresh stream.
  const rebuilt = shopCircuit(2, 25);
  yield* Projection.run(rebuilt.at(params));

  const afterRebuild = {
    customerTotals: new Map([["ava", avaTotal]]),
    topOrder: new Map([["a1", a1]]),
  };

  yield* checkViews("generation 2 rebuild", rebuilt.generation, afterRebuild);
  yield* checkViews("generation 1 after rebuild", initial.generation, afterRemove);

  // Each run above has finished, so generation 1 has no active runner.
  const retirement = yield* Projection.retire(rebuilt, params, { generation: initial.generation });

  const missing = yield* output
    .head({ ...params, generation: initial.generation })
    .pipe(Effect.flip);

  const refused = yield* Projection.run(initial).pipe(Effect.flip);

  if (
    retirement.record !== "retired" ||
    !Predicate.isTagged(missing, "StreamNotFound") ||
    !Predicate.isTagged(refused, "ProjectionFault") ||
    refused.reason !== "retired-generation"
  ) {
    return yield* Effect.die(new Error("Generation 1 was not retired"));
  }

  return yield* checkViews("generation 2 after retirement", rebuilt.generation, afterRebuild);
}).pipe(Effect.provide(host));
