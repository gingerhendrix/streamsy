import { test, expect } from "bun:test";
import { Array, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, StreamAddress, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Projection, StateTable, Transaction } from "@streamsy/projection";
import { View } from "../../src/relation.ts";
import { host } from "./support/host.ts";
import { Shop, Results, Table, circuit, views } from "./support/shop.ts";

const params = { shop: "a" };

const order = { orderId: "o1", customerId: "c1", total: 10 };

const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* View.migrate(sql);
  yield* View.migrate(sql);
  yield* StateTable.migrate(sql);
  const input = yield* Shop;
  yield* input.create(params);
  yield* input.append(
    params,
    DurableState.changes(Shop.at(params), {}, [
      DurableState.upsert("order", order),
      DurableState.upsert("customer", { customerId: "c1", name: "One", tier: "gold" }),
    ]),
  );
  yield* Projection.run(circuit().at(params));
});

const update = Effect.gen(function* () {
  yield* (yield* Shop).append(
    params,
    DurableState.changes(Shop.at(params), {}, [
      DurableState.upsert("order", { ...order, total: 20 }),
    ]),
  );
});

const state = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  return {
    integrals:
      yield* sql`SELECT * FROM streamsy_view_integrals ORDER BY namespace, relation, row_key`,
    arrangements:
      yield* sql`SELECT * FROM streamsy_view_arrangements ORDER BY namespace, arrangement, index_key, sort_key, row_key`,
    accumulators:
      yield* sql`SELECT * FROM streamsy_view_accumulators ORDER BY namespace, node, group_key`,
    circuits: yield* sql`SELECT * FROM streamsy_view_circuits ORDER BY namespace`,
  };
});

test("a changed plan faults without changing SQL, output or checkpoint", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const before = yield* state;
      const checkpoint = yield* Projection.read(circuit().at(params));
      const head = yield* (yield* Results).head(params);
      yield* update;
      const fault = yield* Projection.run(circuit(1, true).at(params)).pipe(Effect.flip);
      expect(fault).toBeInstanceOf(View.PlanChanged);
      expect(yield* state).toEqual(before);
      expect(yield* Projection.read(circuit().at(params))).toEqual(checkpoint);
      expect((yield* (yield* Results).head(params)).nextOffset).toBe(head.nextOffset);
      yield* Projection.run(circuit().at(params));
      yield* Projection.run(Table.at(params));
      expect((yield* Table.at(params).get("customerTotals", "c1")).value?.revenue).toBe(20);
    }).pipe(Effect.provide(host)),
  );
});

test("new generations rebuild and remove the old operator namespace", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const before = yield* state;
      const oldNamespace = before.circuits[0]?.namespace;
      yield* Projection.run(circuit(2).at(params));
      const after = yield* state;
      expect(after.circuits).toHaveLength(1);
      expect(after.circuits[0]?.generation).toBe(2);

      for (const rows of [after.integrals, after.arrangements, after.accumulators])
        expect(Array.some(rows, (row) => row.namespace === oldNamespace)).toBe(false);
      yield* Projection.run(Table.at(params));
      expect((yield* Table.at(params).get("customerTotals", "c1")).value?.revenue).toBe(10);
      yield* update;
      const fault = yield* Projection.run(circuit().at(params)).pipe(Effect.flip);
      expect(fault).toBeInstanceOf(View.ViewFault);
    }).pipe(Effect.provide(host)),
  );
});

test("Projection.forget deletes the checkpoint and all namespace state", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      yield* Projection.forget(circuit().at(params));
      const after = yield* state;
      expect(after).toEqual({ integrals: [], arrangements: [], accumulators: [], circuits: [] });
      expect((yield* Projection.read(circuit().at(params))).offsets).toEqual({
        [Shop.key]: ZERO_OFFSET,
      });
      yield* Projection.run(circuit().at(params));
      yield* Projection.run(Table.at(params));
      expect((yield* Table.at(params).get("customerTotals", "c1")).value?.revenue).toBe(10);
    }).pipe(Effect.provide(host)),
  );
});

test("forget rolls back its checkpoint deletion when state cleanup fails", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const before = yield* state;
      const checkpoint = yield* Projection.read(circuit().at(params));
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TRIGGER fail_cleanup BEFORE DELETE ON streamsy_view_circuits BEGIN SELECT RAISE(ABORT, 'cleanup failed'); END`;
      yield* View.forget(circuit().at(params)).pipe(Effect.flip);
      expect(yield* state).toEqual(before);
      expect(yield* Projection.read(circuit().at(params))).toEqual(checkpoint);
    }).pipe(Effect.provide(host)),
  );
});

test("failed output appends roll back operators and input checkpoints", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const before = yield* state;
      const checkpoint = yield* Projection.read(circuit().at(params));
      yield* (yield* Results).append(params, [], { close: true });
      yield* update;
      yield* Projection.run(circuit().at(params)).pipe(Effect.flip);
      expect(yield* state).toEqual(before);
      expect(yield* Projection.read(circuit().at(params))).toEqual(checkpoint);
    }).pipe(Effect.provide(host)),
  );
});

test("bindings with identical row keys remain independent", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      const input = yield* Shop;
      const other = { shop: "b" };
      yield* input.create(other);
      yield* input.append(
        other,
        DurableState.changes(Shop.at(other), {}, [
          DurableState.upsert("order", { ...order, total: 99 }),
        ]),
      );
      yield* Projection.run(circuit().at(other));
      yield* Projection.run(Table.at(params));
      yield* Projection.run(Table.at(other));
      expect((yield* Table.at(params).get("customerTotals", "c1")).value?.revenue).toBe(10);
      expect((yield* Table.at(other).get("customerTotals", "c1")).value?.revenue).toBe(99);
      expect((yield* state).circuits).toHaveLength(2);
    }).pipe(Effect.provide(host)),
  );
});

test("reordering view declarations preserves the plan and SQL identities", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* seed;
      yield* update;

      const reordered = View.circuit({
        id: "shop-views",
        params: { shop: Schema.String },
        output: Results,
        views: {
          topOrder: views.topOrder,
          goldOrder: views.goldOrder,
          customerTotals: views.customerTotals,
        },
      });

      yield* Projection.run(reordered.at(params));
      yield* Projection.run(Table.at(params));
      expect((yield* Table.at(params).get("customerTotals", "c1")).value?.revenue).toBe(20);
    }).pipe(Effect.provide(host)),
  );
});

const NumericRow = Schema.Struct({ id: Schema.Finite, n: Schema.Finite });

class Numeric extends Streamsy.state<Numeric>()("ViewsTest/Numeric", "numeric", {
  params: {},
  collections: { row: { schema: NumericRow, key: "id" } },
}) {}

class NumericResult extends Streamsy.state<NumericResult>()(
  "ViewsTest/NumericResult",
  "numeric/result",
  { params: {}, collections: { row: { schema: NumericRow, key: "id" } } },
) {}

const numericCircuit = View.circuit({
  id: "numeric",
  params: {},
  output: NumericResult,
  views: { row: View.from(Numeric, "row") },
});

const NumericTable = StateTable.define(NumericResult);

test("numeric wire keys, reset and snapshots across units follow State semantics", async () => {
  const local = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(
      BunStorage.layer(Streamsy.group(Numeric, NumericResult), {
        client: { filename: ":memory:" },
      }),
    ),
  );

  await Effect.runPromise(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* View.migrate(sql);
      yield* StateTable.migrate(sql);
      const input = yield* Numeric;
      yield* input.create({});

      const append = (
        entries: Array.NonEmptyReadonlyArray<DurableState.Entry<typeof Numeric.collections>>,
      ) =>
        Effect.gen(function* () {
          yield* input.append({}, DurableState.changes(Numeric.at({}), {}, entries));
          yield* Projection.run(numericCircuit.at({}));
          yield* Projection.run(NumericTable.at({}));

          return Array.map((yield* NumericTable.at({}).list("row")).rows, ({ value }) => value.id);
        });

      expect(
        yield* append([
          DurableState.upsert("row", { id: 1, n: 10 }),
          DurableState.upsert("row", { id: 2, n: 20 }),
        ]),
      ).toEqual([1, 2]);
      expect(
        yield* append([
          StreamAddress.snapshotStart(),
          DurableState.upsert("row", { id: 1, n: 11 }),
        ]),
      ).toEqual([1, 2]);
      expect(
        yield* append([DurableState.upsert("row", { id: 3, n: 30 }), StreamAddress.snapshotEnd()]),
      ).toEqual([1, 3]);
      expect(
        yield* append([StreamAddress.reset(), DurableState.upsert("row", { id: 5, n: 50 })]),
      ).toEqual([5]);
      expect(yield* append([DurableState.remove("row", 5)])).toEqual([]);
    }).pipe(Effect.provide(local)),
  );
});

test("an attached circuit advances through the normal Projection registry", async () => {
  const migrate = Layer.effectDiscard(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* View.migrate(sql);
      yield* StateTable.migrate(sql);
    }),
  );

  const attached = Projection.attach(circuit(), { durable: true }).pipe(
    Layer.provideMerge(Projection.Registry.layer),
    Layer.provideMerge(migrate.pipe(Layer.provideMerge(host))),
  );

  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const input = yield* Shop;
      yield* input.create(params);

      const receipt = yield* input.append(
        params,
        DurableState.changes(Shop.at(params), {}, [DurableState.upsert("order", order)]),
      );

      yield* registry
        .awaitOffset(circuit().at(params), Shop.key, receipt.offset)
        .pipe(Effect.timeout("5 seconds"));
      yield* Projection.run(Table.at(params));
      expect((yield* Table.at(params).get("customerTotals", "c1")).value?.revenue).toBe(10);
    }).pipe(Effect.provide(attached)),
  );
});
