import { expect, it } from "@effect/vitest";
import { Array, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, StreamAddress, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Projection, StateTable, Transaction } from "@streamsy/projection";
import { View } from "../../src/index.ts";
import { storeFor } from "../../src/sql/store.ts";
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
    owners:
      yield* sql`SELECT * FROM streamsy_view_key_owners ORDER BY namespace, relation, output_key`,
    circuits: yield* sql`SELECT * FROM streamsy_view_circuits ORDER BY namespace`,
  };
});

it.live("a changed plan faults without changing SQL, output or checkpoint", () => {
  return Effect.gen(function* () {
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
  }).pipe(Effect.provide(host));
});

it.live("new generations rebuild and remove the old operator namespace", () => {
  return Effect.gen(function* () {
    yield* seed;
    const before = yield* state;
    const oldNamespace = before.circuits[0]?.namespace;
    yield* Projection.run(circuit(2).at(params));
    const after = yield* state;
    expect(after.circuits).toHaveLength(1);
    expect(after.circuits[0]?.generation).toBe(2);

    expect(before.owners.length).toBeGreaterThan(0);

    for (const rows of [after.integrals, after.arrangements, after.accumulators, after.owners])
      expect(Array.some(rows, (row) => row.namespace === oldNamespace)).toBe(false);
    yield* Projection.run(Table.at(params));
    expect((yield* Table.at(params).get("customerTotals", "c1")).value?.revenue).toBe(10);
    yield* update;
    const fault = yield* Projection.run(circuit().at(params)).pipe(Effect.flip);
    expect(fault).toBeInstanceOf(View.ViewFault);
  }).pipe(Effect.provide(host));
});

it.live("Projection.forget deletes the checkpoint and all namespace state", () => {
  return Effect.gen(function* () {
    yield* seed;
    yield* Projection.forget(circuit().at(params));
    const after = yield* state;
    expect(after).toEqual({
      integrals: [],
      arrangements: [],
      accumulators: [],
      owners: [],
      circuits: [],
    });
    expect((yield* Projection.read(circuit().at(params))).offsets).toEqual({
      [Shop.key]: ZERO_OFFSET,
    });
    yield* Projection.run(circuit().at(params));
    yield* Projection.run(Table.at(params));
    expect((yield* Table.at(params).get("customerTotals", "c1")).value?.revenue).toBe(10);
  }).pipe(Effect.provide(host));
});

it.live("forget rolls back its checkpoint deletion when state cleanup fails", () => {
  return Effect.gen(function* () {
    yield* seed;
    const before = yield* state;
    const checkpoint = yield* Projection.read(circuit().at(params));
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TRIGGER fail_cleanup BEFORE DELETE ON streamsy_view_circuits BEGIN SELECT RAISE(ABORT, 'cleanup failed'); END`;
    yield* View.forget(circuit().at(params)).pipe(Effect.flip);
    expect(yield* state).toEqual(before);
    expect(yield* Projection.read(circuit().at(params))).toEqual(checkpoint);
  }).pipe(Effect.provide(host));
});

it.live("failed output appends roll back operators and input checkpoints", () => {
  return Effect.gen(function* () {
    yield* seed;
    const before = yield* state;
    const checkpoint = yield* Projection.read(circuit().at(params));
    yield* (yield* Results).append(params, [], { close: true });
    yield* update;
    yield* Projection.run(circuit().at(params)).pipe(Effect.flip);
    expect(yield* state).toEqual(before);
    expect(yield* Projection.read(circuit().at(params))).toEqual(checkpoint);
  }).pipe(Effect.provide(host));
});

it.live("bindings with identical row keys remain independent", () => {
  return Effect.gen(function* () {
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
  }).pipe(Effect.provide(host));
});

it.live("reordering view declarations preserves the plan and SQL identities", () => {
  return Effect.gen(function* () {
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
  }).pipe(Effect.provide(host));
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

it.live("numeric wire keys, reset and snapshots across units follow State semantics", () => {
  const local = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(
      BunStorage.layer(Streamsy.group(Numeric, NumericResult), {
        client: { filename: ":memory:" },
      }),
    ),
  );

  return Effect.gen(function* () {
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
      yield* append([StreamAddress.snapshotStart(), DurableState.upsert("row", { id: 1, n: 11 })]),
    ).toEqual([1, 2]);
    expect(
      yield* append([DurableState.upsert("row", { id: 3, n: 30 }), StreamAddress.snapshotEnd()]),
    ).toEqual([1, 3]);
    expect(
      yield* append([StreamAddress.reset(), DurableState.upsert("row", { id: 5, n: 50 })]),
    ).toEqual([5]);
    expect(yield* append([DurableState.remove("row", 5)])).toEqual([]);
  }).pipe(Effect.provide(local));
});

it.live("filters a source integral across snapshots and resumes pre-R4 rows", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* View.migrate(sql);
    yield* StateTable.migrate(sql);
    const input = yield* Numeric;
    yield* input.create({});
    const binding = View.circuit({
      id: "filtered-numeric",
      params: {},
      output: NumericResult,
      views: { row: View.from(Numeric, "row").where((row) => row.n.gt(10)) },
    }).at({});

    const append = (
      entries: Array.NonEmptyReadonlyArray<DurableState.Entry<typeof Numeric.collections>>,
    ) =>
      Effect.gen(function* () {
        yield* input.append({}, DurableState.changes(Numeric.at({}), {}, entries));
        yield* Projection.run(binding);
        yield* Projection.run(NumericTable.at({}));

        return Array.map((yield* NumericTable.at({}).list("row")).rows, ({ value }) => value.id);
      });
    const upsert = (id: number, n: number) => DurableState.upsert("row", { id, n });

    expect(yield* append([upsert(1, 1), upsert(2, 20), upsert(3, 30)])).toEqual([2, 3]);
    const metadata = (yield* state).circuits[0]!;
    const store = storeFor(sql, metadata.namespace);
    const integralKeys = store.rows("r0").pipe(Effect.map(Array.map((row) => row.key)));
    expect(yield* integralKeys).toEqual(["2", "3"]);

    // R3 stored every source row with the same relation ID and plan hash.
    yield* store.put("r0", [{ key: "1", row: { id: 1, n: 1 } }]);
    expect(yield* append([upsert(1, 11), upsert(2, 2), upsert(3, 30)])).toEqual([1, 3]);
    expect(yield* integralKeys).toEqual(["1", "3"]);
    yield* store.put("r0", [{ key: "2", row: { id: 2, n: 2 } }]);
    expect(yield* append([StreamAddress.snapshotStart(), upsert(3, 31)])).toEqual([1, 3]);
    expect(yield* append([upsert(4, 4), StreamAddress.snapshotEnd()])).toEqual([3]);
    expect(yield* integralKeys).toEqual(["3"]);
    expect(yield* append([StreamAddress.reset(), upsert(5, 50), upsert(6, 6)])).toEqual([5]);
    expect(yield* append([DurableState.remove("row", 5)])).toEqual([]);
    expect(yield* integralKeys).toEqual([]);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
        Layer.provideMerge(
          BunStorage.layer(Streamsy.group(Numeric, NumericResult), {
            client: { filename: ":memory:" },
          }),
        ),
      ),
    ),
  ),
);

it.live("an attached circuit advances through the normal Projection registry", () => {
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

  return Effect.gen(function* () {
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
  }).pipe(Effect.provide(attached));
});

const WireKey = Schema.Union([Schema.String, Schema.Finite]);

class WireInput extends Streamsy.state<WireInput>()("ViewsTest/WireInput", "wire/input", {
  params: {},
  collections: {
    row: { schema: Schema.Struct({ id: Schema.String, target: WireKey }), key: "id" },
  },
}) {}

class WireOutput extends Streamsy.state<WireOutput>()("ViewsTest/WireOutput", "wire/output", {
  params: {},
  collections: {
    row: { schema: Schema.Struct({ id: Schema.String, target: WireKey }), key: "target" },
  },
}) {}

it.live(
  "rejects distinct IR keys with the same State wire key and preserves the checkpoint",
  () => {
    const declaration = View.circuit({
      id: "wire-key",
      params: {},
      output: WireOutput,
      views: { row: View.from(WireInput, "row") },
    }).at({});

    const local = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
      Layer.provideMerge(
        BunStorage.layer(Streamsy.group(WireInput, WireOutput), {
          client: { filename: ":memory:" },
        }),
      ),
    );

    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* View.migrate(sql);
      const input = yield* WireInput;
      yield* input.create({});
      yield* input.append(
        {},
        DurableState.changes(WireInput.at({}), {}, [
          DurableState.upsert("row", { id: "one", target: 1 }),
        ]),
      );
      yield* Projection.run(declaration);
      const before = yield* Projection.read(declaration);
      const stream = yield* WireOutput;
      const head = yield* stream.head({});
      yield* input.append(
        {},
        DurableState.changes(WireInput.at({}), {}, [
          DurableState.upsert("row", { id: "two", target: "1" }),
        ]),
      );
      const fault = yield* Projection.run(declaration).pipe(Effect.flip);
      expect(fault).toMatchObject({
        _tag: "ViewDataFault",
        view: "row",
        node: "row/key/0",
        key: "1",
        reason: "duplicate output key",
      });
      expect(yield* Projection.read(declaration)).toEqual(before);
      expect((yield* stream.head({})).nextOffset).toBe(head.nextOffset);
    }).pipe(Effect.provide(local));
  },
);
