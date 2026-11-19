import { expect, it } from "@effect/vitest";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Streamsy } from "@streamsy/core";
import {
  D2,
  filter,
  groupBy,
  groupByOperators,
  groupedOrderByWithFractionalIndex,
  join,
  map,
  output,
  type KeyValue,
  type MultiSet,
} from "@tanstack/db-ivm";
import { Array as Arr, Effect, Record as Rec, Schema } from "effect";
import { fullRecompute, makeEngine, migrate, type StateChange } from "@streamsy/views/sql";
import type { JsonObject, RowKey } from "../../src/ir/contracts.ts";
import { View, compilePlan, keyExpression, type Reference } from "../../src/relation.ts";
import { canonicalJson } from "../../src/runtime/key.ts";
import { identity, materialize } from "./support.ts";

const Order = Schema.Struct({
  id: Schema.String,
  customer: Schema.String,
  region: Schema.String,
  amount: Schema.Finite,
  memo: Schema.optionalKey(Schema.String),
});

const Customer = Schema.Struct({
  id: Schema.String,
  label: Schema.String,
  tier: Schema.optionalKey(Schema.String),
});

class Orders extends Streamsy.state<Orders>()("Conformance/Orders", "orders", {
  params: {},
  collections: { row: { schema: Order, key: "id" } },
}) {}

class Customers extends Streamsy.state<Customers>()("Conformance/Customers", "customers", {
  params: {},
  collections: { row: { schema: Customer, key: "id" } },
}) {}

type Order = typeof Order.Type;

type Customer = typeof Customer.Type;

type RecordByKey = KeyValue<RowKey, JsonObject>;

const orderRelation = View.from(Orders, "row");

const customerRelation = View.from(Customers, "row");

const orderSource = JSON.stringify([Orders.key, "row"]);

const customerSource = JSON.stringify([Customers.key, "row"]);

function seeded(seed: number) {
  let state = seed;

  return (maximum: number) => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;

    return (state >>> 0) % maximum;
  };
}

function plansFor(
  minimum: number,
  requireMemo: boolean,
  outer: boolean,
  descending: boolean,
  limit: number,
) {
  const positive = orderRelation.where((row) => row.amount.gt(minimum));
  const eligible = requireMemo ? positive.where((row) => row.memo.isPresent()) : positive;

  const project = eligible.select((row) => ({
    id: row.id,
    region: row.region,
    amount: row.amount,
    memo: row.memo,
  }));

  const spec = {
    as: "account",
    on: (left: Reference<Order>, right: Reference<Customer>) => left.customer.eq(right.id),
  } as const;

  const linked = outer
    ? eligible.leftJoin(customerRelation, spec)
    : eligible.join(customerRelation, spec);

  const totals = eligible
    .groupBy((row) => ({ region: row.region, memo: row.memo.orElse("∅") }))
    .aggregate((row) => ({
      orders: View.count(),
      amount: View.sum(row.amount),
      lowest: View.min(row.amount),
      highest: View.max(row.amount),
    }));

  const top = eligible.top({
    partitionBy: (row) => row.region,
    by: (row) => [descending ? row.amount.desc() : row.amount.asc(), row.id.asc()],
    limit,
  });

  return {
    selected: compilePlan("selected", {
      kind: "key",
      input: project.expression,
      key: keyExpression(["region", "id"]),
    }),
    linked: compilePlan("linked", linked.expression),
    totals: compilePlan("totals", totals.expression),
    top: compilePlan("top", top.expression),
  };
}

function tanstack(
  minimum: number,
  requireMemo: boolean,
  outer: boolean,
  descending: boolean,
  limit: number,
) {
  const graph = new D2();
  const orders = graph.newInput<Order>();
  const customers = graph.newInput<Customer>();
  const state = new Map<string, Map<string, number>>();

  const consume = (view: string) =>
    output((delta: MultiSet<RecordByKey>) => {
      for (const [[key, row], weight] of delta.getInner()) {
        const id = identity(view, key);
        const payload = canonicalJson(row);
        const counts = state.get(id) ?? new Map<string, number>();
        const count = (counts.get(payload) ?? 0) + weight;

        if (count === 0) counts.delete(payload);
        else counts.set(payload, count);

        if (counts.size === 0) state.delete(id);
        else state.set(id, counts);
      }
    });

  const eligible = orders.pipe(
    filter((row) => row.amount > minimum && (!requireMemo || row.memo !== undefined)),
  );

  eligible.pipe(
    map((row): RecordByKey => {
      const { id, region, amount, memo } = row;
      const projected = { id, region, amount };

      return [[region, id], memo === undefined ? projected : { ...projected, memo }];
    }),
    consume("selected"),
  );
  const rightById = customers.pipe(map((row): KeyValue<string, Customer> => [row.id, row]));
  eligible.pipe(
    map((row): KeyValue<string, Order> => [row.customer, row]),
    join<string, Order, Customer, KeyValue<string, Order>>(rightById, outer ? "left" : "inner"),
    map(([, [left, right]]): RecordByKey => {
      if (!left) throw new Error("unexpected right-only row");
      const leftKey = `s${left.id.length}:${left.id}`;

      if (!right) return [["unmatched", leftKey], left];

      return [["match", leftKey, `s${right.id.length}:${right.id}`], { ...left, account: right }];
    }),
    consume("linked"),
  );

  eligible.pipe(
    groupBy((row: Order) => ({ region: row.region, memo: row.memo ?? "∅" }), {
      orders: groupByOperators.count<Order>(),
      amount: groupByOperators.sum((row: Order) => row.amount),
      lowest: groupByOperators.min((row: Order) => row.amount),
      highest: groupByOperators.max((row: Order) => row.amount),
    }),
    map(([, row]): RecordByKey => {
      const { lowest, highest, ...rest } = row;

      if (lowest === undefined || highest === undefined) throw new Error("nonempty group expected");

      return [[row.region, row.memo], { ...rest, lowest, highest }];
    }),
    consume("totals"),
  );

  eligible.pipe(
    map((row): KeyValue<string, Order> => [row.id, row]),
    groupedOrderByWithFractionalIndex<KeyValue<string, Order>, Order>((row) => row, {
      groupKeyFn: (_, row) => row.region,
      comparator: (a, b) =>
        (descending ? b.amount - a.amount : a.amount - b.amount) ||
        (a.id < b.id ? -1 : a.id > b.id ? 1 : 0),
      limit,
    }),
    map(([key, [row]]): RecordByKey => [key, row]),
    consume("top"),
  );

  graph.finalize();

  return {
    orders,
    customers,
    rows() {
      graph.run();
      const result = new Map<string, string>();

      for (const [id, counts] of state) {
        expect([...counts.values()], id).toEqual([1]);
        const [payload] = counts.keys();

        if (payload !== undefined) result.set(id, payload);
      }

      return result;
    },
  };
}

it.live(
  "matches both oracles for seeded View.from plans and multi-change units",
  () =>
    Effect.gen(function* () {
      yield* migrate();

      for (let seed = 1; seed <= 48; seed++) {
        const random = seeded(seed);
        const minimum = random(9) * 0.25 - 1;
        const requireMemo = random(2) === 0;
        const outer = random(2) === 0;
        const descending = random(2) === 0;
        const limit = 1 + random(4);
        const plans = plansFor(minimum, requireMemo, outer, descending, limit);
        const engine = yield* makeEngine(plans);
        const ivm = tanstack(minimum, requireMemo, outer, descending, limit);
        const orders = new Map<string, Order>();
        const customers = new Map<string, Customer>();
        const actual = new Map<string, string>();

        for (let unit = 0; unit < 30; unit++) {
          const input: StateChange[] = [];
          const orderChanges: [Order, number][] = [];
          const customerChanges: [Customer, number][] = [];

          for (let n = 1 + random(6); n > 0; n--) {
            if (random(4) > 0) {
              const key = `o${random(9)}`;
              const previous = orders.get(key);

              if (previous) orderChanges.push([previous, -1]);

              if (random(4) === 0) {
                orders.delete(key);
                input.push({ sourceId: orderSource, key, kind: "delete" });
              } else {
                const row = {
                  id: key,
                  customer: `c${random(4)}`,
                  region: ["α", "中", "🌿"][random(3)]!,
                  amount: (random(21) - 10) / 8,
                };

                const next = random(3) === 0 ? row : { ...row, memo: `m/${random(3)}` };
                orders.set(key, next);
                orderChanges.push([next, 1]);
                input.push({ sourceId: orderSource, key, kind: "upsert", value: next });
              }
            } else {
              const key = `c${random(4)}`;
              const previous = customers.get(key);

              if (previous) customerChanges.push([previous, -1]);

              if (random(4) === 0) {
                customers.delete(key);
                input.push({ sourceId: customerSource, key, kind: "delete" });
              } else {
                const row = { id: key, label: `顧客 ${random(3)}` };
                const next = random(2) === 0 ? row : { ...row, tier: `t/${random(2)}` };
                customers.set(key, next);
                customerChanges.push([next, 1]);
                input.push({ sourceId: customerSource, key, kind: "upsert", value: next });
              }
            }
          }

          const before = new Map(actual);
          const changes = yield* engine.process(`from-${seed}`, input);
          materialize(actual, changes);
          const expected = new Map<string, string>();

          const sources = {
            [orderSource]: Arr.map([...orders], ([key, row]) => ({ key, row })),
            [customerSource]: Arr.map([...customers], ([key, row]) => ({ key, row })),
          };

          for (const [name, plan] of Rec.toEntries(plans))
            for (const { key, row } of fullRecompute({ plan, sources }).rows)
              expected.set(identity(name, key), canonicalJson(row));
          ivm.orders.sendData(orderChanges);
          ivm.customers.sendData(customerChanges);
          expect(actual, `full recompute seed=${seed} unit=${unit}`).toEqual(expected);
          expect(actual, `TanStack seed=${seed} unit=${unit}`).toEqual(ivm.rows());

          const changed = Arr.filter(
            [...new Set([...before.keys(), ...expected.keys()])],
            (key) => before.get(key) !== expected.get(key),
          );

          const emitted = Arr.map(changes, (change) => identity(change.view, change.key));
          expect(emitted).toHaveLength(changed.length);
          expect(new Set(emitted)).toEqual(new Set(changed));
        }
      }
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  120_000,
);
