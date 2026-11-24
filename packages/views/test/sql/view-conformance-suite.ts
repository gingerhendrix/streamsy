/* oxlint-disable effecttsgo/strict-effect-provide -- Each test provisions its own database. */
/* oxlint-disable anti-slop-effect/no-service-constructor-imports -- The suite constructs its isolated engine or test scheduler. */
import { expect, it } from "@effect/vitest";
import { backendFor } from "../support/backends.ts";
import { CommitBoundary, DurableState, StreamAddress, Streamsy } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
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
import { Array as Arr, Effect, Option, Order as Ord, Record as Rec, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { makeEngine, type StateChange } from "../../src/sql/engine.ts";
import { normalize } from "../../src/sql/normalize.ts";
import { dataFault } from "../../src/sql/data-fault.ts";
import { fullRecompute } from "../support/full-recompute.ts";
import type { JsonObject, RowKey } from "../../src/ir/contracts.ts";
import { View } from "../../src/index.ts";
import { collectPlanIssues } from "../../src/check.ts";
import { compilePlan } from "../../src/relation.ts";
import { keyExpression, type Reference } from "../../src/expression.ts";
import { canonicalJson } from "../../src/runtime/key.ts";
import { identity, materialize } from "./support.ts";
import { circuitHost, migrateViews, type Fixture } from "../support/memory.ts";
import { currentBackend } from "../../src/view-backend.ts";

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

class Orders extends Streamsy.state<Orders>()("Conformance/Orders", "orders/:seed", {
  params: { seed: Schema.FiniteFromString },
  collections: { row: { schema: Order, key: "id" } },
}) {}

class Customers extends Streamsy.state<Customers>()("Conformance/Customers", "customers", {
  params: {},
  collections: { row: { schema: Customer, key: "id" } },
}) {}

class TopOrders extends Streamsy.state<TopOrders>()(
  "Conformance/TopOrders",
  "top-orders/:seed/g/:generation",
  {
    params: { ...Orders.paramCodecs, generation: Schema.FiniteFromString },
    collections: Orders.collections,
  },
) {}

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

function eligibleOrders(minimum: number, requireMemo: boolean) {
  const positive = orderRelation.where((row) => row.amount.gt(minimum));

  return requireMemo ? positive.where((row) => row.memo.isPresent()) : positive;
}

function topByRegion(eligible: typeof orderRelation, descending: boolean, limit: number) {
  return eligible.top({
    partitionBy: (row) => row.region,
    by: (row) => [descending ? row.amount.desc() : row.amount.asc(), row.id.asc()],
    limit,
  });
}

const linkOrder = View.fn(
  "link-order",
  1,
  ({ id: _id, account, ...order }: Order & { readonly account?: Customer }) =>
    account === undefined ? order : { ...order, accountLabel: account.label },
);

function plansFor(
  minimum: number,
  requireMemo: boolean,
  outer: boolean,
  descending: boolean,
  limit: number,
) {
  const eligible = eligibleOrders(minimum, requireMemo);

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
      amounts: View.list(row.amount),
    }));

  const top = topByRegion(eligible, descending, limit);

  return {
    selected: compilePlan("selected", {
      kind: "key",
      input: project.expression,
      key: keyExpression(["region", "id"]),
    }),
    linked: compilePlan("linked", linked.selectRow(linkOrder).expression),
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
      const { id: _id, ...order } = left;

      if (!right) return [["unmatched", leftKey], order];

      return [
        ["match", leftKey, `s${right.id.length}:${right.id}`],
        { ...order, accountLabel: right.label },
      ];
    }),
    consume("linked"),
  );

  eligible.pipe(
    groupBy((row: Order) => ({ region: row.region, memo: row.memo ?? "∅" }), {
      orders: groupByOperators.count<Order>(),
      amount: groupByOperators.sum((row: Order) => row.amount),
      lowest: groupByOperators.min((row: Order) => row.amount),
      highest: groupByOperators.max((row: Order) => row.amount),
      amounts: {
        preMap: (row: Order) => [row.amount],
        reduce: (values: [number[], number][]) =>
          Arr.sort(
            Arr.flatMap(values, ([value, count]) => Array.from({ length: count }, () => value[0]!)),
            Ord.Number,
          ),
        postMap: (values: number[]) => values,
      },
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

const GraphNode = Schema.Struct({
  id: Schema.String,
  parent: Schema.optionalKey(Schema.String),
  part: Schema.Finite,
  other: Schema.Finite,
});

const Member = Schema.Struct({ id: Schema.String, group: Schema.String, ref: Schema.String });

class Graph extends Streamsy.state<Graph>()("Conformance/Graph", "graph", {
  params: {},
  collections: { node: { schema: GraphNode, key: "id" }, member: { schema: Member, key: "id" } },
}) {}

type GraphEntry = DurableState.Entry<typeof Graph.collections>;

function closurePlans(maxDepth: number) {
  const nodes = View.from(Graph, "node");
  const members = View.from(Graph, "member");
  const lineage = nodes.closure({ parent: (row) => row.parent, part: (row) => row.part, maxDepth });

  const direct = members.select((row) => ({
    group: row.group,
    ref: row.ref,
    ancestor: row.ref,
    depth: View.literal(0),
  }));

  const inherited = members
    .join(lineage, { as: "lineage", on: (member, row) => member.ref.eq(row.ancestor) })
    .select((row) => ({
      group: row.group,
      ref: row.lineage.node,
      ancestor: row.lineage.ancestor,
      depth: row.lineage.depth,
      path: View.field(row.lineage, "path"),
    }));

  return {
    lineage: compilePlan("lineage", lineage.expression),
    nearest: compilePlan(
      "nearest",
      direct.concat(inherited).top({
        partitionBy: (row) => [row.group, row.ref],
        by: (row) => [row.depth.asc(), View.key.asc()],
        limit: 1,
      }).expression,
    ),
  };
}

function editNodes(
  state: DurableState.Rows<typeof Graph.collections>,
  size: number,
  random: ReturnType<typeof seeded>,
): GraphEntry[] {
  const nodes = new Map(state.node);

  const edits: GraphEntry[] = Arr.makeBy(1 + random(4), () => {
    const id = String(random(size));
    const previous = nodes.get(id);
    const node = previous ?? { id, part: random(100), other: 0 };
    const operation = random(8);

    if (operation === 0) {
      nodes.delete(id);

      return DurableState.remove("node", id);
    }

    const { parent: _parent, ...root } = node;

    const alternatives = [
      { ...node, parent: String(random(size)) },
      { ...node, parent: "external" },
      { ...node, parent: id },
      root,
      { ...node, part: node.part + 1 },
      { ...node, other: node.other + 1 },
      { ...node, parent: String(random(size)) },
    ];

    const next = alternatives[operation - 1]!;
    nodes.set(id, next);

    return DurableState.upsert("node", next);
  });

  if (random(3) === 0) {
    const group = random(2) === 0 ? "a" : "b";
    const ref = String(random(size));
    const id = JSON.stringify([group, ref]);
    edits.push(
      random(2) === 0
        ? DurableState.remove("member", id)
        : DurableState.upsert("member", { id, group, ref }),
    );
  }

  return edits;
}

const engineBackend = Effect.flatMap(Effect.serviceOption(SqlClient.SqlClient), (sql) =>
  Option.isSome(sql) ? backendFor(sql.value) : currentBackend,
);

export function registerViewConformance(fixture: Fixture) {
  const host = circuitHost(fixture, Streamsy.group(Orders, TopOrders));
  const closureHost = circuitHost(fixture, Streamsy.group(Graph));
  const withOwner = "memory" in fixture;
  it.live(
    "matches both oracles for seeded View.from plans and replays generation changes",
    () =>
      Effect.gen(function* () {
        yield* migrateViews;
        const backend = yield* engineBackend;
        const boundary = yield* CommitBoundary;
        const inputStream = yield* Orders;
        const outputStream = yield* TopOrders;

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
          const params = { seed };

          const appendOrders = (
            entries: Arr.NonEmptyReadonlyArray<DurableState.Entry<typeof Orders.collections>>,
          ) => inputStream.append(params, DurableState.changes(Orders.at(params), {}, entries));

          const lifecycleRandom = seeded(seed + 48);
          let generation = 1;
          let cutoff = minimum;

          const build = () => {
            const top = topByRegion(eligibleOrders(cutoff, requireMemo), descending, limit);

            return {
              plan: compilePlan("top", {
                kind: "key",
                input: top.expression,
                key: keyExpression(TopOrders.collections.row.key),
              }),
              binding: View.circuit({
                id: "seeded-top",
                params: Orders.paramCodecs,
                generation,
                output: Projection.perGeneration(TopOrders, "generation"),
                views: { row: top },
              }).at(params),
            };
          };

          let serving = build();
          yield* inputStream.create(params);
          yield* appendOrders([StreamAddress.reset()]);

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
            const process = engine.process(backend, `from-${seed}`, input);
            const changes = yield* withOwner ? boundary.withTransaction(process) : process;
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

            if (Arr.isReadonlyArrayNonEmpty(orderChanges))
              yield* appendOrders(
                Arr.map(orderChanges, ([row, weight]) =>
                  weight === 1
                    ? DurableState.upsert("row", row)
                    : DurableState.remove("row", row.id),
                ),
              );

            if (lifecycleRandom(8) === 0) {
              generation++;
              cutoff += lifecycleRandom(2) === 0 ? -0.125 : 0.125;
              serving = build();
            }

            yield* Projection.run(serving.binding);

            const materialized = DurableState.materialize(
              DurableState.materialized(TopOrders),
              (yield* outputStream.collect({ ...params, generation })).items,
            );

            const recomputed = fullRecompute({ plan: serving.plan, sources });
            expect(
              materialized.rows.row,
              `circuit seed=${seed} unit=${unit} generation=${generation}`,
            ).toEqual(new Map(Arr.map(recomputed.rows, ({ key, row }) => [key, row])));
          }
        }
      }).pipe(Effect.provide(host)),
    "server" in fixture && fixture.server !== undefined ? 300_000 : 120_000,
  );

  it.live(
    "matches the full recompute for incremental closure and nearest membership in seeded forests",
    () =>
      Effect.gen(function* () {
        yield* migrateViews;
        const backend = yield* engineBackend;
        const boundary = yield* CommitBoundary;
        const nodeSource = canonicalJson([Graph.key, "node"]);
        const memberSource = canonicalJson([Graph.key, "member"]);

        // Closure and concat use the full-recompute oracle. The TanStack plan above uses a different operator set.
        for (let seed = 1; seed <= 50; seed++) {
          const random = seeded(seed);
          const chain = seed > 48;
          const size = chain ? 66 + random(5) : 5 + random(36);
          const maxDepth = chain ? 64 : [1, 2, 3, 5, 64][seed % 5]!;
          const namespace = `closure-${seed}`;
          const plans = closurePlans(maxDepth);
          const engine = yield* makeEngine(plans);
          const store = backend.store(namespace);

          const sourceRelation = (id: string) => {
            const source = Arr.findFirst(
              engine.program.nodes,
              (node) => node.kind === "source" && node.sourceId === id,
            );

            return Option.getOrThrowWith(source, () => new Error(`Missing source ${id}`)).id;
          };

          const nodeRelation = sourceRelation(nodeSource);
          const memberRelation = sourceRelation(memberSource);
          const fault = dataFault({ view: "closure", node: "normalize" });
          const actual = new Map<string, string>();
          let state = DurableState.materialized(Graph);
          let unit = 0;

          const applyBatch = (entries: readonly GraphEntry[]) =>
            Effect.gen(function* () {
              const events = DurableState.changes(Graph.at({}), {}, entries);
              const snapshotOpen = state.snapshot !== undefined;

              const nodeChanges = yield* normalize(
                backend,
                namespace,
                nodeSource,
                nodeRelation,
                "node",
                fault,
                events,
                snapshotOpen,
              );

              const memberChanges = yield* normalize(
                backend,
                namespace,
                memberSource,
                memberRelation,
                "member",
                fault,
                events,
                snapshotOpen,
              );

              materialize(
                actual,
                yield* engine.process(backend, namespace, [...nodeChanges, ...memberChanges]),
              );
              state = DurableState.materialize(state, events);

              const sources = {
                [nodeSource]: Arr.map([...state.rows.node], ([key, row]) => ({ key, row })),
                [memberSource]: Arr.map([...state.rows.member], ([key, row]) => ({ key, row })),
              };

              const expected = new Map<string, string>();
              const label = `seed=${seed}, unit=${unit++}`;

              for (const [name, plan] of Rec.toEntries(plans))
                for (const { key, row } of fullRecompute({ plan, sources }).rows)
                  expected.set(identity(name, key), canonicalJson(row));

              expect(actual, label).toEqual(expected);
              const integral = yield* store.rows(engine.program.outputs.lineage!);
              expect(
                new Map(
                  Arr.map(integral, ({ key, row }) => [
                    identity("lineage", key),
                    canonicalJson(row),
                  ]),
                ),
                label,
              ).toEqual(new Map(Arr.filter([...expected], ([id]) => id.startsWith("lineage/"))));
            });

          const apply = (entries: readonly GraphEntry[]) => {
            const batch = applyBatch(entries);

            return withOwner ? boundary.withTransaction(batch) : batch;
          };

          yield* apply([
            ...Arr.makeBy(size, (index) => {
              const node = { id: String(index), part: index, other: 0 };

              if (index === 0 || (!chain && random(4) === 0))
                return DurableState.upsert("node", node);

              return DurableState.upsert("node", {
                ...node,
                parent: String(chain ? index - 1 : random(index)),
              });
            }),
            DurableState.upsert("member", { id: '["a","0"]', group: "a", ref: "0" }),
            DurableState.upsert("member", { id: '["a","external"]', group: "a", ref: "external" }),
            DurableState.upsert("member", { id: '["b","0"]', group: "b", ref: "0" }),
          ]);

          for (let step = 0; step < 20; step++) {
            const edits = editNodes(state.rows, size, random);

            if (step === 5 && seed % 2 === 0) edits.unshift(StreamAddress.snapshotStart());

            if (step === 7 && state.snapshot !== undefined) edits.push(StreamAddress.snapshotEnd());

            if (step === 12 && seed % 3 === 0) edits.unshift(StreamAddress.reset());
            yield* apply(edits);
          }
        }
      }).pipe(Effect.provide(closureHost)),
    "server" in fixture && fixture.server !== undefined ? 300_000 : 120_000,
  );

  it("compiles a join shared by both concat branches once", () => {
    const members = View.from(Graph, "member");

    const joined = members.leftJoin(View.from(Graph, "node"), {
      as: "agent",
      on: (member, agent) => member.ref.eq(agent.id),
    });

    const selected = joined.select((row) => ({ ref: row.ref }));
    const plan = compilePlan("shared", joined.concat(selected).expression);

    expect(Arr.filter(plan.nodes, (node) => node.kind === "left-join")).toHaveLength(1);
    expect(collectPlanIssues(plan)).toEqual([]);
  });
}
