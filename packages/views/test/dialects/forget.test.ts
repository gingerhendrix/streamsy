import { describe, expect, it } from "@effect/vitest";
import { DurableState, Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection, type ProjectionFault } from "@streamsy/projection";
import {
  Array,
  Context,
  Effect,
  Fiber,
  Layer,
  Option,
  Order as EffectOrder,
  Record,
  Schema,
} from "effect";
import { SqlClient } from "effect/sql";
import {
  holdStatements,
  sqlDialects,
  type StatementMatch,
  withRepeatableRead,
} from "../../../storage/test/dialects/support.ts";
import { View } from "../../src/index.ts";
import { keyExpression } from "../../src/expression.ts";
import { compilePlan } from "../../src/relation.ts";
import { canonicalJson } from "../../src/runtime/key.ts";
import {
  circuit,
  customerTotals,
  Order,
  Results,
  Shop,
  topOrders,
  views,
} from "../circuit/support/shop.ts";
import { fixtureDatabase, hostAt } from "../support/backends.ts";
import { fullRecompute } from "../support/full-recompute.ts";

type OrderRow = typeof Order.Type;

class Routed extends Streamsy.state<Routed>()(
  "ViewsTest/ForgetRouted",
  "forget-routed/:shop/:customerId/g/:generation",
  {
    params: { ...Shop.paramCodecs, customerId: Schema.String, generation: Schema.FiniteFromString },
    collections: {
      topOrder: { schema: Order, key: "orderId" },
      customerTotals: Results.collections.customerTotals,
    },
  },
) {}

const routedCircuit = View.circuit({
  id: "forget-routed",
  params: Shop.paramCodecs,
  output: Projection.perGeneration(Routed, "generation", { routeBy: ["customerId"] }),
  views: {
    topOrder: View.routed(topOrders, {
      by: (row) => ({ customerId: row.customerId }),
      row: (row) => row,
    }),
    customerTotals: View.routed(customerTotals, {
      by: (row) => ({ customerId: row.customerId }),
      row: (row) => row,
    }),
  },
});

const group = Streamsy.group(Shop, Results, Routed);

const order = (orderId: string, total: number, customerId = "c") => ({
  orderId,
  total,
  customerId,
});

const child = (orderId: string, total: number, parentOrderId: string) => ({
  ...order(orderId, total),
  parentOrderId,
});

const customerRows = [
  { customerId: "c", name: "First Customer", tier: "gold" },
  { customerId: "d", name: "Second Customer", tier: "gold" },
] as const;

const first = [order("a", 1), child("b", 2, "a"), order("other", 10, "d")] as const;

const second = [
  order("a", 5),
  child("b", 2, "a"),
  child("e", 7, "b"),
  order("moved", 3, "d"),
] as const;

const seed = (shop: string) =>
  Shop.use((input) =>
    input
      .create({ shop })
      .pipe(
        Effect.andThen(
          input.append(
            { shop },
            DurableState.changes(Shop.at({ shop }), {}, [
              DurableState.upsert("customer", customerRows[0]),
              DurableState.upsert("customer", customerRows[1]),
              DurableState.upsert("order", first[0]),
              DurableState.upsert("order", first[1]),
              DurableState.upsert("order", first[2]),
            ]),
          ),
        ),
      ),
  );

/** Update, insert and delete, so lost operator rows change the output. */
const change = (shop: string) =>
  Shop.use((input) =>
    input.append(
      { shop },
      DurableState.changes(Shop.at({ shop }), {}, [
        DurableState.upsert("order", order("a", 5)),
        DurableState.upsert("order", child("e", 7, "b")),
        DurableState.remove("order", "other"),
        DurableState.upsert("order", order("moved", 3, "d")),
      ]),
    ),
  );

const sorted = (rows: Iterable<unknown>) =>
  Array.sort(Array.map(Array.fromIterable(rows), canonicalJson), EffectOrder.String);

const recompute = (
  name: string,
  relation: (typeof views)[keyof typeof views],
  key: string,
  rows: ReadonlyArray<OrderRow>,
) =>
  sorted(
    Array.map(
      fullRecompute(
        {
          plan: compilePlan(name, {
            kind: "key",
            input: relation.expression,
            key: keyExpression(key),
          }),
          sources: {
            [canonicalJson([Shop.key, "order"])]: Array.map(rows, (row) => ({
              key: row.orderId,
              row,
            })),
            [canonicalJson([Shop.key, "customer"])]: Array.map(customerRows, (row) => ({
              key: row.customerId,
              row,
            })),
          },
        },
        true,
      ).rows,
      ({ row }) => row,
    ),
  );

const checkGenerated = (shop: string, rows: ReadonlyArray<OrderRow>) =>
  Effect.gen(function* () {
    const { items } = yield* (yield* Results).collect({ shop, generation: 1 });
    const actual = DurableState.materialize(DurableState.materialized(Results), items).rows;

    for (const [name, relation] of Record.toEntries(views))
      expect(sorted(actual[name].values()), name).toEqual(
        recompute(name, relation, Results.collections[name].key, rows),
      );
  });

const checkRouted = (shop: string, rows: ReadonlyArray<OrderRow>) =>
  Effect.gen(function* () {
    const output = yield* Routed;

    const actual = yield* Effect.forEach(customerRows, ({ customerId }) =>
      output
        .collect({ shop, customerId, generation: 1 })
        .pipe(
          Effect.map(
            ({ items }) => DurableState.materialize(DurableState.materialized(Routed), items).rows,
          ),
        ),
    );

    for (const [name, relation] of Record.toEntries({ topOrder: topOrders, customerTotals }))
      expect(sorted(Array.flatMap(actual, (found) => [...found[name].values()])), name).toEqual(
        recompute(name, relation, Routed.collections[name].key, rows),
      );
  });

const bound = (shop: string) => circuit().at({ shop });

const routedBound = (shop: string) => routedCircuit.at({ shop });

const runBound = (shop: string) =>
  Projection.run(bound(shop), { limit: 1 }).pipe(Effect.map(({ units }) => units));

const runRouted = (shop: string) =>
  Projection.run(routedBound(shop), { limit: 1 }).pipe(Effect.map(({ units }) => units));

type Run = ReturnType<typeof runBound> | ReturnType<typeof runRouted>;

interface Kind {
  readonly key: (shop: string) => ReturnType<typeof bound> | ReturnType<typeof routedBound>;
  readonly run: (
    shop: string,
  ) => Effect.Effect<Effect.Success<Run>, Effect.Error<Run>, Effect.Services<Run>>;
  readonly forget: (shop: string) => Effect.Effect<void, ProjectionFault, Effect.Services<Run>>;
  readonly check: (
    shop: string,
    rows: ReadonlyArray<OrderRow>,
  ) => Effect.Effect<
    void,
    Effect.Error<ReturnType<typeof checkGenerated> | ReturnType<typeof checkRouted>>,
    Results | Routed
  >;
}

const kinds: Record.ReadonlyRecord<"generation-bound" | "routed", Kind> = {
  "generation-bound": {
    key: bound,
    run: runBound,
    forget: (shop) => Projection.forget(bound(shop)),
    check: checkGenerated,
  },
  routed: {
    key: routedBound,
    run: runRouted,
    forget: (shop) => Projection.forget(routedBound(shop)),
    check: checkRouted,
  },
};

const tables = ["circuits", "integrals", "arrangements", "accumulators", "key_owners"] as const;

const countRows = (sql: SqlClient.SqlClient) =>
  Effect.forEach(tables, (table) =>
    Effect.map(
      sql<{ readonly n: unknown }>`SELECT COUNT(*) AS n FROM ${sql(`streamsy_view_${table}`)}`,
      (rows) => Number(rows[0]?.n),
    ),
  );

const clears =
  (table: (typeof tables)[number]): StatementMatch =>
  (query) =>
    query.startsWith(`DELETE FROM streamsy_view_${table} `);

const isRouteList: StatementMatch = (query) =>
  query.startsWith("SELECT owner_key") && query.includes("owner_key >=");

/** The first statement of a forget that can wait for the unit's rows. */
const startsCleanup: StatementMatch = (query, params) =>
  isRouteList(query, params) || clears("integrals")(query, params);

const writesArrangement: StatementMatch = (query) =>
  query.startsWith("INSERT INTO streamsy_view_arrangements ");

const writesCheckpoint: StatementMatch = (query, params) =>
  query.startsWith("INSERT INTO streamsy_owner_records") &&
  Array.some(params, (param) => String(param).startsWith('["streamsy.projection.v1"'));

const levels = [
  { name: "Postgres", server: sqlDialects.pg, repeatableRead: false },
  { name: "MySQL default", server: sqlDialects.mysql, repeatableRead: false },
  { name: "MySQL REPEATABLE READ", server: sqlDialects.mysql, repeatableRead: true },
] as const;

const orders = [
  { id: "a0", text: "commits after the load and before the route listing" },
  { id: "a1", text: "commits after the load and before the first delete" },
  { id: "b", text: "holds its rows, before its checkpoint write, when cleanup starts" },
  { id: "c.mid", text: "holds some of its rows, and commits while cleanup runs" },
] as const;

/** Time for a statement that another host blocks to reach the server. */
const blocked = Effect.sleep("300 millis");

for (const level of levels) {
  describe.skipIf(level.server.url === undefined)(`forget on ${level.name}`, () => {
    for (const kind of ["generation-bound", "routed"] as const) {
      for (const race of orders) {
        // Only a routed forget lists route records.
        if (race.id === "a0" && kind !== "routed") continue;

        it.live(
          `${race.id}: keeps a ${kind} first unit that ${race.text}`,
          () =>
            Effect.gen(function* () {
              const { key, run, forget, check } = kinds[kind];
              const url = yield* fixtureDatabase(level.server);
              const options = { readLimit: 200, maxConnections: 1 };
              const unitHost = yield* Layer.build(hostAt(level.server, group, url, options));
              const forgetHost = yield* Layer.build(hostAt(level.server, group, url, options));
              const sql = Context.get(unitHost, SqlClient.SqlClient);
              const store = Context.get(unitHost, CheckpointStore);
              const shop = "race";
              const mysql = level.server.name === "mysql";

              // Routed forget at REPEATABLE READ holds the route gap that the unit inserts into.
              const deadlocks =
                level.repeatableRead &&
                kind === "routed" &&
                (race.id === "a1" || race.id === "c.mid");

              yield* View.migrate(sql);
              yield* seed(shop).pipe(Effect.provide(unitHost));

              const on =
                (host: typeof unitHost) =>
                <A, E, R>(effect: Effect.Effect<A, E, R>) =>
                  (level.repeatableRead
                    ? withRepeatableRead(Context.get(host, SqlClient.SqlClient), effect)
                    : effect
                  ).pipe(Effect.provide(host));

              const unitGate = yield* holdStatements(
                race.id === "c.mid" ? writesArrangement : writesCheckpoint,
              );

              const forgetGate = yield* holdStatements(
                race.id === "a0"
                  ? isRouteList
                  : race.id === "c.mid" && !mysql
                    ? clears("accumulators")
                    : race.id === "a1"
                      ? clears("integrals")
                      : startsCleanup,
              );

              // In a0 and a1 the gate is open, so the unit commits whole.
              if (race.id === "a0" || race.id === "a1") yield* unitGate.release();

              const startUnit = run(shop).pipe(unitGate.hold, on(unitHost), Effect.forkScoped);

              const startForget = forget(shop).pipe(
                forgetGate.hold,
                on(forgetHost),
                Effect.result,
                Effect.forkScoped,
              );

              const settled = yield* race.id === "a0" || race.id === "a1"
                ? Effect.gen(function* () {
                    const forgetting = yield* startForget;
                    yield* forgetGate.reached();
                    const unit = yield* startUnit;
                    yield* deadlocks ? blocked : Fiber.await(unit);
                    yield* forgetGate.release();

                    return { unit, forgetting };
                  })
                : Effect.gen(function* () {
                    const unit = yield* startUnit;
                    yield* unitGate.reached();
                    const forgetting = yield* startForget;
                    yield* forgetGate.reached();

                    // Postgres cleanup does not see the open unit's rows. MySQL cleanup waits for them.
                    if (mysql || race.id === "b") yield* forgetGate.release();

                    if (mysql) yield* blocked;
                    else if (race.id === "b") yield* Fiber.await(forgetting);

                    yield* unitGate.release();
                    yield* Fiber.await(unit);
                    yield* forgetGate.release();

                    return { unit, forgetting };
                  });

              expect(yield* Fiber.join(settled.unit)).toBe(1);
              const result = yield* Fiber.join(settled.forgetting);

              if (race.id === "b" && !mysql) expect(result._tag).toBe("Success");
              else
                expect(result).toMatchObject({
                  _tag: "Failure",
                  failure: {
                    _tag: "ProjectionFault",
                    reason: deadlocks ? "storage-failure" : "token-conflict",
                    retryable: true,
                  },
                });

              const rows = yield* countRows(sql);
              expect(Option.isSome((yield* store.load(key(shop))).record)).toBe(true);
              yield* check(shop, first).pipe(Effect.provide(unitHost));

              // A second binding that no forget touched has the same rows.
              yield* seed("control").pipe(Effect.provide(unitHost));
              yield* run("control").pipe(Effect.provide(unitHost));
              expect(yield* countRows(sql)).toEqual(Array.map(rows, (count) => count * 2));

              yield* change(shop).pipe(Effect.provide(unitHost));
              expect(yield* run(shop).pipe(Effect.provide(unitHost))).toBe(1);
              yield* check(shop, second).pipe(Effect.provide(unitHost));

              yield* forget(shop).pipe(Effect.provide(forgetHost));
              expect((yield* store.load(key(shop))).token).toBe("0");
              yield* forget("control").pipe(Effect.provide(forgetHost));
              expect(yield* countRows(sql)).toEqual(Array.map(tables, () => 0));
            }),
          30_000,
        );
      }
    }
  });
}
