/** Run from the repository root: bun run --cwd packages/views bench:sql 10000 <scratch-folder>
 * File-backed WAL, 1,000 input changes per load unit, four shared views.
 * Timings include the Effect SQL transaction and generating output changes.
 * The consumer Map applies changes outside the timed window.
 */
import { Array as Arr, Effect, Layer } from "effect";
import { Transaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { join } from "node:path";
import { SqlClient } from "effect/sql";
import { View, ViewBackend } from "../src/index.ts";
import { currentBackend } from "../src/view-backend.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- This is a pure compiler, not an Effect service constructor.
import { makeEngine, type StateChange, type OutputChange } from "../src/sql/engine.ts";
import { canonicalJson, encodeRowKey } from "../src/runtime/key.ts";
import { shopPlans } from "../test/sql/fixtures.ts";
import type { JsonObject, RowKey } from "../src/ir/contracts.ts";

interface Order {
  readonly orderId: string;
  readonly customerId: string;
  readonly total: number;
  readonly status: string;
}

interface Customer {
  readonly customerId: string;
  readonly name: string;
  readonly tier: string;
}

const count = Number(process.argv[2] ?? 10_000);

const scratch = process.argv[3];

if (!scratch || !Number.isInteger(count) || count < 10 || count % 10)
  throw new Error("usage: bun run bench:sql <orders divisible by 10> <scratch-folder>");

const customerCount = count / 10;

const database = join(scratch, `views-sql-${count}-${Date.now()}.sqlite`);

const host = Layer.merge(Transaction.layer, ViewBackend.layerSql).pipe(
  Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: database } })),
);

const order = (i: number): Order => ({
  orderId: `o-${i}`,
  customerId: `c-${i % customerCount}`,
  total: (i * 7919) % 1000,
  status: "placed",
});

const customer = (i: number): Customer => ({
  customerId: `c-${i}`,
  name: `Customer ${i}`,
  tier: i % 2 === 0 ? "gold" : "standard",
});

const upOrder = (o: Order): StateChange => ({
  sourceId: "orders",
  kind: "upsert",
  key: o.orderId,
  value: { ...o },
});

const upCustomer = (c: Customer): StateChange => ({
  sourceId: "customers",
  kind: "upsert",
  key: c.customerId,
  value: { ...c },
});

const actual = new Map<string, string>();

const orderEdits = new Map<string, Order | undefined>();

const customerEdits = new Map<string, Customer>();

const identity = (view: string, key: RowKey) => `${view}/${encodeRowKey(key)}`;

function consume(changes: readonly OutputChange[]) {
  for (const c of changes) {
    const id = identity(c.view, c.key);

    if (c.kind === "delete") actual.delete(id);
    else actual.set(id, canonicalJson(c.value));
  }
}

await Effect.runPromise(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* View.migrate(sql);
    const owner = yield* Transaction;
    const engine = yield* makeEngine(shopPlans());

    const runUnit = (input: readonly StateChange[]) =>
      owner.withTransaction(
        Effect.flatMap(currentBackend, (backend) => engine.process(backend, "bench", input)),
      );

    console.log(
      JSON.stringify({
        orders: count,
        customers: customerCount,
        database,
        effect: "4.1.5",
        units: "ms",
        mode: "WAL",
      }),
    );
    const start = performance.now();

    for (let i = 0; i < customerCount; i += 1000)
      consume(
        yield* runUnit(
          Array.from({ length: Math.min(1000, customerCount - i) }, (_, k) =>
            upCustomer(customer(i + k)),
          ),
        ),
      );

    for (let i = 0; i < count; i += 1000) {
      consume(
        yield* runUnit(
          Array.from({ length: Math.min(1000, count - i) }, (_, k) => upOrder(order(i + k))),
        ),
      );

      if (i % 100000 === 0) console.log(`loaded orders=${i + Math.min(1000, count - i)}`);
    }

    const loadMs = performance.now() - start;
    console.log(
      JSON.stringify({
        loadMs,
        changes: count + customerCount,
        microsecondsPerChange: (loadMs * 1000) / (count + customerCount),
      }),
    );

    const measure = function* (
      unit: string,
      make: (k: number) => readonly StateChange[],
      runs = 300,
    ) {
      const times: number[] = [];
      let output = 0;

      for (let k = 0; k < runs; k++) {
        const input = make(k);
        const start = performance.now();
        const changes = yield* runUnit(input);
        times.push(performance.now() - start);
        output += changes.length;
        consume(changes);
      }

      times.sort((a, b) => a - b);
      console.log(
        JSON.stringify({
          unit,
          runs,
          medianMs: times[Math.floor(runs / 2)],
          p99Ms: times[Math.floor(runs * 0.99)],
          outputChanges: output / runs,
        }),
      );
    };

    const editOrder = (o: Order) => {
      orderEdits.set(o.orderId, o);

      return [upOrder(o)];
    };

    const editCustomer = (c: Customer) => {
      customerEdits.set(c.customerId, c);

      return [upCustomer(c)];
    };

    yield* measure("order total", (k) => {
      const o = order((k * 37) % count);

      return editOrder({ ...o, total: o.total + 1 + (k % 5) });
    });
    yield* measure("order moves customer", (k) => {
      const o = order((k * 41 + 7) % count);

      return editOrder({ ...o, customerId: `c-${(k * 13 + 1) % customerCount}` });
    });
    yield* measure("customer renamed", (k) =>
      editCustomer({ ...customer((k * 2) % customerCount), name: `Renamed ${k}` }),
    );
    yield* measure("customer changes tier", (k) =>
      editCustomer({ ...customer((k * 2 + 1) % customerCount), tier: "gold" }),
    );
    yield* measure("order deleted", (k) => {
      const key = `o-${(k * 53 + 3) % count}`;
      orderEdits.set(key, undefined);

      return [{ sourceId: "orders", kind: "delete", key }];
    });
    yield* measure(
      "100 orders",
      (k) =>
        Arr.flatMap(
          Array.from({ length: 100 }, (_, i) => i),
          (i) => {
            const o = order((k * 100 + i) % count);

            return editOrder({ ...o, total: (o.total + k + i) % 1000 });
          },
        ),
      50,
    );
    verify();
  }).pipe(Effect.provide(host)),
);

/** Independent full recompute of the spike's four views, with indexed JS joins.
 * Avoid the generic oracle's O(orders × customers) nested-loop join at 1M rows.
 */
function verify(): void {
  const customers = new Map<string, Customer>();

  for (let i = 0; i < customerCount; i++) {
    const c = customer(i);
    customers.set(c.customerId, customerEdits.get(c.customerId) ?? c);
  }

  const groups = new Map<string, { count: number; sum: number; max: number; top: Order[] }>();
  const tiers = new Map<string, { count: number; sum: number }>();

  let expected = 0,
    wrong = 0;

  const check = (view: string, key: RowKey, row: JsonObject) => {
    expected++;

    if (actual.get(identity(view, key)) !== canonicalJson(row)) wrong++;
  };

  for (let i = 0; i < count; i++) {
    const base = order(i);
    const o = orderEdits.has(base.orderId) ? orderEdits.get(base.orderId) : base;

    if (!o) continue;
    const c = customers.get(o.customerId);

    if (c?.tier === "gold")
      check("goldOrders", o.orderId, { orderId: o.orderId, customerName: c.name, total: o.total });

    if (c) {
      const t = tiers.get(c.tier) ?? { count: 0, sum: 0 };
      t.count++;
      t.sum += o.total;
      tiers.set(c.tier, t);
    }

    const g = groups.get(o.customerId) ?? { count: 0, sum: 0, max: -Infinity, top: [] };
    g.count++;
    g.sum += o.total;
    g.max = Math.max(g.max, o.total);
    g.top.push(o);
    g.top.sort(
      (a, b) => b.total - a.total || (a.orderId < b.orderId ? -1 : a.orderId > b.orderId ? 1 : 0),
    );

    if (g.top.length > 3) g.top.pop();
    groups.set(o.customerId, g);
  }

  for (const [customerId, g] of groups) {
    check("totals", [customerId], { customerId, orders: g.count, revenue: g.sum, largest: g.max });

    for (const o of g.top) check("topOrders", o.orderId, { ...o });
  }

  for (const [tier, t] of tiers)
    check("revenue", [tier], { tier, orders: t.count, revenue: t.sum });

  if (actual.size !== expected) wrong += Math.abs(actual.size - expected);
  console.log(JSON.stringify({ oracle: { actual: actual.size, expected, wrong } }));

  if (wrong) throw new Error(`benchmark oracle: ${wrong} differences`);
}
