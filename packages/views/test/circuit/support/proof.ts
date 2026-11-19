import { Array, Effect, Record, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState } from "@streamsy/core";
import { Projection, StateTable } from "@streamsy/projection";
import { View, compilePlan, keyExpression } from "../../../src/relation.ts";
import { fullRecompute } from "../../../src/sql/reference.ts";
import type { JsonObject } from "../../../src/ir/contracts.ts";
import { canonicalJson } from "../../../src/runtime/key.ts";
import { Shop, Results, Table, circuit, views, type Order, type Customer } from "./shop.ts";

export interface Billing {
  readonly rowsRead: number;
  readonly rowsWritten: number;
}

export interface Measure {
  readonly reset: () => void;
  readonly read: () => Billing;
}

const unmeasured: Measure = { reset: () => {}, read: () => ({ rowsRead: 0, rowsWritten: 0 }) };

/** Identical proof on Bun and DO SQLite; only the host Layer changes. */
export const proof = Effect.fn("ViewsTest.proof")(function* (measure: Measure = unmeasured) {
  const sql = yield* SqlClient.SqlClient;
  yield* View.migrate(sql);
  yield* StateTable.migrate(sql);
  const input = yield* Shop;
  const output = yield* Results;
  const params = { shop: "proof" };
  yield* input.create(params);
  const orderRows = new Map<string, typeof Order.Type>();
  const customerRows = new Map<string, typeof Customer.Type>();
  const binding = circuit().at(params);
  let offset: string | undefined;
  const units: { name: string; billing: Billing; outputChanges: number; txids: string[] }[] = [];

  const check = Effect.gen(function* () {
    yield* Projection.run(Table.at(params));
    const sources: Record<string, { key: string; row: JsonObject }[]> = {};
    sources[JSON.stringify([Shop.key, "order"])] = Array.map([...orderRows], ([key, row]) => ({
      key,
      row,
    }));
    sources[JSON.stringify([Shop.key, "customer"])] = Array.map(
      [...customerRows],
      ([key, row]) => ({ key, row }),
    );

    for (const [name, relation] of Record.toEntries(views)) {
      const plan = compilePlan(name, {
        kind: "key",
        input: relation.expression,
        key: keyExpression(Results.collections[name].key),
      });

      const expected = fullRecompute({ plan, sources });
      const actual = (yield* Table.at(params).list(name)).rows;
      const expectedRows = Array.map(expected.rows, ({ row }) => canonicalJson(row)).sort();
      const actualRows = Array.map(actual, ({ value }) => canonicalJson(value)).sort();

      if (JSON.stringify(expectedRows) !== JSON.stringify(actualRows))
        throw new Error(`Oracle mismatch in ${name}`);
    }
  });

  const run = (name: string) =>
    Effect.gen(function* () {
      measure.reset();
      const result = yield* Projection.run(binding, { limit: 1 });
      const billing = measure.read();

      if (result.units !== 1) throw new Error(`${name}: expected exactly one unit`);

      const batches = yield* output
        .read(params, offset === undefined ? {} : { offset })
        .pipe(Stream.runCollect);

      const changes = Array.flatMap(batches, (batch) => batch.items);
      offset = batches.at(-1)?.nextOffset ?? offset;

      const txids = [
        ...new Set(
          Array.map(changes, (change) =>
            "type" in change ? (change.headers.txid ?? "missing") : "control",
          ),
        ),
      ];

      if (txids.length !== 1 || txids[0] === "missing")
        throw new Error(`${name}: output changes do not share one txid`);

      const keys = Array.map(changes, (change) =>
        "type" in change ? JSON.stringify([change.type, change.key]) : "control",
      );

      if (new Set(keys).size !== keys.length)
        throw new Error(`${name}: output changes are not coalesced`);
      units.push({ name, billing, outputChanges: changes.length, txids });
      yield* check;
      const saved = yield* Projection.read(binding);

      if (saved.offsets[Shop.key] !== result.offsets[Shop.key])
        throw new Error("Checkpoint mismatch");
    });

  const customers = Array.makeBy(10, (i) => ({
    customerId: `c${i}`,
    name: `Customer ${i}`,
    tier: i % 2 === 0 ? "gold" : "silver",
  }));

  const orders = Array.makeBy(100, (i) => ({
    orderId: `o${String(i).padStart(3, "0")}`,
    customerId: `c${Math.floor(i / 10)}`,
    total: i + 1,
  }));

  for (const row of customers) customerRows.set(row.customerId, row);

  for (const row of orders) orderRows.set(row.orderId, row);
  yield* input.append(
    params,
    DurableState.changes(Shop.at(params), {}, [
      ...Array.map(customers, (row) => DurableState.upsert("customer", row)),
      ...Array.map(orders, (row) => DurableState.upsert("order", row)),
    ]),
  );
  yield* run("load");
  const order = { orderId: "o000", customerId: "c0", total: 250 };
  orderRows.set(order.orderId, order);
  yield* input.append(
    params,
    DurableState.changes(Shop.at(params), {}, [DurableState.upsert("order", order)]),
  );
  yield* run("single-order");
  const customer = { customerId: "c0", name: "Renamed", tier: "gold" };
  customerRows.set(customer.customerId, customer);
  yield* input.append(
    params,
    DurableState.changes(Shop.at(params), {}, [DurableState.upsert("customer", customer)]),
  );
  yield* run("customer-rename");
  const updated = Array.map(orders, (row) => ({ ...row, total: row.total + 500 }));

  for (const row of updated) orderRows.set(row.orderId, row);
  yield* input.append(
    params,
    DurableState.changes(
      Shop.at(params),
      {},
      Array.map(updated, (row) => DurableState.upsert("order", row)),
    ),
  );
  yield* run("100-changes");
  // No-op upserts advance the checkpoint but append nothing.
  const before = yield* output.head(params);
  yield* input.append(
    params,
    DurableState.changes(
      Shop.at(params),
      {},
      Array.map(updated, (row) => DurableState.upsert("order", row)),
    ),
  );
  const noop = yield* Projection.run(binding);
  const after = yield* output.head(params);

  if (before.nextOffset !== after.nextOffset || noop.units !== 1)
    throw new Error("No-op output was appended");

  return {
    units,
    rows: { goldOrder: 50, customerTotals: 10, topOrder: 30 },
    noopUnits: noop.units,
  };
});
