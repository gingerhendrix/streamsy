# Views 0.5 guide

## Declare relations and a circuit

Use `View.from(StateClass, "collection")`. Relations take their row type, schema and key from the State collection. Do not redeclare sources or schema references for circuits. Every relation edge has unique keys.

```ts
const orders = View.from(Shop, "order");
const customers = View.from(Shop, "customer");

const goldOrders = orders
  .join(customers, {
    as: "customer",
    on: (order, customer) => order.customerId.eq(customer.customerId),
  })
  .where((row) => row.customer.tier.eq("gold"))
  .select((row) => ({ orderId: row.orderId, customerName: row.customer.name, total: row.total }));

const totals = orders
  .groupBy((row) => ({ customerId: row.customerId }))
  .aggregate((row) => ({
    orders: View.count(),
    revenue: View.sum(row.total),
    smallest: View.min(row.total),
    largest: View.max(row.total),
  }));

const top = orders.top({
  partitionBy: (row) => row.customerId,
  by: (row) => [row.total.desc(), row.orderId.asc()],
  limit: 3,
});
```

A join condition must be equality between a left-only and a right-only expression.
A left-joined alias can be absent. Nested references such as `row.customer.name` preserve optionality through every field, and `select` omits absent fields from its output. Use `isPresent()` to test presence, `orElse(fallback)` to supply a value, or `unwrap()` to fail the unit if a value is absent. Use `View.field(row.value, "path")` when a field name overlaps an expression member.

Use `View.fn(name, version, implementation)` to declare a pure, synchronous expression for `select` or `where`. The callback must be deterministic for its arguments and must leave its inputs unchanged. Its name and positive integer version are part of the plan hash. Raise the version when the implementation changes.

```ts
const displayName = View.fn(
  "customer-display-name",
  1,
  (name: string | undefined) => name ?? "Guest",
);
const joined = orders.leftJoin(customers, {
  as: "customer",
  on: (order, customer) => order.customerId.eq(customer.customerId),
});
const selected = joined.select((row) => ({
  orderId: row.orderId,
  customerName: displayName(row.customer.name),
}));
```

A missing alias or optional leaf passes `undefined` to the function. TypeScript requires such arguments to accept `undefined` explicitly, or to be supplied by a fallback or an unwrap expression. Present arguments retain their values, including `null`. A function can return `undefined` intentionally to omit a selected field. A function used as a filter must return a Boolean.

Declare output State collections with their schemas and keys, then pass their relations in `View.circuit({ id, params, output, views })`. Each State collection needs exactly one view. For multiple State classes, pass `outputs: { orders: OrderRows, customers: CustomerRows }` and group `views` under the same output names. A circuit rejects incomplete coverage and repeated State classes at declaration time. Collection names can repeat across different State classes. Row types, including key fields, are checked at compile time, and output encoding validates rows at runtime. The output key comes from the State collection.

A circuit returns a projection declaration with `.at(params)`. `version` and `generation` default to 1. The `retry` option accepts the projection binding policy. State input/output parameters must be subsets of the circuit parameters with the same decoded types. Pass `views: (params) => ({ ... })` to use decoded JSON-valued binding parameters as expressions. For example, a filter can use `row.workspace.eq(params.workspace)`. The plan hash identifies parameter names, and each binding evaluates that plan with its own decoded values and SQL namespace.

Set `wakeAt: (unit) => Projection.wakeAt(deadline)` to save a wake deadline with the unit. Projection supplies the recorded due time in `unit.due` for timer units, and returning void clears the deadline.

## Bun host

```ts
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Projection, StateTable, Transaction } from "@streamsy/projection";
import { View } from "@streamsy/views";

const storage = BunStorage.layer(Streamsy.group(Shop, ShopViews), {
  client: { filename: "./shop.sqlite" },
});
const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(storage),
);
const ShopViewsTable = StateTable.define(ShopViews);

const program = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* View.migrate(sql);
  yield* StateTable.migrate(sql);
  const shop = yield* Shop;
  const params = { shopId: "main" };
  yield* shop.create(params);
  // Append input changes with DurableState.changes as for any State stream.
  yield* Projection.run(shopViews.at(params));
  yield* Projection.run(ShopViewsTable.at(params));
  return yield* ShopViewsTable.at(params).list("customerTotals");
});

await Effect.runPromise(program.pipe(Effect.provide(host)));
```

Build the host once for its lifetime. Migrate before starting runners. The circuit creates its output stream in its transaction if necessary. Its declared outputs must share the transaction's owner. Its inputs can use the normal projection stream services; no synchronized input `StateTable` is required. The circuit integrates exactly the input pages of its unit.

For a long-lived process, attach with `Projection.attach(shopViews, { durable: true })` over these same host services after migration. Attach the output `StateTable` too when readers need it kept current. Use `Projection.Registry.awaitOffset` for read-your-writes. Do not also run/follow an attached binding. The [README](../README.md) contains the complete example State declarations.

## Durable Object host

Use `DurableObjectStorage.layer` with the object's full `storage`, plus `CheckpointStore.layer` and `Transaction.layer`. Call the same `View.migrate(sql)` and `StateTable.migrate(sql)`. Run the bound circuit with `Projection.run`, or attach it through `ProjectionWakeup.attach` in the normal Durable Object alarm host. No alternate view runtime or SQL dialect is needed.

The local workerd proof is `test/circuit/workerd.test.ts`, with its worker in `test/circuit/support/worker.ts`. It uses miniflare and never deploys. It runs the same proof as Bun, through real State appends, projection checkpoints and output StateTables. The proof compares gold orders, customer totals and per-customer top 3 against the full-recompute oracle after each unit. It also checks a no-op unit and a shared `txid` on every emitted change.

```sh
bun run --cwd packages/views test:unit
bun run --cwd packages/views test:workerd
```

The workerd test writes its bundle and `billing.json` under `packages/views/build/workerd/`. Billable counts come from native SQL cursor `rowsRead`/`rowsWritten`, read after consumption. Measurement brackets one `Projection.run(binding, { limit: 1 })`, including input reads, operator SQL, output appends and the checkpoint. Input appends, migration, output-table reads and oracle comparisons are outside that bracket. The three measured workloads are one order total change, one customer rename affecting ten orders and a 100-order change unit.

## Transaction and State semantics

Each unit normalizes input changes using the circuit's own SQL integrals. Numeric State keys use their string wire representation; numeric row fields retain their numeric type. Deletes need no `old_value`. A `reset` retracts the source rows. Snapshots preserve previously visible rows until `snapshot-end`, then retract unseen keys; snapshot bookkeeping persists across units. These controls can visit a whole relation.

The SQL engine shares compatible arrangements within a circuit. It uses affected-key evaluation, differences for `count`/`sum`, and ordered indexes for extrema and top. It coalesces the final differences before output. Output State messages are absolute upserts/deletes and carry `txid: unit.key`. A unit with no changed output rows writes no output messages while still advancing its checkpoint.

The circuit uses `Commit.transactionWithRemove` to delete operator state in the same transaction as checkpoint removal. Operator SQL, output appends, plan metadata and checkpoint either commit together or roll back.

## Plan identity, generations and forget

Operator namespaces include projection id, version, encoded params and generation. Canonical plan hashes are stored per namespace. Changing an operator, filter, output key or other plan IR under the same generation fails the next unit with the typed `View.PlanChanged` fault. No checkpoint, SQL state or output append is committed. The fault exposes `namespace`, `stored` and `requested`. Attached runners stop the affected binding after a `PlanChanged` or `ViewDataFault`. The registry lists the failure and its message. Reordering object properties alone preserves the hash and compiled state identities. Collection codecs are not executable code in the plan hash; a codec change also requires a new generation.

Raise `generation` to rebuild from retained input history. On the first successful unit of the new generation, the circuit removes older operator namespaces for the same id, version and params. A later unit from an older generation fails while the newer metadata exists. Old projection checkpoint records are left to Streamsy. Stop old runners before starting the replacement.

A rebuild appends to the existing output stream. Delete or reset the outputs first when the new plan can remove previously produced keys. This follows the normal projection lifecycle; creating a declaration alone does not touch SQL or streams.

`Projection.forget(bound)` removes its checkpoint and every operator row of its namespace in one owner transaction. `View.forget` is the same function. Stop all runners for that binding first. Forget does not remove output streams or their `StateTable` rows. A subsequent run starts from zero input offsets with empty operator state. Use a new generation for code changes rather than forgetting a running binding.

## Ordering and limits

String expressions compare lexicographically by UTF-16 code units, without locale comparison, case folding or Unicode normalization. `min`, `max` and `top` follow that contract. Top needs a finite positive bound and the row key as its stable final sort term. It selects a keyed relation, not a ranked result list; StateTable reads order by wire key.

Only the SQL tier exists: SQLite on Bun or Durable Objects. Operator state uses integrals, payload-free arrangements and accumulators; a separate circuit metadata table records identity and plan hash. SQL stays in the store module, computes index keys in JavaScript, uses no JSON SQL functions or expression indexes, and keeps statements below 100 bound parameters.

There is no fan-out cap. A key with many dependants can create a large transaction. There is no `avg`, `distinct`, fact fold, closure, memory tier, dynamic parameters-as-data, frontier row or opt-in `old_value` output. Input values must be JSON objects; collection codecs requiring services cannot be used with Streamsy 0.12 projection declarations. Retained State streams are the source of truth; SQL state is rebuildable.

## Lower-level API and migration from 0.4

The package root exports the `View` namespace, fault classes and types. Internal compiler and SQL modules are available to the package's own tests.
