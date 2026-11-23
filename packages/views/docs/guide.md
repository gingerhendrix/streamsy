# Views guide

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

Use `View.fn(name, version, implementation)` to declare a pure, synchronous expression for `select`, `selectRow` or `where`. The callback must be deterministic for its arguments and must leave its inputs unchanged. Its name and positive integer version are part of the plan hash. Raise the version when the implementation changes.

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

const order = View.fn("order-row", 1, (orderId: string, customerName: string) => ({
  orderId,
  customerName,
}));
const projected = joined.selectRow((row) => order(row.orderId, displayName(row.customer.name)));
```

A missing alias or optional leaf passes `undefined` to the function. TypeScript requires such arguments to accept `undefined` explicitly, or to be supplied by a fallback or an unwrap expression. Present arguments retain their values, including `null`. A function can return `undefined` intentionally to omit a selected field. A function used as a filter must return a Boolean.

`selectRow` takes one expression that supplies the entire output row. Use it with `View.fn` or an object field such as `row.value`. Its expression must return a JSON object. Both `select` and `selectRow` keep the relation's input key. A circuit uses its output State collection's declared key to write rows.

`left.concat(right)` keeps every row from both relations, including identical values. It namespaces the input relation keys as `["l", encodedKey]` and `["r", encodedKey]`. The row values pass through unchanged.

`closure({ parent, part, maxDepth })` emits one row for each descendant and each reachable ancestor within a positive integer bound.

```ts
const lineage = agents.closure({
  parent: (agent) => agent.parentRef,
  part: (agent) => agent.createdAt,
  maxDepth: 64,
});
```

Each node is identified by its string relation key and can name one parent. For State sources, `parent` refers to the wire key, so a numeric key field must be converted to a string. An absent parent marks a root. A parent key with no corresponding row is included as the last ancestor. Each upward walk stops before visiting the starting node or repeating an ancestor, so cycles and self parents produce no reflexive rows.

The row contains `node`, `ancestor` and the edge count `depth`. The row key is the pair of encoded node and ancestor keys. With `part`, the row also contains `path`, with one value per edge ordered from the row immediately below the ancestor down to the node. An absent value on an emitted path fails the unit with `ViewDataFault`. Read the optional path with `View.field(row, "path")` because `path` overlaps the expression's own field.

Declare an output State route with a numeric generation parameter in its path, then pass the route as `output: Projection.perGeneration(ShopViews, "generation")`. For multiple outputs, wrap every route in `outputs` and group the `views` under the same output names. Each State collection needs exactly one view. A circuit rejects incomplete coverage and repeated State classes at declaration time. Collection names can repeat across different State classes. The State collection supplies the output key and schema. TypeScript checks the view row type, and output encoding validates each row at runtime.

A circuit returns a projection declaration with `.at(params)`. The circuit's `generation` defaults to 1. The `retry` option accepts the projection binding policy. Input route parameters and the remaining output route parameters must be circuit parameters with the same decoded types. Projection supplies the generation parameter from the circuit's `generation`. Pass `views: (params) => ({ ... })` to use decoded JSON-valued binding parameters as expressions, for example `row.workspace.eq(params.workspace)`. The plan hash includes the parameter names. Each binding evaluates the plan with its own decoded values and SQL namespace.

## Routed outputs

Use `Projection.perGeneration(DayRows, "generation", { routeBy: ["workspace", "date"] })` to have one binding write to multiple workspace and date streams. Wrap every view of that output in `View.routed(relation, { by: (row) => ({ workspace: row.workspace, date: row.date }) })`. The selector supplies exactly the route names and types, using strings, finite numbers or Booleans. Add `row: (row) => row.value` when the relation carries the emitted row as a nested value. Routing and binding parameter expressions can be used together.

The engine uses the route values followed by the collection key. Two streams can hold the same collection key. If a row moves, the old stream gets a delete and the new stream gets an upsert in the same transaction with the same `txid`. Readers following one stream receive its own notification.

A route that has never had an emitted row returns `StreamNotFound`. The first row creates the stream in the checkpoint transaction, and deleting its last row leaves a stream containing its history. If a route value fails to encode, the unit fails with a `ViewDataFault` identifying the row. For a rebuild, check `Projection.caughtUp` on the binding before switching readers.

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
  const outputParams = { ...params, generation: shopViews.generation };
  yield* Projection.run(ShopViewsTable.at(outputParams));
  return yield* ShopViewsTable.at(outputParams).list("customerTotals");
});

await Effect.runPromise(program.pipe(Effect.provide(host)));
```

Build the host once for its lifetime. Migrate before starting runners. Projection creates or checks each output stream in the first unit transaction. Each owned stream must start empty and open. All declared outputs must share the transaction's owner. Its inputs can use the normal projection stream services; no synchronized input `StateTable` is required. The circuit integrates exactly the input pages of its unit.

For a long-lived process, attach with `Projection.attach(shopViews, { durable: true })` over these same host services after migration. Attach the output `StateTable` too when readers need it kept current. Use `Projection.Registry.awaitOffset` for read-your-writes. Do not also run/follow an attached binding. The [README](../README.md) contains the complete example State declarations.

## Durable Object host

Use `DurableObjectStorage.layer` with the object's full `storage`, plus `CheckpointStore.layer` and `Transaction.layer`. Call the same `View.migrate(sql)` and `StateTable.migrate(sql)`. Run the bound circuit with `Projection.run`, or attach it through `ProjectionWakeup.attach` in the normal Durable Object alarm host. No alternate view runtime or SQL dialect is needed.

The local workerd proof is `test/circuit/workerd.test.ts`, with its worker in `test/circuit/support/worker.ts`. It uses miniflare and never deploys. It runs the same proof as Bun, through real State appends, projection checkpoints and output StateTables. The proof compares gold orders, customer totals, per-customer top 3 and order lineage against the full-recompute oracle after each unit. It also checks a no-op unit, a shared `txid` on every emitted change and a generation-2 lineage replay.

```sh
bun run --cwd packages/views test:unit
bun run --cwd packages/views test:workerd
```

## Transaction and State semantics

Each unit normalizes input changes using the circuit's own SQL integrals. Numeric State keys use their string wire representation; numeric row fields retain their numeric type. Deletes need no `old_value`. A `reset` retracts the source rows. Snapshots preserve previously visible rows until `snapshot-end`, then retract unseen keys; snapshot bookkeeping persists across units. These controls can visit a whole relation.

The SQL engine shares compatible arrangements within a circuit. It uses affected-key evaluation, differences for `count`/`sum`, and ordered indexes for extrema, lists and top. It coalesces the final differences before output. Output State messages are absolute upserts/deletes and carry `txid: unit.key`. A unit with no changed output rows writes no output messages while still advancing its checkpoint.

Operator SQL, output appends, plan metadata and the checkpoint commit together in the owner transaction.

## Plan identity, rebuilds and retirement

Operator namespaces are keyed by projection id, encoded params and generation. Canonical plan hashes are stored per namespace. Changing an operator, filter, output key or other plan IR under the same generation fails the next unit with the typed `PlanChanged` fault. No checkpoint, SQL state or output append is committed. The fault exposes `namespace`, `stored` and `requested`. Attached runners stop the affected binding after a `PlanChanged` or `ViewDataFault`. The registry lists the failure and its message. Reordering object properties alone preserves the hash and compiled state identities. Collection codec changes also require a new generation. A release that changes the operator-state table layout or internal plan naming marks it as a **Breaking change:** in the changelog. When upgrading to that release, raise `generation` on every affected circuit to rebuild into fresh namespaces and output streams.

If an older declaration set a circuit `version`, remove that option and raise `generation` above every generation that circuit id has used.

Raise `generation` to rebuild from retained input history into new stream addresses. When the checkpoint store supports listing, attach queues bindings that have a checkpoint at an older generation and none at the current generation. A source write or a call to `Registry.schedule` can also queue a binding. Readers get `StreamNotFound` at a new address until the first unit commits. Keep readers pinned to the old generation until the new one catches up. A downstream circuit needs its own raised `generation` when it changes input addresses.

The previous generation keeps its checkpoint, operator state and output streams. To roll back before retirement, resume that generation from its checkpoint. Old and new copies coexist, so a rebuild can double storage until retirement.

Stop and drain the binding’s runners before calling `Projection.forget(bound)`. Forget clears the checkpoint and operator state in the owner transaction and keeps the output streams. Replaying into a populated output fails with `invalid-output`. Raise `generation` for a fresh replay.

Stop and drain old runners before calling `Projection.retire(circuit, params, { generation })`. Retirement replaces that generation's checkpoint with a tombstone, clears its operator namespace and removes its owned output streams in one transaction. Use `streams: "close"` to close those streams instead. Future runs of the retired generation fail with `retired-generation`.

## Ordering and limits

String expressions compare lexicographically by UTF-16 code units, without locale comparison, case folding or Unicode normalization. `min`, `max`, `list` and `top` follow that contract. Top needs a finite positive bound and a stable final sort term. Use `View.key.asc()` to sort by the encoded relation key, including keys produced by joins and concat. Top selects a keyed relation. StateTable reads order by wire key.

`View.list(expression)` collects a group's string or finite number values in ascending order and keeps duplicates. Numbers precede strings in a mixed group. Absent operands are skipped. A group with no members has no output row. When a member changes the list, the aggregate emits the whole array. `maxAffectedKeys` does not bound group size.

Operator state on Bun and Durable Object SQLite uses integrals, payload-free arrangements and accumulators. A separate circuit metadata table records identity and plan hash. The store computes index keys in JavaScript and binds at most 100 values per statement.

A source used only by direct `where` filters keeps rows that pass at least one of those filters. Another consumer of the unfiltered source keeps its full integral.

Set the optional `maxAffectedKeys` to a positive safe integer to cap a unit's distinct changed relation keys, including intermediate results. Joins also check the number of candidate keys before building large joined results. The unit rolls back on overflow and attached runners stop the binding with a `ViewDataFault` that names a key. The limit governs fan-out, so an operator may still read unchanged rows, for example when recomputing a fractional sum. The limit can be changed without changing the plan hash.

Closure uses a parent index over the input. An edge or part change recomputes the changed node and descendants within `maxDepth - 1` edges in both the before and after graphs. Reparenting a subtree with `s` nodes from a chain of height `h1` to one of height `h2` can change up to `s * (min(h1, maxDepth) + min(h2, maxDepth))` closure rows. `maxAffectedKeys` checks the number of candidate nodes and counts changed closure keys toward the unit limit. A consumer that reads closure through an arrangement stores those rows in the normal integral. With `part`, each stored row carries `depth` path values, so one descendant carries up to `d * (d + 1) / 2` path values across its closure rows, where `d` is its depth capped at `maxDepth`. An entire untruncated chain holds a cubic number of path values in its height.

Unsupported operators and features include `avg`, `distinct`, fact folds, the memory tier, dynamic parameters-as-data, frontier rows and `old_value` output. Input values must be JSON objects and collection codecs must require no services. Views omits `undefined` members from decoded input objects at every depth and rejects `undefined` array elements. Retained State streams are the source of truth for rebuilding SQL state.

## Package exports

The package root exports the `View` namespace, fault classes and types. Internal compiler and SQL modules are available to the package's own tests.
