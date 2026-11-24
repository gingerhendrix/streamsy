# @streamsy/views

Incrementally maintain Streamsy State collections on memory, Bun SQLite, Durable Object SQLite, Postgres, or MySQL. A circuit consumes input pages, updates indexed operator state, appends changed output rows, and saves its checkpoint in one owner transaction.

```sh
bun add @streamsy/views @streamsy/projection @streamsy/core effect
```

Requires Effect 4. On MySQL, install `@streamsy/storage` at the same version as `@streamsy/views`. The package is pre-1.0. Raise a circuit's `generation` when a release note calls for rebuilding after an operator-state layout change. See the [guide](docs/guide.md) for host setup, lifecycle and checks, and the [documentation](https://streamsy.dev/docs/views).

See [Run on MySQL](https://streamsy.dev/docs/views/mysql) for MySQL configuration and limits.

```ts
import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Projection, StateTable } from "@streamsy/projection";
import { View } from "@streamsy/views";

const Order = Schema.Struct({
  orderId: Schema.String,
  customerId: Schema.String,
  total: Schema.Finite,
});

class Shop extends Streamsy.state<Shop>()("Shop/Input", "shop/:shopId", {
  params: { shopId: Schema.String },
  collections: { order: { schema: Order, key: "orderId" } },
}) {}

class ShopViews extends Streamsy.state<ShopViews>()(
  "Shop/Views",
  "shop/:shopId/views/g/:generation",
  {
    params: { shopId: Schema.String, generation: Schema.FiniteFromString },
    collections: {
      customerTotals: {
        schema: Schema.Struct({
          customerId: Schema.String,
          orders: Schema.Finite,
          revenue: Schema.Finite,
        }),
        key: "customerId",
      },
      topOrder: { schema: Order, key: "orderId" },
    },
  },
) {}

const orders = View.from(Shop, "order");
const customerTotals = orders
  .groupBy((row) => ({ customerId: row.customerId }))
  .aggregate((row) => ({ orders: View.count(), revenue: View.sum(row.total) }));
const topOrders = orders.top({
  partitionBy: (row) => row.customerId,
  by: (row) => [row.total.desc(), row.orderId.asc()],
  limit: 3,
});

export const shopViews = View.circuit({
  id: "shop-views",
  params: { shopId: Schema.String },
  output: Projection.perGeneration(ShopViews, "generation"),
  views: { customerTotals, topOrder: topOrders },
});

const bound = shopViews.at({ shopId: "main" });
const attached = Projection.attach(shopViews, { durable: true });
const ShopViewsTable = StateTable.define(ShopViews);
```

`View.from` derives the schema and key from the State class. `View.circuit` checks each view's row type against its output collection, derives its output key, and discovers its input streams. Callback arguments are typed expressions, including joined and optional fields.

Supported operators: `where`, `select`, equality `join`, `leftJoin`, `groupBy` with `count`, `sum`, `min`, `max`, and bounded `top` with optional `partitionBy`. Views share integrals and compatible arrangements. Each unit appends at most one change for each collection/key pair; unchanged rows emit nothing. All emitted changes carry the same `txid`.

String comparisons use **lexicographic UTF-16 code-unit ordering**, without locale comparison, case folding or Unicode normalization. This applies to comparisons, extrema and top ordering. Include the unique row key as the last top sort term. State rows have no result-list order; `StateTable.list` orders by its wire keys.

On SQL hosts, run `View.migrate(sql)` at startup and provide `CheckpointStore.layer`, `Transaction.layer`, and `ViewBackend.layerSql` over the same storage layer. For a SQLite host that uses `StateTable`, also run `StateTable.migrate(sql)`. See [Declare and run a circuit](https://streamsy.dev/docs/views/declare-and-run). A changed plan in the same namespace fails with `PlanChanged` before committing. Raise `generation` to replay into new output streams. Older generations remain available for readers and rollback until you stop their runners and call `Projection.retire`.

Before starting an upgraded process, prepare the output addresses and retain the input history. If a circuit used a `version` other than `1`, stop its old runners and follow the reader cutover and retirement steps. [Migrate a declaration that set `version`](https://streamsy.dev/docs/projections/checkpoints#migrate-a-declaration-that-set-version)

To discard progress, pass the binding from `circuit.at(params)` to `Projection.forget`. Forget clears the checkpoint and operator state, keeps the streams, and marks any route records forgotten so retirement can still find them. [Lifecycle and rebuilds](https://streamsy.dev/docs/views/lifecycle)

Set `maxAffectedKeys` on a circuit to limit the distinct relation keys changed in one unit, including intermediate keys. An over-limit unit rolls back and fails with a permanent `ViewDataFault` that identifies a key. The limit is optional.

Unsupported features include `avg`, `distinct`, fact folds, dynamic parameters-as-data, frontier rows and `old_value` on upserts. Parameters bind separate circuit namespaces. Input values must be JSON objects and collection codecs must need no services, as required by Streamsy 0.12 projection declarations. Retain input history for rebuilding.

| Export            | Purpose                                        |
| ----------------- | ---------------------------------------------- |
| `@streamsy/views` | `View`, `ViewBackend`, fault classes and types |
