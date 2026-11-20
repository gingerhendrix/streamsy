# @streamsy/views 0.5

Incrementally maintain Streamsy State collections on SQLite. A circuit is an ordinary projection: it consumes input pages, updates indexed operator state, appends changed output rows and commits its input checkpoint in one transaction.

This private workspace package uses Streamsy's local core and projection packages and Effect 4.1.2. Runs on Bun SQLite and Durable Object SQLite. See the [guide](docs/guide.md) for host setup, lifecycle and checks.

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

class ShopViews extends Streamsy.state<ShopViews>()("Shop/Views", "shop/:shopId/views", {
  params: { shopId: Schema.String },
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
}) {}

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
  output: ShopViews,
  views: { customerTotals, topOrder: topOrders },
});

const bound = shopViews.at({ shopId: "main" });
const attached = Projection.attach(shopViews, { durable: true });
const ShopViewsTable = StateTable.define(ShopViews);
```

`View.from` derives the schema and key from the State class. `View.circuit` checks each view's row type against its output collection, derives its output key, and discovers its input streams. Callback arguments are typed expressions, including joined and optional fields.

Supported operators: `where`, `select`, equality `join`, `leftJoin`, `groupBy` with `count`, `sum`, `min`, `max`, and bounded `top` with optional `partitionBy`. Views share integrals and compatible arrangements. Each unit appends at most one change for each collection/key pair; unchanged rows emit nothing. All emitted changes carry the same `txid`.

String comparisons use **lexicographic UTF-16 code-unit ordering**, without locale comparison, case folding or Unicode normalization. This applies to comparisons, extrema and top ordering. Include the unique row key as the last top sort term. State rows have no result-list order; `StateTable.list` orders by its wire keys.

Call `View.migrate(sql)` once at startup. A changed plan in the same namespace fails with `View.PlanChanged` before committing. Raise `generation` to rebuild; the first unit removes older operator namespaces for that projection key. Stop older runners first and delete/reset output streams if the rebuild must start with empty outputs. `Projection.forget(bound)` deletes the checkpoint and operator state atomically; `View.forget` is an alias. Neither removes output streams.

Set `maxAffectedKeys` on a circuit to limit the distinct relation keys changed in one unit, including intermediate keys. An over-limit unit rolls back and fails with a permanent `ViewDataFault` that identifies a key. The limit is optional.

Unsupported features include `avg`, `distinct`, fact folds, closure, the memory tier, dynamic parameters-as-data, frontier rows and `old_value` on upserts. Parameters bind separate circuit namespaces. Input values must be JSON objects and collection codecs must need no services, as required by Streamsy 0.12 projection declarations. Only SQLite is supported. Retain input history for rebuilding.

| Export            | Purpose                         |
| ----------------- | ------------------------------- |
| `@streamsy/views` | `View`, fault classes and types |
