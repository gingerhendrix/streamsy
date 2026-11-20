import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { View } from "@streamsy/views";

const Order = Schema.Struct({
  orderId: Schema.String,
  customerId: Schema.String,
  total: Schema.Finite,
});

export class Orders extends Streamsy.state<Orders>()("ViewsShop/Orders", "shop/:shopId/orders", {
  params: { shopId: Schema.String },
  collections: { order: { schema: Order, key: "orderId" } },
}) {}

export class ShopViews extends Streamsy.state<ShopViews>()(
  "ViewsShop/Views",
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

export const shopCircuit = (generation: number, minimumTotal?: number) => {
  const orders = View.from(Orders, "order");

  const selected =
    minimumTotal === undefined ? orders : orders.where((row) => row.total.gte(minimumTotal));

  return View.circuit({
    id: "views-shop",
    params: Orders.paramCodecs,
    generation,
    output: Projection.perGeneration(ShopViews, "generation"),
    views: {
      customerTotals: selected
        .groupBy((row) => ({ customerId: row.customerId }))
        .aggregate((row) => ({ orders: View.count(), revenue: View.sum(row.total) })),
      topOrder: selected.top({
        partitionBy: (row) => row.customerId,
        by: (row) => [row.total.desc(), row.orderId.asc()],
        limit: 1,
      }),
    },
  });
};
