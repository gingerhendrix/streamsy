import { Streamsy } from "@streamsy/core";
import { StateTable } from "@streamsy/projection";
import { Schema } from "effect";
import { View } from "../../../src/index.ts";

export const Order = Schema.Struct({
  orderId: Schema.String,
  customerId: Schema.String,
  total: Schema.Finite,
});

export const Customer = Schema.Struct({
  customerId: Schema.String,
  name: Schema.String,
  tier: Schema.String,
});

export class Shop extends Streamsy.state<Shop>()("ViewsTest/Shop", "shop/:shop", {
  params: { shop: Schema.String },
  collections: {
    order: { schema: Order, key: "orderId" },
    customer: { schema: Customer, key: "customerId" },
  },
}) {}

export class Results extends Streamsy.state<Results>()("ViewsTest/Results", "results/:shop", {
  params: { shop: Schema.String },
  collections: {
    goldOrder: {
      schema: Schema.Struct({
        orderId: Schema.String,
        customerName: Schema.String,
        total: Schema.Finite,
      }),
      key: "orderId",
    },
    customerTotals: {
      schema: Schema.Struct({
        customerId: Schema.String,
        orders: Schema.Finite,
        revenue: Schema.Finite,
        largest: Schema.Finite,
      }),
      key: "customerId",
    },
    topOrder: { schema: Order, key: "orderId" },
  },
}) {}

export const orders = View.from(Shop, "order");

export const customers = View.from(Shop, "customer");

export const goldOrders = orders
  .join(customers, {
    as: "customer",
    on: (order, customer) => order.customerId.eq(customer.customerId),
  })
  .where((row) => row.customer.tier.eq("gold"))
  .select((row) => ({ orderId: row.orderId, customerName: row.customer.name, total: row.total }));

export const customerTotals = orders
  .groupBy((order) => ({ customerId: order.customerId }))
  .aggregate((order) => ({
    orders: View.count(),
    revenue: View.sum(order.total),
    largest: View.max(order.total),
  }));

export const topOrders = orders.top({
  partitionBy: (order) => order.customerId,
  by: (order) => [order.total.desc(), order.orderId.asc()],
  limit: 3,
});

export const views = { goldOrder: goldOrders, customerTotals, topOrder: topOrders };

export const circuit = (generation = 1, changed = false, maxAffectedKeys?: number) =>
  View.circuit({
    id: "shop-views",
    params: { shop: Schema.String },
    output: Results,
    generation,
    maxAffectedKeys,
    views: {
      ...views,
      goldOrder: changed ? goldOrders.where((row) => row.total.gt(100)) : goldOrders,
    },
  });

export const Table = StateTable.define(Results);

export const group = Streamsy.group(Shop, Results);
