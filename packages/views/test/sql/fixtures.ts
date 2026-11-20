import type { Expression, RelationNode, RelationPlan } from "../../src/ir/contracts.ts";

export const ref = (scope: "row" | "left" | "right" | "key", ...path: string[]): Expression => ({
  kind: "reference",
  scope,
  path,
});

export const source = (id: string, key = "id"): RelationNode => ({
  kind: "source",
  id,
  sourceId: id,

  key: ref("row", key),
});

export const plan = (name: string, nodes: readonly RelationNode[]): RelationPlan => ({
  version: 3,
  parameters: [],
  name,
  nodes,
  output: nodes.at(-1)?.id ?? "",
});

type ShopPlans = Readonly<Record<string, RelationPlan>>;

/* oxlint-disable anti-slop/no-conditional-empty-object-spread -- Optional test fixtures add min and leftJoin coverage to the four benchmark views. */
export function shopPlans(minimum = false): ShopPlans {
  const orders = source("orders", "orderId"),
    customers = source("customers", "customerId");

  const joined: RelationNode = {
    kind: "inner-join",
    id: "joined",

    left: "orders",
    right: "customers",
    rightAlias: "customer",
    on: {
      kind: "binary",
      operator: "equal",
      left: ref("left", "customerId"),
      right: ref("right", "customerId"),
    },
  };

  // oxlint-disable-next-line anti-slop/no-known-value-widening -- The fixture intentionally returns the same named plan catalog with optional min/leftJoin coverage.
  return {
    goldOrders: plan("goldOrders", [
      orders,
      customers,
      joined,
      {
        kind: "filter",
        id: "gold",

        input: "joined",
        predicate: {
          kind: "binary",
          operator: "equal",
          left: ref("row", "customer", "tier"),
          right: { kind: "literal", value: "gold" },
        },
      },
      {
        kind: "project",
        id: "selected",

        input: "gold",
        fields: {
          orderId: ref("row", "orderId"),
          customerName: ref("row", "customer", "name"),
          total: ref("row", "total"),
        },
      },
      { kind: "key", id: "keyed", input: "selected", key: ref("row", "orderId") },
    ]),
    totals: plan("totals", [
      orders,
      {
        kind: "grouped-aggregate",
        id: "totals",

        input: "orders",
        groupBy: { customerId: ref("row", "customerId") },
        aggregates: {
          orders: { kind: "aggregate", function: "count" },
          revenue: { kind: "aggregate", function: "sum", expression: ref("row", "total") },
          largest: { kind: "aggregate", function: "max", expression: ref("row", "total") },
          ...(minimum
            ? {
                smallest: {
                  kind: "aggregate" as const,
                  function: "min" as const,
                  expression: ref("row", "total"),
                },
              }
            : {}),
        },
      },
    ]),
    topOrders: plan("topOrders", [
      orders,
      {
        kind: "top-n",
        id: "top",

        input: "orders",
        partitionBy: [ref("row", "customerId")],
        orderBy: [
          { expression: ref("row", "total"), direction: "descending" },
          { expression: ref("row", "orderId"), direction: "ascending" },
        ],
        limit: 3,
      },
    ]),
    revenue: plan("revenue", [
      orders,
      customers,
      joined,
      {
        kind: "grouped-aggregate",
        id: "revenue",

        input: "joined",
        groupBy: { tier: ref("row", "customer", "tier") },
        aggregates: {
          orders: { kind: "aggregate", function: "count" },
          revenue: { kind: "aggregate", function: "sum", expression: ref("row", "total") },
        },
      },
    ]),
    ...(minimum
      ? { left: plan("left", [orders, customers, { ...joined, kind: "left-join", id: "left" }]) }
      : {}),
  };
}
