import { Effect, Schema, type Scope, type Stream } from "effect";
import { View, type CircuitError, type LiveReader, type LiveSnapshot } from "../../src/index.ts";
import { customerTotals, orders, Shop, type Order } from "../circuit/support/shop.ts";

const declaration = View.live({
  id: "live-types",
  params: { ...Shop.paramCodecs, minimum: Schema.FiniteFromString },
  views: ({ minimum }) => ({
    orders: orders.where((row) => row.total.gt(minimum)),
    totals: customerTotals,
  }),
});

const open = declaration.at({ shop: "typed", minimum: 2 });

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type Assert<T extends true> = T;

type Views = { orders: typeof orders; totals: typeof customerTotals };

type Snapshot = LiveSnapshot<Views>;

export type LiveTypeChecks = [
  Assert<Same<Effect.Services<typeof open>, Scope.Scope | Shop>>,
  Assert<Same<Effect.Error<typeof open>, CircuitError>>,
  Assert<Same<Effect.Success<typeof open>, LiveReader<Views>>>,
  Assert<Same<LiveReader<Views>["snapshot"], Effect.Effect<Snapshot>>>,
  Assert<Same<LiveReader<Views>["snapshots"], Stream.Stream<Snapshot, CircuitError>>>,
  Assert<Same<LiveReader<Views>["caughtUp"], Effect.Effect<void, CircuitError>>>,
  Assert<Same<Snapshot["rows"]["orders"], ReadonlyArray<typeof Order.Type>>>,
  Assert<
    Same<
      Snapshot["rows"]["totals"],
      ReadonlyArray<{
        customerId: string;
        orders: number;
        revenue: number;
        largest: number;
      }>
    >
  >,
];

export function read({ rows }: Snapshot) {
  // @ts-expect-error names are inferred from the views
  return rows.missing;
}

// @ts-expect-error the Scope and page services are mandatory
void Effect.runPromise(open);

// @ts-expect-error decoded numeric parameter required
void declaration.at({ shop: "typed", minimum: "2" });

// @ts-expect-error required parameter
void declaration.at({ shop: "typed" });

const missingParam = { id: "missing-param", params: {}, views: { orders } };

// @ts-expect-error the input's shop parameter must be present
View.live(missingParam);

const invalidParam = {
  id: "invalid-param",
  params: { shop: Schema.FiniteFromString },
  views: { orders },
};

// @ts-expect-error the input's shop parameter is a string
View.live(invalidParam);

const routed = View.routed(orders, { by: ({ customerId }) => ({ customerId }) });

View.live({
  id: "routing-not-supported",
  params: Shop.paramCodecs,
  // @ts-expect-error live views have no routed output
  views: { routed },
});
