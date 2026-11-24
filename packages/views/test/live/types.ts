import { CheckpointStore, Transaction } from "@streamsy/projection";
import { Effect, Schema, type Scope, type Stream } from "effect";
import { View, ViewBackend, type CircuitError, type LiveSnapshot } from "../../src/index.ts";
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

type Reader = Effect.Success<typeof open>;

export type LiveTypeChecks = [
  Assert<
    Same<
      Effect.Services<typeof open>,
      Scope.Scope | CheckpointStore | Transaction | ViewBackend | Shop
    >
  >,
  Assert<Same<Effect.Error<typeof open>, CircuitError>>,
  Assert<Same<Reader["caughtUp"], Effect.Effect<void, CircuitError>>>,
];

export function read(handle: Reader) {
  const rows: Effect.Effect<LiveSnapshot<typeof Order.Type>> = handle.rows("orders");

  const totals: Stream.Stream<
    LiveSnapshot<{ customerId: string; orders: number; revenue: number; largest: number }>,
    CircuitError
  > = handle.changes("totals");

  // @ts-expect-error names are inferred from the views
  void handle.rows("missing");
  // @ts-expect-error each view has its own row type
  const wrong: Stream.Stream<LiveSnapshot<typeof Order.Type>, CircuitError> = totals;

  return { rows, totals, wrong };
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
