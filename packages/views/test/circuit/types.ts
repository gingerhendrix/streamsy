import { Projection } from "@streamsy/projection";
import { Streamsy } from "@streamsy/core";
import { Effect, Schema } from "effect";
import { View } from "../../src/index.ts";
import { orders, Shop, Results, circuit, views } from "./support/shop.ts";

const declaration = circuit();

declaration.at({ shop: "a" });

Projection.attach(declaration, { durable: true });

void Projection.forget(declaration.at({ shop: "a" }));

void Projection.retire(declaration, { shop: "a" }, { generation: 1 });

// @ts-expect-error a binding requires the declared params
void declaration.at({});

// @ts-expect-error unknown input collection
void View.from(Shop, "missing");

const config = {
  id: "bad",
  params: Shop.paramCodecs,
  output: Projection.perGeneration(Results, "generation"),
};

const wrongRow = { ...config, views: { goldOrder: orders } };

// @ts-expect-error the output row lacks the customerName field
View.circuit(wrongRow);

const wrongCollection = { ...config, views: { missing: orders } };

// @ts-expect-error unknown output collection
View.circuit(wrongCollection);

const missingKey = orders.select((row) => ({ customerId: row.customerId, total: row.total }));

const wrongKey = { ...config, views: { topOrder: missingKey } };

// @ts-expect-error the output key is required by its row schema
View.circuit(wrongKey);

const conflictingGeneration = { ...config, params: Results.paramCodecs, views };

// @ts-expect-error the generation-bound output parameter is not a circuit parameter
View.circuit(conflictingGeneration);

const missingShop = { ...config, params: {}, views: { topOrder: orders } };

// @ts-expect-error State inputs and output require a matching shop parameter
View.circuit(missingShop);

View.circuit({
  id: "bad",
  params: { shop: Schema.NumberFromString },
  // @ts-expect-error route parameter codecs must have the same decoded type
  output: Projection.perGeneration(Results, "generation"),
  // @ts-expect-error input route parameter codecs must also agree
  views: { topOrder: orders },
});

const retry = View.circuit({
  id: "with-retry",
  params: { shop: Schema.String },
  output: Projection.perGeneration(Results, "generation"),
  views,
  retry: { maxAttempts: 3, onExhausted: () => Effect.fail("reported") },
});

Projection.attach(retry, { durable: true });

// @ts-expect-error a circuit run needs its stream and checkpoint services
void Effect.runPromise(Projection.run(declaration.at({ shop: "a" })));

const routedOutput = Projection.perGeneration(Results, "generation", { routeBy: ["shop"] });

const routedOrders = View.routed(orders, { by: () => ({ shop: View.literal("all") }) });

const inputsRequireShop = {
  id: "routed-with-input-param",
  params: { shop: Schema.String },
  output: routedOutput,
  views: { topOrder: routedOrders },
};

// @ts-expect-error A parameter is either routed or bound.
View.circuit(inputsRequireShop);

const unboundOrders = View.from(
  // A parameterless State class makes the routed output's param checks independent of input params.
  class AllOrders extends Streamsy.state<AllOrders>()("ViewTypes/AllOrders", "all-orders", {
    params: {},
    collections: Shop.collections,
  }) {},
  "order",
);

const routed = { id: "routed", params: {}, output: routedOutput };

View.circuit({
  ...routed,
  views: { topOrder: View.routed(unboundOrders, { by: () => ({ shop: View.literal("all") }) }) },
});

View.circuit({
  ...routed,
  // @ts-expect-error Every collection of a routed output needs explicit route expressions.
  views: { topOrder: unboundOrders },
});

View.circuit({
  ...routed,
  // @ts-expect-error Route expressions must match the decoded route types.
  views: { topOrder: View.routed(unboundOrders, { by: () => ({ shop: View.literal(1) }) }) },
});

View.circuit({
  ...routed,
  views: {
    // @ts-expect-error Route expressions must supply exactly the routed parameter names.
    topOrder: View.routed(unboundOrders, {
      by: () => ({ shop: View.literal("all"), extra: View.literal(1) }),
    }),
  },
});
