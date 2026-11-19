import { Projection } from "@streamsy/projection";
import { Effect, Schema } from "effect";
import { View } from "../../src/relation.ts";
import { orders, Shop, Results, circuit, views } from "./support/shop.ts";

const declaration = circuit();

declaration.at({ shop: "a" });

Projection.attach(declaration, { durable: true });

void Projection.forget(declaration.at({ shop: "a" }));

void View.forget(declaration.at({ shop: "a" }));

// @ts-expect-error a binding requires the declared params
void declaration.at({});

// @ts-expect-error unknown input collection
void View.from(Shop, "missing");

View.circuit({
  id: "bad",
  params: { shop: Schema.String },
  output: Results,
  // @ts-expect-error the output row lacks the customerName field
  views: { goldOrder: orders },
});

View.circuit({
  id: "bad",
  params: { shop: Schema.String },
  output: Results,
  // @ts-expect-error unknown output collection
  views: { missing: orders },
});

View.circuit({
  id: "bad",
  params: { shop: Schema.String },
  output: Results,
  // @ts-expect-error the output key is required by its row schema
  views: { topOrder: orders.select((row) => ({ customerId: row.customerId, total: row.total })) },
});

// @ts-expect-error State inputs and output require a matching shop parameter
View.circuit({ id: "bad", params: {}, output: Results, views: { topOrder: orders } });

View.circuit({
  id: "bad",
  params: { shop: Schema.NumberFromString },
  // @ts-expect-error route parameter codecs must have the same decoded type
  output: Results,
  // @ts-expect-error input route parameter codecs must also agree
  views: { topOrder: orders },
});

const retry = View.circuit({
  id: "with-retry",
  params: { shop: Schema.String },
  output: Results,
  views,
  retry: { maxAttempts: 3, onExhausted: () => Effect.fail("reported") },
});

Projection.attach(retry, { durable: true });

// @ts-expect-error a circuit run needs its stream and checkpoint services
void Effect.runPromise(Projection.run(declaration.at({ shop: "a" })));
