import { Effect, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { StreamRef, Streams } from "@streamsy/core";
import * as Fetch from "@streamsy/core/fetch";

const orders = StreamRef.json("orders", { schema: Schema.Struct({ id: Schema.String }) });
export const remote = Fetch.layer({
  baseUrl: "https://streams.example.com/streams",
}).pipe(Layer.provide(FetchHttpClient.layer));

export const program = Effect.gen(function* () {
  yield* Streams.create(orders);

  return yield* Streams.append(orders, [{ id: "order-1" }]);
}).pipe(Effect.provide(remote));
