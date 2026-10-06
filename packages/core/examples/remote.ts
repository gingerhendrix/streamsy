import { Effect, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
import { Streamsy } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";

class Orders extends Streamsy.json<Orders>()("Remote/Orders", "orders", {
  params: {},
  schema: Schema.Struct({ id: Schema.String }),
}) {}
export const remote = RemoteStreams.layer(Streamsy.group(Orders), {
  baseUrl: "https://streams.example.com/streams",
}).pipe(Layer.provide(FetchHttpClient.layer));
export const program = Effect.gen(function* () {
  const orders = yield* Orders;
  yield* orders.create({});
  return yield* orders.append({}, [{ id: "order-1" }]);
}).pipe(Effect.provide(remote));
