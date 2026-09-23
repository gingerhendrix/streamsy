import { Context, Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { StreamRoute, Streams } from "@streamsy/core";
import { listener } from "../src/bun.ts";
import { Serve } from "../src/index.ts";
const family = StreamRoute.json("events/:seat", {
  params: { seat: Schema.String },
  schema: Schema.String,
});
const state = StreamRoute.state("rows/:seat", {
  params: { seat: Schema.String },
  collections: { rows: { schema: Schema.Struct({ id: Schema.String }), key: "id" } },
});
Serve.stream(family, "/feed/:seat");
Serve.state(state, "/rows/:seat");
// @ts-expect-error wrong parameter
Serve.stream(family, "/feed/:wrong");
// @ts-expect-error missing parameter
Serve.stream(family, "/feed");
// @ts-expect-error extra parameter
Serve.stream(family, "/feed/:seat/:extra");
// @ts-expect-error optional parameters are not supported
Serve.stream(family, "/feed/:seat?");
// @ts-expect-error state requires collections
Serve.state(family, "/rows/:seat");
// @ts-expect-error NoInfer prevents params from widening the family
Serve.stream(family, "/me", { params: Effect.succeed({ seat: 1 }) });
class Seat extends Context.Service<Seat, { readonly seat: string }>()("Seat") {}
const mine = Serve.stream(family, "/me", { params: Seat });
const storage = Streams.layerMemory();
const unsafe = HttpRouter.toWebHandler(mine.pipe(HttpRouter.provideRequest(storage)));
// @ts-expect-error no auth supplied the seat; handler requires a context
void unsafe.handler(new Request("http://host/me"));
const auth = HttpRouter.middleware<{ provides: Seat }>()((handler) =>
  Effect.flatMap(HttpServerRequest.HttpServerRequest, (request) =>
    request.headers.authorization
      ? Effect.provideService(handler, Seat, { seat: "one" })
      : Effect.succeed(HttpServerResponse.empty({ status: 401 })),
  ),
);
const safe = HttpRouter.toWebHandler(
  mine.pipe(Layer.provide(auth.layer), HttpRouter.provideRequest(storage)),
);
void safe.handler(new Request("http://host/me"));
const missingSeat = Layer.launch(
  HttpRouter.serve(mine).pipe(Layer.provide(storage), Layer.provide(listener({ port: 0 }))),
);
// @ts-expect-error a runnable server still needs the seat
void Effect.runPromise(missingSeat);
