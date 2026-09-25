import { Protocol } from "@streamsy/core/protocol";
import { Streamsy } from "@streamsy/core";
import { Context, Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Streams } from "@streamsy/core/protocol";
import { listener } from "../src/bun.ts";
import { Serve } from "../src/index.ts";
class stream extends Streamsy.json<stream>()("serve/serve.type-test/stream", "events/:seat", {
  params: { seat: Schema.String },
  schema: Schema.String,
}) {}
class state extends Streamsy.state<state>()("serve/serve.type-test/state", "rows/:seat", {
  params: { seat: Schema.String },
  collections: { rows: { schema: Schema.Struct({ id: Schema.String }), key: "id" } },
}) {}
Serve.stream(stream, "/feed/:seat");
Serve.state(state, "/rows/:seat");
// @ts-expect-error wrong parameter
Serve.stream(stream, "/feed/:wrong");
// @ts-expect-error missing parameter
Serve.stream(stream, "/feed");
// @ts-expect-error extra parameter
Serve.stream(stream, "/feed/:seat/:extra");
// @ts-expect-error optional parameters are not supported
Serve.stream(stream, "/feed/:seat?");
// @ts-expect-error state requires collections
Serve.state(stream, "/rows/:seat");
// @ts-expect-error NoInfer prevents params from widening the stream
Serve.stream(stream, "/me", { params: Effect.succeed({ seat: 1 }) });
class Seat extends Context.Service<Seat, { readonly seat: string }>()("Seat") {}
const mine = Serve.stream(stream, "/me", { params: Seat });
const protocol = Streams.layerMemory();
const storage = Protocol.bind(protocol, Streamsy.group(stream, state)).pipe(
  Layer.provideMerge(protocol),
);
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
