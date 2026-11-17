import { ChatRequest, Id, responsePath, StopRequest } from "@streamsy/durable-response-contract";
import { Serve } from "@streamsy/serve";
import { Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { Generations } from "./generations.ts";
import { ModelStats } from "./model.ts";
import { Responses } from "./streams.ts";

const json = HttpServerResponse.jsonUnsafe;

const bad = () => json({ error: "Invalid request" }, { status: 400 });

const requestBody = <A, I>(schema: Schema.Codec<A, I>) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;

    return yield* request.json.pipe(Effect.flatMap(Schema.decodeUnknownEffect(schema)));
  });

const routeId = (key: string) =>
  Effect.gen(function* () {
    const { params } = yield* HttpRouter.RouteContext;

    return yield* Schema.decodeUnknownEffect(Id)(params[key]);
  });

const chat = HttpRouter.add(
  "POST",
  "/chat",
  Effect.gen(function* () {
    const input = yield* requestBody(ChatRequest);
    const created = yield* (yield* Generations).start(input);

    return json(created, { status: 201, headers: { location: responsePath(created.id) } });
  }).pipe(
    Effect.catchTags({
      SchemaError: () => Effect.succeed(bad()),
      HttpServerError: () => Effect.succeed(bad()),
    }),
  ),
);

const stop = HttpRouter.add(
  "POST",
  "/responses/:id/stop",
  Effect.gen(function* () {
    const id = yield* routeId("id");
    const input = yield* requestBody(StopRequest);

    return json(yield* (yield* Generations).stop(id, input));
  }).pipe(
    Effect.catchTags({
      SchemaError: () => Effect.succeed(bad()),
      HttpServerError: () => Effect.succeed(bad()),
      StreamNotFound: () => Effect.succeed(json({ error: "Response not found" }, { status: 404 })),
    }),
  ),
);

const probe = HttpRouter.add(
  "GET",
  "/chat/:chatId/stream",
  Effect.gen(function* () {
    const chatId = yield* routeId("chatId");
    const id = yield* (yield* Generations).active(chatId);

    return id
      ? json({ id }, { headers: { location: responsePath(id) } })
      : HttpServerResponse.empty({ status: 204 });
  }).pipe(Effect.catchTag("SchemaError", () => Effect.succeed(bad()))),
);

const debug = HttpRouter.add(
  "GET",
  "/debug/model-calls",
  Effect.map(ModelStats, (stats) => stats.read).pipe(
    Effect.flatten,
    Effect.map((stats) => json(stats)),
  ),
);

export const routes = Layer.mergeAll(
  chat,
  stop,
  probe,
  debug,
  Serve.mount(Responses, "/responses/:id"),
);
