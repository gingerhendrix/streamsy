import { Serve } from "@streamsy/serve";
import { Effect, Layer } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { newestLimit, pollIntervalMs, streamPath } from "./config.ts";
import { NewestStoriesPoller } from "./poller/contract.ts";
import { serveStatic } from "./static.ts";
import { HackerNewsStories } from "./stories.ts";

const currentStats = Effect.gen(function* () {
  const poller = yield* NewestStoriesPoller;
  return yield* poller.stats;
});

export const stateRoute = Serve.family(HackerNewsStories, streamPath);

/** The page and State route share an origin, so no CORS middleware is needed. */
export const app = Layer.mergeAll(
  stateRoute,
  HttpRouter.add(
    "GET",
    "/api/status",
    Effect.gen(function* () {
      return HttpServerResponse.jsonUnsafe({
        streamPath,
        newestLimit,
        pollIntervalMs,
        ...(yield* currentStats),
      });
    }),
  ),
  HttpRouter.add(
    "POST",
    "/api/poll",
    Effect.gen(function* () {
      yield* (yield* NewestStoriesPoller).pollNow;
      return HttpServerResponse.jsonUnsafe({ ok: true, ...(yield* currentStats) });
    }),
  ),
  HttpRouter.add(
    "*",
    "/api/*",
    HttpServerResponse.jsonUnsafe({ error: "Not found" }, { status: 404 }),
  ),
  HttpRouter.add(
    "GET",
    "/*",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return HttpServerResponse.fromWeb(
        yield* Effect.promise(() => serveStatic(new URL(request.url, "http://localhost"))),
      );
    }),
  ),
);
