import { Http } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { Effect, Layer } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import {
  newestLimit,
  pollIntervalMs,
  projectionLimits,
  sourceStreamPath,
  streamPath,
  streamPrefix,
  streamContentType,
} from "./config.ts";
import { NewestStoriesPoller } from "./poller/contract.ts";
import { StoryProjection } from "./projection.ts";
import { serveStatic } from "./static.ts";
import { hackerNewsStoryIndex } from "./story-index-projection.ts";

const currentStats = Effect.gen(function* () {
  const poller = yield* NewestStoriesPoller;
  const projection = yield* StoryProjection;
  return { ...(yield* poller.stats), projection: yield* projection.status };
});

export const stateRoute = Serve.state(hackerNewsStoryIndex.outputs["hn-story"], streamPath);

/** The page and State route share an origin, so no CORS middleware is needed. */
export const app = Layer.mergeAll(
  stateRoute,
  Http.routes({ prefix: streamPrefix }),
  HttpRouter.add(
    "GET",
    "/api/status",
    Effect.gen(function* () {
      return HttpServerResponse.jsonUnsafe(
        {
          streamPath,
          sourceStreamPath,
          newestLimit,
          pollIntervalMs,
          projectionLimits,
          ...(yield* currentStats),
        },
        { headers: { "content-type": streamContentType } },
      );
    }),
  ),
  HttpRouter.add(
    "POST",
    "/api/poll",
    Effect.gen(function* () {
      yield* (yield* NewestStoriesPoller).pollNow;
      return HttpServerResponse.jsonUnsafe(
        { ok: true, ...(yield* currentStats) },
        { headers: { "content-type": streamContentType } },
      );
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
