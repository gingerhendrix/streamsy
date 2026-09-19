import { Effect, Exit, Layer, Scope } from "effect";
import type { StorageFault } from "@streamsy/core";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import * as Host from "@streamsy/serve/alchemy";
type Services = Exclude<Effect.Services<typeof Host.fetch>, HttpServerRequest.HttpServerRequest>;

/** Application-owned construction: one Layer, one lazy successful build per object. */
export const objectHandlers = (layer: Layer.Layer<Services, StorageFault>) =>
  Effect.gen(function* () {
    const owner = yield* Scope.Scope;
    const services = yield* Effect.gen(function* () {
      const attempt = yield* Scope.fork(owner);
      return yield* Layer.buildWithScope(layer, attempt).pipe(
        Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(attempt, exit) : Effect.void)),
      );
    }).pipe(Effect.cachedWithTTL((exit) => (Exit.isSuccess(exit) ? "Infinity" : 0)));
    return {
      fetch: Effect.flatMap(services, (context) => Host.fetch.pipe(Effect.provide(context))).pipe(
        Effect.catchTag("StorageFault", () =>
          Effect.succeed(
            HttpServerResponse.text("Storage unavailable", {
              status: 503,
              headers: {
                "retry-after": "1",
                "x-content-type-options": "nosniff",
                "cross-origin-resource-policy": "cross-origin",
              },
            }),
          ),
        ),
      ),
      alarm: () =>
        Effect.flatMap(services, (context) => Host.alarm.pipe(Effect.provide(context))).pipe(
          Effect.orDie,
        ),
    };
  });
