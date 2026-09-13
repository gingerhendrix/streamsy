import * as Cloudflare from "alchemy/Cloudflare";
import { Effect, Exit, Layer, Scope } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import * as Host from "@streamsy/serve/alchemy";
import { layerProtocol } from "@streamsy/storage/durable-object";
import {
  COMPATIBILITY_DATE,
  COMPATIBILITY_FLAGS,
  CONFORMANCE_LONG_POLL_TIMEOUT_MS,
} from "./contract.ts";

type ObjectServices = Exclude<
  Effect.Services<typeof Host.fetch>,
  HttpServerRequest.HttpServerRequest
>;

/** Application-owned construction: one lazy successful Layer build per object instance. */
const objectHandlers = (layer: Layer.Layer<ObjectServices, Effect.Error<typeof Host.alarm>>) =>
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

/** The runtime phase alone reads raw Durable Object storage. */
export class StreamsObject extends Cloudflare.DurableObject<StreamsObject>()(
  "Streams",
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    return Effect.suspend(() =>
      objectHandlers(
        Layer.mergeAll(
          layerProtocol({
            client: { storage: state.raw.storage },
            longPollTimeoutMs: CONFORMANCE_LONG_POLL_TIMEOUT_MS,
          }),
          Layer.succeed(Host.ObjectOptions, { pathPrefix: "/streams" }),
          Host.alarmLayer(state.raw.storage),
        ),
      ),
    );
  }),
) {}

const worker = Effect.gen(function* () {
  const objects = yield* StreamsObject;
  return {
    fetch: Host.router({
      objects,
      pathPrefix: "/streams",
      placement: Host.Placement.byKey(() => "conformance"),
    }),
  };
});

/** Effect-native source form selected by alchemy.source.run.ts. */
export const sourceWorker = (name: string) =>
  Cloudflare.Worker(
    "Server",
    {
      name,
      main: import.meta.url,
      compatibility: { date: COMPATIBILITY_DATE, flags: [...COMPATIBILITY_FLAGS] },
      workersDev: true,
    },
    worker,
  );
