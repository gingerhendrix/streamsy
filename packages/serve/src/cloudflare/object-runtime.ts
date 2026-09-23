import { Context, Effect, Exit, Layer, Scope } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import {
  Http,
  type Storage,
  type StorageFault,
  type StreamsReader,
  type StreamsWriter,
} from "@streamsy/core";
import { Alarm, reconcileAlarm } from "./alarm.ts";
import { withMutationReconciliation } from "./host-program.ts";

export type ObjectServices<R = never> = StreamsReader | StreamsWriter | Storage | Alarm | R;
export type ObjectApp<R = never> = Layer.Layer<
  never,
  never,
  HttpRouter.HttpRouter | HttpRouter.Request.From<"Requires", ObjectServices<R>>
>;

/** Both object hosts install this middleware, including around user routes. */
function objectApp<R>(app: ObjectApp<R>, context: Context.Context<ObjectServices<R>>) {
  const middleware = HttpRouter.middleware(
    (handler) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const guarded = ["PUT", "POST", "DELETE"].includes(request.method)
          ? withMutationReconciliation(handler, reconcileAlarm())
          : handler;
        return yield* guarded.pipe(
          Effect.catchDefect(() =>
            Effect.succeed(
              HttpServerResponse.text("Internal server error", {
                status: 500,
                headers: Http.securityHeaders,
              }),
            ),
          ),
        );
      }).pipe(Effect.provide(context)),
    { global: true },
  );
  return Layer.merge(app, middleware);
}

export const unavailable = () =>
  HttpServerResponse.text("Storage unavailable", {
    status: 503,
    headers: { ...Http.securityHeaders, "retry-after": "1" },
  });

/** One lazy successful acquisition per object owner. Failed attempts close before retry. */
export function acquireObject<A, R>(
  layer: Layer.Layer<ObjectServices<R>, StorageFault>,
  compile: (context: Context.Context<ObjectServices<R>>) => Effect.Effect<A, never, Scope.Scope>,
): Effect.Effect<Effect.Effect<A, StorageFault>, never, Scope.Scope> {
  return Effect.gen(function* () {
    const owner = yield* Scope.Scope;
    return yield* Effect.gen(function* () {
      const attempt = yield* Scope.fork(owner);
      return yield* Effect.gen(function* () {
        const context = yield* Layer.buildWithScope(layer, attempt);
        return yield* compile(context).pipe(Effect.provideService(Scope.Scope, attempt));
      }).pipe(
        Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(attempt, exit) : Effect.void)),
      );
    }).pipe(Effect.cachedWithTTL((exit) => (Exit.isSuccess(exit) ? "Infinity" : 0)));
  });
}

/** Request markers are all supplied by this exact object context. */
export function providedApp<R>(
  app: ObjectApp<R>,
  context: Context.Context<ObjectServices<R>>,
): Layer.Layer<never, never, HttpRouter.HttpRouter> {
  // SAFETY: ObjectApp only requires the services carried by ObjectServices<R>;
  // provideRequest supplies that complete context. The assertion resolves generic conditional request markers.
  return objectApp(app, context).pipe(
    HttpRouter.provideRequest(Layer.succeedContext(context)),
  ) as Layer.Layer<never, never, HttpRouter.HttpRouter>;
}
