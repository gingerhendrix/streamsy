import { Context, Effect, Exit, Layer, Scope } from "effect";
import {
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
  type HttpServerRespondable,
} from "effect/http";
import { Http, type Storage, type StorageFault } from "@streamsy/core";
import { AlarmSlot } from "./alarm.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- This private composition root acquires the scheduler with the object context before compiling either host.
import { Wakeups, RequestTouch, makeWakeups, type Wakeup } from "./wakeup.ts";
import { withMutationReconciliation } from "./host-program.ts";

export type ObjectServices<R = never> = Storage | Wakeups | R;

export type ObjectApp<R = never> = Layer.Layer<
  never,
  never,
  | HttpRouter.HttpRouter
  | HttpRouter.Request.From<"Requires", ObjectServices<R>>
  | HttpRouter.Request.From<"Error", HttpServerRespondable.Respondable>
>;

/** Both object hosts install this middleware, including around user routes. */
function objectApp<R>(app: ObjectApp<R>, context: Context.Context<ObjectServices<R>>) {
  const middleware = HttpRouter.middleware(
    (handler) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;

        const marker = { touched: false };
        const wakeups = Context.get(context, Wakeups);

        const guarded = withMutationReconciliation(
          handler,
          Effect.suspend(() =>
            ["PUT", "POST", "DELETE"].includes(request.method) || marker.touched
              ? wakeups.reconcile
              : Effect.void,
          ),
        );

        return yield* guarded.pipe(
          Effect.catchDefect(() =>
            Effect.succeed(
              HttpServerResponse.text("Internal server error", {
                status: 500,
                headers: Http.securityHeaders,
              }),
            ),
          ),
          Effect.provideService(RequestTouch, marker),
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
  layer: Layer.Layer<Storage | AlarmSlot | R, StorageFault>,
  compile: (
    context: Context.Context<ObjectServices<R>>,
    alarm: Effect.Effect<void>,
  ) => Effect.Effect<A, never, Scope.Scope>,
  wakeups?: ReadonlyArray<Wakeup<ObjectServices<R>>>,
): Effect.Effect<Effect.Effect<A, StorageFault>, never, Scope.Scope> {
  return Effect.gen(function* () {
    const owner = yield* Scope.Scope;

    return yield* Effect.gen(function* () {
      const attempt = yield* Scope.fork(owner);

      return yield* Effect.gen(function* () {
        const context = yield* Layer.buildWithScope(layer, attempt);

        const scheduler = yield* makeWakeups(context, wakeups);

        return yield* compile(scheduler.context, scheduler.alarm).pipe(
          Effect.provideService(Scope.Scope, attempt),
        );
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
): Layer.Layer<
  never,
  never,
  HttpRouter.HttpRouter | HttpRouter.Request.From<"Error", HttpServerRespondable.Respondable>
> {
  const provided = objectApp(app, context).pipe(
    HttpRouter.provideRequest(Layer.succeedContext(context)),
  );

  // SAFETY: ObjectApp only requires the services carried by ObjectServices<R>;
  // provideRequest supplies that complete context. The assertion resolves generic conditional request markers.
  // oxlint-disable-next-line effecttsgo/unsafe-effect-type-assertion, typescript/no-unsafe-type-assertion -- provideRequest supplies every service that the generic request markers name; see SAFETY.
  return provided as Layer.Layer<
    never,
    never,
    HttpRouter.HttpRouter | HttpRouter.Request.From<"Error", HttpServerRespondable.Respondable>
  >;
}
