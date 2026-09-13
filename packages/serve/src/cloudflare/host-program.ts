import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Protocol } from "@streamsy/core";
import { ObjectOptions } from "./object-options.ts";
import { app } from "@streamsy/core/http";
import { reconcileAlarm } from "./alarm.ts";

const mutates = (method: string): boolean =>
  method === "PUT" || method === "POST" || method === "DELETE";

const securityHeaders = {
  "x-content-type-options": "nosniff",
  "cross-origin-resource-policy": "cross-origin",
};

const internalError = () =>
  HttpServerResponse.setHeaders(
    HttpServerResponse.text("Internal server error", { status: 500 }),
    securityHeaders,
  );

/**
 * Reconciliation belongs to the mutation's finalizer. A request interruption
 * can be pending when the mutation's own uninterruptible region ends, so a
 * second sequential uninterruptible effect is not sufficient.
 */
export const withMutationReconciliation = <A, E, R, E2, R2>(
  effect: Effect.Effect<A, E, R>,
  reconcile: Effect.Effect<void, E2, R2>,
): Effect.Effect<A, E, R | R2> =>
  effect.pipe(Effect.ensuring(reconcile.pipe(Effect.uninterruptible, Effect.ignoreCause)));

/** The request effect; expiry is never selected by request content. */
export const fetch = Effect.gen(function* () {
  const options = yield* ObjectOptions;
  const request = yield* HttpServerRequest.HttpServerRequest;
  const ordinary = mutates(request.method)
    ? withMutationReconciliation(app(options), reconcileAlarm())
    : app(options);
  return yield* ordinary.pipe(Effect.catchDefect(() => Effect.succeed(internalError())));
});

/** A platform alarm runs the sweep directly and leaves failures to platform retry. */
export const alarm = Protocol.expireDue().pipe(Effect.andThen(reconcileAlarm()), Effect.asVoid);
