import { Effect, Option } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Protocol } from "@streamsy/core";
import type { ObjectOptions } from "./object-options.ts";
import { program } from "@streamsy/core/http";
import { reconcileAlarm } from "./alarm.ts";
import { HostCommand } from "./host-command.ts";

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

export const hostProgram = (options: ObjectOptions) =>
  Effect.gen(function* () {
    const command = yield* Effect.serviceOption(HostCommand);
    if (Option.isSome(command)) {
      return yield* Effect.gen(function* () {
        yield* Protocol.expireDue();
        yield* reconcileAlarm();
        return HttpServerResponse.empty({ status: 204 });
      }).pipe(Effect.catchTag("StorageFault", () => Effect.succeed(internalError())));
    }

    const request = yield* HttpServerRequest.HttpServerRequest;
    const ordinary = mutates(request.method)
      ? withMutationReconciliation(program(options), reconcileAlarm())
      : program(options);
    return yield* ordinary.pipe(Effect.catchDefect(() => Effect.succeed(internalError())));
  });
