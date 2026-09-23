import { Effect } from "effect";
import { Protocol } from "@streamsy/core";
import { reconcileAlarm } from "./alarm.ts";

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

/** A platform alarm runs the sweep directly and leaves failures to platform retry. */
export const alarm = Protocol.expireDue().pipe(Effect.andThen(reconcileAlarm()), Effect.asVoid);
