import { Effect } from "effect";

/**
 * Reconciliation belongs to the mutation's finalizer. A request interruption
 * can be pending when the mutation's own uninterruptible region ends, so a
 * second sequential uninterruptible effect is not sufficient. A failed
 * reconciliation is logged and does not replace the mutation's result.
 */
export const withMutationReconciliation = <A, E, R, E2, R2>(
  effect: Effect.Effect<A, E, R>,
  reconcile: Effect.Effect<void, E2, R2>,
): Effect.Effect<A, E, R | R2> =>
  effect.pipe(
    Effect.ensuring(
      reconcile.pipe(
        Effect.uninterruptible,
        // The mutation has committed, so its response stands. The cause is logged.
        Effect.catchCause((cause) => Effect.logError("Wakeup reconciliation failed", cause)),
      ),
    ),
  );
