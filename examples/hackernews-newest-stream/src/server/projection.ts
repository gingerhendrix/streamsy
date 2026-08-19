import {
  StateProjection,
  type CatchUpOutcome,
  type StateProjectionLimits,
} from "@streamsy/experimental/effect/state-projection";
import { Cause, Effect } from "effect";
import { hackerNewsStoryIndex } from "./story-index-projection.ts";
import { hackerNewsSource, hackerNewsTarget } from "./streams.ts";

export type ProjectionStatus = {
  running: boolean;
  lastAttemptStartedAt?: string;
  lastAttemptCompletedAt?: string;
  lastError?: string;
  lastOutcome?: CatchUpOutcome;
};

export function createStoryProjection(limits: StateProjectionLimits) {
  const projection = StateProjection.instance(hackerNewsStoryIndex, {
    source: hackerNewsSource,
    target: hackerNewsTarget,
    generation: "v1",
    producerEpoch: 0,
  });
  let running = false;
  let lastAttemptStartedAt: string | undefined;
  let lastAttemptCompletedAt: string | undefined;
  let lastError: string | undefined;
  let lastOutcome: CatchUpOutcome | undefined;

  function catchUp() {
    return Effect.sync(() => {
      running = true;
      lastAttemptStartedAt = new Date().toISOString();
      lastError = undefined;
    }).pipe(
      Effect.flatMap(() => StateProjection.catchUp(projection, { limits })),
      Effect.tap((outcome) =>
        Effect.sync(() => {
          lastOutcome = outcome;
        }),
      ),
      Effect.catchCause((cause) =>
        Effect.sync(() => {
          const error = Cause.squash(cause);
          lastError = error instanceof Error ? error.message : String(error);
          return undefined;
        }),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          running = false;
          lastAttemptCompletedAt = new Date().toISOString();
        }),
      ),
    );
  }

  function status(): ProjectionStatus {
    return {
      running,
      lastAttemptStartedAt,
      lastAttemptCompletedAt,
      lastError,
      lastOutcome,
    };
  }

  return { catchUp, status };
}
