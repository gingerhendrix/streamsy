import {
  StateProjection,
  type CatchUpOutcome,
  type StateProjectionLimits,
} from "@streamsy/experimental/effect/state-projection";
import { Cause, Effect, Ref } from "effect";
import { hackerNewsStoryIndex } from "./story-index-projection.ts";
import { hackerNewsSource, hackerNewsTarget } from "./stream-resources.ts";
import { errorMessage, nowIso } from "./util.ts";

export type ProjectionStatus = {
  readonly running: boolean;
  readonly lastAttemptStartedAt?: string;
  readonly lastAttemptCompletedAt?: string;
  readonly lastError?: string;
  readonly lastOutcome?: CatchUpOutcome;
};

const initialStatus: ProjectionStatus = { running: false };

export function makeStoryProjectionInstance() {
  return StateProjection.instance(hackerNewsStoryIndex, {
    source: hackerNewsSource,
    target: hackerNewsTarget,
    generation: "v1",
    producerEpoch: 0,
  });
}

export function makeStoryProjection(limits: StateProjectionLimits) {
  return Effect.gen(function* () {
    const projection = makeStoryProjectionInstance();
    const statusRef = yield* Ref.make(initialStatus);
    const patchStatus = (patch: Partial<ProjectionStatus>) =>
      Ref.update(statusRef, (status) => ({ ...status, ...patch }));

    const catchUp = Effect.fn("StoryProjection.catchUp")(function* () {
      yield* patchStatus({
        running: true,
        lastAttemptStartedAt: yield* nowIso,
        lastError: undefined,
      });

      yield* StateProjection.catchUp(projection, { limits }).pipe(
        Effect.tap((outcome) => patchStatus({ lastOutcome: outcome })),
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterrupts(cause),
          (cause) =>
            patchStatus({
              lastError: errorMessage(Cause.squash(cause)),
            }),
        ),
        Effect.ensuring(
          Effect.gen(function* () {
            yield* patchStatus({
              running: false,
              lastAttemptCompletedAt: yield* nowIso,
            });
          }),
        ),
      );
    });

    return {
      catchUp: catchUp().pipe(Effect.orDie),
      status: Ref.get(statusRef),
    };
  });
}
