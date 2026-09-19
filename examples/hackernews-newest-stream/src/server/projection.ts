import { Projection, type Host, type Progress } from "@streamsy/projection";
import { Cause, Context, Effect, Layer, Ref } from "effect";
import { hackerNewsStoryIndex } from "./story-index-projection.ts";
import { errorMessage, nowIso } from "./util.ts";

/** One bounded run: checkpoint transactions per call. */
export type ProjectionLimits = {
  readonly limit: number;
};

/** The last run's status and the source offset the checkpoint accepted. */
export type ProjectionOutcome = {
  readonly status: Progress["status"];
  readonly progress: { readonly sourceThrough: string };
};

export type ProjectionStatus = {
  readonly running: boolean;
  readonly lastAttemptStartedAt?: string;
  readonly lastAttemptCompletedAt?: string;
  readonly lastError?: string;
  readonly lastOutcome?: ProjectionOutcome;
};

export type ProjectionServices = Host;

export interface StoryProjectionService {
  readonly catchUp: Effect.Effect<void, never, ProjectionServices>;
  readonly status: Effect.Effect<ProjectionStatus>;
}

export class StoryProjection extends Context.Service<StoryProjection, StoryProjectionService>()(
  "HackerNews/StoryProjection",
) {}

const initialStatus: ProjectionStatus = { running: false };

const outcomeOf = (progress: Progress): ProjectionOutcome => ({
  status: progress.status,
  progress: { sourceThrough: progress.record.inputs.input },
});

export function makeStoryProjection(
  limits: ProjectionLimits,
): Effect.Effect<StoryProjectionService> {
  return Effect.gen(function* () {
    const statusRef = yield* Ref.make(initialStatus);
    const patchStatus = (patch: Partial<ProjectionStatus>) =>
      Ref.update(statusRef, (status) => ({ ...status, ...patch }));

    const catchUp = Effect.fn("StoryProjection.catchUp")(function* () {
      yield* patchStatus({
        running: true,
        lastAttemptStartedAt: yield* nowIso,
        lastError: undefined,
      });

      yield* Projection.run(hackerNewsStoryIndex, limits).pipe(
        Effect.tap((progress) => patchStatus({ lastOutcome: outcomeOf(progress) })),
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

    return StoryProjection.of({
      catchUp: catchUp().pipe(Effect.orDie),
      status: Ref.get(statusRef),
    });
  });
}

export const storyProjectionLayer = (limits: ProjectionLimits) =>
  Layer.effect(StoryProjection, makeStoryProjection(limits));
