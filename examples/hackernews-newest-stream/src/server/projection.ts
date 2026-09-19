import { Checkpoints, Projection, type Host } from "@streamsy/projection";
import { Cause, Context, Effect, Exit, Fiber, Layer, Option, Ref, Scope } from "effect";
import { hackerNewsStoryIndex } from "./story-index-projection.ts";
import { errorMessage } from "./util.ts";

/** One bounded run: checkpoint transactions per call. */
export type ProjectionLimits = {
  readonly limit: number;
};

export type ProjectionStatus = {
  readonly running: boolean;
  readonly sourceThrough?: string;
  readonly lastError?: string;
};

export type ProjectionServices = Host;

export interface StoryProjectionService {
  readonly status: Effect.Effect<ProjectionStatus, import("@streamsy/projection").ProjectionFault>;
}

export class StoryProjection extends Context.Service<StoryProjection, StoryProjectionService>()(
  "HackerNews/StoryProjection",
) {}

export function makeStoryProjection(
  limits: ProjectionLimits,
): Effect.Effect<
  StoryProjectionService,
  import("@streamsy/projection").ProjectionFault,
  Host | Scope.Scope
> {
  return Effect.gen(function* () {
    const owner = yield* Checkpoints;
    const running = yield* Ref.make(true);
    const lastError = yield* Ref.make<string | undefined>(undefined);
    const follower = yield* Projection.follow(hackerNewsStoryIndex, {
      ...limits,
      repairIntervalMs: 1000,
    });
    yield* Fiber.await(follower).pipe(
      Effect.tap((exit) =>
        Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
          ? Ref.set(lastError, errorMessage(Cause.squash(exit.cause)))
          : Effect.void,
      ),
      Effect.ensuring(Ref.set(running, false)),
      Effect.forkScoped,
    );

    return StoryProjection.of({
      status: Effect.gen(function* () {
        const [isRunning, error, loaded] = yield* Effect.all([
          Ref.get(running),
          Ref.get(lastError),
          owner.load(Projection.key(hackerNewsStoryIndex)),
        ]);
        const sourceThrough = Option.map(loaded.record, (record) => record.inputs.input);
        if (Option.isSome(sourceThrough) && error !== undefined)
          return { running: isRunning, sourceThrough: sourceThrough.value, lastError: error };
        if (Option.isSome(sourceThrough))
          return { running: isRunning, sourceThrough: sourceThrough.value };
        if (error !== undefined) return { running: isRunning, lastError: error };
        return { running: isRunning };
      }),
    });
  });
}

export const storyProjectionLayer = (limits: ProjectionLimits) =>
  Layer.effect(StoryProjection, makeStoryProjection(limits));
