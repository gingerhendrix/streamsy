import type { Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection, ProjectionRetry } from "@streamsy/projection";
import { Cause, Context, Effect, Fiber, FiberMap, Layer } from "effect";
import { readModel } from "./read-model.ts";
import { WorkspaceEvents, Workspaces } from "./streams.ts";

/** Keeps one read model runner for each workspace for the life of the server. */
export class ProjectionWatchers extends Context.Service<
  ProjectionWatchers,
  {
    /** Start the workspace's runner. A second call for the same workspace does nothing. */
    readonly start: (workspaceId: string) => Effect.Effect<void>;
  }
>()("IssueTrackerEventSourced/ProjectionWatchers") {}

export const ProjectionWatchersLive = Layer.effect(
  ProjectionWatchers,
  Effect.gen(function* () {
    // The layer scope owns every runner, so they stop before storage closes.
    const runners = yield* FiberMap.make<string>();
    const context = yield* Effect.context<
      CheckpointStore | WorkspaceEvents | Workspaces | Streamsy.ChangeFeed<WorkspaceEvents>
    >();
    const retry = yield* ProjectionRetry;

    /**
     * Catch up from the checkpoint, then run on each change of the event stream.
     * A failure is logged and the runner starts again after a delay, so one
     * workspace cannot stop the others.
     */
    const runner = (workspaceId: string) => {
      const rows = readModel.at({ workspaceId });
      return Effect.scoped(
        Effect.gen(function* () {
          yield* Projection.run(rows);
          return yield* Fiber.join(yield* Projection.watch(rows));
        }),
      ).pipe(
        Effect.tapCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : Effect.logError("read model stopped", { workspaceId }, cause),
        ),
        Effect.retry(retry),
        Effect.provide(context),
      );
    };

    return ProjectionWatchers.of({
      start: (workspaceId) =>
        FiberMap.run(runners, workspaceId, runner(workspaceId), { onlyIfMissing: true }).pipe(
          Effect.asVoid,
        ),
    });
  }),
);
