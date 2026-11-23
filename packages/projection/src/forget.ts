import type { Commit } from "./commit.ts";
import { ProjectionFault } from "./fault.ts";
import { Effect } from "effect";
import { type ProjectionKey } from "./checkpoint-store.ts";

/**
 * Deletes a live checkpoint and marks its routed outputs as forgotten so retirement
 * can still find them. Preserves retirement tombstones. Stop the follower first,
 * or use a new generation.
 * Delete stream outputs first, because forgetting restarts their producer sequences.
 */
export const forget = <R>(
  projection: ProjectionKey & { readonly commit: Pick<Commit<never, never, R>, "remove"> },
) =>
  Effect.gen(function* () {
    if (projection.commit.remove === undefined)
      return yield* new ProjectionFault({
        phase: "checkpoint",
        reason: "unsupported-composition",
        message: "This Commit does not support forgetting",
      });
    yield* projection.commit.remove(projection);

    return yield* Effect.void;
  });
