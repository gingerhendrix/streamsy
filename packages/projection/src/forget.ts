import type { Commit } from "./commit.ts";
import { ProjectionFault } from "./fault.ts";
import { Effect } from "effect";
import { type ProjectionKey } from "./checkpoint-store.ts";
import { withPermit } from "./serialize.ts";

/**
 * Deletes the record under the serialized permit. Stop watchers before retirement;
 * wait for direct passes and other processes yourself. Delete stream outputs first or
 * use a new generation, because forgetting restarts their producer sequences.
 */
export const forget = <R>(
  projection: ProjectionKey & { readonly commit: Pick<Commit<never, never, R>, "remove"> },
) =>
  withPermit(
    projection,
    Effect.gen(function* () {
      if (projection.commit.remove === undefined)
        return yield* new ProjectionFault({
          phase: "checkpoint",
          reason: "unsupported-composition",
          message: "This Commit does not support forgetting",
        });
      yield* projection.commit.remove(projection);
    }),
    true,
  );
