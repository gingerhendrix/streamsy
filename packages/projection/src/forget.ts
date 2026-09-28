import { Effect } from "effect";
import { CheckpointStore, type ProjectionKey } from "./checkpoint-store.ts";
import { withPermit } from "./serialize.ts";

/**
 * Deletes the record under the serialized permit. Stop watchers before retirement;
 * wait for direct passes and other processes yourself. Delete stream outputs first or
 * use a new generation, because forgetting restarts their producer sequences.
 */
export const forget = (projection: ProjectionKey) =>
  withPermit(
    projection,
    Effect.gen(function* () {
      const checkpoints = yield* CheckpointStore;
      yield* checkpoints.remove(projection);
    }),
    true,
  );
