import { Effect } from "effect";
import { Checkpoints, type ProjectionKey } from "./checkpoint.ts";
import { State } from "./state.ts";
import { withPermit } from "./serialized.ts";

/**
 * Deletes both rows under the serialized permit. Stop watchers before retirement;
 * wait for bare runs and other processes yourself. Delete stream outputs first or
 * use a new generation, because forgetting restarts their producer sequences.
 */
export const forget = (projection: ProjectionKey) =>
  withPermit(
    projection,
    Effect.gen(function* () {
      const checkpoints = yield* Checkpoints;
      const state = yield* State;
      yield* checkpoints.withTransaction(
        Effect.gen(function* () {
          yield* state.remove(projection);
          yield* checkpoints.remove(projection);
        }),
      );
    }),
    true,
  );
