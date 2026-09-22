import { Effect } from "effect";
import { Checkpoints, recordKey, type ProjectionKey } from "./checkpoint.ts";
import { State } from "./state.ts";
import { releaseLock } from "./serialized.ts";

/**
 * Deletes both rows and releases the process lock after commit. Stop live watchers first:
 * their next run would restart from zero. Delete a stream-form output first, or use a new
 * generation, because forgetting restarts its producer sequence. Streams are not deleted.
 */
export const forget = (projection: ProjectionKey) =>
  Effect.gen(function* () {
    const checkpoints = yield* Checkpoints;
    const state = yield* State;
    yield* checkpoints.withTransaction(
      Effect.gen(function* () {
        yield* state.remove(projection);
        yield* checkpoints.remove(projection);
      }),
    );
    releaseLock(recordKey(projection));
  });
