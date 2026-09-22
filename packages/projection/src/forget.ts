import { Effect } from "effect";
import { Checkpoints, recordKey, type ProjectionKey } from "./checkpoint.ts";
import { State } from "./state.ts";
import { releaseLock } from "./serialized.ts";

/**
 * Deletes both rows and releases the process lock after commit. This does not interrupt
 * a live `onChange` or `follow` fiber or an in-flight run. A serialized call can then run
 * beside that run. The record version restarts after `forget`, so token conflict does not
 * refuse the older run when the two save counts match; that run then commits on top of the
 * fresh record and folds its items twice. Stop live watchers and wait for in-flight runs
 * first: the next run would restart from zero. Delete a stream-form output first, or use
 * a new generation, because forgetting restarts its producer sequence. Streams are not
 * deleted.
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
