import { Effect } from "effect";
import type { InputMap, Slices } from "./batch.ts";
import type { Commit } from "./commit.ts";
import { advance, rangesOf, restore, type Identity } from "./checkpoint-store.ts";
import type { BoundState } from "./state.ts";
import { currentState } from "./state.ts";
import { readInputs } from "./read.ts";
import { offsetsOf, type Snapshot } from "./snapshot.ts";
import { unitOf, type Unit } from "./unit.ts";

/** A bound value projection. Output items from its adapter are discarded. */
export interface CurrentProjection<Inputs extends InputMap, S, E, R, Q> extends Identity {
  readonly inputs: Inputs;
  readonly state: BoundState<S>;
  readonly commit: Pick<Commit<never, never, Q>, "load">;
  readonly process: (
    batch: Slices<Inputs>,
    unit: Unit,
    encoded?: string,
  ) => Effect.Effect<{ readonly value: S; readonly encoded: string }, E, R>;
}

/**
 * Folds the checkpoint and input tail in memory, without locks or writes.
 * Includes entries appended before the call. A growing input can delay completion.
 * Fold steps must be replayable. Pending ranges are read from committed offsets.
 */
export const current = Effect.fn("Projection.current")(function* <
  Inputs extends InputMap,
  S,
  E,
  R,
  Q,
>(projection: CurrentProjection<Inputs, S, E, R, Q>) {
  const { record } = yield* restore(projection, projection.commit);
  let offsets = record.inputs;
  let encoded = record.state;
  let state = yield* currentState(projection, projection.state, encoded);

  while (true) {
    const page = yield* readInputs(projection.inputs, offsets);

    if (page.items === 0) {
      const snapshot: Snapshot<S, Inputs> = {
        state,
        offsets: offsetsOf<Inputs>({ inputs: offsets }),
      };

      return snapshot;
    }

    const ranges = rangesOf(page.slices);

    const unit = unitOf(
      projection.id,
      projection.version,
      projection.generation,
      projection.params,
      ranges,
    );

    const result = yield* projection.process(page.slices, unit, encoded);
    state = result.value;
    encoded = result.encoded;
    offsets = advance(offsets, ranges);
  }
});
