import { Effect } from "effect";
import type { Entry, InputMap, Slices } from "./batch.ts";
import type { ProjectionKey } from "./checkpoint-store.ts";
import type { ProjectionFault } from "./fault.ts";
import { items } from "./read.ts";
import { encodeState, currentState, type BoundState } from "./state.ts";
import type { Unit } from "./unit.ts";

/** One fold step: the state and one tagged item give the next state. */
export type Step<Inputs extends InputMap, A, E, R, P = Record<string, string>> = (
  state: A,
  entry: Entry<Inputs>,
  unit: Unit<P>,
) => A | Effect.Effect<A, E, R>;

/** Internal: the unit loads the state once, folds in unit order, and returns its encoded state for the checkpoint write. */
export const foldHandler =
  <Inputs extends InputMap, A, E, R, P>(
    key: ProjectionKey,
    state: BoundState<A>,
    step: Step<Inputs, A, E, R, P>,
  ) =>
  (
    batch: Slices<Inputs>,
    unit: Unit<P>,
    encoded?: string,
  ): Effect.Effect<string, E | ProjectionFault, R> =>
    Effect.gen(function* () {
      let value = yield* currentState(key, state, encoded);
      for (const entry of items(batch)) {
        const next = step(value, entry, unit);
        value = Effect.isEffect(next) ? yield* next : next;
      }
      return yield* encodeState(key, state.schema, value);
    });
