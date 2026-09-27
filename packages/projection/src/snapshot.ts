import { Effect, Option } from "effect";
import type { InputMap } from "./batch.ts";
import { Checkpoints, restore } from "./checkpoint.ts";
import type { ProjectionFault } from "./fault.ts";
import type { Outputs } from "./outputs.ts";
import type { Atomic } from "./projection.ts";
import { State, checkStateOwner, decodeState } from "./state.ts";

/** The accepted offset of each input, keyed by input name. The next pass reads after these. */
export type Offsets<Inputs extends InputMap> = { readonly [K in keyof Inputs]: string };

/** The state and the offsets it is at. `state` is `undefined` when the declaration has no `state:`. */
export interface Snapshot<S, Inputs extends InputMap> {
  readonly state: S;
  readonly offsets: Offsets<Inputs>;
}

/** Internal: the offsets of a record, typed by the declared inputs. */
export const offsetsOf = <Inputs extends InputMap>(record: {
  readonly inputs: Record<string, string>;
}): Offsets<Inputs> =>
  // SAFETY: a restored or advanced record has one offset for every declared input.
  record.inputs as Offsets<Inputs>;

export type StateValue<S> = [S] extends [never] ? undefined : S;

/**
 * Reads the state row and the checkpoint row in one owner transaction, so the state
 * always matches the offsets. Before the first unit, the state is the declared initial
 * value and every offset is the start of its input.
 */
export const read = Effect.fn("Projection.read")(function* <Inputs extends InputMap, S = never>(
  projection: Atomic<Inputs, unknown, unknown, S> | Outputs<Inputs, unknown, unknown, unknown, S>,
): Effect.fn.Return<Snapshot<StateValue<S>, Inputs>, ProjectionFault, Checkpoints | State> {
  const owner = yield* Checkpoints;
  const store = yield* State;
  const declared = projection.state;
  yield* checkStateOwner(projection, owner);
  const [restored, encoded] = yield* owner.withTransaction(
    Effect.all([
      restore(projection, owner),
      declared === undefined ? Effect.succeed(Option.none<string>()) : store.load(projection),
    ]),
  );
  const stored =
    declared === undefined
      ? Option.none()
      : yield* decodeState(projection, declared.schema, encoded);
  // SAFETY: the state is undefined exactly when the declaration has no state.
  const state = (
    declared === undefined ? undefined : Option.getOrElse(stored, declared.initial)
  ) as StateValue<S>;
  return { state, offsets: offsetsOf<Inputs>(restored.record) };
});
