import { Effect } from "effect";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Entry, InputMap, Slices } from "./batch.ts";
import {
  addressesOf,
  declare,
  type Addresses,
  type Common,
  type Declaration,
  type DecodedParams,
  type Inputs,
} from "./declaration.ts";
import type { Identity, ProjectionKey } from "./checkpoint.ts";
import { entries } from "./read.ts";
import type { Outputs } from "./outputs.ts";
import { withParams, type Unit } from "./unit.ts";
import type { ProjectionFault } from "./fault.ts";
import { foldHandler, type Step } from "./fold.ts";
import { stateSource, type StateSource } from "./output-source.ts";
import { bindState, type BoundState, type State, type StateDefinition } from "./state.ts";
export { pass, run } from "./run.ts";
export { follow } from "./follow.ts";
export { watch } from "./watch.ts";
export type { Progress } from "./run.ts";
export type { FollowOptions } from "./follow.ts";
export type { Declaration } from "./declaration.ts";
export type { WatchOptions } from "./watch.ts";

/**
 * A bound atomic projection. The handler runs in the checkpoint owner's transaction. A
 * stream write to another owner fails with `process / unsupported-composition`.
 */
export interface Atomic<Inputs extends InputMap, E, R, S = unknown> {
  readonly _tag: "Atomic";
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  /** Set when the declaration has `state:`. */
  readonly state?: BoundState<S>;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<void, E, R>;
}
export type Projection<
  Inputs extends InputMap = InputMap,
  E = unknown,
  R = unknown,
  OR = unknown,
  S = unknown,
> = Atomic<Inputs, E, R, S> | Outputs<Inputs, E, R, OR, S>;

export interface AtomicDefinition<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  E,
  R,
> extends Common<C, I> {
  readonly process: (
    batch: Slices<Addresses<I>>,
    unit: Unit<StreamRoute.Params<C>>,
  ) => Effect.Effect<void, E, R>;
  readonly state?: undefined;
  readonly fold?: undefined;
}
/**
 * An atomic projection with state. `fold` takes the state and one item and returns the
 * next state. The unit loads the state once, folds every item in order, and saves the
 * state once with the checkpoint.
 */
export interface AtomicStateDefinition<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  A,
  SI,
  E,
  R,
> extends Common<C, I> {
  readonly state: StateDefinition<A, SI, StreamRoute.Params<C>>;
  readonly fold: Step<Addresses<I>, A, E, R, StreamRoute.Params<C>>;
  readonly process?: undefined;
}
/** A bound atomic declaration. With `state:`, it also gives `state`, a source for `Serve.value`. */
export type AtomicDeclaration<C extends StreamRoute.ParamCodecs, I extends Inputs, A, SI, E, R> = [
  A,
] extends [never]
  ? Declaration<C, Atomic<Addresses<I>, E, R, never>>
  : Declaration<C, Atomic<Addresses<I>, E | ProjectionFault, R | State, A>> & {
      readonly state: StateSource<A, SI, StreamRoute.Params<C>>;
    };
/** Inert: acquires no service. Defaults are version 1 and generation 1. */
export function atomic<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  E = never,
  R = never,
  A = never,
  SI = never,
>(
  definition: AtomicDefinition<C, I, E, R> | AtomicStateDefinition<C, I, A, SI, E, R>,
): AtomicDeclaration<C, I, A, SI, E, R>;
export function atomic<E, R>(
  definition: Common<StreamRoute.ParamCodecs, Inputs> & {
    readonly state?: StateDefinition<unknown, unknown, DecodedParams>;
    readonly process?: AtomicHandler<E, R>;
    readonly fold?: Step<InputMap, unknown, E, R, DecodedParams>;
  },
): Declaration<StreamRoute.ParamCodecs, Atomic<InputMap, E | ProjectionFault, R | State>> & {
  readonly state?: unknown;
} {
  const { state, fold, process } = definition;
  if ((fold === undefined) === (process === undefined))
    throw new RangeError("An atomic projection declares exactly one of process and fold");
  if ((fold === undefined) !== (state === undefined))
    throw new RangeError("An atomic projection declares fold together with state");
  const declaration = declare(definition, ({ decoded, ...key }) => {
    const bound = state === undefined ? undefined : bindState(state, decoded);
    const handler: AtomicHandler<E | ProjectionFault, R | State> =
      bound === undefined || fold === undefined ? process! : foldHandler(bound, fold);
    return {
      _tag: "Atomic" as const,
      ...key,
      inputs: addressesOf(definition.inputs, decoded),
      state: bound,
      process: (batch: Slices<InputMap>, unit: Unit) => handler(batch, withParams(unit, decoded)),
    };
  });
  return state === undefined
    ? declaration
    : { ...declaration, state: stateSource(definition, state, declaration.at) };
}
type AtomicHandler<E, R> = (
  batch: Slices<InputMap>,
  unit: Unit<DecodedParams>,
) => Effect.Effect<void, E, R>;

export type { Identity };
export const key = (projection: Identity): ProjectionKey => ({
  id: projection.id,
  version: projection.version,
  generation: projection.generation,
  params: projection.params,
});

/** Tagged items in declaration order, then stream order within each input. */
export const items = <Inputs extends InputMap>(
  batch: Slices<Inputs>,
): ReadonlyArray<Entry<Inputs>> =>
  // SAFETY: each entry's item was decoded by the codec of the input it is tagged with.
  entries(batch) as ReadonlyArray<Entry<Inputs>>;

/** Runs each tagged item in order; keyed output arrays are concatenated in that order. */
export function each<
  Inputs extends InputMap,
  Result extends Readonly<Record<string, ReadonlyArray<unknown>>>,
  E,
  R,
  P = Record<string, string>,
>(
  handle: (entry: Entry<Inputs>, unit: Unit<P>) => Effect.Effect<Result, E, R>,
): (batch: Slices<Inputs>, unit: Unit<P>) => Effect.Effect<Result, E, R>;
export function each<Inputs extends InputMap, E, R, P = Record<string, string>>(
  handle: (entry: Entry<Inputs>, unit: Unit<P>) => Effect.Effect<unknown, E, R>,
): (batch: Slices<Inputs>, unit: Unit<P>) => Effect.Effect<void, E, R>;
export function each<Inputs extends InputMap, E, R, P>(
  handle: (entry: Entry<Inputs>, unit: Unit<P>) => Effect.Effect<unknown, E, R>,
): (batch: Slices<Inputs>, unit: Unit<P>) => Effect.Effect<any, E, R> {
  return (batch: Slices<Inputs>, unit: Unit<P>) =>
    Effect.gen(function* () {
      const combined: Record<string, unknown[]> = {};
      for (const entry of items(batch)) {
        const result = yield* handle(entry, unit);
        // oxlint-disable-next-line anti-slop/no-runtime-typeof -- The atomic overload allows arbitrary ignored success values.
        if (typeof result !== "object" || result === null) continue;
        for (const [name, values] of Object.entries(result)) {
          if (Array.isArray(values)) (combined[name] ??= []).push(...values);
        }
      }
      return Object.keys(combined).length === 0 ? undefined : combined;
    });
}

export { State } from "./state.ts";
export { read } from "./snapshot.ts";
export type { Offsets, Snapshot } from "./snapshot.ts";
export { forget } from "./forget.ts";

export { outputs } from "./outputs.ts";
export type { Outputs, OutputsDefinition } from "./outputs.ts";
