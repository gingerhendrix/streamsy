import * as Commit from "./commit.ts";
import type { Folded } from "./commit.ts";
import type { CheckpointStore } from "./checkpoint-store.ts";
import type { Transaction } from "./transaction.ts";
import type { SqlClient } from "effect/unstable/sql";
import { Data, Effect } from "effect";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { InputMap, Slices } from "./batch.ts";
import {
  addressesOf,
  declare,
  type Addresses,
  type Common,
  type Declaration,
  type DecodedParams,
  type Inputs,
} from "./declaration.ts";
import { withParams, type Unit } from "./unit.ts";
import type { ProjectionFault } from "./fault.ts";
import { foldHandler, type Step } from "./fold.ts";
import {
  bindState,
  stateSource,
  type BoundState,
  type StateDefinition,
  type StateSource,
} from "./state.ts";

/**
 * A bound atomic projection. The handler runs in the checkpoint owner's transaction. A
 * stream write to another owner fails with `process / unsupported-composition`.
 */
export interface Atomic<
  Inputs extends InputMap,
  E,
  R,
  S = unknown,
  Q = CheckpointStore | Transaction,
  P = SqlClient.SqlClient,
  Result = void | Folded<S>,
> {
  readonly _tag: "Atomic";
  readonly commit: Commit.Commit<Result, P, Q>;
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  /** Set when the declaration has `state:`. */
  readonly state?: BoundState<S>;
  readonly process: (
    batch: Slices<Inputs>,
    unit: Unit,
    encoded?: string,
  ) => Effect.Effect<Result, E, R>;
}
/** The atomic projection constructor. It owns the `Atomic` tag. */
const AtomicProjection = Data.TaggedClass("Atomic");

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
  ? Declaration<
      C,
      Atomic<Addresses<I>, E, R, never, CheckpointStore | Transaction, SqlClient.SqlClient, void>
    >
  : Declaration<
      C,
      Atomic<Addresses<I>, E | ProjectionFault, R, A, CheckpointStore, never, Folded<A>>
    > & {
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
): Declaration<StreamRoute.ParamCodecs, unknown> & {
  readonly state?: unknown;
} {
  const { state, fold, process } = definition;
  if ((fold === undefined) === (process === undefined))
    throw new RangeError("An atomic projection declares exactly one of process and fold");
  if ((fold === undefined) !== (state === undefined))
    throw new RangeError("An atomic projection declares fold together with state");
  const declaration = declare(definition, ({ decoded, ...key }) => {
    const bound = state === undefined ? undefined : bindState(state, decoded);
    const handler: AtomicHandler<E | ProjectionFault, R> =
      bound === undefined || fold === undefined
        ? (batch, unit) => Effect.asVoid(process!(batch, unit))
        : foldHandler(key, bound, fold);
    const properties = {
      ...key,
      commit: bound === undefined ? Commit.transaction : Commit.checkpoint,
      inputs: addressesOf(definition.inputs, decoded),
      state: bound,
      process: (batch: Slices<InputMap>, unit: Unit, encoded?: string) =>
        handler(batch, withParams(unit, decoded), encoded),
    };
    return new AtomicProjection<typeof properties>(properties);
  });
  return state === undefined
    ? declaration
    : { ...declaration, state: stateSource(definition, state, declaration.at) };
}
type AtomicHandler<E, R> = (
  batch: Slices<InputMap>,
  unit: Unit<DecodedParams>,
  encoded?: string,
) => Effect.Effect<void | Folded<unknown>, E, R>;
