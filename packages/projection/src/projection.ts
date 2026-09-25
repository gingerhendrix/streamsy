import { Effect } from "effect";
import type { Streamsy } from "@streamsy/core";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Entry, InputMap, ItemOf, Slices } from "./batch.ts";
import {
  addressesOf,
  declare,
  isDeclaration,
  type AddressOf,
  type Addresses,
  type Common,
  type Declaration,
  type Input,
  type Inputs,
  type OneWithin,
} from "./declaration.ts";
import type { TargetServices } from "./output-internal.ts";
import type { Identity, ProjectionKey } from "./checkpoint.ts";
import { entries } from "./read.ts";
import type { Outputs } from "./outputs.ts";
import { withParams, type Unit } from "./unit.ts";
export { pass, run } from "./run.ts";
export { follow } from "./follow.ts";
export { watch } from "./watch.ts";
export type { Progress } from "./run.ts";
export type { FollowOptions } from "./follow.ts";
export type { Declaration } from "./declaration.ts";
export type { WatchOptions } from "./watch.ts";

/**
 * The handler runs inside the checkpoint owner's transaction. Stream writes are checked
 * individually before they execute. Other side effects, including SQL on another client,
 * and ambient Streams.* / StreamsWriter writes are outside this guard. Bind fused output
 * streams directly to their storage protocol graph. Binding fails when the exposed
 * Storage does not own the protocol wires. Declared outputs are checked together before the first write. The owner reference is inherited
 * by nested runs and forked fibers; cross-owner nested runs and detached writes are unsupported.
 */
export interface Fused<Inputs extends InputMap, E, R> {
  readonly _tag: "Fused";
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<void, E, R>;
}
/** The handler returns the items to append; a pinned unit makes the append exactly-once. */
export interface Stream<
  Inputs extends InputMap = InputMap,
  O = unknown,
  E = unknown,
  R = unknown,
  OR = never,
> {
  readonly _tag: "Stream";
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  readonly output: Streamsy.Address<O, OR>;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<ReadonlyArray<O>, E, R>;
}
export type Projection<
  Inputs extends InputMap = InputMap,
  O = unknown,
  E = unknown,
  R = unknown,
  OR = unknown,
> = Fused<Inputs, E, R> | Stream<Inputs, O, E, R, OR> | Outputs<Inputs, E, R, OR>;

export interface FusedDefinition<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  E,
  R,
> extends Common<C, I> {
  readonly process: (
    batch: Slices<Addresses<I>>,
    unit: Unit<StreamRoute.Params<C>>,
  ) => Effect.Effect<void, E, R>;
}
export interface StreamDefinition<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  Out extends Input,
  E,
  R,
> extends Common<C, I> {
  readonly output: Out & OneWithin<C, Out>;
  readonly process: (
    batch: Slices<Addresses<I>>,
    unit: Unit<StreamRoute.Params<C>>,
  ) => Effect.Effect<ReadonlyArray<ItemOf<AddressOf<Out>>>, E, R>;
}

/** Inert: acquires no service. Defaults are version 1 and generation 1. */
export function fused<C extends StreamRoute.ParamCodecs, I extends Inputs, E, R>(
  definition: FusedDefinition<C, I, E, R>,
): Declaration<C, Fused<Addresses<I>, E, R>> {
  return declare(definition, ({ decoded, ...key }) => ({
    _tag: "Fused",
    ...key,
    inputs: addressesOf(definition.inputs, decoded),
    process: (batch, unit) => definition.process(batch, withParams(unit, decoded)),
  }));
}

/** Inert stream form with a declared output on the checkpoint owner. */
export function stream<
  C extends StreamRoute.ParamCodecs,
  I extends Inputs,
  Out extends Input,
  E,
  R,
>(
  definition: StreamDefinition<C, I, Out, E, R>,
): Declaration<
  C,
  Stream<Addresses<I>, ItemOf<AddressOf<Out>>, E, R, TargetServices<AddressOf<Out>>>
> {
  return declare(definition, ({ decoded, ...key }) => {
    const output: Input = definition.output;
    return {
      _tag: "Stream",
      ...key,
      // SAFETY: a declaration resolves to its own address type; a fixed address is kept.
      output: (isDeclaration(output) ? output.at(decoded) : output) as Streamsy.Address<
        ItemOf<AddressOf<Out>>,
        TargetServices<AddressOf<Out>>
      >,
      inputs: addressesOf(definition.inputs, decoded),
      process: (batch, unit) => definition.process(batch, withParams(unit, decoded)),
    };
  });
}

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
        // oxlint-disable-next-line anti-slop/no-runtime-typeof -- The fused overload allows arbitrary ignored success values.
        if (typeof result !== "object" || result === null) continue;
        for (const [name, values] of Object.entries(result)) {
          if (Array.isArray(values)) (combined[name] ??= []).push(...values);
        }
      }
      return Object.keys(combined).length === 0 ? undefined : combined;
    });
}

export { State } from "./state.ts";
export { fold, loadState } from "./fold.ts";
export { forget } from "./forget.ts";

export { outputs } from "./outputs.ts";
export type { Outputs, OutputsDefinition } from "./outputs.ts";
