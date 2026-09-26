import { Effect } from "effect";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { Entry, InputMap, Slices } from "./batch.ts";
import {
  addressesOf,
  declare,
  type Addresses,
  type Common,
  type Declaration,
  type Inputs,
} from "./declaration.ts";
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
 * and ambient Streams.* / StreamsWriter writes are outside this guard. Bind atomic output
 * streams directly to their storage protocol graph. Binding fails when the exposed
 * Storage does not own the protocol wires. Declared outputs are checked together before the first write. The owner reference is inherited
 * by nested runs and forked fibers; cross-owner nested runs and detached writes are unsupported.
 */
export interface Atomic<Inputs extends InputMap, E, R> {
  readonly _tag: "Atomic";
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<void, E, R>;
}
export type Projection<
  Inputs extends InputMap = InputMap,
  E = unknown,
  R = unknown,
  OR = unknown,
> = Atomic<Inputs, E, R> | Outputs<Inputs, E, R, OR>;

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
}
/** Inert: acquires no service. Defaults are version 1 and generation 1. */
export function atomic<C extends StreamRoute.ParamCodecs, I extends Inputs, E, R>(
  definition: AtomicDefinition<C, I, E, R>,
): Declaration<C, Atomic<Addresses<I>, E, R>> {
  return declare(definition, ({ decoded, ...key }) => ({
    _tag: "Atomic",
    ...key,
    inputs: addressesOf(definition.inputs, decoded),
    process: (batch, unit) => definition.process(batch, withParams(unit, decoded)),
  }));
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
export { fold, loadState } from "./fold.ts";
export { forget } from "./forget.ts";

export { outputs } from "./outputs.ts";
export type { Outputs, OutputsDefinition } from "./outputs.ts";
