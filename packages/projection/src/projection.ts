import { Effect } from "effect";
import type { StreamRef } from "@streamsy/core";
import type { Entry, InputMap, Slices } from "./batch.ts";
import type { ProjectionKey } from "./checkpoint.ts";
import { entries } from "./read.ts";
import type { Unit } from "./unit.ts";
export { pass, run } from "./run.ts";
export type { Progress } from "./run.ts";

/** The handler runs inside the checkpoint owner's transaction; its local writes commit with it. */
export interface Fused<Inputs extends InputMap, E, R> {
  readonly _tag: "Fused";
  readonly id: string;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<void, E, R>;
}
export type Projection<Inputs extends InputMap = InputMap, E = unknown, R = unknown> = Fused<
  Inputs,
  E,
  R
>;

interface Common {
  readonly id: string;
  readonly generation?: number;
  readonly params?: Record<string, string>;
}
export interface FusedDefinition<Inputs extends InputMap, E, R> extends Common {
  readonly inputs: Inputs;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<void, E, R>;
}
/** Sugar for one input: the slice is `batch.input`. */
export interface SingleFusedDefinition<A, E, R> extends Common {
  readonly input: StreamRef.StreamRef<A>;
  readonly process: (
    batch: Slices<{ readonly input: StreamRef.StreamRef<A> }>,
    unit: Unit,
  ) => Effect.Effect<void, E, R>;
}

/** Inert: acquires no service. Defaults are generation 1 and no params. */
export function make<A, E, R>(
  definition: SingleFusedDefinition<A, E, R>,
): Fused<{ readonly input: StreamRef.StreamRef<A> }, E, R>;
export function make<Inputs extends InputMap, E, R>(
  definition: FusedDefinition<Inputs, E, R>,
): Fused<Inputs, E, R>;
export function make<Inputs extends InputMap, A, E, R>(
  definition: FusedDefinition<Inputs, E, R> | SingleFusedDefinition<A, E, R>,
): Fused<Inputs, E, R> | Fused<{ readonly input: StreamRef.StreamRef<A> }, E, R> {
  const common = {
    _tag: "Fused",
    id: definition.id,
    generation: definition.generation ?? 1,
    params: definition.params ?? {},
  } as const;
  return "inputs" in definition
    ? { ...common, inputs: definition.inputs, process: definition.process }
    : { ...common, inputs: { input: definition.input }, process: definition.process };
}

/** What the kernel needs to locate a record; every projection value satisfies it. */
export interface Identity {
  readonly id: string;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: InputMap;
}
export const key = (projection: Identity): ProjectionKey => ({
  id: projection.id,
  generation: projection.generation,
  params: projection.params,
});

/** Tagged items in declaration order, then stream order within each input. */
export const items = <Inputs extends InputMap>(
  batch: Slices<Inputs>,
): ReadonlyArray<Entry<Inputs>> =>
  // SAFETY: each entry's item was decoded by the codec of the input it is tagged with.
  entries(batch) as ReadonlyArray<Entry<Inputs>>;

/** A fused handler that runs `handle` once per tagged item, one at a time, in unit order. */
export const each =
  <Inputs extends InputMap, E, R>(
    handle: (entry: Entry<Inputs>, unit: Unit) => Effect.Effect<unknown, E, R>,
  ) =>
  (batch: Slices<Inputs>, unit: Unit): Effect.Effect<void, E, R> =>
    Effect.forEach(items(batch), (entry) => handle(entry, unit), {
      concurrency: 1,
      discard: true,
    });
