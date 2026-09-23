import { Effect } from "effect";
import type { StreamRef } from "@streamsy/core";
import type { Entry, InputMap, Slices } from "./batch.ts";
import type { Identity, ProjectionKey } from "./checkpoint.ts";
import { entries } from "./read.ts";
import type { Named } from "./outputs.ts";
import type { Unit } from "./unit.ts";
export { pass, run } from "./run.ts";
export { follow } from "./follow.ts";
export { family } from "./family.ts";
export { serialized } from "./serialized.ts";
export { onChange } from "./on-change.ts";
export type { Progress } from "./run.ts";
export type { FollowOptions } from "./follow.ts";
export type {
  Family,
  FamilyDefinition,
  FusedFamilyDefinition,
  PinnedFamilyDefinition,
} from "./family.ts";
export type { OnChangeOptions } from "./on-change.ts";

/** The handler runs inside the checkpoint owner's transaction; its local writes commit with it. */
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
export interface Pinned<Inputs extends InputMap = InputMap, O = unknown, E = unknown, R = unknown> {
  readonly _tag: "Stream";
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly params: Record<string, string>;
  readonly inputs: Inputs;
  readonly output: StreamRef.StreamRef<O>;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<ReadonlyArray<O>, E, R>;
}
export type Projection<Inputs extends InputMap = InputMap, O = unknown, E = unknown, R = unknown> =
  | Fused<Inputs, E, R>
  | Pinned<Inputs, O, E, R>
  | Named<Inputs, E, R>;

interface Common {
  readonly id: string;
  readonly version?: number;
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
export interface PinnedDefinition<Inputs extends InputMap, O, E, R> extends Common {
  readonly inputs: Inputs;
  readonly output: StreamRef.StreamRef<O>;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<ReadonlyArray<O>, E, R>;
}
export interface SinglePinnedDefinition<A, O, E, R> extends Common {
  readonly input: StreamRef.StreamRef<A>;
  readonly output: StreamRef.StreamRef<O>;
  readonly process: (
    batch: Slices<{ readonly input: StreamRef.StreamRef<A> }>,
    unit: Unit,
  ) => Effect.Effect<ReadonlyArray<O>, E, R>;
}

const common = (definition: Common) => ({
  id: definition.id,
  version: definition.version ?? 1,
  generation: definition.generation ?? 1,
  params: definition.params ?? {},
});

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
  const base = { _tag: "Fused", ...common(definition) } as const;
  return "inputs" in definition
    ? { ...base, inputs: definition.inputs, process: definition.process }
    : { ...base, inputs: { input: definition.input }, process: definition.process };
}

/** Inert stream form for an output the checkpoint transaction cannot reach. */
export function stream<A, O, E, R>(
  definition: SinglePinnedDefinition<A, O, E, R>,
): Pinned<{ readonly input: StreamRef.StreamRef<A> }, O, E, R>;
export function stream<Inputs extends InputMap, O, E, R>(
  definition: PinnedDefinition<Inputs, O, E, R>,
): Pinned<Inputs, O, E, R>;
export function stream<Inputs extends InputMap, A, O, E, R>(
  definition: PinnedDefinition<Inputs, O, E, R> | SinglePinnedDefinition<A, O, E, R>,
): Pinned<Inputs, O, E, R> | Pinned<{ readonly input: StreamRef.StreamRef<A> }, O, E, R> {
  const base = { _tag: "Stream", ...common(definition), output: definition.output } as const;
  return "inputs" in definition
    ? { ...base, inputs: definition.inputs, process: definition.process }
    : { ...base, inputs: { input: definition.input }, process: definition.process };
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
>(
  handle: (entry: Entry<Inputs>, unit: Unit) => Effect.Effect<Result, E, R>,
): (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<Result, E, R>;
export function each<Inputs extends InputMap, E, R>(
  handle: (entry: Entry<Inputs>, unit: Unit) => Effect.Effect<unknown, E, R>,
): (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<void, E, R>;
export function each<Inputs extends InputMap, E, R>(
  handle: (entry: Entry<Inputs>, unit: Unit) => Effect.Effect<unknown, E, R>,
): (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<any, E, R> {
  return (batch: Slices<Inputs>, unit: Unit) =>
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
export type { Named } from "./outputs.ts";
