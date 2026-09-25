import { Effect } from "effect";
import type { StreamFamily } from "@streamsy/core";
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

/**
 * The handler runs inside the checkpoint owner's transaction. Family writes are checked
 * individually before they execute. Other side effects, including SQL on another client,
 * and ambient Streams.* / StreamsWriter writes are outside this guard. Bind fused output
 * families directly to their storage protocol graph. Binding fails when the exposed
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
export interface Pinned<
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
  readonly output: StreamFamily.Member<O, OR>;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<ReadonlyArray<O>, E, R>;
}
export type Projection<
  Inputs extends InputMap = InputMap,
  O = unknown,
  E = unknown,
  R = unknown,
  OR = unknown,
> = Fused<Inputs, E, R> | Pinned<Inputs, O, E, R, OR> | Named<Inputs, E, R, OR>;

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
export interface SingleFusedDefinition<A, E, R, IR = never> extends Common {
  readonly input: StreamFamily.Member<A, IR>;
  readonly process: (
    batch: Slices<{ readonly input: StreamFamily.Member<A, IR> }>,
    unit: Unit,
  ) => Effect.Effect<void, E, R>;
}
export interface PinnedDefinition<Inputs extends InputMap, O, E, R, OR = never> extends Common {
  readonly inputs: Inputs;
  readonly output: StreamFamily.Member<O, OR>;
  readonly process: (batch: Slices<Inputs>, unit: Unit) => Effect.Effect<ReadonlyArray<O>, E, R>;
}
export interface SinglePinnedDefinition<A, O, E, R, IR = never, OR = never> extends Common {
  readonly input: StreamFamily.Member<A, IR>;
  readonly output: StreamFamily.Member<O, OR>;
  readonly process: (
    batch: Slices<{ readonly input: StreamFamily.Member<A, IR> }>,
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
export function make<A, E, R, IR>(
  definition: SingleFusedDefinition<A, E, R, IR>,
): Fused<{ readonly input: StreamFamily.Member<A, IR> }, E, R>;
export function make<Inputs extends InputMap, E, R>(
  definition: FusedDefinition<Inputs, E, R>,
): Fused<Inputs, E, R>;
export function make<Inputs extends InputMap, A, E, R, IR>(
  definition: FusedDefinition<Inputs, E, R> | SingleFusedDefinition<A, E, R, IR>,
): Fused<Inputs, E, R> | Fused<{ readonly input: StreamFamily.Member<A, IR> }, E, R> {
  const base = { _tag: "Fused", ...common(definition) } as const;
  return "inputs" in definition
    ? { ...base, inputs: definition.inputs, process: definition.process }
    : { ...base, inputs: { input: definition.input }, process: definition.process };
}

/** Inert stream form with a declared output on the checkpoint owner. */
export function stream<A, O, E, R, IR, OR>(
  definition: SinglePinnedDefinition<A, O, E, R, IR, OR>,
): Pinned<{ readonly input: StreamFamily.Member<A, IR> }, O, E, R, OR>;
export function stream<Inputs extends InputMap, O, E, R, OR>(
  definition: PinnedDefinition<Inputs, O, E, R, OR>,
): Pinned<Inputs, O, E, R, OR>;
export function stream<Inputs extends InputMap, A, O, E, R, IR, OR>(
  definition: PinnedDefinition<Inputs, O, E, R, OR> | SinglePinnedDefinition<A, O, E, R, IR, OR>,
):
  | Pinned<Inputs, O, E, R, OR>
  | Pinned<{ readonly input: StreamFamily.Member<A, IR> }, O, E, R, OR> {
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
