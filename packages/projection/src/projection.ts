import { Effect } from "effect";
import type { Entry, InputMap, Slices } from "./batch.ts";
import type { Identity, ProjectionKey } from "./checkpoint-store.ts";
import type { Unit } from "./unit.ts";
import { items } from "./read.ts";
export { fold } from "./fold.ts";
export { process } from "./process.ts";
export { emit } from "./emit.ts";
export { mapAccum, mapAccumBatch } from "./map-accum.ts";
export { atomic } from "./atomic.ts";
export { outputs } from "./outputs.ts";
export { pass, run } from "./run.ts";
export { follow } from "./follow.ts";
export { watch } from "./watch.ts";
export { items };

export const key = (projection: Identity): ProjectionKey => ({
  id: projection.id,
  version: projection.version,
  generation: projection.generation,
  params: projection.params,
});

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

export { read } from "./snapshot.ts";
export { forget } from "./forget.ts";
