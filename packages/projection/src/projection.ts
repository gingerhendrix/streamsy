import { Effect } from "effect";
import type { Entry, InputMap, Slices } from "./batch.ts";
import type { Identity, ProjectionKey } from "./checkpoint-store.ts";
import type { Unit } from "./unit.ts";
import { items } from "./read.ts";
export { fold } from "./fold.ts";
export { process } from "./process.ts";
export { emit } from "./emit.ts";
export { mapAccum, mapAccumBatch } from "./map-accum.ts";
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

/** Runs each tagged entry in order and discards its success value. */
export const each =
  <Inputs extends InputMap, E, R, P = Record<string, string>>(
    handle: (entry: Entry<Inputs>, unit: Unit<P>) => Effect.Effect<unknown, E, R>,
  ): ((batch: Slices<Inputs>, unit: Unit<P>) => Effect.Effect<void, E, R>) =>
  (batch, unit) =>
    Effect.forEach(items(batch), (entry) => handle(entry, unit), { discard: true });

export { read } from "./snapshot.ts";
export { forget } from "./forget.ts";
