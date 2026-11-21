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

export { attach } from "./attach.ts";

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

export { current } from "./current.ts";

export { read } from "./snapshot.ts";

export { bindings } from "./bindings.ts";

export { tails, lag, caughtUp, Tails, type InputLag, type Lag } from "./catch-up.ts";

export { decide } from "./decide.ts";

export { forget } from "./forget.ts";

export { retire, type Retired, type RetiredOutput } from "./retire.ts";

export type { RoutedHandle } from "./routed-outputs.ts";

export {
  generationBinding,
  isPerGeneration,
  perGeneration,
  type PerGeneration,
} from "./per-generation.ts";

export { createOutputs } from "./outputs.ts";

export { wake, type WakeOutput } from "./wake.ts";

export {
  Registry,
  type RegistryApi,
  RunnerEvent,
  type RunnerStatus,
  type RunnerFailure,
} from "./registry.ts";

export { wakeAt, type WakeAt } from "./wake-at.ts";
