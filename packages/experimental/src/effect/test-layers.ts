import { Effect, Layer } from "effect";

/** Provide one fully composed layer at the Vitest or Bun executable test boundary. */
export function provideTestLayers<A, E, R, ROut, E2, RIn>(
  program: Effect.Effect<A, E, R>,
  layer: Layer.Layer<ROut, E2, RIn>,
): Effect.Effect<A, E | E2, RIn | Exclude<R, ROut>> {
  // @effect-diagnostics-next-line strictEffectProvide:off -- Vitest and Bun execute the returned Effect at this shared test boundary.
  return Effect.provide(program, layer);
}
