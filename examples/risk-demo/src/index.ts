/**
 * `risk-demo` — deterministic, headless event-sourced game kernel.
 *
 * The kernel is the executable specification for the Streamsy Risk demo:
 *  - {@link foldAggregate} folds canonical {@link GameEvent}s into the decision model;
 *  - {@link decide} / {@link applyCommand} validate commands against that fold and
 *    append events, resolving any randomness exactly once;
 *  - {@link projectEvents} independently derives the query-shaped board projection;
 *  - the two are cross-checked with {@link boardsEqual}.
 *
 * No persistence, materializer, REST, capability, or UI concerns live here — those
 * arrive in later batches and build on these pure functions.
 */

export * from "./map.ts";
export * from "./rng.ts";
export * from "./dice.ts";
export * from "./events.ts";
export * from "./commands.ts";
export * from "./aggregate.ts";
export * from "./decide.ts";
export * from "./engine.ts";
export * from "./projection.ts";
