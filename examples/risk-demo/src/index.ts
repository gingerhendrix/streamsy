/**
 * `risk-demo` — deterministic, headless event-sourced game kernel.
 *
 * The kernel is the executable specification for the Streamsy Risk demo:
 *  - {@link foldAggregate} folds canonical {@link GameEvent}s into the decision model;
 *  - {@link decide} validates commands against that fold and resolves randomness
 *    exactly once into recorded events;
 *  - {@link projectEvents} independently derives the query-shaped board projection;
 *  - the two are cross-checked with {@link boardsEqual}.
 *
 * No persistence, materializer, REST, capability, or UI concerns live here — those
 * arrive in later batches and build on these pure functions.
 */

export * from "./domain/map.ts";
export * from "./domain/rng.ts";
export * from "./domain/dice.ts";
export * from "./domain/events.ts";
export * from "./domain/commands.ts";
export * from "./domain/aggregate.ts";
export * from "./domain/decide.ts";
export * from "./board/projection.ts";
