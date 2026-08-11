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
 * Persistence, HTTP, capability, and UI concerns live outside these pure
 * functions.
 */

export * from "./domain/map.ts";
export * from "./domain/rng.ts";
export * from "./domain/dice.ts";
export * from "./domain/events.ts";
export * from "./domain/commands.ts";
export * from "./domain/aggregate.ts";
export * from "./domain/decide.ts";
export * from "./domain/hex.ts";
export * from "./domain/generator-rng.ts";
export * from "./domain/map-names.ts";
export * from "./domain/hex-generator.ts";
export * from "./domain/setup.ts";
export * from "./board/projection.ts";
