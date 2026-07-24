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

/**
 * `risk-demo-v2` map kernel. Version-discriminated alongside v1: v2 applies to new
 * games only, and the aggregate/projection that fold its combat events arrive in
 * later slices. The generator runs once per game inside the start-game command
 * service; replay reads the map snapshot recorded in `GameStarted`.
 */
export * from "./domain/hex.ts";
export * from "./domain/generator-rng.ts";
export * from "./domain/map-names.ts";
export * from "./domain/map-v2.ts";
export * from "./domain/hex-generator.ts";
export * from "./domain/setup-v2.ts";
export * from "./domain/events-v2.ts";
