/* oxlint-disable typescript/no-unsafe-type-assertion, anti-slop/require-safety-comment-for-type-assertion -- The overload arity selects the two-argument output descriptor or the three-argument fused fold; assertions only recover types checked by those overloads. */
import { Effect, Option, Schema } from "effect";
import type { Entry, InputMap, Slices } from "./batch.ts";
import type { ProjectionKey } from "./checkpoint.ts";
import { ProjectionFault } from "./fault.ts";
import { entries } from "./read.ts";
import { State } from "./state.ts";
import type { Unit } from "./unit.ts";

export type StateSchema<A, I> = Schema.Codec<A, I, never, never>;
export type Initial<A> = A | ((params: Record<string, string>) => A);
export type FoldHandler<Inputs extends InputMap, E, R> = (
  batch: Slices<Inputs>,
  unit: Unit,
) => Effect.Effect<void, E | ProjectionFault, R | State>;

/** A plain typed read; opens no transaction. */
export const loadState = <A, I>(
  projection: ProjectionKey,
  schema: StateSchema<A, I>,
): Effect.Effect<Option.Option<A>, ProjectionFault, State> =>
  Effect.gen(function* () {
    const encoded = yield* (yield* State).load(projection);
    if (Option.isNone(encoded)) return Option.none();
    const value = yield* Schema.decodeEffect(Schema.fromJsonString(schema))(encoded.value).pipe(
      Effect.mapError(
        (cause) =>
          new ProjectionFault({
            phase: "load",
            reason: "invalid-record",
            message: `Invalid stored state for ${projection.id}`,
            cause,
          }),
      ),
    );
    return Option.some(value);
  });

/** A stream-form fold runs once per reproduced batch; the kernel saves its state at settle. */
export interface OutputFold<Inputs extends InputMap, A, Result, E, R> {
  readonly _tag: "OutputFold";
  readonly initial: Initial<A>;
  readonly step: (
    state: A,
    batch: Slices<Inputs>,
    unit: Unit,
  ) => Result | Effect.Effect<Result, E, R>;
}
export function fold<
  Inputs extends InputMap,
  A,
  Result extends { readonly state: A },
  E = never,
  R = never,
>(
  initial: Initial<A>,
  step: (state: A, batch: Slices<Inputs>, unit: Unit) => Result | Effect.Effect<Result, E, R>,
): OutputFold<Inputs, A, Result, E, R>;
/** Loads once, folds in unit order, and saves once inside the fused handler's transaction. */
export function fold<Inputs extends InputMap, A, I>(
  schema: StateSchema<A, I>,
  initial: Initial<A>,
  step: (state: A, entry: Entry<Inputs>, unit: Unit) => A,
): FoldHandler<Inputs, never, never>;
export function fold<Inputs extends InputMap, A, I, E, R>(
  schema: StateSchema<A, I>,
  initial: Initial<A>,
  step: (state: A, entry: Entry<Inputs>, unit: Unit) => Effect.Effect<A, E, R>,
): FoldHandler<Inputs, E, R>;
export function fold<Inputs extends InputMap, A, I, E, R>(
  schema: StateSchema<A, I> | Initial<A>,
  initial: Initial<A> | ((state: A, batch: Slices<Inputs>, unit: Unit) => any),
  step?: (state: A, entry: Entry<Inputs>, unit: Unit) => A | Effect.Effect<A, E, R>,
): FoldHandler<Inputs, E, R> | OutputFold<Inputs, A, any, E, R> {
  if (step === undefined)
    return {
      _tag: "OutputFold",
      initial: schema as Initial<A>,
      step: initial as OutputFold<Inputs, A, any, E, R>["step"],
    };
  const stateSchema = schema as StateSchema<A, I>;
  const start = initial as Initial<A>;
  return (batch, unit) =>
    Effect.gen(function* () {
      const key: ProjectionKey = {
        id: unit.projectionId,
        version: unit.version,
        generation: unit.generation,
        params: unit.params,
      };
      const previous = yield* loadState(key, stateSchema);
      // SAFETY: JSON state values are not functions; a callable initial value is the params factory.
      let value = Option.isSome(previous)
        ? previous.value
        : // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Initial explicitly accepts a value or a params factory; this dispatch is the API contract.
          typeof start === "function"
          ? (start as (params: Record<string, string>) => A)(unit.params)
          : start;
      // SAFETY: each entry's item was decoded by the codec of the input it is tagged with.
      const items = entries(batch) as ReadonlyArray<Entry<Inputs>>;
      for (const entry of items) {
        const next = step(value, entry, unit);
        value = Effect.isEffect(next) ? yield* next : next;
      }
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(stateSchema))(value).pipe(
        Effect.mapError(
          (cause) =>
            new ProjectionFault({
              phase: "checkpoint",
              reason: "invalid-record",
              message: `Cannot encode state for ${key.id}`,
              cause,
            }),
        ),
      );
      yield* (yield* State).save(key, encoded);
    });
}
