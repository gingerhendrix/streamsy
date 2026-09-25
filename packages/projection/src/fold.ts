/* oxlint-disable typescript/no-unsafe-type-assertion, anti-slop/require-safety-comment-for-type-assertion -- The overload arity selects the two-argument output descriptor or the three-argument fused fold; assertions only recover types checked by those overloads. */
import { Effect, Option, Schema } from "effect";
import type { Entry, InputMap, Slices } from "./batch.ts";
import type { ProjectionKey } from "./checkpoint.ts";
import { ProjectionFault } from "./fault.ts";
import { entries } from "./read.ts";
import { State } from "./state.ts";
import { encodedParams, type Unit } from "./unit.ts";

export type StateSchema<A, I> = Schema.Codec<A, I, never, never>;
/** A value, or a function of the decoded params the projection was bound with. */
export type Initial<A, P = Record<string, string>> = A | ((params: P) => A);
export type FoldHandler<Inputs extends InputMap, E, R, P = Record<string, string>> = (
  batch: Slices<Inputs>,
  unit: Unit<P>,
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
export interface OutputFold<Inputs extends InputMap, A, Result, E, R, P = Record<string, string>> {
  readonly _tag: "OutputFold";
  readonly initial: Initial<A, P>;
  readonly step: (
    state: A,
    batch: Slices<Inputs>,
    unit: Unit<P>,
  ) => Result | Effect.Effect<Result, E, R>;
}
export function fold<
  Inputs extends InputMap,
  A,
  Result extends { readonly state: A },
  E = never,
  R = never,
  P = Record<string, string>,
>(
  initial: Initial<A, P>,
  step: (state: A, batch: Slices<Inputs>, unit: Unit<P>) => Result | Effect.Effect<Result, E, R>,
): OutputFold<Inputs, A, Result, E, R, P>;
/** Loads once, folds in unit order, and saves once inside the fused handler's transaction. */
export function fold<Inputs extends InputMap, A, I, P = Record<string, string>>(
  schema: StateSchema<A, I>,
  initial: Initial<A, P>,
  step: (state: A, entry: Entry<Inputs>, unit: Unit<P>) => A,
): FoldHandler<Inputs, never, never, P>;
export function fold<Inputs extends InputMap, A, I, E, R, P = Record<string, string>>(
  schema: StateSchema<A, I>,
  initial: Initial<A, P>,
  step: (state: A, entry: Entry<Inputs>, unit: Unit<P>) => Effect.Effect<A, E, R>,
): FoldHandler<Inputs, E, R, P>;
export function fold<Inputs extends InputMap, A, I, E, R, P>(
  schema: StateSchema<A, I> | Initial<A, P>,
  initial: Initial<A, P> | ((state: A, batch: Slices<Inputs>, unit: Unit<P>) => any),
  step?: (state: A, entry: Entry<Inputs>, unit: Unit<P>) => A | Effect.Effect<A, E, R>,
): FoldHandler<Inputs, E, R, P> | OutputFold<Inputs, A, any, E, R, P> {
  if (step === undefined)
    return {
      _tag: "OutputFold",
      initial: schema as Initial<A, P>,
      step: initial as OutputFold<Inputs, A, any, E, R, P>["step"],
    };
  const stateSchema = schema as StateSchema<A, I>;
  const start = initial as Initial<A, P>;
  return (batch, unit) =>
    Effect.gen(function* () {
      const key: ProjectionKey = {
        id: unit.projectionId,
        version: unit.version,
        generation: unit.generation,
        params: encodedParams(unit),
      };
      const previous = yield* loadState(key, stateSchema);
      // SAFETY: JSON state values are not functions; a callable initial value is the params factory.
      let value = Option.isSome(previous)
        ? previous.value
        : // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Initial explicitly accepts a value or a params factory; this dispatch is the API contract.
          typeof start === "function"
          ? (start as (params: P) => A)(unit.params)
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
