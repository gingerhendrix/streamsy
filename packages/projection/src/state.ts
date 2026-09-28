import { Data, Effect, Option, Schema } from "effect";
import type { StreamRoute } from "@streamsy/core/protocol";
import { CheckpointStore, restore, type Identity, type ProjectionKey } from "./checkpoint-store.ts";
import { ProjectionFault } from "./fault.ts";

export type StateSchema<A, I = unknown> = Schema.Codec<A, I, never, never>;
/** A value, or a function of the decoded params the projection was bound with. */
export type Initial<A, P = Record<string, string>> = A | ((params: P) => A);
/** The `state:` field of a declaration. */
export interface StateDefinition<A, I = unknown, P = Record<string, string>> {
  readonly schema: StateSchema<A, I>;
  readonly initial: Initial<A, P>;
}
/** The state of a bound projection. `initial` runs only when stored state is absent. */
export interface BoundState<A> {
  readonly schema: StateSchema<A>;
  readonly initial: () => A;
}

export const bindState = <A, P>(
  definition: StateDefinition<A, unknown, P>,
  params: P,
): BoundState<A> => {
  const initial = definition.initial;
  return {
    schema: definition.schema,
    initial: () =>
      // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Initial explicitly accepts a value or a params factory; this dispatch is the API contract.
      typeof initial === "function"
        ? // SAFETY: JSON state values are not functions; a callable initial value is the params factory.
          // oxlint-disable-next-line typescript/no-unsafe-type-assertion
          (initial as (params: P) => A)(params)
        : initial,
  };
};

/** Decodes the state field. `None` means no state is stored. */
export const decodeState = <A>(
  key: ProjectionKey,
  schema: StateSchema<A>,
  encoded: Option.Option<string>,
): Effect.Effect<Option.Option<A>, ProjectionFault> =>
  Option.isNone(encoded)
    ? Effect.succeed(Option.none())
    : Schema.decodeEffect(Schema.fromJsonString(schema))(encoded.value).pipe(
        Effect.map(Option.some),
        Effect.mapError(
          (cause) =>
            new ProjectionFault({
              phase: "load",
              reason: "invalid-record",
              message: `Invalid stored state for ${key.id}`,
              cause,
            }),
        ),
      );

/** The stored state, or the initial value when stored state is absent. */
export const currentState = <A>(
  key: ProjectionKey,
  state: BoundState<A>,
  encoded: string | undefined,
): Effect.Effect<A, ProjectionFault> =>
  Effect.gen(function* () {
    const stored = yield* decodeState(key, state.schema, Option.fromUndefinedOr(encoded));
    return Option.getOrElse(stored, state.initial);
  });

export const encodeState = <A>(
  key: ProjectionKey,
  schema: StateSchema<A>,
  value: A,
): Effect.Effect<string, ProjectionFault> =>
  Schema.encodeEffect(Schema.fromJsonString(schema))(value).pipe(
    Effect.mapError(
      (cause) =>
        new ProjectionFault({
          phase: "process",
          reason: "invalid-output",
          message: `Cannot encode state for ${key.id}`,
          cause,
        }),
    ),
  );

/** The declared state as a source for `Serve.value`. It fails while state is absent. */
export interface StateSource<A, I, P, R = CheckpointStore> {
  readonly id: string;
  readonly paramSchema: StreamRoute.PathSchema<P>;
  readonly schema: StateSchema<A, I>;
  readonly resolve: (
    params: P,
  ) => Effect.Effect<A, { readonly _tag: "ValueUnavailable"; readonly detail: string }, R>;
}

type ValueUnavailable = Data.TaggedEnum<{ ValueUnavailable: { readonly detail: string } }>;
const { ValueUnavailable } = Data.taggedEnum<ValueUnavailable>();

export const stateSource = <A, I, P, R>(
  definition: { readonly id: string; readonly params: StreamRoute.ParamCodecs },
  state: StateDefinition<A, I, P>,
  at: (
    params: P,
  ) => Identity & { readonly commit: Pick<import("./commit.ts").Commit<never, never, R>, "load"> },
): StateSource<A, I, P, R> => {
  const unavailable = (detail: string) => ValueUnavailable({ detail });
  return {
    id: `${definition.id}/state`,
    // SAFETY: Struct owns exactly the codec keys and their decoded parameter types.
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion
    paramSchema: Schema.Struct(definition.params) as StreamRoute.PathSchema<P>,
    schema: state.schema,
    resolve: (params) =>
      Effect.gen(function* () {
        const bound = yield* Effect.try({
          try: () => at(params),
          catch: () => unavailable("Cannot resolve projection params"),
        });
        const value = yield* Effect.gen(function* () {
          const loaded = yield* restore(bound, bound.commit);
          return yield* decodeState(
            bound,
            state.schema,
            Option.fromUndefinedOr(loaded.record.state),
          );
        }).pipe(Effect.mapError(() => unavailable("Cannot read projection state")));
        return Option.isSome(value)
          ? value.value
          : yield* Effect.fail(unavailable("Projection state is absent"));
      }),
  };
};
