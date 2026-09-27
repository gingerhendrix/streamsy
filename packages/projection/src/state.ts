import { attachOwner, ownerOf } from "@streamsy/core/internal/memory";
import { Context, Effect, Option, Schema } from "effect";
import {
  recordKey,
  type CheckpointsApi,
  type EncodedStore,
  type ProjectionKey,
} from "./checkpoint.ts";
import { ProjectionFault } from "./fault.ts";

export interface StateApi {
  /** Internal: read the state through `Projection.read`. */
  readonly load: (key: ProjectionKey) => Effect.Effect<Option.Option<string>, ProjectionFault>;
  /** Internal: overwrites the row; must run inside the owner transaction. */
  readonly save: (key: ProjectionKey, encoded: string) => Effect.Effect<void, ProjectionFault>;
  /** Internal: deletes the row inside the owner transaction. Absent is not an error. */
  readonly remove: (key: ProjectionKey) => Effect.Effect<void, ProjectionFault>;
}
/** One encoded state row per projection key, sharing the checkpoint's owner transaction. */
export class State extends Context.Service<State, StateApi>()("@streamsy/projection/State") {}

export const stateFromStore = (store: EncodedStore): StateApi =>
  attachOwner(
    {
      load: (key) => store.readState(recordKey(key)),
      save: (key, encoded) => store.writeState(recordKey(key), encoded),
      remove: (key) => store.removeState(recordKey(key)),
    },
    ownerOf(store),
  );

/**
 * The state row must commit in the checkpoint owner's transaction. A projection with
 * `state:` and a `State` service from another owner fails with
 * `load / unsupported-composition` before it reads or processes.
 */
export const checkStateOwner = Effect.fnUntraced(function* (
  projection: { readonly id: string; readonly state?: unknown },
  checkpoints: CheckpointsApi,
) {
  if (projection.state === undefined) return;
  // The declaration type requires State whenever `state:` is set.
  const store = yield* Effect.serviceOption(State);
  if (Option.isNone(store) || ownerOf(store.value) === checkpoints.owner) return;
  return yield* new ProjectionFault({
    phase: "load",
    reason: "unsupported-composition",
    message: `State of ${projection.id} does not share the checkpoint owner`,
  });
});

export type StateSchema<A, I = unknown> = Schema.Codec<A, I, never, never>;
/** A value, or a function of the decoded params the projection was bound with. */
export type Initial<A, P = Record<string, string>> = A | ((params: P) => A);
/** The `state:` field of a declaration. */
export interface StateDefinition<A, I = unknown, P = Record<string, string>> {
  readonly schema: StateSchema<A, I>;
  readonly initial: Initial<A, P>;
}
/** The state of a bound projection. `initial` runs only when the row is absent. */
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

/** Decodes a stored row. `None` means the row is absent. */
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

/** The stored state, or the initial value when the row is absent. */
export const currentState = <A>(
  key: ProjectionKey,
  state: BoundState<A>,
): Effect.Effect<A, ProjectionFault, State> =>
  Effect.gen(function* () {
    const stored = yield* decodeState(key, state.schema, yield* (yield* State).load(key));
    return Option.getOrElse(stored, state.initial);
  });

export const encodeState = <A>(
  key: ProjectionKey,
  schema: StateSchema<A>,
  value: A,
  phase: "process" | "checkpoint",
): Effect.Effect<string, ProjectionFault> =>
  Schema.encodeEffect(Schema.fromJsonString(schema))(value).pipe(
    Effect.mapError(
      (cause) =>
        new ProjectionFault({
          phase,
          reason: phase === "process" ? "invalid-output" : "invalid-record",
          message: `Cannot encode state for ${key.id}`,
          cause,
        }),
    ),
  );
