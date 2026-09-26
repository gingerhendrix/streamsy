import type { TargetServices, RoutedMap } from "./output-internal.ts";
/* oxlint-disable typescript/no-explicit-any, typescript/no-unsafe-type-assertion, anti-slop/no-unsafe-dictionary-type, anti-slop/require-safety-comment-for-type-assertion -- This mapped adapter preserves each declaration's schema and name while iterating its record. */
import { Effect, Option, Schema } from "effect";
import { StreamAddress, type Streamsy } from "@streamsy/core";
import { type StreamRoute } from "@streamsy/core/protocol";
import type * as Output from "./output.ts";
import type { ProjectionKey } from "./checkpoint.ts";
import { State, decodeState, type StateDefinition, type StateSchema } from "./state.ts";

type Collection<Name extends string, A> = {
  readonly [K in Name]: {
    readonly schema: Schema.Codec<A, unknown, never, never>;
    readonly key: StreamAddress.StateKey<A>;
  };
};
export type Bound<D extends RoutedMap, P> = {
  readonly [K in keyof D]: D[K] & {
    readonly paramSchema: StreamRoute.PathSchema<P>;
  } & (D[K] extends Output.Stream<infer A, any>
      ? {
          readonly template: string;
          readonly address: (params: P) => Streamsy.Address<A, TargetServices<D[K]["stream"]>>;
        }
      : D[K] extends Output.Rows<infer A, any>
        ? {
            readonly template: string;
            readonly collections: Collection<K & string, A>;
            readonly address: (
              params: P,
            ) => StreamAddress.StateAddress<Collection<K & string, A>> &
              Streamsy.Address<unknown, TargetServices<D[K]["stream"]>>;
          }
        : never);
};

/** The declared state as a source for `Serve.value`. It fails while the row is absent. */
export interface StateSource<A, I, P> {
  readonly id: string;
  readonly paramSchema: StreamRoute.PathSchema<P>;
  readonly schema: StateSchema<A, I>;
  readonly resolve: (
    params: P,
  ) => Effect.Effect<A, { readonly _tag: "ValueUnavailable"; readonly detail: string }, State>;
}

export const stateSource = <A, I, P>(
  definition: { readonly id: string; readonly params: StreamRoute.ParamCodecs },
  state: StateDefinition<A, I, P>,
  at: (params: P) => ProjectionKey,
): StateSource<A, I, P> => {
  const unavailable = (detail: string) => ({ _tag: "ValueUnavailable" as const, detail });
  return {
    id: `${definition.id}/state`,
    // Struct owns exactly the codec keys and their decoded parameter types.
    paramSchema: Schema.Struct(definition.params) as StreamRoute.PathSchema<P>,
    schema: state.schema,
    resolve: (params) =>
      Effect.gen(function* () {
        const bound = yield* Effect.try({
          try: () => at(params),
          catch: () => unavailable("Cannot resolve projection params"),
        });
        const key: ProjectionKey = {
          id: bound.id,
          version: bound.version,
          generation: bound.generation,
          params: bound.params,
        };
        const value = yield* Effect.flatMap(State, (store) => store.load(key)).pipe(
          Effect.flatMap((encoded) => decodeState(key, state.schema, encoded)),
          Effect.mapError(() => unavailable("Cannot read projection state")),
        );
        return Option.isSome(value)
          ? value.value
          : yield* Effect.fail(unavailable("Projection state is absent"));
      }),
  };
};

/** The kernel and served readers build rows through this one protocol codec. */
export const rowsAddress = <A, R>(
  name: string,
  output: Output.Rows<A, Streamsy.Address<unknown, R>>,
) =>
  Object.assign(
    StreamAddress.state(output.stream.id, {
      collections: { [name]: { schema: output.schema, key: output.key } },
    }),
    { stream: output.stream.stream },
  );

export function bind<D extends RoutedMap, P>(
  declarations: D,
  id: string,
  paramSchema: StreamRoute.PathSchema<P>,
  keyAt: (params: P) => ProjectionKey & { readonly outputs: Output.Map },
): Bound<D, P> {
  const sources: Readonly<Record<string, unknown>> = Object.fromEntries(
    Object.entries(declarations).map(([name, declaration]) => {
      const common = { ...declaration, paramSchema };
      const address = (params: P) => {
        const output = keyAt(params).outputs[name]!;
        return output._tag === "Stream" ? output.stream : rowsAddress(name, output);
      };
      return [
        name,
        declaration._tag === "Rows"
          ? {
              ...common,
              template: `${id}/${name}`,
              collections: { [name]: { schema: declaration.schema, key: declaration.key } },
              address,
            }
          : { ...common, template: `${id}/${name}`, address },
      ];
    }),
  );
  // Each entry retains its declaration, and the tag selects exactly its mapped source shape.
  return sources as Bound<D, P>;
}
