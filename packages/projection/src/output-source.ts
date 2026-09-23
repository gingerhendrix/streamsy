/* oxlint-disable typescript/no-explicit-any, typescript/no-unsafe-type-assertion, anti-slop/no-unsafe-dictionary-type, anti-slop/require-safety-comment-for-type-assertion -- This mapped adapter preserves each declaration's schema and name while iterating its record. */
import { Effect, Option, type Schema } from "effect";
import { StreamRef, type StreamRoute } from "@streamsy/core";
import type * as Output from "./output.ts";
import type { ProjectionKey } from "./checkpoint.ts";
import { loadState } from "./fold.ts";
import type { State } from "./state.ts";

export type Fixed<D extends Output.RoutedMap> = {
  readonly [K in keyof D]: D[K] extends Output.Stream<infer A, any>
    ? Output.Stream<A>
    : D[K] extends Output.Rows<infer A, any>
      ? Output.Rows<A>
      : D[K];
};
type Collection<Name extends string, A> = {
  readonly [K in Name]: {
    readonly schema: Schema.Codec<A, unknown, never, never>;
    readonly key: StreamRef.StateKey<A>;
  };
};
export type Bound<D extends Output.RoutedMap, P> = {
  readonly [K in keyof D]: D[K] & {
    readonly paramSchema: StreamRoute.PathSchema<P>;
  } & (D[K] extends Output.Stream<infer A, any>
      ? { readonly template: string; readonly ref: (params: P) => StreamRef.StreamRef<A> }
      : D[K] extends Output.Rows<infer A, any>
        ? {
            readonly template: string;
            readonly collections: Collection<K & string, A>;
            readonly ref: (params: P) => StreamRef.StateRef<Collection<K & string, A>>;
          }
        : D[K] extends Output.Value<infer A, any>
          ? {
              readonly id: string;
              readonly resolve: (
                params: P,
              ) => Effect.Effect<
                A,
                { readonly _tag: "ValueUnavailable"; readonly detail: string },
                State
              >;
            }
          : never);
};

/** The kernel and served readers build rows through this one protocol codec. */
export const rowsRef = <A>(name: string, output: Output.Rows<A>) =>
  StreamRef.state(output.stream, {
    collections: { [name]: { schema: output.schema, key: output.key } },
  });

export function bind<D extends Output.RoutedMap, P>(
  declarations: D,
  id: string,
  paramSchema: StreamRoute.PathSchema<P>,
  member: (params: P) => ProjectionKey & { readonly outputs: Output.Map },
): Bound<D, P> {
  const sources = Object.fromEntries(
    Object.entries(declarations).map(([name, declaration]) => {
      const common = { ...declaration, paramSchema };
      if (declaration._tag === "Value")
        return [
          name,
          {
            ...common,
            id: `${id}/${name}`,
            resolve: (params: P) =>
              Effect.gen(function* () {
                const key = yield* Effect.try({
                  try: () => member(params),
                  catch: () => ({
                    _tag: "ValueUnavailable" as const,
                    detail: "Cannot resolve projection member",
                  }),
                });
                const value = yield* loadState(key, declaration.schema).pipe(
                  Effect.mapError(() => ({
                    _tag: "ValueUnavailable" as const,
                    detail: "Cannot read projection state",
                  })),
                );
                return Option.isSome(value)
                  ? value.value
                  : yield* Effect.fail({
                      _tag: "ValueUnavailable" as const,
                      detail: "Projection state is absent",
                    });
              }),
          },
        ];
      const ref = (params: P) => {
        const output = member(params).outputs[name]!;
        if (output._tag === "Value") throw new TypeError("Expected a stream output");
        return output._tag === "Stream" ? output.stream : rowsRef(name, output);
      };
      return [
        name,
        declaration._tag === "Rows"
          ? {
              ...common,
              template: `${id}/${name}`,
              collections: { [name]: { schema: declaration.schema, key: declaration.key } },
              ref,
            }
          : { ...common, template: `${id}/${name}`, ref },
      ];
    }),
  );
  // Each entry retains its declaration, and the tag selects exactly its mapped source shape.
  return sources as Bound<D, P>;
}
