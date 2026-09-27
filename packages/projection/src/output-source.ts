/* oxlint-disable typescript/no-explicit-any, typescript/no-unsafe-type-assertion, anti-slop/no-unsafe-dictionary-type, anti-slop/require-safety-comment-for-type-assertion -- This mapped adapter preserves each declaration's schema and name while iterating its record. */
import type { TargetServices, RoutedMap } from "./output-internal.ts";
import { Schema } from "effect";
import { StreamAddress, type Streamsy } from "@streamsy/core";
import { type StreamRoute } from "@streamsy/core/protocol";
import type * as Output from "./output.ts";
import type { ProjectionKey } from "./checkpoint.ts";

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
