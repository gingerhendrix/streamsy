import { Effect, Schema } from "effect";
import type { StreamId, StreamRef, StreamRoute } from "@streamsy/core";
import type { CanonicalValue, PublicError } from "./contract.ts";

/** Structural family consumed by Serve, including bound projection outputs. */
export interface StreamSource<P> {
  readonly template: string;
  readonly paramSchema?: StreamRoute.PathSchema<P>;
  readonly ref: (params: P) => { readonly id: StreamId };
}
export interface StateSource<P, C extends StreamRef.Collections> extends StreamSource<P> {
  readonly collections: C;
  readonly ref: (params: P) => StreamRef.StateRef<C>;
}
export type PathSource<P> = { readonly paramSchema: StreamRoute.PathSchema<P> };

/** Internal resolution seam: projection-output adapters can supply their own resolver. */
export interface Source<P, R = never> {
  readonly kind: "stream" | "state";
  readonly template: string;
  readonly paramSchema: StreamRoute.PathSchema<P> | undefined;
  readonly resolve: (params: P) => Effect.Effect<
    {
      readonly id: StreamId;
      readonly descriptor: CanonicalValue;
    },
    Extract<PublicError, { readonly _tag: "InvalidParams" }>,
    R
  >;
}

export function normalize<P>(source: StreamSource<P>, path: string): Source<P>;
export function normalize<P, C extends StreamRef.Collections>(
  source: StateSource<P, C>,
  path: string,
  kind: "state",
): Source<P>;
export function normalize<P>(
  source: StreamSource<P>,
  path: string,
  kind: "stream" | "state" = "stream",
): Source<P> {
  const descriptor =
    kind === "state"
      ? collectionDescriptor("collections" in source ? source.collections : undefined)
      : null;
  return {
    kind,
    template: source.template,
    paramSchema: source.paramSchema,
    resolve: (params) =>
      Effect.try({
        try: () => {
          const ref = source.ref(params);
          return { id: ref.id, descriptor };
        },
        catch: () => ({
          _tag: "InvalidParams" as const,
          route: path,
          parameter: "params",
          detail: "Parameters cannot form a canonical stream id",
        }),
      }),
  };
}

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- Structural family metadata is decoded immediately through the collection descriptor Schema.
function collectionDescriptor(value: unknown): CanonicalValue {
  const collections = Schema.decodeUnknownSync(
    Schema.Record(
      Schema.String,
      Schema.Struct({ key: Schema.Union([Schema.String, Schema.Finite]) }),
    ),
  )(value);
  return Object.fromEntries(
    Object.entries(collections).map(([type, collection]) => [type, collection.key]),
  );
}
