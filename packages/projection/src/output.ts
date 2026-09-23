/* oxlint-disable anti-slop/no-runtime-typeof -- Output stream options explicitly accept either a string id or an inert ref. */
import { Schema, type Effect } from "effect";
import { StreamRef } from "@streamsy/core";

export interface Upsert<A> {
  readonly _tag: "Upsert";
  readonly row: A;
}
export interface Remove {
  readonly _tag: "Remove";
  readonly key: string | number;
}
export const upsert = <A>(row: A): Upsert<A> => ({ _tag: "Upsert", row });
export const remove = (key: string | number): Remove => ({ _tag: "Remove", key });
export type Change<A> = Upsert<A> | Remove;

export interface Stream<A, Identity = StreamRef.StreamRef<A>> {
  readonly _tag: "Stream";
  readonly schema: Schema.Codec<A, unknown, never, never>;
  readonly stream: Identity;
}
export interface Rows<A, Identity = string> {
  readonly _tag: "Rows";
  readonly schema: Schema.Codec<A, unknown, never, never>;
  readonly key: StreamRef.StateKey<A>;
  readonly stream: Identity;
}
export interface Value<A, I = unknown> {
  readonly _tag: "Value";
  readonly schema: Schema.Codec<A, I, never, never>;
}
/** Routed targets supply identity only; the declaration owns the JSON codec. */
export interface Target<P> {
  readonly ref: (params: P) => { readonly id: string };
}
export function stream<A, I, P>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly stream: Target<P> },
): Stream<A, Target<P>>;
export function stream<A, I>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly stream: string | { readonly id: string } },
): Stream<A>;
export function stream<A, I, P>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly stream: string | { readonly id: string } | Target<P> },
): Stream<A, StreamRef.StreamRef<A> | Target<P>> {
  const target = options.stream;
  return {
    _tag: "Stream",
    schema,
    stream:
      typeof target !== "string" && "ref" in target
        ? target
        : StreamRef.json(typeof target === "string" ? target : target.id, { schema }),
  };
}
/** The output name is the Durable State collection type. Deletes need only a key. */
export function rows<A, I, P>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly key: StreamRef.StateKey<A>; readonly stream: Target<P> },
): Rows<A, Target<P>>;
export function rows<A, I>(
  schema: Schema.Codec<A, I, never, never>,
  options: {
    readonly key: StreamRef.StateKey<A>;
    readonly stream: string | { readonly id: string };
  },
): Rows<A>;
export function rows<A, I, P>(
  schema: Schema.Codec<A, I, never, never>,
  options: {
    readonly key: StreamRef.StateKey<A>;
    readonly stream: string | { readonly id: string } | Target<P>;
  },
): Rows<A, string | Target<P>> {
  const target = options.stream;
  return {
    _tag: "Rows",
    schema,
    key: options.key,
    stream: typeof target === "string" ? target : "ref" in target ? target : target.id,
  };
}
export const value = <A, I>(schema: Schema.Codec<A, I, never, never>): Value<A, I> => ({
  _tag: "Value",
  schema,
});

// The mapped public result recovers each declaration's item type before runtime erasure.
export type Declaration = Stream<any> | Rows<any> | Value<any, any>;
export type RoutedMap = Readonly<
  Record<string, Stream<any, Target<any>> | Rows<any, Target<any>> | Declaration>
>;
export type Map = Readonly<Record<string, Declaration>>;
export type Items<D extends RoutedMap> = {
  readonly [K in keyof D as D[K] extends Value<any, any> ? never : K]: ReadonlyArray<
    D[K] extends Stream<infer A, any> ? A : D[K] extends Rows<infer A, any> ? Change<A> : never
  >;
};
export type StateOf<D extends RoutedMap> = {
  [K in keyof D]: D[K] extends Value<infer A, any> ? A : never;
}[keyof D];
export type Result<D extends RoutedMap> = Items<D> &
  ([StateOf<D>] extends [never] ? {} : { readonly state: StateOf<D> });
export interface Processed {
  readonly items: Readonly<Record<string, ReadonlyArray<unknown>>>;
  readonly saveState?: Effect.Effect<void, import("./fault.ts").ProjectionFault>;
}
