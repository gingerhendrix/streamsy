import { Schema, type Effect } from "effect";
import { StreamRef, type StreamFamily } from "@streamsy/core";

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

export interface Stream<A, Identity = StreamFamily.Member<A, any>> {
  readonly _tag: "Stream";
  readonly schema: Schema.Codec<A, unknown, never, never>;
  readonly stream: Identity;
}
export interface Rows<A, Identity = StreamFamily.Member<unknown, any>> {
  readonly _tag: "Rows";
  readonly schema: Schema.Codec<A, unknown, never, never>;
  readonly key: StreamRef.StateKey<A>;
  readonly stream: Identity;
}
export interface Value<A, I = unknown> {
  readonly _tag: "Value";
  readonly schema: Schema.Codec<A, I, never, never>;
}
/** Output declarations own their codec and retain the target family key. */
export type Target<P> = StreamFamily.AnyFamily & {
  readonly ref: (params: P) => { readonly id: string };
};
export type TargetServices<T> = T extends { readonly family: { readonly Identifier: infer R } }
  ? R
  : T extends { readonly Identifier: infer R }
    ? R
    : never;
export type Services<D extends RoutedMap> = {
  [K in keyof D]: D[K] extends { readonly stream: infer T } ? TargetServices<T> : never;
}[keyof D];
export function stream<A, I, T extends Target<any>>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly stream: T },
): Stream<A, T>;
export function stream<A, I, R>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly stream: StreamFamily.Member<unknown, R> },
): Stream<A, StreamFamily.Member<A, R>>;
export function stream<A, I, R>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly stream: Target<any> | StreamFamily.Member<unknown, R> },
): Stream<A, Target<any> | StreamFamily.Member<A, R>> {
  const target = options.stream;
  return {
    _tag: "Stream",
    schema,
    stream:
      "ref" in target
        ? target
        : Object.assign(StreamRef.json(target.id, { schema }), { family: target.family }),
  };
}
/** The output name is the Durable State collection type. Deletes need only a key. */
export function rows<A, I, T extends Target<any> | StreamFamily.Member<unknown, any>>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly key: StreamRef.StateKey<A>; readonly stream: T },
): Rows<A, T> {
  return { _tag: "Rows", schema, key: options.key, stream: options.stream };
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
