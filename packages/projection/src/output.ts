import type { Target, TargetServices, RoutedMap } from "./output-internal.ts";
import { Schema, type Effect } from "effect";
import { StreamAddress, type Streamsy } from "@streamsy/core";

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

export interface Stream<A, Identity = Streamsy.Address<A, unknown>> {
  readonly _tag: "Stream";
  readonly schema: Schema.Codec<A, unknown, never, never>;
  readonly stream: Identity;
}
export interface Rows<A, Identity = Streamsy.Address<unknown, unknown>> {
  readonly _tag: "Rows";
  readonly schema: Schema.Codec<A, unknown, never, never>;
  readonly key: StreamAddress.StateKey<A>;
  readonly stream: Identity;
}
export type Services<D extends RoutedMap> = {
  [K in keyof D]: D[K] extends { readonly stream: infer T } ? TargetServices<T> : never;
}[keyof D];
export function stream<A, I, T extends Target<any>>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly stream: T },
): Stream<A, T>;
export function stream<A, I, R>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly stream: Streamsy.Address<unknown, R> },
): Stream<A, Streamsy.Address<A, R>>;
export function stream<A, I, R>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly stream: Target<any> | Streamsy.Address<unknown, R> },
): Stream<A, Target<any> | Streamsy.Address<A, R>> {
  const target = options.stream;
  return {
    _tag: "Stream",
    schema,
    stream:
      "address" in target
        ? target
        : Object.assign(StreamAddress.json(target.id, { schema }), { stream: target.stream }),
  };
}
/** The output name is the Durable State collection type. Deletes need only a key. */
export function rows<A, I, T extends Target<any> | Streamsy.Address<unknown, any>>(
  schema: Schema.Codec<A, I, never, never>,
  options: { readonly key: StreamAddress.StateKey<A>; readonly stream: T },
): Rows<A, T> {
  return { _tag: "Rows", schema, key: options.key, stream: options.stream };
}

// The mapped public result recovers each declaration's item type before runtime erasure.
export type Declaration = Stream<any> | Rows<any>;
export type Map = Readonly<Record<string, Declaration>>;
export type Items<D extends RoutedMap> = {
  readonly [K in keyof D]: ReadonlyArray<
    D[K] extends Stream<infer A, any> ? A : D[K] extends Rows<infer A, any> ? Change<A> : never
  >;
};
/** The handler result: one array per output, and the next state when the declaration has `state:`. */
export type Result<D extends RoutedMap, S = never> = Items<D> &
  ([S] extends [never] ? {} : { readonly state: S });
export interface Processed {
  readonly items: Readonly<Record<string, ReadonlyArray<unknown>>>;
  readonly saveState?: Effect.Effect<void, import("./fault.ts").ProjectionFault>;
}
