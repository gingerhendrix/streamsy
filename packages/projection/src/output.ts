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

export interface Stream<A> {
  readonly _tag: "Stream";
  readonly stream: StreamRef.StreamRef<A>;
}
export interface Rows<A> {
  readonly _tag: "Rows";
  readonly schema: Schema.Codec<A, unknown, never, never>;
  readonly key: StreamRef.StateKey<A>;
  readonly stream: string;
}
export interface Value<A> {
  readonly _tag: "Value";
  readonly schema: Schema.Codec<A, unknown, never, never>;
}
/** The schema owns the output encoding; a ref supplies its stream identity. */
export const stream = <A, I>(
  schema: Schema.Codec<A, I, never, never>,
  options: {
    readonly stream: string | { readonly id: string };
  },
): Stream<A> => ({
  _tag: "Stream",
  stream: StreamRef.json(typeof options.stream === "string" ? options.stream : options.stream.id, {
    schema,
  }),
});
/** The output name is the Durable State collection type. Deletes need only a key. */
export const rows = <A, I>(
  schema: Schema.Codec<A, I, never, never>,
  options: {
    readonly key: StreamRef.StateKey<A>;
    readonly stream: string | { readonly id: string };
  },
): Rows<A> => ({
  _tag: "Rows",
  schema,
  key: options.key,
  stream: typeof options.stream === "string" ? options.stream : options.stream.id,
});
export const value = <A, I>(schema: Schema.Codec<A, I, never, never>): Value<A> => ({
  _tag: "Value",
  schema,
});

// The mapped public result recovers each declaration's item type before runtime erasure.
export type Declaration = Stream<any> | Rows<any> | Value<any>;
export type Map = Readonly<Record<string, Declaration>>;
export type Items<D extends Map> = {
  readonly [K in keyof D as D[K] extends Value<any> ? never : K]: ReadonlyArray<
    D[K] extends Stream<infer A> ? A : D[K] extends Rows<infer A> ? Change<A> : never
  >;
};
export type StateOf<D extends Map> = {
  [K in keyof D]: D[K] extends Value<infer A> ? A : never;
}[keyof D];
export type Result<D extends Map> = Items<D> &
  ([StateOf<D>] extends [never] ? {} : { readonly state: StateOf<D> });
export interface Processed {
  readonly items: Readonly<Record<string, ReadonlyArray<unknown>>>;
  readonly saveState?: Effect.Effect<void, import("./fault.ts").ProjectionFault>;
}
