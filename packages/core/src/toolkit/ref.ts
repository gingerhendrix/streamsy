import { Schema } from "effect";
import { StreamId } from "../schema/index.ts";

/** Omit `offset` and `txid` rather than setting them to `undefined`. */
export interface StateHeaders {
  readonly operation: "upsert" | "delete";
  readonly offset?: string;
  readonly txid?: string;
  readonly timestamp?: string;
  readonly from?: string;
}

export interface StateUpsert<A, Type extends string = string> {
  readonly type: Type;
  readonly key: string;
  readonly value: A;
  readonly headers: StateHeaders & { readonly operation: "upsert" };
}

export interface StateDelete<A, Type extends string = string> {
  readonly type: Type;
  readonly key: string;
  readonly old_value?: A;
  readonly headers: StateHeaders & { readonly operation: "delete" };
}

/** A change event consumed by `@durable-streams/state`, narrowed by its `type`. */
export type StateChange<A, Type extends string = string> =
  | StateUpsert<A, Type>
  | StateDelete<A, Type>;

type StateEncodedChange<A, Type extends string = string> =
  | (Omit<StateUpsert<A, Type>, "headers"> & {
      readonly headers: Omit<StateHeaders, "operation"> & {
        readonly operation: "insert" | "update" | "upsert";
      };
    })
  | StateDelete<A, Type>;

/** The value fields that may serve as a Durable State key. */
export type StateKey<A> = {
  [K in keyof A]-?: A[K] extends string | number ? K : never;
}[keyof A];

/** One collection of a Durable State stream: its value schema and the value field used as the key. */
export interface Collection<A, I, RD, RE, Key extends StateKey<A> = StateKey<A>> {
  readonly schema: Schema.Codec<A, I, RD, RE>;
  readonly key: Key;
}

/** Collections keyed by the `type` that names them on the wire. */
export type Collections = Record<string, Collection<any, any, any, any, any>>;

/**
 * Each `key` must name a string or number field of its own collection's value.
 * A mismatch reports on the `key` property of the offending collection.
 */
export type ValidCollections<C extends Collections> = {
  readonly [K in keyof C]: C[K]["key"] extends StateKey<C[K]["schema"]["Type"]>
    ? C[K]
    : { readonly key: StateKey<C[K]["schema"]["Type"]> };
};

/** The union of change events across every collection, discriminated by `type`. */
export type CollectionsChange<C extends Collections> = {
  [K in keyof C & string]: StateChange<C[K]["schema"]["Type"], K>;
}[keyof C & string];

export type CollectionsEncodedChange<C extends Collections> = {
  [K in keyof C & string]: StateEncodedChange<C[K]["schema"]["Encoded"], K>;
}[keyof C & string];

export type CollectionsDecodingServices<C extends Collections> =
  C[keyof C]["schema"]["DecodingServices"];
export type CollectionsEncodingServices<C extends Collections> =
  C[keyof C]["schema"]["EncodingServices"];

/** Inert identity and codec. Constructing a ref acquires no service or resource. */
export interface StreamRef<A, RD = never, RE = never> {
  readonly _tag: "Json" | "Bytes";
  readonly id: StreamId;
  readonly contentType: string;
  readonly codec: Schema.Codec<A, string | Uint8Array, RD, RE>;
}

/** A JSON ref whose items are the change events of its collections. */
export interface StateRef<
  C extends Collections,
  RD = CollectionsDecodingServices<C>,
  RE = CollectionsEncodingServices<C>,
> extends StreamRef<CollectionsChange<C>, RD, RE> {
  readonly collections: C;
}

export function json<A, I, RD, RE>(
  id: string,
  options: { readonly schema: Schema.Codec<A, I, RD, RE> },
): StreamRef<A, RD, RE> {
  return {
    _tag: "Json",
    id: StreamId.make(id),
    contentType: "application/json",
    codec: Schema.fromJsonString(Schema.toCodecJson(options.schema)),
  };
}

/**
 * A codec for the upsert and delete change events of one collection.
 * `state` composes one of these per collection. Omit optional headers instead
 * of setting them to `undefined`.
 */
export function stateChange<A, I, RD, RE, const Type extends string>(options: {
  readonly schema: Schema.Codec<A, I, RD, RE>;
  readonly type: Type;
}): Schema.Codec<StateChange<A, Type>, StateEncodedChange<I, Type>, RD, RE> {
  const upsertOperation = Schema.Union([
    Schema.Literal("upsert"),
    Schema.Literal("insert").transform("upsert"),
    Schema.Literal("update").transform("upsert"),
  ]);
  const headers = {
    offset: Schema.optionalKey(Schema.String),
    txid: Schema.optionalKey(Schema.String),
    timestamp: Schema.optionalKey(Schema.String),
    from: Schema.optionalKey(Schema.String),
  };
  return Schema.Union([
    Schema.Struct({
      type: Schema.Literal(options.type),
      key: Schema.String,
      value: options.schema,
      headers: Schema.Struct({ operation: upsertOperation, ...headers }),
    }),
    Schema.Struct({
      type: Schema.Literal(options.type),
      key: Schema.String,
      old_value: Schema.optionalKey(options.schema),
      headers: Schema.Struct({ operation: Schema.Literal("delete"), ...headers }),
    }),
  ]);
}

/**
 * A stream of Durable State change events for several collections. Each record
 * key is the `type` on the wire; each entry gives the value schema and the value
 * field that becomes the event `key`.
 */
export function state<const C extends Collections>(
  id: string,
  options: { readonly collections: C & ValidCollections<C> },
): StateRef<C> {
  const members = Object.entries(options.collections).map(([type, collection]) =>
    stateChange({ schema: collection.schema, type }),
  );
  // SAFETY: `members` holds exactly one `stateChange` codec per entry of
  // `options.collections`, each tagged with that entry's type literal, so the
  // runtime union accepts precisely the events `CollectionsChange<C>` describes.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The invariant is stated in the comment above.
  const schema = Schema.Union(members) as Schema.Codec<
    CollectionsChange<C>,
    CollectionsEncodedChange<C>,
    CollectionsDecodingServices<C>,
    CollectionsEncodingServices<C>
  >;
  return {
    ...json(id, { schema }),
    collections: options.collections,
  };
}

/** Items from one bytes append are joined into one stored message. */
export function bytes(
  id: string,
  options: { readonly contentType?: string } = {},
): StreamRef<Uint8Array> {
  return {
    _tag: "Bytes",
    id: StreamId.make(id),
    contentType: options.contentType ?? "application/octet-stream",
    codec: Schema.Uint8Array,
  };
}
