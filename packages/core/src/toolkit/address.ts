import { Array, Data, Predicate, Record as EffectRecord, Schema } from "effect";
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

/** The control signals of the Durable State protocol (STATE-PROTOCOL.md section 4.2). */
export type StateControlName = "snapshot-start" | "snapshot-end" | "reset";

/**
 * A control message of a Durable State stream. It has no `type` or `key`.
 * `snapshot-start` and `snapshot-end` mark a run of changes that is a complete
 * snapshot. `reset` tells a reader to clear its materialized state. Omit
 * `offset` rather than setting it to `undefined`.
 */
export interface StateControl {
  readonly headers: {
    readonly control: StateControlName;
    readonly offset?: string;
  };
}

/** One item of a Durable State stream: a change of one collection or a control. */
export type StateEvent<A, Type extends string = string> = StateChange<A, Type> | StateControl;

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

/** The items of a State address: the change events of its collections, plus controls. */
export type CollectionsEvent<C extends Collections> = CollectionsChange<C> | StateControl;

export type CollectionsEncodedEvent<C extends Collections> =
  | CollectionsEncodedChange<C>
  | StateControl;

export type CollectionsDecodingServices<C extends Collections> =
  C[keyof C]["schema"]["DecodingServices"];

export type CollectionsEncodingServices<C extends Collections> =
  C[keyof C]["schema"]["EncodingServices"];

/** Inert identity and codec. Constructing an address acquires no service or resource. */
export interface StreamAddress<A, RD = never, RE = never> {
  readonly _tag: "Json" | "Bytes";
  readonly id: StreamId;
  readonly contentType: string;
  readonly codec: Schema.Codec<A, string | Uint8Array, RD, RE>;
}

/** A JSON address whose items are the change events of its collections and State controls. */
export interface StateAddress<
  C extends Collections,
  RD = CollectionsDecodingServices<C>,
  RE = CollectionsEncodingServices<C>,
> extends StreamAddress<CollectionsEvent<C>, RD, RE> {
  readonly collections: C;
}

/** The address constructors. Each class owns the `_tag` of one address kind. */
const JsonAddress = Data.TaggedClass("Json");

const BytesAddress = Data.TaggedClass("Bytes");

export function json<A, I, RD, RE>(
  id: string,
  options: { readonly schema: Schema.Codec<A, I, RD, RE> },
): StreamAddress<A, RD, RE> {
  return new JsonAddress<Omit<StreamAddress<A, RD, RE>, "_tag">>({
    id: StreamId.make(id),
    contentType: "application/json",
    codec: Schema.fromJsonString(Schema.toCodecJson(options.schema)),
  });
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

/** The codec for a State control message. It needs no services. */
export const stateControl: Schema.Codec<StateControl> = Schema.Struct({
  headers: Schema.Struct({
    control: Schema.Literals(["snapshot-start", "snapshot-end", "reset"]),
    offset: Schema.optionalKey(Schema.String),
  }),
});

/** True for a control message. A change has `headers.operation` and no `headers.control`. */
export const isControl = <E extends StateEvent<unknown>>(
  event: E,
): event is Extract<E, StateControl> => Predicate.hasProperty(event.headers, "control");

/** True for an upsert or delete change. */
export const isChange = <E extends StateEvent<unknown>>(
  event: E,
): event is Exclude<E, StateControl> => !isControl(event);

/** True for an upsert change, with its value available. */
export const isUpsert = <E extends StateEvent<unknown>>(
  event: E,
): event is Extract<E, StateUpsert<unknown>> =>
  isChange(event) && event.headers.operation === "upsert";

const control = (name: StateControlName, options: { readonly offset?: string }): StateControl => ({
  headers:
    options.offset === undefined ? { control: name } : { control: name, offset: options.offset },
});

/** A `reset` control. A reader should clear its materialized state and apply what follows. */
export const reset = (options: { readonly offset?: string } = {}): StateControl =>
  control("reset", options);

/** A `snapshot-start` control. The changes up to `snapshot-end` form a complete snapshot. */
export const snapshotStart = (options: { readonly offset?: string } = {}): StateControl =>
  control("snapshot-start", options);

/** A `snapshot-end` control. It closes the snapshot that `snapshot-start` opened. */
export const snapshotEnd = (options: { readonly offset?: string } = {}): StateControl =>
  control("snapshot-end", options);

/**
 * A stream of Durable State events for several collections. Each record
 * key is the `type` on the wire; each entry gives the value schema and the value
 * field that becomes the event `key`. The codec also decodes and encodes the
 * State control messages `snapshot-start`, `snapshot-end` and `reset`.
 */
export function state<const C extends Collections>(
  id: string,
  options: { readonly collections: C & ValidCollections<C> },
): StateAddress<C> {
  const codecs = Array.map(EffectRecord.toEntries(options.collections), ([type, collection]) =>
    stateChange({ schema: collection.schema, type }),
  );

  // SAFETY: `codecs` holds exactly one `stateChange` codec per entry of
  // `options.collections`, each tagged with that entry's type literal, plus the
  // control codec, so the runtime union accepts precisely the events
  // `CollectionsEvent<C>` describes.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The invariant is stated in the comment above.
  const schema = Schema.Union([...codecs, stateControl]) as Schema.Codec<
    CollectionsEvent<C>,
    CollectionsEncodedEvent<C>,
    CollectionsDecodingServices<C>,
    CollectionsEncodingServices<C>
  >;

  const address = json(id, { schema });

  return new JsonAddress<Omit<StateAddress<C>, "_tag">>({
    id: address.id,
    contentType: address.contentType,
    codec: address.codec,
    collections: options.collections,
  });
}

/** Items from one bytes append are joined into one stored message. */
export function bytes(
  id: string,
  options: { readonly contentType?: string } = {},
): StreamAddress<Uint8Array> {
  return new BytesAddress<Omit<StreamAddress<Uint8Array>, "_tag">>({
    id: StreamId.make(id),
    contentType: options.contentType ?? "application/octet-stream",
    codec: Schema.Uint8Array,
  });
}
