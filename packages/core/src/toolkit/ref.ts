import { Schema } from "effect";
import { StreamId } from "../schema/index.ts";

export interface StateHeaders {
  readonly operation: "upsert" | "delete";
  readonly offset?: string;
  readonly txid?: string;
}

export interface StateUpsert<A> {
  readonly type: string;
  readonly key: string;
  readonly value: A;
  readonly headers: StateHeaders & { readonly operation: "upsert" };
}

export interface StateDelete<A> {
  readonly type: string;
  readonly key: string;
  readonly old_value: A;
  readonly headers: StateHeaders & { readonly operation: "delete" };
}

/** A change event consumed by `@durable-streams/state`. */
export type StateChange<A> = StateUpsert<A> | StateDelete<A>;

/** Inert identity and codec. Constructing a ref acquires no service or resource. */
export interface StreamRef<A, RD = never, RE = never> {
  readonly _tag: "Json" | "Bytes";
  readonly id: StreamId;
  readonly contentType: string;
  readonly codec: Schema.Codec<A, string | Uint8Array, RD, RE>;
}

/** A JSON ref with the metadata needed to construct Durable State changes. */
export interface StateRef<
  A,
  RD = never,
  RE = never,
  Key extends keyof A = keyof A,
> extends StreamRef<StateChange<A>, RD, RE> {
  readonly state: {
    readonly type: string;
    readonly key: Key;
  };
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

/** A stream of Durable State upsert and delete change events. */
export function state<A, I, RD, RE, Key extends keyof A>(
  id: string,
  options: {
    readonly schema: Schema.Codec<A, I, RD, RE>;
    readonly type: string;
    readonly key: Key;
  },
): StateRef<A, RD, RE, Key> {
  const headers = {
    offset: Schema.optionalKey(Schema.String),
    txid: Schema.optionalKey(Schema.String),
  };
  const schema = Schema.Union([
    Schema.Struct({
      type: Schema.Literal(options.type),
      key: Schema.String,
      value: options.schema,
      headers: Schema.Struct({ operation: Schema.Literal("upsert"), ...headers }),
    }),
    Schema.Struct({
      type: Schema.Literal(options.type),
      key: Schema.String,
      old_value: options.schema,
      headers: Schema.Struct({ operation: Schema.Literal("delete"), ...headers }),
    }),
  ]);
  return {
    ...json(id, { schema }),
    state: { type: options.type, key: options.key },
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
