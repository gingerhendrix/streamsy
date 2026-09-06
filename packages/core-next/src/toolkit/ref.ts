import { Schema } from "effect";
import { StreamId } from "../schema/index.ts";

/** Inert identity and codec. Constructing a ref acquires no service or resource. */
export interface StreamRef<A, RD = never, RE = never> {
  readonly _tag: "Json" | "Bytes";
  readonly id: StreamId;
  readonly contentType: string;
  readonly codec: Schema.Codec<A, string | Uint8Array, RD, RE>;
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
