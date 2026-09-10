import { Schema } from "effect";

export const Identity = Schema.Struct({
  id: Schema.String,
  version: Schema.String,
  generation: Schema.String,
  source: Schema.String,
  sink: Schema.String,
});
export interface Identity extends Schema.Schema.Type<typeof Identity> {}
export const sameIdentity = (a: Identity, b: Identity): boolean =>
  a.id === b.id &&
  a.version === b.version &&
  a.generation === b.generation &&
  a.source === b.source &&
  a.sink === b.sink;

/** Length-safe encoding of inert string components; no persistence decoding here. */
export const encodeKey = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
