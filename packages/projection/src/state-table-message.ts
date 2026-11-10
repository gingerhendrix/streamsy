import { Data, Effect, Schema } from "effect";

const Message = Schema.Union([
  Schema.Struct({
    headers: Schema.Struct({
      control: Schema.Literals(["reset", "snapshot-start", "snapshot-end"]),
    }),
  }),
  Schema.Struct({
    type: Schema.String,
    key: Schema.String,
    headers: Schema.Struct({ operation: Schema.Literals(["insert", "update", "upsert"]) }),
    value: Schema.Unknown,
  }),
  Schema.Struct({
    type: Schema.String,
    key: Schema.String,
    headers: Schema.Struct({ operation: Schema.Literal("delete") }),
  }),
]);

/** What one stored State message does to the rows. An upsert holds its value as canonical JSON. */
export type Change = Data.TaggedEnum<{
  Control: { readonly control: "reset" | "snapshot-start" | "snapshot-end" };
  Upsert: { readonly collection: string; readonly key: string; readonly value: string };
  Delete: { readonly collection: string; readonly key: string };
}>;

export const Change = Data.taggedEnum<Change>();

const decode = Schema.decodeEffect(Schema.fromJsonString(Message));

/**
 * Only the protocol envelope is decoded; collection values never pass through a codec.
 * `JSON.stringify` gives the bytes that storage holds, because core stores each JSON
 * message as `JSON.stringify` of the parsed item.
 */
export const message = (json: string) =>
  Effect.map(decode(json), (decoded): Change => {
    if (!("type" in decoded)) return Change.Control({ control: decoded.headers.control });

    return "value" in decoded
      ? Change.Upsert({
          collection: decoded.type,
          key: decoded.key,
          value: JSON.stringify(decoded.value),
        })
      : Change.Delete({ collection: decoded.type, key: decoded.key });
  });

/** The stored message of an upsert whose value is already canonical JSON. */
export const upsertJson = (collection: string, key: string, value: string) =>
  `{"type":${JSON.stringify(collection)},"key":${JSON.stringify(key)},"value":${value},"headers":{"operation":"upsert"}}`;

/** The stored message of a delete. */
export const deleteJson = (collection: string, key: string) =>
  JSON.stringify({ type: collection, key, headers: { operation: "delete" } });
