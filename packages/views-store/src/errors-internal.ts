import { Schema } from "effect";
import { ViewStateRestorePoison } from "./errors.ts";
import type { JsonValue, StoredChange } from "./contracts.ts";

const JsonString = Schema.fromJsonString(Schema.Json);
const RowKey = Schema.Union([
  Schema.Null,
  Schema.Boolean,
  Schema.Finite,
  Schema.String,
  Schema.Array(Schema.Json),
]);
const StoredChange = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("enter"),
    relationId: Schema.String,
    key: RowKey,
    after: Schema.Json,
  }),
  Schema.Struct({
    kind: Schema.Literal("update"),
    relationId: Schema.String,
    key: RowKey,
    before: Schema.Json,
    after: Schema.Json,
  }),
  Schema.Struct({
    kind: Schema.Literal("exit"),
    relationId: Schema.String,
    key: RowKey,
    before: Schema.Json,
  }),
]);
const decodeJsonString = Schema.decodeUnknownSync(JsonString);
const decodeStoredChangeString = Schema.decodeUnknownSync(Schema.fromJsonString(StoredChange));

export function decodeJson(table: string, identity: string, key: string, value: string): JsonValue {
  try {
    return decodeJsonString(value);
  } catch (cause) {
    throw new ViewStateRestorePoison({
      table,
      identity,
      key,
      detail: cause instanceof Error ? cause.message : String(cause),
    });
  }
}

export function decodeStoredChange(
  table: string,
  identity: string,
  key: string,
  value: string,
): StoredChange {
  try {
    return decodeStoredChangeString(value);
  } catch (cause) {
    throw new ViewStateRestorePoison({
      table,
      identity,
      key,
      detail: cause instanceof Error ? cause.message : String(cause),
    });
  }
}
