import type { JsonValue, StreamProtocolClient, StreamProtocolHandle } from "@streamsy/core";
import { Schema } from "effect";

const UpsertHeaders = Schema.Struct({ operation: Schema.Literal("upsert") });
const DeleteHeaders = Schema.Struct({ operation: Schema.Literal("delete") });
const CompositeJsonValue = Schema.Union([
  Schema.Null,
  Schema.Array(Schema.Json),
  Schema.Record(Schema.String, Schema.Json),
]);
const isCompositeJsonValue = Schema.is(CompositeJsonValue);
const decodeScalarJsonValue = Schema.decodeUnknownSync(
  Schema.Union([Schema.String, Schema.Finite, Schema.Boolean]),
);
const LineageEnvelope = Schema.Struct({
  type: Schema.Literal("__streamsy.mesh.lineage.v1"),
});

export const BoardFact = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("row"),
    key: Schema.String,
    value: Schema.Struct({ last: Schema.Finite }),
    headers: UpsertHeaders,
  }),
  Schema.Struct({
    type: Schema.Literal("row"),
    key: Schema.String,
    headers: DeleteHeaders,
  }),
]);
export type BoardFact = typeof BoardFact.Type;

export const MembershipFact = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("join"),
    member: Schema.String,
    from: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({ type: Schema.Literal("leave"), member: Schema.String }),
]);
export type MembershipFact = typeof MembershipFact.Type;

export const MemberValue = Schema.Struct({ v: Schema.Finite });
export type MemberValue = typeof MemberValue.Type;

export const TotalFact = Schema.Struct({
  type: Schema.Literal("total"),
  key: Schema.String,
  value: Schema.Struct({
    total: Schema.Finite,
    applied: Schema.optionalKey(Schema.Finite),
  }),
  headers: UpsertHeaders,
});
export type TotalFact = typeof TotalFact.Type;

export const decodeBoardFact = Schema.decodeUnknownSync(BoardFact);
export const decodeBoardFactOption = Schema.decodeUnknownOption(BoardFact);
export const decodeMembershipFact = Schema.decodeUnknownSync(MembershipFact);
export const decodeMemberValue = Schema.decodeUnknownSync(MemberValue);
export const decodeTotalFact = Schema.decodeUnknownSync(TotalFact);
export const decodeTotalFactOption = Schema.decodeUnknownOption(TotalFact);
export const decodeJsonValue = Schema.decodeUnknownSync(Schema.Json);

export function parseStoredJson(text: string): JsonValue {
  return decodeJsonValue(JSON.parse(text));
}

export function jsonValueKey(value: JsonValue): string {
  return isCompositeJsonValue(value)
    ? (JSON.stringify(value) ?? "null")
    : String(decodeScalarJsonValue(value));
}

export const isLineageValue = Schema.is(LineageEnvelope);

export function replaceFirstTotal(
  items: readonly JsonValue[],
  total: number,
): readonly JsonValue[] {
  return items.map((item, index) => {
    if (index !== 0) return item;
    const fact = decodeTotalFact(item);
    return { ...fact, value: { ...fact.value, total } };
  });
}

export function lostResponseClient(
  underlying: StreamProtocolClient,
  acceptedItems: (items: readonly JsonValue[]) => readonly JsonValue[],
): StreamProtocolClient {
  let staged = false;
  return {
    close: (cause) => underlying.close(cause),
    stream: (streamId) => {
      const handle = underlying.stream(streamId);
      const wrapped: StreamProtocolHandle = {
        id: handle.id,
        head: (options) => handle.head(options),
        create: (options) => handle.create(options),
        append: (data, options) => handle.append(data, options),
        appendJsonBatch: (items, options) => {
          if (!staged) {
            staged = true;
            return handle
              .appendJsonBatch(acceptedItems(items), options)
              .then(() => handle.appendJsonBatch(items, options));
          }
          return handle.appendJsonBatch(items, options);
        },
        close: (options) => handle.close(options),
        read: <T extends JsonValue>(options?: Parameters<StreamProtocolHandle["read"]>[0]) =>
          handle.read<T>(options),
      };
      return wrapped;
    },
  };
}
