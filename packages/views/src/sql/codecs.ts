import { Schema } from "effect";
import { rowKeySchema } from "./data-fault.ts";

const rowCodec = Schema.fromJsonString(
  Schema.Struct({
    key: rowKeySchema,
    row: Schema.JsonObject,
  }),
);

const accCodec = Schema.fromJsonString(
  Schema.Struct({
    count: Schema.Finite,
    sums: Schema.Record(Schema.String, Schema.Finite),
    integerMass: Schema.optionalKey(Schema.Record(Schema.String, Schema.Finite)),
  }),
);

export const decodeRow = Schema.decodeUnknownSync(rowCodec);

export const decodeAcc = Schema.decodeUnknownSync(accCodec);
