import { Schema } from "effect";
export class StorageFault extends Schema.TaggedError<StorageFault>()("StorageFault", {
  operation: Schema.String,
  message: Schema.String,
  retryable: Schema.Boolean,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
export class EncodeFault extends Schema.TaggedError<EncodeFault>()("EncodeFault", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
export class DecodeFault extends Schema.TaggedError<DecodeFault>()("DecodeFault", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
