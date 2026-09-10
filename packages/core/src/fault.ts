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

/** Failures of the remote boundary; protocol classifications remain values. */
export class TransportFault extends Schema.TaggedError<TransportFault>()("TransportFault", {
  operation: Schema.String,
  reason: Schema.Literals(["request", "response", "decode", "configuration"]),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
export type StreamsFault = StorageFault | TransportFault;
