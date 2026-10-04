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

/** Failures of the remote boundary; protocol rejections have their own typed errors. */
export class TransportFault extends Schema.TaggedError<TransportFault>()("TransportFault", {
  operation: Schema.String,
  reason: Schema.Literals(["request", "response", "decode", "configuration"]),
  /** HTTP status of the response, when available. */
  status: Schema.optionalKey(Schema.Int),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {
  /** Failed requests, 429s and 5xx responses can succeed later. */
  get retryable(): boolean {
    return (
      this.reason === "request" ||
      (this.status !== undefined && (this.status === 429 || this.status >= 500))
    );
  }
}
export const StreamsFault = Schema.Union([StorageFault, TransportFault]);
export type StreamsFault = typeof StreamsFault.Type;
/** A storage or transport failure, as opposed to a protocol rejection. */
export const isStreamsFault = Schema.is(StreamsFault);
/** A storage or transport failure that can succeed later. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This public guard validates unknown failures with the StreamsFault schema.
export const isRetryableFault = (error: unknown): error is StreamsFault =>
  isStreamsFault(error) && error.retryable;
