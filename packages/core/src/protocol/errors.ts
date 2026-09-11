import { Schema } from "effect";
import { StreamId } from "../schema/index.ts";

/** Protocol rejections. Optional create reasons reflect the public HTTP wire's loss of detail. */
export class StreamNotFound extends Schema.TaggedError<StreamNotFound>()("StreamNotFound", {
  id: StreamId,
}) {}
export class StreamGone extends Schema.TaggedError<StreamGone>()("StreamGone", { id: StreamId }) {}
export class StreamClosed extends Schema.TaggedError<StreamClosed>()("StreamClosed", {
  id: StreamId,
  offset: Schema.String,
}) {}
export class OffsetMismatch extends Schema.TaggedError<OffsetMismatch>()("OffsetMismatch", {
  id: StreamId,
  expected: Schema.String,
  actual: Schema.String,
}) {}
export class AppendConflict extends Schema.TaggedError<AppendConflict>()("AppendConflict", {
  id: StreamId,
  message: Schema.String,
}) {}
export class StreamBusy extends Schema.TaggedError<StreamBusy>()("StreamBusy", { id: StreamId }) {}
export class StaleEpoch extends Schema.TaggedError<StaleEpoch>()("StaleEpoch", {
  id: StreamId,
  currentEpoch: Schema.Finite,
}) {}
export class ProducerGap extends Schema.TaggedError<ProducerGap>()("ProducerGap", {
  id: StreamId,
  expectedSeq: Schema.Finite,
  receivedSeq: Schema.Finite,
}) {}
export class InvalidEpochSeq extends Schema.TaggedError<InvalidEpochSeq>()("InvalidEpochSeq", {
  id: StreamId,
}) {}
export class InvalidAppendRequest extends Schema.TaggedError<InvalidAppendRequest>()(
  "InvalidAppendRequest",
  { id: StreamId, message: Schema.String },
) {}
export class CreateConflict extends Schema.TaggedError<CreateConflict>()("CreateConflict", {
  id: StreamId,
  reason: Schema.optionalKey(
    Schema.Literals([
      "config-mismatch",
      "soft-deleted",
      "fork-content-type",
      "fork-source-soft-deleted",
    ]),
  ),
  message: Schema.String,
}) {}
export class ForkSourceNotFound extends Schema.TaggedError<ForkSourceNotFound>()(
  "ForkSourceNotFound",
  { id: StreamId, source: StreamId },
) {}
export class InvalidForkRequest extends Schema.TaggedError<InvalidForkRequest>()(
  "InvalidForkRequest",
  { id: StreamId, message: Schema.String },
) {}
export class NotSupported extends Schema.TaggedError<NotSupported>()("NotSupported", {
  id: StreamId,
  feature: Schema.String,
}) {}
export type CreateConflictReason = NonNullable<CreateConflict["reason"]>;
export type HeadError = StreamNotFound | StreamGone;
export type ReadError = StreamNotFound | StreamGone;
export type ReadNextError = ReadError | NotSupported;
export type CreateError = CreateConflict | ForkSourceNotFound | InvalidForkRequest | NotSupported;
export type AppendError =
  | StreamNotFound
  | StreamGone
  | StreamClosed
  | OffsetMismatch
  | AppendConflict
  | StreamBusy
  | StaleEpoch
  | ProducerGap
  | InvalidEpochSeq
  | InvalidAppendRequest
  | NotSupported;
export type RemoveError = StreamNotFound | StreamGone | StreamBusy;
export type ProtocolError = HeadError | ReadNextError | CreateError | AppendError | RemoveError;

/** Exhaustive record of every protocol tag, checked against the union it must cover. */
const protocolErrorTags = {
  StreamNotFound: true,
  StreamGone: true,
  StreamClosed: true,
  OffsetMismatch: true,
  AppendConflict: true,
  StreamBusy: true,
  StaleEpoch: true,
  ProducerGap: true,
  InvalidEpochSeq: true,
  InvalidAppendRequest: true,
  CreateConflict: true,
  ForkSourceNotFound: true,
  InvalidForkRequest: true,
  NotSupported: true,
} satisfies Record<ProtocolError["_tag"], true>;

/** The HTTP edge answers every protocol rejection through one mapper, so it needs one guard. */
export const isProtocolError = (error: { readonly _tag: string }): error is ProtocolError =>
  Object.hasOwn(protocolErrorTags, error._tag);
