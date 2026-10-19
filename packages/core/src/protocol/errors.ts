import { Schema } from "effect";
import { StreamId } from "../schema/index.ts";

/** Protocol rejections. Optional create reasons reflect the public HTTP wire's loss of detail. */
export class StreamNotFound extends Schema.TaggedError<StreamNotFound>()("StreamNotFound", {
  id: StreamId,
}) {
  override get message(): string {
    return `Stream ${this.id} was not found`;
  }
}

export class StreamGone extends Schema.TaggedError<StreamGone>()("StreamGone", { id: StreamId }) {
  override get message(): string {
    return `Stream ${this.id} was deleted`;
  }
}

export class StreamClosed extends Schema.TaggedError<StreamClosed>()("StreamClosed", {
  id: StreamId,
  offset: Schema.String,
}) {
  override get message(): string {
    return `Stream ${this.id} is closed at ${this.offset}`;
  }
}

export class OffsetMismatch extends Schema.TaggedError<OffsetMismatch>()("OffsetMismatch", {
  id: StreamId,
  expected: Schema.String,
  actual: Schema.String,
}) {
  override get message(): string {
    return `Stream ${this.id} is at ${this.actual}, not at the expected ${this.expected}`;
  }
}

export class AppendConflict extends Schema.TaggedError<AppendConflict>()("AppendConflict", {
  id: StreamId,
  message: Schema.String,
}) {}

export class StreamBusy extends Schema.TaggedError<StreamBusy>()("StreamBusy", { id: StreamId }) {
  override get message(): string {
    return `Stream ${this.id} is busy; try again`;
  }
}

export class StaleEpoch extends Schema.TaggedError<StaleEpoch>()("StaleEpoch", {
  id: StreamId,
  currentEpoch: Schema.Finite,
}) {
  override get message(): string {
    return `Producer epoch is stale on ${this.id}; the current epoch is ${this.currentEpoch}`;
  }
}

export class ProducerGap extends Schema.TaggedError<ProducerGap>()("ProducerGap", {
  id: StreamId,
  expectedSeq: Schema.Finite,
  receivedSeq: Schema.Finite,
}) {
  override get message(): string {
    return `Producer sequence gap on ${this.id}: expected ${this.expectedSeq}, received ${this.receivedSeq}`;
  }
}

export class InvalidEpochSeq extends Schema.TaggedError<InvalidEpochSeq>()("InvalidEpochSeq", {
  id: StreamId,
}) {
  override get message(): string {
    return `Invalid producer epoch or sequence on ${this.id}`;
  }
}

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
) {
  override get message(): string {
    return `Fork source ${this.source} of ${this.id} was not found`;
  }
}

export class InvalidForkRequest extends Schema.TaggedError<InvalidForkRequest>()(
  "InvalidForkRequest",
  { id: StreamId, message: Schema.String },
) {}

export class NotSupported extends Schema.TaggedError<NotSupported>()("NotSupported", {
  id: StreamId,
  feature: Schema.String,
}) {
  override get message(): string {
    return `Stream ${this.id} does not support ${this.feature}`;
  }
}

export type CreateConflictReason = NonNullable<CreateConflict["reason"]>;

export type HeadError = StreamNotFound | StreamGone;

export class InvalidReadRequest extends Schema.TaggedError<InvalidReadRequest>()(
  "InvalidReadRequest",
  {
    id: StreamId,
    message: Schema.String,
  },
) {}

export type ReadError = StreamNotFound | StreamGone | InvalidReadRequest;

export type ReadNextError = ReadError | NotSupported;

export type CreateError = CreateConflict | ForkSourceNotFound | InvalidForkRequest | NotSupported;

/**
 * Append rejections that any call can get, whatever its options. The memory and
 * storage hosts never answer `NotSupported`; the remote client does when the server
 * or its own capabilities refuse a feature.
 */
export type AppendBaseError =
  | StreamNotFound
  | StreamGone
  | StreamClosed
  | StreamBusy
  | AppendConflict
  | NotSupported;

export type AppendError =
  | AppendBaseError
  | InvalidAppendRequest
  | OffsetMismatch
  | StaleEpoch
  | ProducerGap
  | InvalidEpochSeq;

/**
 * The append rejections that call options `O` make possible. `OffsetMismatch`
 * needs `expectedOffset`, and the producer rejections need `producer`. An option
 * that may be set counts as set.
 */
export type AppendErrorFor<O> =
  | AppendBaseError
  | (true extends MaySet<O, "expectedOffset"> ? OffsetMismatch : never)
  | (true extends MaySet<O, "producer"> ? StaleEpoch | ProducerGap : never);

/** Whether options `O` may hold a value for `K`. A union holds it if any member may. */
type MaySet<O, K extends PropertyKey> = O extends unknown
  ? K extends keyof O
    ? [Exclude<O[K], undefined>] extends [never]
      ? false
      : true
    : false
  : never;

export type RemoveError = StreamNotFound | StreamGone | StreamBusy;

export type ProtocolError = HeadError | ReadNextError | CreateError | AppendError | RemoveError;

/** Exhaustive record of every protocol tag, checked against the union it must cover. */
const protocolErrorTags = {
  InvalidReadRequest: true,
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
