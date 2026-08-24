/**
 * Typed application failures.
 *
 * These are the expected operational outcomes of the command, maintenance and
 * publication paths. They travel in the Effect error channel so the router can
 * translate them by `_tag`; nothing throws to signal an expected failure, and a
 * `TypeError` reaching the edge is a real defect reported as one.
 *
 * `@streamsy/experimental/effect` already owns `StreamReadError`,
 * `StreamAppendError` and `StateRestorePoison`. This module adds only what the
 * application owns.
 */
import { Schema } from "effect";

/** A request value that cannot be accepted. Always a 400. */
export class InvalidRequest extends Schema.TaggedError<InvalidRequest>()("InvalidRequest", {
  field: Schema.String,
  detail: Schema.String,
}) {
  static of(field: string, detail: string): InvalidRequest {
    return new InvalidRequest({ field, detail });
  }
}

/** A body that is not JSON, or does not decode to the declared request shape. */
export class MalformedBody extends Schema.TaggedError<MalformedBody>()("MalformedBody", {
  detail: Schema.String,
}) {}

export class UnknownIssue extends Schema.TaggedError<UnknownIssue>()("UnknownIssue", {
  issueId: Schema.String,
}) {}

/** A durable stream could not be created or is not usable. */
export class StreamUnavailable extends Schema.TaggedError<StreamUnavailable>()(
  "StreamUnavailable",
  { streamId: Schema.String, status: Schema.String },
) {}

/**
 * The protocol refused an append for a reason that is neither acceptance nor a
 * producer duplicate. It is never reported as a success.
 */
export class AppendRejected extends Schema.TaggedError<AppendRejected>()("AppendRejected", {
  stream: Schema.String,
  status: Schema.String,
}) {}

/** A durable source item that the declared source schema rejects. */
export class SourcePoison extends Schema.TaggedError<SourcePoison>()("SourcePoison", {
  sourceId: Schema.String,
  position: Schema.String,
  collection: Schema.optionalKey(Schema.String),
  key: Schema.optionalKey(Schema.String),
  detail: Schema.String,
}) {}

/** A State delete reached an A3 relation that intentionally supports upserts only. */
export class UnsupportedStateOperation extends Schema.TaggedError<UnsupportedStateOperation>()(
  "UnsupportedStateOperation",
  {
    sourceId: Schema.String,
    position: Schema.String,
    collection: Schema.String,
    key: Schema.String,
    operation: Schema.Literal("delete"),
  },
) {}

/** The reducer could not fold a decoded source item into a declared row. */
export class MaintenanceFault extends Schema.TaggedError<MaintenanceFault>()("MaintenanceFault", {
  view: Schema.String,
  phase: Schema.String,
  detail: Schema.String,
}) {}

/** Durable maintained state could not be read back into typed rows. */
export class StoreRestorePoison extends Schema.TaggedError<StoreRestorePoison>()(
  "StoreRestorePoison",
  { table: Schema.String, key: Schema.String, detail: Schema.String },
) {}

/** The maintained-state store itself failed. Never a validation outcome. */
export class StoreUnavailable extends Schema.TaggedError<StoreUnavailable>()("StoreUnavailable", {
  operation: Schema.String,
  detail: Schema.String,
}) {}

/** A resume offset outside retained history, with the sink's recovery policy. */
export class SessionResumeUnavailable extends Schema.TaggedError<SessionResumeUnavailable>()(
  "SessionResumeUnavailable",
  {
    sink: Schema.String,
    reason: Schema.Literal("out-of-window"),
    fallback: Schema.Literal("snapshot-then-live"),
  },
) {}

/** The request carries no scope, or not the scope the sink declares. */
export class Unauthorized extends Schema.TaggedError<Unauthorized>()("Unauthorized", {
  required: Schema.String,
}) {}

export type ApplicationError =
  | InvalidRequest
  | MalformedBody
  | UnknownIssue
  | StreamUnavailable
  | AppendRejected
  | SourcePoison
  | UnsupportedStateOperation
  | MaintenanceFault
  | StoreRestorePoison
  | StoreUnavailable
  | SessionResumeUnavailable
  | Unauthorized;
