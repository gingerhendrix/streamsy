/**
 * Typed application failures.
 *
 * These are the expected operational outcomes of the command and query paths.
 * They travel in the Effect error channel, so the router translates them by
 * `_tag` instead of inspecting defects. Nothing in the application throws to
 * signal an expected failure: a `TypeError` reaching the edge is a real defect
 * and is reported as one.
 *
 * Mesh failures (`StreamReadError`, `StreamAppendError`, `StateRestorePoison`,
 * …) already have typed classes in `@streamsy/experimental/effect`; this module
 * only adds the ones this application owns.
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

/** A malformed JSON body, or a body that does not decode to the request shape. */
export class MalformedBody extends Schema.TaggedError<MalformedBody>()("MalformedBody", {
  detail: Schema.String,
}) {}

export class UnknownProject extends Schema.TaggedError<UnknownProject>()("UnknownProject", {
  projectId: Schema.String,
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
 * producer duplicate — an offset conflict, a rejected producer tuple, a missing
 * or gone stream. It is never reported as a success.
 */
export class AppendRejected extends Schema.TaggedError<AppendRejected>()("AppendRejected", {
  stream: Schema.String,
  status: Schema.String,
}) {}

/** Every expected failure this application owns. */
export type ApplicationError =
  | InvalidRequest
  | MalformedBody
  | UnknownProject
  | UnknownIssue
  | StreamUnavailable
  | AppendRejected;
