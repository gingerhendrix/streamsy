import { Schema, Struct } from "effect";
import { Comment, Issue, Project } from "./state-schema.ts";

// Wire schemas for the JSON API. They import only `Schema`, so the browser can
// decode responses without bundling the HttpApi modules.

/** `main` and server-generated ids. Checked before any stream lookup. */
export const WorkspaceId = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9-]{0,63}$/));

/** A client transaction id, echoed in the event headers for `awaitTxId`. */
export const TxId = Schema.String.check(Schema.isUUID());

/**
 * Mutation bodies carry a partial entity: absent fields take a server default,
 * present fields must already match the wire schema. The server stamps the
 * time fields, so a body cannot set them.
 */
const input = <
  Fields extends Schema.Struct.Fields,
  const Stamped extends ReadonlyArray<keyof Fields>,
>(
  entity: Schema.Struct<Fields>,
  stamped: Stamped,
) =>
  entity.mapFields((fields) => ({
    ...Struct.map(Struct.omit(fields, stamped), Schema.optionalKey),
    txid: Schema.optionalKey(TxId),
  }));

export const ProjectInput = input(Project, ["createdAt"]);

export type ProjectInput = typeof ProjectInput.Type;

export const IssueInput = input(Issue, ["createdAt", "updatedAt"]);

export type IssueInput = typeof IssueInput.Type;

export const CommentInput = input(Comment, ["createdAt"]);

export type CommentInput = typeof CommentInput.Type;

/** Every accepted write returns the stream offset and txid to wait for. */
export const Written = Schema.Struct({ awaitOffset: Schema.String, txid: Schema.String });

export interface Written extends Schema.Schema.Type<typeof Written> {}

export const ProjectWritten = Written.mapFields(Struct.assign({ project: Project }));

export const IssueWritten = Written.mapFields(Struct.assign({ issue: Issue }));

export const CommentWritten = Written.mapFields(Struct.assign({ comment: Comment }));

export const WorkspaceCreated = Schema.Struct({ id: WorkspaceId });

/** Error bodies keep the `{ "error": string }` field the browser shows. */
export class BadRequest extends Schema.TaggedError<BadRequest>()(
  "BadRequest",
  { error: Schema.String },
  { httpApiStatus: 400 },
) {}

export class NotFound extends Schema.TaggedError<NotFound>()(
  "NotFound",
  { error: Schema.String },
  { httpApiStatus: 404 },
) {}

export class Conflict extends Schema.TaggedError<Conflict>()(
  "Conflict",
  { error: Schema.String },
  { httpApiStatus: 409 },
) {}

/**
 * A business rule refused the write. Unlike `Conflict`, a retry gives the same
 * answer until the workspace changes.
 */
export class WipLimitReached extends Schema.TaggedError<WipLimitReached>()(
  "WipLimitReached",
  { error: Schema.String },
  { httpApiStatus: 422 },
) {}

export class InternalServerError extends Schema.TaggedError<InternalServerError>()(
  "InternalServerError",
  { error: Schema.String },
  { httpApiStatus: 500 },
) {}
