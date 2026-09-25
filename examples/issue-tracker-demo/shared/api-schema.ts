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
 * present fields must already match the wire schema.
 */
const input = <Fields extends Schema.Struct.Fields>(entity: Schema.Struct<Fields>) =>
  entity.mapFields((fields) => ({
    ...Struct.map(fields, Schema.optionalKey),
    txid: Schema.optionalKey(TxId),
  }));

export const ProjectInput = input(Project);
export type ProjectInput = typeof ProjectInput.Type;
export const IssueInput = input(Issue);
export type IssueInput = typeof IssueInput.Type;
export const CommentInput = input(Comment);
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
