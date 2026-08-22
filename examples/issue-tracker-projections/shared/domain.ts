/**
 * Application-owned domain for the projection issue tracker.
 *
 * This module owns Effect Schemas and pure transitions. Web-standard values,
 * types, and stream naming live in `model.ts` and are re-exported here, so the
 * browser can share them without pulling the Effect runtime into its bundle.
 *
 * It owns no recovery, lineage, or commit behaviour: those belong to the
 * experimental mesh kernels.
 */
import { Schema } from "effect";
import {
  Identifier,
  IssuePrioritySchema,
  IssueStatusSchema,
  Prose,
  type IssueDetail,
} from "./model.ts";

export * from "./model.ts";

const Base = {
  commandId: Schema.NonEmptyString,
  at: Schema.NonEmptyString,
};

export const IssueCreated = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssueCreated"),
  issueId: Identifier,
  issueKey: Identifier,
  projectId: Identifier,
  title: Prose,
  status: IssueStatusSchema,
  priority: IssuePrioritySchema,
  creatorId: Schema.NonEmptyString,
});

export const IssueRenamed = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssueRenamed"),
  title: Prose,
});

export const IssueStatusChanged = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssueStatusChanged"),
  status: IssueStatusSchema,
});

export const IssuePriorityChanged = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssuePriorityChanged"),
  priority: IssuePrioritySchema,
});

export const IssueAssigned = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssueAssigned"),
  assigneeId: Schema.NullOr(Schema.NonEmptyString),
});

export const CommentAdded = Schema.Struct({
  ...Base,
  type: Schema.Literal("CommentAdded"),
  commentId: Identifier,
  authorId: Schema.NonEmptyString,
  body: Prose,
});

export const IssueEvent = Schema.Union([
  IssueCreated,
  IssueRenamed,
  IssueStatusChanged,
  IssuePriorityChanged,
  IssueAssigned,
  CommentAdded,
]);
export type IssueEvent = Schema.Schema.Type<typeof IssueEvent>;

export const ProjectMembershipFact = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("IssueJoined"),
    issueId: Identifier,
    /** Detail-stream position the board should start from, when known. */
    from: Schema.NullOr(Schema.NonEmptyString),
  }),
  Schema.Struct({ type: Schema.Literal("IssueLeft"), issueId: Identifier }),
]);
export type ProjectMembershipFact = Schema.Schema.Type<typeof ProjectMembershipFact>;

/** Fold one issue event into detail state. A missing creation is a domain fault. */
export function evolveIssue(current: IssueDetail | undefined, event: IssueEvent): IssueDetail {
  if (event.type === "IssueCreated") {
    if (current !== undefined) return current;
    return {
      issueId: event.issueId,
      issueKey: event.issueKey,
      projectId: event.projectId,
      title: event.title,
      status: event.status,
      priority: event.priority,
      assigneeId: null,
      comments: [],
      createdAt: event.at,
      updatedAt: event.at,
    };
  }
  if (current === undefined) {
    throw new TypeError(`Issue event ${event.type} arrived before IssueCreated`);
  }
  const touched = { ...current, updatedAt: event.at };
  // Every remaining variant returns, so the fold is total without a defensive
  // branch: adding an event type makes the final block stop typechecking.
  if (event.type === "IssueRenamed") return { ...touched, title: event.title };
  if (event.type === "IssueStatusChanged") return { ...touched, status: event.status };
  if (event.type === "IssuePriorityChanged") return { ...touched, priority: event.priority };
  if (event.type === "IssueAssigned") return { ...touched, assigneeId: event.assigneeId };
  return current.comments.some((comment) => comment.commentId === event.commentId)
    ? touched
    : {
        ...touched,
        comments: [
          ...current.comments,
          {
            commentId: event.commentId,
            authorId: event.authorId,
            body: event.body,
            at: event.at,
          },
        ],
      };
}
