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
  ISSUE_PRIORITIES,
  ISSUE_STATUSES,
  type BoardRow,
  type IssueDetail,
  type Project,
} from "./model.ts";

export * from "./model.ts";

export const IssueStatusSchema = Schema.Literals(ISSUE_STATUSES);
export const IssuePrioritySchema = Schema.Literals(ISSUE_PRIORITIES);

/**
 * Application values that are written into durable State streams.
 *
 * Every restored or served value is decoded through these schemas. A row that
 * carries the right collection tag but a malformed application value is a
 * fault, not something to accept quietly: in a projection restore the throw
 * becomes the kernel's typed `StateRestorePoison`.
 */
export const CommentSchema = Schema.Struct({
  commentId: Schema.NonEmptyString,
  authorId: Schema.NonEmptyString,
  body: Schema.NonEmptyString,
  at: Schema.NonEmptyString,
});

export const IssueDetailSchema = Schema.Struct({
  issueId: Schema.NonEmptyString,
  issueKey: Schema.NonEmptyString,
  projectId: Schema.NonEmptyString,
  title: Schema.NonEmptyString,
  status: IssueStatusSchema,
  priority: IssuePrioritySchema,
  assigneeId: Schema.NullOr(Schema.NonEmptyString),
  comments: Schema.Array(CommentSchema),
  createdAt: Schema.NonEmptyString,
  updatedAt: Schema.NonEmptyString,
});

export const BoardRowSchema = Schema.Struct({
  issueId: Schema.NonEmptyString,
  issueKey: Schema.NonEmptyString,
  title: Schema.NonEmptyString,
  status: IssueStatusSchema,
  priority: IssuePrioritySchema,
  assigneeId: Schema.NullOr(Schema.NonEmptyString),
  commentCount: Schema.Int,
  updatedAt: Schema.NonEmptyString,
});

export const ProjectSchema = Schema.Struct({
  projectId: Schema.NonEmptyString,
  projectKey: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
});

const decodeDetailValue = Schema.decodeUnknownSync(IssueDetailSchema);
const decodeBoardRowValue = Schema.decodeUnknownSync(BoardRowSchema);
const decodeProjectValue = Schema.decodeUnknownSync(ProjectSchema);

/** Decode one durable issue-detail value. Throws on a malformed value. */
export const decodeIssueDetail = (value: unknown): IssueDetail => decodeDetailValue(value);

/** Decode one durable board-row value. Throws on a malformed value. */
export const decodeBoardRow = (value: unknown): BoardRow => decodeBoardRowValue(value);

/** Decode one durable project row. Throws on a malformed value. */
export const decodeProject = (value: unknown): Project => decodeProjectValue(value);

const Base = {
  commandId: Schema.NonEmptyString,
  at: Schema.NonEmptyString,
};

export const IssueCreated = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssueCreated"),
  issueId: Schema.NonEmptyString,
  issueKey: Schema.NonEmptyString,
  projectId: Schema.NonEmptyString,
  title: Schema.NonEmptyString,
  status: IssueStatusSchema,
  priority: IssuePrioritySchema,
  creatorId: Schema.NonEmptyString,
});

export const IssueRenamed = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssueRenamed"),
  title: Schema.NonEmptyString,
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
  commentId: Schema.NonEmptyString,
  authorId: Schema.NonEmptyString,
  body: Schema.NonEmptyString,
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
    issueId: Schema.NonEmptyString,
    /** Detail-stream position the board should start from, when known. */
    from: Schema.NullOr(Schema.NonEmptyString),
  }),
  Schema.Struct({ type: Schema.Literal("IssueLeft"), issueId: Schema.NonEmptyString }),
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
  switch (event.type) {
    case "IssueRenamed":
      return { ...touched, title: event.title };
    case "IssueStatusChanged":
      return { ...touched, status: event.status };
    case "IssuePriorityChanged":
      return { ...touched, priority: event.priority };
    case "IssueAssigned":
      return { ...touched, assigneeId: event.assigneeId };
    case "CommentAdded":
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
}
