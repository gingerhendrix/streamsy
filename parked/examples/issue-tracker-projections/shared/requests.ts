/**
 * Schemas for every request body the API accepts.
 *
 * The HTTP edge is an external trust boundary, so a body is decoded here before
 * any application code sees it. Nothing downstream re-checks a title, an
 * identifier, a status, or a team member: if it decoded, it is already a value
 * of the declared type.
 *
 * `shared/api.ts` re-exports the derived types, so the browser keeps importing
 * the same names with `import type` and pulls no Effect runtime into its bundle.
 */
import { Schema } from "effect";
import { Identifier, IssuePrioritySchema, IssueStatusSchema, MemberId, Prose } from "./model.ts";

export { Identifier, MemberId, Prose } from "./model.ts";
export type { MemberId as MemberIdType } from "./model.ts";

export const CreateProjectRequest = Schema.Struct({
  projectId: Identifier,
  projectKey: Identifier,
  name: Prose,
});
export type CreateProjectRequest = Schema.Schema.Type<typeof CreateProjectRequest>;

export const CreateIssueRequest = Schema.Struct({
  commandId: Schema.NonEmptyString,
  issueId: Identifier,
  projectId: Identifier,
  title: Prose,
  priority: Schema.optionalKey(IssuePrioritySchema),
  status: Schema.optionalKey(IssueStatusSchema),
  creatorId: Schema.optionalKey(MemberId),
});
export type CreateIssueRequest = Schema.Schema.Type<typeof CreateIssueRequest>;

const CommandBase = { commandId: Schema.NonEmptyString };

export const IssueCommandRequest = Schema.Union([
  Schema.Struct({ ...CommandBase, type: Schema.Literal("rename"), title: Prose }),
  Schema.Struct({ ...CommandBase, type: Schema.Literal("status"), status: IssueStatusSchema }),
  Schema.Struct({
    ...CommandBase,
    type: Schema.Literal("priority"),
    priority: IssuePrioritySchema,
  }),
  Schema.Struct({
    ...CommandBase,
    type: Schema.Literal("assign"),
    assigneeId: Schema.NullOr(MemberId),
  }),
  Schema.Struct({
    ...CommandBase,
    type: Schema.Literal("comment"),
    commentId: Identifier,
    authorId: MemberId,
    body: Prose,
  }),
]);
export type IssueCommandRequest = Schema.Schema.Type<typeof IssueCommandRequest>;
