import { Schema } from "effect";
import { Identifier, IssueStatus, Title } from "../domain/issue.ts";

export const CommandRequest = Schema.Union([
  Schema.Struct({ type: Schema.Literal("create"), commandId: Identifier, issueId: Identifier, projectId: Identifier, title: Title, status: Schema.optionalKey(IssueStatus) }),
  Schema.Struct({ type: Schema.Literal("status"), commandId: Identifier, issueId: Identifier, status: IssueStatus }),
  Schema.Struct({ type: Schema.Literal("assign"), commandId: Identifier, issueId: Identifier, assigneeId: Identifier }),
]);
export type CommandRequest = typeof CommandRequest.Type;
