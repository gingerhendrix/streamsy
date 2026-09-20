import { Schema } from "effect";
import { Identifier, IssueStatus, Timestamp, Title } from "./issue.ts";

export const NotificationDraft = Schema.Struct({
  workspaceId: Identifier, issueId: Identifier, assigneeId: Identifier,
  title: Title, status: IssueStatus, eventId: Identifier, occurredAt: Timestamp,
});
export type NotificationDraft = typeof NotificationDraft.Type;
