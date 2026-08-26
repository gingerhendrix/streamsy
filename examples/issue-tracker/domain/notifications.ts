/**
 * The tracker's one effect sink: notify an assignee when an issue lands on them.
 *
 * `boardIssues` publishes state a consumer reads. This declares the opposite
 * kind of product — an external effect the world observes once. So the contract
 * is not a route and a protocol, it is an idempotency key, a retry budget and a
 * dead-letter terminus.
 *
 * The key is the workspace and the accepted command's event id. That makes the
 * notification's identity a durable fact rather than an observation time, so a
 * retried command, a recovered receipt and a replayed enqueue all describe the
 * same delivery instead of three of them.
 */
import { defineEffectSink, type EffectSinkChange } from "@streamsy/effect-sink";
import { Schema } from "effect";
import { Identifier, IssueStatus, Timestamp, Title } from "./issue.ts";
import type { IssueEvent, IssueRow } from "./issue.ts";

export const AssignmentNotification = Schema.Struct({
  workspaceId: Identifier,
  issueId: Identifier,
  assigneeId: Identifier,
  /** The issue's title and status when the assignment was accepted. */
  title: Title,
  status: IssueStatus,
  /** The canonical fact that caused this notification. */
  eventId: Identifier,
  occurredAt: Timestamp,
});
export type AssignmentNotification = typeof AssignmentNotification.Type;

export const decodeAssignmentNotification = Schema.decodeUnknownSync(AssignmentNotification);
const encodeAssignmentNotification = Schema.encodeUnknownSync(
  Schema.fromJsonString(AssignmentNotification),
);

export const assignmentNotifications = defineEffectSink<
  AssignmentNotification,
  { readonly key: "issueId" }
>({
  name: "issue-tracker.assignment-notifications",
  from: { key: "issueId" },
  handler: { name: "issue-tracker.notify-assignee", version: 1 },
  payload: {
    encode: (value) => encodeAssignmentNotification(value),
    decode: (value) => decodeAssignmentNotification(value),
  },
  idempotencyKey: (value) => `${value.workspaceId}/${value.eventId}`,
  partitionBy: (value) => value.workspaceId,
  /**
   * Three attempts with a wide, clamped backoff. A notifier that is briefly
   * unreachable recovers inside the budget; one that is genuinely broken stops
   * consuming the lane instead of retrying forever.
   */
  delivery: {
    maxAttempts: 3,
    initialBackoffMs: 250,
    backoffFactor: 4,
    maxBackoffMs: 30_000,
  },
});

/**
 * The notification one issue change implies, if any.
 *
 * Only a change that *newly* points the row at an assignee is an assignment: a
 * status move that leaves the assignee alone is not, and neither is a repeat of
 * the assignee the row already carried. An `exit` never notifies, because a row
 * leaving the relation is not an assignment.
 */
export function assignmentOf(
  change: EffectSinkChange<IssueRow>,
  event: IssueEvent,
): AssignmentNotification | undefined {
  if (change.kind === "exit") return undefined;
  const assigneeId = change.after.assigneeId;
  if (assigneeId === undefined) return undefined;
  const before = change.kind === "update" ? change.before.assigneeId : undefined;
  if (before === assigneeId) return undefined;
  return {
    workspaceId: change.after.workspaceId,
    issueId: change.after.issueId,
    assigneeId,
    title: change.after.title,
    status: change.after.status,
    eventId: event.eventId,
    occurredAt: change.after.updatedAt,
  };
}
