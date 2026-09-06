/**
 * The user domain's product: one person's cross-workspace assignment inbox.
 *
 * An inbox row is a *projection of one canonical fact*, and nothing else. It
 * carries only what `IssueAssigned` carries, so the same fact projects to a
 * byte-identical row every time it is exchanged — which is what makes the
 * exchange replay-safe without a second reconciliation mechanism. Enriching
 * the row from the maintained relation would have made it a function of *when*
 * the exchange ran, and a replay would then rewrite history.
 *
 * `inboxId` is the row key and it is derived, not generated: the source
 * workspace and the canonical event id. Two workspaces cannot collide, and one
 * fact cannot produce two rows however many times it is delivered.
 */
import { Schema } from "effect";
import { Identifier, IssueStatus, Sequence, Timestamp } from "./issue.ts";

/**
 * A derived key of two identifiers.
 *
 * It has its own pattern rather than reusing `Identifier`: two 64-character
 * ids and a separator do not fit in an identifier, and silently truncating a
 * row key is exactly the kind of collision this key exists to prevent.
 */
export const INBOX_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$/;
export const InboxId = Schema.String.check(Schema.isPattern(INBOX_ID_PATTERN));

export const InboxRow = Schema.Struct({
  inboxId: InboxId,
  /** The user this row belongs to. Always the partition the row lives in. */
  userId: Identifier,
  /** The workspace the assignment happened in. */
  workspaceId: Identifier,
  issueId: Identifier,
  status: IssueStatus,
  /** The canonical fact this row projects. */
  eventId: Identifier,
  occurredAt: Timestamp,
  /** The fact's domain sequence inside its source workspace. */
  sequence: Sequence,
  /** How many source records the exchange had consumed when this one arrived. */
  arrival: Sequence,
});
export type InboxRow = typeof InboxRow.Type;

export const decodeInboxRow = Schema.decodeUnknownSync(InboxRow);

/** The row key one source fact produces. Derived, so a replay cannot duplicate it. */
export function inboxIdOf(workspaceId: string, eventId: string): string {
  return `${workspaceId}.${eventId}`;
}

/**
 * The order an inbox is read in.
 *
 * Total and content-derived: no two rows compare equal, because `inboxId` is
 * unique, so two hosts holding the same rows serve them in the same order
 * whatever order they were exchanged in.
 */
export function compareInboxRows(left: InboxRow, right: InboxRow): number {
  if (left.occurredAt !== right.occurredAt) return left.occurredAt < right.occurredAt ? -1 : 1;
  if (left.workspaceId !== right.workspaceId) {
    return left.workspaceId < right.workspaceId ? -1 : 1;
  }
  if (left.sequence !== right.sequence) return left.sequence - right.sequence;
  return left.inboxId < right.inboxId ? -1 : left.inboxId > right.inboxId ? 1 : 0;
}
