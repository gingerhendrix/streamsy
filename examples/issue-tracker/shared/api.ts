/**
 * The wire contracts shared by the host, the scripts, the tests and the
 * browser.
 *
 * Request bodies are decoded here before any workflow sees them, so an unknown
 * status or a wrong-typed field is a 400 and never a durable fact. Response
 * shapes are declared here too, so a test decodes exactly what a client would.
 */
import { Schema } from "effect";
import { DomainKind } from "../domain/domains.ts";
import { EXCHANGE_CURSOR_DOMAIN } from "../domain/exchange.ts";
import { InboxRow } from "../domain/inbox.ts";
import {
  Identifier,
  IssueLabelRow,
  IssueRow,
  IssueStatus,
  IssueTransition,
  LabelCountRow,
  Title,
} from "../domain/issue.ts";
import { AssignmentNotification } from "../domain/notifications.ts";
import { CatalogCollection } from "../domain/catalog.ts";

export const CreateIssueRequest = Schema.Struct({
  commandId: Identifier,
  issueId: Identifier,
  projectId: Identifier,
  title: Title,
  status: Schema.optionalKey(IssueStatus),
});
export type CreateIssueRequest = typeof CreateIssueRequest.Type;

export const ChangeStatusRequest = Schema.Struct({
  commandId: Identifier,
  status: IssueStatus,
});
export type ChangeStatusRequest = typeof ChangeStatusRequest.Type;

export const AssignIssueRequest = Schema.Struct({
  commandId: Identifier,
  assigneeId: Identifier,
});
export type AssignIssueRequest = typeof AssignIssueRequest.Type;

/**
 * One membership command.
 *
 * The issue is in the path and the label is in the body, which is the same
 * split `assignIssue` uses: the path names what is being changed, the body
 * names what it is being changed to.
 */
export const LabelMembershipRequest = Schema.Struct({
  commandId: Identifier,
  labelId: Identifier,
});
export type LabelMembershipRequest = typeof LabelMembershipRequest.Type;

export const Ack = Schema.Struct({ stream: Schema.String, offset: Schema.String });

export const MaintenanceReportBody = Schema.Struct({
  checkpoint: Schema.NullOr(Schema.String),
  folded: Schema.Number,
  changed: Schema.Number,
  publication: Schema.Literals(["none", "changes", "snapshot"]),
});

export const CommandResponse = Schema.Struct({
  commandId: Schema.String,
  workspaceId: Schema.String,
  issueId: Schema.String,
  eventId: Schema.String,
  sequence: Schema.Number,
  ack: Ack,
  /** True when this command had already been accepted; `ack` is the original. */
  reconciled: Schema.Boolean,
  maintenance: MaintenanceReportBody,
  row: Schema.NullOr(IssueRow),
});
export type CommandResponse = typeof CommandResponse.Type;

export const LabelCommandResponse = Schema.Struct({
  commandId: Schema.String,
  workspaceId: Schema.String,
  issueId: Schema.String,
  labelId: Schema.String,
  membershipId: Schema.String,
  attached: Schema.Boolean,
  eventId: Schema.String,
  sequence: Schema.Number,
  ack: Ack,
  reconciled: Schema.Boolean,
  maintenance: MaintenanceReportBody,
  row: Schema.NullOr(IssueLabelRow),
});
export type LabelCommandResponse = typeof LabelCommandResponse.Type;

/** Every membership the workspace maintains, attached or not. */
export const IssueLabelsResponse = Schema.Struct({
  workspaceId: Schema.String,
  relation: Schema.String,
  rows: Schema.Array(IssueLabelRow),
});
export type IssueLabelsResponse = typeof IssueLabelsResponse.Type;

/**
 * The maintained label counts, as a plain read model.
 *
 * The live product is the checked State sink; this endpoint exists so a script
 * or a test can assert the counts without binding a session, and so the two can
 * be compared.
 */
export const LabelCountsResponse = Schema.Struct({
  workspaceId: Schema.String,
  view: Schema.String,
  sink: Schema.String,
  contractFingerprint: Schema.String,
  rows: Schema.Array(LabelCountRow),
});
export type LabelCountsResponse = typeof LabelCountsResponse.Type;

export const IssuesResponse = Schema.Struct({
  workspaceId: Schema.String,
  view: Schema.String,
  planHash: Schema.String,
  rows: Schema.Array(IssueRow),
});
export type IssuesResponse = typeof IssuesResponse.Type;

export const CatalogUpsertRequest = Schema.Struct({
  key: Identifier,
  value: Schema.Json,
});
export type CatalogUpsertRequest = typeof CatalogUpsertRequest.Type;

export const CatalogRowsResponse = Schema.Struct({
  workspaceId: Schema.String,
  collection: CatalogCollection,
  checkpoint: Schema.NullOr(Schema.String),
  folded: Schema.Number,
  changed: Schema.Number,
  rows: Schema.Array(Schema.Json),
});
export type CatalogRowsResponse = typeof CatalogRowsResponse.Type;

/**
 * One page of the issue-transitions feed.
 *
 * `order` is a literal rather than a free string: a consumer that decodes this
 * page has checked that the feed still promises arrival order, so it can append
 * the events as they came without re-sorting them. `nextOffset` is a batch
 * boundary, so resuming from it neither re-reads nor skips half a batch.
 */
export const TransitionFeedResponse = Schema.Struct({
  sink: Schema.String,
  feed: Schema.Struct({
    name: Schema.String,
    type: Schema.String,
    /** The key of the relation whose changes this feed carries. */
    subjectKey: Schema.String,
  }),
  order: Schema.Literal("arrival"),
  events: Schema.Array(IssueTransition),
  nextOffset: Schema.String,
  upToDate: Schema.Boolean,
});
export type TransitionFeedResponse = typeof TransitionFeedResponse.Type;

/** What a consumer needs to bind the sink's public product. */
export const SinkSessionResponse = Schema.Struct({
  sink: Schema.String,
  route: Schema.String,
  transport: Schema.Literal("durable-state"),
  fallback: Schema.Literal("snapshot-then-live"),
  protocolVersion: Schema.Literal(1),
  durableStateVersion: Schema.Literal(1),
  contractFingerprint: Schema.String,
  /** Native Durable Streams offset at the sink's current tail. */
  offset: Schema.String,
  /** The workspace's second checked State product, named in the same round trip. */
  labelCounts: Schema.Struct({
    sink: Schema.String,
    route: Schema.String,
    contractFingerprint: Schema.String,
    offset: Schema.String,
  }),
});
export type SinkSessionResponse = typeof SinkSessionResponse.Type;

export const SeedResponse = Schema.Struct({
  workspaceId: Schema.String,
  issues: Schema.Array(Schema.String),
  /** The catalog labels a seeded workspace opens onto. */
  labels: Schema.Array(Schema.String),
  seeded: Schema.Boolean,
});
export type SeedResponse = typeof SeedResponse.Type;

export const HealthResponse = Schema.Struct({
  status: Schema.Literal("ok"),
  deployment: Schema.String,
  schemaVersion: Schema.String,
  view: Schema.String,
  planHash: Schema.String,
});
export type HealthResponse = typeof HealthResponse.Type;

/** One durable delivery decision, as an operator reads it. */
export const NotificationEntry = Schema.Struct({
  id: Schema.Number,
  idempotencyKey: Schema.String,
  state: Schema.Literals(["pending", "delivered", "dead"]),
  attempts: Schema.Number,
  nextAttemptAtMs: Schema.Number,
  lastError: Schema.NullOr(Schema.String),
  deadLetterReason: Schema.NullOr(
    Schema.Literals(["attempts-exhausted", "permanent", "payload-poison"]),
  ),
  payload: AssignmentNotification,
});

export const NotificationsResponse = Schema.Struct({
  workspaceId: Schema.String,
  sink: Schema.String,
  contractFingerprint: Schema.String,
  pending: Schema.Number,
  delivered: Schema.Number,
  dead: Schema.Number,
  outbox: Schema.Array(NotificationEntry),
  /** What the handler actually accepted, deduplicated by idempotency key. */
  notified: Schema.Array(AssignmentNotification),
});
export type NotificationsResponse = typeof NotificationsResponse.Type;

export const DrainResponse = Schema.Struct({
  workspaceId: Schema.String,
  sink: Schema.String,
  claimed: Schema.Number,
  delivered: Schema.Number,
  retried: Schema.Number,
  deadLettered: Schema.Number,
});
export type DrainResponse = typeof DrainResponse.Type;

/**
 * One user's cross-workspace inbox.
 *
 * The rows come from more than one workspace, which is the whole point: this
 * is the first product surface in the tracker that no single workspace
 * partition could have served.
 */
export const InboxResponse = Schema.Struct({
  userId: Identifier,
  exchange: Schema.String,
  rows: Schema.Array(InboxRow),
});
export type InboxResponse = typeof InboxResponse.Type;

/**
 * Where every exchange has got to.
 *
 * `domain` is decoded as the exchange cursor's own literal, so a client that
 * decodes this response has *checked* that the position it is reading is an
 * exchange position and not a stream offset or a store checkpoint.
 */
export const ExchangeCursorBody = Schema.Struct({
  domain: Schema.Literal(EXCHANGE_CURSOR_DOMAIN),
  exchange: Schema.String,
  version: Schema.Number,
  source: Schema.Struct({ kind: DomainKind, id: Identifier }),
  arrival: Schema.Number,
  applied: Schema.Number,
});

export const ExchangeStatusResponse = Schema.Struct({
  cursors: Schema.Array(ExchangeCursorBody),
});
export type ExchangeStatusResponse = typeof ExchangeStatusResponse.Type;

export const ApiError = Schema.Struct({
  error: Schema.String,
  detail: Schema.optionalKey(Schema.String),
  /** Present when the failure has a declared recovery, such as an unavailable offset. */
  fallback: Schema.optionalKey(Schema.String),
});
export type ApiError = typeof ApiError.Type;
