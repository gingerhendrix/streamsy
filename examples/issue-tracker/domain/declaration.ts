/**
 * The user declaration this slice makes executable.
 *
 * This module is the whole "user code" surface of the vertical slice: sources, a
 * reducer, a view, and the three public sinks the view is published through — a
 * state sink for its rows, a stream sink for its transitions, and a document
 * sink for the workspace summary. Everything else in `server/` is the runtime
 * that carries it. Read it alongside the plan artefact — if a later
 * change makes `issues.plan` hash differently, the maintained state is a
 * different thing and the host must say so.
 */
import { defineStateSink, STATE_SINK_ERROR_TAGS } from "@streamsy/state-sink";
import {
  defineDocumentSink,
  defineStreamSink,
  DOCUMENT_SINK_ERROR_TAGS,
  STREAM_SINK_ERROR_TAGS,
} from "@streamsy/sinks";
import {
  decodeIdentifier,
  decodeIssueTransition,
  decodeLabelCountRow,
  decodeProjectBoardCard,
  decodeWorkspaceSummary,
  IssueEvent,
  IssueLabelEvent,
  IssueLabelRow,
  IssueRow,
} from "./issue.ts";
import type {
  IssueEvent as IssueEventType,
  IssueLabelEvent as IssueLabelEventType,
  IssueLabelRow as IssueLabelRowType,
  IssueRow as IssueRowType,
} from "./issue.ts";
import { changes, from, literal, reducer, selectors, source, view } from "@streamsy/views";
import { labelCounts, projectBoard } from "./views.ts";

/**
 * The four Durable State catalog sources are declared beside their row schemas
 * in `catalog.ts`, because one declaration there drives both their plan keys
 * and the host's schema/type/primary-key table. They are re-exported so this
 * module stays the whole declaration surface, and the summary document below
 * names them as relations it is derived from.
 */
import { labels, projects, users, workspaceMetadata } from "./catalog.ts";

export { labels, projects, users, workspaceMetadata };

/**
 * The one effect sink is declared beside its payload schema in
 * `notifications.ts`, and re-exported so this module stays the whole
 * declaration surface.
 */
export { assignmentNotifications } from "./notifications.ts";

/** Selectors over one canonical fact, the fold state, and the maintained row. */
const x = selectors<IssueEventType, IssueEventType, IssueRowType>();

export const issueEvents = source("issue-tracker.issue-events", {
  schema: IssueEvent,
  schemaRef: { name: "issue-tracker.IssueEvent", version: 1 },
  partitionBy: x.row.workspaceId,
  key: "eventId",
  mode: "facts",
});

export const issueLifecycle = reducer(
  { name: "issue-tracker.issue-lifecycle", version: 1 },
  {
    state: IssueRow,
    stateRef: { name: "issue-tracker.IssueRow", version: 1 },
    input: IssueEvent,
    discriminator: "type",
    evolve: {
      IssueCreated: (e) => ({
        issueId: e.event.issueId,
        workspaceId: e.event.workspaceId,
        projectId: e.event.projectId,
        title: e.event.title,
        status: e.event.status,
        updatedAt: e.event.occurredAt,
      }),
      IssueStatusChanged: (e) => ({
        status: e.event.status,
        updatedAt: e.event.occurredAt,
      }),
      IssueAssigned: (e) => ({
        assigneeId: e.event.assigneeId,
        updatedAt: e.event.occurredAt,
      }),
    },
  },
);

export const issues = view(
  "issue-tracker.issues",
  {
    schema: IssueRow,
    schemaRef: { name: "issue-tracker.IssueRow", version: 1 },
    key: "issueId",
  },
  from(issueEvents).reduceByKey({
    key: "issueId",
    reducer: issueLifecycle,
  }),
);

/**
 * The membership fact source, and the relation it folds into.
 *
 * Membership is a second canonical fact family on its own durable stream. The
 * declaration language is what makes that necessary rather than optional:
 * `reduceByKey` is only available directly on a fact source, so a plan cannot
 * filter one stream into two relations keyed by different fields. An issue is
 * keyed by `issueId`; a membership is keyed by an (issue, label) pair. Two
 * keys, two fact families, two streams.
 */
const m = selectors<IssueLabelEventType, IssueLabelEventType, IssueLabelRowType>();

export const issueLabelEvents = source("issue-tracker.issue-label-events", {
  schema: IssueLabelEvent,
  schemaRef: { name: "issue-tracker.IssueLabelEvent", version: 1 },
  partitionBy: m.row.workspaceId,
  key: "eventId",
  mode: "facts",
});

/**
 * `attached` is written as a literal rather than read off the fact.
 *
 * The two facts are already distinguished by their own tags, so a redundant
 * boolean on the wire would be a second place for the same truth to be told —
 * and a place it could be told wrongly. A literal in the evolve branch keeps
 * the fold inside the reference-and-literal language the interpreter executes.
 */
export const issueLabelLifecycle = reducer(
  { name: "issue-tracker.issue-label-lifecycle", version: 1 },
  {
    state: IssueLabelRow,
    stateRef: { name: "issue-tracker.IssueLabelRow", version: 2 },
    input: IssueLabelEvent,
    discriminator: "type",
    evolve: {
      LabelAttached: (e) => ({
        membershipId: e.event.membershipId,
        issueId: e.event.issueId,
        labelId: e.event.labelId,
        workspaceId: e.event.workspaceId,
        attached: literal(true),
        updatedAt: e.event.occurredAt,
      }),
      LabelDetached: (e) => ({
        attached: literal(false),
        updatedAt: e.event.occurredAt,
      }),
    },
  },
);

export const issueLabelMemberships = view(
  "issue-tracker.issue-labels",
  {
    schema: IssueLabelRow,
    schemaRef: { name: "issue-tracker.IssueLabelRow", version: 2 },
    key: "membershipId",
  },
  from(issueLabelEvents).reduceByKey({
    key: "membershipId",
    reducer: issueLabelLifecycle,
  }),
);

export const boardIssues = defineStateSink({
  name: "issue-tracker.board-issues",
  from: projectBoard,
  row: { decode: decodeProjectBoardCard },
  route: "/state/workspaces/:workspaceId/issues",
  params: { workspaceId: { decode: decodeIdentifier } },
  collection: { name: "issues", type: "issue" },
  protocol: {
    sessionVersion: 1,
    durableStateVersion: 1,
    transport: "durable-state",
    resume: true,
    fallback: "snapshot-then-live",
  },
  errors: STATE_SINK_ERROR_TAGS,
});

/**
 * Live label counts, published the same way the board is.
 *
 * This is the second checked State sink and the second contract fingerprint the
 * tracker has. It exists because "the label-count plan is tested" and "a person
 * can see label counts change" are different claims, and Integration 2 owes the
 * second one. It publishes into the workspace partition, which already owns
 * durable stream storage, a State publisher and a maintained-state store — so
 * the machinery this sink needs is the machinery the board already has.
 */
export const boardLabelCounts = defineStateSink({
  name: "issue-tracker.board-label-counts",
  from: labelCounts,
  row: { decode: decodeLabelCountRow },
  route: "/state/workspaces/:workspaceId/label-counts",
  params: { workspaceId: { decode: decodeIdentifier } },
  collection: { name: "labelCounts", type: "label-count" },
  protocol: {
    sessionVersion: 1,
    durableStateVersion: 1,
    transport: "durable-state",
    resume: true,
    fallback: "snapshot-then-live",
  },
  errors: STATE_SINK_ERROR_TAGS,
});

/**
 * The activity feed: what happened to `issue-tracker.issues`.
 *
 * `changes(issues)` is the declaration's own statement that this publishes the
 * relation's transitions rather than its rows, and the change stream carries
 * both the relation it is derived from and its key. Arrival order is the whole
 * ordering contract, so a consumer reading this feed sees transitions in the
 * order the fold observed the facts, never re-sorted by `sequence`.
 */
export const issueTransitions = defineStreamSink({
  name: "issue-tracker.issue-transitions",
  from: changes(issues),
  event: { decode: decodeIssueTransition },
  route: "/feed/workspaces/:workspaceId/issue-transitions",
  params: { workspaceId: { decode: decodeIdentifier } },
  feed: { name: "issue-transitions", type: "issue-transition" },
  protocol: {
    sessionVersion: 1,
    transport: "durable-stream",
    resume: true,
    order: "arrival",
    fallback: "replay-from-start",
  },
  errors: STREAM_SINK_ERROR_TAGS,
});

/**
 * The cached workspace summary.
 *
 * `from` names the relations the document is derived from, so what invalidates
 * it is part of the contract rather than a comment. The cache policy is
 * declared, not chosen by the host: the document is derived from live
 * relations, so a consumer may keep it but must revalidate it against the
 * entity tag before trusting it again.
 */
export const workspaceSummary = defineDocumentSink({
  name: "issue-tracker.workspace-summary",
  from: [issues, projects, users, labels],
  document: { decode: decodeWorkspaceSummary },
  route: "/document/workspaces/:workspaceId/summary",
  params: { workspaceId: { decode: decodeIdentifier } },
  cache: { visibility: "private", maxAgeSeconds: 0, mustRevalidate: true },
  errors: DOCUMENT_SINK_ERROR_TAGS,
});

/** Durable stream names. Identity and stream id are kept equal so lineage reads by inspection. */
export const streamNames = {
  issueEvents: (workspaceId: string): string => `workspaces/${workspaceId}/issue-events`,
  issueLabelEvents: (workspaceId: string): string => `workspaces/${workspaceId}/issue-label-events`,
  boardState: (workspaceId: string): string => `state/workspaces/${workspaceId}/issues`,
  labelCountState: (workspaceId: string): string => `state/workspaces/${workspaceId}/label-counts`,
  projects: (workspaceId: string): string => `state/workspaces/${workspaceId}/projects`,
  users: (workspaceId: string): string => `state/workspaces/${workspaceId}/users`,
  labels: (workspaceId: string): string => `state/workspaces/${workspaceId}/labels`,
  metadata: (workspaceId: string): string => `state/workspaces/${workspaceId}/metadata`,
  issueTransitions: (workspaceId: string): string => `workspaces/${workspaceId}/issue-transitions`,
} as const;
