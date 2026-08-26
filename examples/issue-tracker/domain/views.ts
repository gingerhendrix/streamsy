/**
 * The A1 proof declarations, and the two Integration 2 executes as product.
 *
 * Every relation these views join is declared exactly once. The catalog's
 * `projects`, `users` and `labels` sources are imported from `catalog.ts`
 * rather than restated here: before Integration 2 this module declared its own
 * `issue-tracker.ProjectRow`, `issue-tracker.UserRow` and
 * `issue-tracker.LabelRow` sources whose shapes differed from the catalog rows
 * the host actually ingests, so one descriptor named two shapes and a plan
 * claimed a row it never saw. The contract-freeze audit removed the duplicates;
 * see `integration-2-decisions.md`.
 */
import {
  aggregate,
  defineView,
  from,
  joinSelectors,
  parameter,
  selectors,
  source,
} from "@streamsy/views";
import {
  AssigneeQueueRow,
  Identifier,
  IssueEvent,
  IssueLabelRow,
  IssueRow,
  LabelCountRow,
  ProjectBoardCard,
  RecentActivityRow,
  Sequence,
} from "./issue.ts";
import type {
  AssigneeQueueRow as AssigneeQueueRowType,
  IssueEvent as IssueEventType,
  IssueLabelRow as IssueLabelRowType,
  IssueRow as IssueRowType,
  ProjectBoardCard as ProjectBoardCardType,
  RecentActivityRow as RecentActivityRowType,
} from "./issue.ts";
import { labels, projects, users } from "./catalog.ts";
import type {
  LabelRow as LabelRowType,
  ProjectRow as ProjectRowType,
  UserRow as UserRowType,
} from "./catalog.ts";

export { labels, projects, users };

const issue = selectors<IssueRowType>();
const issueLabel = selectors<IssueLabelRowType>();
const activity = selectors<IssueEventType>();

export const issueRows = source("issue-tracker.issues", {
  schema: IssueRow,
  schemaRef: { name: "issue-tracker.IssueRow", version: 1 },
  partitionBy: issue.row.workspaceId,
  key: "issueId",
  mode: "state",
});

/**
 * The maintained membership relation, read as state.
 *
 * It is partitioned by workspace like every other relation in this
 * application, and keyed by the derived `membershipId` the canonical fact
 * carries, so the plan's key and the fold's key are the same declared field.
 */
export const issueLabels = source("issue-tracker.issue-labels", {
  schema: IssueLabelRow,
  schemaRef: { name: "issue-tracker.IssueLabelRow", version: 2 },
  partitionBy: issueLabel.row.workspaceId,
  key: "membershipId",
  mode: "state",
});

export const issueActivity = source("issue-tracker.issue-events", {
  schema: IssueEvent,
  schemaRef: { name: "issue-tracker.IssueEvent", version: 1 },
  partitionBy: activity.row.workspaceId,
  key: "eventId",
  mode: "facts",
});

const projectId = parameter("projectId", Identifier, {
  schemaRef: { name: "issue-tracker.Identifier", version: 1 },
});
const assigneeId = parameter("assigneeId", Identifier, {
  schemaRef: { name: "issue-tracker.Identifier", version: 1 },
});
const workspaceId = parameter("workspaceId", Identifier, {
  schemaRef: { name: "issue-tracker.Identifier", version: 1 },
});
const activityLimit = parameter("limit", Sequence, {
  schemaRef: { name: "issue-tracker.Sequence", version: 1 },
  maximum: 200,
});

const issueProject = joinSelectors<IssueRowType, ProjectRowType>();
type IssueWithProject = IssueRowType & { readonly project: ProjectRowType };
const issueProjectUser = joinSelectors<IssueWithProject, UserRowType>();
type BoardJoined = IssueWithProject & { readonly assignee?: UserRowType };
const board = selectors<BoardJoined>();
const boardCard = selectors<ProjectBoardCardType>();

export const projectBoard = defineView({
  name: "issue-tracker.project-board",
  params: { projectId },
  schema: ProjectBoardCard,
  schemaRef: { name: "issue-tracker.ProjectBoardCard", version: 1 },
  key: "issueId",
  query: (params) =>
    from(issueRows)
      .where(issue.row.projectId.eq(params.projectId))
      .join(projects, {
        on: issueProject.left.projectId.eq(issueProject.right.projectId),
        as: "project",
      })
      .leftJoin(users, {
        on: issueProjectUser.left.assigneeId.value.eq(issueProjectUser.right.userId),
        as: "assignee",
      })
      .select({
        issueId: board.row.issueId,
        projectId: board.row.projectId,
        projectName: board.row.project.name,
        title: board.row.title,
        status: board.row.status,
        assignee: board.row.assigneeId.orElse("unassigned"),
        updatedAt: board.row.updatedAt,
      })
      .top({
        by: [boardCard.row.updatedAt.desc(), boardCard.row.issueId.asc()],
        partitionBy: [boardCard.row.status],
        limit: 100,
      }),
});

const issueUser = joinSelectors<IssueRowType, UserRowType>();
type QueueJoined = IssueRowType & { readonly assignee: UserRowType };
const queue = selectors<QueueJoined>();
const queueRow = selectors<AssigneeQueueRowType>();

export const assigneeQueue = defineView({
  name: "issue-tracker.assignee-queue",
  params: { assigneeId },
  schema: AssigneeQueueRow,
  schemaRef: { name: "issue-tracker.AssigneeQueueRow", version: 1 },
  key: "issueId",
  query: (params) =>
    from(issueRows)
      .where(issue.row.assigneeId.isPresent())
      .where(issue.row.assigneeId.value.eq(params.assigneeId))
      .join(users, {
        on: issueUser.left.assigneeId.value.eq(issueUser.right.userId),
        as: "assignee",
      })
      .select({
        issueId: queue.row.issueId,
        assigneeId: queue.row.assigneeId.value,
        assigneeName: queue.row.assignee.name,
        title: queue.row.title,
        status: queue.row.status,
        updatedAt: queue.row.updatedAt,
      })
      .top({ by: [queueRow.row.updatedAt.desc(), queueRow.row.issueId.asc()], limit: 100 }),
});

const membershipIssue = joinSelectors<IssueLabelRowType, IssueRowType>();
type MembershipWithIssue = IssueLabelRowType & { readonly issue: IssueRowType };
const membershipLabel = joinSelectors<MembershipWithIssue, LabelRowType>();
type LabelJoined = MembershipWithIssue & { readonly label: LabelRowType };
const labelJoined = selectors<LabelJoined>();

export const labelCounts = defineView({
  name: "issue-tracker.label-counts",
  params: { projectId },
  schema: LabelCountRow,
  schemaRef: { name: "issue-tracker.LabelCountRow", version: 1 },
  key: "labelId",
  query: (params) =>
    from(issueLabels)
      // A detached membership stays in the relation, so the count is over the
      // attached ones. Without this the relation's tombstones would be counted.
      .where(issueLabel.row.attached.eq(true))
      .join(issueRows, {
        on: membershipIssue.left.issueId.eq(membershipIssue.right.issueId),
        as: "issue",
      })
      .where(labelJoined.row.issue.projectId.eq(params.projectId))
      .join(labels, {
        on: membershipLabel.left.labelId.eq(membershipLabel.right.labelId),
        as: "label",
      })
      .groupBy({
        labelId: labelJoined.row.label.labelId,
        labelName: labelJoined.row.label.name,
      })
      .aggregate({ issueCount: aggregate.count() }),
});

const recent = selectors<RecentActivityRowType>();

export const recentActivity = defineView({
  name: "issue-tracker.recent-activity",
  params: { workspaceId, limit: activityLimit },
  schema: RecentActivityRow,
  schemaRef: { name: "issue-tracker.RecentActivityRow", version: 1 },
  key: "eventId",
  query: (params) =>
    from(issueActivity)
      .where(activity.row.workspaceId.eq(params.workspaceId))
      .select({
        eventId: activity.row.eventId,
        issueId: activity.row.issueId,
        eventType: activity.row.type,
        status: activity.row.status,
        sequence: activity.row.sequence,
        occurredAt: activity.row.occurredAt,
      })
      .top({ by: [recent.row.sequence.desc(), recent.row.eventId.asc()], limit: params.limit }),
});

export const a1Views = Object.freeze([projectBoard, assigneeQueue, labelCounts, recentActivity]);

/** The Integration 1 host registry: these checked declarations are executable A2 plans. */
export const executableViews = Object.freeze({
  projectBoard,
  assigneeQueue,
  labelCounts,
  recentActivity,
});
