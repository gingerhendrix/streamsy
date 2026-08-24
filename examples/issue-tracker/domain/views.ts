/**
 * Four inert A1 proof declarations. They compile and check, but no Slice 1 host
 * registers or executes their post-reducer operators.
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
  LabelRow,
  ProjectBoardCard,
  ProjectRow,
  RecentActivityRow,
  Sequence,
  UserRow,
} from "./issue.ts";
import type {
  AssigneeQueueRow as AssigneeQueueRowType,
  IssueEvent as IssueEventType,
  IssueLabelRow as IssueLabelRowType,
  IssueRow as IssueRowType,
  LabelCountRow as LabelCountRowType,
  LabelRow as LabelRowType,
  ProjectBoardCard as ProjectBoardCardType,
  ProjectRow as ProjectRowType,
  RecentActivityRow as RecentActivityRowType,
  UserRow as UserRowType,
} from "./issue.ts";

const issue = selectors<IssueRowType>();
const project = selectors<ProjectRowType>();
const user = selectors<UserRowType>();
const label = selectors<LabelRowType>();
const issueLabel = selectors<IssueLabelRowType>();
const activity = selectors<IssueEventType>();

export const issueRows = source("issue-tracker.issues", {
  schema: IssueRow,
  schemaRef: { name: "issue-tracker.IssueRow", version: 1 },
  key: issue.row.issueId,
  order: issue.row.updatedAt,
  partitionBy: issue.row.workspaceId,
});

export const projects = source("issue-tracker.projects", {
  schema: ProjectRow,
  schemaRef: { name: "issue-tracker.ProjectRow", version: 1 },
  key: project.row.projectId,
  order: project.row.revision,
  partitionBy: project.row.workspaceId,
});

export const users = source("issue-tracker.users", {
  schema: UserRow,
  schemaRef: { name: "issue-tracker.UserRow", version: 1 },
  key: user.row.userId,
  order: user.row.revision,
  partitionBy: user.row.workspaceId,
});

export const labels = source("issue-tracker.labels", {
  schema: LabelRow,
  schemaRef: { name: "issue-tracker.LabelRow", version: 1 },
  key: label.row.labelId,
  order: label.row.revision,
  partitionBy: label.row.workspaceId,
});

export const issueLabels = source("issue-tracker.issue-labels", {
  schema: IssueLabelRow,
  schemaRef: { name: "issue-tracker.IssueLabelRow", version: 1 },
  key: issueLabel.key(issueLabel.row.issueId, issueLabel.row.labelId),
  order: issueLabel.row.revision,
  partitionBy: issueLabel.row.issueId,
});

export const issueActivity = source("issue-tracker.issue-events", {
  schema: IssueEvent,
  schemaRef: { name: "issue-tracker.IssueEvent", version: 1 },
  key: activity.row.eventId,
  order: activity.row.sequence,
  partitionBy: activity.row.workspaceId,
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
  key: boardCard.row.issueId,
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
      .keyBy(boardCard.row.issueId)
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
  key: queueRow.row.issueId,
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
        assigneeName: queue.row.assignee.displayName,
        title: queue.row.title,
        status: queue.row.status,
        updatedAt: queue.row.updatedAt,
      })
      .keyBy(queueRow.row.issueId)
      .top({ by: [queueRow.row.updatedAt.desc(), queueRow.row.issueId.asc()], limit: 100 }),
});

const membershipIssue = joinSelectors<IssueLabelRowType, IssueRowType>();
type MembershipWithIssue = IssueLabelRowType & { readonly issue: IssueRowType };
const membershipLabel = joinSelectors<MembershipWithIssue, LabelRowType>();
type LabelJoined = MembershipWithIssue & { readonly label: LabelRowType };
const labelJoined = selectors<LabelJoined>();
const labelCount = selectors<LabelCountRowType>();

export const labelCounts = defineView({
  name: "issue-tracker.label-counts",
  params: { projectId },
  schema: LabelCountRow,
  schemaRef: { name: "issue-tracker.LabelCountRow", version: 1 },
  key: labelCount.row.labelId,
  query: (params) =>
    from(issueLabels)
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
      .aggregate({ issueCount: aggregate.count() })
      .keyBy(labelCount.row.labelId),
});

const recent = selectors<RecentActivityRowType>();

export const recentActivity = defineView({
  name: "issue-tracker.recent-activity",
  params: { workspaceId, limit: activityLimit },
  schema: RecentActivityRow,
  schemaRef: { name: "issue-tracker.RecentActivityRow", version: 1 },
  key: recent.row.eventId,
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
      .keyBy(recent.row.eventId)
      .top({ by: [recent.row.sequence.desc(), recent.row.eventId.asc()], limit: params.limit }),
});

export const a1Views = Object.freeze([projectBoard, assigneeQueue, labelCounts, recentActivity]);
