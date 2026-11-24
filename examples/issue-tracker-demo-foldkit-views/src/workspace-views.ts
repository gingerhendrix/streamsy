import { Streamsy } from "@streamsy/core";
import { View } from "@streamsy/views";
import { Schema } from "effect";
import {
  Comment,
  Issue,
  IssueStatus,
  Project,
} from "@streamsy/issue-tracker-demo-contract/state-schema";

// The base server declares the same route and schemas in server/streams.ts.
// Its module also loads Bun storage, so declare the browser route here.
export class Workspaces extends Streamsy.state<Workspaces>()(
  "IssueTrackerDemo/Workspaces",
  "workspace/:workspaceId",
  {
    params: { workspaceId: Schema.String },
    collections: {
      project: { schema: Project, key: "id" },
      issue: { schema: Issue, key: "id" },
      comment: { schema: Comment, key: "id" },
    },
  },
) {}

const projects = View.from(Workspaces, "project");

const issues = View.from(Workspaces, "issue");

const comments = View.from(Workspaces, "comment");

export const workspaceViews = View.live({
  id: "issue-tracker-page",
  params: Workspaces.paramCodecs,
  views: {
    projects,
    visibleIssues: issues.join(projects, {
      as: "project",
      on: (issue, project) => issue.projectId.eq(project.id),
    }),
    commentsOn: comments,
    issueCountByProject: issues
      .groupBy((issue) => ({ projectId: issue.projectId }))
      .aggregate(() => ({ count: View.count() })),
    statusCounts: issues
      .groupBy((issue) => ({ projectId: issue.projectId, status: issue.status }))
      .aggregate(() => ({ count: View.count() })),
  },
});

export const Counts = Schema.Struct({
  issueCountByProject: Schema.Record(Schema.String, Schema.Int),
  statusCounts: Schema.Array(
    Schema.Struct({ projectId: Schema.String, status: IssueStatus, count: Schema.Int }),
  ),
});

export type Counts = typeof Counts.Type;
