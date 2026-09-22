import { StreamRoute } from "@streamsy/core";
import { LabelRow, ProjectRow, UserRow } from "../domain/catalog.ts";
import { Identifier, IssueEvent, IssueLabelEvent } from "../domain/issue.ts";

const params = { workspaceId: Identifier } as const;
export const events = StreamRoute.json("issue-tracker/:workspaceId/issue-events", {
  params,
  schema: IssueEvent,
});
export const labelEvents = StreamRoute.json("issue-tracker/:workspaceId/issue-label-events", {
  params,
  schema: IssueLabelEvent,
});

export const projects = StreamRoute.state("issue-tracker/:workspaceId/projects", {
  params,
  collections: { project: { schema: ProjectRow, key: "projectId" } },
});
export const projectStream = (workspaceId: string) => projects.ref({ workspaceId });
export const users = StreamRoute.state("issue-tracker/:workspaceId/users", {
  params,
  collections: { user: { schema: UserRow, key: "userId" } },
});
export const userStream = (workspaceId: string) => users.ref({ workspaceId });
export const labels = StreamRoute.state("issue-tracker/:workspaceId/labels", {
  params,
  collections: { label: { schema: LabelRow, key: "labelId" } },
});
export const labelStream = (workspaceId: string) => labels.ref({ workspaceId });
export const routes = { events, labelEvents, projects, users, labels } as const;
export const refs = (workspaceId: string) =>
  Object.values(routes).map((route) => route.ref({ workspaceId }));
