import { StreamFamily } from "@streamsy/core";
import { LabelRow, ProjectRow, UserRow } from "../domain/catalog.ts";
import { Identifier, IssueEvent, IssueLabelEvent } from "../domain/issue.ts";

const params = { workspaceId: Identifier } as const;
export class Events extends StreamFamily.json<Events>()(
  "IssueTracker/Events",
  "issue-tracker/:workspaceId/issue-events",
  {
    params,
    schema: IssueEvent,
  },
) {}
export class LabelEvents extends StreamFamily.json<LabelEvents>()(
  "IssueTracker/LabelEvents",
  "issue-tracker/:workspaceId/issue-label-events",
  {
    params,
    schema: IssueLabelEvent,
  },
) {}

export class Projects extends StreamFamily.state<Projects>()(
  "IssueTracker/Projects",
  "issue-tracker/:workspaceId/projects",
  {
    params,
    collections: { project: { schema: ProjectRow, key: "projectId" } },
  },
) {}
export const projectStream = (workspaceId: string) => Projects.at({ workspaceId });
export class Users extends StreamFamily.state<Users>()(
  "IssueTracker/Users",
  "issue-tracker/:workspaceId/users",
  {
    params,
    collections: { user: { schema: UserRow, key: "userId" } },
  },
) {}
export const userStream = (workspaceId: string) => Users.at({ workspaceId });
export class Labels extends StreamFamily.state<Labels>()(
  "IssueTracker/Labels",
  "issue-tracker/:workspaceId/labels",
  {
    params,
    collections: { label: { schema: LabelRow, key: "labelId" } },
  },
) {}
export const labelStream = (workspaceId: string) => Labels.at({ workspaceId });
export const routes = {
  events: Events,
  labelEvents: LabelEvents,
  projects: Projects,
  users: Users,
  labels: Labels,
} as const;
export const refs = (workspaceId: string) =>
  Object.values(routes).map((route) => route.ref({ workspaceId }));
