import { Streamsy } from "@streamsy/core";
import { LabelRow, ProjectRow, UserRow } from "../domain/catalog.ts";
import { Identifier, IssueEvent, IssueLabelEvent } from "../domain/issue.ts";

const params = { workspaceId: Identifier } as const;
export class Events extends Streamsy.json<Events>()(
  "Bugsy/Events",
  "bugsy/:workspaceId/issue-events",
  {
    params,
    schema: IssueEvent,
  },
) {}
export class LabelEvents extends Streamsy.json<LabelEvents>()(
  "Bugsy/LabelEvents",
  "bugsy/:workspaceId/issue-label-events",
  {
    params,
    schema: IssueLabelEvent,
  },
) {}

export class Projects extends Streamsy.state<Projects>()(
  "Bugsy/Projects",
  "bugsy/:workspaceId/projects",
  {
    params,
    collections: { project: { schema: ProjectRow, key: "projectId" } },
  },
) {}
export class Users extends Streamsy.state<Users>()("Bugsy/Users", "bugsy/:workspaceId/users", {
  params,
  collections: { user: { schema: UserRow, key: "userId" } },
}) {}
export class Labels extends Streamsy.state<Labels>()("Bugsy/Labels", "bugsy/:workspaceId/labels", {
  params,
  collections: { label: { schema: LabelRow, key: "labelId" } },
}) {}
export const routes = {
  events: Events,
  labelEvents: LabelEvents,
  projects: Projects,
  users: Users,
  labels: Labels,
} as const;
