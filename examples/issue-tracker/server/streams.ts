import { StreamRef, StreamRoute } from "@streamsy/core";
import { Option } from "effect";
import { LabelRow, ProjectRow, UserRow } from "../domain/catalog.ts";
import { Identifier, IssueEvent, IssueLabelEvent } from "../domain/issue.ts";

const params = { workspaceId: Identifier } as const;
export const events = StreamRoute.json("issue-tracker/:workspaceId/issue-events", { params, schema: IssueEvent });
export const labelEvents = StreamRoute.json("issue-tracker/:workspaceId/issue-label-events", { params, schema: IssueLabelEvent });

const parseState = (suffix: string, id: string) => {
  const match = id.match(new RegExp(`^issue-tracker/([^/]+)/${suffix}$`));
  if (match?.[1] === undefined) return Option.none();
  return Option.some({ workspaceId: match[1] });
};
export const projects = StreamRoute.custom({
  parse: (id) => parseState("projects", id),
  ref: ({ workspaceId }) => StreamRef.state(`issue-tracker/${workspaceId}/projects`, { collections: { project: { schema: ProjectRow, key: "projectId" } } }),
});
export const users = StreamRoute.custom({
  parse: (id) => parseState("users", id),
  ref: ({ workspaceId }) => StreamRef.state(`issue-tracker/${workspaceId}/users`, { collections: { user: { schema: UserRow, key: "userId" } } }),
});
export const labels = StreamRoute.custom({
  parse: (id) => parseState("labels", id),
  ref: ({ workspaceId }) => StreamRef.state(`issue-tracker/${workspaceId}/labels`, { collections: { label: { schema: LabelRow, key: "labelId" } } }),
});
export const routes = { events, labelEvents, projects, users, labels } as const;
export const refs = (workspaceId: string) => Object.values(routes).map((route) => route.ref({ workspaceId }));
