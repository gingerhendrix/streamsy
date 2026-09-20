import { StreamRef, StreamRoute } from "@streamsy/core";
import { Option, Schema } from "effect";
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

const projectPattern = /^issue-tracker\/([^/]+)\/projects$/;
const userPattern = /^issue-tracker\/([^/]+)\/users$/;
const labelPattern = /^issue-tracker\/([^/]+)\/labels$/;
const decodeIdentifier = Schema.decodeUnknownOption(Identifier);
const parseState = (pattern: RegExp, id: string) => {
  const match = id.match(pattern);
  if (match?.[1] === undefined) return Option.none();
  return Option.map(decodeIdentifier(match[1]), (workspaceId) => ({ workspaceId }));
};
export const projects = StreamRoute.custom({
  parse: (id) => parseState(projectPattern, id),
  ref: ({ workspaceId }) => projectStream(workspaceId),
});
export const users = StreamRoute.custom({
  parse: (id) => parseState(userPattern, id),
  ref: ({ workspaceId }) => userStream(workspaceId),
});
export const labels = StreamRoute.custom({
  parse: (id) => parseState(labelPattern, id),
  ref: ({ workspaceId }) => labelStream(workspaceId),
});
export const projectStream = (workspaceId: string) =>
  StreamRef.state(`issue-tracker/${workspaceId}/projects`, {
    collections: { project: { schema: ProjectRow, key: "projectId" } },
  });
export const userStream = (workspaceId: string) =>
  StreamRef.state(`issue-tracker/${workspaceId}/users`, {
    collections: { user: { schema: UserRow, key: "userId" } },
  });
export const labelStream = (workspaceId: string) =>
  StreamRef.state(`issue-tracker/${workspaceId}/labels`, {
    collections: { label: { schema: LabelRow, key: "labelId" } },
  });
export const routes = { events, labelEvents, projects, users, labels } as const;
export const refs = (workspaceId: string) =>
  Object.values(routes).map((route) => route.ref({ workspaceId }));
