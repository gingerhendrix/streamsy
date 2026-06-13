import type { BunRequest } from "bun";
import type { Issue } from "../../shared/types.ts";
import { isValidWorkspaceId } from "../config.ts";
import { issueUpdate, issueUpsert, mutateWorkspace, newIssue, nextIssue } from "../state.ts";
import type { DemoStreams } from "../streams.ts";
import { badRequest, json, notFound, type MutationBody } from "../utils.ts";

export function issueRoutes(streams: DemoStreams) {
  return {
    "/api/w/:ws/issues": {
      async POST(request: BunRequest<"/api/w/:ws/issues">): Promise<Response> {
        const workspaceId = request.params.ws;
        if (!isValidWorkspaceId(workspaceId)) return badRequest("Invalid workspace id");

        const body = (await request.json()) as MutationBody<Issue>;
        return mutateWorkspace(streams, workspaceId, (state) => {
          const issue = newIssue(body);
          if (!state.getProject(issue.projectId)) {
            return { response: badRequest("Unknown projectId") };
          }
          const event = issueUpsert(issue, body.txid);
          return {
            event,
            respond: ({ offset }) =>
              json({ issue, awaitOffset: offset, txid: event.headers.txid }, { status: 201 }),
          };
        });
      },
    },

    "/api/w/:ws/issues/:id": {
      async PATCH(request: BunRequest<"/api/w/:ws/issues/:id">): Promise<Response> {
        const workspaceId = request.params.ws;
        if (!isValidWorkspaceId(workspaceId)) return badRequest("Invalid workspace id");

        const issueId = decodeURIComponent(request.params.id);
        const body = (await request.json()) as MutationBody<Issue>;
        return mutateWorkspace(streams, workspaceId, (state) => {
          const previous = state.getIssue(issueId);
          if (!previous) return { response: notFound("Issue not found") };

          const issue = nextIssue(previous, body);
          const event = issueUpdate(issue, previous, body.txid);
          return {
            event,
            respond: ({ offset }) => json({ issue, awaitOffset: offset, txid: event.headers.txid }),
          };
        });
      },
    },
  };
}
