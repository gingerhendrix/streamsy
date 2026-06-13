import type { BunRequest } from "bun";
import type { Issue } from "../../shared/types.ts";
import {
  getIssue,
  getProject,
  insertIssue,
  newIssue,
  nextIssue,
  updateIssueState,
} from "../state.ts";
import type { DemoStreams } from "../streams.ts";
import { badRequest, json, notFound, type MutationBody } from "../utils.ts";

export function issueRoutes(streams: DemoStreams) {
  return {
    "/api/issues": {
      async POST(request: BunRequest<"/api/issues">): Promise<Response> {
        const body = (await request.json()) as MutationBody<Issue>;
        const issue = newIssue(body);
        if (!getProject(issue.projectId)) {
          return badRequest("Unknown projectId");
        }
        const result = await insertIssue(streams, issue, body.txid);
        return json(
          { issue, awaitOffset: result.offset, txid: result.event.headers.txid },
          { status: 201 },
        );
      },
    },

    "/api/issues/:id": {
      async PATCH(request: BunRequest<"/api/issues/:id">): Promise<Response> {
        const issueId = decodeURIComponent(request.params.id);
        const previous = getIssue(issueId);
        if (!previous) return notFound("Issue not found");

        const body = (await request.json()) as MutationBody<Issue>;
        const issue = nextIssue(previous, body);
        const result = await updateIssueState(streams, issue, previous, body.txid);
        return json({ issue, awaitOffset: result.offset, txid: result.event.headers.txid });
      },
    },
  };
}
