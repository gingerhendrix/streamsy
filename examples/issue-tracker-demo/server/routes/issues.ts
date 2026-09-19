import type { BunRequest } from "bun";
import { issueInput } from "../../shared/state-schema.ts";
import { isValidWorkspaceId } from "../config.ts";
import { issueUpsert, mutateWorkspace, newIssue, nextIssue } from "../state.ts";
import type { DemoRuntime } from "../streams.ts";
import { badRequest, invalidBody, json, notFound, readMutation } from "../utils.ts";

export function issueRoutes(runtime: DemoRuntime) {
  return {
    "/api/w/:ws/issues": {
      async POST(request: BunRequest<"/api/w/:ws/issues">): Promise<Response> {
        const workspaceId = request.params.ws;
        if (!isValidWorkspaceId(workspaceId)) return badRequest("Invalid workspace id");

        const mutation = await readMutation(request);
        if (mutation instanceof Response) return mutation;
        const input = issueInput.safeParse(mutation.body);
        if (!input.success) return invalidBody("issue", input.error);

        return runtime.runPromise(
          mutateWorkspace(workspaceId, (state) => {
            const issue = newIssue(input.data);
            if (!state.getProject(issue.projectId)) {
              return { response: badRequest("Unknown projectId") };
            }
            const event = issueUpsert(issue, mutation.txid);
            return {
              event,
              respond: ({ offset }) =>
                json({ issue, awaitOffset: offset, txid: event.headers.txid }, { status: 201 }),
            };
          }),
        );
      },
    },

    "/api/w/:ws/issues/:id": {
      async PATCH(request: BunRequest<"/api/w/:ws/issues/:id">): Promise<Response> {
        const workspaceId = request.params.ws;
        if (!isValidWorkspaceId(workspaceId)) return badRequest("Invalid workspace id");

        const issueId = decodeURIComponent(request.params.id);
        const mutation = await readMutation(request);
        if (mutation instanceof Response) return mutation;
        const input = issueInput.safeParse(mutation.body);
        if (!input.success) return invalidBody("issue", input.error);

        return runtime.runPromise(
          mutateWorkspace(workspaceId, (state) => {
            const previous = state.getIssue(issueId);
            if (!previous) return { response: notFound("Issue not found") };

            const issue = nextIssue(previous, input.data);
            const event = issueUpsert(issue, mutation.txid);
            return {
              event,
              respond: ({ offset }) =>
                json({ issue, awaitOffset: offset, txid: event.headers.txid }),
            };
          }),
        );
      },
    },
  };
}
