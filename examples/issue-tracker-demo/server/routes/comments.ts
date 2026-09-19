import type { BunRequest } from "bun";
import { commentInput } from "../../shared/state-schema.ts";
import { isValidWorkspaceId } from "../config.ts";
import { commentUpsert, mutateWorkspace, newComment } from "../state.ts";
import type { DemoRuntime } from "../streams.ts";
import { badRequest, invalidBody, json, readMutation } from "../utils.ts";

export function commentRoutes(runtime: DemoRuntime) {
  return {
    "/api/w/:ws/comments": {
      async POST(request: BunRequest<"/api/w/:ws/comments">): Promise<Response> {
        const workspaceId = request.params.ws;
        if (!isValidWorkspaceId(workspaceId)) return badRequest("Invalid workspace id");

        const mutation = await readMutation(request);
        if (mutation instanceof Response) return mutation;
        const input = commentInput.safeParse(mutation.body);
        if (!input.success) return invalidBody("comment", input.error);

        return runtime.runPromise(
          mutateWorkspace(workspaceId, (state) => {
            const comment = newComment(input.data);
            if (!state.getIssue(comment.issueId)) {
              return { response: badRequest("Unknown issueId") };
            }
            const event = commentUpsert(comment, mutation.txid);
            return {
              event,
              respond: ({ offset }) =>
                json({ comment, awaitOffset: offset, txid: event.headers.txid }, { status: 201 }),
            };
          }),
        );
      },
    },
  };
}
