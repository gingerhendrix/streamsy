import type { BunRequest } from "bun";
import type { Comment } from "../../shared/types.ts";
import { isValidWorkspaceId } from "../config.ts";
import { commentUpsert, mutateWorkspace, newComment } from "../state.ts";
import type { DemoStreams } from "../streams.ts";
import { badRequest, json, type MutationBody } from "../utils.ts";

export function commentRoutes(streams: DemoStreams) {
  return {
    "/api/w/:ws/comments": {
      async POST(request: BunRequest<"/api/w/:ws/comments">): Promise<Response> {
        const workspaceId = request.params.ws;
        if (!isValidWorkspaceId(workspaceId)) return badRequest("Invalid workspace id");

        const body = (await request.json()) as MutationBody<Comment>;
        return mutateWorkspace(streams, workspaceId, (state) => {
          const comment = newComment(body);
          if (!state.getIssue(comment.issueId)) {
            return { response: badRequest("Unknown issueId") };
          }
          const event = commentUpsert(comment, body.txid);
          return {
            event,
            respond: ({ offset }) =>
              json({ comment, awaitOffset: offset, txid: event.headers.txid }, { status: 201 }),
          };
        });
      },
    },
  };
}
