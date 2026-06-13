import type { BunRequest } from "bun";
import type { Comment } from "../../shared/types.ts";
import { getIssue, insertComment, newComment } from "../state.ts";
import type { DemoStreams } from "../streams.ts";
import { badRequest, json, type MutationBody } from "../utils.ts";

export function commentRoutes(streams: DemoStreams) {
  return {
    "/api/comments": {
      async POST(request: BunRequest<"/api/comments">): Promise<Response> {
        const body = (await request.json()) as MutationBody<Comment>;
        const comment = newComment(body);
        if (!getIssue(comment.issueId)) {
          return badRequest("Unknown issueId");
        }
        const result = await insertComment(streams, comment, body.txid);
        return json(
          { comment, awaitOffset: result.offset, txid: result.event.headers.txid },
          { status: 201 },
        );
      },
    },
  };
}
