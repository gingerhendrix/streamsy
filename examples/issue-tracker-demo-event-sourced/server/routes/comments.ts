import { Effect } from "effect";
import { HttpApiBuilder } from "effect/http-api";
import { IssueTrackerApi } from "@streamsy/issue-tracker-demo-contract/api";
import { IssueTracker } from "../issue-tracker.ts";
import { createComment } from "../decide.ts";
import { apiErrors } from "./errors.ts";

export const CommentsLive = HttpApiBuilder.group(IssueTrackerApi, "comments", (handlers) =>
  Effect.gen(function* () {
    const tracker = yield* IssueTracker;
    return handlers.handle(
      "create",
      Effect.fn("comments.create")(function* ({ params, payload }) {
        const stamp = yield* tracker.stamp("comment", payload.txid);
        const written = yield* tracker.mutate(params.ws, createComment(payload, stamp));
        return { comment: written.value, awaitOffset: written.awaitOffset, txid: stamp.txid };
      }, apiErrors),
    );
  }),
);
