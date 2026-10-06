import { Effect } from "effect";
import { HttpApiBuilder } from "effect/http-api";
import { IssueTrackerApi } from "../../shared/api.ts";
import { IssueTracker } from "../issue-tracker.ts";
import { createIssue, updateIssue } from "../state.ts";
import { issueApiErrors } from "./errors.ts";

export const IssuesLive = HttpApiBuilder.group(IssueTrackerApi, "issues", (handlers) =>
  Effect.gen(function* () {
    const tracker = yield* IssueTracker;

    return handlers
      .handle(
        "create",
        Effect.fn("issues.create")(function* ({ params, payload }) {
          const stamp = yield* tracker.stamp("issue", payload.txid);
          const written = yield* tracker.mutate(params.ws, createIssue(payload, stamp));

          return { issue: written.value, awaitOffset: written.awaitOffset, txid: stamp.txid };
        }, issueApiErrors),
      )
      .handle(
        "update",
        Effect.fn("issues.update")(function* ({ params, payload }) {
          const stamp = yield* tracker.stamp("issue", payload.txid);
          const written = yield* tracker.mutate(params.ws, updateIssue(params.id, payload, stamp));

          return { issue: written.value, awaitOffset: written.awaitOffset, txid: stamp.txid };
        }, issueApiErrors),
      );
  }),
);
