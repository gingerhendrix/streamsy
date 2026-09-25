import { Effect } from "effect";
import { HttpApiBuilder } from "effect/unstable/httpapi";
import { IssueTrackerApi } from "../../shared/api.ts";
import { IssueTracker } from "../issue-tracker.ts";
import { createIssue, updateIssue } from "../state.ts";
import { toApiError } from "./errors.ts";

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
        }, Effect.mapError(toApiError)),
      )
      .handle(
        "update",
        Effect.fn("issues.update")(function* ({ params, payload }) {
          const stamp = yield* tracker.stamp("issue", payload.txid);
          const written = yield* tracker.mutate(params.ws, updateIssue(params.id, payload, stamp));
          return { issue: written.value, awaitOffset: written.awaitOffset, txid: stamp.txid };
        }, Effect.mapError(toApiError)),
      );
  }),
);
