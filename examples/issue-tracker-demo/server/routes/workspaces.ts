import { Effect } from "effect";
import { HttpApiBuilder } from "effect/http-api";
import { IssueTrackerApi } from "@streamsy/issue-tracker-demo-contract/api";
import { IssueTracker } from "../issue-tracker.ts";
import { apiErrors } from "./errors.ts";

export const WorkspacesLive = HttpApiBuilder.group(IssueTrackerApi, "workspaces", (handlers) =>
  Effect.gen(function* () {
    const tracker = yield* IssueTracker;

    return handlers.handle("create", () =>
      Effect.map(tracker.create, (workspaceId) => ({ id: workspaceId })).pipe(apiErrors),
    );
  }),
);
