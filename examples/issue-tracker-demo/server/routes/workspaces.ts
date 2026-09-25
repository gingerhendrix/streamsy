import { Effect } from "effect";
import { HttpApiBuilder } from "effect/unstable/httpapi";
import { IssueTrackerApi } from "../../shared/api.ts";
import { IssueTracker } from "../issue-tracker.ts";

export const WorkspacesLive = HttpApiBuilder.group(IssueTrackerApi, "workspaces", (handlers) =>
  Effect.gen(function* () {
    const tracker = yield* IssueTracker;
    return handlers.handle("create", () =>
      Effect.map(tracker.create, (workspaceId) => ({ id: workspaceId })),
    );
  }),
);
