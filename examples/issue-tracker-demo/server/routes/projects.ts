import { Effect } from "effect";
import { HttpApiBuilder } from "effect/http-api";
import { IssueTrackerApi } from "../../shared/api.ts";
import { IssueTracker } from "../issue-tracker.ts";
import { createProject } from "../state.ts";
import { apiErrors } from "./errors.ts";

export const ProjectsLive = HttpApiBuilder.group(IssueTrackerApi, "projects", (handlers) =>
  Effect.gen(function* () {
    const tracker = yield* IssueTracker;

    return handlers.handle(
      "create",
      Effect.fn("projects.create")(function* ({ params, payload }) {
        const stamp = yield* tracker.stamp("proj", payload.txid);
        const written = yield* tracker.mutate(params.ws, createProject(payload, stamp));

        return { project: written.value, awaitOffset: written.awaitOffset, txid: stamp.txid };
      }, apiErrors),
    );
  }),
);
