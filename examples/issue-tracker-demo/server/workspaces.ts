import { Streams } from "@streamsy/core";
import type { BunRequest } from "bun";
import { Effect } from "effect";
import { generateWorkspaceId } from "./config.ts";
import { seedStarterProject } from "./state.ts";
import { workspaceEvents, type DemoRuntime } from "./streams.ts";
import { json } from "./utils.ts";

/** Create a fresh shared workspace and seed its first project. */
export const createWorkspace = Effect.fn("Workspace.create")(function* () {
  const workspaceId = generateWorkspaceId();
  yield* Streams.create(workspaceEvents(workspaceId));
  yield* seedStarterProject(workspaceId);
  return workspaceId;
});

export function workspaceRoutes(runtime: DemoRuntime) {
  return {
    "/api/workspaces": {
      async POST(_request: BunRequest<"/api/workspaces">): Promise<Response> {
        const workspaceId = await runtime.runPromise(createWorkspace());
        return json({ id: workspaceId }, { status: 201 });
      },
    },
  };
}
