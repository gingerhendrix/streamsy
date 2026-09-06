import type { BunRequest } from "bun";
import { generateWorkspaceId, workspaceStreamId } from "./config.ts";
import { seedStarterProject } from "./state.ts";
import type { DemoStreams } from "./streams.ts";
import { json } from "./utils.ts";

/**
 * Create a fresh shared workspace: server-generated random id, new durable
 * stream, one starter project. Random ids cannot collide in practice, so
 * there is no duplicate-id path; creation never contends with anything.
 */
export async function createWorkspace(streams: DemoStreams): Promise<string> {
  const workspaceId = generateWorkspaceId();
  await streams.ensureStream(workspaceStreamId(workspaceId));
  await seedStarterProject(streams, workspaceId);
  return workspaceId;
}

/** A workspace exists iff its stream exists — the protocol is the registry. */
export async function workspaceExists(streams: DemoStreams, workspaceId: string): Promise<boolean> {
  return (await streams.getJsonStream(workspaceStreamId(workspaceId))) !== undefined;
}

export function workspaceRoutes(streams: DemoStreams) {
  return {
    "/api/workspaces": {
      // Any request body is ignored: the server names everything.
      async POST(_request: BunRequest<"/api/workspaces">): Promise<Response> {
        const workspaceId = await createWorkspace(streams);
        return json({ id: workspaceId }, { status: 201 });
      },
    },
  };
}
