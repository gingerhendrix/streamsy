import type { BunRequest } from "bun";
import type { Project } from "../../shared/types.ts";
import { isValidWorkspaceId } from "../config.ts";
import { mutateWorkspace, newProject, projectUpsert } from "../state.ts";
import type { DemoStreams } from "../streams.ts";
import { badRequest, json, type MutationBody } from "../utils.ts";

export function projectRoutes(streams: DemoStreams) {
  return {
    "/api/w/:ws/projects": {
      async POST(request: BunRequest<"/api/w/:ws/projects">): Promise<Response> {
        const workspaceId = request.params.ws;
        if (!isValidWorkspaceId(workspaceId)) return badRequest("Invalid workspace id");

        const body = (await request.json()) as MutationBody<Project>;
        return mutateWorkspace(streams, workspaceId, () => {
          const project = newProject(body);
          const event = projectUpsert(project, body.txid);
          return {
            event,
            respond: ({ offset }) =>
              json({ project, awaitOffset: offset, txid: event.headers.txid }, { status: 201 }),
          };
        });
      },
    },
  };
}
