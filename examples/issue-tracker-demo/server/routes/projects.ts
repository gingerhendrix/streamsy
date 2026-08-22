import type { BunRequest } from "bun";
import { projectInput } from "../../shared/state-schema.ts";
import { isValidWorkspaceId } from "../config.ts";
import { mutateWorkspace, newProject, projectUpsert } from "../state.ts";
import type { DemoStreams } from "../streams.ts";
import { badRequest, invalidBody, json, readMutation } from "../utils.ts";

export function projectRoutes(streams: DemoStreams) {
  return {
    "/api/w/:ws/projects": {
      async POST(request: BunRequest<"/api/w/:ws/projects">): Promise<Response> {
        const workspaceId = request.params.ws;
        if (!isValidWorkspaceId(workspaceId)) return badRequest("Invalid workspace id");

        const mutation = await readMutation(request);
        if (mutation instanceof Response) return mutation;
        const input = projectInput.safeParse(mutation.body);
        if (!input.success) return invalidBody("project", input.error);

        return mutateWorkspace(streams, workspaceId, () => {
          const project = newProject(input.data);
          const event = projectUpsert(project, mutation.txid);
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
