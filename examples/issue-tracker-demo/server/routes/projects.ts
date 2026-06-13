import type { BunRequest } from "bun";
import type { Project } from "../../shared/types.ts";
import { insertProject, newProject } from "../state.ts";
import type { DemoStreams } from "../streams.ts";
import { json, type MutationBody } from "../utils.ts";

export function projectRoutes(streams: DemoStreams) {
  return {
    "/api/projects": {
      async POST(request: BunRequest<"/api/projects">): Promise<Response> {
        const body = (await request.json()) as MutationBody<Project>;
        const project = newProject(body);
        const result = await insertProject(streams, project, body.txid);
        return json(
          { project, awaitOffset: result.offset, txid: result.event.headers.txid },
          { status: 201 },
        );
      },
    },
  };
}
