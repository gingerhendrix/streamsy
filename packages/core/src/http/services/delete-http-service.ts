import type { StreamProtocolInterface } from "../../types/protocol.ts";
import { HttpResponseFactory } from "../responses.ts";

export class DeleteHttpService {
  constructor(
    private deps: { protocol: StreamProtocolInterface; responses: HttpResponseFactory },
  ) {}

  async execute(streamId: string): Promise<Response> {
    const result = await this.deps.protocol.delete(streamId);
    if (result.status === "not-found") return this.deps.responses.notFound();
    if (result.status === "gone") return this.deps.responses.gone();
    return this.deps.responses.empty(204);
  }
}
