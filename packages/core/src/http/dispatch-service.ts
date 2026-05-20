import type { HttpRouteContext } from "./types.ts";
import { HttpResponseFactory } from "./responses.ts";
import { StreamPathService } from "./stream-path-service.ts";
import { AppendHttpService } from "./services/append-http-service.ts";
import { CreateHttpService } from "./services/create-http-service.ts";
import { DeleteHttpService } from "./services/delete-http-service.ts";
import { MetadataHttpService } from "./services/metadata-http-service.ts";
import { ReadHttpService } from "./services/read-http-service.ts";

export class HttpDispatchService {
  constructor(
    private deps: {
      path: StreamPathService;
      responses: HttpResponseFactory;
      create: CreateHttpService;
      append: AppendHttpService;
      read: ReadHttpService;
      metadata: MetadataHttpService;
      delete: DeleteHttpService;
    },
  ) {}

  async fetch(request: Request): Promise<Response> {
    return this.deps.responses.secure(await this.execute(request));
  }

  private async execute(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const streamId = this.deps.path.strip(url.pathname);
    if (!streamId || streamId === url.pathname) {
      return this.deps.responses.badRequest(`Stream path required: ${this.deps.path.requiredPathPattern()}`);
    }
    const ctx: HttpRouteContext = { request, url, streamId };
    try {
      switch (request.method) {
        case "PUT": return await this.deps.create.execute(ctx);
        case "POST": return await this.deps.append.execute(ctx);
        case "GET": return await this.deps.read.execute(ctx);
        case "HEAD": return await this.deps.metadata.execute(streamId);
        case "DELETE": return await this.deps.delete.execute(streamId);
        default: return this.deps.responses.methodNotAllowed();
      }
    } catch (error) {
      console.error("Error handling request:", error);
      return this.deps.responses.internalError();
    }
  }
}
