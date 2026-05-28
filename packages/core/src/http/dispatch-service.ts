import type { BoundHttpRouteContext, HttpRouteContext } from "./types.ts";
import type { StreamProtocolFactory } from "../types/protocol.ts";
import { isNotSupported } from "../types/factory.ts";
import { notSupportedResponse } from "./not-supported.ts";
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
      protocol: StreamProtocolFactory;
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
      return this.deps.responses.badRequest(
        `Stream path required: ${this.deps.path.requiredPathPattern()}`,
      );
    }
    const ctx: HttpRouteContext = { request, url, streamId };
    try {
      switch (request.method) {
        case "PUT":
          return await this.deps.create.execute(ctx);
        case "POST":
          return await this.withBoundStream(ctx, (bound) => this.deps.append.execute(bound));
        case "GET":
          return await this.withBoundStream(ctx, (bound) => this.deps.read.execute(bound));
        case "HEAD":
          return await this.withBoundStream(ctx, (bound) => this.deps.metadata.execute(bound));
        case "DELETE":
          return await this.withBoundStream(ctx, (bound) => this.deps.delete.execute(bound));
        default:
          return this.deps.responses.methodNotAllowed();
      }
    } catch (error) {
      console.error("Error handling request:", error);
      return this.deps.responses.internalError();
    }
  }

  private async withBoundStream(
    ctx: HttpRouteContext,
    fn: (ctx: BoundHttpRouteContext) => Promise<Response>,
  ): Promise<Response> {
    const lookup = await this.deps.protocol.get(ctx.streamId);
    if (lookup.status === "not-found") return this.deps.responses.notFound();
    if (lookup.status === "gone") return this.deps.responses.gone();
    if (isNotSupported(lookup)) return notSupportedResponse(lookup, this.deps.responses);
    return fn({ ...ctx, stream: lookup.stream });
  }
}
