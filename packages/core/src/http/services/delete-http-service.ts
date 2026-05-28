import type { BoundHttpRouteContext } from "../types.ts";
import { HttpResponseFactory } from "../responses.ts";

export class DeleteHttpService {
  constructor(private deps: { responses: HttpResponseFactory }) {}

  async execute(ctx: BoundHttpRouteContext): Promise<Response> {
    const result = await ctx.stream.delete();
    if (result.status === "not-found") return this.deps.responses.notFound();
    if (result.status === "gone") return this.deps.responses.gone();
    return this.deps.responses.empty(204);
  }
}
