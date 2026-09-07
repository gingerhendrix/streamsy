import type { DurableObjectNamespace, ExportedHandler } from "@cloudflare/workers-types";
import { StreamPathService } from "@streamsy/core/http";
import { Placement, type Placement as PlacementType } from "./placement.ts";

export interface RouterOptions<Env> {
  readonly namespace: (env: Env) => DurableObjectNamespace;
  readonly placement?: PlacementType;
  readonly pathPrefix?: string;
}

const securityHeaders = {
  "x-content-type-options": "nosniff",
  "cross-origin-resource-policy": "cross-origin",
};

const badRequest = (message: string): Response =>
  new Response(message, { status: 400, headers: securityHeaders });

const internalError = (): Response =>
  new Response("Internal server error", { status: 500, headers: securityHeaders });

const invalidPlacement = (): Response => badRequest("Invalid placement key");

const resolvePlacement = (
  placement: PlacementType,
  streamPath: string,
):
  | { readonly ok: true; readonly name: string }
  | { readonly ok: false; readonly response: Response } => {
  try {
    const candidate: unknown = placement.name(streamPath);
    const name = String(candidate);
    return candidate === name && name.length > 0
      ? { ok: true, name }
      : { ok: false, response: invalidPlacement() };
  } catch {
    return { ok: false, response: internalError() };
  }
};

export const router = <Env>(options: RouterOptions<Env>): ExportedHandler<Env> => {
  const placement = options.placement ?? Placement.byStream();
  const path = new StreamPathService(options.pathPrefix ?? "/");

  return {
    fetch(request, env) {
      const url = new URL(request.url);
      const streamPath = path.strip(url.pathname);
      if (!streamPath || streamPath === url.pathname)
        return badRequest(`Stream path required: ${path.requiredPathPattern()}`);

      const child = resolvePlacement(placement, streamPath);
      if (!child.ok) return child.response;

      const namespace = options.namespace(env);
      return namespace.get(namespace.idFromName(child.name)).fetch(request);
    },
  };
};
