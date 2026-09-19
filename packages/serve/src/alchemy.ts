import type { DurableObject } from "alchemy/Cloudflare";
import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import type { HttpServerError } from "effect/unstable/http/HttpServerError";
import { makeStreamPath } from "@streamsy/core/http";
import { Placement, type Placement as PlacementType } from "./cloudflare/placement.ts";
import { resolvePlacement } from "./cloudflare/router.ts";

export { fetch, alarm } from "./cloudflare/host-program.ts";
export { alarmLayer } from "./cloudflare/alarm.ts";
export { Placement };
export { ObjectOptions } from "./cloudflare/object-options.ts";

/** An Alchemy HttpEffect; authenticate before evaluating this router. */
export const router = (options: {
  readonly objects: Pick<DurableObject, "getByName">;
  readonly pathPrefix?: string;
  readonly placement?: PlacementType;
}): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  HttpServerError,
  HttpServerRequest.HttpServerRequest
> => {
  const path = makeStreamPath(options.pathPrefix);
  const placement = options.placement ?? Placement.byStream();
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const pathname = new URL(request.url, "https://streamsy.internal").pathname;
    const streamPath = path.strip(pathname);
    if (!streamPath || streamPath === pathname) {
      return HttpServerResponse.text(`Stream path required: ${path.requiredPathPattern()}`, {
        status: 400,
        headers: {
          "x-content-type-options": "nosniff",
          "cross-origin-resource-policy": "cross-origin",
        },
      });
    }
    const child = resolvePlacement(placement, streamPath);
    if (!child.ok) {
      const headers: Record<string, string> = {};
      child.response.headers.forEach((value, key) => {
        headers[key] = value;
      });
      return HttpServerResponse.raw(child.response, {
        status: child.response.status,
        headers,
      });
    }
    return yield* options.objects.getByName(child.name).fetch(request);
  });
};
