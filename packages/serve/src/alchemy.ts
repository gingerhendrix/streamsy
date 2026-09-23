import type { StorageFault } from "@streamsy/core";
import {
  acquireObject,
  providedApp,
  unavailable,
  type ObjectApp,
  type ObjectServices,
} from "./cloudflare/object-runtime.ts";
import { alarm } from "./cloudflare/host-program.ts";
import type { DurableObject } from "alchemy/Cloudflare";
import { Effect, type Layer, type Scope } from "effect";
import {
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
  HttpServerRespondable,
} from "effect/unstable/http";
import type { HttpServerError } from "effect/unstable/http/HttpServerError";
import { securityHeaders, streamPath as protocolPath } from "@streamsy/core/http";
import { Placement, type Placement as PlacementType } from "./cloudflare/placement.ts";
import { resolvePlacement } from "./cloudflare/router.ts";

export { alarm } from "./cloudflare/host-program.ts";
export { Alarm, alarmLayer } from "./cloudflare/alarm.ts";
export { Placement };
export {
  rule,
  type FamilyRoute,
  type OwnerRule,
  type ErasedOwnerRule,
} from "./cloudflare/placement.ts";

/** An Alchemy HttpEffect; authenticate before evaluating this router. */
export const router = (options: {
  readonly objects: Pick<DurableObject, "getByName">;
  readonly prefix?: `/${string}`;
  readonly placement?: PlacementType;
}): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  HttpServerError,
  HttpServerRequest.HttpServerRequest
> => {
  const path = protocolPath(options.prefix);
  const placement = options.placement ?? Placement.byStream();
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const pathname = new URL(request.url, "https://streamsy.internal").pathname;
    const streamPath = path.strip(pathname);
    if (!streamPath || streamPath === pathname) {
      return HttpServerResponse.text(`Stream path required: ${path.requiredPathPattern()}`, {
        status: 400,
        headers: securityHeaders,
      });
    }
    const child = resolvePlacement(placement, streamPath);
    if (!child.ok) {
      return HttpServerResponse.fromWeb(child.response);
    }
    return yield* options.objects.getByName(child.name).fetch(request);
  });
};

export type ObjectHandlers = {
  readonly fetch: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    never,
    HttpServerRequest.HttpServerRequest
  >;
  readonly alarm: () => Effect.Effect<void>;
};
export function objectHandlers<R = never>(options: {
  readonly app: ObjectApp<R>;
  readonly layer: Layer.Layer<ObjectServices<R>, StorageFault>;
}): Effect.Effect<ObjectHandlers, never, Scope.Scope> {
  return Effect.gen(function* () {
    const get = yield* acquireObject(options.layer, (context) =>
      Effect.gen(function* () {
        const fetch = yield* HttpRouter.toHttpEffect(providedApp(options.app, context));
        return {
          fetch: fetch.pipe(
            Effect.scoped,
            Effect.catchTag("HttpServerError", HttpServerRespondable.toResponse),
          ),
          alarm: alarm.pipe(Effect.provide(context)),
        };
      }),
    );
    return {
      fetch: Effect.flatMap(get, (compiled) => compiled.fetch).pipe(
        Effect.catchTag("StorageFault", () => Effect.succeed(unavailable())),
      ),
      alarm: () => Effect.flatMap(get, (compiled) => compiled.alarm).pipe(Effect.orDie),
    };
  });
}
