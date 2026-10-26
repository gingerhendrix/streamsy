import type { Storage, StorageFault } from "@streamsy/core";
import {
  acquireObject,
  providedApp,
  unavailable,
  type ObjectApp,
} from "./cloudflare/object-runtime.ts";
import type { AlarmSlot } from "./cloudflare/alarm.ts";
import type { WakeupSource } from "./cloudflare/wakeup.ts";
import { withLinks } from "./cloudflare/links.ts";
import type { LinkClient } from "./cloudflare/object-link.ts";
import { objectTransportError, type ObjectFetch } from "./cloudflare/object-streams.ts";
import type { DurableObject } from "alchemy/Cloudflare";
import { Effect, Option, type Layer, type Scope } from "effect";
import {
  HttpRouter,
  type HttpServerError,
  HttpServerRequest,
  HttpServerRespondable,
  HttpServerResponse,
} from "effect/http";
import { securityHeaders, streamPath as protocolPath } from "@streamsy/core/http";
import { Placement, type Placement as PlacementType } from "./cloudflare/placement.ts";
import { isReserved, resolvePlacement } from "./cloudflare/router.ts";

export { Wakeup, Wakeups, type WakeupProgress, type WakeupSource } from "./cloudflare/wakeup.ts";

export { alarmSlotLayer } from "./cloudflare/alarm.ts";

export { Placement };

export {
  ObjectStreams,
  type ObjectFetch,
  type ObjectStreamsOptions,
} from "./cloudflare/object-streams.ts";

export {
  ObjectLink,
  type LinkAccess,
  type LinkClient,
  type LinkRoutes,
  type ObjectLinkOptions,
} from "./cloudflare/object-link.ts";

export { rule } from "./cloudflare/placement.ts";

/** An Alchemy HttpEffect; authenticate before evaluating this router. */
export const router = (options: {
  readonly objects: Pick<DurableObject, "getByName">;
  readonly prefix?: `/${string}`;
  readonly placement?: PlacementType;
}): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  HttpServerError.HttpServerError,
  HttpServerRequest.HttpServerRequest
> => {
  const path = protocolPath(options.prefix);
  const placement = options.placement ?? Placement.byStream();

  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const pathname = new URL(request.url, "https://streamsy.internal").pathname;

    if (isReserved(pathname))
      return HttpServerResponse.empty({ status: 404, headers: securityHeaders });

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

/**
 * An `ObjectFetch` over an Alchemy Durable Object binding, for
 * `ObjectStreams.layer` and `ObjectLink.client`.
 */
export const objectFetch = (objects: Pick<DurableObject, "getByName">): ObjectFetch =>
  Effect.fn("Alchemy.objectFetch")(function* (name, request) {
    const response = yield* objects
      .getByName(name)
      .fetch(HttpServerRequest.fromWeb(request))
      .pipe(Effect.mapError((cause) => objectTransportError(request, cause)));

    return HttpServerResponse.toWeb(response);
  });

export type ObjectHandlers = {
  readonly fetch: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    never,
    HttpServerRequest.HttpServerRequest
  >;
  readonly alarm: () => Effect.Effect<void>;
};

export function objectHandlers<R = never, L = never>(options: {
  readonly app: ObjectApp<R | L>;
  readonly layer: Layer.Layer<Storage | AlarmSlot | R, StorageFault, L>;
  readonly wakeups?: ReadonlyArray<WakeupSource<Storage | R | L>>;
  /** Streams that live on other objects, through `link.client(objectFetch(binding))`. */
  readonly links?: ReadonlyArray<LinkClient<L>>;
}): Effect.Effect<ObjectHandlers, never, Scope.Scope> {
  return Effect.gen(function* () {
    const get = yield* acquireObject(
      withLinks(options.layer, options.links),
      (context, alarm) =>
        Effect.gen(function* () {
          const fetch = yield* HttpRouter.toHttpEffect(providedApp(options.app, context));

          return {
            fetch: fetch.pipe(Effect.scoped, Effect.catch(HttpServerRespondable.toResponse)),
            alarm: alarm.pipe(Effect.provide(context)),
          };
        }),
      options.wakeups,
    );

    return {
      fetch: Effect.flatMap(
        get.pipe(
          Effect.asSome,
          Effect.catch(() => Effect.succeedNone),
          Effect.catchDefect(() => Effect.succeedNone),
        ),
        (compiled) =>
          Option.isSome(compiled) ? compiled.value.fetch : Effect.succeed(unavailable()),
      ),
      alarm: () =>
        // Acquisition failed, so no reconcile ran. Platform retry is the only re-arm.
        Effect.flatMap(get, (compiled) => compiled.alarm).pipe(Effect.orDie),
    };
  });
}
