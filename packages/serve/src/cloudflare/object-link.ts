import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import type { Layer } from "effect";
import type { HttpRouter } from "effect/http";
import { Http, type Streamsy } from "@streamsy/core";
import { ObjectStreams, type ObjectFetch } from "./object-streams.ts";
import type { Placement } from "./placement.ts";

/** What callers may do on a link mount. It applies to wake streams too. */
export type LinkAccess = "read" | "append" | "full";

export interface ObjectLinkOptions<Keys extends ReadonlyArray<Streamsy.Any>> {
  /** The streams that the owner serves to other objects. */
  readonly group: Streamsy.Group<Keys>;
  /** The internal mount. It must start with "/__", so that no package router forwards it. */
  readonly prefix: `/__${string}`;
  /** Names the owner object of each stream id. */
  readonly placement: Placement;
  readonly access: LinkAccess;
}

/**
 * The caller side of a link. Return it from the caller object's `links`. The
 * host refuses an object whose own layer also provides one of `streams`.
 */
export interface LinkClient<I> {
  readonly streams: ReadonlyArray<Streamsy.Any>;
  readonly layer: Layer.Layer<I>;
}

/** The owner mount, typed as `Http.routes` types it. */
export type LinkRoutes<I> = Layer.Layer<
  never,
  never,
  HttpRouter.HttpRouter | HttpRouter.Request.From<"Requires", Exclude<I, HttpRouter.Provided>>
>;

export interface ObjectLink<Keys extends ReadonlyArray<Streamsy.Any>> {
  readonly group: Streamsy.Group<Keys>;
  readonly prefix: `/__${string}`;
  /** The owner side. Add it to the owner object's `app`. */
  readonly routes: LinkRoutes<Streamsy.Services<Streamsy.Group<Keys>>>;
  /**
   * The caller side. `objects` is the owner namespace, or an `ObjectFetch` such
   * as Alchemy's `objectFetch(binding)`. Return the result from the caller's `links`.
   */
  readonly client: (
    objects: DurableObjectNamespace | ObjectFetch,
  ) => LinkClient<Streamsy.Services<Streamsy.Group<Keys>>>;
}

/**
 * One value for both sides of an internal mount between objects: the owner
 * mount and the caller client read the same group, prefix and access.
 */
const make = <const Keys extends ReadonlyArray<Streamsy.Any>>(
  options: ObjectLinkOptions<Keys>,
): ObjectLink<Keys> => {
  if (!/^\/__[^/]/.test(options.prefix))
    throw new RangeError(`An object link prefix must start with "/__", got "${options.prefix}"`);

  return {
    group: options.group,
    prefix: options.prefix,
    routes: Http.routes(options.group, {
      prefix: options.prefix,
      access: options.access,
    }),
    client: (objects) => ({
      streams: options.group.streams,
      layer: ObjectStreams.layer(options.group, {
        objects,
        placement: options.placement,
        prefix: options.prefix,
      }),
    }),
  };
};

export const ObjectLink = { make } as const;
