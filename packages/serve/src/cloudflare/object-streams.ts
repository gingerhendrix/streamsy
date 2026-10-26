import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { Array, Effect, Layer, Predicate } from "effect";
import { HttpClient, HttpClientError, HttpClientRequest, HttpClientResponse } from "effect/http";
import type { Streamsy } from "@streamsy/core";
import { streamPath as protocolPath } from "@streamsy/core/http";
import { RemoteStreams } from "@streamsy/core/remote";
import type { Placement } from "./placement.ts";
import { resolvePlacement } from "./router.ts";

/** Sends one request to the object with this name. */
export type ObjectFetch = (
  name: string,
  request: Request,
) => Effect.Effect<Response, HttpClientError.HttpClientError>;

/** Translate a host transport error at the Web request boundary. */
export const objectTransportError = (request: Request, cause: unknown) =>
  new HttpClientError.HttpClientError({
    reason: new HttpClientError.TransportError({
      request: HttpClientRequest.fromWeb(request),
      cause,
    }),
  });

export interface ObjectStreamsOptions {
  /** The namespace of the owner objects, or a function that sends a request to one object by name. */
  readonly objects: DurableObjectNamespace | ObjectFetch;
  /** Names the owner object of each stream id. Use the same value that the app gives to `router`. */
  readonly placement: Placement;
  /** The mount prefix on the owner object, for example "/streams" or "/__wake". */
  readonly prefix: `/${string}`;
  /** Extra headers on each request. */
  readonly headers?: Readonly<Record<string, string>>;
}

/** The host is never resolved. It only makes a valid base URL for `RemoteStreams`. */
const base = "https://streamsy.internal";

/** A namespace becomes "send to the object with this name". */
const toObjectFetch = (objects: DurableObjectNamespace | ObjectFetch): ObjectFetch =>
  Predicate.isFunction(objects)
    ? objects
    : (name, request) =>
        Effect.tryPromise({
          try: () => Promise.resolve(objects.get(objects.idFromName(name)).fetch(request)),
          catch: (cause) => objectTransportError(request, cause),
        });

/** An HTTP client that calls `fetch` in place of the network. */
const stubClient = (send: ReturnType<typeof objectRouter>) =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request, _url, signal) =>
      HttpClientRequest.toWeb(request, { signal }).pipe(
        Effect.mapError(
          (cause) =>
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.InvalidUrlError({ request, cause }),
            }),
        ),
        Effect.flatMap(send),
        Effect.map((response) => HttpClientResponse.fromWeb(request, response)),
      ),
    ),
  );

/**
 * The `fetch` that `ObjectStreams` gives its HTTP client. A path outside the
 * prefix, or a stream outside the group, answers 404 here and calls no
 * object. A bad placement answers as the public router does.
 */
export const objectRouter = (
  group: Streamsy.Group,
  options: Pick<ObjectStreamsOptions, "objects" | "placement" | "prefix">,
) => {
  const path = protocolPath(options.prefix);
  const send = toObjectFetch(options.objects);

  return Effect.fn("ObjectStreams.route")(function* (request: Request) {
    const pathname = new URL(request.url).pathname;
    const id = path.strip(pathname);

    if (!id || id === pathname || !Array.some(group.streams, (stream) => stream.match(id)))
      return new Response(null, { status: 404 });

    const owner = resolvePlacement(options.placement, id);

    return owner.ok ? yield* send(owner.name, request) : owner.response;
  });
};

/**
 * A `RemoteStreams` layer whose requests go to the object that owns each
 * stream, and never to the network. The owner side is always a Streamsy
 * `Http.routes` mount, so the client declares producer and expected-offset support.
 */
function layer<const Keys extends ReadonlyArray<Streamsy.Any>>(
  group: Streamsy.Group<Keys>,
  options: ObjectStreamsOptions,
): Layer.Layer<Streamsy.Services<Streamsy.Group<Keys>>>;
function layer(group: Streamsy.Group, options: ObjectStreamsOptions) {
  return RemoteStreams.layer(group, {
    baseUrl: `${base}${options.prefix}`,
    headers: options.headers ?? {},
    capabilities: { producer: true, expectedOffset: true },
  }).pipe(Layer.provide(stubClient(objectRouter(group, options))), Layer.orDie);
}

export const ObjectStreams = { layer } as const;
