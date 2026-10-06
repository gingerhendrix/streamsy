import { Effect, type Layer } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import { checkStreams, type Any, type Identifiers } from "../toolkit/streamsy.ts";
import type * as Group from "../toolkit/streamsy.ts";
import { app, type HttpOptions } from "./program.ts";
import { requestUrl } from "./route.ts";
import { streamPath } from "./stream-path-service.ts";
import { discardRequestBody } from "./request-body-discard.ts";
import { securityHeaders } from "./security-headers.ts";

type RouteLayer<R> = Layer.Layer<
  never,
  never,
  HttpRouter.HttpRouter | HttpRouter.Request.From<"Requires", Exclude<R, HttpRouter.Provided>>
>;

export interface Access<S extends Any = Any> {
  readonly stream: S;
  /** append permits POST; full also permits PUT and DELETE. */
  readonly access: "read" | "append" | "full";
}

export interface RoutesOptions extends Omit<HttpOptions, "pathPrefix"> {
  readonly prefix?: `/${string}`;
  readonly access: Access["access"] | ((stream: Any) => Access["access"]);
}

/** One prefix controls both route registration and protocol path stripping. */
export function routes<const Keys extends ReadonlyArray<Any>>(
  group: Group.Group<Keys>,
  options: RoutesOptions,
): RouteLayer<Identifiers<Keys>>;
export function routes(group: Group.Group, options: RoutesOptions) {
  const entries: ReadonlyArray<Access> = group.streams.map((stream) => ({
    stream,
    // oxlint-disable-next-line anti-slop/no-runtime-typeof -- The typed host policy is a literal or a callback, not external input.
    access: typeof options.access === "function" ? options.access(stream) : options.access,
  }));

  const settings = options;
  const prefix = settings.prefix?.replace(/\/$/, "") || "/";

  if (!prefix.startsWith("/") || /[?:#*]/.test(prefix) || prefix.includes("//")) {
    throw new RangeError("HTTP prefix must be an absolute literal path");
  }

  const path = streamPath(prefix);
  const program = app({ ...settings, pathPrefix: prefix });
  checkStreams(entries.map((entry) => entry.stream));

  const handler = Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const url = requestUrl(request);
    const id = url === undefined ? undefined : path.strip(url.pathname);
    const entry = entries.find(({ stream }) => id !== undefined && stream.match(id));

    if (entry === undefined) {
      yield* discardRequestBody(request);

      return HttpServerResponse.empty({ status: 404, headers: securityHeaders });
    }

    const allow =
      entry.access === "read"
        ? "GET, HEAD, OPTIONS"
        : entry.access === "append"
          ? "GET, HEAD, POST, OPTIONS"
          : "GET, HEAD, PUT, POST, DELETE, OPTIONS";

    if (entry.access !== "full" && !allow.split(", ").includes(request.method)) {
      yield* discardRequestBody(request);

      return HttpServerResponse.empty({
        status: 405,
        headers: { ...securityHeaders, allow },
      });
    }

    const service = yield* entry.stream;

    const response = yield* program.pipe(
      Effect.provideService(StreamsReader, service.wire),
      Effect.provideService(StreamsWriter, service.wire),
    );

    return request.method === "OPTIONS" && entry.access !== "full"
      ? HttpServerResponse.setHeader(response, "access-control-allow-methods", allow)
      : response;
  });

  return HttpRouter.add("*", prefix === "/" ? "/*" : `/${prefix.slice(1)}/*`, handler);
}
