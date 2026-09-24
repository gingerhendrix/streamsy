import { Effect, type Layer } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import { checkFamilies, type AnyFamily, type Identifiers } from "../toolkit/family.ts";
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

export interface RoutesOptions extends Omit<HttpOptions, "pathPrefix"> {
  readonly prefix?: `/${string}`;
}
export interface FamilyAccess<F extends AnyFamily = AnyFamily> {
  readonly family: F;
  /** append permits POST; full also permits PUT and DELETE. */
  readonly access: "read" | "append" | "full";
}
export function routes<const Entries extends ReadonlyArray<FamilyAccess>>(
  entries: Entries,
  options?: RoutesOptions,
): RouteLayer<Identifiers<{ readonly [K in keyof Entries]: Entries[K]["family"] }>>;
export function routes(options?: RoutesOptions): RouteLayer<StreamsReader | StreamsWriter>;
/** One prefix controls both route registration and protocol path stripping. */
export function routes(
  input: RoutesOptions | ReadonlyArray<FamilyAccess> = {},
  options: RoutesOptions = {},
) {
  // SAFETY: the overload union contains only an options object or a readonly entry array.
  const entries = Array.isArray(input) ? (input as ReadonlyArray<FamilyAccess>) : undefined;
  // SAFETY: absence of entries identifies the options branch of the overload union.
  const settings = entries === undefined ? (input as RoutesOptions) : options;
  const prefix = settings.prefix?.replace(/\/$/, "") || "/";
  if (!prefix.startsWith("/") || /[?:#*]/.test(prefix) || prefix.includes("//")) {
    throw new RangeError("HTTP prefix must be an absolute literal path");
  }
  const program = app({ ...settings, pathPrefix: prefix });
  if (entries !== undefined) checkFamilies(entries.map((entry) => entry.family));
  const handler =
    entries === undefined
      ? program
      : Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const url = requestUrl(request);
          const id = url === undefined ? undefined : streamPath(prefix).strip(url.pathname);
          const entry = entries.find(({ family }) => id !== undefined && family.match(id));
          const denied =
            entry === undefined
              ? 404
              : ["GET", "HEAD", "OPTIONS"].includes(request.method) ||
                  entry.access === "full" ||
                  (entry.access === "append" && request.method === "POST")
                ? undefined
                : 405;
          if (denied !== undefined) {
            yield* discardRequestBody(request);
            return HttpServerResponse.empty({ status: denied, headers: securityHeaders });
          }
          if (entry === undefined) return HttpServerResponse.empty({ status: 404 });
          const service = yield* entry.family;
          return yield* program.pipe(
            Effect.provideService(StreamsReader, service.wire),
            Effect.provideService(StreamsWriter, service.wire),
          );
        });
  return HttpRouter.add("*", prefix === "/" ? "/*" : `/${prefix.slice(1)}/*`, handler);
}
