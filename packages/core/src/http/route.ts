import { Option } from "effect";
import { HttpServerRequest } from "effect/unstable/http";

/** Preserve the Web request origin; rc.112 toURL otherwise reconstructs it from Host. */
export function requestUrl(request: HttpServerRequest.HttpServerRequest): URL | undefined {
  return URL.parse(request.originalUrl) ?? Option.getOrUndefined(HttpServerRequest.toURL(request));
}
