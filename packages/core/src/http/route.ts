import { Option } from "effect";
import { HttpServerRequest } from "effect/http";

/** Preserve the Web request origin; Effect 4.0.1 toURL otherwise reconstructs it from Host. */
export function requestUrl(request: HttpServerRequest.HttpServerRequest): URL | undefined {
  return URL.parse(request.originalUrl) ?? Option.getOrUndefined(HttpServerRequest.toURL(request));
}
