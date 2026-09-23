import { HttpRouter } from "effect/unstable/http";
import { app, type HttpOptions } from "./program.ts";

export interface RoutesOptions extends Omit<HttpOptions, "pathPrefix"> {
  readonly prefix?: `/${string}`;
}

/** One prefix controls both route registration and protocol path stripping. */
export function routes(options: RoutesOptions = {}) {
  const prefix = options.prefix?.replace(/\/$/, "") || "/";
  if (!prefix.startsWith("/") || /[?:#*]/.test(prefix) || prefix.includes("//")) {
    throw new RangeError("HTTP prefix must be an absolute literal path");
  }
  return HttpRouter.add(
    "*",
    prefix === "/" ? "/*" : `/${prefix.slice(1)}/*`,
    app({
      ...options,
      pathPrefix: prefix,
    }),
  );
}
