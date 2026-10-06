import { HttpRouter } from "effect/http";
import { app } from "../http/program.ts";
import type { RoutesOptions } from "../http/routes.ts";

/** Raw protocol route for adapters and wire conformance tests. */
export const routes = (options: Omit<RoutesOptions, "access"> = {}) => {
  const prefix = options.prefix?.replace(/\/$/, "") || "/";
  if (!prefix.startsWith("/") || /[?:#*]/.test(prefix) || prefix.includes("//"))
    throw new RangeError("HTTP prefix must be an absolute literal path");
  return HttpRouter.add(
    "*",
    prefix === "/" ? "/*" : `/${prefix.slice(1)}/*`,
    app({ ...options, pathPrefix: prefix }),
  );
};
