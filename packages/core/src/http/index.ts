export { makeEdge } from "./edge.ts";
export { app } from "./program.ts";
/**
 * @deprecated Use `Http.app`. The alias keeps existing host callers valid for
 * one release and will be removed in a later round.
 */
export { app as program } from "./program.ts";
export type { HttpOptions } from "./program.ts";
export { StreamPathService } from "./stream-path-service.ts";
