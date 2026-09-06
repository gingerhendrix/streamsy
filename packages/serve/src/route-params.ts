/**
 * The compile-time check that a sink's route and its parameter codecs agree.
 *
 * A route names its parameters and the declaration supplies one codec per name.
 * Neither side may carry a name the other does not, so a typo is a type error at
 * the declaration rather than a 404 at run time.
 */
import type { SinkParamCodecs } from "./route.ts";

type RouteParameterNames<Route extends string> =
  Route extends `${string}:${infer Parameter}/${infer Rest}`
    ? Parameter | RouteParameterNames<`/${Rest}`>
    : Route extends `${string}:${infer Parameter}`
      ? Parameter
      : never;

export type ExactRouteParams<Route extends string, Params extends SinkParamCodecs> =
  Exclude<RouteParameterNames<Route>, keyof Params> extends never
    ? Exclude<keyof Params, RouteParameterNames<Route>> extends never
      ? unknown
      : never
    : never;
