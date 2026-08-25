import { compileSinkRoute, type DecodedSinkParams, type SinkParamCodecs } from "./route.ts";
import type { StateSinkErrorTag, StateSinkPublicError } from "./errors.ts";

export interface StateSinkRowCodec<Row> {
  readonly decode: (value: unknown) => Row;
}

export interface StateSinkProtocol {
  readonly sessionVersion: 1;
  readonly durableStateVersion: 1;
  readonly transport: "durable-state";
  readonly resume: true;
  readonly fallback: "snapshot-then-live";
}

export interface StateSinkAuthorizationContract {
  readonly policy: string;
  readonly required: string;
}

export interface StateSinkCollection<Row, Key extends keyof Row & string> {
  readonly name: string;
  readonly type: string;
  readonly primaryKey: Key;
}

export interface StateSinkSpec<
  Row,
  Key extends keyof Row & string,
  Params extends SinkParamCodecs,
  From = unknown,
> {
  readonly name: string;
  readonly from: From;
  readonly row: StateSinkRowCodec<Row>;
  readonly key: Key;
  readonly route: string;
  readonly params: Params;
  readonly collection: StateSinkCollection<Row, Key>;
  readonly protocol: StateSinkProtocol;
  readonly auth: StateSinkAuthorizationContract;
  readonly errors?: readonly StateSinkErrorTag[];
}

export interface CheckedStateSink<
  Row,
  Key extends keyof Row & string,
  Params extends SinkParamCodecs,
  From = unknown,
> extends StateSinkSpec<Row, Key, Params, From> {
  readonly kind: "checked-state-sink";
  readonly fingerprint: string;
  readonly compiledRoute: ReturnType<typeof compileSinkRoute<Params>>;
}

type RouteParameterNames<Route extends string> =
  Route extends `${string}:${infer Parameter}/${infer Rest}`
    ? Parameter | RouteParameterNames<`/${Rest}`>
    : Route extends `${string}:${infer Parameter}`
      ? Parameter
      : never;

type ExactRouteParams<Route extends string, Params extends SinkParamCodecs> =
  Exclude<RouteParameterNames<Route>, keyof Params> extends never
    ? Exclude<keyof Params, RouteParameterNames<Route>> extends never
      ? unknown
      : never
    : never;

export function defineStateSink<
  Row,
  const Key extends keyof Row & string,
  const Route extends string,
  const Params extends SinkParamCodecs,
  From,
>(
  spec: StateSinkSpec<Row, Key, Params, From> & { readonly route: Route } & ExactRouteParams<
      Route,
      Params
    >,
): CheckedStateSink<Row, Key, Params, From> {
  if (spec.collection.primaryKey !== spec.key) {
    throw new Error("state-sink collection primary key must match the declared key");
  }
  const compiledRoute = compileSinkRoute(spec.route, spec.params);
  const fingerprint = hashContract({
    name: spec.name,
    route: spec.route,
    params: compiledRoute.parameterNames,
    key: spec.key,
    collection: spec.collection,
    protocol: spec.protocol,
    auth: spec.auth,
    errors: spec.errors ?? [],
  });
  return Object.freeze({ ...spec, kind: "checked-state-sink", fingerprint, compiledRoute });
}

function hashContract(value: object): string {
  const input = JSON.stringify(value);
  let hash = 0x811c9dc5;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

export type RowOf<Sink> =
  Sink extends CheckedStateSink<infer Row, infer _Key, infer _Params, infer _From> ? Row : never;
export type KeyOf<Sink> =
  Sink extends CheckedStateSink<infer Row, infer Key, infer _Params, infer _From>
    ? Row[Key]
    : never;
export type ParamsOf<Sink> =
  Sink extends CheckedStateSink<infer _Row, infer _Key, infer Params, infer _From>
    ? DecodedSinkParams<Params>
    : never;
export type ErrorOf<Sink> =
  Sink extends CheckedStateSink<infer _Row, infer _Key, infer _Params, infer _From>
    ? StateSinkPublicError
    : never;
export type StatusOf<Sink> =
  Sink extends CheckedStateSink<infer _Row, infer _Key, infer _Params, infer _From>
    ?
        | { readonly kind: "connecting" }
        | { readonly kind: "live"; readonly offset?: string }
        | { readonly kind: "resetting"; readonly reason: string }
        | { readonly kind: "failed"; readonly error: StateSinkPublicError | Error }
    : never;
