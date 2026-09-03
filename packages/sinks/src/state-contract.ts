import { compileSinkRoute, type DecodedSinkParams, type SinkParamCodecs } from "./route.ts";
import type { StateSinkErrorTag, StateSinkPublicError } from "./state-errors.ts";

export interface StateSinkRowCodec<Row> {
  /* oxlint-disable-next-line anti-slop/no-unknown-parameters -- This decoder is the sink row's external wire boundary. */
  readonly decode: (value: unknown) => Row;
}

export interface StateSinkProtocol {
  readonly sessionVersion: 1;
  readonly durableStateVersion: 1;
  readonly transport: "durable-state";
  readonly resume: true;
  readonly fallback: "snapshot-then-live";
}

/**
 * The public collection a sink publishes.
 *
 * The declaration names it and tags its wire type. Its primary key is not
 * declared here: it is the key of the relation the sink publishes, so a sink
 * and its relation cannot disagree about what identifies a row.
 */
export interface StateSinkCollection {
  readonly name: string;
  readonly type: string;
}

/** The part of a keyed relation a sink needs: the key its rows are declared by. */
export interface StateSinkRelation<Key extends string = string> {
  readonly key: Key;
}

/** The key a relation declares, narrowed to a field of the sink's row. */
export type SinkKeyOf<Row, From> = (From extends StateSinkRelation<infer Key> ? Key : never) &
  keyof Row &
  string;

export interface StateSinkSpec<
  Row,
  Params extends SinkParamCodecs,
  From extends StateSinkRelation = StateSinkRelation,
> {
  readonly name: string;
  readonly from: From;
  readonly row: StateSinkRowCodec<Row>;
  readonly route: string;
  readonly params: Params;
  readonly collection: StateSinkCollection;
  readonly protocol: StateSinkProtocol;
  readonly errors?: readonly StateSinkErrorTag[];
}

export interface CheckedStateSink<
  Row,
  Key extends keyof Row & string,
  Params extends SinkParamCodecs,
  From extends StateSinkRelation = StateSinkRelation,
> extends StateSinkSpec<Row, Params, From> {
  readonly kind: "checked-state-sink";
  /** The relation's declared key, carried so consumers read one key, not two. */
  readonly key: Key;
  readonly collection: StateSinkCollection & { readonly primaryKey: Key };
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
  const Route extends string,
  const Params extends SinkParamCodecs,
  From extends StateSinkRelation<keyof Row & string>,
>(
  spec: StateSinkSpec<Row, Params, From> & { readonly route: Route } & ExactRouteParams<
      Route,
      Params
    >,
): CheckedStateSink<Row, SinkKeyOf<Row, From>, Params, From> {
  // SAFETY: `From` is constrained to declare a key of `Row`, so the relation's declared key is that field name.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion, anti-slop/no-known-value-widening
  const key = spec.from.key as SinkKeyOf<Row, From>;
  const collection = { ...spec.collection, primaryKey: key };
  const compiledRoute = compileSinkRoute(spec.route, spec.params);
  const fingerprint = hashContract({
    name: spec.name,
    route: spec.route,
    params: compiledRoute.parameterNames,
    key,
    collection,
    protocol: spec.protocol,
    errors: spec.errors ?? [],
  });
  return Object.freeze({
    ...spec,
    kind: "checked-state-sink",
    key,
    collection: Object.freeze(collection),
    fingerprint,
    compiledRoute,
  });
}

interface ContractFingerprintInput {
  readonly name: string;
  readonly route: string;
  readonly params: readonly string[];
  readonly key: string;
  readonly collection: object;
  readonly protocol: StateSinkProtocol;
  readonly errors: readonly StateSinkErrorTag[];
}

function hashContract(value: ContractFingerprintInput): string {
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
