/**
 * The checked stream-sink contract.
 *
 * A state sink publishes what a relation *is*; a stream sink publishes what
 * happened to it. Its input is therefore a change stream over a keyed relation —
 * `changes(issues)` — and the only ordering it promises is the one the engine
 * actually observes: Durable Stream arrival order. Nothing here re-sorts by a
 * domain field, so a producer that needs a domain order must append in that
 * order.
 *
 * The declaration is inert data. A host reads its route, its feed metadata and
 * its fingerprint; the runtime that serves it lives in `./server/stream.ts`.
 */
import { compileSinkRoute, type DecodedSinkParams, type SinkParamCodecs } from "./route.ts";
import type { StreamSinkErrorTag } from "./stream-errors.ts";
import { contractFingerprint } from "./fingerprint.ts";
import type { ExactRouteParams } from "./route-params.ts";

export interface StreamSinkEventCodec<Event> {
  /* oxlint-disable-next-line anti-slop/no-unknown-parameters -- This decoder is the feed event's external wire boundary. */
  readonly decode: (value: unknown) => Event;
}

/**
 * The part of a change stream a sink needs.
 *
 * `kind` is required so only a declared change stream can feed a stream sink:
 * a relation publishes its rows through a state sink, and its changes here.
 */
export interface StreamSinkChangeStream<Key extends string = string> {
  readonly kind: "change-stream";
  readonly name: string;
  readonly key: Key;
  /** Changes are observed in arrival order and never re-sorted. */
  readonly order: "arrival";
}

/** The key a change stream declares, narrowed to a field of the sink's event. */
export type StreamKeyOf<Event, From> = (From extends StreamSinkChangeStream<infer Key>
  ? Key
  : never) &
  keyof Event &
  string;

/**
 * The public feed a stream sink publishes.
 *
 * `name` is the feed a consumer subscribes to and `type` is the wire tag on
 * every event in it, exactly as a state sink's collection names and tags its
 * rows.
 */
export interface StreamSinkFeed {
  readonly name: string;
  readonly type: string;
}

export interface StreamSinkProtocol {
  readonly sessionVersion: 1;
  readonly transport: "durable-stream";
  readonly resume: true;
  /** Arrival order is the contract; there is no other ordering to negotiate. */
  readonly order: "arrival";
  readonly fallback: "replay-from-start";
}

export interface StreamSinkSpec<
  Event,
  Params extends SinkParamCodecs,
  From extends StreamSinkChangeStream = StreamSinkChangeStream,
> {
  readonly name: string;
  readonly from: From;
  readonly event: StreamSinkEventCodec<Event>;
  readonly route: string;
  readonly params: Params;
  readonly feed: StreamSinkFeed;
  readonly protocol: StreamSinkProtocol;
  readonly errors?: readonly StreamSinkErrorTag[];
}

export interface CheckedStreamSink<
  Event,
  Key extends keyof Event & string,
  Params extends SinkParamCodecs,
  From extends StreamSinkChangeStream = StreamSinkChangeStream,
> extends StreamSinkSpec<Event, Params, From> {
  readonly kind: "checked-stream-sink";
  /** The change stream's declared key, carried so consumers read one key, not two. */
  readonly key: Key;
  readonly feed: StreamSinkFeed & { readonly subjectKey: Key };
  readonly fingerprint: string;
  readonly compiledRoute: ReturnType<typeof compileSinkRoute<Params>>;
}

export function defineStreamSink<
  Event,
  const Route extends string,
  const Params extends SinkParamCodecs,
  From extends StreamSinkChangeStream<keyof Event & string>,
>(
  spec: StreamSinkSpec<Event, Params, From> & { readonly route: Route } & ExactRouteParams<
      Route,
      Params
    >,
): CheckedStreamSink<Event, StreamKeyOf<Event, From>, Params, From> {
  // SAFETY: `From` is constrained to declare a key of `Event`, so the change stream's declared key is that field name.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion, anti-slop/no-known-value-widening
  const key = spec.from.key as StreamKeyOf<Event, From>;
  const feed = { ...spec.feed, subjectKey: key };
  const compiledRoute = compileSinkRoute(spec.route, spec.params);
  // The fingerprint input is written out rather than spread, so a reviewer can
  // see exactly which declared facts a consumer's contract check is bound to.
  const fingerprint = contractFingerprint({
    name: spec.name,
    source: spec.from.name,
    route: spec.route,
    params: [...compiledRoute.parameterNames],
    key,
    feed: { name: feed.name, type: feed.type, subjectKey: feed.subjectKey },
    protocol: {
      sessionVersion: spec.protocol.sessionVersion,
      transport: spec.protocol.transport,
      resume: spec.protocol.resume,
      order: spec.protocol.order,
      fallback: spec.protocol.fallback,
    },
    errors: [...(spec.errors ?? [])],
  });
  return Object.freeze({
    ...spec,
    kind: "checked-stream-sink",
    key,
    feed: Object.freeze(feed),
    fingerprint,
    compiledRoute,
  });
}

export type EventOf<Sink> =
  Sink extends CheckedStreamSink<infer Event, infer _Key, infer _Params, infer _From>
    ? Event
    : never;
export type StreamParamsOf<Sink> =
  Sink extends CheckedStreamSink<infer _Event, infer _Key, infer Params, infer _From>
    ? DecodedSinkParams<Params>
    : never;
