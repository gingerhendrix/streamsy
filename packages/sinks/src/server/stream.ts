/**
 * Serving one checked stream sink.
 *
 * The handler owns the public protocol and nothing else: route matching,
 * version and contract negotiation, resume by native offset, and the declared
 * decode. Reading the feed itself is a capability the host supplies, because
 * only the host knows where its durable stream lives.
 *
 * Order is not a policy this handler applies. It serves the page it is given,
 * in the order it is given, and the declaration says that order is arrival
 * order.
 */
import { Effect, Schema } from "effect";
import type { SinkParamCodecs, DecodedSinkParams } from "../route.ts";
import type { CheckedStreamSink } from "../stream-contract.ts";
import type { StreamSinkPublicError } from "../stream-errors.ts";
import { STREAM_SINK_CONTRACT_HEADER, STREAM_SINK_VERSION_HEADER } from "../stream-protocol.ts";

export class StreamSinkSourceFailure extends Schema.TaggedError<StreamSinkSourceFailure>()(
  "StreamSinkSourceFailure",
  {
    reason: Schema.Literals(["unavailable", "invalid-offset", "history-unavailable"]),
    detail: Schema.String,
  },
) {}

/** One bounded page of the feed, already read in arrival order. */
export interface StreamSinkPage {
  readonly events: readonly unknown[];
  /** After-exclusive native offset a consumer passes back to continue. */
  readonly nextOffset: string;
  readonly upToDate: boolean;
}

export interface StreamSinkServerCapabilities<Params, Requirements = never> {
  readonly read: (
    params: Params,
    /** The requested after-exclusive offset, or `undefined` for the start of the feed. */
    offset: string | undefined,
  ) => Effect.Effect<StreamSinkPage, StreamSinkSourceFailure, Requirements>;
}

export function handleStreamSink<
  Event,
  Key extends keyof Event & string,
  Params extends SinkParamCodecs,
  Requirements,
>(
  sink: CheckedStreamSink<Event, Key, Params>,
  request: Request,
  capabilities: StreamSinkServerCapabilities<DecodedSinkParams<Params>, Requirements>,
): Effect.Effect<Response, never, Requirements> {
  const url = new URL(request.url);
  const matched = sink.compiledRoute.match(url.pathname);
  if (matched.kind === "mismatch") {
    return Effect.succeed(
      errorResponse(404, {
        _tag: "InvalidSinkParams",
        sink: sink.name,
        parameter: "route",
        detail: "request path does not match the checked sink route",
      }),
    );
  }
  if (matched.kind === "invalid") {
    return Effect.succeed(
      errorResponse(400, {
        _tag: "InvalidSinkParams",
        sink: sink.name,
        parameter: matched.parameter,
        detail: matched.detail,
      }),
    );
  }

  const params = matched.params;
  return Effect.gen(function* () {
    const receivedVersion = request.headers.get(STREAM_SINK_VERSION_HEADER) ?? "1";
    if (receivedVersion !== String(sink.protocol.sessionVersion)) {
      return errorResponse(409, {
        _tag: "ProtocolVersionUnsupported",
        sink: sink.name,
        supported: sink.protocol.sessionVersion,
        received: receivedVersion,
        recovery: sink.protocol.fallback,
      });
    }

    const receivedContract = request.headers.get(STREAM_SINK_CONTRACT_HEADER);
    if (receivedContract !== null && receivedContract !== sink.fingerprint) {
      return errorResponse(409, {
        _tag: "ResumeRejected",
        sink: sink.name,
        reason: "contract-changed",
        recovery: sink.protocol.fallback,
      });
    }

    const requested = url.searchParams.get("offset");
    const offset = requested === null || requested === "-1" ? undefined : requested;
    const page = yield* capabilities.read(params, offset);

    const events: Event[] = [];
    for (const value of page.events) {
      const decoded = yield* decodeEvent(sink, value);
      events.push(decoded);
    }

    const headers = sessionHeaders(sink);
    headers.set("content-type", "application/json");
    headers.set("cache-control", "no-store");
    headers.set("stream-next-offset", page.nextOffset);
    headers.set("stream-up-to-date", String(page.upToDate));
    const body = {
      sink: sink.name,
      feed: sink.feed,
      order: sink.protocol.order,
      events,
      nextOffset: page.nextOffset,
      upToDate: page.upToDate,
    };
    return Response.json(body, { status: 200, headers });
  }).pipe(
    Effect.catchTags({
      StreamSinkSourceFailure: (failure: StreamSinkSourceFailure) =>
        Effect.succeed(sourceFailureResponse(sink.name, sink.protocol.fallback, failure)),
      StreamSinkDecodeFailure: (failure: StreamSinkDecodeFailure) =>
        Effect.succeed(
          errorResponse(500, {
            _tag: "WireDecodeFailed",
            sink: sink.name,
            detail: failure.detail,
          }),
        ),
    }),
  );
}

class StreamSinkDecodeFailure extends Schema.TaggedError<StreamSinkDecodeFailure>()(
  "StreamSinkDecodeFailure",
  { detail: Schema.String },
) {}

const decodeEvent = <Event, Key extends keyof Event & string, Params extends SinkParamCodecs>(
  sink: CheckedStreamSink<Event, Key, Params>,
  /* oxlint-disable-next-line anti-slop/no-unknown-parameters -- This is the raw feed item, decoded by the declared codec on the next line. */
  value: unknown,
): Effect.Effect<Event, StreamSinkDecodeFailure> =>
  Effect.try({
    try: () => sink.event.decode(value),
    catch: (cause) =>
      new StreamSinkDecodeFailure({
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });

function sourceFailureResponse(
  sink: string,
  recovery: "replay-from-start",
  failure: StreamSinkSourceFailure,
): Response {
  if (failure.reason === "unavailable") {
    return errorResponse(503, { _tag: "FeedUnavailable", sink, detail: failure.detail });
  }
  return errorResponse(409, { _tag: "ResumeRejected", sink, reason: failure.reason, recovery });
}

function sessionHeaders<Event, Key extends keyof Event & string, Params extends SinkParamCodecs>(
  sink: CheckedStreamSink<Event, Key, Params>,
): Headers {
  return new Headers({
    [STREAM_SINK_VERSION_HEADER]: String(sink.protocol.sessionVersion),
    [STREAM_SINK_CONTRACT_HEADER]: sink.fingerprint,
  });
}

function errorResponse(status: number, error: StreamSinkPublicError): Response {
  return new Response(JSON.stringify(error), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}
