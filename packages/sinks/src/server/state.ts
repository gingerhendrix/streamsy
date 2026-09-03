import { Effect, Schema } from "effect";
import type { CheckedStateSink } from "../state-contract.ts";
import type { StateSinkPublicError } from "../state-errors.ts";
import {
  STATE_SINK_CONTRACT_HEADER,
  STATE_SINK_RESET_HEADER,
  STATE_SINK_RESET_VALUE,
  STATE_SINK_VERSION_HEADER,
} from "../state-protocol.ts";
import type { SinkParamCodecs } from "../route.ts";
import type { DecodedSinkParams } from "../route.ts";

export class StateSinkSourceFailure extends Schema.TaggedError<StateSinkSourceFailure>()(
  "StateSinkSourceFailure",
  {
    phase: Schema.Literals(["snapshot", "suffix"]),
    detail: Schema.String,
  },
) {}

export interface StateSinkSnapshot<Row> {
  readonly rows: readonly Row[];
  readonly offset: string;
}

export interface StateSinkServerCapabilities<Row, Params, Requirements = never> {
  readonly snapshot: (
    params: Params,
  ) => Effect.Effect<StateSinkSnapshot<Row>, StateSinkSourceFailure, Requirements>;
  readonly suffix: (
    request: Request,
    params: Params,
  ) => Effect.Effect<Response, StateSinkSourceFailure, Requirements>;
}

export function handleStateSink<
  Row extends object,
  Key extends keyof Row & string,
  Params extends SinkParamCodecs,
  Requirements,
>(
  sink: CheckedStateSink<Row, Key, Params>,
  request: Request,
  capabilities: StateSinkServerCapabilities<Row, DecodedSinkParams<Params>, Requirements>,
): Effect.Effect<Response, never, Requirements> {
  const matched = sink.compiledRoute.match(new URL(request.url).pathname);
  if (matched.kind === "mismatch") return Effect.succeed(errorResponse(404, invalid(sink.name)));
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

  return Effect.gen(function* () {
    const receivedVersion = request.headers.get(STATE_SINK_VERSION_HEADER) ?? "1";
    if (receivedVersion !== String(sink.protocol.sessionVersion)) {
      return errorResponse(409, {
        _tag: "ProtocolVersionUnsupported",
        sink: sink.name,
        supported: sink.protocol.sessionVersion,
        received: receivedVersion,
        recovery: sink.protocol.fallback,
      });
    }

    const receivedContract = request.headers.get(STATE_SINK_CONTRACT_HEADER);
    if (receivedContract !== null && receivedContract !== sink.fingerprint) {
      return errorResponse(409, {
        _tag: "ResumeRejected",
        sink: sink.name,
        reason: "contract-changed",
        recovery: sink.protocol.fallback,
      });
    }

    if (request.headers.get(STATE_SINK_RESET_HEADER) === STATE_SINK_RESET_VALUE) {
      const snapshot = yield* capabilities.snapshot(matched.params);
      return snapshotResponse(sink, snapshot);
    }

    const response = yield* capabilities.suffix(request, matched.params);
    const offset = new URL(request.url).searchParams.get("offset");
    if (offset !== null && offset !== "-1" && [400, 404, 410].includes(response.status)) {
      return errorResponse(409, {
        _tag: "ResumeRejected",
        sink: sink.name,
        reason: response.status === 400 ? "invalid-offset" : "history-unavailable",
        recovery: sink.protocol.fallback,
      });
    }
    return withSessionHeaders(response, sink);
  }).pipe(
    Effect.catchTags({
      StateSinkSourceFailure: (error: StateSinkSourceFailure) =>
        Effect.succeed(
          errorResponse(503, {
            _tag: error.phase === "snapshot" ? "SnapshotUnavailable" : "TransportUnavailable",
            sink: sink.name,
            detail: error.detail,
          }),
        ),
    }),
  );
}

function snapshotResponse<
  Row extends object,
  Key extends keyof Row & string,
  Params extends SinkParamCodecs,
>(sink: CheckedStateSink<Row, Key, Params>, snapshot: StateSinkSnapshot<Row>): Response {
  const messages: object[] = [
    { headers: { control: "reset" } },
    { headers: { control: "snapshot-start" } },
    ...snapshot.rows.map((row) => ({
      type: sink.collection.type,
      key: String(row[sink.collection.primaryKey]),
      value: row,
      headers: { operation: "upsert" },
    })),
    { headers: { control: "snapshot-end" } },
  ];
  const headers = sessionHeaders(sink);
  headers.set("content-type", "application/json");
  headers.set("stream-next-offset", snapshot.offset);
  headers.set("stream-up-to-date", "true");
  headers.set(STATE_SINK_RESET_HEADER, STATE_SINK_RESET_VALUE);
  return new Response(JSON.stringify(messages), { status: 200, headers });
}

function withSessionHeaders<
  Row extends object,
  Key extends keyof Row & string,
  Params extends SinkParamCodecs,
>(response: Response, sink: CheckedStateSink<Row, Key, Params>): Response {
  const headers = new Headers(response.headers);
  sessionHeaders(sink).forEach((value, name) => headers.set(name, value));
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function sessionHeaders<
  Row extends object,
  Key extends keyof Row & string,
  Params extends SinkParamCodecs,
>(sink: CheckedStateSink<Row, Key, Params>): Headers {
  return new Headers({
    [STATE_SINK_VERSION_HEADER]: String(sink.protocol.sessionVersion),
    [STATE_SINK_CONTRACT_HEADER]: sink.fingerprint,
  });
}

function errorResponse(status: number, error: StateSinkPublicError): Response {
  return new Response(JSON.stringify(error), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}

function invalid(sink: string): StateSinkPublicError {
  return {
    _tag: "InvalidSinkParams",
    sink,
    parameter: "route",
    detail: "request path does not match the checked sink route",
  };
}
