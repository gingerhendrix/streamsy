import { Effect, Schema } from "effect";
import type { CheckedStateSink } from "./contract.ts";
import type { StateSinkPublicError } from "./errors.ts";
import {
  STATE_SINK_AUTHORIZATION_GENERATION_HEADER,
  STATE_SINK_CONTRACT_HEADER,
  STATE_SINK_RESET_HEADER,
  STATE_SINK_RESET_VALUE,
  STATE_SINK_VERSION_HEADER,
} from "./protocol.ts";
import type { SinkParamCodecs } from "./route.ts";
import type { DecodedSinkParams } from "./route.ts";
import {
  SinkAuthorizationDenied,
  SinkAuthorizationUnavailable,
  StateSinkAuthorizer,
  type AuthorizedSinkContext,
} from "./authorization.ts";

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
    authorization: AuthorizedSinkContext,
  ) => Effect.Effect<StateSinkSnapshot<Row>, StateSinkSourceFailure, Requirements>;
  readonly suffix: (
    request: Request,
    params: Params,
    authorization: AuthorizedSinkContext,
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
): Effect.Effect<Response, never, StateSinkAuthorizer | Requirements> {
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
    const authorizer = yield* StateSinkAuthorizer;
    const authorization = yield* authorizer.authorize({
      request,
      sink,
      params: matched.params,
    });

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

    const receivedGeneration = request.headers.get(STATE_SINK_AUTHORIZATION_GENERATION_HEADER);
    if (receivedGeneration !== null && receivedGeneration !== authorization.generation) {
      return errorResponse(409, {
        _tag: "ResumeRejected",
        sink: sink.name,
        reason: "authorization-generation-changed",
        recovery: sink.protocol.fallback,
      });
    }

    if (request.headers.get(STATE_SINK_RESET_HEADER) === STATE_SINK_RESET_VALUE) {
      const snapshot = yield* capabilities.snapshot(matched.params, authorization);
      return snapshotResponse(sink, snapshot, authorization);
    }

    const response = yield* capabilities.suffix(request, matched.params, authorization);
    const offset = new URL(request.url).searchParams.get("offset");
    if (offset !== null && offset !== "-1" && [400, 404, 410].includes(response.status)) {
      return errorResponse(409, {
        _tag: "ResumeRejected",
        sink: sink.name,
        reason: response.status === 400 ? "invalid-offset" : "history-unavailable",
        recovery: sink.protocol.fallback,
      });
    }
    return withSessionHeaders(response, sink, authorization);
  }).pipe(
    Effect.catchTags({
      SinkAuthorizationDenied: (error: SinkAuthorizationDenied) =>
        Effect.succeed(
          errorResponse(403, {
            _tag: "SinkUnauthorized",
            sink: sink.name,
            required: error.required,
          }),
        ),
      SinkAuthorizationUnavailable: (error: SinkAuthorizationUnavailable) =>
        Effect.succeed(
          errorResponse(503, {
            _tag: "TransportUnavailable",
            sink: sink.name,
            detail: error.detail,
          }),
        ),
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
>(
  sink: CheckedStateSink<Row, Key, Params>,
  snapshot: StateSinkSnapshot<Row>,
  authorization: AuthorizedSinkContext,
): Response {
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
  const headers = sessionHeaders(sink, authorization);
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
>(
  response: Response,
  sink: CheckedStateSink<Row, Key, Params>,
  authorization: AuthorizedSinkContext,
): Response {
  const headers = new Headers(response.headers);
  sessionHeaders(sink, authorization).forEach((value, name) => headers.set(name, value));
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
>(sink: CheckedStateSink<Row, Key, Params>, authorization: AuthorizedSinkContext): Headers {
  return new Headers({
    [STATE_SINK_VERSION_HEADER]: String(sink.protocol.sessionVersion),
    [STATE_SINK_CONTRACT_HEADER]: sink.fingerprint,
    [STATE_SINK_AUTHORIZATION_GENERATION_HEADER]: authorization.generation,
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
