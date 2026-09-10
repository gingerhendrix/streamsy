/**
 * Standard Durable Streams HTTP response primitives.
 *
 * The transport decodes the public protocol only: status codes, the documented
 * response headers, and the content-type-framed body. Streamsy adds no private
 * media type, response header, or envelope. Where a public response cannot carry
 * a field the direct Layer returns, the decoder reports what the wire has.
 */
import { Effect, Schema, Stream } from "effect";
import type { HttpClientResponse } from "effect/unstable/http";
import { TransportFault } from "../fault.ts";
import type { ReadMessage } from "../protocol/outcomes.ts";

export type HttpResponse = HttpClientResponse.HttpClientResponse;
export type FaultReason = "request" | "response" | "decode" | "configuration";

const INTEGER = /^(0|[1-9]\d*)$/;
const OFFSET = /^\d{16}_\d{16}$/;
const CONTENT_TYPE = /^[^\s/;]+\/[^\s;]+(?:;.*)?$/;

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- Transport, UTF-8 and body failures are retained as diagnostic causes.
export const failure = (
  operation: string,
  reason: FaultReason,
  message: string,
  cause?: unknown,
): TransportFault => new TransportFault({ operation, reason, message, cause });

export const header = (response: HttpResponse, name: string): string | undefined =>
  response.headers[name];

/** `stream-closed` is absent, `true`, or `false`; anything else is a wire fault. */
export const closedHeader = (
  operation: string,
  response: HttpResponse,
): Effect.Effect<boolean, TransportFault> => {
  const value = header(response, "stream-closed");
  if (value === undefined || value === "true" || value === "false")
    return Effect.succeed(value === "true");
  return Effect.fail(failure(operation, "response", `Invalid stream-closed header: ${value}`));
};

export const upToDate = (response: HttpResponse): boolean =>
  header(response, "stream-up-to-date") === "true";

/** The protocol emits a cursor only while a live read can be resumed. */
export const cursor = (response: HttpResponse): string => header(response, "stream-cursor") ?? "";

export const optionalOffset = (
  operation: string,
  response: HttpResponse,
): Effect.Effect<string | undefined, TransportFault> => {
  const value = header(response, "stream-next-offset");
  if (value === undefined) return Effect.succeed(undefined);
  if (!OFFSET.test(value))
    return Effect.fail(
      failure(operation, "response", `Invalid stream-next-offset header: ${value}`),
    );
  return Effect.succeed(value);
};

export const offset = (
  operation: string,
  response: HttpResponse,
): Effect.Effect<string, TransportFault> =>
  optionalOffset(operation, response).pipe(
    Effect.flatMap((value) =>
      value === undefined
        ? Effect.fail(failure(operation, "response", "Missing stream-next-offset header"))
        : Effect.succeed(value),
    ),
  );

export const contentType = (
  operation: string,
  response: HttpResponse,
): Effect.Effect<string, TransportFault> => {
  const value = header(response, "content-type");
  if (value === undefined)
    return Effect.fail(failure(operation, "response", "Missing content-type header"));
  if (!CONTENT_TYPE.test(value))
    return Effect.fail(failure(operation, "response", `Invalid content-type header: ${value}`));
  return Effect.succeed(value);
};

export const integer = (
  operation: string,
  name: string,
  value: string | undefined,
): Effect.Effect<number | undefined, TransportFault> => {
  if (value === undefined) return Effect.succeed(undefined);
  if (!INTEGER.test(value))
    return Effect.fail(failure(operation, "response", `Invalid ${name} header: ${value}`));
  return Effect.succeed(Number(value));
};

export const body = (
  operation: string,
  response: HttpResponse,
): Effect.Effect<Uint8Array, TransportFault> =>
  response.stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => {
      const bytes = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.length, 0));
      let position = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, position);
        position += chunk.length;
      }
      return bytes;
    }),
    Effect.mapError((cause) => failure(operation, "decode", "Cannot read response body", cause)),
  );

export const text = (operation: string, bytes: Uint8Array): Effect.Effect<string, TransportFault> =>
  Effect.try({
    try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    catch: (cause) => failure(operation, "decode", "Response body is not valid UTF-8", cause),
  });

export const bodyText = (
  operation: string,
  response: HttpResponse,
): Effect.Effect<string, TransportFault> =>
  body(operation, response).pipe(Effect.flatMap((bytes) => text(operation, bytes)));

/** Message for a status the operation has no outcome for, keeping any plain-text detail. */
export const unexpectedWith = (
  operation: string,
  status: number,
  detail: string,
): TransportFault => {
  const trimmed = detail.trim();
  return failure(
    operation,
    "response",
    trimmed === "" ? `Unexpected HTTP ${status}` : `Unexpected HTTP ${status}: ${trimmed}`,
  );
};

/** Classify an unrecognized status, keeping the host's plain-text detail in the message. */
export const unexpected = (
  operation: string,
  response: HttpResponse,
): Effect.Effect<never, TransportFault> =>
  bodyText(operation, response).pipe(
    Effect.flatMap((detail) => Effect.fail(unexpectedWith(operation, response.status, detail))),
  );

/**
 * A JSON read body is one JSON array whose elements are the stored messages. The
 * elements decode by value; re-serializing them returns canonical JSON text, not
 * the stored bytes. Text and binary bodies stay a single merged payload.
 */
export const readMessages = (
  operation: string,
  response: HttpResponse,
  bytes: Uint8Array,
): Effect.Effect<ReadMessage[], TransportFault> => {
  if (bytes.byteLength === 0) return Effect.succeed([]);
  return contentType(operation, response).pipe(
    Effect.flatMap((value) => {
      if (value.split(";")[0]?.trim().toLowerCase() === "application/json")
        return text(operation, bytes).pipe(
          Effect.flatMap((decoded) => jsonMessages(operation, decoded)),
        );
      return Effect.succeed([{ data: bytes }]);
    }),
  );
};

const jsonArray = Schema.fromJsonString(Schema.Array(Schema.Unknown));

const jsonMessages = (
  operation: string,
  decoded: string,
): Effect.Effect<ReadMessage[], TransportFault> =>
  Schema.decodeUnknownEffect(jsonArray)(decoded).pipe(
    Effect.mapError((cause) =>
      failure(operation, "decode", "Read body is not a JSON array", cause),
    ),
    Effect.map((items) =>
      items.map((item) => ({ data: new TextEncoder().encode(JSON.stringify(item)) })),
    ),
  );
