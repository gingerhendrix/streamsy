import { DateTime, Effect, Option, Schema, Stream } from "effect";
import type { HttpClientResponse } from "effect/unstable/http";
import { TransportFault } from "../fault.ts";
import * as Wire from "./wire.ts";

type Outcome =
  | typeof Wire.head.Type
  | typeof Wire.read.Type
  | typeof Wire.readNext.Type
  | typeof Wire.create.Type
  | typeof Wire.append.Type
  | typeof Wire.remove.Type;

export function validate(
  operation: string,
  response: HttpClientResponse.HttpClientResponse,
  result: Outcome,
) {
  const status = result.status;
  const statuses = {
    created: [201],
    exists: [200],
    appended: [200, 204],
    duplicate: [204],
    "not-found": [404],
    gone: [410],
    conflict: [409],
    "producer-gap": [409],
    busy: [503],
    "stale-epoch": [403],
    "not-supported": [400],
    "bad-request": [400],
    "invalid-epoch-seq": [400],
    timeout: [200],
    ok: operation === "remove" ? [204] : [200],
  } satisfies Record<Outcome["status"], ReadonlyArray<number>>;
  const expected = statuses[status];
  const invalid = (message: string) =>
    Effect.fail(new TransportFault({ operation, reason: "response", message }));
  if (!expected.includes(response.status))
    return invalid(`Outcome ${status} contradicts HTTP ${response.status}`);
  if ((operation === "head" && status === "ok") || status === "created" || status === "exists") {
    if (!("nextOffset" in result) || response.headers["stream-next-offset"] !== result.nextOffset)
      return invalid("Missing or inconsistent stream-next-offset");
    if (!("contentType" in result) || response.headers["content-type"] !== result.contentType)
      return invalid("Missing or inconsistent content-type");
  }
  if (
    status === "appended" ||
    status === "duplicate" ||
    (status === "conflict" && "offset" in result)
  ) {
    if (!("offset" in result) || response.headers["stream-next-offset"] !== result.offset)
      return invalid("Missing or inconsistent append offset");
  }
  if (status === "appended" || status === "duplicate") {
    if ((result.producerEpoch === undefined) !== (result.producerSeq === undefined))
      return invalid("Incomplete producer acknowledgement");
    if (
      result.producerEpoch !== undefined &&
      (response.headers["producer-epoch"] !== String(result.producerEpoch) ||
        response.headers["producer-seq"] !== String(result.producerSeq))
    )
      return invalid("Missing or inconsistent producer acknowledgement");
  }
  if ("closed" in result && !("messages" in result)) {
    const expectedClosed = result.closed ? "true" : undefined;
    if (response.headers["stream-closed"] !== expectedClosed)
      return invalid("Inconsistent closed header");
  }
  if (
    "contentType" in result &&
    (status === "ok" || status === "created" || status === "exists") &&
    !/^[^\s/;]+\/[^\s;]+(?:;.*)?$/.test(result.contentType)
  )
    return invalid("Invalid content type");
  if (operation === "head" && status === "ok" && "contentType" in result) {
    if (
      "ttlSeconds" in result &&
      result.ttlSeconds !== undefined &&
      response.headers["stream-ttl"] !== (result.ttlSeconds ? String(result.ttlSeconds) : undefined)
    )
      return invalid("Inconsistent TTL header");
    if (
      "expiresAt" in result &&
      result.expiresAt !== undefined &&
      (response.headers["stream-expires-at"] !== result.expiresAt ||
        Option.isNone(DateTime.make(result.expiresAt)))
    )
      return invalid("Invalid expiry header");
  }
  if (
    status === "stale-epoch" &&
    response.headers["producer-epoch"] !== String(result.currentEpoch)
  )
    return invalid("Inconsistent current epoch");
  if (
    status === "producer-gap" &&
    (response.headers["producer-expected-seq"] !== String(result.expectedSeq) ||
      response.headers["producer-received-seq"] !== String(result.receivedSeq))
  )
    return invalid("Inconsistent producer gap");
  if ("messages" in result && result.messages.length > 0) {
    if (result.messages.at(-1)?.offset !== result.nextOffset)
      return invalid("Last message does not match the read cursor");
    for (let index = 1; index < result.messages.length; index++) {
      if (result.messages[index - 1]!.offset >= result.messages[index]!.offset)
        return invalid("Message offsets are not increasing");
    }
  }
  return Effect.void;
}
export type { Outcome };

export const decode = <S extends Schema.Constraint & { readonly Type: Outcome }>(
  operation: string,
  response: HttpClientResponse.HttpClientResponse,
  schema: S,
) =>
  Effect.gen(function* () {
    // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Errors from HTTP, UTF-8 and schema decoding are retained as diagnostic causes.
    const fault = (cause: unknown) =>
      new TransportFault({ operation, reason: "decode", message: "Cannot decode response", cause });
    const raw = response.headers[Wire.resultHeader];
    const json = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
    const value = yield* Effect.gen(function* () {
      if (raw !== undefined)
        return yield* Effect.try({ try: () => decodeURIComponent(raw), catch: fault }).pipe(
          Effect.flatMap(json),
          Effect.mapError(fault),
        );
      if (response.headers["content-type"] === Wire.format) {
        const chunks = yield* response.stream.pipe(Stream.runCollect, Effect.mapError(fault));
        const text = yield* Effect.try({
          try: () => {
            const bytes = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.length, 0));
            let offset = 0;
            for (const chunk of chunks) {
              bytes.set(chunk, offset);
              offset += chunk.length;
            }
            return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
          },
          catch: fault,
        });
        return yield* json(text).pipe(Effect.mapError(fault));
      }
      if (operation === "remove" && response.status === 204) return { status: "ok" };
      if (
        (operation === "remove" || operation === "head" || operation === "append") &&
        (response.status === 404 || response.status === 410)
      )
        return { status: response.status === 404 ? "not-found" : "gone" };
      if (operation === "remove" && response.status === 503) return { status: "busy" };
      return yield* new TransportFault({
        operation,
        reason: "response",
        message: `Missing Streamsy outcome representation (HTTP ${response.status})`,
      });
    });
    const result = yield* Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(fault));
    yield* validate(operation, response, result);
    return result;
  });
