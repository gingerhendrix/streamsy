/**
 * Decode one standard Durable Streams HTTP response into its protocol outcome.
 *
 * Status and documented headers are the source of truth. The direct Layer
 * supplies more than the public wire can carry, so the transport reports what
 * the wire expresses: batch-level read metadata, message payloads, and the
 * conflict classifications the host's fixed 409 bodies name.
 */
import { DateTime, Effect, Option } from "effect";
import type {
  AppendOutcome,
  CreateOutcome,
  HeadOutcome,
  ReadMessage,
  ReadNextOutcome,
  ReadOutcome,
  RemoveOutcome,
} from "../protocol/outcomes.ts";
import type { TransportFault } from "../fault.ts";
import * as Wire from "./wire.ts";

export type Operation = "head" | "read" | "readNext" | "create" | "append" | "remove";

export type Outcome =
  | AppendOutcome
  | CreateOutcome
  | HeadOutcome
  | ReadOutcome
  | ReadNextOutcome
  | RemoveOutcome;

export interface DecodeContext {
  /** The request carried producer headers: 200 acknowledges a write, 204 a duplicate. */
  readonly producer?: boolean;
}

const emptyToUndefined = (value: string): string | undefined => {
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
};

export const head = (
  operation: Operation,
  response: Wire.HttpResponse,
): Effect.Effect<HeadOutcome, TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 404) return { status: "not-found" as const };
    if (response.status === 410) return { status: "gone" as const };
    if (response.status !== 200) return yield* Wire.unexpected(operation, response);
    const contentType = yield* Wire.contentType(operation, response);
    const nextOffset = yield* Wire.offset(operation, response);
    const ttlSeconds = yield* Wire.integer(
      operation,
      "stream-ttl",
      Wire.header(response, "stream-ttl"),
    );
    const expiresAt = Wire.header(response, "stream-expires-at");
    if (expiresAt !== undefined && Option.isNone(DateTime.make(expiresAt)))
      return yield* Effect.fail(
        Wire.failure(operation, "response", `Invalid stream-expires-at header: ${expiresAt}`),
      );
    const closed = yield* Wire.closedHeader(operation, response);
    return { status: "ok" as const, contentType, nextOffset, ttlSeconds, expiresAt, closed };
  });

export const create = (
  operation: Operation,
  response: Wire.HttpResponse,
): Effect.Effect<CreateOutcome, TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 200 || response.status === 201) {
      const contentType = yield* Wire.contentType(operation, response);
      const nextOffset = yield* Wire.offset(operation, response);
      const closed = yield* Wire.closedHeader(operation, response);
      return {
        status: response.status === 201 ? ("created" as const) : ("exists" as const),
        nextOffset,
        contentType,
        closed,
      };
    }
    if (response.status === 400) {
      const feature = Wire.header(response, "stream-not-supported");
      const message = emptyToUndefined(yield* Wire.bodyText(operation, response));
      if (feature !== undefined) return { status: "not-supported" as const, feature, message };
      return {
        status: "bad-request" as const,
        nextOffset: "",
        contentType: "",
        errorMessage: message,
      };
    }
    if (response.status === 404)
      return {
        status: "not-found" as const,
        nextOffset: "",
        contentType: "",
        errorMessage: emptyToUndefined(yield* Wire.bodyText(operation, response)),
      };
    if (response.status === 409)
      return {
        status: "conflict" as const,
        nextOffset: "",
        contentType: "",
        errorMessage: emptyToUndefined(yield* Wire.bodyText(operation, response)),
      };
    return yield* Wire.unexpected(operation, response);
  });

export const append = (
  operation: Operation,
  response: Wire.HttpResponse,
  context: DecodeContext,
): Effect.Effect<AppendOutcome, TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 200 || response.status === 204) {
      const offset = yield* Wire.offset(operation, response);
      const producerEpoch = yield* Wire.integer(
        operation,
        "producer-epoch",
        Wire.header(response, "producer-epoch"),
      );
      const producerSeq = yield* Wire.integer(
        operation,
        "producer-seq",
        Wire.header(response, "producer-seq"),
      );
      const closed = yield* Wire.closedHeader(operation, response);
      if (response.status === 200)
        return { status: "appended" as const, offset, producerEpoch, producerSeq, closed };
      // The public wire acknowledges a producer write and a duplicate with 204.
      if (context.producer !== true) return { status: "appended" as const, offset, closed };
      if (producerEpoch === undefined || producerSeq === undefined)
        return yield* Effect.fail(
          Wire.failure(
            operation,
            "response",
            "Duplicate acknowledgement is missing producer state",
          ),
        );
      return { status: "duplicate" as const, offset, producerEpoch, producerSeq, closed };
    }
    if (response.status === 404) return { status: "not-found" as const };
    if (response.status === 410) return { status: "gone" as const };
    if (response.status === 503) return { status: "busy" as const };
    if (response.status === 403) {
      const currentEpoch = yield* Wire.integer(
        operation,
        "producer-epoch",
        Wire.header(response, "producer-epoch"),
      );
      if (currentEpoch === undefined)
        return yield* Effect.fail(
          Wire.failure(operation, "response", "Stale epoch response is missing producer-epoch"),
        );
      return { status: "stale-epoch" as const, currentEpoch };
    }
    if (response.status === 400) {
      const feature = Wire.header(response, "stream-not-supported");
      if (feature !== undefined) {
        const message = emptyToUndefined(yield* Wire.bodyText(operation, response));
        return { status: "not-supported" as const, feature, message };
      }
      const detail = yield* Wire.bodyText(operation, response);
      // The public classification for a new-epoch sequence error is a plain 400 body.
      if (detail.includes("New epoch must start at seq=0"))
        return { status: "invalid-epoch-seq" as const };
      return yield* Effect.fail(Wire.unexpectedWith(operation, response.status, detail));
    }
    if (response.status === 409) return yield* appendConflict(operation, response);
    return yield* Wire.unexpected(operation, response);
  });

const appendConflict = (
  operation: Operation,
  response: Wire.HttpResponse,
): Effect.Effect<AppendOutcome, TransportFault> =>
  Effect.gen(function* () {
    const expectedSeq = yield* Wire.integer(
      operation,
      "producer-expected-seq",
      Wire.header(response, "producer-expected-seq"),
    );
    const receivedSeq = yield* Wire.integer(
      operation,
      "producer-received-seq",
      Wire.header(response, "producer-received-seq"),
    );
    if (expectedSeq !== undefined && receivedSeq !== undefined)
      return { status: "producer-gap" as const, expectedSeq, receivedSeq };
    const closed = yield* Wire.closedHeader(operation, response);
    const offset = yield* Wire.optionalOffset(operation, response);
    if (closed && offset !== undefined)
      return {
        status: "conflict" as const,
        conflictReason: "closed" as const,
        offset,
        closed: true as const,
      };
    if (!closed && offset !== undefined)
      return { status: "conflict" as const, conflictReason: "expected-offset" as const, offset };
    // A public Durable Streams host names these two failures in the 409 body only.
    const detail = yield* Wire.bodyText(operation, response);
    if (detail.includes("Content-Type mismatch"))
      return { status: "conflict" as const, conflictReason: "content-type" as const };
    if (detail.includes("Sequence conflict"))
      return { status: "conflict" as const, conflictReason: "sequence" as const };
    return yield* Effect.fail(
      Wire.failure(operation, "response", `Unclassified 409 conflict: ${detail.trim()}`),
    );
  });

export const read = (
  operation: Operation,
  response: Wire.HttpResponse,
): Effect.Effect<ReadOutcome, TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 404) return { status: "not-found" as const };
    if (response.status === 410) return { status: "gone" as const };
    if (response.status !== 200) return yield* Wire.unexpected(operation, response);
    const nextOffset = yield* Wire.offset(operation, response);
    const messages = yield* readBatch(operation, response);
    const closed = yield* Wire.closedHeader(operation, response);
    return {
      status: "ok" as const,
      messages,
      nextOffset,
      upToDate: Wire.upToDate(response),
      closed,
    };
  });

export const readNext = (
  operation: Operation,
  response: Wire.HttpResponse,
): Effect.Effect<ReadNextOutcome, TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 404) return missing("not-found");
    if (response.status === 410) return missing("gone");
    if (response.status === 400) {
      const feature = Wire.header(response, "stream-not-supported");
      if (feature === undefined) return yield* Wire.unexpected(operation, response);
      const message = emptyToUndefined(yield* Wire.bodyText(operation, response));
      return { status: "not-supported" as const, feature, message };
    }
    if (response.status !== 200 && response.status !== 204)
      return yield* Wire.unexpected(operation, response);
    const nextOffset = yield* Wire.offset(operation, response);
    const closed = yield* Wire.closedHeader(operation, response);
    // An empty long poll is 204 whether it timed out or woke without data.
    if (response.status === 204)
      return {
        status: "timeout" as const,
        messages: [],
        nextOffset,
        upToDate: Wire.upToDate(response),
        cursor: Wire.cursor(response),
        closed,
      };
    const messages = yield* readBatch(operation, response);
    return {
      status: "ok" as const,
      messages,
      nextOffset,
      upToDate: Wire.upToDate(response),
      cursor: Wire.cursor(response),
      closed,
    };
  });

const missing = (status: "not-found" | "gone"): ReadNextOutcome => ({
  status,
  messages: [],
  nextOffset: "",
  upToDate: false,
  cursor: "",
});

const readBatch = (
  operation: Operation,
  response: Wire.HttpResponse,
): Effect.Effect<ReadMessage[], TransportFault> =>
  Wire.body(operation, response).pipe(
    Effect.flatMap((bytes) => Wire.readMessages(operation, response, bytes)),
  );

export const remove = (
  operation: Operation,
  response: Wire.HttpResponse,
): Effect.Effect<RemoveOutcome, TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 204) return { status: "ok" as const };
    if (response.status === 404) return { status: "not-found" as const };
    if (response.status === 410) return { status: "gone" as const };
    if (response.status === 503) return { status: "busy" as const };
    return yield* Wire.unexpected(operation, response);
  });
