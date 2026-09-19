/** Decode standard HTTP successes and fail with the protocol details carried by the wire. */
import { DateTime, Effect, Option } from "effect";
import type {
  AppendResult,
  CreateResult,
  HeadResult,
  ReadMessage,
  ReadNextResult,
  ReadResult,
} from "../protocol/results.ts";
import { StreamId } from "../schema/index.ts";
import {
  StreamNotFound,
  StreamGone,
  StreamBusy,
  StreamClosed,
  OffsetMismatch,
  AppendConflict,
  StaleEpoch,
  ProducerGap,
  InvalidAppendRequest,
  CreateConflict,
  ForkSourceNotFound,
  InvalidForkRequest,
  NotSupported,
  type HeadError,
  type ReadError,
  type ReadNextError,
  type CreateError,
  type AppendError,
  type RemoveError,
} from "../protocol/errors.ts";
import type { TransportFault } from "../fault.ts";
import * as Wire from "./wire.ts";

export type Operation = "head" | "read" | "readNext" | "create" | "append" | "remove";

export interface DecodeContext {
  readonly id: StreamId;
  readonly source?: StreamId;
  readonly expectedOffset?: string;
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
  context: DecodeContext,
): Effect.Effect<HeadResult, HeadError | TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 404) return yield* new StreamNotFound({ id: context.id });
    if (response.status === 410) return yield* new StreamGone({ id: context.id });
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
    return { contentType, nextOffset, ttlSeconds, expiresAt, closed };
  });

export const create = (
  operation: Operation,
  response: Wire.HttpResponse,
  context: DecodeContext,
): Effect.Effect<CreateResult, CreateError | TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 200 || response.status === 201) {
      const contentType = yield* Wire.contentType(operation, response);
      const nextOffset = yield* Wire.offset(operation, response);
      const closed = yield* Wire.closedHeader(operation, response);
      return {
        _tag: response.status === 201 ? ("Created" as const) : ("Exists" as const),
        nextOffset,
        contentType,
        closed,
      };
    }
    if (response.status === 400) {
      const feature = Wire.header(response, "stream-not-supported");
      if (feature !== undefined) return yield* new NotSupported({ id: context.id, feature });
      const message = emptyToUndefined(yield* Wire.bodyText(operation, response));
      return yield* new InvalidForkRequest({
        id: context.id,
        message: message ?? "Invalid fork parameters",
      });
    }
    if (response.status === 404)
      return yield* new ForkSourceNotFound({
        id: context.id,
        source: context.source ?? context.id,
      });
    if (response.status === 409)
      return yield* new CreateConflict({
        id: context.id,
        message:
          (yield* Wire.bodyText(operation, response)) ||
          "Stream exists with different configuration",
      });
    return yield* Wire.unexpected(operation, response);
  });

export const append = (
  operation: Operation,
  response: Wire.HttpResponse,
  context: DecodeContext,
): Effect.Effect<AppendResult, AppendError | TransportFault> =>
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
        return { _tag: "Appended" as const, offset, producerEpoch, producerSeq, closed };
      // Close-only writes use 204 too; an already closed stream carries no tuple.
      if (context.producer !== true || (producerEpoch === undefined && producerSeq === undefined))
        return { _tag: "Appended" as const, offset, closed };
      if (producerEpoch === undefined || producerSeq === undefined)
        return yield* Effect.fail(
          Wire.failure(
            operation,
            "response",
            "Duplicate acknowledgement is missing producer state",
          ),
        );
      return { _tag: "Duplicate" as const, offset, producerEpoch, producerSeq, closed };
    }
    if (response.status === 404) return yield* new StreamNotFound({ id: context.id });
    if (response.status === 410) return yield* new StreamGone({ id: context.id });
    if (response.status === 503) return yield* new StreamBusy({ id: context.id });
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
      return yield* new StaleEpoch({ id: context.id, currentEpoch });
    }
    if (response.status === 400) {
      const feature = Wire.header(response, "stream-not-supported");
      if (feature !== undefined) return yield* new NotSupported({ id: context.id, feature });
      const detail = yield* Wire.bodyText(operation, response);
      // A bare 400 cannot distinguish invalid epoch/sequence from other request errors.
      return yield* new InvalidAppendRequest({ id: context.id, message: detail });
    }
    if (response.status === 409) return yield* appendConflict(operation, response, context);
    return yield* Wire.unexpected(operation, response);
  });

const appendConflict = (
  operation: Operation,
  response: Wire.HttpResponse,
  context: DecodeContext,
): Effect.Effect<AppendResult, AppendError | TransportFault> =>
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
      return yield* new ProducerGap({ id: context.id, expectedSeq, receivedSeq });
    const closed = yield* Wire.closedHeader(operation, response);
    const offset = yield* Wire.optionalOffset(operation, response);
    if (closed && offset !== undefined) return yield* new StreamClosed({ id: context.id, offset });
    if (!closed && offset !== undefined && context.expectedOffset !== undefined)
      return yield* new OffsetMismatch({
        id: context.id,
        expected: context.expectedOffset,
        actual: offset,
      });
    return yield* new AppendConflict({
      id: context.id,
      message: yield* Wire.bodyText(operation, response),
    });
  });

export const read = (
  operation: Operation,
  response: Wire.HttpResponse,
  context: DecodeContext,
): Effect.Effect<ReadResult, ReadError | TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 404) return yield* new StreamNotFound({ id: context.id });
    if (response.status === 410) return yield* new StreamGone({ id: context.id });
    if (response.status !== 200) return yield* Wire.unexpected(operation, response);
    const nextOffset = yield* Wire.offset(operation, response);
    const messages = yield* readBatch(operation, response);
    const closed = yield* Wire.closedHeader(operation, response);
    return {
      messages,
      nextOffset,
      upToDate: Wire.upToDate(response),
      closed,
    };
  });

export const readNext = (
  operation: Operation,
  response: Wire.HttpResponse,
  context: DecodeContext,
): Effect.Effect<ReadNextResult, ReadNextError | TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 404) return yield* new StreamNotFound({ id: context.id });
    if (response.status === 410) return yield* new StreamGone({ id: context.id });
    if (response.status === 400) {
      const feature = Wire.header(response, "stream-not-supported");
      if (feature === undefined) return yield* Wire.unexpected(operation, response);
      return yield* new NotSupported({ id: context.id, feature });
    }
    if (response.status !== 200 && response.status !== 204)
      return yield* Wire.unexpected(operation, response);
    const nextOffset = yield* Wire.offset(operation, response);
    const closed = yield* Wire.closedHeader(operation, response);
    // An empty long poll is 204 whether it timed out or woke without data.
    if (response.status === 204)
      return {
        timedOut: true,
        messages: [],
        nextOffset,
        upToDate: Wire.upToDate(response),
        cursor: Wire.cursor(response),
        closed,
      };
    const messages = yield* readBatch(operation, response);
    return {
      messages,
      timedOut: false,
      nextOffset,
      upToDate: Wire.upToDate(response),
      cursor: Wire.cursor(response),
      closed,
    };
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
  context: DecodeContext,
): Effect.Effect<void, RemoveError | TransportFault> =>
  Effect.gen(function* () {
    if (response.status === 204) return;
    if (response.status === 404) return yield* new StreamNotFound({ id: context.id });
    if (response.status === 410) return yield* new StreamGone({ id: context.id });
    if (response.status === 503) return yield* new StreamBusy({ id: context.id });
    return yield* Wire.unexpected(operation, response);
  });
