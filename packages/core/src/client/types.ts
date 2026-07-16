/**
 * Transport-neutral client seam.
 *
 * This is "the internal protocol, made remotable": the same result-object
 * style and status vocabulary as `types/protocol.ts` where semantics coincide,
 * minus what the wire cannot guarantee (per-message offsets/timestamps, exact
 * append acks, forks, delete), plus batch reads.
 *
 * Operations return discriminated-union results and never throw for
 * operational failures. Throwing is reserved for programmer misuse
 * (invalid stream ids, double iteration, concurrent `next()`).
 */

import type { StreamProtocolFactory } from "../types/protocol.ts";

export type StreamOffset = string;

export type JsonPrimitive = null | boolean | number | string;
export type JsonValue =
  | JsonPrimitive
  | { readonly [key: string]: JsonValue }
  | readonly JsonValue[];

export interface ClientRequestOptions {
  signal?: AbortSignal;
}

/**
 * The single client-only failure member. It covers transport/auth/abort and
 * other cross-cutting outcomes so per-operation unions stay small and the
 * direct adapter can pass protocol results through with minimal narrowing.
 */
export type ClientErrorCode =
  | "transport"
  | "unauthorized"
  | "forbidden"
  | "rate-limited"
  | "bad-request"
  | "busy"
  | "parse-error"
  | "not-supported"
  | "aborted"
  | "client-closed"
  | "unknown";

export interface ClientFailure {
  status: "error";
  code: ClientErrorCode;
  message: string;
  httpStatus?: number;
  retryable: boolean;
  cause?: unknown;
}

export interface CreateStreamOptions extends ClientRequestOptions {
  contentType?: string;
  ttlSeconds?: number;
  expiresAt?: string;
  initialData?: Uint8Array | string;
  closed?: boolean;
}

export interface AppendStreamOptions extends ClientRequestOptions {
  /** Required where the substrate requires it (direct append). */
  contentType?: string;
  seq?: string;
}

export interface CloseStreamOptions extends ClientRequestOptions {
  contentType?: string;
  finalData?: Uint8Array | string;
}

export type ClientHeadResult =
  | {
      status: "ok";
      contentType?: string;
      /** Current tail/read-resume token when supplied by the substrate. */
      offset?: StreamOffset;
      closed: boolean;
      etag?: string;
      cacheControl?: string;
    }
  | { status: "not-found" }
  | { status: "gone" }
  | ClientFailure;

export type ClientCreateResult =
  | { status: "created"; contentType?: string }
  | { status: "conflict" }
  | ClientFailure;

export type ClientAppendResult =
  | { status: "appended" }
  | { status: "not-found" }
  | { status: "gone" }
  | { status: "closed" }
  | { status: "conflict" }
  | ClientFailure;

export type ClientCloseResult =
  | { status: "closed"; finalOffset: StreamOffset }
  | { status: "not-found" }
  | { status: "gone" }
  | { status: "conflict" }
  | ClientFailure;

export type ClientReadResult<T extends JsonValue = JsonValue> =
  | { status: "ok"; session: StreamReadSession<T> }
  | { status: "not-found" }
  | { status: "gone" }
  | ClientFailure;

export type ClientLiveMode = false | "long-poll" | "sse";

export interface ReadStreamOptions extends ClientRequestOptions {
  /** After-exclusive token. Omission means the start of the stream. */
  offset?: StreamOffset;
  /** false is catch-up only. Live modes continue until EOF, cancel, or error. */
  live?: ClientLiveMode;
}

export interface StreamBatchMeta {
  /** Pass this unchanged as the next read's offset. */
  offset: StreamOffset;
  cursor?: string;
  upToDate: boolean;
  streamClosed: boolean;
}

export interface JsonStreamBatch<T extends JsonValue = JsonValue> extends StreamBatchMeta {
  kind: "json";
  items: readonly T[];
}

export interface TextStreamBatch extends StreamBatchMeta {
  kind: "text";
  text: string;
}

export interface ByteStreamBatch extends StreamBatchMeta {
  kind: "bytes";
  data: Uint8Array;
}

export type StreamBatch<T extends JsonValue = JsonValue> =
  | JsonStreamBatch<T>
  | TextStreamBatch
  | ByteStreamBatch;

/** Terminal outcome of a read session. Delivered through `done`, never thrown. */
export type ReadEndResult = { status: "done" } | { status: "cancelled" } | ClientFailure;

/**
 * A cancellable session of content-aware batches. Iteration yields batches and
 * NEVER throws for operational failures; the terminal outcome is a result
 * object resolved through {@link StreamReadSession.done}.
 */
export interface StreamReadSession<T extends JsonValue = JsonValue> extends AsyncIterable<
  StreamBatch<T>
> {
  readonly contentType?: string;
  readonly startOffset?: StreamOffset;
  /** Latest batch metadata after that batch has been delivered. */
  readonly offset: StreamOffset;
  readonly cursor?: string;
  readonly upToDate: boolean;
  readonly streamClosed: boolean;
  cancel(reason?: unknown): void;
  /** Always resolves, never rejects. */
  readonly done: Promise<ReadEndResult>;
}

export interface StreamProtocolHandle {
  readonly id: string;
  head(options?: ClientRequestOptions): Promise<ClientHeadResult>;
  /** Create-only. An existing stream is a `conflict`, never success. */
  create(options?: CreateStreamOptions): Promise<ClientCreateResult>;
  /** `contentType` is required in `options` where the substrate requires it. */
  append(data: Uint8Array | string, options?: AppendStreamOptions): Promise<ClientAppendResult>;
  /** Permanently close the stream, optionally appending finalData atomically. */
  close(options?: CloseStreamOptions): Promise<ClientCloseResult>;
  read<T extends JsonValue = JsonValue>(options?: ReadStreamOptions): Promise<ClientReadResult<T>>;
}

export interface StreamProtocolClient {
  /** Cold handle creation: no I/O. Repeated calls need not return the same object. */
  stream(streamId: string): StreamProtocolHandle;
  /** Cancel all sessions created by this client and reject future operations. */
  close(reason?: unknown): Promise<void>;
}

export interface StreamsyProtocolClient extends StreamProtocolClient {
  /** Exact local Streamsy semantics; absent from the official adapter. */
  readonly streamsy: StreamProtocolFactory;
}
