/**
 * Protocol layer types.
 *
 * The public protocol is factory-shaped: callers create or look up a
 * protocol-bound stream, then operate on that bound stream. Storage-bound
 * streams live in `types/factory.ts`; this file describes protocol-facing
 * objects and result shapes.
 */

import type { NotSupportedResult } from "./factory.ts";
import type { StoredMessage, StreamId } from "./storage.ts";

// === Protocol Inputs ===

export interface CreateOptions {
  contentType?: string;
  ttlSeconds?: number;
  expiresAt?: string;
  initialData?: Uint8Array;
  closed?: boolean;
  forkedFrom?: string;
  forkOffset?: string;
}

export interface ProducerOptions {
  producerId: string;
  producerEpoch: number;
  producerSeq: number;
}

export interface AppendOptions {
  data: Uint8Array;
  contentType: string;
  seq?: string;
  producer?: ProducerOptions;
  close?: boolean;
  /**
   * Optimistic-concurrency precondition: append only if the stream's tail
   * offset still equals this offset. On mismatch the append fails with a
   * `conflict`/`expected-offset` result carrying the actual tail, and no
   * state (messages, close flag, producer state) is changed. `ZERO_OFFSET`
   * means "append only if the stream is still empty". Streamsy extension to
   * the Durable Streams protocol.
   */
  expectedOffset?: string;
}

export interface ReadOptions {
  offset?: string;
}

export interface ReadLiveOptions {
  offset: string;
  mode: "long-poll" | "sse";
  cursor?: string;
  signal?: AbortSignal;
}

// === Protocol Outputs ===

export type CreateConflictReason =
  | "config-mismatch"
  | "soft-deleted"
  | "fork-content-type"
  | "fork-source-soft-deleted";

type CreateFailureResult =
  | {
      status: "conflict";
      nextOffset: string;
      contentType: string;
      conflictReason?: CreateConflictReason;
      errorMessage?: string;
    }
  | {
      status: "not-found" | "bad-request";
      nextOffset: string;
      contentType: string;
      errorMessage?: string;
    }
  | NotSupportedResult;

/**
 * Create/fork result produced by the protocol services, before the bound
 * protocol stream is attached. The protocol factory grafts the resolved
 * `stream` onto success outcomes to produce the public {@link CreateResult}.
 *
 * `created` and `exists` are kept as separate members (rather than a combined
 * `"created" | "exists"` discriminant) so callers can narrow either status and
 * still eliminate the other union members.
 */
export type CreateOutcome =
  | { status: "created"; nextOffset: string; contentType: string; closed?: boolean }
  | { status: "exists"; nextOffset: string; contentType: string; closed?: boolean }
  | CreateFailureResult;

export type CreateResult =
  | {
      status: "created";
      stream: ProtocolStream;
      nextOffset: string;
      contentType: string;
      closed?: boolean;
    }
  | {
      status: "exists";
      stream: ProtocolStream;
      nextOffset: string;
      contentType: string;
      closed?: boolean;
    }
  | CreateFailureResult;

export type AppendConflictReason = "content-type" | "sequence" | "closed" | "expected-offset";

export type AppendResult =
  | {
      status: "appended";
      /**
       * Exact stream offset after this append: the offset of the last message
       * written by it (for a close-only append with no body, the unchanged
       * tail offset). This is the write-acknowledgement token — a reader or
       * mirror that has passed `offset` has observed this write. Because reads
       * are after-exclusive, it is also the read cursor.
       */
      offset: string;
      producerEpoch?: number;
      producerSeq?: number;
      closed?: boolean;
    }
  | {
      status: "duplicate";
      /**
       * Current tail offset at acknowledgement time. The originally appended
       * message sits at or before `offset`, so it remains a valid
       * write-acknowledgement token for sync ("synced once your mirror passes
       * offset X").
       */
      offset: string;
      producerEpoch: number;
      producerSeq: number;
      closed?: boolean;
    }
  | { status: "not-found" }
  | { status: "gone" }
  | { status: "conflict"; conflictReason: "closed"; offset: string; closed: true }
  | {
      status: "conflict";
      conflictReason: "expected-offset";
      /**
       * Actual tail offset at the time the `expectedOffset` precondition
       * failed, so callers can see how far behind they were. Retry loops
       * should re-read state and rebuild the append from the new tail.
       */
      offset: string;
    }
  | { status: "conflict"; conflictReason: "content-type" | "sequence" }
  | { status: "busy" }
  | { status: "stale-epoch"; currentEpoch: number }
  | { status: "producer-gap"; expectedSeq: number; receivedSeq: number }
  | { status: "invalid-epoch-seq" }
  | NotSupportedResult;

export type ReadResult =
  | {
      status: "ok";
      messages: StoredMessage[];
      nextOffset: string;
      upToDate: boolean;
      closed?: boolean;
    }
  | { status: "not-found" }
  | { status: "gone" };

export type ReadLiveResult =
  | {
      status: "ok" | "timeout" | "not-found" | "gone";
      messages: StoredMessage[];
      nextOffset: string;
      upToDate: boolean;
      cursor: string;
      closed?: boolean;
    }
  | NotSupportedResult;

export type MetadataResult =
  | {
      status: "ok";
      contentType: string;
      nextOffset: string;
      ttlSeconds?: number;
      expiresAt?: string;
      closed?: boolean;
    }
  | { status: "not-found" }
  | { status: "gone" };

export type DeleteResult = { status: "ok" } | { status: "not-found" } | { status: "gone" };

// === Protocol-bound stream and factory ===

export interface ProtocolStream {
  readonly id: StreamId;
  append(options: AppendOptions): Promise<AppendResult>;
  read(options: ReadOptions): Promise<ReadResult>;
  readLive(options: ReadLiveOptions): Promise<ReadLiveResult>;
  metadata(): Promise<MetadataResult>;
  delete(): Promise<DeleteResult>;
}

export type ProtocolGetResult =
  | { status: "ok"; stream: ProtocolStream }
  | { status: "not-found" }
  | { status: "gone" }
  | NotSupportedResult;

export interface StreamProtocolFactory {
  create(streamId: string, options: CreateOptions): Promise<CreateResult>;
  get(streamId: string): Promise<ProtocolGetResult>;
}
