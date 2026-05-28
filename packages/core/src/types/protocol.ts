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

export type CreateResult =
  | {
      status: "created" | "exists";
      stream: ProtocolStream;
      nextOffset: string;
      contentType: string;
      closed?: boolean;
    }
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

export type AppendResult =
  | {
      status: "appended";
      nextOffset: string;
      producerEpoch?: number;
      producerSeq?: number;
      closed?: boolean;
    }
  | {
      status: "duplicate";
      nextOffset: string;
      producerEpoch: number;
      producerSeq: number;
      closed?: boolean;
    }
  | { status: "not-found" }
  | { status: "gone" }
  | {
      status: "conflict";
      conflictReason: "content-type" | "sequence" | "closed";
      nextOffset?: string;
      closed?: boolean;
    }
  | { status: "stale-epoch"; currentEpoch: number }
  | { status: "producer-gap"; expectedSeq: number; receivedSeq: number }
  | { status: "invalid-epoch-seq" }
  | NotSupportedResult;

export interface ReadResult {
  status: "ok" | "not-found" | "gone";
  messages: StoredMessage[];
  nextOffset: string;
  upToDate: boolean;
  closed?: boolean;
}

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

export interface MetadataResult {
  status: "ok" | "not-found" | "gone";
  contentType?: string;
  nextOffset?: string;
  ttlSeconds?: number;
  expiresAt?: string;
  closed?: boolean;
}

export interface DeleteResult {
  status: "ok" | "not-found" | "gone";
}

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
