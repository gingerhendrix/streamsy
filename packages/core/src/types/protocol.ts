/**
 * Protocol Layer Types
 *
 * Types for the business logic layer that handles validation,
 * JSON mode processing, cursor generation, and orchestration.
 */

import type { StoredMessage, StreamStorage } from "./storage.ts";

// === Protocol Inputs ===

export interface CreateOptions {
  contentType?: string;
  ttlSeconds?: number;
  expiresAt?: string;
  initialData?: Uint8Array;
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

export interface CreateResult {
  status: "created" | "exists" | "conflict";
  nextOffset: string;
  contentType: string;
}

export type AppendResult =
  | {
      status: "appended";
      nextOffset: string;
      producerEpoch?: number;
      producerSeq?: number;
    }
  | {
      status: "duplicate";
      nextOffset: string;
      producerEpoch: number;
      producerSeq: number;
    }
  | { status: "not-found" }
  | { status: "conflict"; conflictReason: "content-type" | "sequence" }
  | { status: "stale-epoch"; currentEpoch: number }
  | { status: "producer-gap"; expectedSeq: number; receivedSeq: number }
  | { status: "invalid-epoch-seq" };

export interface ReadResult {
  status: "ok" | "not-found" | "gone";
  messages: StoredMessage[];
  nextOffset: string;
  upToDate: boolean;
}

export interface ReadLiveResult {
  status: "ok" | "timeout" | "not-found";
  messages: StoredMessage[];
  nextOffset: string;
  upToDate: boolean;
  cursor: string;
}

export interface MetadataResult {
  status: "ok" | "not-found";
  contentType?: string;
  nextOffset?: string;
  ttlSeconds?: number;
  expiresAt?: string;
}

export interface DeleteResult {
  status: "ok" | "not-found";
}

// === Storage Factory Type ===

export type StorageFactory = (streamId: string) => StreamStorage;

// === Protocol Interface ===

/**
 * Protocol Layer Interface
 *
 * Handles validation, JSON mode processing, cursor generation,
 * and orchestration between HTTP and storage layers.
 *
 * All methods take streamId as first parameter to identify the stream.
 */
export interface StreamProtocolInterface {
  create(streamId: string, options: CreateOptions): Promise<CreateResult>;
  append(streamId: string, options: AppendOptions): Promise<AppendResult>;
  read(streamId: string, options: ReadOptions): Promise<ReadResult>;
  readLive(streamId: string, options: ReadLiveOptions): Promise<ReadLiveResult>;
  metadata(streamId: string): Promise<MetadataResult>;
  delete(streamId: string): Promise<DeleteResult>;
}
