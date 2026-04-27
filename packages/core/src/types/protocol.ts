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

export interface CreateResult {
  status: "created" | "exists" | "conflict" | "not-found" | "bad-request";
  nextOffset: string;
  contentType: string;
  closed?: boolean;
  conflictReason?: CreateConflictReason;
  errorMessage?: string;
}

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
  | { status: "conflict"; conflictReason: "content-type" | "sequence" | "closed"; nextOffset?: string; closed?: boolean }
  | { status: "stale-epoch"; currentEpoch: number }
  | { status: "producer-gap"; expectedSeq: number; receivedSeq: number }
  | { status: "invalid-epoch-seq" };

export interface ReadResult {
  status: "ok" | "not-found" | "gone";
  messages: StoredMessage[];
  nextOffset: string;
  upToDate: boolean;
  closed?: boolean;
}

export interface ReadLiveResult {
  status: "ok" | "timeout" | "not-found" | "gone";
  messages: StoredMessage[];
  nextOffset: string;
  upToDate: boolean;
  cursor: string;
  closed?: boolean;
}

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
