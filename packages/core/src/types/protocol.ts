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
}

export interface AppendOptions {
  data: Uint8Array;
  contentType: string;
  seq?: string;
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

export interface CreateResult {
  status: "created" | "exists" | "conflict";
  nextOffset: string;
  contentType: string;
  closed?: boolean;
}

export interface AppendResult {
  status: "ok" | "conflict" | "not-found";
  nextOffset?: string;
  conflictReason?: "content-type" | "sequence" | "closed";
  closed?: boolean;
}

export interface ReadResult {
  status: "ok" | "not-found" | "gone";
  messages: StoredMessage[];
  nextOffset: string;
  upToDate: boolean;
  closed?: boolean;
}

export interface ReadLiveResult {
  status: "ok" | "timeout" | "not-found";
  messages: StoredMessage[];
  nextOffset: string;
  upToDate: boolean;
  cursor: string;
  closed?: boolean;
}

export interface MetadataResult {
  status: "ok" | "not-found";
  contentType?: string;
  nextOffset?: string;
  ttlSeconds?: number;
  expiresAt?: string;
  closed?: boolean;
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
