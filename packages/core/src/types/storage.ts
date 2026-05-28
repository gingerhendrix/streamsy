/**
 * Simplified storage boundary for Streamsy.
 *
 * Adapters persist facts and may provide runtime capabilities (locks,
 * notification, expiration scheduling). Protocol/lifecycle policy lives in core.
 */
export type StreamId = string;
export type Offset = string;

export interface StoredMessage {
  data: Uint8Array;
  offset: Offset;
  timestamp: number;
}

export interface StreamConfig {
  contentType: string;
  ttlSeconds?: number;
  expiresAt?: string;
  createdAt: number;
}

export interface StreamLifecycleState {
  lastSeq?: string;
  closed?: boolean;
  closedAt?: number;
  forkedFrom?: StreamId;
  forkOffset?: Offset;
  childRefCount: number;
  softDeleted?: boolean;
  /**
   * Effective expiration deadline in epoch ms. Core updates this on create
   * and on TTL touches; lazy `expireIfNeeded()` reads it. Absent when the
   * stream has neither `ttlSeconds` nor `expiresAt`.
   */
  expiresAtMs?: number;
}

export interface StreamRecord {
  id: StreamId;
  config: StreamConfig;
  lifecycle: StreamLifecycleState;
  currentOffset: Offset;
  counter: number;
}

export interface StreamRecordPatch {
  config?: Partial<StreamConfig>;
  lifecycle?: Partial<StreamLifecycleState>;
  currentOffset?: Offset;
  counter?: number;
}

export type CreateStreamRecordResult =
  | { status: "created" }
  | { status: "exists"; record: StreamRecord };

export interface ProducerState {
  epoch: number;
  lastSeq: number;
}

export interface ListMessagesOptions {
  after?: Offset;
  until?: Offset;
  limit?: number;
}

export type StreamEventType = "message" | "closed" | "deleted" | "soft-deleted";

export interface WaitForEventOptions {
  timeoutMs: number;
  signal?: AbortSignal;
}

export type WaitForEventResult =
  | { status: "notified"; type?: StreamEventType }
  | { status: "timeout" }
  | { status: "aborted" };

export interface Clock {
  now(): number;
  date(value?: number | string): Date;
}
