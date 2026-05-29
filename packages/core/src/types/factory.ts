/**
 * Factory / storage-bound stream seam.
 *
 * The protocol-facing seam is a {@link StreamFactory} returning {@link Stream}
 * instances bound to one stream id. A `Stream` implements every storage method
 * the protocol needs directly. Facet interfaces describe small implementation
 * surfaces for adapters and tests, but they are inherited by `Stream` rather
 * than exposed as nested public capability objects.
 *
 * This file only declares types/results.
 */
import type {
  CreateStreamRecordResult,
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  StreamEventType,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
  WaitForEventOptions,
  WaitForEventResult,
} from "./storage.ts";

/** Bound record store: every method targets the single stream this store represents. */
export interface StreamRecordStore {
  getRecord(): Promise<StreamRecord | null>;
  createRecord(record: StreamRecord): Promise<CreateStreamRecordResult>;
  updateRecord(patch: StreamRecordPatch): Promise<StreamRecord>;
  deleteRecord(): Promise<void>;
}

/** Bound message store for a single stream. */
export interface StreamMessageStore {
  appendMessages(messages: StoredMessage[]): Promise<void>;
  listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]>;
  deleteMessages(): Promise<void>;
}

/** Bound producer-state store for a single stream. */
export interface StreamProducerStore {
  getProducerState(producerId: string): Promise<ProducerState | undefined>;
  setProducerState(producerId: string, state: ProducerState): Promise<void>;
  deleteProducerStates(): Promise<void>;
}

/** Bound parent/child reference tracker for a single stream. */
export interface StreamReferenceTracker {
  incrementChildRefCount(): Promise<number>;
  decrementChildRefCount(): Promise<number>;
}

/** Per-stream mutation coordinator (lock-like). */
export interface StreamMutationCoordinator {
  withMutationLock<T>(fn: () => Promise<T>): Promise<T>;
}

/** Per-stream live-read notification hub. */
export interface StreamEventHub {
  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult>;
  notify(type: StreamEventType): Promise<void> | void;
}

/** Per-stream active expiry scheduler. */
export interface StreamExpiryScheduler {
  scheduleExpiry(at: number, callback?: () => Promise<void>): Promise<void> | void;
  cancelExpiry(): Promise<void> | void;
}

/**
 * Protocol-facing stream. Represents one stream identified by `id` and exposes
 * all protocol storage behavior as direct methods. Adapter-private store
 * composition must stay behind this object.
 */
export interface Stream
  extends
    StreamRecordStore,
    StreamMessageStore,
    StreamProducerStore,
    StreamReferenceTracker,
    StreamMutationCoordinator,
    StreamEventHub,
    StreamExpiryScheduler {
  readonly id: StreamId;
}

/**
 * Storage factory. Maps a public stream id to a storage-bound `Stream`.
 * Lookup, routing, and dependency composition are factory-owned concerns
 * and are not exposed on the returned `Stream`.
 */
export interface StreamFactory {
  getStream(streamId: StreamId): Promise<Stream>;
}

/**
 * Structured "feature not supported" protocol result. Protocol methods map
 * typed storage unsupported behavior to this public result shape.
 */
export interface NotSupportedResult {
  status: "not-supported";
  feature: string;
  message?: string;
}

/** Internal/storage-level unsupported-feature error. */
export class NotSupportedError extends Error {
  constructor(
    readonly feature: string,
    message?: string,
  ) {
    super(message ?? `Feature not supported: ${feature}`);
    this.name = "NotSupportedError";
  }
}

/** Convenience constructor for a {@link NotSupportedResult}. */
export function notSupported(feature: string, message?: string): NotSupportedResult {
  return message === undefined
    ? { status: "not-supported", feature }
    : { status: "not-supported", feature, message };
}

/** Convenience constructor for a storage-level unsupported-feature error. */
export function unsupported(feature: string, message?: string): NotSupportedError {
  return new NotSupportedError(feature, message);
}

/** Type guard for storage-level unsupported-feature errors. */
export function isNotSupportedError(value: unknown): value is NotSupportedError {
  return value instanceof NotSupportedError;
}

/** Convert storage-level unsupported-feature errors to public protocol results. */
export function notSupportedFromError(error: NotSupportedError): NotSupportedResult {
  return notSupported(error.feature, error.message);
}

/** Type guard for protocol results that may be a {@link NotSupportedResult}. */
export function isNotSupported(value: unknown): value is NotSupportedResult {
  if (typeof value !== "object" || value === null) return false;
  const status = (value as { status?: unknown }).status;
  return status === "not-supported";
}
