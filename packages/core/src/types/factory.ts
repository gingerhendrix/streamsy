/**
 * Factory / composed-stream storage seam.
 *
 * The protocol-facing seam is a {@link StreamFactory} returning {@link Stream}
 * instances bound to one stream id. A `Stream` implements the protocol-facing
 * record/message operations directly. Optional behaviour (producer state,
 * fork reference counts, mutation gates, live-read events, active expiry) is
 * surfaced as additional members on the returned `Stream`. When a feature is
 * unavailable, protocol methods covering it should return a structured
 * {@link NotSupportedResult}.
 *
 * This file only declares types/results. The compatibility composer that
 * wraps the existing {@link StreamStoreAdapter} into a `StreamFactory` lives
 * in `../factory/`.
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

/** Bound producer-state store for a single stream. Optional. */
export interface StreamProducerStore {
  getProducerState(producerId: string): Promise<ProducerState | undefined>;
  setProducerState(producerId: string, state: ProducerState): Promise<void>;
  deleteProducerStates(): Promise<void>;
}

/** Bound parent/child reference tracker for a single stream. Optional. */
export interface StreamReferenceTracker {
  incrementChildRefCount(): Promise<number>;
  decrementChildRefCount(): Promise<number>;
}

/** Per-stream mutation coordinator (lock-like). Optional. */
export interface StreamMutationCoordinator {
  withMutationLock<T>(fn: () => Promise<T>): Promise<T>;
}

/** Per-stream live-read notification hub. Optional. */
export interface StreamEventHub {
  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult>;
  notify(type: StreamEventType): Promise<void> | void;
}

/** Per-stream active expiry scheduler. Optional. */
export interface StreamExpiryScheduler {
  scheduleExpiry(at: number, callback?: () => Promise<void>): Promise<void> | void;
  cancelExpiry(): Promise<void> | void;
}

/**
 * Protocol-facing stream. Represents one stream identified by `id` and
 * implements the record/message operations directly. Optional behaviour is
 * surfaced as additional members; missing optional members signal that the
 * adapter does not support that behaviour for this stream. There is no
 * public dependency bag on a `Stream`: composition with backing stores is a
 * factory implementation detail handled by `composeStream`.
 */
export interface Stream extends StreamRecordStore, StreamMessageStore {
  readonly id: StreamId;
  readonly producers?: StreamProducerStore;
  readonly references?: StreamReferenceTracker;
  readonly mutations?: StreamMutationCoordinator;
  readonly events?: StreamEventHub;
  readonly expiry?: StreamExpiryScheduler;
}

/**
 * Adapter factory. Maps a public stream id to a `Stream` bound to that id.
 * Lookup, routing, and dependency composition are factory-owned concerns
 * and are not exposed on the returned `Stream`.
 */
export interface StreamFactory {
  getStream(streamId: StreamId): Promise<Stream> | Stream;
}

/**
 * Dependencies that adapter authors compose into a `Stream`. All
 * dependencies must already be bound to the same stream id.
 */
export interface ComposedStreamDeps {
  id: StreamId;
  recordStore: StreamRecordStore;
  messageStore: StreamMessageStore;
  producerStore?: StreamProducerStore;
  referenceTracker?: StreamReferenceTracker;
  mutations?: StreamMutationCoordinator;
  events?: StreamEventHub;
  expiry?: StreamExpiryScheduler;
}

/**
 * Structured "feature not supported" protocol result. Protocol methods that
 * cover optional behaviour return this when the active adapter does not
 * implement the requested behaviour for the target stream. HTTP handlers
 * map this to a 4xx response (see `notSupportedResponse`).
 *
 * `feature` is a machine-readable identifier such as `"producer-idempotency"`,
 * `"fork"`, `"live-read"`, or `"active-expiry"`. `message` is an optional
 * human-readable detail intended for response bodies and logs.
 */
export interface NotSupportedResult {
  status: "not-supported";
  feature: string;
  message?: string;
}

/** Convenience constructor for a {@link NotSupportedResult}. */
export function notSupported(feature: string, message?: string): NotSupportedResult {
  return message === undefined
    ? { status: "not-supported", feature }
    : { status: "not-supported", feature, message };
}

/** Type guard for protocol results that may be a {@link NotSupportedResult}. */
export function isNotSupported(value: unknown): value is NotSupportedResult {
  if (typeof value !== "object" || value === null) return false;
  const status = (value as { status?: unknown }).status;
  return status === "not-supported";
}
