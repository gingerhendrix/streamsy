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
  ListMessagesOptions,
  Offset,
  ProducerState,
  StoredMessage,
  StreamEventType,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
  WaitForEventOptions,
  WaitForEventResult,
} from "./storage.ts";

// Read surface used by core to build plans and serve reads.
export interface StreamReader {
  getRecord(): Promise<StreamRecord | null>;
  listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]>;
  getProducerState(producerId: string): Promise<ProducerState | undefined>;
}

export interface AfterCommitEffects {
  notify?: StreamEventType;
  scheduleExpiryAt?: number;
  cancelExpiry?: boolean;
}

export interface MutationPlan {
  createRecord?: StreamRecord;
  preconditions: {
    expectedOffset?: Offset;
    expectedClosed?: boolean;
    producer?: {
      producerId: string;
      expected?: ProducerState;
      next: ProducerState;
    };
  };
  appendMessages?: StoredMessage[];
  recordPatch?: StreamRecordPatch;
  afterCommit?: AfterCommitEffects;
}

export type CommitResult =
  | { status: "committed"; record: StreamRecord }
  | { status: "precondition-failed"; record: StreamRecord | null };

export interface CreatePlan {
  record: StreamRecord;
  initialMessages?: StoredMessage[];
  closeAfter?: boolean;
  afterCommit?: AfterCommitEffects;
}

export type CreateCommit =
  | { status: "created"; record: StreamRecord }
  | { status: "exists"; record: StreamRecord };

export interface ForkPlan {
  child: StreamRecord;
  sourceId: StreamId;
  initialMessages?: StoredMessage[];
  precondition: { sourceLiveAtOffset: Offset };
  afterCommit?: AfterCommitEffects;
}

export type ForkCommit =
  | { status: "created"; record: StreamRecord }
  | { status: "exists" }
  | { status: "fork-source-gone" };

export interface DeletePlan {
  streamId: StreamId;
  reason: "delete" | "expiry";
  afterCommit?: AfterCommitEffects;
}

export type DeleteCommit =
  | { status: "purged" }
  | { status: "retained-soft-deleted" }
  | { status: "not-found" }
  | { status: "gone" };

export interface StreamMutator {
  commit(plan: MutationPlan): Promise<CommitResult>;
}

/** Per-stream live-read notification hub. */
export interface StreamEventHub {
  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult>;
  notify(type: StreamEventType): Promise<void> | void;
}

/** Per-stream active expiry scheduler. */
export interface StreamExpiryScheduler {
  scheduleExpiry(at: number): Promise<void> | void;
  cancelExpiry(): Promise<void> | void;
}

/**
 * Protocol-facing stream. Represents one stream identified by `id` and exposes
 * all protocol storage behavior as direct methods. Adapter-private store
 * composition must stay behind this object.
 */
export interface Stream extends StreamReader, StreamMutator, StreamEventHub, StreamExpiryScheduler {
  readonly id: StreamId;
}

/**
 * Storage factory. Maps a public stream id to a storage-bound `Stream`.
 * Lookup, routing, and dependency composition are factory-owned concerns
 * and are not exposed on the returned `Stream`.
 */
export interface StreamFactory {
  getStream(streamId: StreamId): Promise<Stream>;
  create(plan: CreatePlan): Promise<CreateCommit>;
  fork?(plan: ForkPlan): Promise<ForkCommit>;
  delete(plan: DeletePlan): Promise<DeleteCommit>;
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
