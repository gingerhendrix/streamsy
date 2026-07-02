/**
 * Storage adapter seam.
 *
 * The protocol-facing seam is a single flat {@link StorageAdapter}: every
 * per-stream method takes `streamId` as its first argument, and the lifecycle
 * intents (`create`/`fork`/`delete`) take a plan carrying the id. Nothing
 * lifetime-bearing or non-serializable crosses the seam — there is no returned
 * per-stream handle. Core builds a thin `BoundStream` view on its own side
 * (`bindStream`) for ergonomic per-stream call sites.
 *
 * Facet interfaces (`StreamReader` / `StreamAppender` / `StreamLiveWaiter` /
 * `StreamExpiryScheduler`) are `streamId`-first grouping interfaces that
 * `StorageAdapter extends`, preserving the documentation structure.
 *
 * This file only declares types/results. The adapter-level result unions carry
 * a `Storage` prefix (`StorageAppendResult` / `StorageCreateResult` /
 * `StorageForkResult` / `StorageDeleteResult`) so they are root-exported without
 * colliding with the protocol-level `AppendResult` / `CreateResult` /
 * `DeleteResult`, giving adapter authors one importable name per type.
 */
import type {
  AwaitChangeOptions,
  AwaitChangeResult,
  ListMessagesOptions,
  Offset,
  ProducerState,
  StoredMessage,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
} from "./storage.ts";

// Read surface used by core to build plans and serve reads (streamId-first).
export interface StreamReader {
  getRecord(streamId: StreamId): Promise<StreamRecord | null>;
  listMessages(streamId: StreamId, options?: ListMessagesOptions): Promise<StoredMessage[]>;
  getProducerState(streamId: StreamId, producerId: string): Promise<ProducerState | undefined>;
}

/**
 * Streamsy's real atomic write: messages + record advance + optional producer
 * compare-and-set + preconditions, in one transaction. `recordPatch` is required.
 * A message-bearing append advances `currentOffset` / `counter` (a pure close folds
 * into the same patch via `lifecycle.closed`); a lifecycle-only TTL renewal
 * (`ExpiryPolicy.touch`) is the one append shape that patches the record without
 * advancing offset/counter. Closure-free and serializable.
 */
export interface AppendPlan {
  preconditions: {
    expectedOffset?: Offset;
    expectedClosed?: boolean;
    /**
     * Producer compare-and-set. An **absent** `expected` means "the producer
     * must not exist yet" (insert-if-absent) — it does NOT mean "don't check,
     * just set". Every implementation must fail the append with reason
     * `"producer"` when `expected` is absent but a state already exists.
     */
    producer?: {
      producerId: string;
      expected?: ProducerState;
      next: ProducerState;
    };
  };
  messages?: StoredMessage[];
  recordPatch: StreamRecordPatch;
}

/**
 * `reason` is required on `precondition-failed` and names the precondition that
 * tripped. When multiple preconditions fail simultaneously, attribute in the
 * order **offset → closed → producer**. Backends with opaque conditional writes
 * may derive `reason` best-effort from a post-failure re-read under concurrency
 * (report `"offset"` when the failure cannot be attributed, e.g. the record was
 * concurrently purged); single-writer attribution must be exact — the contract
 * kit tests exactly that case. Core's retry loop re-plans from `record` and does
 * not branch on `reason`; it exists for adapter diagnostics and kit sharpness.
 */
export type StorageAppendResult =
  | { status: "appended"; record: StreamRecord }
  | {
      status: "precondition-failed";
      record: StreamRecord | null;
      reason: "offset" | "closed" | "producer";
    };

export interface StreamAppender {
  append(streamId: StreamId, plan: AppendPlan): Promise<StorageAppendResult>;
}

/**
 * `record` is the single source of truth for the created stream, including a
 * created-closed stream (`record.lifecycle.closed` / `closedAt` are pre-folded
 * by core). Adapters persist the record as given; there is no separate
 * close-after step.
 */
export interface CreatePlan {
  record: StreamRecord;
  initialMessages?: StoredMessage[];
}

export type StorageCreateResult =
  | { status: "created"; record: StreamRecord }
  | { status: "exists"; record: StreamRecord };

export interface ForkPlan {
  child: StreamRecord;
  sourceId: StreamId;
  initialMessages?: StoredMessage[];
  /**
   * Source liveness check: the fork proceeds only if the source exists, is not
   * soft-deleted, and its `currentOffset` is `>=` `sourceLiveAtOffset`
   * (lexicographic offset comparison). Its only failure is `fork-source-gone`.
   */
  precondition: { sourceLiveAtOffset: Offset };
}

/**
 * `exists` carries the existing child record so core can run the same
 * config-match idempotency it applies to `create` — a byte-identical racing
 * fork resolves as idempotent success rather than a bogus conflict.
 */
export type StorageForkResult =
  | { status: "created"; record: StreamRecord }
  | { status: "exists"; record: StreamRecord }
  | { status: "fork-source-gone" };

export interface DeletePlan {
  streamId: StreamId;
  reason: "delete" | "expiry";
}

export type StorageDeleteResult =
  | { status: "purged" }
  | { status: "retained-soft-deleted" }
  | { status: "not-found" }
  | { status: "gone" };

/**
 * Per-stream level-triggered live waiter. Serializable in and out: the argument
 * and result carry only plain data, so the seam crosses a Durable Object RPC (or
 * any future remote adapter) boundary unchanged.
 *
 * Required: every adapter implements `awaitChange`. A backend that can wake
 * cheaply does so directly; one that cannot implements it by polling its own
 * durable reads. Core wires no polling fallback in silently, but it exports the
 * contract-faithful loop — `runAwaitChangeLoop` — plus the shared
 * `buildChangeSnapshot` / `changeSnapshotDiffers` primitives, so an adapter
 * supplies only `readRecord` + `waitForWake` (a minimal polling adapter's
 * `waitForWake` is a sleep) and stays faithful to the level-triggered contract.
 */
export interface StreamLiveWaiter {
  awaitChange(streamId: StreamId, options: AwaitChangeOptions): Promise<AwaitChangeResult>;
}

/** Per-stream active expiry scheduler. */
export interface StreamExpiryScheduler {
  scheduleExpiry(streamId: StreamId, at: number): Promise<void> | void;
  cancelExpiry(streamId: StreamId): Promise<void> | void;
}

/**
 * The storage backend an adapter author implements: one flat, fully serializable
 * interface. Per-stream methods take `streamId` first; lifecycle intents carry
 * the id in their plan. Adapters keep a private per-stream handle and delegate to
 * it internally.
 */
export interface StorageAdapter
  extends StreamReader, StreamAppender, StreamLiveWaiter, StreamExpiryScheduler {
  create(plan: CreatePlan): Promise<StorageCreateResult>;
  fork?(plan: ForkPlan): Promise<StorageForkResult>;
  delete(plan: DeletePlan): Promise<StorageDeleteResult>;
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
