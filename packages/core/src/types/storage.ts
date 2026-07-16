/**
 * Simplified storage boundary for Streamsy.
 *
 * Adapters persist facts and may provide runtime capabilities (notification,
 * expiration scheduling). Atomic mutation is expressed through append and
 * lifecycle intents on the storage adapter; protocol/lifecycle policy lives in
 * core.
 */
export type StreamId = string;

/**
 * Opaque, case-sensitive stream position. Within one stream incarnation,
 * generated offsets are unique and strictly increasing under ordinary string
 * comparison. Adapters must store, sort, and window them lexicographically;
 * they must not parse or normalize generator-specific structure.
 */
export type Offset = string;

/**
 * One stored message. `data` is binary (`Uint8Array`) — it survives structured
 * clone (the Durable Object RPC boundary) but is NOT plain JSON; a remote
 * adapter transporting messages over JSON must encode it (e.g. base64).
 */
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
  /**
   * Sub-message boundary inside the source message that follows `forkOffset`.
   * Only recorded when a fork materialized a partial-message prefix (`> 0`).
   * For binary/text it counts bytes within the next source message; for JSON it
   * counts flattened messages/items. Kept purely for fork idempotency/config
   * matching — reads use the materialized prefix stored as the child's own
   * messages, not this value.
   */
  forkSubOffset?: number;
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
  /**
   * Deprecated adapter metadata retained for persisted-record compatibility.
   * Core increments it for message-bearing mutations but never derives or
   * orders offsets from it.
   */
  counter: number;
}

/**
 * **Patches set values; they never delete them.** An absent field means "leave
 * unchanged". Clearing a field is deliberately inexpressible: `undefined` is
 * dropped by JSON but preserved by structured clone, so "set to undefined"
 * would behave differently across transports. Core never clears fields.
 */
export interface StreamRecordPatch {
  config?: Partial<StreamConfig>;
  lifecycle?: Partial<StreamLifecycleState>;
  currentOffset?: Offset;
  counter?: number;
}

export interface ProducerState {
  epoch: number;
  lastSeq: number;
}

export interface ListMessagesOptions {
  after?: Offset;
  until?: Offset;
  limit?: number;
}

/**
 * Observable, serializable snapshot of the change-relevant state of one stream.
 * Every field is a JSON-serializable primitive so the snapshot crosses a Durable
 * Object RPC (or any future remote adapter) boundary unchanged.
 */
export interface StreamChangeSnapshot {
  /** `false` once the record has been purged. */
  present: boolean;
  currentOffset: Offset;
  closed: boolean;
  softDeleted: boolean;
}

/**
 * Serializable, closure-free input to the level-triggered live-wait seam. The
 * caller passes the position/lifecycle state it already observed; the adapter
 * re-reads durable state and decides whether anything relevant advanced.
 */
export interface AwaitChangeOptions {
  /** The offset the live reader is parked at. */
  fromOffset: Offset;
  /** Closed state the caller already observed (treated as `false` when absent). */
  observedClosed?: boolean;
  /** Soft-deleted state the caller already observed (treated as `false` when absent). */
  observedSoftDeleted?: boolean;
  timeoutMs: number;
}

/**
 * Result of {@link AwaitChangeOptions}. There is no `aborted` status:
 * cancellation is caller-local and the adapter never learns about it.
 *
 * `timeout` means "no relevant change observed yet" — it does NOT promise the
 * full `timeoutMs` budget elapsed. Adapters MAY return `timeout` early (bounded
 * parks are encouraged for remote backends, e.g. a Durable Object capping the
 * total wait so a single RPC never strands the actor). Callers MUST re-park on
 * `timeout` rather than assume the budget was consumed.
 */
export type AwaitChangeResult =
  | { status: "changed"; snapshot: StreamChangeSnapshot }
  | { status: "timeout"; snapshot: StreamChangeSnapshot };

export interface Clock {
  now(): number;
  date(value?: number | string): Date;
}
