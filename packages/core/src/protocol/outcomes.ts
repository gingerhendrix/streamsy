export interface NotSupportedResult {
  status: "not-supported";
  feature: string;
  message?: string;
}
/**
 * One read message as the protocol exposes it.
 *
 * The read contract carries message payloads and batch-level metadata only.
 * Per-message offsets and timestamps stay in storage: no reader consumes them,
 * and a pure Durable Streams HTTP response cannot express them.
 */
export interface ReadMessage {
  readonly data: Uint8Array;
}
export type CreateConflictReason =
  | "config-mismatch"
  | "soft-deleted"
  | "fork-content-type"
  | "fork-source-soft-deleted"
  | "fork-copy-limit";

type CreateFailureResult =
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

/** Transport-neutral classifications; no bound handle is attached. */
export type CreateOutcome =
  | { status: "created"; nextOffset: string; contentType: string; closed?: boolean }
  | { status: "exists"; nextOffset: string; contentType: string; closed?: boolean }
  | CreateFailureResult;

export type AppendConflictReason = "content-type" | "sequence" | "closed" | "expected-offset";

export type AppendOutcome =
  | {
      status: "appended";
      /**
       * Exact stream offset after this append: the offset of the last message
       * written by it (for a close-only append with no body, the unchanged
       * tail offset). This is the write-acknowledgement token — a reader or
       * mirror that has passed `offset` has observed this write. Because reads
       * are after-exclusive, it is also the read cursor.
       */
      offset: string;
      producerEpoch?: number;
      producerSeq?: number;
      closed?: boolean;
    }
  | {
      status: "duplicate";
      /**
       * Current tail offset at acknowledgement time. The originally appended
       * message sits at or before `offset`, so it remains a valid
       * write-acknowledgement token for sync ("synced once your mirror passes
       * offset X").
       */
      offset: string;
      producerEpoch: number;
      producerSeq: number;
      closed?: boolean;
    }
  | { status: "not-found" }
  | { status: "gone" }
  | { status: "conflict"; conflictReason: "closed"; offset: string; closed: true }
  | {
      status: "conflict";
      conflictReason: "expected-offset";
      /**
       * Actual tail offset at the time the `expectedOffset` precondition
       * failed, so callers can see how far behind they were. Retry loops
       * should re-read state and rebuild the append from the new tail.
       */
      offset: string;
    }
  | { status: "conflict"; conflictReason: "content-type" | "sequence" }
  | { status: "busy" }
  | { status: "stale-epoch"; currentEpoch: number }
  | { status: "producer-gap"; expectedSeq: number; receivedSeq: number }
  | { status: "invalid-epoch-seq" }
  | NotSupportedResult;

export type ReadOutcome =
  | {
      status: "ok";
      messages: ReadMessage[];
      nextOffset: string;
      upToDate: boolean;
      closed?: boolean;
    }
  | { status: "not-found" }
  | { status: "gone" };

export type ReadNextOutcome =
  | {
      status: "ok" | "timeout" | "not-found" | "gone";
      messages: ReadMessage[];
      nextOffset: string;
      upToDate: boolean;
      cursor: string;
      closed?: boolean;
    }
  | NotSupportedResult;

export type HeadOutcome =
  | {
      status: "ok";
      contentType: string;
      nextOffset: string;
      ttlSeconds?: number;
      expiresAt?: string;
      closed?: boolean;
    }
  | { status: "not-found" }
  | { status: "gone" };

export type RemoveOutcome =
  | { status: "ok" }
  | { status: "not-found" }
  | { status: "gone" }
  | { status: "busy" };
