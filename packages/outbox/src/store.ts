import { Context, Data, type Effect, Schema } from "effect";
import type { Option } from "effect";
import type { StoreFailure } from "./errors.ts";

export const EntryState = Schema.Literals(["pending", "delivered", "dead"]);

export type EntryState = typeof EntryState.Type;

export const DeadLetterReason = Schema.Literals([
  "attempts-exhausted",
  "permanent",
  "payload-poison",
]);

export type DeadLetterReason = typeof DeadLetterReason.Type;

export interface OutboxEntry {
  readonly id: number;
  readonly name: string;
  readonly partitionId: string;
  readonly idempotencyKey: string;
  /** The stored JSON text. `Delivery.payload` is the decoded value. */
  readonly encodedPayload: string;
  readonly state: EntryState;
  /** Persisted outcomes, not the number of external calls started. */
  readonly attempts: number;
  readonly nextAttemptAtMs: number;
  readonly lastError: string | undefined;
  readonly deadLetterReason: DeadLetterReason | undefined;
  readonly enqueuedAtMs: number;
  readonly settledAtMs: number | undefined;
}

export interface OutboxDraft {
  readonly name: string;
  readonly partitionId: string;
  readonly idempotencyKey: string;
  readonly encodedPayload: string;
  readonly enqueuedAtMs: number;
}

export interface EnqueueReport {
  readonly enqueued: number;
  readonly absorbed: number;
}

/** Entries of one name. An omitted partition selects every partition. */
export interface OutboxScope {
  readonly name: string;
  readonly partitionId?: string;
}

/** A scope for due reads. Ordered scopes block behind their oldest pending entry. */
export interface DueScope extends OutboxScope {
  readonly ordered?: boolean;
}

/** The recorded outcome of one attempt. `attempts` is the new persisted count. */
export type Settlement = Data.TaggedEnum<{
  Delivered: { readonly attempts: number; readonly atMs: number };
  Retry: { readonly attempts: number; readonly nextAttemptAtMs: number; readonly detail: string };
  Dead: {
    readonly attempts: number;
    readonly reason: DeadLetterReason;
    readonly detail: string;
    readonly atMs: number;
  };
}>;

export const Settlement = Data.taggedEnum<Settlement>();

/**
 * Adapter protocol. Reads acquire no distributed ownership or lease.
 * Only `Outbox.enqueue` and `Outbox.drain` call `enqueue`, `readDue` and `settle`.
 * Applications use `nextDue` and `list`.
 */
export interface OutboxBacking {
  readonly enqueue: (drafts: readonly OutboxDraft[]) => Effect.Effect<EnqueueReport, StoreFailure>;
  readonly nextDue: (scope: DueScope) => Effect.Effect<Option.Option<number>, StoreFailure>;
  /** Ordered reads include the future head; the runtime stops there. */
  readonly readDue: (
    scope: DueScope,
    nowMs: number,
    limit: number,
  ) => Effect.Effect<readonly OutboxEntry[], StoreFailure>;
  readonly settle: (id: number, settlement: Settlement) => Effect.Effect<void, StoreFailure>;
  readonly list: (scope: OutboxScope) => Effect.Effect<readonly OutboxEntry[], StoreFailure>;
}

export class OutboxStore extends Context.Service<OutboxStore, OutboxBacking>()(
  "streamsy/outbox/OutboxStore",
) {}
