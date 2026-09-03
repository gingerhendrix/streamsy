/**
 * The durable outbox.
 *
 * An action sink cannot enqueue by holding work in memory: the process that
 * decided to deliver is the same process that can die before delivering. So the
 * decision to deliver is written down first, in the same durable step as the
 * fact that caused it, and delivery is a separate, resumable pass over what was
 * written.
 */
import { Context, Effect, Layer } from "effect";
import { OutboxUnavailable, type DeadLetterReason } from "./errors.ts";

export type OutboxEntryState = "pending" | "delivered" | "dead";

export interface OutboxEntry {
  /** Monotonic enqueue order. Delivery walks a lane in this order. */
  readonly id: number;
  readonly sink: string;
  readonly partitionId: string;
  readonly idempotencyKey: string;
  /** The declared payload, as the JSON text that was durably written. */
  readonly payload: string;
  readonly state: OutboxEntryState;
  /** Attempts already spent. Zero until the first delivery is tried. */
  readonly attempts: number;
  /** The earliest instant this entry may be attempted again. */
  readonly nextAttemptAtMs: number;
  readonly lastError: string | undefined;
  readonly deadLetterReason: DeadLetterReason | undefined;
  readonly enqueuedAtMs: number;
  readonly settledAtMs: number | undefined;
}

/** One decision to deliver, before it has an identity in the outbox. */
export interface OutboxDraft {
  readonly sink: string;
  readonly partitionId: string;
  readonly idempotencyKey: string;
  readonly payload: string;
  readonly enqueuedAtMs: number;
}

export interface OutboxEnqueueReport {
  readonly enqueued: number;
  /** Drafts whose sink and idempotency key were already durable. Never a second effect. */
  readonly absorbed: number;
}

/** The durable storage boundary for one outbox implementation. */
export interface OutboxBacking {
  readonly enqueue: (
    drafts: readonly OutboxDraft[],
  ) => Effect.Effect<OutboxEnqueueReport, OutboxUnavailable>;
  /** Pending entries in one lane whose next attempt instant has arrived, in enqueue order. */
  readonly claimDue: (
    sink: string,
    partitionId: string | undefined,
    nowMs: number,
    limit: number,
  ) => Effect.Effect<readonly OutboxEntry[], OutboxUnavailable>;
  readonly markDelivered: (
    id: number,
    attempts: number,
    atMs: number,
  ) => Effect.Effect<void, OutboxUnavailable>;
  readonly reschedule: (
    id: number,
    attempts: number,
    nextAttemptAtMs: number,
    detail: string,
  ) => Effect.Effect<void, OutboxUnavailable>;
  readonly deadLetter: (
    id: number,
    attempts: number,
    reason: DeadLetterReason,
    detail: string,
    atMs: number,
  ) => Effect.Effect<void, OutboxUnavailable>;
  readonly list: (
    sink: string,
    partitionId: string | undefined,
  ) => Effect.Effect<readonly OutboxEntry[], OutboxUnavailable>;
}

export interface OutboxStoreService extends OutboxBacking {}

export class OutboxStore extends Context.Service<OutboxStore, OutboxStoreService>()(
  "streamsy/action-sink/OutboxStore",
) {}

/** Lift one durable backing into the service every runtime asks for. */
export function outboxStore(backing: OutboxBacking): OutboxStoreService {
  return OutboxStore.of(backing);
}

export const outboxStoreLayer = (backing: OutboxBacking): Layer.Layer<OutboxStore> =>
  Layer.sync(OutboxStore, () => outboxStore(backing));

/**
 * The in-memory backing.
 *
 * It is not a stub. It enforces the same sink-and-key uniqueness and the same
 * lane ordering as SQLite, so a declaration that behaves one way on the memory
 * host behaves the same way on the durable one.
 */
export function makeMemoryOutboxBacking(): OutboxBacking {
  const entries = new Map<number, OutboxEntry>();
  const identities = new Map<string, Set<string>>();
  let nextId = 1;

  const replace = (id: number, patch: Partial<OutboxEntry>): void => {
    const existing = entries.get(id);
    if (existing === undefined) throw new Error(`outbox entry ${id} does not exist`);
    entries.set(id, { ...existing, ...patch });
  };
  const ordered = (): readonly OutboxEntry[] =>
    [...entries.values()].toSorted((left, right) => left.id - right.id);

  return {
    enqueue: (drafts) =>
      Effect.sync(() => {
        let enqueued = 0;
        let absorbed = 0;
        for (const draft of drafts) {
          const sinkIdentities = identities.get(draft.sink);
          if (sinkIdentities?.has(draft.idempotencyKey) === true) {
            absorbed += 1;
            continue;
          }
          const id = nextId;
          nextId += 1;
          if (sinkIdentities === undefined)
            identities.set(draft.sink, new Set([draft.idempotencyKey]));
          else sinkIdentities.add(draft.idempotencyKey);
          entries.set(id, {
            id,
            sink: draft.sink,
            partitionId: draft.partitionId,
            idempotencyKey: draft.idempotencyKey,
            payload: draft.payload,
            state: "pending",
            attempts: 0,
            nextAttemptAtMs: draft.enqueuedAtMs,
            lastError: undefined,
            deadLetterReason: undefined,
            enqueuedAtMs: draft.enqueuedAtMs,
            settledAtMs: undefined,
          });
          enqueued += 1;
        }
        return { enqueued, absorbed };
      }),
    claimDue: (sink, partitionId, nowMs, limit) =>
      Effect.sync(() =>
        ordered()
          .filter(
            (entry) =>
              entry.sink === sink &&
              entry.state === "pending" &&
              entry.nextAttemptAtMs <= nowMs &&
              (partitionId === undefined || entry.partitionId === partitionId),
          )
          .slice(0, limit),
      ),
    markDelivered: (id, attempts, atMs) =>
      Effect.sync(() => {
        replace(id, { state: "delivered", attempts, settledAtMs: atMs, lastError: undefined });
      }),
    reschedule: (id, attempts, nextAttemptAtMs, detail) =>
      Effect.sync(() => {
        replace(id, { state: "pending", attempts, nextAttemptAtMs, lastError: detail });
      }),
    deadLetter: (id, attempts, reason, detail, atMs) =>
      Effect.sync(() => {
        replace(id, {
          state: "dead",
          attempts,
          lastError: detail,
          deadLetterReason: reason,
          settledAtMs: atMs,
        });
      }),
    list: (sink, partitionId) =>
      Effect.sync(() =>
        ordered().filter(
          (entry) =>
            entry.sink === sink &&
            (partitionId === undefined || entry.partitionId === partitionId),
        ),
      ),
  };
}
