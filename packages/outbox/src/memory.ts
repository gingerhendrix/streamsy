import { Array, Effect, Layer, Option } from "effect";
import { OutboxUnavailable } from "./errors.ts";
import {
  OutboxStore,
  Settlement,
  type OutboxBacking,
  type OutboxEntry,
  type OutboxScope,
} from "./store.ts";

/** The fields a settlement writes. */
const settledFields = Settlement.$match({
  Delivered: ({ attempts, atMs }): Partial<OutboxEntry> => ({
    state: "delivered",
    attempts,
    settledAtMs: atMs,
    lastError: undefined,
  }),
  Retry: ({ attempts, nextAttemptAtMs, detail }): Partial<OutboxEntry> => ({
    state: "pending",
    attempts,
    nextAttemptAtMs,
    lastError: detail,
  }),
  Dead: ({ attempts, reason, detail, atMs }): Partial<OutboxEntry> => ({
    state: "dead",
    attempts,
    lastError: detail,
    deadLetterReason: reason,
    settledAtMs: atMs,
  }),
});

function makeStore(): OutboxBacking {
  // Ids only grow and settlement replaces in place, so insertion order is id order.
  const entries = new Map<number, OutboxEntry>();
  const identities = new Map<string, Set<string>>();
  let nextId = 1;

  const scoped = ({ name, partitionId }: OutboxScope) =>
    Array.filter(
      [...entries.values()],
      (entry) =>
        entry.name === name && (partitionId === undefined || entry.partitionId === partitionId),
    );

  const pending = (scope: OutboxScope) =>
    Array.filter(scoped(scope), (entry) => entry.state === "pending");

  return {
    enqueue: (drafts) =>
      Effect.sync(() => {
        let enqueued = 0;

        for (const draft of drafts) {
          const keys = identities.get(draft.name) ?? new Set<string>();

          if (keys.has(draft.idempotencyKey)) continue;
          identities.set(draft.name, keys.add(draft.idempotencyKey));
          const id = nextId;
          nextId += 1;
          entries.set(id, {
            id,
            name: draft.name,
            partitionId: draft.partitionId,
            idempotencyKey: draft.idempotencyKey,
            encodedPayload: draft.encodedPayload,
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

        return { enqueued, absorbed: drafts.length - enqueued };
      }),
    nextDue: (scope) =>
      Effect.sync(() => {
        const due = pending(scope);
        const first = due[0];

        if (first === undefined) return Option.none();

        return Option.some(
          scope.ordered === true
            ? first.nextAttemptAtMs
            : Array.reduce(due, first.nextAttemptAtMs, (at, entry) =>
                Math.min(at, entry.nextAttemptAtMs),
              ),
        );
      }),
    readDue: (scope, nowMs, limit) =>
      Effect.sync(() =>
        Array.filter(
          pending(scope),
          (entry) => scope.ordered === true || entry.nextAttemptAtMs <= nowMs,
        ).slice(0, limit),
      ),
    settle: (id, settlement) =>
      Effect.suspend(() => {
        const existing = entries.get(id);

        if (existing === undefined)
          return Effect.fail(
            new OutboxUnavailable({
              operation: "settle",
              detail: `outbox entry ${id} does not exist`,
            }),
          );
        entries.set(id, { ...existing, ...settledFields(settlement) });

        return Effect.void;
      }),
    list: (scope) => Effect.sync(() => scoped(scope)),
  };
}

/**
 * An ephemeral store with the same retained identity and ordering semantics as SQL.
 * Each build creates an independent store, with its own drain permits.
 */
export const layerMemory: Layer.Layer<OutboxStore> = Layer.sync(OutboxStore, makeStore);
