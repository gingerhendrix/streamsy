import { Array, Effect, Layer, Option } from "effect";
import { validateOptions, validatePrune } from "./validation.ts";
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
            nextAttemptAtMs: Math.max(draft.enqueuedAtMs, draft.notBeforeMs ?? draft.enqueuedAtMs),
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

        if (existing === undefined || existing.state !== "pending")
          return Effect.fail(
            new OutboxUnavailable({
              operation: "settle",
              detail: `outbox entry ${id} does not exist or is not pending`,
            }),
          );
        entries.set(id, { ...existing, ...settledFields(settlement) });

        return Effect.void;
      }),
    list: (scope) =>
      validateOptions("list", scope).pipe(
        Effect.andThen(
          Effect.sync(() =>
            scoped(scope)
              .filter(
                (entry) =>
                  (scope.state === undefined || entry.state === scope.state) &&
                  (scope.afterId === undefined || entry.id > scope.afterId),
              )
              .slice(0, scope.limit),
          ),
        ),
      ),
    prune: (options) =>
      validatePrune(options).pipe(
        Effect.andThen(
          Effect.sync(() => {
            const eligible = scoped({ name: options.name })
              .filter(
                (entry) =>
                  entry.state === options.state &&
                  entry.settledAtMs !== undefined &&
                  entry.settledAtMs < options.settledBeforeMs,
              )
              .slice(0, options.limit);
            for (const entry of eligible) {
              entries.delete(entry.id);
              identities.get(entry.name)?.delete(entry.idempotencyKey);
            }
            return eligible.length;
          }),
        ),
      ),
    count: (scope) =>
      validateOptions("count", scope).pipe(
        Effect.andThen(
          Effect.sync(
            () =>
              scoped(scope).filter(
                (entry) =>
                  (scope.state === undefined || entry.state === scope.state) &&
                  (scope.dueBeforeMs === undefined || entry.nextAttemptAtMs <= scope.dueBeforeMs),
              ).length,
          ),
        ),
      ),
  };
}

/**
 * An ephemeral store with the same retained identity and ordering semantics as SQL.
 * Each build creates an independent store, with its own drain permits.
 */
export const layerMemory: Layer.Layer<OutboxStore> = Layer.sync(OutboxStore, makeStore);
