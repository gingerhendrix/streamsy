import { Array, Context, Predicate, Effect, Layer, Option } from "effect";
import { attachOwner, ownerOf } from "../../internal/owner.ts";
import { Storage } from "../storage.ts";
import {
  MutationRejected,
  Mutations,
  OperationResults,
  Operations,
  type Mutation,
  type MutationApplied,
  type Operation,
  type OperationResult,
} from "../mutation.ts";
import type { StreamId } from "../../schema/index.ts";
import { copyMessage, copyRecord, indexDeadlines, patchRecord, type State } from "./state.ts";
import { addEdge, composeMessages, hasDependents, purge } from "./lineage.ts";
import { createNotifier } from "./notifier.ts";
import { OwnerRecords } from "../../internal/owner-records.ts";
import { CommitBoundary } from "../commit-boundary.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- This Layer owns the boundary and its exact stream state.
import { makeBoundary } from "./boundary.ts";
import { changes } from "./changes.ts";

interface CommitResult {
  outcome: MutationApplied;
  changed: Set<StreamId>;
}

export interface MemoryOptions {
  readonly constrained?: boolean;
  readonly pollIntervalMs?: number;
}

function reject(state: State, operation: Operation, index: number): MutationRejected | undefined {
  const id = Predicate.isTagged(operation, "Create") ? operation.record.id : operation.streamId;
  const entry = state.entries.get(id);
  const record = entry?.record;

  const rejected = (reason: MutationRejected["reason"]): MutationRejected =>
    new MutationRejected({
      index,
      reason,
      record: record ? Option.some(copyRecord(record)) : Option.none(),
    });

  if (Predicate.isTagged(operation, "Create")) {
    if (record) return rejected("exists");

    if (operation.forkSource) {
      const source = state.entries.get(operation.forkSource.id)?.record;

      if (
        !source ||
        source.lifecycle.softDeleted ||
        source.currentOffset < operation.forkSource.liveAtOffset
      )
        return rejected("fork-source-gone");
    }

    return undefined;
  }

  if (!entry || !record) return rejected("not-found");

  if (record.lifecycle.softDeleted) return rejected("gone");

  if (Predicate.isTagged(operation, "Delete")) {
    if (
      operation.reason === "expiry" &&
      (operation.expectedExpiresAtMs === undefined ||
        record.lifecycle.expiresAtMs !== operation.expectedExpiresAtMs)
    )
      return rejected("expiry-mismatch");

    return undefined;
  }

  if (operation.expectedOffset !== undefined && operation.expectedOffset !== record.currentOffset)
    return rejected("offset");

  if (
    operation.expectedClosed !== undefined &&
    operation.expectedClosed !== record.lifecycle.closed
  )
    return rejected("closed");

  if (operation.producer) {
    const actual = entry.producers.get(operation.producer.producerId);
    const expected = Option.getOrUndefined(operation.producer.expected);

    if (actual?.epoch !== expected?.epoch || actual?.lastSeq !== expected?.lastSeq)
      return rejected("producer");
  }

  return undefined;
}

export const layer = (
  options: MemoryOptions = {},
): Layer.Layer<Storage | CommitBoundary | OwnerRecords> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const chain = !options.constrained;
      const interval = options.pollIntervalMs ?? 25;

      if (!Number.isFinite(interval) || interval <= 0)
        return yield* Effect.die(new RangeError("pollIntervalMs must be positive"));
      const committed: State = { entries: new Map(), children: new Map(), deadlines: [] };
      const bus = yield* createNotifier;
      const owner = yield* makeBoundary(committed, bus.publish);

      const commit = (state: State, mutation: Mutation): CommitResult | MutationRejected => {
        const changed = new Set<StreamId>();

        for (const [index, operation] of mutation.operations.entries()) {
          const rejection = reject(state, operation, index);

          if (rejection) return rejection;
        }

        const results: OperationResult[] = [];

        // Create edges before deleting sources, independent of operation order.
        for (const operation of mutation.operations) {
          if (!Predicate.isTagged(operation, "Create")) continue;
          const record = copyRecord(operation.record);
          state.entries.set(record.id, {
            record,
            messages: Array.map(operation.initialMessages, copyMessage),
            producers: new Map(),
          });

          if (chain && record.lifecycle.forkedFrom !== undefined)
            addEdge(state, record.lifecycle.forkedFrom, record.id);
        }

        for (const operation of mutation.operations) {
          const id = Predicate.isTagged(operation, "Create")
            ? operation.record.id
            : operation.streamId;

          const entry = state.entries.get(id);

          if (!entry) throw new Error("Mutation target disappeared after preflight");
          changed.add(id);
          Operations.$match(operation, {
            Create: () => {
              results.push(OperationResults.Created({ record: copyRecord(entry.record) }));
            },
            Append: (append) => {
              entry.messages.push(...Array.map(append.messages, copyMessage));
              entry.record = patchRecord(entry.record, append.patch);

              if (append.producer)
                entry.producers.set(append.producer.producerId, { ...append.producer.next });
              results.push(OperationResults.Appended({ record: copyRecord(entry.record) }));
            },
            Delete: () => {
              if (chain && hasDependents(state, id)) {
                entry.record = patchRecord(entry.record, { lifecycle: { softDeleted: true } });
                results.push(OperationResults.SoftDeleted({ record: copyRecord(entry.record) }));
              } else {
                results.push(OperationResults.Purged({ record: copyRecord(entry.record) }));
                purge(state, entry.record, chain, changed);
              }
            },
          });
        }

        if (chain) indexDeadlines(state);
        const [first, ...rest] = results;

        if (!first) throw new Error("Empty mutation");

        return { outcome: Mutations.Applied({ results: [first, ...rest] }), changed };
      };

      const storage = Storage.of({
        capabilities: {
          fork: chain ? "chain" : "copy",
          atomicScope: chain ? "store" : "stream",
          wake: chain ? "push" : "poll",
          expiryIndex: chain ? "indexed" : "lazy",
        },
        record: Effect.fn("Memory.record")((id) =>
          owner.access((state) =>
            Option.fromUndefinedOr(state.entries.get(id)?.record).pipe(Option.map(copyRecord)),
          ),
        ),
        messages: Effect.fn("Memory.messages")((id, window) =>
          owner.access((state) =>
            Array.map(
              Array.filter(
                composeMessages(state, id, chain),
                (message) =>
                  (window.after === undefined || message.offset > window.after) &&
                  (window.until === undefined || message.offset <= window.until),
              ).slice(0, window.limit === undefined ? undefined : Math.max(0, window.limit)),
              copyMessage,
            ),
          ),
        ),
        producer: Effect.fn("Memory.producer")((id, producerId) =>
          owner.access((state) =>
            Option.fromUndefinedOr(state.entries.get(id)?.producers.get(producerId)).pipe(
              Option.map((value) => ({ epoch: value.epoch, lastSeq: value.lastSeq })),
            ),
          ),
        ),
        mutate: Effect.fn("Memory.mutate")(function* (mutation) {
          const ids = Array.map(mutation.operations, (operation) =>
            Predicate.isTagged(operation, "Create") ? operation.record.id : operation.streamId,
          );

          if (ids.length === 0 || new Set(ids).size !== ids.length || (!chain && ids.length > 1))
            return yield* Effect.die(
              new Error("Mutation requires distinct streams within atomicScope"),
            );

          return yield* Effect.gen(function* () {
            const result = yield* owner.access((state) => commit(state, mutation));

            if (Predicate.isTagged(result, "MutationRejected")) return yield* result;
            const { outcome, changed } = result;

            if (changed.size > 0) yield* owner.changed;

            return outcome;
          }).pipe(owner.withTransaction, Effect.uninterruptible);
        }),
        changes: (id) => changes(committed, bus, id, chain, interval),
        nextExpiry: owner.access((state) =>
          Option.fromUndefinedOr(state.deadlines[0]).pipe(
            Option.map((deadline) => ({ ...deadline })),
          ),
        ),
      });

      return Context.make(Storage, attachOwner(storage, ownerOf(owner.boundary))).pipe(
        Context.add(CommitBoundary, owner.boundary),
        Context.add(OwnerRecords, owner.records),
      );
    }),
  );
