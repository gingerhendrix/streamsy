import { Context, Predicate, Effect, Layer, Option } from "effect";
import { Storage } from "../storage.ts";
import {
  MutationRejected,
  type Mutation,
  type MutationApplied,
  type Operation,
  type OperationResult,
} from "../mutation.ts";
import type { StreamId } from "../../schema/index.ts";
import { copyMessage, copyRecord, indexDeadlines, patchRecord, type State } from "./state.ts";
import { addEdge, composeMessages, hasDependents, purge } from "./lineage.ts";
import { createNotifier } from "./notifier.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- This Layer owns the boundary and its exact stream state.
import { makeBoundary, MemoryCommitBoundary } from "./boundary.ts";
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

export const layer = (options: MemoryOptions = {}): Layer.Layer<Storage | MemoryCommitBoundary> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const chain = !options.constrained;
      const interval = options.pollIntervalMs ?? 25;
      if (!Number.isFinite(interval) || interval <= 0)
        return yield* Effect.die(new RangeError("pollIntervalMs must be positive"));
      const committed: State = { entries: new Map(), children: new Map(), deadlines: [] };
      const bus = yield* createNotifier;
      const boundary = yield* makeBoundary(committed, bus.publish);
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
            messages: operation.initialMessages.map(copyMessage),
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
          const { _tag: tag } = operation;
          switch (tag) {
            case "Create":
              results.push({ _tag: "Created", record: copyRecord(entry.record) });
              break;
            case "Append": {
              entry.messages.push(...operation.messages.map(copyMessage));
              entry.record = patchRecord(entry.record, operation.patch);
              if (operation.producer)
                entry.producers.set(operation.producer.producerId, { ...operation.producer.next });
              results.push({ _tag: "Appended", record: copyRecord(entry.record) });
              break;
            }
            case "Delete": {
              if (chain && hasDependents(state, id)) {
                entry.record = patchRecord(entry.record, { lifecycle: { softDeleted: true } });
                results.push({ _tag: "SoftDeleted", record: copyRecord(entry.record) });
              } else {
                results.push({ _tag: "Purged", record: copyRecord(entry.record) });
                purge(state, entry.record, chain, changed);
              }
              break;
            }
          }
        }
        if (chain) indexDeadlines(state);
        const [first, ...rest] = results;
        if (!first) throw new Error("Empty mutation");
        return { outcome: { _tag: "Applied", results: [first, ...rest] }, changed };
      };
      const storage = Storage.of({
        capabilities: {
          fork: chain ? "chain" : "copy",
          atomicScope: chain ? "store" : "stream",
          wake: chain ? "push" : "poll",
          expiryIndex: chain ? "indexed" : "lazy",
        },
        record: Effect.fn("Memory.record")((id) =>
          boundary.access((state) =>
            Option.fromUndefinedOr(state.entries.get(id)?.record).pipe(Option.map(copyRecord)),
          ),
        ),
        messages: Effect.fn("Memory.messages")((id, window) =>
          boundary.access((state) =>
            composeMessages(state, id, chain)
              .filter(
                (message) =>
                  (window.after === undefined || message.offset > window.after) &&
                  (window.until === undefined || message.offset <= window.until),
              )
              .slice(0, window.limit === undefined ? undefined : Math.max(0, window.limit))
              .map(copyMessage),
          ),
        ),
        producer: Effect.fn("Memory.producer")((id, producerId) =>
          boundary.access((state) =>
            Option.fromUndefinedOr(state.entries.get(id)?.producers.get(producerId)).pipe(
              Option.map((value) => ({ epoch: value.epoch, lastSeq: value.lastSeq })),
            ),
          ),
        ),
        mutate: Effect.fn("Memory.mutate")(function* (mutation) {
          const ids = mutation.operations.map((operation) =>
            Predicate.isTagged(operation, "Create") ? operation.record.id : operation.streamId,
          );
          if (ids.length === 0 || new Set(ids).size !== ids.length || (!chain && ids.length > 1))
            return yield* Effect.die(
              new Error("Mutation requires distinct streams within atomicScope"),
            );
          return yield* Effect.gen(function* () {
            const result = yield* boundary.access((state) => commit(state, mutation));
            if (Predicate.isTagged(result, "MutationRejected")) return yield* result;
            const { outcome, changed } = result;
            if (changed.size > 0) yield* boundary.changed;
            return outcome;
          }).pipe(boundary.api.withTransaction, Effect.uninterruptible);
        }),
        changes: (id) => changes(committed, bus, id, chain, interval),
        nextExpiry: boundary.access((state) =>
          Option.fromUndefinedOr(state.deadlines[0]).pipe(
            Option.map((deadline) => ({ ...deadline })),
          ),
        ),
      });
      return Context.make(Storage, storage).pipe(Context.add(MemoryCommitBoundary, boundary.api));
    }),
  );
