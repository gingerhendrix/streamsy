import { Context, Effect, Layer, Schema } from "effect";
import type {
  Checkpoint,
  CheckpointDescriptor,
  IndexMutation,
  JsonValue,
  MaintenanceCommit,
  NamespaceRef,
  RetentionPolicy,
  RowKey,
  SaveCheckpoint,
  StoredChange,
  StoredChangeBatch,
  ValueMutation,
  ViewIdentity,
  ViewStoreService,
} from "./contracts.ts";
import {
  ViewCheckpointIncompatible,
  ViewCursorConflict,
  ViewHistoryExpired,
  ViewStateRestorePoison,
  ViewStoreUnavailable,
} from "./errors.ts";
import { decodeJson, decodeStoredChange } from "./errors-internal.ts";
import { decodeKey, encodeKey } from "./key-codec.ts";

export class ViewStore extends Context.Service<ViewStore, ViewStoreService>()(
  "@streamsy/views/store/ViewStore",
) {}

interface Partition {
  planHash: string;
  sourceId: string;
  cursor?: string;
  epoch: number;
  nextSequence: number;
  floor: number;
}
interface RawBatch {
  batchId: string;
  sourceCursor: string;
  committedAtMs: number;
  changes: readonly string[];
}
interface RawCheckpoint {
  descriptor: CheckpointDescriptor;
  generation: number;
  sourceCursor: string;
  createdAtMs: number;
  entries: Map<string, string>;
}
export interface MemoryBacking {
  partitions: Map<string, Partition>;
  rows: Map<string, string>;
  operatorValues: Map<string, string>;
  indexes: Map<string, string | undefined>;
  reducerStates: Map<string, string>;
  batches: Map<string, RawBatch>;
  checkpoints: Map<string, RawCheckpoint[]>;
}
export const makeMemoryBacking = (): MemoryBacking => ({
  partitions: new Map(),
  rows: new Map(),
  operatorValues: new Map(),
  indexes: new Map(),
  reducerStates: new Map(),
  batches: new Map(),
  checkpoints: new Map(),
});

const partitionKey = (i: ViewIdentity) => JSON.stringify([i.planName, i.partition]);
const namespaceKey = (n: NamespaceRef) => JSON.stringify([n.planName, n.partition, n.id]);
const valueKey = (n: NamespaceRef, key: RowKey) => `${namespaceKey(n)}\u0000${encodeKey(key)}`;
const indexPrefix = (n: NamespaceRef, name: string, key: RowKey) =>
  `${namespaceKey(n)}\u0000${name}\u0000${encodeKey(key)}\u0000`;
const indexKey = (m: IndexMutation) =>
  `${indexPrefix(m.namespace, m.indexName, m.indexKey)}${encodeKey(m.sortKey)}\u0000${encodeKey(m.rowKey)}`;
const batchKey = (i: ViewIdentity, sequence: number) =>
  `${partitionKey(i)}\u0000${sequence.toString().padStart(16, "0")}`;
const checkpointKey = (d: CheckpointDescriptor) =>
  JSON.stringify([d.planName, d.partition, d.reducerId]);
const raw = (value: JsonValue | StoredChange) => JSON.stringify(value);
const isCheckpointIncompatible = Schema.is(ViewCheckpointIncompatible);
const isCursorConflict = Schema.is(ViewCursorConflict);
const isHistoryExpired = Schema.is(ViewHistoryExpired);
const isRestorePoison = Schema.is(ViewStateRestorePoison);

interface ParsedIndex {
  readonly sortKey: RowKey;
  readonly rowKey: RowKey;
}
function parseIndex(encoded: string, prefix: string): ParsedIndex {
  const [sort, row] = encoded.slice(prefix.length).split("\u0000");
  if (sort === undefined || row === undefined) throw new TypeError("malformed index key");
  return { sortKey: decodeKey(sort), rowKey: decodeKey(row) };
}
function applyValues(target: Map<string, string>, mutations: readonly ValueMutation[]): void {
  for (const mutation of mutations) {
    if (mutation.kind === "put")
      target.set(valueKey(mutation.namespace, mutation.key), raw(mutation.value));
    else target.delete(valueKey(mutation.namespace, mutation.key));
  }
}
function storeError(
  operation: string,
  cause: unknown,
): ViewStoreUnavailable | ViewStateRestorePoison | ViewCheckpointIncompatible {
  if (isRestorePoison(cause) || isCheckpointIncompatible(cause)) return cause;
  return new ViewStoreUnavailable({
    operation,
    detail: cause instanceof Error ? cause.message : String(cause),
  });
}

export const memoryLayer = (backing: MemoryBacking = makeMemoryBacking()): Layer.Layer<ViewStore> =>
  Layer.succeed(ViewStore, memoryService(backing));

export function memoryService(backing: MemoryBacking): ViewStoreService {
  const readValue = (
    table: string,
    map: Map<string, string>,
    namespace: NamespaceRef,
    key: RowKey,
  ) =>
    Effect.try({
      try: () => {
        const found = map.get(valueKey(namespace, key));
        return found === undefined
          ? undefined
          : decodeJson(table, namespaceKey(namespace), encodeKey(key), found);
      },
      catch: (cause) => storeError(table, cause),
    });
  return ViewStore.of({
    commit: Effect.fn("ViewStore.commit")(
      (input: MaintenanceCommit, retention: RetentionPolicy = {}) =>
        Effect.try({
          try: () => {
            const pkey = partitionKey(input.identity);
            const existing = backing.partitions.get(pkey);
            if (existing !== undefined && existing.planHash !== input.identity.planHash)
              throw new ViewCheckpointIncompatible({
                reducerId: input.identity.planName,
                reason: `stored plan ${existing.planHash} does not match ${input.identity.planHash}`,
              });
            const duplicate = [...backing.batches.entries()].find(
              ([key, batch]) =>
                key.startsWith(`${pkey}\u0000`) &&
                batch.sourceCursor === input.afterExclusiveCursor,
            );
            if (duplicate !== undefined && duplicate[1].batchId !== input.batchId)
              throw new ViewCheckpointIncompatible({
                reducerId: input.identity.planName,
                reason: `source cursor ${input.afterExclusiveCursor} is already committed as batch ${duplicate[1].batchId}`,
              });
            if (duplicate !== undefined)
              return { epoch: existing?.epoch ?? 1, sequence: Number(duplicate[0].slice(-16)) };
            if (existing?.cursor !== input.expectedCursor)
              throw new ViewCursorConflict({
                planName: input.identity.planName,
                partition: input.identity.partition,
                expected: input.expectedCursor ?? null,
                actual: existing?.cursor ?? null,
              });
            const rows = new Map(backing.rows);
            const values = new Map(backing.operatorValues);
            const indexes = new Map(backing.indexes);
            const states = new Map(backing.reducerStates);
            const batches = new Map(backing.batches);
            applyValues(rows, input.rows ?? []);
            applyValues(values, input.operatorValues ?? []);
            applyValues(states, input.reducerStates ?? []);
            for (const mutation of input.operatorIndexes ?? []) {
              if (mutation.kind === "put")
                indexes.set(
                  indexKey(mutation),
                  mutation.value === undefined ? undefined : raw(mutation.value),
                );
              else indexes.delete(indexKey(mutation));
            }
            const sequence = existing?.nextSequence ?? 1;
            const epoch = existing?.epoch ?? 1;
            batches.set(batchKey(input.identity, sequence), {
              batchId: input.batchId,
              sourceCursor: input.afterExclusiveCursor,
              committedAtMs: input.committedAtMs,
              changes: (input.changes ?? []).map(raw),
            });
            let floor = existing?.floor ?? 1;
            const candidates = [...batches.entries()]
              .filter(([key]) => key.startsWith(`${pkey}\u0000`))
              .toSorted(([a], [b]) => a.localeCompare(b));
            const keepCount = Math.max(0, retention.keepLastBatches ?? candidates.length);
            const cutoff =
              retention.keepForMilliseconds === undefined
                ? -Infinity
                : input.committedAtMs - retention.keepForMilliseconds;
            for (const [key, batch] of candidates) {
              const seq = Number(key.slice(-16));
              const countExpired = seq <= sequence - keepCount;
              const timeExpired = batch.committedAtMs < cutoff;
              if (countExpired || timeExpired) {
                batches.delete(key);
                floor = Math.max(floor, seq + 1);
              }
            }
            backing.rows = rows;
            backing.operatorValues = values;
            backing.indexes = indexes;
            backing.reducerStates = states;
            backing.batches = batches;
            backing.partitions.set(pkey, {
              planHash: input.identity.planHash,
              sourceId: input.identity.sourceId,
              cursor: input.afterExclusiveCursor,
              epoch,
              nextSequence: sequence + 1,
              floor,
            });
            return { epoch, sequence };
          },
          catch: (cause) => (isCursorConflict(cause) ? cause : storeError("commit", cause)),
        }),
    ),
    getRow: Effect.fn("ViewStore.getRow")((namespace, key) =>
      readValue("rows", backing.rows, namespace, key),
    ),
    snapshotRows: Effect.fn("ViewStore.snapshotRows")((namespace) =>
      Effect.try({
        try: () => ({
          sourceCursor: backing.partitions.get(partitionKey(namespace))?.cursor,
          rows: [...backing.rows.entries()]
            .filter(([key]) => key.startsWith(`${namespaceKey(namespace)}\u0000`))
            .toSorted(([a], [b]) => a.localeCompare(b))
            .map(([key, value]) => ({
              key: decodeKey(key.slice(namespaceKey(namespace).length + 1)),
              value: decodeJson("rows", namespaceKey(namespace), key, value),
            })),
        }),
        catch: (cause) => storeError("snapshotRows", cause),
      }),
    ),
    getOperatorValue: Effect.fn("ViewStore.getOperatorValue")((namespace, key) =>
      readValue("operator_values", backing.operatorValues, namespace, key),
    ),
    scanOperatorValues: Effect.fn("ViewStore.scanOperatorValues")((namespace) =>
      Effect.try({
        try: () =>
          [...backing.operatorValues.entries()]
            .filter(([key]) => key.startsWith(`${namespaceKey(namespace)}\u0000`))
            .toSorted(([a], [b]) => a.localeCompare(b))
            .map(([key, value]) => ({
              key: decodeKey(key.slice(namespaceKey(namespace).length + 1)),
              value: decodeJson("operator_values", namespaceKey(namespace), key, value),
            })),
        catch: (cause) => storeError("scanOperatorValues", cause),
      }),
    ),
    lookupIndex: Effect.fn("ViewStore.lookupIndex")((namespace, name, key, range = {}) =>
      Effect.try({
        try: () => {
          const prefix = indexPrefix(namespace, name, key);
          return [...backing.indexes.entries()]
            .filter(([encoded]) => encoded.startsWith(prefix))
            .toSorted(([a], [b]) => a.localeCompare(b))
            .map(([encoded, value]) => indexEntry(encoded, prefix, value, namespace))
            .filter(
              (entry) =>
                (range.from === undefined || encodeKey(entry.sortKey) >= encodeKey(range.from)) &&
                (range.to === undefined || encodeKey(entry.sortKey) <= encodeKey(range.to)),
            )
            .slice(0, range.limit);
        },
        catch: (cause) => storeError("lookupIndex", cause),
      }),
    ),
    getReducerState: Effect.fn("ViewStore.getReducerState")((namespace, key) =>
      readValue("reducer_state", backing.reducerStates, namespace, key),
    ),
    sourceProgress: Effect.fn("ViewStore.sourceProgress")((identity) =>
      Effect.sync(() => backing.partitions.get(partitionKey(identity))?.cursor),
    ),
    historyBounds: Effect.fn("ViewStore.historyBounds")((identity, relationId) =>
      Effect.sync(() => {
        const p = backing.partitions.get(partitionKey(identity));
        const batches = decodeBatches(backing, identity, relationId);
        return {
          epoch: p?.epoch ?? 1,
          first: batches[0]?.position.sequence,
          latest: batches.at(-1)?.position.sequence,
        };
      }),
    ),
    changesAfter: Effect.fn("ViewStore.changesAfter")((identity, position, limit, relationId) =>
      Effect.try({
        try: () => {
          const p = backing.partitions.get(partitionKey(identity));
          const floor = p?.floor ?? 1;
          const requested = position?.sequence ?? floor - 1;
          if (
            position !== undefined &&
            (position.epoch !== (p?.epoch ?? 1) || requested < floor - 1)
          )
            throw new ViewHistoryExpired({
              epoch: p?.epoch ?? 1,
              requested,
              first: floor,
              latest: (p?.nextSequence ?? 1) - 1,
            });
          return decodeBatches(backing, identity, relationId)
            .filter((batch) => batch.position.sequence > requested)
            .slice(0, limit);
        },
        catch: (cause) => (isHistoryExpired(cause) ? cause : storeError("changesAfter", cause)),
      }),
    ),
    saveCheckpoint: Effect.fn("ViewStore.saveCheckpoint")((input: SaveCheckpoint) =>
      Effect.try({
        try: () => {
          const key = checkpointKey(input);
          const generations = backing.checkpoints.get(key) ?? [];
          const generation = (generations.at(-1)?.generation ?? 0) + 1;
          const saved: RawCheckpoint = {
            descriptor: input,
            generation,
            sourceCursor: input.sourceCursor,
            createdAtMs: input.createdAtMs,
            entries: new Map(
              input.entries.map((entry) => [encodeKey(entry.key), raw(entry.value)]),
            ),
          };
          backing.checkpoints.set(
            key,
            [...generations, saved].slice(-(input.keepGenerations ?? 2)),
          );
          return decodeCheckpoint(saved);
        },
        catch: (cause) => storeError("saveCheckpoint", cause),
      }),
    ),
    loadCheckpoint: Effect.fn("ViewStore.loadCheckpoint")((descriptor) =>
      Effect.try({
        try: () => {
          const generations = backing.checkpoints.get(checkpointKey(descriptor)) ?? [];
          const match = generations
            .filter(
              (item) =>
                item.descriptor.planHash === descriptor.planHash &&
                item.descriptor.sourceId === descriptor.sourceId &&
                item.descriptor.reducerVersion === descriptor.reducerVersion,
            )
            .at(-1);
          if (match === undefined && generations.length > 0)
            throw new ViewCheckpointIncompatible({
              reducerId: descriptor.reducerId,
              reason: "no active generation matches the plan, source, and reducer version",
            });
          return match === undefined ? undefined : decodeCheckpoint(match);
        },
        catch: (cause) => storeError("loadCheckpoint", cause),
      }),
    ),
  });
}

function decodeBatches(
  backing: MemoryBacking,
  identity: ViewIdentity,
  relationId?: string,
): StoredChangeBatch[] {
  const p = backing.partitions.get(partitionKey(identity));
  return [...backing.batches.entries()]
    .filter(([key]) => key.startsWith(`${partitionKey(identity)}\u0000`))
    .toSorted(([a], [b]) => a.localeCompare(b))
    .map(([key, batch]) => ({
      position: { epoch: p?.epoch ?? 1, sequence: Number(key.slice(-16)) },
      sourceCursor: batch.sourceCursor,
      changes: batch.changes
        .map((value, ordinal) =>
          decodeStoredChange("changes", partitionKey(identity), `${key}:${ordinal}`, value),
        )
        .filter((change) => relationId === undefined || change.relationId === relationId),
    }))
    .filter((batch) => batch.changes.length > 0 || relationId === undefined);
}

function indexEntry(
  encoded: string,
  prefix: string,
  value: string | undefined,
  namespace: NamespaceRef,
) {
  const entry: { sortKey: RowKey; rowKey: RowKey; value?: JsonValue } = parseIndex(encoded, prefix);
  if (value !== undefined)
    entry.value = decodeJson("operator_index", namespaceKey(namespace), encoded, value);
  return entry;
}
function decodeCheckpoint(rawCheckpoint: RawCheckpoint): Checkpoint {
  return {
    ...rawCheckpoint.descriptor,
    generation: rawCheckpoint.generation,
    sourceCursor: rawCheckpoint.sourceCursor,
    createdAtMs: rawCheckpoint.createdAtMs,
    entries: [...rawCheckpoint.entries]
      .toSorted(([a], [b]) => a.localeCompare(b))
      .map(([key, value]) => ({
        key: decodeKey(key),
        value: decodeJson("checkpoint_entries", rawCheckpoint.descriptor.reducerId, key, value),
      })),
  };
}

/*
 * The `@streamsy/views/store` subpath resolves to this module. The memory
 * backend is the tier's reference implementation; `contracts.ts`, `errors.ts`,
 * `key-codec.ts` and `recovery.ts` are store internals that no consumer
 * outside the package reaches directly, so their published names travel
 * through this entry rather than through subpaths of their own.
 */
export type {
  Checkpoint,
  CheckpointDescriptor,
  HistoryBounds,
  HistoryPosition,
  IndexMutation,
  JsonScalar,
  JsonValue,
  MaintenanceCommit,
  NamespaceRef,
  RetentionPolicy,
  RowKey,
  SaveCheckpoint,
  Snapshot,
  StoreError,
  StoredChange,
  StoredChangeBatch,
  ValueMutation,
  ViewIdentity,
  ViewStoreService,
} from "./contracts.ts";
export {
  ViewCheckpointIncompatible,
  ViewCursorConflict,
  ViewHistoryExpired,
  ViewStateRestorePoison,
  ViewStoreUnavailable,
} from "./errors.ts";
export { canonicalJson, compareKeys, decodeKey, encodeKey } from "./key-codec.ts";
export { recover } from "./recovery.ts";
export type { RecoverOptions, RecoveryFold, RecoveryResult, RecoverySource } from "./recovery.ts";
