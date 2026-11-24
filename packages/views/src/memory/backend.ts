import { enlist } from "@streamsy/core/internal";
import { Array as Arr, Effect, Exit } from "effect";
import type { Backend } from "../backend.ts";
import { compareRowKeys, encodeRowKey } from "../runtime/key.ts";
import type { Accumulator, Entry, IndexEntry, Store } from "../store.ts";

type Table<A> = Map<string, Map<string, A>>;

interface Namespace {
  hash: string | undefined;
  readonly integrals: Table<Entry>;
  readonly arrangements: Table<IndexEntry[]>;
  readonly accumulators: Table<Accumulator>;
  readonly keyOwners: Table<string>;
}

const memoryState = new WeakMap<Backend, ReadonlyMap<string, Namespace>>();

/** Internal state for test dumps, read while the owner transaction is open. */
export const memoryStateOf = (backend: Backend) => memoryState.get(backend);

const emptyNamespace = (): Namespace => ({
  hash: undefined,
  integrals: new Map(),
  arrangements: new Map(),
  accumulators: new Map(),
  keyOwners: new Map(),
});

function select<A>(table: ReadonlyMap<string, A> | undefined, keys: readonly string[]) {
  const selected = new Map<string, A>();

  for (const key of keys) {
    const value = table?.get(key);

    if (value !== undefined) selected.set(key, value);
  }

  return selected;
}

const compareIndex = (left: IndexEntry, right: IndexEntry) =>
  compareRowKeys(left.sort_key, right.sort_key) || compareRowKeys(left.row_key, right.row_key);

function lowerBound(bucket: readonly IndexEntry[], entry: IndexEntry) {
  let first = 0;
  let end = bucket.length;

  while (first < end) {
    const mid = Math.floor((first + end) / 2);

    if (compareIndex(bucket[mid]!, entry) < 0) first = mid + 1;
    else end = mid;
  }

  return first;
}

function findEntry(bucket: readonly IndexEntry[], entry: IndexEntry, first: number) {
  for (let i = first; i < bucket.length; i++) {
    const candidate = bucket[i]!;

    if (candidate.row_key === entry.row_key && candidate.sort_key === entry.sort_key) return i;

    // Different lone surrogates can compare equal after UTF-8 replacement.
    if (compareIndex(candidate, entry) !== 0) break;
  }

  return -1;
}

/** One owner serializes access to these maps and the shared undo journal. */
export function makeMemoryBackend(
  // oxlint-disable-next-line anti-slop/no-object-parameters -- The owner is the opaque core transaction identity.
  owner: object,
): Backend {
  const namespaces = new Map<string, Namespace>();
  const journal: (() => void)[] = [];

  function undoTo(mark: number) {
    while (journal.length > mark) journal.pop()!();
  }

  // Registration is idempotent within the draft. Recheck access on every
  // operation so a forked fiber cannot use an already enlisted store.
  const access = enlist(
    owner,
    {
      onCommit: () => {
        journal.length = 0;
      },
      onRollback: () => undoTo(0),
    },
    "fail",
  );

  function write<A>(map: Map<string, A>, key: string, value: A | undefined) {
    const previous = map.get(key);

    if (previous === value) return;

    journal.push(() => {
      if (previous === undefined) map.delete(key);
      else map.set(key, previous);
    });

    if (value === undefined) map.delete(key);
    else map.set(key, value);
  }

  function obtain<A>(map: Map<string, A>, key: string, create: () => A) {
    const existing = map.get(key);

    if (existing !== undefined) return existing;

    const value = create();
    write(map, key, value);

    return value;
  }

  const withAccess = <A>(run: () => A) => access.pipe(Effect.andThen(Effect.sync(run)));

  const namespaceFor = (namespace: string) => obtain(namespaces, namespace, emptyNamespace);

  function insert(arrangements: Table<IndexEntry[]>, arrangement: string, entry: IndexEntry) {
    const buckets = obtain(arrangements, arrangement, () => new Map<string, IndexEntry[]>());
    const bucket = obtain(buckets, entry.index_key, () => []);
    const first = lowerBound(bucket, entry);

    if (findEntry(bucket, entry, first) !== -1) return;

    bucket.splice(first, 0, entry);
    journal.push(() => {
      bucket.splice(first, 1);
    });
  }

  const store = (namespace: string): Store => {
    const read = <A>(run: (state: Namespace | undefined) => A) =>
      withAccess(() => run(namespaces.get(namespace)));

    const update = (run: (state: Namespace) => void) =>
      withAccess(() => run(namespaceFor(namespace)));

    return {
      rows: (relation) =>
        read((state) => Arr.fromIterable(state?.integrals.get(relation)?.values() ?? [])),
      get: (relation, key) => read((state) => state?.integrals.get(relation)?.get(key)),
      getMany: (relation, keys) => read((state) => select(state?.integrals.get(relation), keys)),
      put: (relation, entries) =>
        update(({ integrals }) => {
          const rows = obtain(integrals, relation, () => new Map<string, Entry>());

          for (const entry of entries) write(rows, encodeRowKey(entry.key), entry);
        }),
      remove: (relation, keys) =>
        read((state) => {
          const rows = state?.integrals.get(relation);

          if (rows !== undefined) for (const key of keys) write(rows, key, undefined);
        }),
      snapshotMark: (arrangement, key) =>
        update(({ arrangements }) => {
          insert(arrangements, arrangement, { index_key: "", sort_key: "", row_key: key });
        }),
      clearIndex: (arrangement) =>
        read((state) => {
          if (state !== undefined) write(state.arrangements, arrangement, undefined);
        }),
      indexPut: (arrangement, entries) =>
        update(({ arrangements }) => {
          for (const entry of entries) insert(arrangements, arrangement, entry);
        }),
      indexRemove: (arrangement, entries) =>
        read((state) => {
          const buckets = state?.arrangements.get(arrangement);

          if (buckets === undefined) return;

          for (const entry of entries) {
            const bucket = buckets.get(entry.index_key);

            if (bucket === undefined) continue;
            const index = findEntry(bucket, entry, lowerBound(bucket, entry));

            if (index === -1) continue;
            const [removed] = bucket.splice(index, 1);
            journal.push(() => {
              bucket.splice(index, 0, removed!);
            });

            if (bucket.length === 0) write(buckets, entry.index_key, undefined);
          }
        }),
      lookup: (arrangement, indexKey, limit, reverse = false) =>
        read((state) => {
          const bucket = state?.arrangements.get(arrangement)?.get(indexKey);

          if (bucket === undefined) return [];

          const rows =
            reverse && limit !== undefined
              ? bucket.slice(Math.max(0, bucket.length - limit))
              : bucket.slice(0, limit);

          if (reverse) rows.reverse();

          return Arr.map(rows, ({ row_key, sort_key }) => ({ row_key, sort_key }));
        }),
      accumulator: (node, groupKey) =>
        read((state) => state?.accumulators.get(node)?.get(groupKey)),
      accumulatorPut: (node, groupKey, value) =>
        update(({ accumulators }) => {
          write(
            obtain(accumulators, node, () => new Map<string, Accumulator>()),
            groupKey,
            value,
          );
        }),
      accumulatorRemove: (node, groupKey) =>
        read((state) => {
          const groups = state?.accumulators.get(node);

          if (groups !== undefined) write(groups, groupKey, undefined);
        }),
      owners: (relation, keys) => read((state) => select(state?.keyOwners.get(relation), keys)),
      ownersPut: (relation, entries) =>
        update(({ keyOwners }) => {
          const owners = obtain(keyOwners, relation, () => new Map<string, string>());

          for (const [key, ownerKey] of entries) write(owners, key, ownerKey);
        }),
      ownersRemove: (relation, keys) =>
        read((state) => {
          const owners = state?.keyOwners.get(relation);

          if (owners !== undefined) for (const key of keys) write(owners, key, undefined);
        }),
    };
  };

  const backend: Backend = {
    store,
    circuit: (namespace) => ({
      load: withAccess(() => namespaces.get(namespace)?.hash),
      save: (hash) =>
        withAccess(() => {
          const state = namespaceFor(namespace);
          const previous = state.hash;
          journal.push(() => {
            state.hash = previous;
          });
          state.hash = hash;
        }),
    }),
    clear: (namespace) => withAccess(() => write(namespaces, namespace, undefined)),
    atomic: (run) =>
      access.pipe(
        Effect.andThen(
          Effect.suspend(() => {
            const mark = journal.length;

            return run.pipe(
              Effect.onExit((exit) =>
                Exit.isFailure(exit) ? Effect.sync(() => undoTo(mark)) : Effect.void,
              ),
            );
          }),
        ),
      ),
  };

  memoryState.set(backend, namespaces);

  return backend;
}
