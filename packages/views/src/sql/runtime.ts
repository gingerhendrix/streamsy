import { Array as Arr, Effect } from "effect";
import { evaluateOptional, isMissing } from "../runtime/expression.ts";
import { canonicalJson, encodeRowKey } from "../runtime/key.ts";
import type { JsonObject, JsonValue, RowKey } from "../ir/contracts.ts";
import { checkData, dataFault, type DataFault } from "./data-fault.ts";
import { orderKey } from "./encoding.ts";
import type { Arrangement, Program } from "./planner.ts";
import type { Entry, IndexEntry, Store } from "./store.ts";

export interface Delta {
  readonly key: RowKey;
  readonly before: JsonObject | undefined;
  readonly after: JsonObject | undefined;
}

export type Deltas = ReadonlyMap<string, Delta>;

export function delta(
  key: RowKey,
  before: JsonObject | undefined,
  after: JsonObject | undefined,
): Delta | undefined {
  return (before === undefined && after === undefined) ||
    (before !== undefined && after !== undefined && canonicalJson(before) === canonicalJson(after))
    ? undefined
    : { key, before, after };
}

/** Compare whole keyed relations only within the affected set. */
export function difference(
  before: readonly Entry[],
  after: readonly Entry[],
  fault: DataFault,
): Deltas {
  const old = unique(before, fault),
    next = unique(after, fault);

  const result = new Map<string, Delta>();

  for (const id of new Set([...old.keys(), ...next.keys()])) {
    const a = old.get(id),
      b = next.get(id);

    const change = delta(a?.key ?? b!.key, a?.row, b?.row);

    if (change) result.set(id, change);
  }

  return result;
}

function unique(entries: readonly Entry[], fault: DataFault): Map<string, Entry> {
  const result = new Map<string, Entry>();

  for (const e of entries) {
    const key = encodeRowKey(e.key);

    if (result.has(key)) throw fault(e.key, "duplicate output key");
    result.set(key, e);
  }

  return result;
}

export function indexEntry(a: Arrangement, entry: Entry): IndexEntry | undefined {
  const values = Arr.map(a.key, (e) => evaluateOptional(e, { row: entry.row, key: entry.key }));

  if (Arr.some(values, isMissing)) return undefined;

  if (
    a.optional &&
    Arr.some(a.order, (t) =>
      isMissing(evaluateOptional(t.expression, { row: entry.row, key: entry.key })),
    )
  )
    return undefined;
  // Every missing value was excluded above. canonicalJson accepts the narrowed array.
  const present = Arr.filter(values, (v): v is JsonValue => !isMissing(v));

  return {
    index_key: canonicalJson(present),
    sort_key: orderKey(entry, a.order),
    row_key: encodeRowKey(entry.key),
  };
}

/** Unit-local overlays reconstruct the before image after shared inputs advance.
 * Ordered reads fetch at most limit + changed entries, never the whole partition.
 * No cache or payload survives the unit; SQL is authoritative between units.
 */
export function runtime(program: Program, store: Store) {
  const fault = (relation: string) => {
    const location = program.locations.get(relation);

    if (!location) throw new Error(`missing relation location for ${relation}`);

    return dataFault(location);
  };

  const changes = new Map<string, Deltas>();
  const cache = new Map<string, Map<string, Entry | undefined>>();

  const indexChanges = new Map<
    string,
    Map<string, Map<string, { before?: IndexEntry; after?: IndexEntry }>>
  >();

  const get = (
    relation: string,
    key: string,
    before: boolean,
  ): Effect.Effect<Entry | undefined, Effect.Error<ReturnType<Store["get"]>>> =>
    Effect.gen(function* () {
      const d = changes.get(relation)?.get(key);

      if (before && d) return d.before === undefined ? undefined : { key: d.key, row: d.before };
      const rows = cache.get(relation) ?? new Map<string, Entry | undefined>();
      cache.set(relation, rows);

      if (!rows.has(key)) rows.set(key, yield* store.get(relation, key));

      return rows.get(key);
    });

  const lookup = (
    a: Arrangement,
    key: string,
    before: boolean,
    options: { readonly limit?: number; readonly reverse?: boolean; readonly batch?: boolean } = {},
  ) =>
    Effect.gen(function* () {
      const { limit, reverse = false, batch = false } = options;

      const pending =
        indexChanges.get(a.id)?.get(key) ??
        new Map<string, { before?: IndexEntry; after?: IndexEntry }>();

      const entries = yield* store.lookup(
        a.id,
        key,
        limit === undefined ? undefined : limit + (before ? pending.size : 0),
        reverse,
      );

      const candidates = new Map<string, { row_key: string; sort_key: string }>();

      for (const e of entries) if (!before || !pending.has(e.row_key)) candidates.set(e.row_key, e);

      if (before) for (const [id, d] of pending) if (d.before) candidates.set(id, d.before);
      const ordered = [...candidates.values()];
      ordered.sort((a, b) => {
        const x =
          a.sort_key < b.sort_key
            ? -1
            : a.sort_key > b.sort_key
              ? 1
              : a.row_key < b.row_key
                ? -1
                : a.row_key > b.row_key
                  ? 1
                  : 0;

        return reverse ? -x : x;
      });
      const selected = limit === undefined ? ordered : ordered.slice(0, limit);

      if (batch) {
        const rows = cache.get(a.relation) ?? new Map<string, Entry | undefined>();
        cache.set(a.relation, rows);

        const keys = Arr.map(
          Arr.filter(
            selected,
            (e) => !rows.has(e.row_key) && !(before && changes.get(a.relation)?.has(e.row_key)),
          ),
          (e) => e.row_key,
        );

        const found = yield* store.getMany(a.relation, keys);

        for (const id of keys) rows.set(id, found.get(id));
      }

      const rows: Entry[] = [];

      for (const e of selected) {
        const row = yield* get(a.relation, e.row_key, before);

        if (!row) throw new TypeError("arrangement references absent integral row");
        rows.push(row);
      }

      return rows;
    });

  const apply = (relation: string, deltas: Deltas) =>
    Effect.gen(function* () {
      changes.set(relation, deltas);

      if (!program.stored.has(relation) || deltas.size === 0) return;

      const puts: Entry[] = [],
        deletes: string[] = [];

      const rows = cache.get(relation) ?? new Map<string, Entry | undefined>();
      cache.set(relation, rows);

      for (const [key, d] of deltas) {
        const entry = d.after === undefined ? undefined : { key: d.key, row: d.after };
        rows.set(key, entry);

        if (entry) puts.push(entry);
        else deletes.push(key);
      }

      yield* store.remove(relation, deletes);
      yield* store.put(relation, puts);

      for (const a of program.arrangements) {
        if (a.relation !== relation) continue;

        const old: IndexEntry[] = [],
          next: IndexEntry[] = [];

        const buckets = new Map<string, Map<string, { before?: IndexEntry; after?: IndexEntry }>>();
        indexChanges.set(a.id, buckets);

        for (const d of deltas.values()) {
          const before =
            d.before === undefined
              ? undefined
              : checkData(fault(a.node), d.key, () =>
                  indexEntry(a, { key: d.key, row: d.before! }),
                );

          const after =
            d.after === undefined
              ? undefined
              : checkData(fault(a.node), d.key, () => indexEntry(a, { key: d.key, row: d.after! }));

          if (before?.index_key === after?.index_key && before?.sort_key === after?.sort_key)
            continue;

          if (before) old.push(before);

          if (after) next.push(after);

          for (const e of [before, after]) {
            if (!e) continue;

            const bucket =
              buckets.get(e.index_key) ??
              new Map<string, { before?: IndexEntry; after?: IndexEntry }>();

            bucket.set(e.row_key, {
              before: before?.index_key === e.index_key ? before : undefined,
              after: after?.index_key === e.index_key ? after : undefined,
            });
            buckets.set(e.index_key, bucket);
          }
        }

        yield* store.indexRemove(a.id, old);
        yield* store.indexPut(a.id, next);
      }
    });

  return { changes, get, lookup, apply, fault };
}

export type Runtime = ReturnType<typeof runtime>;
