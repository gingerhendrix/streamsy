import type { Effect } from "effect";
import type { SqlError } from "effect/sql";
import type { JsonObject, RowKey } from "./ir/contracts.ts";
import type { ViewDataFault } from "./sql/data-fault.ts";

export interface Entry {
  readonly key: RowKey;
  readonly row: JsonObject;
}

export interface IndexEntry {
  readonly index_key: string;
  readonly sort_key: string;
  readonly row_key: string;
}

export interface Accumulator {
  readonly count: number;
  readonly sums: Readonly<Record<string, number>>;
  readonly integerMass?: Readonly<Record<string, number>>;
}

/** The typed rejection returned by the memory owner's enlist operation. */
export type MemoryEnlistFailure = Error &
  Pick<Effect.Error<ReturnType<typeof import("@streamsy/core/internal").enlist>>, "_tag">;

export type Query<A> = Effect.Effect<A, SqlError.SqlError | ViewDataFault | MemoryEnlistFailure>;

/** Namespace-local operator state. Keys passed to batch reads are encoded identities. */
export interface Store {
  rows(relation: string): Query<readonly Entry[]>;
  get(relation: string, key: string): Query<Entry | undefined>;
  getMany(relation: string, keys: readonly string[]): Query<ReadonlyMap<string, Entry>>;
  put(relation: string, entries: readonly Entry[]): Query<void>;
  remove(relation: string, keys: readonly string[]): Query<void>;
  snapshotMark(arrangement: string, key: string): Query<void>;
  clearIndex(arrangement: string): Query<void>;
  indexPut(arrangement: string, entries: readonly IndexEntry[]): Query<void>;
  indexRemove(arrangement: string, entries: readonly IndexEntry[]): Query<void>;
  lookup(
    arrangement: string,
    indexKey: string,
    limit?: number,
    reverse?: boolean,
  ): Query<readonly Pick<IndexEntry, "row_key" | "sort_key">[]>;
  accumulator(node: string, groupKey: string): Query<Accumulator | undefined>;
  accumulatorPut(node: string, groupKey: string, value: Accumulator): Query<void>;
  accumulatorRemove(node: string, groupKey: string): Query<void>;
  owners(relation: string, keys: readonly string[]): Query<ReadonlyMap<string, string>>;
  ownersPut(relation: string, owners: ReadonlyMap<string, string>): Query<void>;
  ownersRemove(relation: string, keys: readonly string[]): Query<void>;
}
