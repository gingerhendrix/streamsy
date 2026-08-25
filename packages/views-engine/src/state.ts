import type { JsonObject, JsonValue, RowKey } from "@streamsy/views-ir";

export interface StateRow {
  readonly key: RowKey;
  readonly row: JsonObject;
}

export interface RelationState {
  readonly relationId: string;
  readonly rows: readonly StateRow[];
}

export interface ArrangementEntry {
  readonly value: JsonValue;
  readonly rowKeys: readonly RowKey[];
}

/** Shared, covering-free lookup index; payload remains in its authoritative relation. */
export interface ArrangementState {
  readonly id: string;
  readonly relationId: string;
  readonly retainedFields: readonly string[];
  readonly entries: readonly ArrangementEntry[];
}

export interface CountedValue {
  readonly value: JsonValue;
  readonly count: number;
}

export interface AggregateGroupState {
  readonly key: RowKey;
  readonly group: JsonObject;
  readonly memberCount: number;
  readonly sums: Readonly<Record<string, number>>;
  readonly maxima: Readonly<Record<string, readonly CountedValue[]>>;
  readonly conditionalCounts: Readonly<Record<string, number>>;
}

export interface AggregateState {
  readonly nodeId: string;
  readonly groups: readonly AggregateGroupState[];
}

export interface TopCandidate {
  readonly key: RowKey;
  readonly row: JsonObject;
  readonly sortValues: readonly JsonValue[];
}

export interface TopPartitionState {
  readonly key: RowKey;
  readonly candidates: readonly TopCandidate[];
}

/** Exact top state has one authority: the complete ordered candidate set. */
export interface TopState {
  readonly nodeId: string;
  readonly partitions: readonly TopPartitionState[];
}

/** Portable, JSON-serializable pure-engine state. Derived winner vectors are omitted. */
export interface OperatorStateSnapshot {
  readonly version: 1;
  readonly planName: string;
  readonly revision: number;
  readonly relations: readonly RelationState[];
  readonly arrangements: readonly ArrangementState[];
  readonly aggregates: readonly AggregateState[];
  readonly tops: readonly TopState[];
}

/** Transaction-neutral value mutation. A host maps `id` to its durable namespace. */
export type OperatorValueMutation =
  | {
      readonly id: string;
      readonly operation: "put";
      readonly key: RowKey;
      readonly value: JsonValue;
    }
  | { readonly id: string; readonly operation: "delete"; readonly key: RowKey };

/** Transaction-neutral index mutation. A host maps `id` to its durable namespace. */
export type OperatorIndexMutation =
  | {
      readonly id: string;
      readonly operation: "put";
      readonly indexName: string;
      readonly indexKey: RowKey;
      readonly sortKey: RowKey;
      readonly rowKey: RowKey;
      readonly value?: JsonValue;
    }
  | {
      readonly id: string;
      readonly operation: "delete";
      readonly indexName: string;
      readonly indexKey: RowKey;
      readonly sortKey: RowKey;
      readonly rowKey: RowKey;
    };

export interface OperatorMutationPatch {
  readonly version: 1;
  readonly planName: string;
  readonly baseRevision: number;
  readonly nextRevision: number;
  readonly operatorValues: readonly OperatorValueMutation[];
  readonly operatorIndexes: readonly OperatorIndexMutation[];
}

export const emptyOperatorState = (planName: string): OperatorStateSnapshot => ({
  version: 1,
  planName,
  revision: 0,
  relations: [],
  arrangements: [],
  aggregates: [],
  tops: [],
});
