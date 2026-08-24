import type { Effect } from "effect";
import type {
  ViewCursorConflict,
  ViewHistoryExpired,
  ViewStateRestorePoison,
  ViewStoreUnavailable,
} from "./errors.ts";

export type JsonScalar = null | boolean | number | string;
export type JsonValue = JsonScalar | { readonly [key: string]: JsonValue } | readonly JsonValue[];
export type RowKey = JsonScalar | readonly JsonValue[];

export interface ViewIdentity {
  readonly planName: string;
  readonly planHash: string;
  readonly partition: string;
  readonly sourceId: string;
}

export interface NamespaceRef extends ViewIdentity {
  readonly id: string;
}

export type ValueMutation =
  | {
      readonly kind: "put";
      readonly namespace: NamespaceRef;
      readonly key: RowKey;
      readonly value: JsonValue;
    }
  | { readonly kind: "delete"; readonly namespace: NamespaceRef; readonly key: RowKey };

export type IndexMutation =
  | {
      readonly kind: "put";
      readonly namespace: NamespaceRef;
      readonly indexName: string;
      readonly indexKey: RowKey;
      readonly sortKey: RowKey;
      readonly rowKey: RowKey;
      readonly value?: JsonValue;
    }
  | {
      readonly kind: "delete";
      readonly namespace: NamespaceRef;
      readonly indexName: string;
      readonly indexKey: RowKey;
      readonly sortKey: RowKey;
      readonly rowKey: RowKey;
    };

export type StoredChange =
  | {
      readonly kind: "enter";
      readonly relationId: string;
      readonly key: RowKey;
      readonly after: JsonValue;
    }
  | {
      readonly kind: "update";
      readonly relationId: string;
      readonly key: RowKey;
      readonly before: JsonValue;
      readonly after: JsonValue;
    }
  | {
      readonly kind: "exit";
      readonly relationId: string;
      readonly key: RowKey;
      readonly before: JsonValue;
    };

export interface MaintenanceCommit {
  readonly identity: ViewIdentity;
  readonly expectedCursor: string | undefined;
  readonly afterExclusiveCursor: string;
  readonly batchId: string;
  readonly committedAtMs: number;
  readonly rows?: readonly ValueMutation[];
  readonly operatorValues?: readonly ValueMutation[];
  readonly operatorIndexes?: readonly IndexMutation[];
  readonly reducerStates?: readonly ValueMutation[];
  readonly changes?: readonly StoredChange[];
}

export interface HistoryPosition {
  readonly epoch: number;
  readonly sequence: number;
}
export interface HistoryBounds {
  readonly epoch: number;
  readonly first: number | undefined;
  readonly latest: number | undefined;
}
export interface StoredChangeBatch {
  readonly position: HistoryPosition;
  readonly sourceCursor: string;
  readonly changes: readonly StoredChange[];
}
export interface Snapshot {
  readonly sourceCursor: string | undefined;
  readonly rows: readonly { readonly key: RowKey; readonly value: JsonValue }[];
}

export interface CheckpointDescriptor extends ViewIdentity {
  readonly reducerId: string;
  readonly reducerVersion: number;
}
export interface Checkpoint extends CheckpointDescriptor {
  readonly generation: number;
  readonly sourceCursor: string;
  readonly createdAtMs: number;
  readonly entries: readonly { readonly key: RowKey; readonly value: JsonValue }[];
}
export interface SaveCheckpoint extends CheckpointDescriptor {
  readonly sourceCursor: string;
  readonly createdAtMs: number;
  readonly entries: readonly { readonly key: RowKey; readonly value: JsonValue }[];
  readonly keepGenerations?: number;
}

export interface RetentionPolicy {
  readonly keepLastBatches?: number;
  readonly keepForMilliseconds?: number;
}
export type StoreError =
  | ViewStoreUnavailable
  | ViewStateRestorePoison
  | ViewCursorConflict
  | ViewHistoryExpired;

export interface ViewStoreService {
  readonly commit: (
    input: MaintenanceCommit,
    retention?: RetentionPolicy,
  ) => Effect.Effect<HistoryPosition, StoreError | ViewCursorConflict>;
  readonly getRow: (
    relation: NamespaceRef,
    key: RowKey,
  ) => Effect.Effect<JsonValue | undefined, StoreError>;
  readonly snapshotRows: (relation: NamespaceRef) => Effect.Effect<Snapshot, StoreError>;
  readonly getOperatorValue: (
    operator: NamespaceRef,
    key: RowKey,
  ) => Effect.Effect<JsonValue | undefined, StoreError>;
  readonly scanOperatorValues: (
    operator: NamespaceRef,
  ) => Effect.Effect<readonly { readonly key: RowKey; readonly value: JsonValue }[], StoreError>;
  readonly lookupIndex: (
    operator: NamespaceRef,
    indexName: string,
    indexKey: RowKey,
    range?: { readonly from?: RowKey; readonly to?: RowKey; readonly limit?: number },
  ) => Effect.Effect<
    readonly { readonly sortKey: RowKey; readonly rowKey: RowKey; readonly value?: JsonValue }[],
    StoreError
  >;
  readonly getReducerState: (
    reducer: NamespaceRef,
    key: RowKey,
  ) => Effect.Effect<JsonValue | undefined, StoreError>;
  readonly sourceProgress: (
    identity: ViewIdentity,
  ) => Effect.Effect<string | undefined, StoreError>;
  readonly historyBounds: (
    identity: ViewIdentity,
    relationId?: string,
  ) => Effect.Effect<HistoryBounds, StoreError>;
  readonly changesAfter: (
    identity: ViewIdentity,
    position: HistoryPosition | undefined,
    limit: number,
    relationId?: string,
  ) => Effect.Effect<readonly StoredChangeBatch[], StoreError | ViewHistoryExpired>;
  readonly saveCheckpoint: (checkpoint: SaveCheckpoint) => Effect.Effect<Checkpoint, StoreError>;
  readonly loadCheckpoint: (
    descriptor: CheckpointDescriptor,
  ) => Effect.Effect<Checkpoint | undefined, StoreError>;
}
