/** Maps the pure A2 mutation seam into one atomic A4 maintenance commit. */
/* oxlint-disable anti-slop/no-chained-type-assertions, anti-slop/no-runtime-typeof, anti-slop/require-safety-comment-for-type-assertion -- This module is the explicit checked conversion boundary between A2's structurally JSON snapshot and A4's closed persistence grammar. */
import type { Change, JsonObject, RelationPlan, RowKey } from "@streamsy/views-ir";
import type { OperatorMutationPatch, OperatorStateSnapshot } from "@streamsy/views-engine";
import { Schema } from "effect";
import type {
  IndexMutation,
  JsonValue,
  MaintenanceCommit,
  NamespaceRef,
  StoredChange,
  ValueMutation,
  ViewIdentity,
} from "@streamsy/views-store";

export interface OperatorCommitInput {
  readonly plan: RelationPlan;
  readonly planHash: string;
  readonly partition: string;
  readonly sourceId: string;
  readonly expectedCursor: string | undefined;
  readonly afterExclusiveCursor: string;
  readonly batchId: string;
  readonly committedAtMs: number;
  readonly expectedRevision: number;
  readonly patch: OperatorMutationPatch;
  readonly snapshot: OperatorStateSnapshot;
  readonly relationId: string;
  readonly changes: readonly Change<JsonObject>[];
  /**
   * Extra operator values written in the *same* commit as the graph state.
   *
   * This is what lets a caller keep a durable position of its own beside the
   * operator snapshot without inventing a second transaction: the position and
   * the state it describes land together or neither does.
   */
  readonly extraValues?: readonly {
    readonly id: string;
    readonly key: RowKey;
    readonly value: JsonValue;
  }[];
}

const RowKey = Schema.Union([
  Schema.Boolean,
  Schema.Finite,
  Schema.String,
  Schema.Array(Schema.Union([Schema.Boolean, Schema.Finite, Schema.String])),
]);
const JsonObject = Schema.Record(Schema.String, Schema.Json);
const StateRow = Schema.Struct({ key: RowKey, row: JsonObject });
const CountedValue = Schema.Struct({ value: Schema.Json, count: Schema.Finite });
const OperatorStateSnapshotSchema = Schema.Struct({
  version: Schema.Literal(1),
  planName: Schema.String,
  revision: Schema.Finite,
  relations: Schema.Array(
    Schema.Struct({ relationId: Schema.String, rows: Schema.Array(StateRow) }),
  ),
  arrangements: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      relationId: Schema.String,
      retainedFields: Schema.Array(Schema.String),
      entries: Schema.Array(Schema.Struct({ value: Schema.Json, rowKeys: Schema.Array(RowKey) })),
    }),
  ),
  aggregates: Schema.Array(
    Schema.Struct({
      nodeId: Schema.String,
      groups: Schema.Array(
        Schema.Struct({
          key: RowKey,
          group: JsonObject,
          memberCount: Schema.Finite,
          sums: Schema.Record(Schema.String, Schema.Finite),
          maxima: Schema.Record(Schema.String, Schema.Array(CountedValue)),
          conditionalCounts: Schema.Record(Schema.String, Schema.Finite),
        }),
      ),
    }),
  ),
  tops: Schema.Array(
    Schema.Struct({
      nodeId: Schema.String,
      partitions: Schema.Array(
        Schema.Struct({
          key: RowKey,
          candidates: Schema.Array(
            Schema.Struct({
              key: RowKey,
              row: JsonObject,
              sortValues: Schema.Array(Schema.Json),
            }),
          ),
        }),
      ),
    }),
  ),
});
const decodeOperatorStateSnapshot = Schema.decodeUnknownSync(OperatorStateSnapshotSchema);

export function operatorMaintenanceCommit(input: OperatorCommitInput): MaintenanceCommit {
  if (input.patch.planName !== input.plan.name) {
    throw new TypeError(`patch plan ${input.patch.planName} does not match ${input.plan.name}`);
  }
  if (input.patch.baseRevision !== input.expectedRevision) {
    throw new TypeError(
      `patch revision ${input.patch.baseRevision} does not match ${input.expectedRevision}`,
    );
  }
  if (
    input.patch.nextRevision !== input.snapshot.revision ||
    input.snapshot.planName !== input.plan.name
  ) {
    throw new TypeError("patch and snapshot describe different operator revisions");
  }

  const identity: ViewIdentity = {
    planName: input.plan.name,
    planHash: input.planHash,
    partition: input.partition,
    sourceId: input.sourceId,
  };
  const namespace = (id: string): NamespaceRef => ({ ...identity, id });
  const rows: ValueMutation[] = input.changes.map((change) =>
    change.kind === "exit"
      ? { kind: "delete", namespace: namespace(input.relationId), key: change.key }
      : {
          kind: "put",
          namespace: namespace(input.relationId),
          key: change.key,
          value: change.after,
        },
  );
  const operatorValues: ValueMutation[] = [
    ...input.patch.operatorValues.map((mutation) =>
      mutation.operation === "delete"
        ? { kind: "delete" as const, namespace: namespace(mutation.id), key: mutation.key }
        : {
            kind: "put" as const,
            namespace: namespace(mutation.id),
            key: mutation.key,
            value: mutation.value,
          },
    ),
    {
      kind: "put",
      namespace: namespace("__graph_snapshot__"),
      key: "state",
      value: decodeOperatorStateSnapshot(input.snapshot),
    },
    ...(input.extraValues ?? []).map((extra) => ({
      kind: "put" as const,
      namespace: namespace(extra.id),
      key: extra.key,
      value: extra.value,
    })),
  ];
  const operatorIndexes: IndexMutation[] = input.patch.operatorIndexes.map((mutation) =>
    mutation.operation === "delete"
      ? {
          kind: "delete",
          namespace: namespace(mutation.id),
          indexName: mutation.indexName,
          indexKey: mutation.indexKey,
          sortKey: mutation.sortKey,
          rowKey: mutation.rowKey,
        }
      : mutation.value === undefined
        ? {
            kind: "put",
            namespace: namespace(mutation.id),
            indexName: mutation.indexName,
            indexKey: mutation.indexKey,
            sortKey: mutation.sortKey,
            rowKey: mutation.rowKey,
          }
        : {
            kind: "put",
            namespace: namespace(mutation.id),
            indexName: mutation.indexName,
            indexKey: mutation.indexKey,
            sortKey: mutation.sortKey,
            rowKey: mutation.rowKey,
            value: mutation.value,
          },
  );

  return {
    identity,
    expectedCursor: input.expectedCursor,
    afterExclusiveCursor: input.afterExclusiveCursor,
    batchId: input.batchId,
    committedAtMs: input.committedAtMs,
    rows,
    operatorValues,
    operatorIndexes,
    changes: input.changes.map((change): StoredChange => encodeChange(input.relationId, change)),
  };
}

function encodeChange(relationId: string, change: Change<JsonObject>): StoredChange {
  if (change.kind === "enter") return { ...change, relationId };
  if (change.kind === "update") return { ...change, relationId };
  return { ...change, relationId };
}

export const operatorSnapshotRef = (
  plan: RelationPlan,
  planHash: string,
  partition: string,
  sourceId: string,
): NamespaceRef => ({
  planName: plan.name,
  planHash,
  partition,
  sourceId,
  id: "__graph_snapshot__",
});

export function decodeOperatorSnapshot(
  plan: RelationPlan,
  value: JsonValue | undefined,
): OperatorStateSnapshot | undefined {
  if (value === undefined) return undefined;
  const candidate = decodeOperatorStateSnapshot(value);
  if (candidate.version !== 1 || candidate.planName !== plan.name) {
    throw new TypeError("stored operator snapshot is incompatible with the checked plan");
  }
  return candidate;
}

export const rowKey = (value: RowKey): RowKey => value;
