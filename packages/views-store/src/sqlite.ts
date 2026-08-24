import { Database } from "bun:sqlite";
import { Effect, Layer } from "effect";
import type {
  Checkpoint,
  HistoryPosition,
  IndexMutation,
  JsonValue,
  MaintenanceCommit,
  NamespaceRef,
  RetentionPolicy,
  RowKey,
  SaveCheckpoint,
  StoredChange,
  StoredChangeBatch,
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
import { decodeJson } from "./errors-internal.ts";
import { decodeKey, encodeKey } from "./key-codec.ts";
import { ViewStore } from "./memory.ts";
import { importLegacyIssueStore, migrateViewStore, type LegacyImport } from "./sqlite-schema.ts";

export {
  importLegacyIssueStore,
  migrateViewStore,
  VIEW_SCHEMA_VERSION,
  type LegacyImport,
} from "./sqlite-schema.ts";
export interface SqliteStoreOptions {
  readonly filename?: string;
  readonly database?: Database;
  readonly legacyImport?: LegacyImport;
  readonly now?: () => number;
}
const fail = (operation: string, cause: unknown) =>
  cause instanceof ViewCheckpointIncompatible ||
  cause instanceof ViewCursorConflict ||
  cause instanceof ViewHistoryExpired ||
  cause instanceof ViewStateRestorePoison
    ? cause
    : new ViewStoreUnavailable({
        operation,
        detail: cause instanceof Error ? cause.message : String(cause),
      });
const attempt = <A>(operation: string, run: () => A) =>
  Effect.try({ try: run, catch: (cause) => fail(operation, cause) });

export const sqliteLayer = (options: SqliteStoreOptions = {}): Layer.Layer<ViewStore> =>
  Layer.effect(
    ViewStore,
    Effect.acquireRelease(
      Effect.sync(() => {
        const owned = options.database === undefined;
        const database =
          options.database ?? new Database(options.filename ?? ":memory:", { create: true });
        database.run("PRAGMA foreign_keys=ON");
        if (owned && options.filename !== undefined && options.filename !== ":memory:")
          database.run("PRAGMA journal_mode=WAL");
        migrateViewStore(database, options.now?.());
        if (options.legacyImport !== undefined)
          importLegacyIssueStore(database, options.legacyImport, options.now?.());
        return { database, owned };
      }),
      ({ database, owned }) =>
        Effect.sync(() => {
          if (owned) database.close(false);
        }),
    ).pipe(Effect.map(({ database }) => sqliteService(database))),
  );

export function sqliteService(database: Database): ViewStoreService {
  const transaction = database.transaction(
    (input: MaintenanceCommit, retention: RetentionPolicy): HistoryPosition => {
      const i = input.identity;
      const current = database
        .query<
          {
            plan_hash: string;
            source_cursor: string | null;
            history_epoch: number;
            next_history_seq: number;
            history_floor: number;
          },
          [string, string]
        >(
          "SELECT plan_hash,source_cursor,history_epoch,next_history_seq,history_floor FROM streamsy_view_partitions WHERE plan_name=? AND partition_key=?",
        )
        .get(i.planName, i.partition);
      if (current !== null && current.plan_hash !== i.planHash)
        throw new ViewCheckpointIncompatible({
          reducerId: i.planName,
          reason: `stored plan ${current.plan_hash} does not match ${i.planHash}`,
        });
      const duplicate = database
        .query<
          { history_epoch: number; history_seq: number; batch_id: string },
          [string, string, string, string]
        >(
          "SELECT history_epoch,history_seq,batch_id FROM streamsy_view_change_batches WHERE plan_name=? AND partition_key=? AND source_id=? AND source_cursor=?",
        )
        .get(i.planName, i.partition, i.sourceId, input.afterExclusiveCursor);
      if (duplicate !== null && duplicate.batch_id !== input.batchId)
        throw new ViewCheckpointIncompatible({
          reducerId: i.planName,
          reason: `source cursor ${input.afterExclusiveCursor} is already committed as batch ${duplicate.batch_id}`,
        });
      if (duplicate !== null)
        return { epoch: duplicate.history_epoch, sequence: duplicate.history_seq };
      const actual = current?.source_cursor ?? undefined;
      if (actual !== input.expectedCursor)
        throw new ViewCursorConflict({
          planName: i.planName,
          partition: i.partition,
          expected: input.expectedCursor ?? null,
          actual: actual ?? null,
        });
      const epoch = current?.history_epoch ?? 1;
      const sequence = current?.next_history_seq ?? 1;
      database.run(
        "INSERT INTO streamsy_view_partitions VALUES (?,?,?,?,?,?,?,1,?) ON CONFLICT(plan_name,partition_key) DO UPDATE SET plan_hash=excluded.plan_hash,source_id=excluded.source_id,source_cursor=excluded.source_cursor,next_history_seq=excluded.next_history_seq,updated_at_ms=excluded.updated_at_ms",
        [
          i.planName,
          i.partition,
          i.planHash,
          i.sourceId,
          input.afterExclusiveCursor,
          epoch,
          sequence + 1,
          input.committedAtMs,
        ],
      );
      for (const [surface, mutations] of [
        ["rows", input.rows ?? []],
        ["operator", input.operatorValues ?? []],
        ["reducer", input.reducerStates ?? []],
      ] as const)
        for (const mutation of mutations) {
          if (mutation.kind === "put")
            database.run(
              "INSERT INTO streamsy_view_values VALUES (?,?,?,?,?,?) ON CONFLICT DO UPDATE SET value_json=excluded.value_json",
              [
                surface,
                i.planName,
                i.partition,
                mutation.namespace.id,
                encodeKey(mutation.key),
                JSON.stringify(mutation.value),
              ],
            );
          else
            database.run(
              "DELETE FROM streamsy_view_values WHERE surface=? AND plan_name=? AND partition_key=? AND namespace_id=? AND value_key=?",
              [surface, i.planName, i.partition, mutation.namespace.id, encodeKey(mutation.key)],
            );
        }
      for (const mutation of input.operatorIndexes ?? []) writeIndex(database, mutation);
      database.run("INSERT INTO streamsy_view_change_batches VALUES (?,?,?,?,?,?,?,?,?)", [
        i.planName,
        i.partition,
        epoch,
        sequence,
        i.sourceId,
        input.afterExclusiveCursor,
        input.batchId,
        i.planHash,
        input.committedAtMs,
      ]);
      (input.changes ?? []).forEach((change, ordinal) =>
        database.run("INSERT INTO streamsy_view_changes VALUES (?,?,?,?,?,?,?,?,?,?)", [
          i.planName,
          i.partition,
          epoch,
          sequence,
          ordinal,
          change.relationId,
          change.kind,
          encodeKey(change.key),
          "before" in change ? JSON.stringify(change.before) : null,
          "after" in change ? JSON.stringify(change.after) : null,
        ]),
      );
      const keep = retention.keepLastBatches;
      const cutoff =
        retention.keepForMilliseconds === undefined
          ? undefined
          : input.committedAtMs - retention.keepForMilliseconds;
      if (keep !== undefined || cutoff !== undefined) {
        const rows = database
          .query<{ history_seq: number; committed_at_ms: number }, [string, string]>(
            "SELECT history_seq,committed_at_ms FROM streamsy_view_change_batches WHERE plan_name=? AND partition_key=? ORDER BY history_seq",
          )
          .all(i.planName, i.partition);
        let floor = current?.history_floor ?? 1;
        rows.forEach((row, index) => {
          if (
            (keep !== undefined && index < rows.length - keep) ||
            (cutoff !== undefined && row.committed_at_ms < cutoff)
          ) {
            database.run(
              "DELETE FROM streamsy_view_change_batches WHERE plan_name=? AND partition_key=? AND history_epoch=? AND history_seq=?",
              [i.planName, i.partition, epoch, row.history_seq],
            );
            floor = Math.max(floor, row.history_seq + 1);
          }
        });
        database.run(
          "UPDATE streamsy_view_partitions SET history_floor=? WHERE plan_name=? AND partition_key=?",
          [floor, i.planName, i.partition],
        );
      }
      return { epoch, sequence };
    },
  );
  const readValue = (surface: string, n: NamespaceRef, key: RowKey) =>
    attempt(`get:${surface}`, () => {
      const row = database
        .query<{ value_json: string }, [string, string, string, string, string]>(
          "SELECT value_json FROM streamsy_view_values WHERE surface=? AND plan_name=? AND partition_key=? AND namespace_id=? AND value_key=?",
        )
        .get(surface, n.planName, n.partition, n.id, encodeKey(key));
      return row === null
        ? undefined
        : decodeJson(`streamsy_view_${surface}`, n.id, encodeKey(key), row.value_json);
    });
  return ViewStore.of({
    commit: Effect.fn("ViewStore.commit")((input, retention = {}) =>
      attempt("commit", () => transaction(input, retention)),
    ),
    getRow: Effect.fn("ViewStore.getRow")((n, k) => readValue("rows", n, k)),
    snapshotRows: Effect.fn("ViewStore.snapshotRows")((n) =>
      attempt("snapshotRows", () => ({
        sourceCursor: progress(database, n),
        rows: scanValues(database, "rows", n),
      })),
    ),
    getOperatorValue: Effect.fn("ViewStore.getOperatorValue")((n, k) =>
      readValue("operator", n, k),
    ),
    scanOperatorValues: Effect.fn("ViewStore.scanOperatorValues")((n) =>
      attempt("scanOperatorValues", () => scanValues(database, "operator", n)),
    ),
    lookupIndex: Effect.fn("ViewStore.lookupIndex")((n, name, key, range = {}) =>
      attempt("lookupIndex", () =>
        database
          .query<
            { sort_key: string; row_key: string; value_json: string | null },
            [string, string, string, string, string]
          >(
            "SELECT sort_key,row_key,value_json FROM streamsy_view_operator_index WHERE plan_name=? AND partition_key=? AND operator_id=? AND index_name=? AND index_key=? ORDER BY sort_key,row_key",
          )
          .all(n.planName, n.partition, n.id, name, encodeKey(key))
          .map((row) => decodeIndexRow(row, n.id))
          .filter(
            (entry) =>
              (range.from === undefined || encodeKey(entry.sortKey) >= encodeKey(range.from)) &&
              (range.to === undefined || encodeKey(entry.sortKey) <= encodeKey(range.to)),
          )
          .slice(0, range.limit),
      ),
    ),
    getReducerState: Effect.fn("ViewStore.getReducerState")((n, k) => readValue("reducer", n, k)),
    sourceProgress: Effect.fn("ViewStore.sourceProgress")((i) =>
      attempt("sourceProgress", () => progress(database, i)),
    ),
    historyBounds: Effect.fn("ViewStore.historyBounds")((i, relation) =>
      attempt("historyBounds", () => bounds(database, i, relation)),
    ),
    changesAfter: Effect.fn("ViewStore.changesAfter")((i, position, limit, relation) =>
      attempt("changesAfter", () => changesAfter(database, i, position, limit, relation)),
    ),
    saveCheckpoint: Effect.fn("ViewStore.saveCheckpoint")((input) =>
      attempt("saveCheckpoint", () => saveCheckpoint(database, input)),
    ),
    loadCheckpoint: Effect.fn("ViewStore.loadCheckpoint")((input) =>
      attempt("loadCheckpoint", () => loadCheckpoint(database, input)),
    ),
  });
}

function writeIndex(db: Database, m: IndexMutation): void {
  const args = [
    m.namespace.planName,
    m.namespace.partition,
    m.namespace.id,
    m.indexName,
    encodeKey(m.indexKey),
    encodeKey(m.sortKey),
    encodeKey(m.rowKey),
  ];
  if (m.kind === "put")
    db.run(
      "INSERT INTO streamsy_view_operator_index VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO UPDATE SET value_json=excluded.value_json",
      [...args, m.value === undefined ? null : JSON.stringify(m.value)],
    );
  else
    db.run(
      "DELETE FROM streamsy_view_operator_index WHERE plan_name=? AND partition_key=? AND operator_id=? AND index_name=? AND index_key=? AND sort_key=? AND row_key=?",
      args,
    );
}

interface DecodedIndexRow {
  readonly sortKey: RowKey;
  readonly rowKey: RowKey;
  value?: JsonValue;
}
function decodeIndexRow(
  row: { sort_key: string; row_key: string; value_json: string | null },
  operatorId: string,
) {
  const entry: DecodedIndexRow = {
    sortKey: decodeKey(row.sort_key),
    rowKey: decodeKey(row.row_key),
  };
  if (row.value_json !== null)
    entry.value = decodeJson(
      "streamsy_view_operator_index",
      operatorId,
      row.row_key,
      row.value_json,
    );
  return entry;
}
function progress(db: Database, i: ViewIdentity): string | undefined {
  return (
    db
      .query<{ source_cursor: string | null }, [string, string]>(
        "SELECT source_cursor FROM streamsy_view_partitions WHERE plan_name=? AND partition_key=?",
      )
      .get(i.planName, i.partition)?.source_cursor ?? undefined
  );
}
function scanValues(db: Database, surface: string, n: NamespaceRef) {
  return db
    .query<{ value_key: string; value_json: string }, [string, string, string, string]>(
      "SELECT value_key,value_json FROM streamsy_view_values WHERE surface=? AND plan_name=? AND partition_key=? AND namespace_id=? ORDER BY value_key",
    )
    .all(surface, n.planName, n.partition, n.id)
    .map((row) => ({
      key: decodeKey(row.value_key),
      value: decodeJson(`streamsy_view_${surface}`, n.id, row.value_key, row.value_json),
    }));
}
function bounds(db: Database, i: ViewIdentity, relation?: string) {
  const clause =
    relation === undefined
      ? ""
      : " AND EXISTS (SELECT 1 FROM streamsy_view_changes c WHERE c.plan_name=b.plan_name AND c.partition_key=b.partition_key AND c.history_epoch=b.history_epoch AND c.history_seq=b.history_seq AND c.relation_id=?)";
  const args =
    relation === undefined ? [i.planName, i.partition] : [i.planName, i.partition, relation];
  const row = db
    .query<{ first: number | null; latest: number | null }, string[]>(
      `SELECT MIN(history_seq) first,MAX(history_seq) latest FROM streamsy_view_change_batches b WHERE plan_name=? AND partition_key=?${clause}`,
    )
    .get(...args);
  const epoch =
    db
      .query<{ history_epoch: number }, [string, string]>(
        "SELECT history_epoch FROM streamsy_view_partitions WHERE plan_name=? AND partition_key=?",
      )
      .get(i.planName, i.partition)?.history_epoch ?? 1;
  return { epoch, first: row?.first ?? undefined, latest: row?.latest ?? undefined };
}
function changesAfter(
  db: Database,
  i: ViewIdentity,
  position: HistoryPosition | undefined,
  limit: number,
  relation?: string,
): StoredChangeBatch[] {
  const p = db
    .query<
      { history_epoch: number; history_floor: number; next_history_seq: number },
      [string, string]
    >(
      "SELECT history_epoch,history_floor,next_history_seq FROM streamsy_view_partitions WHERE plan_name=? AND partition_key=?",
    )
    .get(i.planName, i.partition);
  const epoch = p?.history_epoch ?? 1;
  const floor = p?.history_floor ?? 1;
  const requested = position?.sequence ?? floor - 1;
  if (position !== undefined && (position.epoch !== epoch || requested < floor - 1))
    throw new ViewHistoryExpired({
      epoch,
      requested,
      first: floor,
      latest: (p?.next_history_seq ?? 1) - 1,
    });
  const batches = db
    .query<{ history_seq: number; source_cursor: string }, [string, string, number, number]>(
      "SELECT history_seq,source_cursor FROM streamsy_view_change_batches WHERE plan_name=? AND partition_key=? AND history_seq>? ORDER BY history_seq LIMIT ?",
    )
    .all(i.planName, i.partition, requested, limit);
  return batches
    .map((batch) => ({
      position: { epoch, sequence: batch.history_seq },
      sourceCursor: batch.source_cursor,
      changes: db
        .query<
          {
            ordinal: number;
            relation_id: string;
            kind: string;
            row_key: string;
            before_json: string | null;
            after_json: string | null;
          },
          [string, string, number]
        >(
          "SELECT ordinal,relation_id,kind,row_key,before_json,after_json FROM streamsy_view_changes WHERE plan_name=? AND partition_key=? AND history_seq=? ORDER BY ordinal",
        )
        .all(i.planName, i.partition, batch.history_seq)
        .filter((row) => relation === undefined || row.relation_id === relation)
        .map(decodeChange),
    }))
    .filter((batch) => relation === undefined || batch.changes.length > 0);
}
function decodeChange(row: {
  relation_id: string;
  kind: string;
  row_key: string;
  before_json: string | null;
  after_json: string | null;
}): StoredChange {
  const base = { relationId: row.relation_id, key: decodeKey(row.row_key) };
  if (row.kind === "enter")
    return {
      ...base,
      kind: "enter",
      after: decodeJson("streamsy_view_changes", row.relation_id, row.row_key, row.after_json!),
    };
  if (row.kind === "exit")
    return {
      ...base,
      kind: "exit",
      before: decodeJson("streamsy_view_changes", row.relation_id, row.row_key, row.before_json!),
    };
  return {
    ...base,
    kind: "update",
    before: decodeJson("streamsy_view_changes", row.relation_id, row.row_key, row.before_json!),
    after: decodeJson("streamsy_view_changes", row.relation_id, row.row_key, row.after_json!),
  };
}
function saveCheckpoint(db: Database, input: SaveCheckpoint): Checkpoint {
  return db.transaction(() => {
    const generation =
      (db
        .query<{ generation: number | null }, [string, string, string]>(
          "SELECT MAX(generation) generation FROM streamsy_view_checkpoint_manifests WHERE plan_name=? AND partition_key=? AND reducer_id=?",
        )
        .get(input.planName, input.partition, input.reducerId)?.generation ?? 0) + 1;
    db.run(
      "UPDATE streamsy_view_checkpoint_manifests SET status='retired' WHERE plan_name=? AND partition_key=? AND reducer_id=?",
      [input.planName, input.partition, input.reducerId],
    );
    db.run(
      "INSERT INTO streamsy_view_checkpoint_manifests VALUES (?,?,?,?,?,?,?,?,?,?, 'active')",
      [
        input.planName,
        input.partition,
        input.reducerId,
        generation,
        input.planHash,
        input.reducerVersion,
        input.sourceId,
        input.sourceCursor,
        input.createdAtMs,
        input.entries.length,
      ],
    );
    input.entries.forEach((entry) =>
      db.run("INSERT INTO streamsy_view_checkpoint_entries VALUES (?,?,?,?,?,?)", [
        input.planName,
        input.partition,
        input.reducerId,
        generation,
        encodeKey(entry.key),
        JSON.stringify(entry.value),
      ]),
    );
    const keep = input.keepGenerations ?? 2;
    db.run(
      "DELETE FROM streamsy_view_checkpoint_manifests WHERE plan_name=? AND partition_key=? AND reducer_id=? AND generation<=?",
      [input.planName, input.partition, input.reducerId, generation - keep],
    );
    return { ...input, generation };
  })();
}
function loadCheckpoint(
  db: Database,
  input: SaveCheckpoint | Omit<SaveCheckpoint, "sourceCursor" | "createdAtMs" | "entries">,
): Checkpoint | undefined {
  const row = db
    .query<
      { generation: number; source_cursor: string; created_at_ms: number; entry_count: number },
      [string, string, string, string, number, string]
    >(
      "SELECT generation,source_cursor,created_at_ms,entry_count FROM streamsy_view_checkpoint_manifests WHERE plan_name=? AND partition_key=? AND reducer_id=? AND plan_hash=? AND reducer_version=? AND source_id=? AND status='active' ORDER BY generation DESC LIMIT 1",
    )
    .get(
      input.planName,
      input.partition,
      input.reducerId,
      input.planHash,
      input.reducerVersion,
      input.sourceId,
    );
  if (row === null) {
    const anyGeneration = db
      .query<{ present: number }, [string, string, string]>(
        "SELECT 1 present FROM streamsy_view_checkpoint_manifests WHERE plan_name=? AND partition_key=? AND reducer_id=? AND status='active' LIMIT 1",
      )
      .get(input.planName, input.partition, input.reducerId);
    if (anyGeneration !== null)
      throw new ViewCheckpointIncompatible({
        reducerId: input.reducerId,
        reason: "no active generation matches the plan, source, and reducer version",
      });
    return undefined;
  }
  const entries = db
    .query<{ row_key: string; value_json: string }, [string, string, string, number]>(
      "SELECT row_key,value_json FROM streamsy_view_checkpoint_entries WHERE plan_name=? AND partition_key=? AND reducer_id=? AND generation=? ORDER BY row_key",
    )
    .all(input.planName, input.partition, input.reducerId, row.generation)
    .map((entry) => ({
      key: decodeKey(entry.row_key),
      value: decodeJson(
        "streamsy_view_checkpoint_entries",
        input.reducerId,
        entry.row_key,
        entry.value_json,
      ),
    }));
  if (entries.length !== row.entry_count)
    throw new ViewStateRestorePoison({
      table: "streamsy_view_checkpoint_entries",
      identity: input.reducerId,
      key: String(row.generation),
      detail: `expected ${row.entry_count} entries, found ${entries.length}`,
    });
  return {
    ...input,
    generation: row.generation,
    sourceCursor: row.source_cursor,
    createdAtMs: row.created_at_ms,
    entries,
  };
}
