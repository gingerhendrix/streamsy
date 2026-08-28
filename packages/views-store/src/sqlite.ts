import { Effect, Layer, Schema } from "effect";
import type { SqlClient } from "effect/unstable/sql/SqlClient";
import * as SqlClientTag from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import type {
  Checkpoint,
  HistoryPosition,
  IndexMutation,
  JsonValue,
  NamespaceRef,
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

export {
  importLegacyIssueStore,
  migrateViewStore,
  VIEW_MIGRATIONS,
  VIEW_SCHEMA_VERSION,
  type LegacyImport,
} from "./sqlite-schema.ts";
import { VIEW_MIGRATIONS } from "./sqlite-schema.ts";

const isCheckpointIncompatible = Schema.is(ViewCheckpointIncompatible);
const isCursorConflict = Schema.is(ViewCursorConflict);
const isHistoryExpired = Schema.is(ViewHistoryExpired);
const isRestorePoison = Schema.is(ViewStateRestorePoison);
const JsonString = Schema.fromJsonString(Schema.Unknown);
const encodeJsonString = Schema.encodeUnknownSync(JsonString);
type ViewSqlError =
  | ViewCheckpointIncompatible
  | ViewCursorConflict
  | ViewHistoryExpired
  | ViewStateRestorePoison
  | ViewStoreUnavailable;
const fail = (operation: string, cause: ViewSqlError | SqlError): ViewSqlError =>
  isCheckpointIncompatible(cause) ||
  isCursorConflict(cause) ||
  isHistoryExpired(cause) ||
  isRestorePoison(cause)
    ? cause
    : new ViewStoreUnavailable({
        operation,
        detail: cause instanceof Error ? cause.message : String(cause),
      });
const attempt = <A>(
  operation: string,
  effect: Effect.Effect<A, ViewSqlError | SqlError>,
): Effect.Effect<A, ViewSqlError> =>
  effect.pipe(Effect.mapError((cause) => fail(operation, cause)));
const first = <A>(rows: ReadonlyArray<A>): A | undefined => rows[0];

/** Apply the maintained-view schema through any Effect SQL SQLite client. */
export function migrateViewStoreSql(sql: SqlClient): Effect.Effect<void, SqlError> {
  return Effect.gen(function* () {
    yield* sql
      .unsafe<Record<string, never>>(
        "CREATE TABLE IF NOT EXISTS streamsy_view_schema_version(" +
          "version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)",
      )
      .pipe(Effect.asVoid);
    const current = yield* sql
      .unsafe<{ readonly version: number | null }>(
        "SELECT MAX(version) version FROM streamsy_view_schema_version",
      )
      .pipe(Effect.map((rows) => first(rows)?.version ?? 0));
    for (let version = current + 1; version <= VIEW_MIGRATIONS.length; version++) {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql
            .unsafe<Record<string, never>>(VIEW_MIGRATIONS[version - 1]!)
            .pipe(Effect.asVoid);
          yield* sql
            .unsafe<Record<string, never>>(
              "INSERT INTO streamsy_view_schema_version(version, applied_at_ms)" +
                " VALUES (?, CAST(unixepoch('subsec') * 1000 AS INTEGER))",
              [version],
            )
            .pipe(Effect.asVoid);
        }),
      );
    }
  });
}

export const sqliteLayer: Layer.Layer<ViewStore, never, SqlClient> = Layer.effect(
  ViewStore,
  Effect.map(SqlClientTag.SqlClient, sqliteService),
);

export function sqliteService(sql: SqlClient): ViewStoreService {
  const queryAll = <A extends object>(statement: string, params: ReadonlyArray<unknown> = []) =>
    sql.unsafe<A>(statement, params);
  const queryFirst = <A extends object>(statement: string, params: ReadonlyArray<unknown> = []) =>
    queryAll<A>(statement, params).pipe(Effect.map(first));
  const execute = (statement: string, params: ReadonlyArray<unknown> = []) =>
    queryAll<Record<string, never>>(statement, params).pipe(Effect.asVoid);

  const readValue = (surface: string, n: NamespaceRef, key: RowKey) =>
    attempt(
      `get:${surface}`,
      queryFirst<{ readonly value_json: string }>(
        "SELECT value_json FROM streamsy_view_values" +
          " WHERE surface=? AND plan_name=? AND partition_key=? AND namespace_id=? AND value_key=?",
        [surface, n.planName, n.partition, n.id, encodeKey(key)],
      ).pipe(
        Effect.map((row) =>
          row === undefined
            ? undefined
            : decodeJson(`streamsy_view_${surface}`, n.id, encodeKey(key), row.value_json),
        ),
      ),
    );

  return ViewStore.of({
    commit: Effect.fn("ViewStore.commit")((input, retention = {}) =>
      attempt(
        "commit",
        sql.withTransaction(
          Effect.gen(function* () {
            const i = input.identity;
            const current = yield* queryFirst<{
              readonly plan_hash: string;
              readonly source_cursor: string | null;
              readonly history_epoch: number;
              readonly next_history_seq: number;
              readonly history_floor: number;
            }>(
              "SELECT plan_hash,source_cursor,history_epoch,next_history_seq,history_floor" +
                " FROM streamsy_view_partitions WHERE plan_name=? AND partition_key=?",
              [i.planName, i.partition],
            );
            if (current !== undefined && current.plan_hash !== i.planHash)
              return yield* new ViewCheckpointIncompatible({
                reducerId: i.planName,
                reason: `stored plan ${current.plan_hash} does not match ${i.planHash}`,
              });
            const duplicate = yield* queryFirst<{
              readonly history_epoch: number;
              readonly history_seq: number;
              readonly batch_id: string;
            }>(
              "SELECT history_epoch,history_seq,batch_id FROM streamsy_view_change_batches" +
                " WHERE plan_name=? AND partition_key=? AND source_id=? AND source_cursor=?",
              [i.planName, i.partition, i.sourceId, input.afterExclusiveCursor],
            );
            if (duplicate !== undefined && duplicate.batch_id !== input.batchId)
              return yield* new ViewCheckpointIncompatible({
                reducerId: i.planName,
                reason:
                  `source cursor ${input.afterExclusiveCursor}` +
                  ` is already committed as batch ${duplicate.batch_id}`,
              });
            if (duplicate !== undefined)
              return { epoch: duplicate.history_epoch, sequence: duplicate.history_seq };
            const actual = current?.source_cursor ?? undefined;
            if (actual !== input.expectedCursor)
              return yield* new ViewCursorConflict({
                planName: i.planName,
                partition: i.partition,
                expected: input.expectedCursor ?? null,
                actual: actual ?? null,
              });
            const epoch = current?.history_epoch ?? 1;
            const sequence = current?.next_history_seq ?? 1;
            yield* execute(
              "INSERT INTO streamsy_view_partitions VALUES (?,?,?,?,?,?,?,1,?)" +
                " ON CONFLICT(plan_name,partition_key) DO UPDATE SET" +
                " plan_hash=excluded.plan_hash,source_id=excluded.source_id," +
                " source_cursor=excluded.source_cursor,next_history_seq=excluded.next_history_seq," +
                " updated_at_ms=excluded.updated_at_ms",
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
            ] as const) {
              for (const mutation of mutations) {
                if (mutation.kind === "put") {
                  yield* execute(
                    "INSERT INTO streamsy_view_values VALUES (?,?,?,?,?,?)" +
                      " ON CONFLICT DO UPDATE SET value_json=excluded.value_json",
                    [
                      surface,
                      i.planName,
                      i.partition,
                      mutation.namespace.id,
                      encodeKey(mutation.key),
                      encodeJsonString(mutation.value),
                    ],
                  );
                } else {
                  yield* execute(
                    "DELETE FROM streamsy_view_values" +
                      " WHERE surface=? AND plan_name=? AND partition_key=?" +
                      " AND namespace_id=? AND value_key=?",
                    [
                      surface,
                      i.planName,
                      i.partition,
                      mutation.namespace.id,
                      encodeKey(mutation.key),
                    ],
                  );
                }
              }
            }
            for (const mutation of input.operatorIndexes ?? []) {
              yield* writeIndex(sql, mutation);
            }
            yield* execute("INSERT INTO streamsy_view_change_batches VALUES (?,?,?,?,?,?,?,?,?)", [
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
            for (const [ordinal, change] of (input.changes ?? []).entries()) {
              yield* execute("INSERT INTO streamsy_view_changes VALUES (?,?,?,?,?,?,?,?,?,?)", [
                i.planName,
                i.partition,
                epoch,
                sequence,
                ordinal,
                change.relationId,
                change.kind,
                encodeKey(change.key),
                "before" in change ? encodeJsonString(change.before) : null,
                "after" in change ? encodeJsonString(change.after) : null,
              ]);
            }
            const keep = retention.keepLastBatches;
            const cutoff =
              retention.keepForMilliseconds === undefined
                ? undefined
                : input.committedAtMs - retention.keepForMilliseconds;
            if (keep !== undefined || cutoff !== undefined) {
              const rows = yield* queryAll<{
                readonly history_seq: number;
                readonly committed_at_ms: number;
              }>(
                "SELECT history_seq,committed_at_ms FROM streamsy_view_change_batches" +
                  " WHERE plan_name=? AND partition_key=? ORDER BY history_seq",
                [i.planName, i.partition],
              );
              let floor = current?.history_floor ?? 1;
              for (const [index, row] of rows.entries()) {
                if (
                  (keep !== undefined && index < rows.length - keep) ||
                  (cutoff !== undefined && row.committed_at_ms < cutoff)
                ) {
                  yield* execute(
                    "DELETE FROM streamsy_view_change_batches" +
                      " WHERE plan_name=? AND partition_key=? AND history_epoch=? AND history_seq=?",
                    [i.planName, i.partition, epoch, row.history_seq],
                  );
                  floor = Math.max(floor, row.history_seq + 1);
                }
              }
              yield* execute(
                "UPDATE streamsy_view_partitions SET history_floor=?" +
                  " WHERE plan_name=? AND partition_key=?",
                [floor, i.planName, i.partition],
              );
            }
            return { epoch, sequence };
          }),
        ),
      ),
    ),
    getRow: Effect.fn("ViewStore.getRow")((n, k) => readValue("rows", n, k)),
    snapshotRows: Effect.fn("ViewStore.snapshotRows")((n) =>
      attempt("snapshotRows", sql.withTransaction(snapshotRows(sql, n))),
    ),
    getOperatorValue: Effect.fn("ViewStore.getOperatorValue")((n, k) =>
      readValue("operator", n, k),
    ),
    scanOperatorValues: Effect.fn("ViewStore.scanOperatorValues")((n) =>
      attempt("scanOperatorValues", scanValues(sql, "operator", n)),
    ),
    lookupIndex: Effect.fn("ViewStore.lookupIndex")((n, name, key, range = {}) =>
      attempt(
        "lookupIndex",
        queryAll<{
          readonly sort_key: string;
          readonly row_key: string;
          readonly value_json: string | null;
        }>(
          "SELECT sort_key,row_key,value_json FROM streamsy_view_operator_index" +
            " WHERE plan_name=? AND partition_key=? AND operator_id=? AND index_name=? AND index_key=?" +
            " ORDER BY sort_key,row_key",
          [n.planName, n.partition, n.id, name, encodeKey(key)],
        ).pipe(
          Effect.map((rows) =>
            rows
              .map((row) => decodeIndexRow(row, n.id))
              .filter(
                (entry) =>
                  (range.from === undefined || encodeKey(entry.sortKey) >= encodeKey(range.from)) &&
                  (range.to === undefined || encodeKey(entry.sortKey) <= encodeKey(range.to)),
              )
              .slice(0, range.limit),
          ),
        ),
      ),
    ),
    getReducerState: Effect.fn("ViewStore.getReducerState")((n, k) => readValue("reducer", n, k)),
    sourceProgress: Effect.fn("ViewStore.sourceProgress")((i) =>
      attempt("sourceProgress", progress(sql, i)),
    ),
    historyBounds: Effect.fn("ViewStore.historyBounds")((i, relation) =>
      attempt("historyBounds", sql.withTransaction(bounds(sql, i, relation))),
    ),
    changesAfter: Effect.fn("ViewStore.changesAfter")((i, position, limit, relation) =>
      attempt("changesAfter", sql.withTransaction(changesAfter(sql, i, position, limit, relation))),
    ),
    saveCheckpoint: Effect.fn("ViewStore.saveCheckpoint")((input) =>
      attempt("saveCheckpoint", saveCheckpoint(sql, input)),
    ),
    loadCheckpoint: Effect.fn("ViewStore.loadCheckpoint")((input) =>
      attempt("loadCheckpoint", sql.withTransaction(loadCheckpoint(sql, input))),
    ),
  });
}

function writeIndex(sql: SqlClient, m: IndexMutation) {
  const args = [
    m.namespace.planName,
    m.namespace.partition,
    m.namespace.id,
    m.indexName,
    encodeKey(m.indexKey),
    encodeKey(m.sortKey),
    encodeKey(m.rowKey),
  ];
  return m.kind === "put"
    ? sql
        .unsafe<Record<string, never>>(
          "INSERT INTO streamsy_view_operator_index VALUES (?,?,?,?,?,?,?,?)" +
            " ON CONFLICT DO UPDATE SET value_json=excluded.value_json",
          [...args, m.value === undefined ? null : JSON.stringify(m.value)],
        )
        .pipe(Effect.asVoid)
    : sql
        .unsafe<Record<string, never>>(
          "DELETE FROM streamsy_view_operator_index" +
            " WHERE plan_name=? AND partition_key=? AND operator_id=?" +
            " AND index_name=? AND index_key=? AND sort_key=? AND row_key=?",
          args,
        )
        .pipe(Effect.asVoid);
}

interface DecodedIndexRow {
  readonly sortKey: RowKey;
  readonly rowKey: RowKey;
  value?: JsonValue;
}

function decodeIndexRow(
  row: { readonly sort_key: string; readonly row_key: string; readonly value_json: string | null },
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

function progress(sql: SqlClient, i: ViewIdentity) {
  return sql
    .unsafe<{ readonly source_cursor: string | null }>(
      "SELECT source_cursor FROM streamsy_view_partitions WHERE plan_name=? AND partition_key=?",
      [i.planName, i.partition],
    )
    .pipe(Effect.map((rows) => rows[0]?.source_cursor ?? undefined));
}

function scanValues(sql: SqlClient, surface: string, n: NamespaceRef) {
  return sql
    .unsafe<{ readonly value_key: string; readonly value_json: string }>(
      "SELECT value_key,value_json FROM streamsy_view_values" +
        " WHERE surface=? AND plan_name=? AND partition_key=? AND namespace_id=?" +
        " ORDER BY value_key",
      [surface, n.planName, n.partition, n.id],
    )
    .pipe(
      Effect.map((rows) =>
        rows.map((row) => ({
          key: decodeKey(row.value_key),
          value: decodeJson(`streamsy_view_${surface}`, n.id, row.value_key, row.value_json),
        })),
      ),
    );
}

function bounds(sql: SqlClient, i: ViewIdentity, relation?: string) {
  const relationClause =
    relation === undefined
      ? ""
      : " AND EXISTS (SELECT 1 FROM streamsy_view_changes c" +
        " WHERE c.plan_name=b.plan_name AND c.partition_key=b.partition_key" +
        " AND c.history_epoch=b.history_epoch AND c.history_seq=b.history_seq" +
        " AND c.relation_id=?)";
  return sql
    .unsafe<{
      readonly history_epoch: number | null;
      readonly first: number | null;
      readonly latest: number | null;
    }>(
      "SELECT MIN(history_seq) first,MAX(history_seq) latest," +
        " (SELECT history_epoch FROM streamsy_view_partitions" +
        " WHERE plan_name=? AND partition_key=?) history_epoch" +
        " FROM streamsy_view_change_batches b WHERE plan_name=? AND partition_key=?" +
        relationClause,
      relation === undefined
        ? [i.planName, i.partition, i.planName, i.partition]
        : [i.planName, i.partition, i.planName, i.partition, relation],
    )
    .pipe(
      Effect.map(first),
      Effect.map((row) => ({
        epoch: row?.history_epoch ?? 1,
        first: row?.first ?? undefined,
        latest: row?.latest ?? undefined,
      })),
    );
}

function snapshotRows(sql: SqlClient, n: NamespaceRef) {
  return sql
    .unsafe<{
      readonly source_cursor: string | null;
      readonly value_key: string | null;
      readonly value_json: string | null;
    }>(
      "SELECT p.source_cursor,v.value_key,v.value_json" +
        " FROM (SELECT source_cursor FROM streamsy_view_partitions" +
        " WHERE plan_name=? AND partition_key=?) p" +
        " LEFT JOIN streamsy_view_values v ON v.surface='rows'" +
        " AND v.plan_name=? AND v.partition_key=? AND v.namespace_id=?" +
        " ORDER BY v.value_key",
      [n.planName, n.partition, n.planName, n.partition, n.id],
    )
    .pipe(
      Effect.map((found) => ({
        sourceCursor: found[0]?.source_cursor ?? undefined,
        rows: found.flatMap((row) =>
          row.value_key === null || row.value_json === null
            ? []
            : [
                {
                  key: decodeKey(row.value_key),
                  value: decodeJson("streamsy_view_rows", n.id, row.value_key, row.value_json),
                },
              ],
        ),
      })),
    );
}

function changesAfter(
  sql: SqlClient,
  i: ViewIdentity,
  position: HistoryPosition | undefined,
  limit: number,
  relation?: string,
) {
  return sql
    .unsafe<{
      readonly epoch: number;
      readonly floor: number;
      readonly next_sequence: number;
      readonly history_seq: number | null;
      readonly source_cursor: string | null;
      readonly ordinal: number | null;
      readonly relation_id: string | null;
      readonly kind: string | null;
      readonly row_key: string | null;
      readonly before_json: string | null;
      readonly after_json: string | null;
    }>(
      "WITH p AS (SELECT" +
        " COALESCE((SELECT history_epoch FROM streamsy_view_partitions" +
        " WHERE plan_name=? AND partition_key=?),1) epoch," +
        " COALESCE((SELECT history_floor FROM streamsy_view_partitions" +
        " WHERE plan_name=? AND partition_key=?),1) floor," +
        " COALESCE((SELECT next_history_seq FROM streamsy_view_partitions" +
        " WHERE plan_name=? AND partition_key=?),1) next_sequence)," +
        " selected AS (SELECT b.history_seq,b.source_cursor FROM streamsy_view_change_batches b,p" +
        " WHERE b.plan_name=? AND b.partition_key=?" +
        " AND b.history_seq>COALESCE(?,p.floor-1) ORDER BY b.history_seq LIMIT ?)" +
        " SELECT p.epoch,p.floor,p.next_sequence,b.history_seq,b.source_cursor," +
        " c.ordinal,c.relation_id,c.kind,c.row_key,c.before_json,c.after_json" +
        " FROM p LEFT JOIN selected b ON 1=1 LEFT JOIN streamsy_view_changes c" +
        " ON c.plan_name=? AND c.partition_key=? AND c.history_epoch=p.epoch" +
        " AND c.history_seq=b.history_seq ORDER BY b.history_seq,c.ordinal",
      [
        i.planName,
        i.partition,
        i.planName,
        i.partition,
        i.planName,
        i.partition,
        i.planName,
        i.partition,
        position?.sequence ?? null,
        limit,
        i.planName,
        i.partition,
      ],
    )
    .pipe(
      Effect.flatMap((rows) => {
        const state = rows[0] ?? { epoch: 1, floor: 1, next_sequence: 1 };
        const requested = position?.sequence ?? state.floor - 1;
        if (
          position !== undefined &&
          (position.epoch !== state.epoch || requested < state.floor - 1)
        ) {
          return Effect.fail(
            new ViewHistoryExpired({
              epoch: state.epoch,
              requested,
              first: state.floor,
              latest: state.next_sequence - 1,
            }),
          );
        }
        const grouped = new Map<number, { sourceCursor: string; changes: StoredChange[] }>();
        for (const row of rows) {
          if (row.history_seq === null || row.source_cursor === null) continue;
          let batch = grouped.get(row.history_seq);
          if (batch === undefined) {
            batch = { sourceCursor: row.source_cursor, changes: [] };
            grouped.set(row.history_seq, batch);
          }
          if (
            row.ordinal !== null &&
            row.relation_id !== null &&
            row.kind !== null &&
            row.row_key !== null &&
            (relation === undefined || row.relation_id === relation)
          ) {
            batch.changes.push(
              decodeChange({
                relation_id: row.relation_id,
                kind: row.kind,
                row_key: row.row_key,
                before_json: row.before_json,
                after_json: row.after_json,
              }),
            );
          }
        }
        const restored: StoredChangeBatch[] = [];
        for (const [sequence, batch] of grouped) {
          if (relation === undefined || batch.changes.length > 0) {
            restored.push({
              position: { epoch: state.epoch, sequence },
              sourceCursor: batch.sourceCursor,
              changes: batch.changes,
            });
          }
        }
        return Effect.succeed(restored);
      }),
    );
}

function decodeChange(row: {
  readonly relation_id: string;
  readonly kind: string;
  readonly row_key: string;
  readonly before_json: string | null;
  readonly after_json: string | null;
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

function saveCheckpoint(sql: SqlClient, input: SaveCheckpoint) {
  return sql.withTransaction(
    Effect.gen(function* () {
      const current = yield* sql
        .unsafe<{ readonly generation: number | null }>(
          "SELECT MAX(generation) generation FROM streamsy_view_checkpoint_manifests" +
            " WHERE plan_name=? AND partition_key=? AND reducer_id=?",
          [input.planName, input.partition, input.reducerId],
        )
        .pipe(Effect.map(first));
      const generation = (current?.generation ?? 0) + 1;
      yield* sql
        .unsafe<Record<string, never>>(
          "UPDATE streamsy_view_checkpoint_manifests SET status='retired'" +
            " WHERE plan_name=? AND partition_key=? AND reducer_id=?",
          [input.planName, input.partition, input.reducerId],
        )
        .pipe(Effect.asVoid);
      yield* sql
        .unsafe<Record<string, never>>(
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
        )
        .pipe(Effect.asVoid);
      for (const entry of input.entries) {
        yield* sql
          .unsafe<Record<string, never>>(
            "INSERT INTO streamsy_view_checkpoint_entries VALUES (?,?,?,?,?,?)",
            [
              input.planName,
              input.partition,
              input.reducerId,
              generation,
              encodeKey(entry.key),
              encodeJsonString(entry.value),
            ],
          )
          .pipe(Effect.asVoid);
      }
      const keep = input.keepGenerations ?? 2;
      yield* sql
        .unsafe<Record<string, never>>(
          "DELETE FROM streamsy_view_checkpoint_manifests" +
            " WHERE plan_name=? AND partition_key=? AND reducer_id=? AND generation<=?",
          [input.planName, input.partition, input.reducerId, generation - keep],
        )
        .pipe(Effect.asVoid);
      return { ...input, generation };
    }),
  );
}

function loadCheckpoint(
  sql: SqlClient,
  input: SaveCheckpoint | Omit<SaveCheckpoint, "sourceCursor" | "createdAtMs" | "entries">,
): Effect.Effect<
  Checkpoint | undefined,
  SqlError | ViewCheckpointIncompatible | ViewStateRestorePoison
> {
  return Effect.gen(function* () {
    const rows = yield* sql.unsafe<{
      readonly active_present: number;
      readonly generation: number | null;
      readonly source_cursor: string | null;
      readonly created_at_ms: number | null;
      readonly entry_count: number | null;
      readonly row_key: string | null;
      readonly value_json: string | null;
    }>(
      "WITH target AS (SELECT generation,source_cursor,created_at_ms,entry_count" +
        " FROM streamsy_view_checkpoint_manifests" +
        " WHERE plan_name=? AND partition_key=? AND reducer_id=? AND plan_hash=?" +
        " AND reducer_version=? AND source_id=? AND status='active'" +
        " ORDER BY generation DESC LIMIT 1)," +
        " presence AS (SELECT EXISTS(SELECT 1 FROM streamsy_view_checkpoint_manifests" +
        " WHERE plan_name=? AND partition_key=? AND reducer_id=? AND status='active') active_present)" +
        " SELECT presence.active_present,target.generation,target.source_cursor," +
        " target.created_at_ms,target.entry_count,e.row_key,e.value_json" +
        " FROM presence LEFT JOIN target ON 1=1 LEFT JOIN streamsy_view_checkpoint_entries e" +
        " ON e.plan_name=? AND e.partition_key=? AND e.reducer_id=?" +
        " AND e.generation=target.generation ORDER BY e.row_key",
      [
        input.planName,
        input.partition,
        input.reducerId,
        input.planHash,
        input.reducerVersion,
        input.sourceId,
        input.planName,
        input.partition,
        input.reducerId,
        input.planName,
        input.partition,
        input.reducerId,
      ],
    );
    const row = rows[0];
    if (row === undefined || row.generation === null) {
      if (row?.active_present === 1) {
        return yield* new ViewCheckpointIncompatible({
          reducerId: input.reducerId,
          reason: "no active generation matches the plan, source, and reducer version",
        });
      }
      return undefined;
    }
    const entries = rows.flatMap((entry) =>
      entry.row_key === null || entry.value_json === null
        ? []
        : [
            {
              key: decodeKey(entry.row_key),
              value: decodeJson(
                "streamsy_view_checkpoint_entries",
                input.reducerId,
                entry.row_key,
                entry.value_json,
              ),
            },
          ],
    );
    const expected = row.entry_count ?? 0;
    if (entries.length !== expected) {
      return yield* new ViewStateRestorePoison({
        table: "streamsy_view_checkpoint_entries",
        identity: input.reducerId,
        key: String(row.generation),
        detail: `expected ${expected} entries, found ${entries.length}`,
      });
    }
    if (row.source_cursor === null || row.created_at_ms === null) {
      return yield* Effect.die("active checkpoint manifest contains null required fields");
    }
    return {
      ...input,
      generation: row.generation,
      sourceCursor: row.source_cursor,
      createdAtMs: row.created_at_ms,
      entries,
    };
  });
}
