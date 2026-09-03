import type { Database } from "bun:sqlite";

export const VIEW_SCHEMA_VERSION = 1;
export const VIEW_MIGRATIONS = [
  `
CREATE TABLE streamsy_view_partitions (
 plan_name TEXT NOT NULL, partition_key TEXT NOT NULL, plan_hash TEXT NOT NULL, source_id TEXT NOT NULL,
 source_cursor TEXT, history_epoch INTEGER NOT NULL DEFAULT 1, next_history_seq INTEGER NOT NULL DEFAULT 1,
 history_floor INTEGER NOT NULL DEFAULT 1, updated_at_ms INTEGER NOT NULL,
 PRIMARY KEY(plan_name, partition_key)
);
CREATE TABLE streamsy_view_values (
 surface TEXT NOT NULL, plan_name TEXT NOT NULL, partition_key TEXT NOT NULL, namespace_id TEXT NOT NULL,
 value_key TEXT NOT NULL, value_json TEXT NOT NULL,
 PRIMARY KEY(surface, plan_name, partition_key, namespace_id, value_key)
);
CREATE TABLE streamsy_view_operator_index (
 plan_name TEXT NOT NULL, partition_key TEXT NOT NULL, operator_id TEXT NOT NULL, index_name TEXT NOT NULL,
 index_key TEXT NOT NULL, sort_key TEXT NOT NULL, row_key TEXT NOT NULL, value_json TEXT,
 PRIMARY KEY(plan_name, partition_key, operator_id, index_name, index_key, sort_key, row_key)
);
CREATE TABLE streamsy_view_change_batches (
 plan_name TEXT NOT NULL, partition_key TEXT NOT NULL, history_epoch INTEGER NOT NULL, history_seq INTEGER NOT NULL,
 source_id TEXT NOT NULL, source_cursor TEXT NOT NULL, batch_id TEXT NOT NULL, plan_hash TEXT NOT NULL, committed_at_ms INTEGER NOT NULL,
 PRIMARY KEY(plan_name, partition_key, history_epoch, history_seq),
 UNIQUE(plan_name, partition_key, source_id, source_cursor), UNIQUE(plan_name, partition_key, batch_id)
);
CREATE TABLE streamsy_view_changes (
 plan_name TEXT NOT NULL, partition_key TEXT NOT NULL, history_epoch INTEGER NOT NULL, history_seq INTEGER NOT NULL,
 ordinal INTEGER NOT NULL, relation_id TEXT NOT NULL, kind TEXT NOT NULL, row_key TEXT NOT NULL, before_json TEXT, after_json TEXT,
 PRIMARY KEY(plan_name, partition_key, history_epoch, history_seq, ordinal),
 FOREIGN KEY(plan_name, partition_key, history_epoch, history_seq) REFERENCES streamsy_view_change_batches(plan_name, partition_key, history_epoch, history_seq) ON DELETE CASCADE
);
CREATE TABLE streamsy_view_checkpoint_manifests (
 plan_name TEXT NOT NULL, partition_key TEXT NOT NULL, reducer_id TEXT NOT NULL, generation INTEGER NOT NULL,
 plan_hash TEXT NOT NULL, reducer_version INTEGER NOT NULL, source_id TEXT NOT NULL, source_cursor TEXT NOT NULL,
 created_at_ms INTEGER NOT NULL, entry_count INTEGER NOT NULL, status TEXT NOT NULL,
 PRIMARY KEY(plan_name, partition_key, reducer_id, generation)
);
CREATE TABLE streamsy_view_checkpoint_entries (
 plan_name TEXT NOT NULL, partition_key TEXT NOT NULL, reducer_id TEXT NOT NULL, generation INTEGER NOT NULL,
 row_key TEXT NOT NULL, value_json TEXT NOT NULL,
 PRIMARY KEY(plan_name, partition_key, reducer_id, generation, row_key),
 FOREIGN KEY(plan_name, partition_key, reducer_id, generation) REFERENCES streamsy_view_checkpoint_manifests(plan_name, partition_key, reducer_id, generation) ON DELETE CASCADE
);
CREATE INDEX streamsy_view_value_scan ON streamsy_view_values(surface, plan_name, partition_key, namespace_id, value_key);
CREATE INDEX streamsy_view_index_lookup ON streamsy_view_operator_index(plan_name, partition_key, operator_id, index_name, index_key, sort_key, row_key);
`,
];

export function migrateViewStore(
  database: Database,
  now = sqliteCurrentTimeMillis(database),
): void {
  database.run(
    "CREATE TABLE IF NOT EXISTS streamsy_view_schema_version(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)",
  );
  const current =
    database
      .query<{ version: number | null }, []>(
        "SELECT MAX(version) version FROM streamsy_view_schema_version",
      )
      .get()?.version ?? 0;
  for (let version = current + 1; version <= VIEW_MIGRATIONS.length; version++)
    database.transaction(() => {
      database.run(VIEW_MIGRATIONS[version - 1]!);
      database.run("INSERT INTO streamsy_view_schema_version VALUES (?, ?)", [version, now]);
    })();
}

export interface LegacyImport {
  readonly planName: string;
  readonly planHash: string;
  readonly partition: string;
  readonly sourceId: string;
  readonly relationId: string;
  readonly reducerId: string;
  readonly reducerVersion: number;
}
/** Import the accepted Slice 1 tables once, leaving them intact for rollback. */
export function importLegacyIssueStore(
  database: Database,
  config: LegacyImport,
  now = sqliteCurrentTimeMillis(database),
): void {
  database.run(
    "CREATE TABLE IF NOT EXISTS streamsy_view_legacy_import(import_name TEXT PRIMARY KEY, imported_at_ms INTEGER NOT NULL)",
  );
  const tables = new Set(
    database
      .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='table'")
      .all()
      .map((row) => row.name),
  );
  if (
    !tables.has("view_rows") ||
    !tables.has("reducer_state") ||
    !tables.has("view_progress") ||
    database
      .query("SELECT 1 FROM streamsy_view_legacy_import WHERE import_name='issue-tracker-v1'")
      .get() !== null
  )
    return;
  database.transaction(() => {
    const progress = database
      .query<{ checkpoint: string | null }, [string]>(
        "SELECT checkpoint FROM view_progress WHERE workspace_id=?",
      )
      .get(config.partition);
    database.run(
      "INSERT OR IGNORE INTO streamsy_view_partitions VALUES (?, ?, ?, ?, ?, 1, 1, 1, ?)",
      [
        config.planName,
        config.partition,
        config.planHash,
        config.sourceId,
        progress?.checkpoint ?? null,
        now,
      ],
    );
    for (const surface of [
      { old: "view_rows", next: "rows", id: config.relationId },
      { old: "reducer_state", next: "reducer", id: config.reducerId },
    ])
      database.run(
        `INSERT OR IGNORE INTO streamsy_view_values SELECT '${surface.next}', ?, workspace_id, ?, json_quote(row_key), value FROM ${surface.old}`,
        [config.planName, surface.id],
      );
    database.run("INSERT INTO streamsy_view_legacy_import VALUES ('issue-tracker-v1', ?)", [now]);
  })();
}

function sqliteCurrentTimeMillis(database: Database): number {
  const row = database
    .query<{ now_ms: number }, []>("SELECT CAST(unixepoch('subsec') * 1000 AS INTEGER) now_ms")
    .get();
  if (row === null) throw new Error("SQLite did not provide the current time");
  return row.now_ms;
}
