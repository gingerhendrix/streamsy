import { Effect } from "effect";
import { StorageFault } from "@streamsy/core";
import type { SqlClient } from "effect/unstable/sql/SqlClient";
import { isSqlError, type SqlError } from "effect/unstable/sql/SqlError";
import { STORAGE_MIGRATIONS, STORAGE_SCHEMA_VERSION, STORAGE_VERSION_TABLE } from "./schema.ts";

interface TableRow {
  readonly name: string;
}

interface VersionRow {
  readonly version: number | null;
}

export interface MigrationHooks {
  readonly beforeVersion?: (version: number) => Effect.Effect<void, StorageFault>;
}

const fault = (operation: string, error: SqlError): StorageFault =>
  new StorageFault({
    operation,
    message: error.message,
    retryable: error.isRetryable,
    cause: error,
  });

const sqlAttempt = <A>(operation: string, effect: Effect.Effect<A, SqlError>) =>
  effect.pipe(Effect.mapError((error) => fault(operation, error)));

/**
 * Migrate one fresh-format database under a single write transaction. The
 * legacy check runs before the first write, and every schema/version statement
 * rolls back together if acquisition is interrupted or a migration fails.
 */
export const migrate = (
  sql: SqlClient,
  hooks: MigrationHooks = {},
): Effect.Effect<void, StorageFault> =>
  sql
    .withTransaction(
      Effect.gen(function* () {
        const tables = yield* sqlAttempt(
          "migration.inspect",
          sql.unsafe<TableRow>("SELECT name FROM sqlite_master WHERE type = 'table'"),
        );
        const names = new Set(tables.map(({ name }) => name));
        const ownsFormat = names.has(STORAGE_VERSION_TABLE);
        const legacy =
          names.has("streamsy_schema_version") ||
          (!ownsFormat &&
            ["streamsy_streams", "streamsy_messages", "streamsy_producers"].some((name) =>
              names.has(name),
            ));
        if (legacy)
          return yield* new StorageFault({
            operation: "migration.open",
            message:
              "Unsupported pre-0.4 Streamsy SQLite format; use a fresh database path (the file was not modified)",
            retryable: false,
          });

        if (!ownsFormat)
          yield* sqlAttempt(
            "migration.create-version-table",
            sql
              .unsafe(
                `CREATE TABLE ${STORAGE_VERSION_TABLE} (` +
                  "version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)",
              )
              .pipe(Effect.asVoid),
          );

        const current = yield* sqlAttempt(
          "migration.read-version",
          sql
            .unsafe<VersionRow>(`SELECT MAX(version) version FROM ${STORAGE_VERSION_TABLE}`)
            .pipe(Effect.map((rows) => rows[0]?.version ?? 0)),
        );
        if (current > STORAGE_SCHEMA_VERSION)
          return yield* new StorageFault({
            operation: "migration.open",
            message: `Unsupported newer Streamsy SQLite schema version ${current}; supported version is ${STORAGE_SCHEMA_VERSION}`,
            retryable: false,
          });

        for (let version = current + 1; version <= STORAGE_SCHEMA_VERSION; version++) {
          if (hooks.beforeVersion !== undefined) yield* hooks.beforeVersion(version);
          const statements = STORAGE_MIGRATIONS[version - 1];
          if (statements === undefined) return yield* Effect.die(new Error("Missing migration"));
          for (const statement of statements)
            yield* sqlAttempt(
              `migration.apply.${version}`,
              sql.unsafe(statement).pipe(Effect.asVoid),
            );
          yield* sqlAttempt(
            `migration.record.${version}`,
            sql
              .unsafe(
                `INSERT INTO ${STORAGE_VERSION_TABLE}(version, applied_at_ms) ` +
                  "VALUES (?, CAST(unixepoch('subsec') * 1000 AS INTEGER))",
                [version],
              )
              .pipe(Effect.asVoid),
          );
        }
        return undefined;
      }),
    )
    .pipe(Effect.catchIf(isSqlError, (error) => Effect.die(error)));
