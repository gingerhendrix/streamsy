import { Clock, Effect } from "effect";
import { StorageFault } from "@streamsy/core";
import type { SqlClient } from "effect/unstable/sql/SqlClient";
import { isSqlError, type SqlError } from "effect/unstable/sql/SqlError";
import { storageDialect, type StorageDialect } from "./dialect.ts";
import {
  LEGACY_VERSION_TABLES,
  STORAGE_MIGRATIONS,
  STORAGE_SCHEMA_VERSION,
  STORAGE_TABLES,
  STORAGE_VERSION_TABLE,
  STORAGE_VERSION_TABLE_DDL,
} from "./schema.ts";
import {
  retryWithPolicy,
  transactionRetryPolicy,
  type TransactionRetryPolicy,
} from "./transaction-retry.ts";

interface TableRow {
  readonly name: string;
}

interface VersionRow {
  readonly version: number | bigint | null;
}

interface LockRow {
  readonly acquired: number | bigint | null;
}

export interface MigrationHooks {
  readonly beforeVersion?: (version: number) => Effect.Effect<void, StorageFault>;
  readonly onTransactionAttempt?: () => void;
}

/** Fixed `pg_advisory_xact_lock` key: "streamsy" in ASCII, read as a 64-bit integer. */
const PG_MIGRATION_LOCK_KEY = "8319400191411975033";
/** Named lock that serialises MySQL migrations, whose DDL cannot share one transaction. */
const MYSQL_MIGRATION_LOCK = STORAGE_VERSION_TABLE;
const MYSQL_MIGRATION_LOCK_TIMEOUT_SECONDS = 30;

const fault = (operation: string, error: SqlError): StorageFault =>
  new StorageFault({
    operation,
    message: error.message,
    retryable: error.isRetryable,
    cause: error,
  });

const sqlAttempt = <A, R>(operation: string, effect: Effect.Effect<A, SqlError, R>) =>
  effect.pipe(Effect.mapError((error) => fault(operation, error)));

/** Return whether the database already holds this format, or fail on an older format. */
export const inspectFormat = (
  tables: ReadonlySet<string>,
): Effect.Effect<boolean, StorageFault> => {
  const ownsFormat = tables.has(STORAGE_VERSION_TABLE);
  const legacy =
    LEGACY_VERSION_TABLES.some((name) => tables.has(name)) ||
    (!ownsFormat && STORAGE_TABLES.some((name) => tables.has(name)));
  return legacy
    ? Effect.fail(
        new StorageFault({
          operation: "migration.open",
          message:
            "Unsupported older Streamsy storage format; use a fresh database path (the database was not modified)",
          retryable: false,
        }),
      )
    : Effect.succeed(ownsFormat);
};

export const newerVersionFault = (version: number) =>
  new StorageFault({
    operation: "migration.open",
    message: `Unsupported newer Streamsy storage schema version ${version}; supported version is ${STORAGE_SCHEMA_VERSION}`,
    retryable: false,
  });

const listTables = (sql: SqlClient, dialect: StorageDialect) =>
  sqlAttempt(
    "migration.inspect",
    {
      sqlite: () => sql<TableRow>`SELECT name FROM sqlite_master WHERE type = 'table'`,
      pg: () =>
        sql<TableRow>`SELECT table_name AS name FROM information_schema.tables
 WHERE table_schema = current_schema()`,
      mysql: () =>
        sql<TableRow>`SELECT table_name AS name FROM information_schema.tables
 WHERE table_schema = DATABASE()`,
    }[dialect](),
  ).pipe(Effect.map((rows) => new Set(rows.map(({ name }) => name))));

const migrateVersions = (sql: SqlClient, dialect: StorageDialect, hooks: MigrationHooks) =>
  Effect.gen(function* () {
    const ownsFormat = yield* inspectFormat(yield* listTables(sql, dialect));
    if (!ownsFormat)
      yield* sqlAttempt(
        "migration.create-version-table",
        sql.unsafe(STORAGE_VERSION_TABLE_DDL[dialect]).pipe(Effect.asVoid),
      );

    const current = yield* sqlAttempt(
      "migration.read-version",
      sql<VersionRow>`SELECT MAX(version) AS version FROM ${sql(STORAGE_VERSION_TABLE)}`.pipe(
        Effect.map((rows) => Number(rows[0]?.version ?? 0)),
      ),
    );
    if (current > STORAGE_SCHEMA_VERSION) return yield* newerVersionFault(current);

    for (let version = current + 1; version <= STORAGE_SCHEMA_VERSION; version++) {
      if (hooks.beforeVersion !== undefined) yield* hooks.beforeVersion(version);
      const statements = STORAGE_MIGRATIONS[version - 1]?.[dialect];
      if (statements === undefined) return yield* Effect.die(new Error("Missing migration"));
      for (const statement of statements)
        yield* sqlAttempt(`migration.apply.${version}`, sql.unsafe(statement).pipe(Effect.asVoid));
      const appliedAtMs = yield* Clock.currentTimeMillis;
      // MySQL may rerun a version after a crash, so its version row must be safe to insert twice.
      yield* sqlAttempt(
        `migration.record.${version}`,
        (dialect === "mysql"
          ? sql`INSERT INTO ${sql(STORAGE_VERSION_TABLE)} (version, applied_at_ms) VALUES (${version}, ${appliedAtMs})
 ON DUPLICATE KEY UPDATE applied_at_ms = applied_at_ms`
          : sql`INSERT INTO ${sql(STORAGE_VERSION_TABLE)} (version, applied_at_ms) VALUES (${version}, ${appliedAtMs})`
        ).pipe(Effect.asVoid),
      );
    }
    return undefined;
  });

/**
 * MySQL commits each DDL statement by itself, so one transaction cannot hold
 * the migration. A named lock on one reserved connection serialises
 * instances instead, and every MySQL statement is safe to run twice.
 */
const migrateMysql = (sql: SqlClient, hooks: MigrationHooks) =>
  Effect.scoped(
    Effect.gen(function* () {
      const connection = yield* sqlAttempt("migration.connection", sql.reserve);
      // GET_LOCK belongs to one session. Statements use the transaction connection
      // when one is provided, so this pins every statement below to `connection`.
      return yield* Effect.provideService(
        Effect.acquireUseRelease(
          sqlAttempt(
            "migration.lock",
            sql<LockRow>`SELECT GET_LOCK(${MYSQL_MIGRATION_LOCK}, ${MYSQL_MIGRATION_LOCK_TIMEOUT_SECONDS}) AS acquired`,
          ).pipe(
            Effect.flatMap((rows) =>
              Number(rows[0]?.acquired ?? 0) === 1
                ? Effect.void
                : Effect.fail(
                    new StorageFault({
                      operation: "migration.lock",
                      message: `Timed out waiting for the MySQL migration lock ${MYSQL_MIGRATION_LOCK}`,
                      retryable: false,
                    }),
                  ),
            ),
          ),
          () => migrateVersions(sql, "mysql", hooks),
          () => sql`SELECT RELEASE_LOCK(${MYSQL_MIGRATION_LOCK})`.pipe(Effect.ignore),
        ),
        sql.transactionService,
        [connection, 0],
      );
    }),
  );

/**
 * Migrate one fresh-format database. SQLite and Postgres run under a single
 * write transaction: the legacy check runs before the first write, and every
 * schema and version statement rolls back together if acquisition is
 * interrupted or a migration fails. Postgres takes an advisory lock first, so
 * concurrent instances migrate one after the other. MySQL uses a named lock
 * and statements that are safe to run twice.
 */
export const migrate = (
  sql: SqlClient,
  hooks: MigrationHooks = {},
  transactionRetry: TransactionRetryPolicy = transactionRetryPolicy({}),
): Effect.Effect<void, StorageFault> =>
  Effect.flatMap(storageDialect(sql), (dialect) =>
    retryWithPolicy(
      Effect.suspend(() => {
        hooks.onTransactionAttempt?.();
        if (dialect === "mysql") return migrateMysql(sql, hooks);
        return sql.withTransaction(
          dialect === "pg"
            ? sqlAttempt(
                "migration.lock",
                sql`SELECT pg_advisory_xact_lock(${sql.literal(PG_MIGRATION_LOCK_KEY)})`,
              ).pipe(Effect.andThen(migrateVersions(sql, dialect, hooks)))
            : migrateVersions(sql, dialect, hooks),
        );
      }),
      transactionRetry,
      (error) => (isSqlError(error) ? error.isRetryable : error.retryable),
    ).pipe(
      Effect.catchIf(isSqlError, (error) => Effect.fail(fault("migration.transaction", error))),
    ),
  );
