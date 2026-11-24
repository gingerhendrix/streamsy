import { Array, Effect } from "effect";
import { ViewFault } from "../view-fault.ts";
import type { ServerDialect } from "./server.ts";

const text = "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";

const migrationLock = "streamsy_view_layout";

export const mysql: ServerDialect = {
  ddl: [
    `CREATE TABLE IF NOT EXISTS streamsy_view_layout (
      id SMALLINT PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL) ENGINE=InnoDB`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_circuits (
      ns BINARY(32) PRIMARY KEY, namespace ${text} NOT NULL, plan_hash ${text} NOT NULL) ENGINE=InnoDB`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_integrals (
      ns BINARY(32) NOT NULL, relation VARBINARY(255) NOT NULL, row_hash BINARY(32) NOT NULL,
      row_key LONGBLOB NOT NULL, value ${text} NOT NULL,
      PRIMARY KEY (ns, relation, row_hash)) ENGINE=InnoDB`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_arrangements (
      ns BINARY(32) NOT NULL, arrangement VARBINARY(255) NOT NULL, index_hash BINARY(32) NOT NULL,
      ord_prefix VARBINARY(1024) NOT NULL, row_hash BINARY(32) NOT NULL,
      sort_key ${text} NOT NULL, row_key LONGBLOB NOT NULL,
      PRIMARY KEY (ns, arrangement, index_hash, ord_prefix, row_hash)) ENGINE=InnoDB`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_accumulators (
      ns BINARY(32) NOT NULL, node VARBINARY(255) NOT NULL, group_hash BINARY(32) NOT NULL,
      group_key ${text} NOT NULL, value ${text} NOT NULL,
      PRIMARY KEY (ns, node, group_hash)) ENGINE=InnoDB`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_key_owners (
      ns BINARY(32) NOT NULL, relation VARBINARY(255) NOT NULL, output_hash BINARY(32) NOT NULL,
      output_key LONGBLOB NOT NULL, owner_key LONGBLOB NOT NULL,
      PRIMARY KEY (ns, relation, output_hash)) ENGINE=InnoDB`,
  ],
  upsert: (_keys, updates) =>
    `ON DUPLICATE KEY UPDATE ${Array.join(
      Array.map(updates, (column) => `${column} = VALUES(${column})`),
      ", ",
    )}`,
  arrangementInsertIgnore: "ON DUPLICATE KEY UPDATE row_hash = row_hash",
  layoutExists: (sql) =>
    sql`SELECT 1 FROM information_schema.tables
      WHERE table_schema = DATABASE() AND table_name = ${migrationLock}`.pipe(
      Effect.map(Array.isReadonlyArrayNonEmpty),
    ),
  withMigrationLock: (sql, migrate) =>
    Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* sql.reserve;

        // MySQL DDL autocommits. Keep the named lock and every statement on
        // the reserved session without opening a SQL transaction.
        return yield* Effect.acquireUseRelease(
          Effect.gen(function* () {
            const [row] = yield* sql<{ acquired: number | null }>`SELECT GET_LOCK(
              ${migrationLock}, 30) AS acquired`;

            if (Number(row?.acquired) !== 1)
              return yield* new ViewFault({
                message: `Could not acquire MySQL migration lock ${migrationLock}`,
              });

            return undefined;
          }),
          () => migrate,
          () => sql`SELECT RELEASE_LOCK(${migrationLock})`.pipe(Effect.ignore),
        ).pipe(Effect.provideService(sql.transactionService, [connection, 0]));
      }),
    ),
};
