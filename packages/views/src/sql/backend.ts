import { Effect } from "effect";
import type { SqlClient } from "effect/sql";
import type { Backend } from "../backend.ts";
import { ViewFault } from "../view-fault.ts";
import { migrate, migrateCircuits, sqliteBackend } from "./sqlite.ts";

const unsupportedDialect = (dialect: string) =>
  Effect.fail(
    new ViewFault({ message: `Unsupported Views SQL dialect ${dialect}; Views requires SQLite` }),
  );

const sqliteOnly = <A, E>(sql: SqlClient.SqlClient, use: () => Effect.Effect<A, E>) =>
  sql.onDialectOrElse({
    sqlite: use,
    pg: () => unsupportedDialect("pg"),
    mysql: () => unsupportedDialect("mysql"),
    mssql: () => unsupportedDialect("mssql"),
    clickhouse: () => unsupportedDialect("clickhouse"),
    orElse: () => unsupportedDialect("unknown"),
  });

export const backendFor = (sql: SqlClient.SqlClient): Effect.Effect<Backend, ViewFault> =>
  sqliteOnly(sql, () => Effect.succeed(sqliteBackend(sql)));

export const migrateBackend = (sql: SqlClient.SqlClient) =>
  sqliteOnly(sql, () => migrate(sql).pipe(Effect.andThen(migrateCircuits(sql))));
