import { Effect } from "effect";
import type { SqlClient, SqlError } from "effect/sql";
import type { Backend } from "../backend.ts";
import { ViewFault } from "../view-fault.ts";
import { postgres } from "./postgres.ts";
import { migrateServer, serverBackend } from "./server.ts";
import { migrate, migrateCircuits, sqliteBackend } from "./sqlite.ts";

const unsupportedDialect = (dialect: string) =>
  Effect.fail(
    new ViewFault({
      message: `Unsupported Views SQL dialect ${dialect}; Views supports sqlite and pg`,
    }),
  );

const supportedDialect = <A, E>(
  sql: SqlClient.SqlClient,
  sqlite: () => Effect.Effect<A, E>,
  pg: () => Effect.Effect<A, E>,
) =>
  sql.onDialectOrElse({
    sqlite,
    pg,
    mysql: () => unsupportedDialect("mysql"),
    mssql: () => unsupportedDialect("mssql"),
    clickhouse: () => unsupportedDialect("clickhouse"),
    orElse: () => unsupportedDialect(sql.driver),
  });

export const backendFor = (sql: SqlClient.SqlClient): Effect.Effect<Backend, ViewFault> =>
  supportedDialect(
    sql,
    () => Effect.succeed(sqliteBackend(sql)),
    () => Effect.succeed(serverBackend(sql, postgres)),
  );

export const migrateBackend = (
  sql: SqlClient.SqlClient,
): Effect.Effect<void, SqlError.SqlError | ViewFault> =>
  supportedDialect(
    sql,
    () => migrate(sql).pipe(Effect.andThen(migrateCircuits(sql))),
    () => migrateServer(sql, postgres),
  );
