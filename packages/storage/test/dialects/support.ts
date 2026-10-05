/* oxlint-disable effecttsgo/crypto-random-uuid -- Bun owns the database fixture names. */
import { MysqlClient } from "@effect/sql-mysql2";
import { PgClient } from "@effect/sql-pg";
import { Config, Context, Effect, Layer, Option, Redacted, type Scope } from "effect";
import type * as Reactivity from "effect/reactivity/Reactivity";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { clientLayer } from "../../src/boundary.ts";

const optionalUrl = (name: string) =>
  Effect.runSync(Config.option(Config.String(name))).pipe(Option.getOrUndefined);

export interface ClientOptions {
  readonly maxConnections?: number;
}

/** One server dialect under test. A test skips the dialect when `url` is undefined. */
export interface SqlDialect {
  readonly name: "pg" | "mysql";
  readonly url: string | undefined;
  readonly client: (
    url: string,
    options?: ClientOptions,
  ) => Effect.Effect<SqlClient.SqlClient, SqlError, Scope.Scope | Reactivity.Reactivity>;
  /** Lists the tables of the connected database as `name` rows. */
  readonly tables: string;
  readonly dropDatabase: (name: string) => string;
}

export const sqlDialects = {
  pg: {
    name: "pg",
    url: optionalUrl("STREAMSY_PG_URL"),
    client: (url, options = {}) => PgClient.make({ url: Redacted.make(url), ...options }),
    tables:
      "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = current_schema() ORDER BY name",
    // FORCE ends any session a failed test left open.
    dropDatabase: (name) => `DROP DATABASE IF EXISTS ${name} WITH (FORCE)`,
  },
  mysql: {
    name: "mysql",
    url: optionalUrl("STREAMSY_MYSQL_URL"),
    client: (url, options = {}) => MysqlClient.make({ url: Redacted.make(url), ...options }),
    tables:
      "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY name",
    dropDatabase: (name) => `DROP DATABASE IF EXISTS ${name}`,
  },
} satisfies Record<SqlDialect["name"], SqlDialect>;

/** A client layer through the public `clientLayer`, so client and storage share Reactivity. */
export const dialectClient = (dialect: SqlDialect, url: string, options?: ClientOptions) =>
  clientLayer(dialect.client(url, options));

const databaseName = () => `streamsy_${crypto.randomUUID().replaceAll("-", "").slice(0, 16)}`;

/** Create a database with a random `streamsy_` name, and drop it when the scope closes. */
export const freshDatabase = (
  dialect: SqlDialect,
  baseUrl: string,
): Effect.Effect<string, SqlError, Scope.Scope> =>
  Effect.gen(function* () {
    const name = yield* Effect.sync(databaseName);
    const admin = Context.get(
      yield* Layer.build(dialectClient(dialect, baseUrl, { maxConnections: 1 })),
      SqlClient.SqlClient,
    );
    yield* Effect.acquireRelease(admin.unsafe(`CREATE DATABASE ${name}`), () =>
      admin.unsafe(dialect.dropDatabase(name)).pipe(Effect.ignore),
    );
    const url = new URL(baseUrl);
    url.pathname = `/${name}`;
    return url.toString();
  });
