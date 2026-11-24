/* oxlint-disable effecttsgo/crypto-random-uuid -- Bun owns the database fixture names. */
import { MysqlClient } from "@effect/sql-mysql2";
import { PgClient } from "@effect/sql-pg";
import {
  Array,
  Config,
  Context,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Option,
  Redacted,
  type Scope,
} from "effect";
import type { Reactivity } from "effect/reactivity";
import { SqlClient, Statement, type SqlError } from "effect/sql";
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
  ) => Effect.Effect<SqlClient.SqlClient, SqlError.SqlError, Scope.Scope | Reactivity.Reactivity>;
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

/** Fork before entering a transaction so the other host has its own transaction context. */
export const forkWaiting = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Deferred.make<void>().pipe(
    Effect.flatMap((ready) =>
      Deferred.await(ready).pipe(
        Effect.andThen(effect),
        Effect.forkScoped,
        Effect.map((worker) =>
          Deferred.succeed(ready, undefined).pipe(Effect.andThen(Fiber.join(worker))),
        ),
      ),
    ),
  );

export type StatementMatch = (query: string, params: ReadonlyArray<unknown>) => boolean;

/** Hold the first statement that each matcher accepts, before it runs. */
export const holdStatements = (...matchers: ReadonlyArray<StatementMatch>) =>
  Effect.gen(function* () {
    const gates = yield* Effect.forEach(matchers, (matches) =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();

        return { matches, reached, proceed, held: false };
      }),
    );

    return {
      reached: (index = 0) => Deferred.await(gates[index]!.reached),
      release: (index = 0) => Deferred.succeed(gates[index]!.proceed, undefined),
      hold: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(
          Effect.provideService(Statement.CurrentTransformer, (statement) => {
            const [query, params] = statement.compile();

            const gate = Array.findFirst(
              gates,
              ({ held, matches }) => !held && matches(query, params),
            );

            if (Option.isNone(gate)) return Effect.succeed(statement);
            gate.value.held = true;

            return Deferred.succeed(gate.value.reached, undefined).pipe(
              Effect.andThen(Deferred.await(gate.value.proceed)),
              Effect.as(statement),
            );
          }),
        ),
    };
  });

/** Use with a single-connection MySQL fixture so the transaction reuses this session. */
export const withRepeatableRead = <A, E, R>(
  sql: SqlClient.SqlClient,
  effect: Effect.Effect<A, E, R>,
) =>
  sql`SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ`.pipe(
    Effect.andThen(
      effect.pipe(Effect.provideService(SqlClient.BeginTransaction, "START TRANSACTION")),
    ),
  );

const databaseName = () => `streamsy_${crypto.randomUUID().replaceAll("-", "").slice(0, 16)}`;

/** Create a database with a random `streamsy_` name, and drop it when the scope closes. */
export const freshDatabase = (
  dialect: SqlDialect,
  baseUrl: string,
): Effect.Effect<string, SqlError.SqlError, Scope.Scope> =>
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
