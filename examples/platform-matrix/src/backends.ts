/**
 * The storage backends of the matrix app. Each backend is a Streamsy storage
 * Layer for the wildcard group. A backend with a config key is mounted only
 * when that key is set. The runtime entry passes the parts that depend on the
 * runtime, today the SQLite client. The Postgres and MySQL drivers are pure
 * JS and run on Bun, Node and Deno (spike S3), so this module imports them.
 */
import { MysqlClient } from "@effect/sql-mysql2";
import { PgClient } from "@effect/sql-pg";
import { MemoryStorage, type StorageFault, Streamsy } from "@streamsy/core";
import { SqlStorage } from "@streamsy/storage";
import { Config, Effect, Layer, Option, type Scope } from "effect";
import type { Reactivity } from "effect/reactivity";
import type { SqlClient, SqlError } from "effect/sql";

export const streams = Streamsy.group(Streamsy.Wildcard);

/** The services a backend Layer provides to the protocol routes. */
export type StreamServices = Streamsy.Services<typeof streams>;

/** How a backend fails to start. */
export type BackendError = StorageFault | SqlError.SqlError;

/** Opens a SQL client in the scope of the backend. */
export type MakeClient = Effect.Effect<
  SqlClient.SqlClient,
  SqlError.SqlError,
  Scope.Scope | Reactivity.Reactivity
>;

/** The clients that each runtime entry supplies. */
export interface Drivers {
  readonly sqlite: (filename: string) => MakeClient;
}

export interface Mounted {
  readonly name: string;
  readonly layer: Layer.Layer<StreamServices, BackendError>;
}

export interface Unmounted {
  readonly name: string;
  readonly reason: string;
}

export interface Selection {
  readonly mounted: ReadonlyArray<Mounted>;
  readonly unmounted: ReadonlyArray<Unmounted>;
}

const StorageConfig = Config.all({
  longPollTimeoutMs: Config.Int("LONG_POLL_TIMEOUT_MS").pipe(Config.option),
});

/**
 * One planned backend: its config, read as an `Option`, and the Layer for a
 * present value. A backend without a config key reads `Option.some`.
 */
interface Definition<A> {
  readonly name: string;
  readonly config: Config.Config<Option.Option<A>>;
  readonly missing: string;
  readonly layer: (
    value: A,
    options: { readonly longPollTimeoutMs?: number },
  ) => Layer.Layer<StreamServices, BackendError>;
}

const define = <A>(definition: Definition<A>) =>
  Effect.gen(function* () {
    const value = yield* definition.config;
    const options = yield* StorageConfig;
    const storage = Option.match(options.longPollTimeoutMs, {
      onNone: () => ({}),
      onSome: (longPollTimeoutMs) => ({ longPollTimeoutMs }),
    });
    return Option.match(value, {
      onNone: (): Mounted | Unmounted => ({ name: definition.name, reason: definition.missing }),
      onSome: (present): Mounted | Unmounted => ({
        name: definition.name,
        layer: definition.layer(present, storage),
      }),
    });
  });

/** Root storage over one SQL client, through the public `clientLayer`. */
const sqlLayer = (client: MakeClient, options: { readonly longPollTimeoutMs?: number }) =>
  SqlStorage.layer(streams, options).pipe(Layer.provide(SqlStorage.clientLayer(client)));

const definitions = (drivers: Drivers) => [
  define({
    name: "memory",
    config: Config.succeed(Option.some(undefined)),
    missing: "",
    layer: (_, options) => MemoryStorage.layer(streams, options),
  }),
  define({
    name: "sqlite",
    config: Config.String("SQLITE_PATH").pipe(Config.option),
    missing: "SQLITE_PATH is not set",
    layer: (filename, options) => sqlLayer(drivers.sqlite(filename), options),
  }),
  define({
    name: "pg",
    config: Config.Redacted("STREAMSY_PG_URL").pipe(Config.option),
    missing: "STREAMSY_PG_URL is not set",
    layer: (url, options) => sqlLayer(PgClient.make({ url }), options),
  }),
  define({
    name: "mysql",
    config: Config.Redacted("STREAMSY_MYSQL_URL").pipe(Config.option),
    missing: "STREAMSY_MYSQL_URL is not set",
    layer: (url, options) => sqlLayer(MysqlClient.make({ url }), options),
  }),
];

const isMounted = (backend: Mounted | Unmounted): backend is Mounted => "layer" in backend;

/** Reads the config of every planned backend and splits them into mounted and not. */
export const select = (drivers: Drivers) =>
  Effect.gen(function* () {
    const backends = yield* Effect.all(definitions(drivers));
    return {
      mounted: backends.filter(isMounted),
      unmounted: backends.filter((backend): backend is Unmounted => !isMounted(backend)),
    } satisfies Selection;
  });
