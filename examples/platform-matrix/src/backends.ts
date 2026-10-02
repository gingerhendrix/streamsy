/**
 * The storage backends of the matrix app. Each backend is a Streamsy storage
 * Layer for the wildcard group. A backend with a config key is mounted only
 * when that key is set. The runtime entry passes the parts that depend on the
 * runtime, today the SQLite client.
 */
import { Memory, type StorageFault, Streamsy } from "@streamsy/core";
import * as Storage from "@streamsy/storage";
import { Config, Effect, Layer, Option, type Scope } from "effect";
import type { Reactivity } from "effect/unstable/reactivity";
import type { SqlClient, SqlError } from "effect/unstable/sql";

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

const definitions = (drivers: Drivers) => [
  define({
    name: "memory",
    config: Config.succeed(Option.some(undefined)),
    missing: "",
    layer: (_, options) => Memory.layer(streams, options),
  }),
  define({
    name: "sqlite",
    config: Config.String("SQLITE_PATH").pipe(Config.option),
    missing: "SQLITE_PATH is not set",
    layer: (filename, options) =>
      Storage.layer(streams, options).pipe(
        Layer.provide(Storage.clientLayer(drivers.sqlite(filename))),
      ),
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
