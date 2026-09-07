import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Database } from "bun:sqlite";
import { Cause, Effect, Layer, Option, Result, Schema } from "effect";
import { StorageFault } from "@streamsy/core";
import { Protocol } from "@streamsy/core";
import { sharedSqlClientLayer } from "./boundary.ts";
import { STORAGE_SCHEMA_VERSION, STORAGE_VERSION_TABLE } from "./schema.ts";
import { layer as sqlLayer, type SqlStorageOptions } from "./storage.ts";
import { retryWithPolicy, transactionRetryPolicy } from "./transaction-retry.ts";

export interface BunStorageOptions extends SqlStorageOptions {
  readonly client: Parameters<typeof SqliteClient.make>[0];
}

export interface BunProtocolOptions extends BunStorageOptions, Protocol.ProtocolOptions {}
type BunClientOptions = BunStorageOptions["client"];

const SqliteOpenError = Schema.Struct({
  code: Schema.optional(Schema.String),
  errno: Schema.optional(Schema.Finite),
});
type SqliteOpenError = typeof SqliteOpenError.Type;
const decodeSqliteOpenError = Schema.decodeUnknownOption(SqliteOpenError);
const isBusy = ({ code, errno }: SqliteOpenError): boolean =>
  code === "SQLITE_BUSY" || code === "SQLITE_LOCKED" || errno === 5 || errno === 6;
const isSqliteOperationalError = ({ code, errno }: SqliteOpenError): boolean =>
  (code?.startsWith("SQLITE_") ?? false) || errno !== undefined;
const openFault = (error: SqliteOpenError, rendered: string) =>
  new StorageFault({
    operation: "migration.open",
    message: rendered,
    retryable: isBusy(error),
  });

const normalizeSqliteDefect = <A, R>(
  acquisition: Effect.Effect<A, never, R>,
): Effect.Effect<A, StorageFault, R> =>
  acquisition.pipe(
    Effect.catchCause((cause) =>
      Result.match(Cause.findDefect(cause), {
        onFailure: () => Effect.failCause(cause),
        onSuccess: (defect) =>
          Option.match(decodeSqliteOpenError(defect), {
            onNone: () => Effect.failCause(cause),
            onSome: (error) =>
              isSqliteOperationalError(error)
                ? Effect.fail(openFault(error, String(defect)))
                : Effect.failCause(cause),
          }),
      }),
    ),
  );

const preflightFile = (
  filename: string,
  retryPolicy: ReturnType<typeof transactionRetryPolicy>,
): Effect.Effect<void, StorageFault> =>
  filename === ":memory:"
    ? Effect.void
    : Effect.gen(function* () {
        if (!(yield* Effect.promise(() => Bun.file(filename).exists()))) return undefined;
        const inspect = Effect.acquireUseRelease(
          normalizeSqliteDefect(Effect.sync(() => new Database(filename, { readonly: true }))),
          (database) =>
            normalizeSqliteDefect(
              Effect.sync(() => {
                const names = new Set(
                  database
                    .query<{ name: string }, []>(
                      "SELECT name FROM sqlite_master WHERE type='table'",
                    )
                    .all()
                    .map(({ name }) => name),
                );
                const ownsFormat = names.has(STORAGE_VERSION_TABLE);
                const legacy =
                  names.has("streamsy_schema_version") ||
                  (!ownsFormat &&
                    ["streamsy_streams", "streamsy_messages", "streamsy_producers"].some((name) =>
                      names.has(name),
                    ));
                const version = ownsFormat
                  ? (database
                      .query<{ version: number | null }, []>(
                        `SELECT MAX(version) version FROM ${STORAGE_VERSION_TABLE}`,
                      )
                      .get()?.version ?? 0)
                  : 0;
                return { legacy, version };
              }),
            ),
          (database) => Effect.sync(() => database.close(false)),
        );
        const state = yield* retryWithPolicy(inspect, retryPolicy, (error) => error.retryable);
        if (state.legacy)
          return yield* new StorageFault({
            operation: "migration.open",
            message:
              "Unsupported pre-0.4 Streamsy SQLite format; use a fresh database path (the file was not modified)",
            retryable: false,
          });
        if (state.version > STORAGE_SCHEMA_VERSION)
          return yield* new StorageFault({
            operation: "migration.open",
            message: `Unsupported newer Streamsy SQLite schema version ${state.version}; supported version is ${STORAGE_SCHEMA_VERSION}`,
            retryable: false,
          });
        return undefined;
      });

const prepareJournal = (
  options: BunClientOptions,
  retryPolicy: ReturnType<typeof transactionRetryPolicy>,
): Effect.Effect<void, StorageFault> =>
  options.filename === ":memory:"
    ? Effect.void
    : retryWithPolicy(
        Effect.acquireUseRelease(
          normalizeSqliteDefect(
            Effect.sync(
              () =>
                new Database(options.filename, {
                  readonly: false,
                  readwrite: options.readwrite ?? true,
                  create: options.create ?? true,
                }),
            ),
          ),
          (database) =>
            normalizeSqliteDefect(
              Effect.sync(() => {
                database.run("PRAGMA busy_timeout=0");
                database.run("PRAGMA journal_mode=WAL");
              }),
            ),
          (database) => Effect.sync(() => database.close(false)),
        ),
        retryPolicy,
        (error) => error.retryable,
      );

const validateClientOpen = (
  options: BunClientOptions,
  retryPolicy: ReturnType<typeof transactionRetryPolicy>,
): Effect.Effect<void, StorageFault> =>
  options.filename === ":memory:"
    ? Effect.void
    : retryWithPolicy(
        Effect.acquireUseRelease(
          normalizeSqliteDefect(
            Effect.sync(() => {
              const readonly = options.readonly === true;
              return new Database(options.filename, {
                readonly,
                readwrite: readonly ? false : (options.readwrite ?? true),
                create: readonly ? false : (options.create ?? true),
              });
            }),
          ),
          () => Effect.void,
          (database) => Effect.sync(() => database.close(false)),
        ),
        retryPolicy,
        (error) => error.retryable,
      );

/** Official Bun SQLite host layer with one shared SqlClient/Reactivity graph. */
export const layer = (options: BunStorageOptions) => {
  const retryPolicy = transactionRetryPolicy(options);
  const preparesJournal =
    options.client.filename !== ":memory:" &&
    options.client.readonly !== true &&
    options.client.disableWAL !== true;
  const clientLayer = Layer.effectContext(
    sharedSqlClientLayer(
      normalizeSqliteDefect(
        SqliteClient.make({
          busyTimeout: 0,
          ...options.client,
          disableWAL: preparesJournal ? true : options.client.disableWAL,
        }),
      ),
    ),
  );
  return Layer.unwrap(
    Effect.gen(function* () {
      yield* preflightFile(options.client.filename, retryPolicy);
      yield* validateClientOpen(options.client, retryPolicy);
      if (preparesJournal) yield* prepareJournal(options.client, retryPolicy);
      return sqlLayer(options).pipe(Layer.provideMerge(clientLayer));
    }),
  );
};

/** Complete persistent protocol Layer for the scoped `@streamsy/serve/bun` host. */
export const layerProtocol = (options: BunProtocolOptions) =>
  Protocol.layer(options).pipe(Layer.provide(layer(options)));
