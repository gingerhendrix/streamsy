import { StorageFault, type Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import * as BunFileSystem from "@effect/platform-bun/BunFileSystem";
import * as BunPath from "@effect/platform-bun/BunPath";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Database } from "bun:sqlite";
import { Cause, Effect, Layer, Option, Result, Schema } from "effect";
import { sharedSqlClientLayer } from "./boundary.ts";
import * as SqliteFile from "./sqlite-file.ts";
import { storageLayer as sqlLayer, type SqlStorageOptions } from "./storage.ts";
import { transactionRetryPolicy } from "./transaction-retry.ts";

export interface BunStorageOptions extends SqlStorageOptions {
  readonly client: Parameters<typeof SqliteClient.make>[0];
}

export interface BunProtocolOptions extends BunStorageOptions, Protocol.ProtocolOptions {}

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

/** Raw `bun:sqlite` opens for the shared SQLite file step. */
const bunDriver: SqliteFile.SqliteFileDriver = {
  read: (filename, use) =>
    Effect.acquireUseRelease(
      normalizeSqliteDefect(Effect.sync(() => new Database(filename, { readonly: true }))),
      (database) =>
        normalizeSqliteDefect(Effect.sync(() => use({ all: (sql) => database.query(sql).all() }))),
      (database) => Effect.sync(() => database.close(false)),
    ),
  run: (filename, mode, statements) =>
    Effect.acquireUseRelease(
      normalizeSqliteDefect(Effect.sync(() => new Database(filename, mode))),
      (database) =>
        normalizeSqliteDefect(
          Effect.sync(() => {
            for (const statement of statements) database.run(statement);
          }),
        ),
      (database) => Effect.sync(() => database.close(false)),
    ),
};

const BunPlatform = Layer.mergeAll(BunFileSystem.layer, BunPath.layer);

/** Official Bun SQLite host layer with one shared SqlClient/Reactivity graph. */
const storageLayer = (options: BunStorageOptions) => {
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
    SqliteFile.prepare(bunDriver, options.client, retryPolicy).pipe(
      Effect.as(sqlLayer(options).pipe(Layer.provideMerge(clientLayer))),
    ),
  ).pipe(Layer.provide(BunPlatform));
};

/** Complete persistent protocol Layer for an HttpRouter application. */
export const layerProtocol = (options: BunProtocolOptions) =>
  Protocol.layer(options).pipe(Layer.provideMerge(storageLayer(options)));

export function layer<const Keys extends ReadonlyArray<Streamsy.Any>>(
  options: BunProtocolOptions,
  group: Streamsy.Group<Keys>,
): Layer.Layer<
  | Layer.Success<ReturnType<typeof storageLayer>>
  | Streamsy.Services<Streamsy.Group<Keys>>
  | Streamsy.ChangeFeed<Streamsy.Services<Streamsy.Group<Keys>>>,
  Layer.Error<ReturnType<typeof storageLayer>>
>;
export function layer(options: BunProtocolOptions, group: Streamsy.Group) {
  const storage = storageLayer(options);
  const protocol = Protocol.layer(options).pipe(Layer.provideMerge(storage));
  return Protocol.bind(protocol, group).pipe(Layer.provideMerge(storage));
}
