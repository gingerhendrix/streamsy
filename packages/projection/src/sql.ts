import { Context, Effect, Layer, Option } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { ownerOf, attachOwner } from "@streamsy/core/internal/memory";
import { CommitBoundary } from "@streamsy/storage";
import { CheckpointStore, fromStore, type EncodedStore } from "./checkpoint-store.ts";
import { ProjectionFault } from "./fault.ts";
import { Transaction } from "./transaction.ts";

const TABLE = "streamsy_projection_v3_records";

const storageFailure = (phase: ProjectionFault["phase"], message: string) => (cause: unknown) =>
  new ProjectionFault({ phase, reason: "storage-failure", message, cause });

/** `key` is reserved in MySQL, so the columns carry a prefix. */
const createTable = (sql: SqlClient.SqlClient) =>
  sql.onDialectOrElse({
    mysql: () =>
      sql`CREATE TABLE IF NOT EXISTS streamsy_projection_v3_records (
 checkpoint_key VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
 checkpoint_value LONGTEXT NOT NULL)`,
    orElse: () =>
      sql`CREATE TABLE IF NOT EXISTS streamsy_projection_v3_records (
 checkpoint_key TEXT PRIMARY KEY NOT NULL, checkpoint_value TEXT NOT NULL)`,
  });

/** Build both services on the selected transaction boundary. */
const make = (sql: SqlClient.SqlClient, transaction: typeof Transaction.Service) =>
  Effect.gen(function* () {
    // Runs at layer build, outside any transaction, so MySQL DDL commits nothing early.
    yield* createTable(sql).pipe(
      Effect.mapError(storageFailure("load", `Cannot prepare ${TABLE}`)),
    );
    const read = Effect.fn("Projection.Sql.read")((key: string) =>
      sql<{
        readonly checkpoint_value: string;
      }>`SELECT checkpoint_value FROM streamsy_projection_v3_records
 WHERE checkpoint_key = ${key}`.pipe(
        Effect.mapError(storageFailure("load", `Cannot read ${TABLE}`)),
        Effect.map((rows) => Option.fromUndefinedOr(rows[0]?.checkpoint_value)),
      ),
    );
    const write = Effect.fn("Projection.Sql.write")((key: string, value: string) => {
      const row = sql.insert({ checkpoint_key: key, checkpoint_value: value });
      return sql
        .onDialectOrElse({
          mysql: () =>
            sql`INSERT INTO streamsy_projection_v3_records ${row}
 ON DUPLICATE KEY UPDATE checkpoint_value = VALUES(checkpoint_value)`,
          orElse: () =>
            sql`INSERT INTO streamsy_projection_v3_records ${row}
 ON CONFLICT (checkpoint_key) DO UPDATE SET checkpoint_value = excluded.checkpoint_value`,
        })
        .pipe(
          Effect.mapError(storageFailure("checkpoint", `Cannot write ${TABLE}`)),
          Effect.asVoid,
        );
    });
    const remove = Effect.fn("Projection.Sql.remove")((key: string) =>
      sql`DELETE FROM streamsy_projection_v3_records WHERE checkpoint_key = ${key}`.pipe(
        Effect.mapError(storageFailure("checkpoint", `Cannot delete from ${TABLE}`)),
        Effect.asVoid,
      ),
    );
    const store: EncodedStore = {
      read,
      write,
      remove,
      withTransaction: transaction.withTransaction,
    };
    const checkpoints = fromStore(store);
    attachOwner(checkpoints.load, transaction.owner);
    return Context.make(CheckpointStore, checkpoints).pipe(Context.add(Transaction, transaction));
  });

/** Checkpoints and transactions on a plain SQL client. */
export const layer = Layer.effectContext(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return yield* make(sql, {
      owner: ownerOf(sql),
      sql,
      withTransaction: (body) =>
        sql
          .withTransaction(body)
          .pipe(
            Effect.catchTag("SqlError", storageFailure("checkpoint", "The SQL transaction failed")),
          ),
    });
  }),
);

/** Checkpoints join the storage owner's stream and SQL transaction. */
export const layerStorage = Layer.effectContext(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const boundary = yield* CommitBoundary;
    return yield* make(sql, {
      owner: ownerOf(boundary),
      sql,
      withTransaction: (body) =>
        boundary
          .withTransaction(body)
          .pipe(
            Effect.catchTag(
              "SqlError",
              storageFailure("checkpoint", "The owner transaction failed"),
            ),
          ),
    });
  }),
);
