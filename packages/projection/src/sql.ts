import { Context, Effect, Layer, Option } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { ownerOf, attachOwner } from "@streamsy/core/internal/memory";
import { CommitBoundary } from "@streamsy/storage";
import { CheckpointStore, fromStore, type EncodedStore } from "./checkpoint-store.ts";
import { ProjectionFault } from "./fault.ts";
import { Transaction } from "./transaction.ts";

const TABLE = "streamsy_projection_v2_records";

const storageFailure = (phase: ProjectionFault["phase"], message: string) => (cause: unknown) =>
  new ProjectionFault({ phase, reason: "storage-failure", message, cause });

/** Build both services on the selected transaction boundary. */
const make = (sql: SqlClient.SqlClient, transaction: typeof Transaction.Service) =>
  Effect.gen(function* () {
    yield* sql
      .unsafe(
        `CREATE TABLE IF NOT EXISTS ${TABLE} (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)`,
      )
      .pipe(Effect.mapError(storageFailure("load", `Cannot prepare ${TABLE}`)));
    const read = (table: string) =>
      Effect.fn("Projection.Sql.read")((key: string) =>
        sql
          .unsafe<{ readonly value: string }>(`SELECT value FROM ${table} WHERE key = ?`, [key])
          .pipe(
            Effect.mapError(storageFailure("load", `Cannot read ${table}`)),
            Effect.map((rows) => Option.fromUndefinedOr(rows[0]?.value)),
          ),
      );
    const write = (table: string) =>
      Effect.fn("Projection.Sql.write")((key: string, value: string) =>
        sql
          .unsafe(
            `INSERT INTO ${table} (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
            [key, value],
          )
          .pipe(
            Effect.mapError(storageFailure("checkpoint", `Cannot write ${table}`)),
            Effect.asVoid,
          ),
      );
    const remove = (table: string) =>
      Effect.fn("Projection.Sql.remove")((key: string) =>
        sql
          .unsafe(`DELETE FROM ${table} WHERE key = ?`, [key])
          .pipe(
            Effect.mapError(storageFailure("checkpoint", `Cannot delete from ${table}`)),
            Effect.asVoid,
          ),
      );
    const store: EncodedStore = {
      read: read(TABLE),
      write: write(TABLE),
      remove: remove(TABLE),
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
