import { Context, Effect, Layer, Option } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { CommitBoundary } from "@streamsy/storage";
import { Checkpoints, fromStore, type EncodedStore } from "./checkpoint.ts";
import { ProjectionFault } from "./fault.ts";
import { State, stateFromStore } from "./state.ts";

const STATE_TABLE = "streamsy_projection_v1_state";
const TABLE = "streamsy_projection_v1_records";

const storageFailure = (phase: ProjectionFault["phase"], message: string) => (cause: unknown) =>
  new ProjectionFault({ phase, reason: "storage-failure", message, cause });

/**
 * Checkpoints over the host's `CommitBoundary` and shared `SqlClient`. The
 * package-owned tables are created additively at acquisition and never reset; a
 * fused handler's SQL and stream writes on the same client join the owner
 * transaction the checkpoint commits in. Neither SQL driver is imported here.
 */
export const layer = Layer.effectContext(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const boundary = yield* CommitBoundary;
    yield* boundary
      .withTransaction(
        Effect.forEach([TABLE, STATE_TABLE], (table) =>
          sql.unsafe(
            `CREATE TABLE IF NOT EXISTS ${table} (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)`,
          ),
        ),
      )
      .pipe(Effect.mapError(storageFailure("load", `Cannot prepare ${TABLE} and ${STATE_TABLE}`)));
    const read = (table: string) =>
      Effect.fn("Projection.Sqlite.read")((key: string) =>
        sql
          .unsafe<{ readonly value: string }>(`SELECT value FROM ${table} WHERE key = ?`, [key])
          .pipe(
            Effect.mapError(storageFailure("load", `Cannot read ${table}`)),
            Effect.map((rows) => Option.fromUndefinedOr(rows[0]?.value)),
          ),
      );
    const write = (table: string) =>
      Effect.fn("Projection.Sqlite.write")((key: string, value: string) =>
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
      Effect.fn("Projection.Sqlite.remove")((key: string) =>
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
      readState: read(STATE_TABLE),
      writeState: write(STATE_TABLE),
      removeState: remove(STATE_TABLE),
      withTransaction: (body) =>
        boundary
          .withTransaction(body)
          .pipe(
            Effect.catchTag(
              "SqlError",
              storageFailure("checkpoint", "The owner transaction failed"),
            ),
          ),
    };
    return Context.make(Checkpoints, fromStore(store)).pipe(
      Context.add(State, stateFromStore(store)),
    );
  }),
);
