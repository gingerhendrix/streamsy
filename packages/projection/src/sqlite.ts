import { Effect, Layer, Option } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { CommitBoundary } from "@streamsy/storage";
import { Checkpoints, fromStore, type EncodedStore } from "./checkpoint.ts";
import { ProjectionFault } from "./fault.ts";

const TABLE = "streamsy_projection_v1_records";

const storageFailure = (phase: ProjectionFault["phase"], message: string) => (cause: unknown) =>
  new ProjectionFault({ phase, reason: "storage-failure", message, cause });

/**
 * Checkpoints over the host's `CommitBoundary` and shared `SqlClient`. The
 * package-owned table is created additively at acquisition and never reset; a
 * fused handler's SQL and stream writes on the same client join the owner
 * transaction the checkpoint commits in. Neither SQL driver is imported here.
 */
export const layer = Layer.effect(
  Checkpoints,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const boundary = yield* CommitBoundary;
    yield* boundary
      .withTransaction(
        sql.unsafe(
          `CREATE TABLE IF NOT EXISTS ${TABLE} (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)`,
        ),
      )
      .pipe(Effect.mapError(storageFailure("load", `Cannot prepare ${TABLE}`)));
    const store: EncodedStore = {
      read: Effect.fn("Projection.Sqlite.read")(function* (key: string) {
        const rows = yield* sql
          .unsafe<{ readonly value: string }>(`SELECT value FROM ${TABLE} WHERE key = ?`, [key])
          .pipe(Effect.mapError(storageFailure("load", `Cannot read ${TABLE}`)));
        return Option.fromUndefinedOr(rows[0]?.value);
      }),
      write: Effect.fn("Projection.Sqlite.write")(function* (key: string, value: string) {
        yield* sql
          .unsafe(
            `INSERT INTO ${TABLE} (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
            [key, value],
          )
          .pipe(Effect.mapError(storageFailure("checkpoint", `Cannot write ${TABLE}`)));
      }),
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
    return Checkpoints.of(fromStore(store));
  }),
);
