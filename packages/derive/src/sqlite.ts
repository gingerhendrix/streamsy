import { Effect, Layer, Option, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { CommitBoundary } from "@streamsy/storage";
import { Commit } from "./commit.ts";
import { DeriveFault } from "./fault.ts";
import { Checkpoint, StateRecord, records } from "./stores.ts";

const storageFault = () =>
  new DeriveFault({ reason: "storage-failure", message: "Derive SQLite operation failed" });

/** Additive package-owned format. The versioned table is never reset or repurposed. */
export const layer = Layer.effect(
  Commit,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const boundary = yield* CommitBoundary;
    yield* boundary
      .withTransaction(
        sql.unsafe(
          "CREATE TABLE IF NOT EXISTS streamsy_derive_v1_records (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)",
        ),
      )
      .pipe(Effect.mapError(storageFault));
    const store = {
      read: Effect.fn("Derive.Sqlite.read")(function* (key: string) {
        const rows = yield* sql
          .unsafe<{ readonly value: string }>(
            "SELECT value FROM streamsy_derive_v1_records WHERE key = ?",
            [key],
          )
          .pipe(Effect.mapError(storageFault));
        return Option.fromUndefinedOr(rows[0]?.value);
      }),
      write: Effect.fn("Derive.Sqlite.write")(function* (key: string, value: string) {
        yield* sql
          .unsafe(
            "INSERT INTO streamsy_derive_v1_records (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [key, value],
          )
          .pipe(Effect.mapError(storageFault));
      }),
    };
    return Commit.of({
      withTransaction: (body) =>
        boundary
          .withTransaction(body)
          .pipe(Effect.catchTag("SqlError", () => Effect.fail(storageFault()))),
      checkpoints: records(Schema.fromJsonString(Checkpoint), "checkpoint", store),
      states: records(Schema.fromJsonString(StateRecord), "state", store),
    });
  }),
);
