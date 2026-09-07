import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Database } from "bun:sqlite";
import { Effect, Layer } from "effect";
import { StorageFault } from "@streamsy/core";
import { sharedSqlClientLayer } from "./boundary.ts";
import { STORAGE_SCHEMA_VERSION, STORAGE_VERSION_TABLE } from "./schema.ts";
import { layer as sqlLayer, type SqlStorageOptions } from "./storage.ts";

export interface BunStorageOptions extends SqlStorageOptions {
  readonly client: Parameters<typeof SqliteClient.make>[0];
}

const preflightFile = (filename: string): Effect.Effect<void, StorageFault> =>
  filename === ":memory:"
    ? Effect.void
    : Effect.gen(function* () {
        if (!(yield* Effect.promise(() => Bun.file(filename).exists()))) return undefined;
        const state = yield* Effect.acquireUseRelease(
          Effect.sync(() => new Database(filename, { readonly: true })),
          (database) =>
            Effect.sync(() => {
              const names = new Set(
                database
                  .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='table'")
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
          (database) => Effect.sync(() => database.close(false)),
        );
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

/** Official Bun SQLite host layer with one shared SqlClient/Reactivity graph. */
export const layer = (options: BunStorageOptions) => {
  const clientLayer = Layer.effectContext(sharedSqlClientLayer(SqliteClient.make(options.client)));
  return Layer.unwrap(
    preflightFile(options.client.filename).pipe(
      Effect.map(() => sqlLayer(options).pipe(Layer.provide(clientLayer))),
    ),
  );
};
