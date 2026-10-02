import { StorageFault } from "@streamsy/core";
import { Effect, FileSystem, Path, Schema } from "effect";
import { STORAGE_SCHEMA_VERSION, STORAGE_VERSION_TABLE } from "./schema.ts";
import { retryWithPolicy, type TransactionRetryPolicy } from "./transaction-retry.ts";

/** The open flags of one short-lived SQLite connection. */
export interface SqliteOpenMode {
  readonly readonly: boolean;
  readonly readwrite: boolean;
  readonly create: boolean;
}

/** Read access to a connection that the driver opened read-only. */
export interface SqliteFileReader {
  readonly all: (sql: string) => ReadonlyArray<unknown>;
}

/**
 * The host-specific part of a SQLite file step: raw opens and error classification.
 * Each call opens the file, uses it, and closes it. SQLite errors become a
 * `StorageFault` with `operation: "migration.open"`, retryable when the file is busy.
 */
export interface SqliteFileDriver {
  readonly read: <A>(
    filename: string,
    use: (reader: SqliteFileReader) => A,
  ) => Effect.Effect<A, StorageFault>;
  readonly run: (
    filename: string,
    mode: SqliteOpenMode,
    statements: ReadonlyArray<string>,
  ) => Effect.Effect<void, StorageFault>;
}

/** The client open options that the file step reads. */
export interface SqliteFileOptions {
  readonly filename: string;
  readonly readonly?: boolean | undefined;
  readonly readwrite?: boolean | undefined;
  readonly create?: boolean | undefined;
  readonly disableWAL?: boolean | undefined;
}

const openFault = (message: string) =>
  new StorageFault({ operation: "migration.open", message, retryable: false });

const decodeTableNames = Schema.decodeUnknownSync(
  Schema.Array(Schema.Struct({ name: Schema.String })),
);
const decodeVersions = Schema.decodeUnknownSync(
  Schema.Array(Schema.Struct({ version: Schema.NullOr(Schema.Finite) })),
);

const inspectFormat = (reader: SqliteFileReader) => {
  const names = new Set(
    decodeTableNames(reader.all("SELECT name FROM sqlite_master WHERE type='table'")).map(
      ({ name }) => name,
    ),
  );
  const ownsFormat = names.has(STORAGE_VERSION_TABLE);
  const legacy =
    names.has("streamsy_schema_version") ||
    (!ownsFormat &&
      ["streamsy_streams", "streamsy_messages", "streamsy_producers"].some((name) =>
        names.has(name),
      ));
  const version = ownsFormat
    ? (decodeVersions(reader.all(`SELECT MAX(version) version FROM ${STORAGE_VERSION_TABLE}`)).at(0)
        ?.version ?? 0)
    : 0;
  return { legacy, version };
};

const preflight = (
  filename: string,
  driver: SqliteFileDriver,
  retry: TransactionRetryPolicy,
): Effect.Effect<void, StorageFault> =>
  Effect.gen(function* () {
    const state = yield* retryWithPolicy(
      driver.read(filename, inspectFormat),
      retry,
      (error) => error.retryable,
    );
    if (state.legacy)
      return yield* openFault(
        "Unsupported pre-0.4 Streamsy SQLite format; use a fresh database path (the file was not modified)",
      );
    if (state.version > STORAGE_SCHEMA_VERSION)
      return yield* openFault(
        `Unsupported newer Streamsy SQLite schema version ${state.version}; supported version is ${STORAGE_SCHEMA_VERSION}`,
      );
    return undefined;
  });

/**
 * Prepare a SQLite file before the client opens it: create the parent folder,
 * refuse a foreign format, check the open mode, and set WAL with busy timeout 0.
 * A read-only or no-create open never creates a folder or a file.
 */
export const prepare = (
  driver: SqliteFileDriver,
  options: SqliteFileOptions,
  retry: TransactionRetryPolicy,
): Effect.Effect<void, StorageFault, FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function* () {
    const { filename } = options;
    if (filename === ":memory:") return;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const readonly = options.readonly === true;
    const mode: SqliteOpenMode = {
      readonly,
      readwrite: readonly ? false : (options.readwrite ?? true),
      create: readonly ? false : (options.create ?? true),
    };
    if (mode.readwrite && mode.create)
      yield* fs
        .makeDirectory(path.dirname(filename), { recursive: true })
        .pipe(
          Effect.mapError((error) =>
            openFault(`Cannot create the folder for SQLite file ${filename}: ${error.message}`),
          ),
        );
    const exists = yield* fs
      .exists(filename)
      .pipe(
        Effect.mapError((error) =>
          openFault(`Cannot check SQLite file ${filename}: ${error.message}`),
        ),
      );
    if (exists) yield* preflight(filename, driver, retry);
    const retried = (step: Effect.Effect<void, StorageFault>) =>
      retryWithPolicy(step, retry, (error) => error.retryable);
    yield* retried(driver.run(filename, mode, []));
    if (!readonly && options.disableWAL !== true)
      yield* retried(
        driver.run(
          filename,
          { readonly: false, readwrite: options.readwrite ?? true, create: options.create ?? true },
          ["PRAGMA busy_timeout=0", "PRAGMA journal_mode=WAL"],
        ),
      );
  });
