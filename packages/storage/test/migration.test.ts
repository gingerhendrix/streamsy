/* oxlint-disable effecttsgo/async-function, effecttsgo/crypto-random-uuid, effecttsgo/node-builtin-import -- Bun owns retained migration fixtures. */
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { Cause, Config, Context, Effect, Exit, Layer, Option, Scope } from "effect";
import { Storage, StorageFault, StreamId } from "@streamsy/core";
import * as SqlClientTag from "effect/sql/SqlClient";
import { BunStorage } from "@streamsy/storage/bun";
import { sharedSqlClientLayer } from "../src/boundary.ts";
import { migrate } from "../src/migrations.ts";
import {
  STORAGE_MIGRATIONS,
  STORAGE_SCHEMA_VERSION,
  STORAGE_VERSION_TABLE,
} from "../src/schema.ts";

const scratch = Effect.runSync(
  Config.String("STREAMSY_STORAGE_SCRATCH").pipe(Config.withDefault("/tmp")),
);

const filename = (label: string) =>
  `${scratch}/${label}-${process.pid}-${crypto.randomUUID()}.sqlite`;

const acquire = (path: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();

    const context = yield* Layer.build(
      BunStorage.layerProtocol({ client: { filename: path, busyTimeout: "50 millis" } }),
    ).pipe(Scope.provide(scope));

    return { scope, storage: Context.get(context, Storage) };
  });

test("fresh creation and repeated open retain the current version", async () => {
  const path = filename("fresh-repeat");
  const first = await Effect.runPromise(acquire(path));
  await Effect.runPromise(Scope.close(first.scope, Exit.void));
  const second = await Effect.runPromise(acquire(path));
  await Effect.runPromise(Scope.close(second.scope, Exit.void));
  const db = new Database(path, { readonly: true });
  expect(
    db
      .query<{ version: number }, []>(`SELECT MAX(version) version FROM ${STORAGE_VERSION_TABLE}`)
      .get()?.version,
  ).toBe(STORAGE_SCHEMA_VERSION);
  db.close(false);
});

test("public Bun acquisition preserves explicit create and readonly open modes", async () => {
  const expectTypedMissing = async (
    label: string,
    options: { readonly?: boolean; create?: boolean; disableWAL?: boolean },
  ) => {
    const missing = filename(label);
    expect(await Bun.file(missing).exists()).toBe(false);

    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Layer.build(BunStorage.layerProtocol({ client: { filename: missing, ...options } })),
      ),
    );

    expect(Exit.isFailure(exit)).toBe(true);

    if (Exit.isSuccess(exit)) throw new Error("Expected missing-file acquisition to fail");
    expect(Option.getOrThrow(Cause.findErrorOption(exit.cause))).toMatchObject({
      _tag: "StorageFault",
      retryable: false,
    });
    expect(await Bun.file(missing).exists()).toBe(false);
  };

  await expectTypedMissing("create-false", { create: false });
  await expectTypedMissing("readonly-missing", { readonly: true });
  await expectTypedMissing("create-false-no-wal", { create: false, disableWAL: true });

  const readonly = filename("readonly-delete");
  const initialized = await Effect.runPromise(acquire(readonly));
  await Effect.runPromise(Scope.close(initialized.scope, Exit.void));
  const writable = new Database(readonly, { readwrite: true });
  expect(
    writable.query<{ journal_mode: string }, []>("PRAGMA journal_mode=DELETE").get()?.journal_mode,
  ).toBe("delete");
  writable.close(false);
  const before = new Uint8Array(await Bun.file(readonly).arrayBuffer());

  const readonlyExit = await Effect.runPromiseExit(
    Effect.scoped(
      Layer.build(BunStorage.layerProtocol({ client: { filename: readonly, readonly: true } })),
    ),
  );

  expect(Exit.isSuccess(readonlyExit)).toBe(true);
  const after = new Uint8Array(await Bun.file(readonly).arrayBuffer());
  expect(after).toEqual(before);
  const inspected = new Database(readonly, { readonly: true });
  expect(
    inspected.query<{ journal_mode: string }, []>("PRAGMA journal_mode").get()?.journal_mode,
  ).toBe("delete");
  inspected.close(false);
});

test("reopens a nonempty current-format v1 database", async () => {
  const path = filename("reopen-v1");
  const db = new Database(path, { create: true });
  db.run(
    `CREATE TABLE ${STORAGE_VERSION_TABLE}(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)`,
  );

  for (const statement of STORAGE_MIGRATIONS[0]?.sqlite ?? []) db.run(statement);
  db.run(`INSERT INTO ${STORAGE_VERSION_TABLE} VALUES (1, 1)`);
  db.run(
    "INSERT INTO streamsy_streams VALUES ('kept','text/plain',NULL,NULL,0,'0000000000000000_0000000000000000',NULL,0,NULL,NULL,NULL,NULL,0,42)",
  );
  db.close(false);
  const opened = await Effect.runPromise(acquire(path));
  expect(await Effect.runPromise(opened.storage.nextExpiry)).toMatchObject({
    _tag: "Some",
    value: { at: 42, streamId: "kept" },
  });
  await Effect.runPromise(Scope.close(opened.scope, Exit.void));
});

test("an injected migration failure rolls back schema and version rows", async () => {
  const path = filename("rollback");
  const owner = await Effect.runPromise(Scope.make());

  const clientContext = await Effect.runPromise(
    Layer.build(
      Layer.effectContext(sharedSqlClientLayer(SqliteClient.make({ filename: path }))),
    ).pipe(Scope.provide(owner)),
  );

  const sql = Context.get(clientContext, SqlClientTag.SqlClient);

  const failed = await Effect.runPromiseExit(
    migrate(sql, {
      beforeVersion: (version) =>
        version === 1
          ? Effect.fail(
              new StorageFault({
                operation: "migration.injected",
                message: "injected",
                retryable: false,
              }),
            )
          : Effect.void,
    }),
  );

  expect(Exit.isFailure(failed)).toBe(true);
  await Effect.runPromise(Scope.close(owner, Exit.void));
  const db = new Database(path, { readonly: true });
  expect(
    db.query("SELECT name FROM sqlite_master WHERE name=?").get(STORAGE_VERSION_TABLE),
  ).toBeNull();
  expect(db.query("SELECT name FROM sqlite_master WHERE name='streamsy_streams'").get()).toBeNull();
  db.close(false);
});

test("legacy and too-new files reject without changing retained bytes", async () => {
  for (const kind of ["legacy", "legacy-0.4", "owner-v3", "owner-only", "newer"] as const) {
    const path = filename(kind);
    const db = new Database(path, { create: true });

    if (kind === "legacy") {
      db.run(
        "CREATE TABLE streamsy_schema_version(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)",
      );
      db.run("INSERT INTO streamsy_schema_version VALUES (3, 1)");
    } else if (kind === "legacy-0.4") {
      db.run(
        "CREATE TABLE streamsy_storage_schema_version(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)",
      );
      db.run("INSERT INTO streamsy_storage_schema_version VALUES (2, 1)");
    } else if (kind === "owner-v3") {
      // The pre-reset 0.4 layout with migration 3 and its unprefixed owner records.
      db.run(
        "CREATE TABLE streamsy_storage_schema_version(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)",
      );
      db.run("INSERT INTO streamsy_storage_schema_version VALUES (3, 1)");
      db.run(
        "CREATE TABLE streamsy_owner_records (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)",
      );
    } else if (kind === "owner-only") {
      db.run(
        "CREATE TABLE streamsy_owner_records (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)",
      );
    } else {
      db.run(
        `CREATE TABLE ${STORAGE_VERSION_TABLE}(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)`,
      );
      db.run(`INSERT INTO ${STORAGE_VERSION_TABLE} VALUES (?, 1)`, [STORAGE_SCHEMA_VERSION + 1]);
    }

    db.close(false);
    const before = await Bun.file(path).arrayBuffer();
    const exit = await Effect.runPromiseExit(acquire(path));
    expect(Exit.isFailure(exit)).toBe(true);

    if (kind !== "legacy" && kind !== "newer" && Exit.isFailure(exit))
      expect(Option.getOrThrow(Cause.findErrorOption(exit.cause)).message).toContain(
        "use a fresh database path",
      );
    const after = await Bun.file(path).arrayBuffer();
    expect(new Uint8Array(after)).toEqual(new Uint8Array(before));
  }
});

test("concurrent initialization exposes a complete schema", async () => {
  const path = filename("concurrent");

  const [left, right] = await Promise.all([
    Effect.runPromise(acquire(path)),
    Effect.runPromise(acquire(path)),
  ]);

  expect(await Effect.runPromise(left.storage.record(StreamId.make("x")))).toMatchObject({
    _tag: "None",
  });
  expect(await Effect.runPromise(right.storage.record(StreamId.make("x")))).toMatchObject({
    _tag: "None",
  });
  await Promise.all([
    Effect.runPromise(Scope.close(left.scope, Exit.void)),
    Effect.runPromise(Scope.close(right.scope, Exit.void)),
  ]);
});
