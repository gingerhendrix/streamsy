import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Context, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Hash, Path, Version } from "../server/domain.ts";

const Known = Schema.Struct({
  folderId: Schema.String,
  path: Path,
  version: Schema.NullOr(Version),
  hash: Schema.NullOr(Hash),
  localHash: Schema.NullOr(Hash),
  localVersion: Schema.NullOr(Version),
});

export type Known = typeof Known.Type;

const Pending = Schema.Struct({
  seq: Schema.Int,
  folderId: Schema.String,
  path: Path,
  opId: Schema.String,
  base: Schema.NullOr(Version),
  hash: Schema.NullOr(Hash),
  bytes: Schema.NullOr(Schema.Uint8Array),
});

export type Pending = typeof Pending.Type;

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`CREATE TABLE IF NOT EXISTS known (
    folderId TEXT NOT NULL, path TEXT NOT NULL, version TEXT, hash TEXT, localHash TEXT, localVersion TEXT,
    PRIMARY KEY (folderId, path))`);
  yield* sql.unsafe(`CREATE TABLE IF NOT EXISTS outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, folderId TEXT NOT NULL, path TEXT NOT NULL,
    opId TEXT NOT NULL UNIQUE, base TEXT, hash TEXT, bytes BLOB)`);
  yield* sql.unsafe(
    "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
  );
  yield* sql.unsafe("CREATE TABLE IF NOT EXISTS folders (folderId TEXT PRIMARY KEY)");

  const heads = Effect.fn("SyncState.heads")(function* () {
    return yield* Schema.decodeUnknownEffect(Schema.Array(Known))(yield* sql`SELECT * FROM known`);
  });

  const head = Effect.fn("SyncState.head")(function* (folderId: string, path: string) {
    const rows = yield* sql`SELECT * FROM known WHERE folderId = ${folderId} AND path = ${path}`;

    return (yield* Schema.decodeUnknownEffect(Schema.Array(Known))(rows))[0];
  });

  const save = Effect.fn("SyncState.save")(function* (row: Known) {
    yield* sql`INSERT INTO known ${sql.insert(row)} ON CONFLICT (folderId, path) DO UPDATE SET
      version = excluded.version, hash = excluded.hash, localHash = excluded.localHash, localVersion = excluded.localVersion`;
  });

  const pending = Effect.fn("SyncState.pending")(function* () {
    return yield* Schema.decodeUnknownEffect(Schema.Array(Pending))(
      yield* sql`SELECT * FROM outbox ORDER BY seq`,
    );
  });

  const enqueue = Effect.fn("SyncState.enqueue")(function* (
    row: Omit<Pending, "seq">,
    known: Known,
  ) {
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO outbox ${sql.insert(row)}`;
        yield* save(known);
      }),
    );
  });

  const drop = Effect.fn("SyncState.drop")(function* (seq: number) {
    yield* sql`DELETE FROM outbox WHERE seq = ${seq}`;
  });

  const get = Effect.fn("SyncState.get")(function* (key: string) {
    const rows = yield* sql<{ value: string }>`SELECT value FROM metadata WHERE key = ${key}`;

    return rows[0]?.value;
  });

  const set = Effect.fn("SyncState.set")(function* (key: string, value: string) {
    yield* sql`INSERT INTO metadata (key, value) VALUES (${key}, ${value})
      ON CONFLICT (key) DO UPDATE SET value = excluded.value`;
  });

  const folders = Effect.fn("SyncState.folders")(function* () {
    return (yield* sql<{ folderId: string }>`SELECT folderId FROM folders ORDER BY folderId`).map(
      (row) => row.folderId,
    );
  });

  const addFolder = Effect.fn("SyncState.addFolder")(function* (folderId: string) {
    yield* sql`INSERT INTO folders (folderId) VALUES (${folderId}) ON CONFLICT DO NOTHING`;
  });

  return { heads, head, save, pending, enqueue, drop, get, set, folders, addFolder };
});

export class SyncState extends Context.Service<SyncState, Effect.Success<typeof make>>()(
  "FolderSync/SyncState",
) {}

export const stateLayer = (filename: string) =>
  Layer.effect(SyncState, make).pipe(Layer.provide(SqliteClient.layer({ filename })));
