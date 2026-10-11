import { Context, Crypto, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { sha256 } from "./domain.ts";

export class BlobFailure extends Schema.TaggedError<BlobFailure>()("BlobFailure", {
  message: Schema.String,
}) {}

export class BlobStore extends Context.Service<
  BlobStore,
  {
    readonly put: (bytes: Uint8Array) => Effect.Effect<string, BlobFailure>;
    readonly get: (hash: string) => Effect.Effect<Uint8Array | undefined, BlobFailure>;
  }
>()("FolderSync/BlobStore") {}

export const memoryBlobs = Layer.effect(
  BlobStore,
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;

    const hash = (bytes: Uint8Array) =>
      sha256(bytes).pipe(Effect.provideService(Crypto.Crypto, crypto));

    const blobs = new Map<string, Uint8Array>();

    return BlobStore.of({
      put: Effect.fn("BlobStore.memory.put")(function* (bytes) {
        const key = yield* hash(bytes).pipe(
          Effect.mapError(() => new BlobFailure({ message: "Cannot hash blob" })),
        );

        blobs.set(key, bytes.slice());

        return key;
      }),
      get: Effect.fn("BlobStore.memory.get")((key) => Effect.sync(() => blobs.get(key)?.slice())),
    });
  }),
);

export const sqliteBlobs = Layer.effect(
  BlobStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const crypto = yield* Crypto.Crypto;

    const hash = (bytes: Uint8Array) =>
      sha256(bytes).pipe(Effect.provideService(Crypto.Crypto, crypto));

    yield* sql
      .unsafe(
        "CREATE TABLE IF NOT EXISTS folder_sync_blobs (hash TEXT PRIMARY KEY, bytes BLOB NOT NULL)",
      )
      .pipe(Effect.mapError(() => new BlobFailure({ message: "Cannot initialise blobs" })));

    return BlobStore.of({
      put: Effect.fn("BlobStore.sqlite.put")(function* (bytes) {
        const key = yield* hash(bytes).pipe(
          Effect.mapError(() => new BlobFailure({ message: "Cannot hash blob" })),
        );

        yield* sql`INSERT INTO folder_sync_blobs (hash, bytes) VALUES (${key}, ${bytes}) ON CONFLICT (hash) DO NOTHING`.pipe(
          Effect.mapError(() => new BlobFailure({ message: "Cannot save blob" })),
        );

        return key;
      }),
      get: Effect.fn("BlobStore.sqlite.get")((key) =>
        sql<{ bytes: Uint8Array }>`SELECT bytes FROM folder_sync_blobs WHERE hash = ${key}`.pipe(
          Effect.map((rows) => rows[0]?.bytes.slice()),
          Effect.mapError(() => new BlobFailure({ message: "Cannot read blob" })),
        ),
      ),
    });
  }),
);
