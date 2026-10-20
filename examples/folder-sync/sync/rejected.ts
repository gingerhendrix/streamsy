import { BunServices } from "@effect/platform-bun";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Console, Effect, FileSystem, Layer, Path, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { ArgumentsError } from "./arguments.ts";
import { safePath } from "./local.ts";

export const parseRejectedArguments = (args: ReadonlyArray<string>) => {
  if (args.length !== 2 || args[0] !== "--dir" || !args[1])
    throw new ArgumentsError({ message: "bun run sync rejected --dir <path>" });

  return args[1];
};

const Summary = Schema.Struct({
  seq: Schema.Int,
  folderId: Schema.String,
  path: Schema.String,
  txid: Schema.String,
  base: Schema.NullOr(Schema.Int),
  size: Schema.Int,
  status: Schema.Int,
  reason: Schema.String,
});

/** Read-only inspection works offline and never loads retained file bodies. */
export const listRejected = (directory: string) => {
  const database = Layer.unwrap(
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const fs = yield* FileSystem.FileSystem;
      const file = yield* safePath(path.resolve(directory), ".folder-sync/state.sqlite");

      if (!(yield* fs.exists(file)))
        return yield* new ArgumentsError({ message: "No sync state in this directory" });

      return SqliteClient.layer({ filename: file, readonly: true });
    }),
  ).pipe(Layer.provide(BunServices.layer));

  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    const rows = yield* Schema.decodeUnknownEffect(Schema.Array(Summary))(
      yield* sql`SELECT seq, folderId, path, txid, base, size, status, reason FROM rejected ORDER BY seq`,
    );

    yield* Console.log(
      yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Summary)))(rows),
    );
  }).pipe(
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Executable inspection boundary owns its scoped read-only SQLite connection.
    Effect.provide(database),
    Effect.scoped,
  );
};
