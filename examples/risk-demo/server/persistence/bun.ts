/* oxlint-disable effecttsgo/async-function -- The Bun host owns the storage runtime and native metadata connection. */
import { Database } from "bun:sqlite";
import { BunStorage } from "@streamsy/storage/bun";
import { ManagedRuntime } from "effect";
import { gameStreams, withCheckpoints, type GameStreams } from "../game/streams.ts";
import { createSqliteStores } from "./sqlite-store.ts";

/** Bind the shared game streams to a BunStorage SQLite file. */
export const sqliteStreams = (filename: string): GameStreams =>
  ManagedRuntime.make(withCheckpoints(BunStorage.layer(gameStreams, { client: { filename } })));

export async function openBunStorage(filename: string) {
  const streams = sqliteStreams(filename);
  let db: Database | undefined;
  try {
    // Let BunStorage prepare the SQLite file and WAL before opening metadata.
    await streams.context();
    const connection = new Database(filename);
    db = connection;
    const stores = createSqliteStores(connection);
    return {
      streams,
      stores,
      db: connection,
      async close() {
        await streams.dispose();
        connection.close();
      },
    };
  } catch (error) {
    db?.close();
    await streams.dispose();
    throw error;
  }
}
