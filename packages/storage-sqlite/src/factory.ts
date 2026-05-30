/**
 * Bun SQLite `StreamFactory`.
 *
 * Opens (or adopts) a single `bun:sqlite` database, applies migrations, and
 * returns protocol-facing `SqliteStream` instances bound to one id each. The
 * returned factory also exposes the underlying `state` so callers/tests can
 * reach the `Database` or close it.
 */
import type { Stream, StreamFactory, StreamId } from "@streamsy/core";
import { SqliteStreamState, type SqliteStreamStateOptions } from "./state.ts";

export interface SqliteStreamFactoryOptions extends SqliteStreamStateOptions {
  /** Share an existing state instead of opening a new database. */
  state?: SqliteStreamState;
}

export interface SqliteStreamFactory extends StreamFactory {
  readonly state: SqliteStreamState;
  /** Close the underlying database connection. */
  close(): void;
}

export function createSqliteStreamFactory(
  options: SqliteStreamFactoryOptions = {},
): SqliteStreamFactory {
  const { state: existing, ...stateOptions } = options;
  const state = existing ?? new SqliteStreamState(stateOptions);
  return {
    state,
    async getStream(streamId: StreamId): Promise<Stream> {
      return state.getStream(streamId);
    },
    close() {
      state.close();
    },
  };
}
