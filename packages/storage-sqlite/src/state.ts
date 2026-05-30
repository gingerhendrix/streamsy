import { Database } from "bun:sqlite";
import { migrate } from "./schema.ts";
import { SqliteStream } from "./stream.ts";

export interface SqliteStreamStateOptions {
  /** File path, or `:memory:` (default) for an isolated in-memory database. */
  filename?: string;
  /** Use a caller-owned `Database` instead of opening one. */
  database?: Database;
  /** Apply outstanding migrations on open. Default `true`. */
  migrate?: boolean;
  /** Enable WAL journal mode for file-backed databases. Default `true`. */
  wal?: boolean;
  /** SQLite busy timeout in milliseconds. Default `5000`. */
  busyTimeoutMs?: number;
}

/**
 * Owns the shared `Database` connection and a per-id cache of `SqliteStream`
 * objects. Caching keeps the in-process mutation lock / notifier / expiry timer
 * stable across concurrent lookups for the same stream id.
 */
export class SqliteStreamState {
  readonly db: Database;
  private readonly streams = new Map<string, SqliteStream>();

  constructor(options: SqliteStreamStateOptions = {}) {
    const filename = options.filename ?? ":memory:";
    this.db = options.database ?? new Database(filename, { create: true });

    const isFile = options.database === undefined && filename !== ":memory:";
    this.db.run("pragma foreign_keys = ON");
    if ((options.wal ?? true) && isFile) this.db.run("pragma journal_mode = WAL");
    this.db.run(`pragma busy_timeout = ${options.busyTimeoutMs ?? 5000}`);

    if (options.migrate ?? true) migrate(this.db);
  }

  getStream(id: string): SqliteStream {
    let stream = this.streams.get(id);
    if (!stream) {
      stream = new SqliteStream(this.db, id, () => this.streams.delete(id));
      this.streams.set(id, stream);
    }
    return stream;
  }

  close(): void {
    this.db.close();
  }
}
