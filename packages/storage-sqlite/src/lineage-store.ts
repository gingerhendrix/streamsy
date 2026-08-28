import type { Database } from "bun:sqlite";
import type { LineageStore, StreamId, StreamRecord } from "@streamsy/core";
import { rowToRecord, type StreamRow } from "./lib/codec.ts";
import type { SqliteStreamState } from "./state.ts";

export class SqliteLineageStore implements LineageStore {
  constructor(
    private readonly db: Database,
    private readonly state: SqliteStreamState,
  ) {}

  async getRecord(id: StreamId): Promise<StreamRecord | null> {
    const row =
      this.db
        .query<StreamRow, [StreamId]>("select * from streamsy_streams where stream_id = ?")
        .get(id) ?? null;
    return row ? rowToRecord(row) : null;
  }

  async purgeSelf(id: StreamId, expectedExpiresAtMs?: number): Promise<boolean> {
    const purge = this.db.transaction(() => {
      const row = this.db
        .query<StreamRow, [StreamId]>("select * from streamsy_streams where stream_id = ?")
        .get(id);
      if (!row || (expectedExpiresAtMs !== undefined && row.expires_at_ms !== expectedExpiresAtMs))
        return false;
      this.db.run("delete from streamsy_messages where stream_id = ?", [id]);
      this.db.run("delete from streamsy_producers where stream_id = ?", [id]);
      this.db.run("delete from streamsy_streams where stream_id = ?", [id]);
      return true;
    });
    const purged = purge();
    if (!purged) return false;
    this.state.getExistingStream(id)?.wake();
    this.state.deleteFromCache(id);
    return true;
  }

  async softDelete(id: StreamId, expectedExpiresAtMs?: number): Promise<boolean> {
    const updated =
      expectedExpiresAtMs === undefined
        ? this.db.run("update streamsy_streams set soft_deleted = 1 where stream_id = ?", [id])
        : this.db.run(
            `update streamsy_streams set soft_deleted = 1
             where stream_id = ? and expires_at_ms = ?`,
            [id, expectedExpiresAtMs],
          );
    return updated.changes > 0;
  }
}
