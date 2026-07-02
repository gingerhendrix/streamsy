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

  async purgeSelf(id: StreamId): Promise<void> {
    const stream = this.state.getExistingStream(id);
    if (stream) {
      stream.purgeSelf();
      return;
    }
    const purge = this.db.transaction(() => {
      this.db.run("delete from streamsy_messages where stream_id = ?", [id]);
      this.db.run("delete from streamsy_producers where stream_id = ?", [id]);
      this.db.run("delete from streamsy_streams where stream_id = ?", [id]);
    });
    purge();
    this.state.deleteFromCache(id);
  }

  async softDelete(id: StreamId): Promise<void> {
    this.db.run("update streamsy_streams set soft_deleted = 1 where stream_id = ?", [id]);
  }
}
