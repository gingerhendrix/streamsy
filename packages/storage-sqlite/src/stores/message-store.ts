import type { Database } from "bun:sqlite";
import type { ListMessagesOptions, StoredMessage, StreamId } from "@streamsy/core";
import type { RecordStore } from "./record-store.ts";

interface MessageRow {
  offset: string;
  timestamp: number;
  data: Uint8Array;
}

export class MessageStore {
  constructor(
    private readonly db: Database,
    private readonly id: StreamId,
    private readonly records: RecordStore,
  ) {}

  async appendMessages(messages: StoredMessage[]): Promise<void> {
    this.records.requireRecord();
    if (messages.length === 0) return;
    const insert = this.db.query(
      "insert into streamsy_messages (stream_id, offset, timestamp, data) values (?, ?, ?, ?)",
    );
    const run = this.db.transaction((batch: StoredMessage[]) => {
      for (const message of batch) {
        insert.run(this.id, message.offset, message.timestamp, message.data);
      }
    });
    run(messages);
  }

  async listMessages(options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    const clauses = ["stream_id = ?"];
    const params: (string | number)[] = [this.id];
    if (options.after !== undefined) {
      clauses.push("offset > ?");
      params.push(options.after);
    }
    if (options.until !== undefined) {
      clauses.push("offset <= ?");
      params.push(options.until);
    }
    let sql = `select offset, timestamp, data from streamsy_messages where ${clauses.join(
      " and ",
    )} order by offset asc`;
    if (options.limit !== undefined) {
      sql += " limit ?";
      params.push(options.limit);
    }
    const rows = this.db.query<MessageRow, (string | number)[]>(sql).all(...params);
    return rows.map((row) => ({
      offset: row.offset,
      timestamp: row.timestamp,
      data: new Uint8Array(row.data),
    }));
  }

  async deleteMessages(): Promise<void> {
    this.db.run("delete from streamsy_messages where stream_id = ?", [this.id]);
  }
}
