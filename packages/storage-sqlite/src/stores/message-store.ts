import type { Database } from "bun:sqlite";
import type { ListMessagesOptions, StoredMessage, StreamId } from "@streamsy/core";
interface MessageRow {
  offset: string;
  timestamp: number;
  data: Uint8Array;
}

export class MessageStore {
  constructor(
    private readonly db: Database,
    private readonly id: StreamId,
  ) {}

  appendMessagesSync(messages: StoredMessage[]): void {
    if (messages.length === 0) return;
    const insert = this.db.query(
      "insert into streamsy_messages (stream_id, offset, timestamp, data) values (?, ?, ?, ?)",
    );
    for (const message of messages) {
      insert.run(this.id, message.offset, message.timestamp, message.data);
    }
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

  deleteMessagesSync(): void {
    this.db.run("delete from streamsy_messages where stream_id = ?", [this.id]);
  }
}
