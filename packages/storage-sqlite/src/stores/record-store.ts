import type { Database } from "bun:sqlite";
import type { StreamId, StreamRecord, StreamRecordPatch } from "@streamsy/core";
import { recordToRow, rowToRecord, STREAM_COLUMNS, type StreamRow } from "../lib/codec.ts";

export type CreateRecordResult = { status: "created" } | { status: "exists"; record: StreamRecord };

export const INSERT_SQL = `insert into streamsy_streams (${STREAM_COLUMNS.join(", ")}) values (${STREAM_COLUMNS.map(
  () => "?",
).join(", ")}) on conflict(stream_id) do nothing`;

const UPDATE_SQL = `update streamsy_streams set ${STREAM_COLUMNS.filter((c) => c !== "stream_id")
  .map((c) => `${c} = ?`)
  .join(", ")} where stream_id = ?`;

export class RecordStore {
  constructor(
    private readonly db: Database,
    readonly id: StreamId,
  ) {}

  getRecord(): StreamRecord | null {
    const row = this.row();
    return row ? rowToRecord(row) : null;
  }

  createRecord(record: StreamRecord): CreateRecordResult {
    if (record.id !== this.id) {
      throw new Error(`Record id ${record.id} does not match bound stream ${this.id}`);
    }
    const result = this.db.run(INSERT_SQL, recordToRow(record));
    if (result.changes > 0) return { status: "created" };
    return { status: "exists", record: rowToRecord(this.requireRow()) };
  }

  updateRecord(patch: StreamRecordPatch): StreamRecord {
    const record = rowToRecord(this.requireRow());
    const next: StreamRecord = {
      ...record,
      config: { ...record.config, ...patch.config },
      lifecycle: { ...record.lifecycle, ...patch.lifecycle },
      currentOffset: patch.currentOffset ?? record.currentOffset,
      counter: patch.counter ?? record.counter,
    };
    const [, ...rest] = recordToRow(next);
    this.db.run(UPDATE_SQL, [...rest, this.id]);
    return next;
  }

  deleteRecord(): void {
    this.db.run("delete from streamsy_streams where stream_id = ?", [this.id]);
  }

  requireRecord(): StreamRecord {
    return rowToRecord(this.requireRow());
  }

  private row(): StreamRow | null {
    return (
      this.db
        .query<StreamRow, [StreamId]>("select * from streamsy_streams where stream_id = ?")
        .get(this.id) ?? null
    );
  }

  private requireRow(): StreamRow {
    const row = this.row();
    if (!row) throw new Error(`Stream not found: ${this.id}`);
    return row;
  }
}
