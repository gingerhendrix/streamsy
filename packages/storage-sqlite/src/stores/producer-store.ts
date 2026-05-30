import type { Database } from "bun:sqlite";
import type { ProducerState, StreamId } from "@streamsy/core";
import type { RecordStore } from "./record-store.ts";

interface ProducerRow {
  epoch: number;
  last_seq: number;
}

export class ProducerStore {
  constructor(
    private readonly db: Database,
    private readonly id: StreamId,
    private readonly records: RecordStore,
  ) {}

  async getProducerState(producerId: string): Promise<ProducerState | undefined> {
    const row = this.db
      .query<ProducerRow, [StreamId, string]>(
        "select epoch, last_seq from streamsy_producers where stream_id = ? and producer_id = ?",
      )
      .get(this.id, producerId);
    return row ? { epoch: row.epoch, lastSeq: row.last_seq } : undefined;
  }

  async setProducerState(producerId: string, state: ProducerState): Promise<void> {
    this.records.requireRecord();
    this.db.run(
      `insert into streamsy_producers (stream_id, producer_id, epoch, last_seq)
       values (?, ?, ?, ?)
       on conflict(stream_id, producer_id)
       do update set epoch = excluded.epoch, last_seq = excluded.last_seq`,
      [this.id, producerId, state.epoch, state.lastSeq],
    );
  }

  async deleteProducerStates(): Promise<void> {
    this.db.run("delete from streamsy_producers where stream_id = ?", [this.id]);
  }
}
