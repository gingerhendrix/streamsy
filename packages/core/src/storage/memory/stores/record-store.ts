import type { StreamId, StreamRecord, StreamRecordPatch } from "../../../types/storage.ts";
import { clone } from "../lib/clone.ts";

export type CreateRecordResult = { status: "created" } | { status: "exists"; record: StreamRecord };

export class RecordStore {
  private record?: StreamRecord;

  constructor(readonly id: StreamId) {}

  getRecord(): StreamRecord | null {
    return clone(this.record ?? null);
  }

  createRecord(record: StreamRecord): CreateRecordResult {
    if (record.id !== this.id) {
      throw new Error(`Record id ${record.id} does not match bound stream ${this.id}`);
    }
    if (this.record) return { status: "exists", record: clone(this.record) };
    this.record = clone(record);
    return { status: "created" };
  }

  updateRecord(patch: StreamRecordPatch): StreamRecord {
    const record = this.requireRecord();
    this.record = {
      ...record,
      config: { ...record.config, ...patch.config },
      lifecycle: { ...record.lifecycle, ...patch.lifecycle },
      currentOffset: patch.currentOffset ?? record.currentOffset,
      counter: patch.counter ?? record.counter,
    };
    return clone(this.record);
  }

  deleteRecord(): void {
    this.record = undefined;
  }

  requireRecord(): StreamRecord {
    if (!this.record) throw new Error(`Stream not found: ${this.id}`);
    return this.record;
  }
}
