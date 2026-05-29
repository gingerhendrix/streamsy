import type {
  CreateStreamRecordResult,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
} from "@streamsy/core";
import { clone } from "./clone.ts";

export class MemoryRecordStore {
  private record?: StreamRecord;

  constructor(readonly id: StreamId) {}

  async getRecord(): Promise<StreamRecord | null> {
    return clone(this.record ?? null);
  }

  async createRecord(record: StreamRecord): Promise<CreateStreamRecordResult> {
    if (record.id !== this.id) {
      throw new Error(`Record id ${record.id} does not match bound stream ${this.id}`);
    }
    if (this.record) return { status: "exists", record: clone(this.record) };
    this.record = clone(record);
    return { status: "created" };
  }

  async updateRecord(patch: StreamRecordPatch): Promise<StreamRecord> {
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

  async deleteRecord(): Promise<void> {
    this.record = undefined;
  }

  requireRecord(): StreamRecord {
    if (!this.record) throw new Error(`Stream not found: ${this.id}`);
    return this.record;
  }

  updateStoredRecord(update: (record: StreamRecord) => StreamRecord): StreamRecord {
    this.record = update(this.requireRecord());
    return this.record;
  }
}
