import type { CreateStreamRecordResult, StreamRecord, StreamRecordPatch } from "@streamsy/core";
import type { MemoryStream } from "./stream.ts";
import { clone } from "./memory-entry.ts";

export class MemoryRecordStore {
  constructor(private readonly stream: MemoryStream) {}

  async getRecord(): Promise<StreamRecord | null> {
    return clone(this.stream.entry?.record ?? null);
  }

  async createRecord(record: StreamRecord): Promise<CreateStreamRecordResult> {
    if (record.id !== this.stream.id) {
      throw new Error(`Record id ${record.id} does not match bound stream ${this.stream.id}`);
    }
    if (this.stream.entry) return { status: "exists", record: clone(this.stream.entry.record) };
    this.stream.entry = { record: clone(record), messages: [], producers: new Map() };
    return { status: "created" };
  }

  async updateRecord(patch: StreamRecordPatch): Promise<StreamRecord> {
    const entry = this.stream.mustEntry();
    entry.record = {
      ...entry.record,
      config: { ...entry.record.config, ...patch.config },
      lifecycle: { ...entry.record.lifecycle, ...patch.lifecycle },
      currentOffset: patch.currentOffset ?? entry.record.currentOffset,
      counter: patch.counter ?? entry.record.counter,
    };
    return clone(entry.record);
  }

  async deleteRecord(): Promise<void> {
    this.stream.entry = undefined;
    await this.stream.cancelExpiry();
    this.stream.onDeleted();
  }
}
