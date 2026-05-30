import type {
  CreateStreamRecordResult,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
} from "@streamsy/core";
import { RECORD_KEY } from "../lib/keys.ts";

type DurableObjectKv = DurableObjectStorage["kv"];

export class RecordStore {
  constructor(
    private readonly getId: () => Promise<StreamId>,
    private readonly kv: DurableObjectKv,
  ) {}

  async getRecord(): Promise<StreamRecord | null> {
    return (await this.kv.get<StreamRecord>(RECORD_KEY)) ?? null;
  }

  async createRecord(record: StreamRecord): Promise<CreateStreamRecordResult> {
    const id = await this.getId();
    if (record.id !== id) {
      throw new Error(`Record id ${record.id} does not match bound stream ${id}`);
    }
    const existing = await this.getRecord();
    if (existing) return { status: "exists", record: existing };
    await this.kv.put(RECORD_KEY, record);
    return { status: "created" };
  }

  async updateRecord(patch: StreamRecordPatch): Promise<StreamRecord> {
    const existing = await this.requireRecord();
    const updated: StreamRecord = {
      ...existing,
      config: { ...existing.config, ...patch.config },
      lifecycle: { ...existing.lifecycle, ...patch.lifecycle },
      currentOffset: patch.currentOffset ?? existing.currentOffset,
      counter: patch.counter ?? existing.counter,
    };
    await this.kv.put(RECORD_KEY, updated);
    return updated;
  }

  async deleteRecord(): Promise<void> {
    await this.kv.delete(RECORD_KEY);
  }

  async incrementChildRefCount(): Promise<number> {
    const record = await this.requireRecord();
    const next = record.lifecycle.childRefCount + 1;
    await this.updateRecord({ lifecycle: { childRefCount: next } });
    return next;
  }

  async decrementChildRefCount(): Promise<number> {
    const record = await this.requireRecord();
    const next = Math.max(0, record.lifecycle.childRefCount - 1);
    await this.updateRecord({ lifecycle: { childRefCount: next } });
    return next;
  }

  async requireRecord(): Promise<StreamRecord> {
    const record = await this.getRecord();
    if (!record) throw new Error(`Stream not found: ${await this.getId()}`);
    return record;
  }
}
