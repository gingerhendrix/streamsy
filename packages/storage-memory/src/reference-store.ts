import type { MemoryRecordStore } from "./record-store.ts";

export class MemoryReferenceStore {
  constructor(private readonly records: MemoryRecordStore) {}

  async incrementChildRefCount(): Promise<number> {
    const updated = this.records.updateStoredRecord((record) => {
      const next = record.lifecycle.childRefCount + 1;
      return {
        ...record,
        lifecycle: { ...record.lifecycle, childRefCount: next },
      };
    });
    return updated.lifecycle.childRefCount;
  }

  async decrementChildRefCount(): Promise<number> {
    const updated = this.records.updateStoredRecord((record) => {
      const next = Math.max(0, record.lifecycle.childRefCount - 1);
      return {
        ...record,
        lifecycle: { ...record.lifecycle, childRefCount: next },
      };
    });
    return updated.lifecycle.childRefCount;
  }
}
