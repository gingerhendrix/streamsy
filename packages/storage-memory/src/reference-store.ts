import type { MemoryStream } from "./stream.ts";

export class MemoryReferenceStore {
  constructor(private readonly stream: MemoryStream) {}

  async incrementChildRefCount(): Promise<number> {
    const entry = this.stream.mustEntry();
    const next = entry.record.lifecycle.childRefCount + 1;
    entry.record = {
      ...entry.record,
      lifecycle: { ...entry.record.lifecycle, childRefCount: next },
    };
    return next;
  }

  async decrementChildRefCount(): Promise<number> {
    const entry = this.stream.mustEntry();
    const next = Math.max(0, entry.record.lifecycle.childRefCount - 1);
    entry.record = {
      ...entry.record,
      lifecycle: { ...entry.record.lifecycle, childRefCount: next },
    };
    return next;
  }
}
