import type { ProducerState, StoredMessage, StreamRecord } from "@streamsy/core";

export interface MemoryEntry {
  record: StreamRecord;
  messages: StoredMessage[];
  producers: Map<string, ProducerState>;
}

export function clone<T>(value: T): T {
  if (value === null || value === undefined) return value;
  return structuredClone(value);
}
