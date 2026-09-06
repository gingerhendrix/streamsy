import type {
  ProducerId,
  ProducerState,
  RecordPatch,
  StoredMessage,
  StreamId,
  StreamRecord,
} from "../../schema/index.ts";

export interface Entry {
  record: StreamRecord;
  messages: StoredMessage[];
  producers: Map<ProducerId, ProducerState>;
}
export interface State {
  entries: Map<StreamId, Entry>;
  children: Map<StreamId, Set<StreamId>>;
  deadlines: Array<{ at: number; streamId: StreamId }>;
}
export const copyRecord = (record: StreamRecord): StreamRecord => ({
  id: record.id,
  currentOffset: record.currentOffset,
  config: { ...record.config },
  lifecycle: { ...record.lifecycle },
});
export const copyMessage = (message: StoredMessage): StoredMessage => ({
  offset: message.offset,
  timestamp: message.timestamp,
  data: new Uint8Array(message.data),
});
export function patchRecord(record: StreamRecord, patch: RecordPatch): StreamRecord {
  const config = { ...record.config, ...patch.config };
  const lifecycle = { ...record.lifecycle, ...patch.lifecycle };
  for (const key of patch.clear ?? []) {
    switch (key) {
      case "ttlSeconds":
        delete config.ttlSeconds;
        break;
      case "expiresAt":
        delete config.expiresAt;
        break;
      case "expiresAtMs":
        delete lifecycle.expiresAtMs;
        break;
      case "lastSeq":
        delete lifecycle.lastSeq;
        break;
    }
  }
  return {
    id: record.id,
    currentOffset: patch.currentOffset ?? record.currentOffset,
    config,
    lifecycle,
  };
}
export function indexDeadlines(state: State): void {
  state.deadlines = [...state.entries.values()]
    .flatMap(({ record }) =>
      record.lifecycle.softDeleted || record.lifecycle.expiresAtMs === undefined
        ? []
        : [{ at: record.lifecycle.expiresAtMs, streamId: record.id }],
    )
    .toSorted((a, b) => a.at - b.at || a.streamId.localeCompare(b.streamId));
}
