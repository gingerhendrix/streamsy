import { composeStream } from "../factory/compose-stream.ts";
import type { StreamFactory } from "../types/factory.ts";
import type { StreamId } from "../types/storage.ts";
import type {
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  StreamRecord,
  StreamRecordPatch,
  WaitForEventOptions,
  WaitForEventResult,
  StreamEventType,
} from "../types/storage.ts";

interface Entry {
  record: StreamRecord;
  messages: StoredMessage[];
  producers: Map<string, ProducerState>;
}

export function createInMemoryFactory(): StreamFactory {
  const entries = new Map<string, Entry>();
  const waiters = new Map<string, Set<(r: WaitForEventResult) => void>>();
  const locks = new Map<string, Promise<void>>();
  const must = (id: string) => {
    const entry = entries.get(id);
    if (!entry) throw new Error(`missing ${id}`);
    return entry;
  };
  const withLock = async <T>(key: string, fn: () => Promise<T>): Promise<T> => {
    while (locks.has(key)) await locks.get(key);
    let release!: () => void;
    locks.set(key, new Promise<void>((resolve) => (release = resolve)));
    try {
      return await fn();
    } finally {
      locks.delete(key);
      release();
    }
  };
  const notify = (id: string, type: StreamEventType) => {
    const active = [...(waiters.get(id) ?? [])];
    waiters.delete(id);
    for (const waiter of active) waiter({ status: "notified", type });
  };
  return {
    getStream(id: StreamId) {
      return composeStream({
        id,
        recordStore: {
          getRecord: async () => structuredClone(entries.get(id)?.record ?? null),
          createRecord: async (record) => {
            const existing = entries.get(id);
            if (existing) return { status: "exists", record: structuredClone(existing.record) };
            entries.set(id, {
              record: structuredClone(record),
              messages: [],
              producers: new Map(),
            });
            return { status: "created" };
          },
          updateRecord: async (patch: StreamRecordPatch) => {
            const entry = must(id);
            entry.record = {
              ...entry.record,
              config: { ...entry.record.config, ...patch.config },
              lifecycle: { ...entry.record.lifecycle, ...patch.lifecycle },
              currentOffset: patch.currentOffset ?? entry.record.currentOffset,
              counter: patch.counter ?? entry.record.counter,
            };
            return structuredClone(entry.record);
          },
          deleteRecord: async () => {
            entries.delete(id);
          },
        },
        messageStore: {
          appendMessages: async (messages: StoredMessage[]) => {
            must(id).messages.push(...structuredClone(messages));
          },
          listMessages: async (options: ListMessagesOptions = {}) => {
            let out = entries.get(id)?.messages ?? [];
            if (options.after) out = out.filter((m) => m.offset > options.after!);
            if (options.until) out = out.filter((m) => m.offset <= options.until!);
            if (options.limit !== undefined) out = out.slice(0, options.limit);
            return structuredClone(out);
          },
          deleteMessages: async () => {
            const entry = entries.get(id);
            if (entry) entry.messages = [];
          },
        },
        producerStore: {
          getProducerState: async (producerId) =>
            structuredClone(entries.get(id)?.producers.get(producerId)),
          setProducerState: async (producerId, state) => {
            must(id).producers.set(producerId, structuredClone(state));
          },
          deleteProducerStates: async () => entries.get(id)?.producers.clear(),
        },
        referenceTracker: {
          incrementChildRefCount: async () => {
            const entry = must(id);
            return ++entry.record.lifecycle.childRefCount;
          },
          decrementChildRefCount: async () => {
            const entry = must(id);
            return (entry.record.lifecycle.childRefCount = Math.max(
              0,
              entry.record.lifecycle.childRefCount - 1,
            ));
          },
        },
        mutations: { withMutationLock: (fn) => withLock(`stream:${id}`, fn) },
        events: {
          waitForEvent: async (options: WaitForEventOptions) =>
            new Promise((resolve) => {
              const timeout = setTimeout(() => finish({ status: "timeout" }), options.timeoutMs);
              const finish = (result: WaitForEventResult) => {
                clearTimeout(timeout);
                waiters.get(id)?.delete(finish);
                resolve(result);
              };
              waiters.set(id, waiters.get(id) ?? new Set());
              waiters.get(id)!.add(finish);
            }),
          notify: (type) => notify(id, type),
        },
        expiry: { scheduleExpiry: () => undefined, cancelExpiry: () => undefined },
      });
    },
  };
}
