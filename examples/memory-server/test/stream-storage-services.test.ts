import { describe, expect, it } from "vitest";
import { MemoryStreamStore } from "@streamsy/storage-memory";
import { ExpiryPolicy } from "../../../packages/core/src/protocol/helpers/expiry-policy.ts";
import { ZERO_OFFSET } from "../../../packages/core/src/protocol/helpers/offset-generator.ts";
import { StreamMessageReader } from "../../../packages/core/src/protocol/storage/stream-message-reader.ts";
import { StreamMessageWriter } from "../../../packages/core/src/protocol/storage/stream-message-writer.ts";
import { StreamRecordFactory } from "../../../packages/core/src/protocol/storage/stream-record-factory.ts";
import type {
  Clock,
  StoredMessage,
  StreamConfig,
  StreamLifecycleState,
  StreamRecord,
  StreamStoreAdapter,
} from "../../../packages/core/src/types/storage.ts";

const clock: Clock = {
  now: () => 1_000,
  date: (value?: number | string) => new Date(value ?? 1_000),
};

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function message(offset: string, value: string): StoredMessage {
  return { offset, data: bytes(value), timestamp: 1_000 };
}

function makeRecord(
  overrides: Partial<Omit<StreamRecord, "config" | "lifecycle">> & {
    config?: Partial<StreamConfig>;
    lifecycle?: Partial<StreamLifecycleState>;
  } = {},
): StreamRecord {
  const { config, lifecycle, ...rest } = overrides;
  return {
    id: "stream-1",
    currentOffset: ZERO_OFFSET,
    counter: 0,
    ...rest,
    config: { contentType: "application/octet-stream", createdAt: 0, ...config },
    lifecycle: { childRefCount: 0, ...lifecycle },
  };
}

function makeStore(initialRecords: StreamRecord[] = []): {
  store: StreamStoreAdapter;
  records: Map<string, StreamRecord>;
  messages: Map<string, StoredMessage[]>;
  notifications: Array<{ streamId: string; type: string }>;
} {
  const records = new Map(initialRecords.map((record) => [record.id, record]));
  const messages = new Map<string, StoredMessage[]>();
  const notifications: Array<{ streamId: string; type: string }> = [];
  const store: StreamStoreAdapter = {
    async get(streamId) {
      return records.get(streamId) ?? null;
    },
    async create(record) {
      const existing = records.get(record.id);
      if (existing) return { status: "exists", record: existing };
      records.set(record.id, record);
      return { status: "created" };
    },
    async update(streamId, patch) {
      const current = records.get(streamId);
      if (!current) throw new Error(`missing record: ${streamId}`);
      const next: StreamRecord = {
        ...current,
        ...patch,
        config: { ...current.config, ...patch.config },
        lifecycle: { ...current.lifecycle, ...patch.lifecycle },
      };
      records.set(streamId, next);
      return next;
    },
    async delete(streamId) {
      records.delete(streamId);
    },
    async append(streamId, newMessages) {
      messages.set(streamId, [...(messages.get(streamId) ?? []), ...newMessages]);
    },
    async list(streamId, options = {}) {
      return (messages.get(streamId) ?? []).filter((item) => {
        if (options.after !== undefined && item.offset <= options.after) return false;
        if (options.until !== undefined && item.offset > options.until) return false;
        return true;
      });
    },
    async deleteMessages(streamId) {
      messages.delete(streamId);
    },
    async getProducerState() {
      return undefined;
    },
    async setProducerState() {},
    async deleteProducerStates() {},
    async incrementChildRefCount(parentId) {
      const parent = records.get(parentId)!;
      const count = parent.lifecycle.childRefCount + 1;
      records.set(parentId, {
        ...parent,
        lifecycle: { ...parent.lifecycle, childRefCount: count },
      });
      return count;
    },
    async decrementChildRefCount(parentId) {
      const parent = records.get(parentId)!;
      const count = parent.lifecycle.childRefCount - 1;
      records.set(parentId, {
        ...parent,
        lifecycle: { ...parent.lifecycle, childRefCount: count },
      });
      return count;
    },
    notify(streamId, type) {
      notifications.push({ streamId, type });
    },
  };
  return { store, records, messages, notifications };
}

function expiryPolicy(store: StreamStoreAdapter): ExpiryPolicy {
  return new ExpiryPolicy(store, clock, async () => {});
}

describe("MemoryStreamStore.create", () => {
  it("returns exists without overwriting records, messages, or producer states", async () => {
    const store = new MemoryStreamStore();
    const original = makeRecord({
      id: "stream-1",
      currentOffset: "0000000000000001_0000000000000000",
      counter: 1,
      config: { contentType: "text/plain", createdAt: 0 },
    });
    const replacement = makeRecord({
      id: "stream-1",
      currentOffset: "0000000000000099_0000000000000000",
      counter: 99,
      config: { contentType: "application/json", createdAt: 0 },
    });

    await expect(store.create(original)).resolves.toEqual({ status: "created" });
    await store.append("stream-1", [message("0000000000000001_0000000000000000", "kept")]);
    await store.setProducerState("stream-1", "producer-1", { epoch: 1, lastSeq: 7 });

    await expect(store.create(replacement)).resolves.toEqual({
      status: "exists",
      record: original,
    });

    await expect(store.get("stream-1")).resolves.toEqual(original);
    await expect(store.list("stream-1")).resolves.toEqual([
      message("0000000000000001_0000000000000000", "kept"),
    ]);
    await expect(store.getProducerState("stream-1", "producer-1")).resolves.toEqual({
      epoch: 1,
      lastSeq: 7,
    });
  });
});

describe("StreamRecordFactory", () => {
  it("initializes plain and forked records", () => {
    const { store } = makeStore();
    const factory = new StreamRecordFactory(clock, expiryPolicy(store));

    expect(factory.newRecord("plain", "text/plain", {})).toMatchObject({
      id: "plain",
      currentOffset: ZERO_OFFSET,
      counter: 0,
      config: { contentType: "text/plain", createdAt: 1_000 },
      lifecycle: { childRefCount: 0 },
    });

    const forked = factory.newRecord(
      "fork",
      "text/plain",
      { ttlSeconds: 5 },
      {
        forkedFrom: "plain",
        forkOffset: "0000000000000003_0000000000000000",
      },
    );
    expect(forked.currentOffset).toBe("0000000000000003_0000000000000000");
    expect(forked.counter).toBe(3);
    expect(forked.lifecycle).toMatchObject({
      childRefCount: 0,
      forkedFrom: "plain",
      forkOffset: "0000000000000003_0000000000000000",
      expiresAtMs: 6_000,
    });
  });
});

describe("StreamMessageWriter", () => {
  it("allocates offsets, persists messages, updates record, and notifies", async () => {
    const record = makeRecord();
    const { store, records, messages, notifications } = makeStore([record]);
    const writer = new StreamMessageWriter(store, clock, expiryPolicy(store));

    const nextOffset = await writer.appendMessages(
      "stream-1",
      record,
      [bytes("a"), bytes("b")],
      "seq-1",
    );

    expect(nextOffset).toBe("0000000000000002_0000000000000000");
    expect(messages.get("stream-1")?.map((item) => item.offset)).toEqual([
      "0000000000000001_0000000000000000",
      "0000000000000002_0000000000000000",
    ]);
    expect(records.get("stream-1")).toMatchObject({
      currentOffset: "0000000000000002_0000000000000000",
      counter: 2,
      lifecycle: { lastSeq: "seq-1" },
    });
    expect(notifications).toEqual([{ streamId: "stream-1", type: "message" }]);
  });

  it("closes with optional data and preserves latest counter", async () => {
    const record = makeRecord();
    const { store, records, notifications } = makeStore([record]);
    const writer = new StreamMessageWriter(store, clock, expiryPolicy(store));

    const nextOffset = await writer.closeRecord("stream-1", record, [bytes("final")], "seq-2");

    expect(nextOffset).toBe("0000000000000001_0000000000000000");
    expect(records.get("stream-1")).toMatchObject({
      currentOffset: "0000000000000001_0000000000000000",
      counter: 1,
      lifecycle: { closed: true, closedAt: 1_000, lastSeq: "seq-2" },
    });
    expect(notifications).toEqual([
      { streamId: "stream-1", type: "message" },
      { streamId: "stream-1", type: "closed" },
    ]);
  });
});

describe("StreamMessageReader", () => {
  it("reads inherited fork messages and touches only the requested stream", async () => {
    const source = makeRecord({
      id: "source",
      currentOffset: "0000000000000002_0000000000000000",
      counter: 2,
    });
    const fork = makeRecord({
      id: "fork",
      currentOffset: "0000000000000003_0000000000000000",
      counter: 3,
      config: { ttlSeconds: 10 },
      lifecycle: {
        forkedFrom: "source",
        forkOffset: "0000000000000002_0000000000000000",
      },
    });
    const { store, messages, records } = makeStore([source, fork]);
    messages.set("source", [
      message("0000000000000001_0000000000000000", "a"),
      message("0000000000000002_0000000000000000", "b"),
    ]);
    messages.set("fork", [message("0000000000000003_0000000000000000", "c")]);
    const reader = new StreamMessageReader(store, expiryPolicy(store));

    const result = await reader.readChain("fork", fork, undefined);

    expect(result.map((item) => item.offset)).toEqual([
      "0000000000000001_0000000000000000",
      "0000000000000002_0000000000000000",
      "0000000000000003_0000000000000000",
    ]);
    expect(records.get("fork")?.lifecycle.expiresAtMs).toBe(11_000);
    expect(records.get("source")?.lifecycle.expiresAtMs).toBeUndefined();
  });

  it("reads only one stream and returns the current tail when no messages are found", async () => {
    const record = makeRecord({
      currentOffset: "0000000000000002_0000000000000000",
      counter: 2,
    });
    const { store, messages } = makeStore([record]);
    messages.set("stream-1", [message("0000000000000001_0000000000000000", "a")]);
    const reader = new StreamMessageReader(store, expiryPolicy(store));

    expect(await reader.readOwn("stream-1", "0000000000000001_0000000000000000")).toEqual({
      messages: [],
      nextOffset: "0000000000000002_0000000000000000",
    });
  });
});
