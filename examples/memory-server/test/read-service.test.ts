import { describe, expect, it } from "vitest";
import { ReadService, normalizeReadOffset, type ReadChain } from "../../../packages/core/src/protocol/read-service.ts";
import type { StoredMessage, StreamRecord, StreamStoreAdapter } from "../../../packages/core/src/types/storage.ts";

const ZERO_OFFSET = "0000000000000000_0000000000000000";

function record(overrides: Partial<StreamRecord> = {}): StreamRecord {
  return {
    id: "stream-1",
    config: {
      contentType: "application/octet-stream",
      createdAt: 0,
    },
    lifecycle: {
      childRefCount: 0,
    },
    currentOffset: "0000000000000002_0000000000000000",
    counter: 2,
    ...overrides,
  };
}

function message(offset: string): StoredMessage {
  return {
    offset,
    data: new TextEncoder().encode(offset),
    timestamp: 0,
  };
}

function throwingStore(recordValue: StreamRecord | null): StreamStoreAdapter {
  return {
    async get(streamId) {
      expect(streamId).toBe("stream-1");
      return recordValue;
    },
    async create() {
      throw new Error("unexpected create");
    },
    async update() {
      throw new Error("unexpected update");
    },
    async delete() {
      throw new Error("unexpected delete");
    },
    async append() {
      throw new Error("unexpected append");
    },
    async list() {
      throw new Error("unexpected list");
    },
    async deleteMessages() {
      throw new Error("unexpected deleteMessages");
    },
    async getProducerState() {
      throw new Error("unexpected getProducerState");
    },
    async setProducerState() {
      throw new Error("unexpected setProducerState");
    },
    async deleteProducerStates() {
      throw new Error("unexpected deleteProducerStates");
    },
    async incrementChildRefCount() {
      throw new Error("unexpected incrementChildRefCount");
    },
    async decrementChildRefCount() {
      throw new Error("unexpected decrementChildRefCount");
    },
  };
}

const unexpectedReadChain: ReadChain = async () => {
  throw new Error("unexpected readChain");
};

describe("normalizeReadOffset", () => {
  it("treats absent, empty, and -1 offsets as undefined", () => {
    expect(normalizeReadOffset()).toBeUndefined();
    expect(normalizeReadOffset("")).toBeUndefined();
    expect(normalizeReadOffset("-1")).toBeUndefined();
  });

  it("preserves all other offset strings", () => {
    expect(normalizeReadOffset(ZERO_OFFSET)).toBe(ZERO_OFFSET);
    expect(normalizeReadOffset("abc")).toBe("abc");
  });
});

describe("ReadService.execute", () => {
  it("returns not-found without invoking readChain", async () => {
    const service = new ReadService(throwingStore(null), unexpectedReadChain);

    await expect(service.execute("stream-1", {})).resolves.toEqual({
      status: "not-found",
      messages: [],
      nextOffset: "",
      upToDate: false,
    });
  });

  it("returns gone for soft-deleted records without invoking readChain", async () => {
    const service = new ReadService(
      throwingStore(record({ lifecycle: { childRefCount: 0, softDeleted: true } })),
      unexpectedReadChain,
    );

    await expect(service.execute("stream-1", {})).resolves.toEqual({
      status: "gone",
      messages: [],
      nextOffset: "",
      upToDate: false,
    });
  });

  it("normalizes the requested offset before delegating to readChain", async () => {
    const offsets: Array<string | undefined> = [];
    const base = record();
    const service = new ReadService(throwingStore(base), async (streamId, rec, afterOffset) => {
      expect(streamId).toBe("stream-1");
      expect(rec).toBe(base);
      offsets.push(afterOffset);
      return [];
    });

    await service.execute("stream-1", {});
    await service.execute("stream-1", { offset: "" });
    await service.execute("stream-1", { offset: "-1" });
    await service.execute("stream-1", { offset: ZERO_OFFSET });

    expect(offsets).toEqual([undefined, undefined, undefined, ZERO_OFFSET]);
  });

  it("returns currentOffset and upToDate true when no messages are read", async () => {
    const base = record({ currentOffset: "0000000000000005_0000000000000000" });
    const service = new ReadService(throwingStore(base), async () => []);

    await expect(service.execute("stream-1", {})).resolves.toEqual({
      status: "ok",
      messages: [],
      nextOffset: "0000000000000005_0000000000000000",
      upToDate: true,
      closed: false,
    });
  });

  it("uses the last message offset as nextOffset when it is ahead of record currentOffset", async () => {
    const base = record({ currentOffset: "0000000000000002_0000000000000000" });
    const messages = [
      message("0000000000000003_0000000000000000"),
      message("0000000000000004_0000000000000000"),
    ];
    const service = new ReadService(throwingStore(base), async () => messages);

    await expect(service.execute("stream-1", {})).resolves.toEqual({
      status: "ok",
      messages,
      nextOffset: "0000000000000004_0000000000000000",
      upToDate: false,
      closed: false,
    });
  });

  it("keeps record currentOffset as nextOffset when the last message is at or before the tail", async () => {
    const base = record({ currentOffset: "0000000000000005_0000000000000000" });
    const messages = [message("0000000000000003_0000000000000000")];
    const service = new ReadService(throwingStore(base), async () => messages);

    await expect(service.execute("stream-1", {})).resolves.toEqual({
      status: "ok",
      messages,
      nextOffset: "0000000000000005_0000000000000000",
      upToDate: true,
      closed: false,
    });
  });

  it("sets closed true only when record is closed and the read is up to date", async () => {
    const closedTail = record({
      currentOffset: "0000000000000005_0000000000000000",
      lifecycle: { childRefCount: 0, closed: true },
    });
    const closedAhead = record({
      currentOffset: "0000000000000005_0000000000000000",
      lifecycle: { childRefCount: 0, closed: true },
    });

    await expect(new ReadService(throwingStore(closedTail), async () => []).execute("stream-1", {}))
      .resolves.toMatchObject({ status: "ok", upToDate: true, closed: true });

    await expect(
      new ReadService(throwingStore(closedAhead), async () => [
        message("0000000000000006_0000000000000000"),
      ]).execute("stream-1", {}),
    ).resolves.toMatchObject({ status: "ok", upToDate: false, closed: false });
  });
});
