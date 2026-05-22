/**
 * Focused unit coverage for CreateStreamService extracted from StreamProtocol.
 *
 * The full create surface is covered by the conformance suite. These cases
 * pin the orchestration order of the extracted service:
 *
 *   existing record -> idempotency / conflict shape
 *     -> no existing + forkedFrom -> delegate to createFork callback
 *     -> no existing + non-fork -> default contentType, frame initialData,
 *        newRecord, store.create, scheduleExpiry, appendMessages, closeRecord
 *        -> CreateResult { status: "created" }
 */

import { describe, expect, it } from "vitest";
import {
  CreateStreamService,
  type CreateStreamMutators,
} from "../protocol/create-stream-service.ts";
import type { CreateOptions, CreateResult } from "../types/protocol.ts";
import type { StreamId, StreamRecord, StreamStoreAdapter } from "../types/storage.ts";

const CONTENT_TYPE = "application/octet-stream";

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function notImplemented(method: string): never {
  throw new Error(`store.${method} not implemented in test stub`);
}

interface CreateCall {
  record: StreamRecord;
}

interface Stub {
  store: StreamStoreAdapter;
  records: Map<StreamId, StreamRecord>;
  createCalls: CreateCall[];
}

function makeStub(initialRecords: StreamRecord[] = []): Stub {
  const records = new Map<StreamId, StreamRecord>();
  for (const r of initialRecords) records.set(r.id, r);
  const createCalls: CreateCall[] = [];
  const store: StreamStoreAdapter = {
    async get(streamId) {
      return records.get(streamId) ?? null;
    },
    async create(record) {
      createCalls.push({ record });
      const existing = records.get(record.id);
      if (existing) return { status: "exists", record: existing };
      records.set(record.id, record);
      return { status: "created" };
    },
    update: () => notImplemented("update"),
    delete: () => notImplemented("delete"),
    append: () => notImplemented("append"),
    list: () => notImplemented("list"),
    deleteMessages: () => notImplemented("deleteMessages"),
    getProducerState: () => notImplemented("getProducerState"),
    setProducerState: () => notImplemented("setProducerState"),
    deleteProducerStates: () => notImplemented("deleteProducerStates"),
    incrementChildRefCount: () => notImplemented("incrementChildRefCount"),
    decrementChildRefCount: () => notImplemented("decrementChildRefCount"),
  };
  return { store, records, createCalls };
}

function makeRecord(overrides: Partial<StreamRecord> = {}): StreamRecord {
  const { config, lifecycle, ...rest } = overrides;
  return {
    id: "stream-1",
    currentOffset: "00000000000000000000",
    counter: 0,
    ...rest,
    config: { contentType: CONTENT_TYPE, createdAt: 0, ...config },
    lifecycle: { childRefCount: 0, ...lifecycle },
  };
}

interface MutatorLog {
  newRecord: Array<{ streamId: StreamId; contentType: string; options: CreateOptions }>;
  scheduleExpiry: StreamRecord[];
  appendMessages: Array<{ streamId: StreamId; record: StreamRecord; data: Uint8Array[] }>;
  closeRecord: Array<{ streamId: StreamId; record: StreamRecord; data: Uint8Array[] }>;
  createFork: Array<{ streamId: StreamId; options: CreateOptions }>;
}

function makeMutators(
  options: {
    appendNextOffset?: string;
    closeNextOffset?: string;
    forkResult?: CreateResult;
    overrideRecord?: (defaults: StreamRecord) => StreamRecord;
  } = {},
): { mutators: CreateStreamMutators; log: MutatorLog } {
  const log: MutatorLog = {
    newRecord: [],
    scheduleExpiry: [],
    appendMessages: [],
    closeRecord: [],
    createFork: [],
  };
  const mutators: CreateStreamMutators = {
    newRecord(streamId, contentType, opts) {
      log.newRecord.push({ streamId, contentType, options: opts });
      const base: StreamRecord = {
        id: streamId,
        currentOffset: "00000000000000000000",
        counter: 0,
        config: {
          contentType,
          ttlSeconds: opts.ttlSeconds,
          expiresAt: opts.expiresAt,
          createdAt: 0,
        },
        lifecycle: {
          childRefCount: 0,
          expiresAtMs: opts.ttlSeconds !== undefined ? 60_000 : undefined,
        },
      };
      return options.overrideRecord ? options.overrideRecord(base) : base;
    },
    async scheduleExpiry(record) {
      log.scheduleExpiry.push(record);
    },
    async appendMessages(streamId, record, data) {
      log.appendMessages.push({ streamId, record, data });
      return options.appendNextOffset ?? "00000000000000000003";
    },
    async closeRecord(streamId, record, data) {
      log.closeRecord.push({ streamId, record, data });
      return options.closeNextOffset ?? options.appendNextOffset ?? "00000000000000000003";
    },
    async createFork(streamId, opts) {
      log.createFork.push({ streamId, options: opts });
      return (
        options.forkResult ?? {
          status: "created",
          nextOffset: "00000000000000000000",
          contentType: CONTENT_TYPE,
        }
      );
    },
  };
  return { mutators, log };
}

describe("CreateStreamService.execute - existing record", () => {
  it("returns conflict/soft-deleted when existing target is soft-deleted", async () => {
    const stub = makeStub([makeRecord({ lifecycle: { childRefCount: 0, softDeleted: true } })]);
    const { mutators, log } = makeMutators();
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", { contentType: CONTENT_TYPE });

    expect(result).toEqual({
      status: "conflict",
      nextOffset: "",
      contentType: "",
      conflictReason: "soft-deleted",
    });
    expect(stub.createCalls).toHaveLength(0);
    expect(log.newRecord).toHaveLength(0);
  });

  it("returns conflict/config-mismatch when config does not match", async () => {
    const stub = makeStub([
      makeRecord({
        config: { contentType: CONTENT_TYPE, createdAt: 0, ttlSeconds: 60 },
      }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", { contentType: CONTENT_TYPE, ttlSeconds: 30 });

    expect(result).toEqual({
      status: "conflict",
      nextOffset: "",
      contentType: "",
      conflictReason: "config-mismatch",
    });
    expect(stub.createCalls).toHaveLength(0);
    expect(log.newRecord).toHaveLength(0);
  });

  it("returns idempotent exists with current offset/contentType/closed when config matches", async () => {
    const stub = makeStub([
      makeRecord({
        currentOffset: "00000000000000000007",
        config: { contentType: "application/json", createdAt: 0 },
        lifecycle: { childRefCount: 0, closed: true },
      }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", {
      contentType: "application/json",
      closed: true,
    });

    expect(result).toEqual({
      status: "exists",
      nextOffset: "00000000000000000007",
      contentType: "application/json",
      closed: true,
    });
    expect(stub.createCalls).toHaveLength(0);
    expect(log.newRecord).toHaveLength(0);
  });

  it("reports closed: false on idempotent exists when the existing record is open", async () => {
    const stub = makeStub([
      makeRecord({
        currentOffset: "00000000000000000004",
        config: { contentType: CONTENT_TYPE, createdAt: 0 },
      }),
    ]);
    const { mutators } = makeMutators();
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", { contentType: CONTENT_TYPE });

    expect(result).toEqual({
      status: "exists",
      nextOffset: "00000000000000000004",
      contentType: CONTENT_TYPE,
      closed: false,
    });
  });
});

describe("CreateStreamService.execute - fork delegation", () => {
  it("delegates to createFork when no existing record and forkedFrom is set", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators({
      forkResult: {
        status: "created",
        nextOffset: "00000000000000000005",
        contentType: CONTENT_TYPE,
      },
    });
    const service = new CreateStreamService(stub.store, mutators);

    const opts: CreateOptions = { forkedFrom: "src", forkOffset: "00000000000000000005" };
    const result = await service.execute("stream-1", opts);

    expect(result).toEqual({
      status: "created",
      nextOffset: "00000000000000000005",
      contentType: CONTENT_TYPE,
    });
    expect(log.createFork).toEqual([{ streamId: "stream-1", options: opts }]);
    expect(stub.createCalls).toHaveLength(0);
    expect(log.newRecord).toHaveLength(0);
    expect(log.scheduleExpiry).toHaveLength(0);
    expect(log.appendMessages).toHaveLength(0);
    expect(log.closeRecord).toHaveLength(0);
  });

  it("does not delegate to createFork when an existing record is found, even if forkedFrom is set", async () => {
    const stub = makeStub([
      makeRecord({
        currentOffset: "00000000000000000002",
        config: { contentType: CONTENT_TYPE, createdAt: 0 },
        lifecycle: { childRefCount: 0, forkedFrom: "src", forkOffset: "00000000000000000000" },
      }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", {
      forkedFrom: "src",
      forkOffset: "00000000000000000000",
    });

    expect(result).toEqual({
      status: "exists",
      nextOffset: "00000000000000000002",
      contentType: CONTENT_TYPE,
      closed: false,
    });
    expect(log.createFork).toHaveLength(0);
  });
});

describe("CreateStreamService.execute - non-fork creation", () => {
  it("defaults contentType to application/octet-stream and persists a record", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators();
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", {});

    expect(result).toEqual({
      status: "created",
      nextOffset: "00000000000000000000",
      contentType: CONTENT_TYPE,
      closed: false,
    });
    expect(log.newRecord).toEqual([
      { streamId: "stream-1", contentType: CONTENT_TYPE, options: {} },
    ]);
    expect(stub.createCalls).toHaveLength(1);
    expect(stub.createCalls[0]!.record.id).toBe("stream-1");
    expect(stub.createCalls[0]!.record.config.contentType).toBe(CONTENT_TYPE);
    expect(log.appendMessages).toHaveLength(0);
    expect(log.closeRecord).toHaveLength(0);
  });

  it("uses the provided contentType when set", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators();
    const service = new CreateStreamService(stub.store, mutators);

    await service.execute("stream-1", { contentType: "text/plain" });

    expect(log.newRecord[0]!.contentType).toBe("text/plain");
    expect(stub.createCalls[0]!.record.config.contentType).toBe("text/plain");
  });

  it("schedules expiry after the record is created", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators();
    const service = new CreateStreamService(stub.store, mutators);

    await service.execute("stream-1", { ttlSeconds: 60 });

    expect(stub.createCalls).toHaveLength(1);
    expect(log.scheduleExpiry).toHaveLength(1);
    expect(log.scheduleExpiry[0]!.id).toBe("stream-1");
    expect(log.scheduleExpiry[0]!.lifecycle.expiresAtMs).toBe(60_000);
  });

  it("schedules expiry even when the record has no expiresAtMs (delegated no-op)", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators();
    const service = new CreateStreamService(stub.store, mutators);

    await service.execute("stream-1", {});

    expect(log.scheduleExpiry).toHaveLength(1);
    expect(log.scheduleExpiry[0]!.lifecycle.expiresAtMs).toBeUndefined();
  });

  it("frames initialData and forwards messages to appendMessages", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators({ appendNextOffset: "00000000000000000002" });
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", {
      contentType: "application/json",
      initialData: bytes('[{"a":1},{"b":2}]'),
    });

    expect(result).toEqual({
      status: "created",
      nextOffset: "00000000000000000002",
      contentType: "application/json",
      closed: false,
    });
    expect(log.appendMessages).toHaveLength(1);
    expect(log.appendMessages[0]!.streamId).toBe("stream-1");
    expect(log.appendMessages[0]!.data).toHaveLength(2);
    expect(log.closeRecord).toHaveLength(0);
  });

  it("passes through a single non-JSON initialData payload to appendMessages", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators({ appendNextOffset: "00000000000000000001" });
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", {
      contentType: CONTENT_TYPE,
      initialData: bytes("hello"),
    });

    expect(result.nextOffset).toBe("00000000000000000001");
    expect(log.appendMessages).toHaveLength(1);
    expect(log.appendMessages[0]!.data).toHaveLength(1);
    expect(new TextDecoder().decode(log.appendMessages[0]!.data[0]!)).toBe("hello");
  });

  it("does not call appendMessages for an empty JSON array initialData", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators();
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", {
      contentType: "application/json",
      initialData: bytes("[]"),
    });

    expect(result.status).toBe("created");
    expect(log.appendMessages).toHaveLength(0);
  });

  it("invokes closeRecord with empty data after creation when closed: true", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators({ closeNextOffset: "00000000000000000000" });
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", { closed: true });

    expect(result).toEqual({
      status: "created",
      nextOffset: "00000000000000000000",
      contentType: CONTENT_TYPE,
      closed: true,
    });
    expect(stub.createCalls).toHaveLength(1);
    expect(log.closeRecord).toHaveLength(1);
    expect(log.closeRecord[0]!.streamId).toBe("stream-1");
    expect(log.closeRecord[0]!.data).toEqual([]);
  });

  it("returns adapter exists result without scheduling expiry or appending initial messages", async () => {
    const existing = makeRecord({
      currentOffset: "00000000000000000005",
      config: { contentType: CONTENT_TYPE, createdAt: 0 },
    });
    const stub = makeStub();
    const baseStore = stub.store;
    const store: StreamStoreAdapter = {
      ...baseStore,
      async get() {
        return null;
      },
      async create(record) {
        stub.createCalls.push({ record });
        return { status: "exists", record: existing };
      },
    };
    const { mutators, log } = makeMutators();
    const service = new CreateStreamService(store, mutators);

    const result = await service.execute("stream-1", {
      contentType: CONTENT_TYPE,
      initialData: bytes("should-not-append"),
    });

    expect(result).toEqual({
      status: "exists",
      nextOffset: "00000000000000000005",
      contentType: CONTENT_TYPE,
      closed: false,
    });
    expect(stub.createCalls).toHaveLength(1);
    expect(log.scheduleExpiry).toHaveLength(0);
    expect(log.appendMessages).toHaveLength(0);
    expect(log.closeRecord).toHaveLength(0);
  });

  it("reloads latest record before closing on closed-on-create", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators({
      appendNextOffset: "00000000000000000004",
      closeNextOffset: "00000000000000000004",
    });
    const service = new CreateStreamService(stub.store, mutators);

    const result = await service.execute("stream-1", {
      contentType: CONTENT_TYPE,
      initialData: bytes("payload"),
      closed: true,
    });

    expect(result).toEqual({
      status: "created",
      nextOffset: "00000000000000000004",
      contentType: CONTENT_TYPE,
      closed: true,
    });
    expect(log.appendMessages).toHaveLength(1);
    expect(log.closeRecord).toHaveLength(1);
    expect(log.closeRecord[0]!.data).toEqual([]);
    // closeRecord receives the reloaded record (the one persisted by store.create)
    expect(log.closeRecord[0]!.record.id).toBe("stream-1");
  });
});
