/**
 * Focused unit coverage for ForkService extracted from StreamProtocol.createFork.
 *
 * The full fork surface is covered by the conformance suite. These cases
 * pin the orchestration order of the extracted service:
 *
 *   source lazy expiry -> source load -> source missing/soft-deleted shaping
 *     -> fork offset defaulting/validation
 *     -> content-type inheritance/compatibility
 *     -> resolveForkExpiry precedence
 *     -> newRecord(... fork descriptor)
 *     -> inTransaction(create + incrementChildRefCount)
 *     -> scheduleExpiry
 *     -> initialData framing + appendMessages
 *     -> CreateResult { status: "created" } with no closed field
 */

import { describe, expect, it } from "vitest";
import {
  ForkService,
  resolveForkExpiry,
  type ForkServiceMutators,
} from "../../protocol/helpers/fork-service.ts";
import type { CreateOptions } from "../../types/protocol.ts";
import type { StreamId, StreamRecord, StreamStoreAdapter } from "../../types/storage.ts";
import { ZERO_OFFSET } from "../../protocol/helpers/offset-generator.ts";

const SOURCE_ID = "src";
const FORK_ID = "fork";
const CONTENT_TYPE = "application/octet-stream";
const SOURCE_TAIL = "0000000000000010_0000000000000000";
const MID_OFFSET = "0000000000000005_0000000000000000";
const PAST_TAIL = "0000000000000099_0000000000000000";

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function notImplemented(method: string): never {
  throw new Error(`store.${method} not implemented in test stub`);
}

interface TxCall {
  kind: "create" | "incrementChildRefCount";
  detail: unknown;
  insideTransaction: boolean;
}

interface Stub {
  store: StreamStoreAdapter;
  records: Map<StreamId, StreamRecord>;
  txCalls: TxCall[];
  transactionRuns: number;
}

function makeStub(initialRecords: StreamRecord[] = []): Stub {
  const records = new Map<StreamId, StreamRecord>();
  for (const r of initialRecords) records.set(r.id, r);
  const txCalls: TxCall[] = [];
  let inTransaction = false;
  let transactionRuns = 0;

  const store: StreamStoreAdapter = {
    async get(streamId) {
      return records.get(streamId) ?? null;
    },
    async create(record) {
      txCalls.push({ kind: "create", detail: record, insideTransaction: inTransaction });
      const existing = records.get(record.id);
      if (existing) return { status: "exists", record: existing };
      records.set(record.id, record);
      return { status: "created" };
    },
    async incrementChildRefCount(parentId) {
      const parent = records.get(parentId);
      const next = (parent?.lifecycle.childRefCount ?? 0) + 1;
      txCalls.push({
        kind: "incrementChildRefCount",
        detail: parentId,
        insideTransaction: inTransaction,
      });
      return next;
    },
    async transaction(fn) {
      transactionRuns += 1;
      inTransaction = true;
      try {
        return await fn(store);
      } finally {
        inTransaction = false;
      }
    },
    update: () => notImplemented("update"),
    delete: () => notImplemented("delete"),
    append: () => notImplemented("append"),
    list: () => notImplemented("list"),
    deleteMessages: () => notImplemented("deleteMessages"),
    getProducerState: () => notImplemented("getProducerState"),
    setProducerState: () => notImplemented("setProducerState"),
    deleteProducerStates: () => notImplemented("deleteProducerStates"),
    decrementChildRefCount: () => notImplemented("decrementChildRefCount"),
  };
  return {
    store,
    records,
    txCalls,
    get transactionRuns() {
      return transactionRuns;
    },
  } as Stub;
}

function makeSource(overrides: Partial<StreamRecord> = {}): StreamRecord {
  const { config, lifecycle, ...rest } = overrides;
  return {
    id: SOURCE_ID,
    currentOffset: SOURCE_TAIL,
    counter: 16,
    ...rest,
    config: { contentType: CONTENT_TYPE, createdAt: 0, ...config },
    lifecycle: { childRefCount: 0, ...lifecycle },
  };
}

interface MutatorLog {
  expireIfNeeded: StreamId[];
  newRecord: Array<{
    streamId: StreamId;
    contentType: string;
    options: CreateOptions;
    fork: { forkedFrom: string; forkOffset: string };
  }>;
  scheduleExpiry: StreamRecord[];
  appendMessages: Array<{ streamId: StreamId; record: StreamRecord; data: Uint8Array[] }>;
}

interface OrderEvent {
  kind: "expireIfNeeded" | "store.get" | "newRecord" | "scheduleExpiry" | "appendMessages";
  detail?: unknown;
}

function makeMutators(
  options: {
    appendNextOffset?: string;
    overrideRecord?: (defaults: StreamRecord) => StreamRecord;
    order?: OrderEvent[];
    onExpire?: (streamId: StreamId) => void;
  } = {},
): { mutators: ForkServiceMutators; log: MutatorLog } {
  const log: MutatorLog = {
    expireIfNeeded: [],
    newRecord: [],
    scheduleExpiry: [],
    appendMessages: [],
  };
  const mutators: ForkServiceMutators = {
    async expireIfNeeded(streamId) {
      log.expireIfNeeded.push(streamId);
      options.order?.push({ kind: "expireIfNeeded", detail: streamId });
      options.onExpire?.(streamId);
    },
    newRecord(streamId, contentType, opts, fork) {
      log.newRecord.push({ streamId, contentType, options: opts, fork });
      options.order?.push({ kind: "newRecord" });
      const base: StreamRecord = {
        id: streamId,
        currentOffset: fork.forkOffset,
        counter: 0,
        config: {
          contentType,
          ttlSeconds: opts.ttlSeconds,
          expiresAt: opts.expiresAt,
          createdAt: 0,
        },
        lifecycle: {
          childRefCount: 0,
          forkedFrom: fork.forkedFrom,
          forkOffset: fork.forkOffset,
          expiresAtMs: opts.ttlSeconds !== undefined ? 60_000 : undefined,
        },
      };
      return options.overrideRecord ? options.overrideRecord(base) : base;
    },
    async scheduleExpiry(record) {
      log.scheduleExpiry.push(record);
      options.order?.push({ kind: "scheduleExpiry" });
    },
    async appendMessages(streamId, record, data) {
      log.appendMessages.push({ streamId, record, data });
      options.order?.push({ kind: "appendMessages" });
      return options.appendNextOffset ?? record.currentOffset;
    },
  };
  return { mutators, log };
}

describe("ForkService.execute - source gates", () => {
  it("returns not-found with errorMessage when the source is missing", async () => {
    const stub = makeStub();
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });

    expect(result).toEqual({
      status: "not-found",
      nextOffset: "",
      contentType: "",
      errorMessage: `Source stream not found: ${SOURCE_ID}`,
    });
    expect(stub.txCalls).toHaveLength(0);
    expect(log.newRecord).toHaveLength(0);
    expect(log.scheduleExpiry).toHaveLength(0);
    expect(log.appendMessages).toHaveLength(0);
  });

  it("returns conflict/fork-source-soft-deleted when the source is soft-deleted", async () => {
    const stub = makeStub([makeSource({ lifecycle: { childRefCount: 0, softDeleted: true } })]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });

    expect(result).toEqual({
      status: "conflict",
      nextOffset: "",
      contentType: "",
      conflictReason: "fork-source-soft-deleted",
      errorMessage: `Source stream is soft-deleted: ${SOURCE_ID}`,
    });
    expect(stub.txCalls).toHaveLength(0);
    expect(log.newRecord).toHaveLength(0);
  });

  it("invokes expireIfNeeded(sourcePath) before loading the source", async () => {
    const events: string[] = [];
    const records = new Map<StreamId, StreamRecord>();
    records.set(SOURCE_ID, makeSource());
    const store: StreamStoreAdapter = {
      async get(streamId) {
        events.push(`get:${streamId}`);
        return records.get(streamId) ?? null;
      },
      async create() {
        events.push("create");
        return { status: "created" };
      },
      async incrementChildRefCount() {
        events.push("incrementChildRefCount");
        return 1;
      },
      async transaction(fn) {
        events.push("transaction");
        return fn(store);
      },
      update: () => notImplemented("update"),
      delete: () => notImplemented("delete"),
      append: () => notImplemented("append"),
      list: () => notImplemented("list"),
      deleteMessages: () => notImplemented("deleteMessages"),
      getProducerState: () => notImplemented("getProducerState"),
      setProducerState: () => notImplemented("setProducerState"),
      deleteProducerStates: () => notImplemented("deleteProducerStates"),
      decrementChildRefCount: () => notImplemented("decrementChildRefCount"),
    };
    const { mutators } = makeMutators({
      onExpire: (id) => events.push(`expireIfNeeded:${id}`),
    });
    const service = new ForkService(store, mutators);

    await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });

    expect(events.indexOf(`expireIfNeeded:${SOURCE_ID}`)).toBeLessThan(
      events.indexOf(`get:${SOURCE_ID}`),
    );
    expect(events.indexOf(`expireIfNeeded:${SOURCE_ID}`)).toBeGreaterThanOrEqual(0);
    expect(events.indexOf(`get:${SOURCE_ID}`)).toBeGreaterThanOrEqual(0);
  });
});

describe("ForkService.execute - fork offset", () => {
  it("defaults forkOffset to source.currentOffset when omitted", async () => {
    const stub = makeStub([makeSource()]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });

    expect(result.status).toBe("created");
    expect(log.newRecord).toHaveLength(1);
    expect(log.newRecord[0]!.fork).toEqual({ forkedFrom: SOURCE_ID, forkOffset: SOURCE_TAIL });
  });

  it("returns bad-request with invalid format errorMessage when forkOffset is malformed", async () => {
    const stub = makeStub([makeSource()]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      forkOffset: "not-an-offset",
    });

    expect(result).toEqual({
      status: "bad-request",
      nextOffset: "",
      contentType: "",
      errorMessage: "Invalid Stream-Fork-Offset format",
    });
    expect(stub.txCalls).toHaveLength(0);
    expect(log.newRecord).toHaveLength(0);
  });

  it("returns bad-request when forkOffset exceeds source tail", async () => {
    const stub = makeStub([makeSource()]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      forkOffset: PAST_TAIL,
    });

    expect(result).toEqual({
      status: "bad-request",
      nextOffset: "",
      contentType: "",
      errorMessage: "Stream-Fork-Offset exceeds source tail",
    });
    expect(stub.txCalls).toHaveLength(0);
    expect(log.newRecord).toHaveLength(0);
  });

  it("accepts ZERO_OFFSET as a valid fork offset at the lower bound", async () => {
    const stub = makeStub([makeSource()]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      forkOffset: ZERO_OFFSET,
    });

    expect(result.status).toBe("created");
    expect(log.newRecord[0]!.fork.forkOffset).toBe(ZERO_OFFSET);
  });
});

describe("ForkService.execute - content type", () => {
  it("inherits source contentType when options.contentType is omitted", async () => {
    const stub = makeStub([
      makeSource({ config: { contentType: "application/json", createdAt: 0 } }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });

    expect(result.contentType).toBe("application/json");
    expect(log.newRecord[0]!.contentType).toBe("application/json");
  });

  it("inherits source contentType when options.contentType is blank", async () => {
    const stub = makeStub([makeSource({ config: { contentType: "text/plain", createdAt: 0 } })]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      contentType: "   ",
    });

    expect(result.contentType).toBe("text/plain");
    expect(log.newRecord[0]!.contentType).toBe("text/plain");
  });

  it("returns conflict/fork-content-type when options.contentType does not match the source", async () => {
    const stub = makeStub([
      makeSource({ config: { contentType: "application/json", createdAt: 0 } }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      contentType: "text/plain",
    });

    expect(result).toEqual({
      status: "conflict",
      nextOffset: "",
      contentType: "",
      conflictReason: "fork-content-type",
      errorMessage: "Fork Content-Type does not match source",
    });
    expect(stub.txCalls).toHaveLength(0);
    expect(log.newRecord).toHaveLength(0);
  });

  it("accepts a compatible explicit contentType (parameter-stripped equality)", async () => {
    const stub = makeStub([
      makeSource({ config: { contentType: "application/json", createdAt: 0 } }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      contentType: "application/json; charset=utf-8",
    });

    expect(result.status).toBe("created");
    expect(result.contentType).toBe("application/json; charset=utf-8");
    expect(log.newRecord[0]!.contentType).toBe("application/json; charset=utf-8");
  });
});

describe("ForkService.execute - expiry resolution", () => {
  it("uses explicit ttlSeconds when set on the request", async () => {
    const stub = makeStub([
      makeSource({ config: { contentType: CONTENT_TYPE, createdAt: 0, ttlSeconds: 60 } }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    await service.execute(FORK_ID, { forkedFrom: SOURCE_ID, ttlSeconds: 30 });

    expect(log.newRecord[0]!.options.ttlSeconds).toBe(30);
    expect(log.newRecord[0]!.options.expiresAt).toBeUndefined();
  });

  it("uses explicit expiresAt when set on the request and ttlSeconds is absent", async () => {
    const stub = makeStub([
      makeSource({ config: { contentType: CONTENT_TYPE, createdAt: 0, ttlSeconds: 60 } }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      expiresAt: "2030-01-01T00:00:00Z",
    });

    expect(log.newRecord[0]!.options.ttlSeconds).toBeUndefined();
    expect(log.newRecord[0]!.options.expiresAt).toBe("2030-01-01T00:00:00Z");
  });

  it("inherits source ttlSeconds when neither ttlSeconds nor expiresAt is set", async () => {
    const stub = makeStub([
      makeSource({ config: { contentType: CONTENT_TYPE, createdAt: 0, ttlSeconds: 120 } }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });

    expect(log.newRecord[0]!.options.ttlSeconds).toBe(120);
    expect(log.newRecord[0]!.options.expiresAt).toBeUndefined();
  });

  it("inherits source expiresAt when neither is set on the request and source has expiresAt only", async () => {
    const stub = makeStub([
      makeSource({
        config: { contentType: CONTENT_TYPE, createdAt: 0, expiresAt: "2031-01-01T00:00:00Z" },
      }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });

    expect(log.newRecord[0]!.options.ttlSeconds).toBeUndefined();
    expect(log.newRecord[0]!.options.expiresAt).toBe("2031-01-01T00:00:00Z");
  });

  it("returns no expiry fields when neither request nor source has any", async () => {
    const stub = makeStub([makeSource()]);
    const { mutators } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    expect(resolveForkExpiry({ forkedFrom: SOURCE_ID }, makeSource())).toEqual({});

    await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });
  });
});

describe("ForkService.execute - persistence and scheduling", () => {
  it("creates the record and increments the parent ref count inside a transaction", async () => {
    const stub = makeStub([makeSource()]);
    const { mutators } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    await service.execute(FORK_ID, { forkedFrom: SOURCE_ID, forkOffset: MID_OFFSET });

    expect(stub.transactionRuns).toBe(1);
    expect(stub.txCalls).toHaveLength(2);
    expect(stub.txCalls[0]!.kind).toBe("create");
    expect(stub.txCalls[0]!.insideTransaction).toBe(true);
    expect(stub.txCalls[1]!.kind).toBe("incrementChildRefCount");
    expect(stub.txCalls[1]!.insideTransaction).toBe(true);
    expect(stub.txCalls[1]!.detail).toBe(SOURCE_ID);
  });

  it("does not increment parent or schedule expiry when adapter create reports existing target", async () => {
    const existingTarget = makeSource({ id: FORK_ID });
    const stub = makeStub([makeSource(), existingTarget]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });

    expect(result).toEqual({
      status: "conflict",
      nextOffset: "",
      contentType: "",
      conflictReason: "config-mismatch",
      errorMessage: `Stream already exists: ${FORK_ID}`,
    });
    expect(stub.txCalls.map((call) => call.kind)).toEqual(["create"]);
    expect(log.scheduleExpiry).toHaveLength(0);
    expect(log.appendMessages).toHaveLength(0);
  });

  it("schedules expiry for the newly created fork record", async () => {
    const stub = makeStub([makeSource()]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    await service.execute(FORK_ID, { forkedFrom: SOURCE_ID, ttlSeconds: 60 });

    expect(log.scheduleExpiry).toHaveLength(1);
    expect(log.scheduleExpiry[0]!.id).toBe(FORK_ID);
    expect(log.scheduleExpiry[0]!.lifecycle.expiresAtMs).toBe(60_000);
  });

  it("schedules expiry even when the record has no expiresAtMs (delegated no-op)", async () => {
    const stub = makeStub([makeSource()]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });

    expect(log.scheduleExpiry).toHaveLength(1);
    expect(log.scheduleExpiry[0]!.lifecycle.expiresAtMs).toBeUndefined();
  });

  it("falls back to direct calls when the store has no transaction support", async () => {
    const stub = makeStub([makeSource()]);
    // remove the transaction capability
    (stub.store as { transaction?: unknown }).transaction = undefined;
    const { mutators } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, { forkedFrom: SOURCE_ID });

    expect(result.status).toBe("created");
    expect(stub.txCalls.map((c) => c.kind)).toEqual(["create", "incrementChildRefCount"]);
    expect(stub.txCalls.every((c) => c.insideTransaction === false)).toBe(true);
  });
});

describe("ForkService.execute - initial data and result shape", () => {
  it("frames JSON array initialData into multiple stored messages", async () => {
    const stub = makeStub([
      makeSource({ config: { contentType: "application/json", createdAt: 0 } }),
    ]);
    const { mutators, log } = makeMutators({
      appendNextOffset: "0000000000000012_0000000000000000",
    });
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      initialData: bytes('[{"a":1},{"b":2}]'),
    });

    expect(result).toEqual({
      status: "created",
      nextOffset: "0000000000000012_0000000000000000",
      contentType: "application/json",
    });
    expect(result).not.toHaveProperty("closed");
    expect(log.appendMessages).toHaveLength(1);
    expect(log.appendMessages[0]!.streamId).toBe(FORK_ID);
    expect(log.appendMessages[0]!.data).toHaveLength(2);
  });

  it("does not call appendMessages for an empty JSON array initialData", async () => {
    const stub = makeStub([
      makeSource({ config: { contentType: "application/json", createdAt: 0 } }),
    ]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      initialData: bytes("[]"),
    });

    expect(result.status).toBe("created");
    expect(log.appendMessages).toHaveLength(0);
  });

  it("returns a created result without a closed field even when options.closed is set", async () => {
    const stub = makeStub([makeSource()]);
    const { mutators } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      forkOffset: MID_OFFSET,
      closed: true,
    });

    expect(result.status).toBe("created");
    expect(result.nextOffset).toBe(MID_OFFSET);
    expect(result.contentType).toBe(CONTENT_TYPE);
    expect(result).not.toHaveProperty("closed");
  });

  it("reports nextOffset = record.currentOffset when no initial data is provided", async () => {
    const stub = makeStub([makeSource()]);
    const { mutators, log } = makeMutators();
    const service = new ForkService(stub.store, mutators);

    const result = await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      forkOffset: MID_OFFSET,
    });

    expect(result.nextOffset).toBe(MID_OFFSET);
    expect(log.appendMessages).toHaveLength(0);
  });

  it("preserves orchestration order: expire -> get -> newRecord -> tx (create+incr) -> scheduleExpiry -> appendMessages", async () => {
    const stub = makeStub([
      makeSource({ config: { contentType: "application/json", createdAt: 0 } }),
    ]);
    const order: OrderEvent[] = [];
    const baseStore = stub.store;
    const wrappedStore: StreamStoreAdapter = {
      ...baseStore,
      get: async (id) => {
        order.push({ kind: "store.get", detail: id });
        return baseStore.get(id);
      },
      create: baseStore.create.bind(baseStore),
      incrementChildRefCount: baseStore.incrementChildRefCount.bind(baseStore),
      transaction: (fn) => baseStore.transaction!(fn),
    };
    const { mutators } = makeMutators({
      order,
      appendNextOffset: "0000000000000012_0000000000000000",
    });
    const service = new ForkService(wrappedStore, mutators);

    await service.execute(FORK_ID, {
      forkedFrom: SOURCE_ID,
      initialData: bytes('[{"a":1}]'),
    });

    const kinds = order.map((e) => e.kind);
    expect(kinds).toEqual([
      "expireIfNeeded",
      "store.get",
      "newRecord",
      "scheduleExpiry",
      "appendMessages",
    ]);
  });
});
