/**
 * Focused unit coverage for StreamGcService extracted from
 * StreamProtocol.delete / handleScheduledExpiry / purgeWithCascade.
 *
 * The full lifecycle/GC surface is exercised via the conformance suite.
 * These cases pin the bounded extraction's contract:
 *
 *   delete:
 *     - missing -> not-found
 *     - soft-deleted -> gone
 *     - childRefCount > 0 -> soft-delete + notify "soft-deleted"
 *     - unreferenced -> purgeWithCascade and ok
 *
 *   handleScheduledExpiry:
 *     - missing -> no-op
 *     - !isExpired(record) -> no-op (stale-timer guard)
 *     - childRefCount > 0 -> soft-delete + notify "soft-deleted"
 *     - unreferenced -> purgeWithCascade
 *
 *   purgeWithCascade:
 *     - cancelExpiry -> deleteMessages -> deleteProducerStates
 *       -> store.delete -> notify "deleted" order
 *     - fork child decrements parent refcount
 *     - soft-deleted parent recursively purges when refcount reaches zero
 *     - non-soft-deleted parent is not recursively purged
 */

import { describe, expect, it } from "vitest";
import {
  StreamGcService,
  type StreamGcServiceMutators,
} from "../../protocol/helpers/stream-gc-service.ts";
import type {
  StreamEventType,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
  StreamStoreAdapter,
} from "../../types/storage.ts";

const STREAM_ID = "s";
const PARENT_ID = "p";
const GRANDPARENT_ID = "gp";

interface Event {
  kind:
    | "get"
    | "update"
    | "cancelExpiry"
    | "deleteMessages"
    | "deleteProducerStates"
    | "delete"
    | "notify"
    | "decrementChildRefCount";
  detail?: unknown;
}

interface Stub {
  store: StreamStoreAdapter;
  records: Map<StreamId, StreamRecord>;
  events: Event[];
  refCounts: Map<StreamId, number>;
}

function notImplemented(method: string): never {
  throw new Error(`store.${method} not implemented in test stub`);
}

function makeRecord(
  id: StreamId,
  overrides: { lifecycle?: Partial<StreamRecord["lifecycle"]> } = {},
): StreamRecord {
  return {
    id,
    currentOffset: "",
    counter: 0,
    config: { contentType: "application/octet-stream", createdAt: 0 },
    lifecycle: { childRefCount: 0, ...overrides.lifecycle },
  };
}

function makeStub(initial: StreamRecord[] = []): Stub {
  const records = new Map<StreamId, StreamRecord>();
  for (const r of initial) records.set(r.id, r);
  const events: Event[] = [];
  const refCounts = new Map<StreamId, number>();
  for (const r of initial) refCounts.set(r.id, r.lifecycle.childRefCount);

  const store: StreamStoreAdapter = {
    async get(streamId) {
      events.push({ kind: "get", detail: streamId });
      return records.get(streamId) ?? null;
    },
    async update(streamId, patch: StreamRecordPatch) {
      events.push({ kind: "update", detail: { streamId, patch } });
      const existing = records.get(streamId);
      if (!existing) throw new Error(`record missing in stub: ${streamId}`);
      const merged: StreamRecord = {
        ...existing,
        ...patch,
        config: { ...existing.config, ...patch.config },
        lifecycle: { ...existing.lifecycle, ...patch.lifecycle },
      };
      records.set(streamId, merged);
      return merged;
    },
    async delete(streamId) {
      events.push({ kind: "delete", detail: streamId });
      records.delete(streamId);
    },
    async deleteMessages(streamId) {
      events.push({ kind: "deleteMessages", detail: streamId });
    },
    async deleteProducerStates(streamId) {
      events.push({ kind: "deleteProducerStates", detail: streamId });
    },
    async cancelExpiry(streamId) {
      events.push({ kind: "cancelExpiry", detail: streamId });
    },
    async notify(streamId, type: StreamEventType) {
      events.push({ kind: "notify", detail: { streamId, type } });
    },
    async incrementChildRefCount(parentId) {
      const next = (refCounts.get(parentId) ?? 0) + 1;
      refCounts.set(parentId, next);
      return next;
    },
    async decrementChildRefCount(parentId) {
      const next = (refCounts.get(parentId) ?? 0) - 1;
      refCounts.set(parentId, next);
      events.push({ kind: "decrementChildRefCount", detail: { parentId, next } });
      const parent = records.get(parentId);
      if (parent) {
        records.set(parentId, {
          ...parent,
          lifecycle: { ...parent.lifecycle, childRefCount: next },
        });
      }
      return next;
    },
    create: () => notImplemented("create"),
    append: () => notImplemented("append"),
    list: () => notImplemented("list"),
    getProducerState: () => notImplemented("getProducerState"),
    setProducerState: () => notImplemented("setProducerState"),
  };
  return { store, records, events, refCounts };
}

function makeMutators(isExpired: (record: StreamRecord) => boolean = () => true): {
  mutators: StreamGcServiceMutators;
  isExpiredCalls: StreamRecord[];
} {
  const isExpiredCalls: StreamRecord[] = [];
  return {
    isExpiredCalls,
    mutators: {
      isExpired(record) {
        isExpiredCalls.push(record);
        return isExpired(record);
      },
    },
  };
}

describe("StreamGcService.delete", () => {
  it("returns not-found when the stream does not exist", async () => {
    const stub = makeStub();
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    const result = await service.delete(STREAM_ID);

    expect(result).toEqual({ status: "not-found" });
    expect(stub.events.map((e) => e.kind)).toEqual(["get"]);
  });

  it("returns gone when the stream is soft-deleted", async () => {
    const stub = makeStub([
      makeRecord(STREAM_ID, { lifecycle: { childRefCount: 0, softDeleted: true } }),
    ]);
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    const result = await service.delete(STREAM_ID);

    expect(result).toEqual({ status: "gone" });
    expect(stub.events.map((e) => e.kind)).toEqual(["get"]);
  });

  it("soft-deletes and notifies when childRefCount > 0, returning ok without purging", async () => {
    const stub = makeStub([makeRecord(STREAM_ID, { lifecycle: { childRefCount: 2 } })]);
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    const result = await service.delete(STREAM_ID);

    expect(result).toEqual({ status: "ok" });
    expect(stub.events.map((e) => e.kind)).toEqual(["get", "update", "notify"]);
    const updateEvent = stub.events.find((e) => e.kind === "update");
    expect(updateEvent?.detail).toEqual({
      streamId: STREAM_ID,
      patch: { lifecycle: { softDeleted: true } },
    });
    const notifyEvent = stub.events.find((e) => e.kind === "notify");
    expect(notifyEvent?.detail).toEqual({ streamId: STREAM_ID, type: "soft-deleted" });
  });

  it("purges and returns ok when there are no child refs", async () => {
    const stub = makeStub([makeRecord(STREAM_ID)]);
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    const result = await service.delete(STREAM_ID);

    expect(result).toEqual({ status: "ok" });
    expect(stub.events.map((e) => e.kind)).toEqual([
      "get",
      "cancelExpiry",
      "deleteMessages",
      "deleteProducerStates",
      "delete",
      "notify",
    ]);
    const notify = stub.events.find((e) => e.kind === "notify");
    expect(notify?.detail).toEqual({ streamId: STREAM_ID, type: "deleted" });
  });

  it("does not call isExpired during explicit delete", async () => {
    const stub = makeStub([makeRecord(STREAM_ID)]);
    const { mutators, isExpiredCalls } = makeMutators(() => false);
    const service = new StreamGcService(stub.store, mutators);

    await service.delete(STREAM_ID);

    expect(isExpiredCalls).toHaveLength(0);
  });
});

describe("StreamGcService.handleScheduledExpiry", () => {
  it("is a no-op when the stream does not exist", async () => {
    const stub = makeStub();
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    await service.handleScheduledExpiry(STREAM_ID);

    expect(stub.events.map((e) => e.kind)).toEqual(["get"]);
  });

  it("is a no-op when isExpired(record) returns false (stale timer guard)", async () => {
    const stub = makeStub([makeRecord(STREAM_ID)]);
    const { mutators, isExpiredCalls } = makeMutators(() => false);
    const service = new StreamGcService(stub.store, mutators);

    await service.handleScheduledExpiry(STREAM_ID);

    expect(isExpiredCalls).toHaveLength(1);
    expect(isExpiredCalls[0]!.id).toBe(STREAM_ID);
    expect(stub.events.map((e) => e.kind)).toEqual(["get"]);
  });

  it("soft-deletes and notifies when childRefCount > 0", async () => {
    const stub = makeStub([makeRecord(STREAM_ID, { lifecycle: { childRefCount: 1 } })]);
    const { mutators } = makeMutators(() => true);
    const service = new StreamGcService(stub.store, mutators);

    await service.handleScheduledExpiry(STREAM_ID);

    expect(stub.events.map((e) => e.kind)).toEqual(["get", "update", "notify"]);
    const update = stub.events.find((e) => e.kind === "update");
    expect(update?.detail).toEqual({
      streamId: STREAM_ID,
      patch: { lifecycle: { softDeleted: true } },
    });
    const notify = stub.events.find((e) => e.kind === "notify");
    expect(notify?.detail).toEqual({ streamId: STREAM_ID, type: "soft-deleted" });
  });

  it("purges when isExpired and there are no child refs", async () => {
    const stub = makeStub([makeRecord(STREAM_ID)]);
    const { mutators } = makeMutators(() => true);
    const service = new StreamGcService(stub.store, mutators);

    await service.handleScheduledExpiry(STREAM_ID);

    expect(stub.events.map((e) => e.kind)).toEqual([
      "get",
      "cancelExpiry",
      "deleteMessages",
      "deleteProducerStates",
      "delete",
      "notify",
    ]);
    const notify = stub.events.find((e) => e.kind === "notify");
    expect(notify?.detail).toEqual({ streamId: STREAM_ID, type: "deleted" });
  });
});

describe("StreamGcService purge cascade", () => {
  it("preserves cancel/deleteMessages/deleteProducerStates/delete/notify ordering", async () => {
    const stub = makeStub([makeRecord(STREAM_ID)]);
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    await service.delete(STREAM_ID);

    const cascadeKinds = stub.events.map((e) => e.kind).filter((k) => k !== "get");
    expect(cascadeKinds).toEqual([
      "cancelExpiry",
      "deleteMessages",
      "deleteProducerStates",
      "delete",
      "notify",
    ]);
  });

  it("decrements parent refcount when purging a fork child", async () => {
    const parent = makeRecord(PARENT_ID, { lifecycle: { childRefCount: 1 } });
    const child = makeRecord(STREAM_ID, {
      lifecycle: { childRefCount: 0, forkedFrom: PARENT_ID, forkOffset: "" },
    });
    const stub = makeStub([parent, child]);
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    await service.delete(STREAM_ID);

    const dec = stub.events.find((e) => e.kind === "decrementChildRefCount");
    expect(dec).toBeDefined();
    expect(dec?.detail).toEqual({ parentId: PARENT_ID, next: 0 });
  });

  it("recursively purges a soft-deleted parent when its refcount reaches zero", async () => {
    const parent = makeRecord(PARENT_ID, {
      lifecycle: { childRefCount: 1, softDeleted: true },
    });
    const child = makeRecord(STREAM_ID, {
      lifecycle: { childRefCount: 0, forkedFrom: PARENT_ID, forkOffset: "" },
    });
    const stub = makeStub([parent, child]);
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    await service.delete(STREAM_ID);

    const deletes = stub.events.filter((e) => e.kind === "delete").map((e) => e.detail);
    expect(deletes).toEqual([STREAM_ID, PARENT_ID]);
    const notifies = stub.events.filter((e) => e.kind === "notify").map((e) => e.detail);
    expect(notifies).toEqual([
      { streamId: STREAM_ID, type: "deleted" },
      { streamId: PARENT_ID, type: "deleted" },
    ]);
  });

  it("does not recursively purge a parent that is not soft-deleted even when refcount reaches zero", async () => {
    const parent = makeRecord(PARENT_ID, {
      lifecycle: { childRefCount: 1, softDeleted: false },
    });
    const child = makeRecord(STREAM_ID, {
      lifecycle: { childRefCount: 0, forkedFrom: PARENT_ID, forkOffset: "" },
    });
    const stub = makeStub([parent, child]);
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    await service.delete(STREAM_ID);

    const deletes = stub.events.filter((e) => e.kind === "delete").map((e) => e.detail);
    expect(deletes).toEqual([STREAM_ID]);
    expect(stub.records.has(PARENT_ID)).toBe(true);
  });

  it("does not recursively purge a soft-deleted parent when other forks still reference it", async () => {
    const parent = makeRecord(PARENT_ID, {
      lifecycle: { childRefCount: 2, softDeleted: true },
    });
    const child = makeRecord(STREAM_ID, {
      lifecycle: { childRefCount: 0, forkedFrom: PARENT_ID, forkOffset: "" },
    });
    const stub = makeStub([parent, child]);
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    await service.delete(STREAM_ID);

    const deletes = stub.events.filter((e) => e.kind === "delete").map((e) => e.detail);
    expect(deletes).toEqual([STREAM_ID]);
    const dec = stub.events.find((e) => e.kind === "decrementChildRefCount");
    expect(dec?.detail).toEqual({ parentId: PARENT_ID, next: 1 });
    expect(stub.records.has(PARENT_ID)).toBe(true);
  });

  it("recursively purges multiple soft-deleted ancestors when both reach zero", async () => {
    const grandparent = makeRecord(GRANDPARENT_ID, {
      lifecycle: { childRefCount: 1, softDeleted: true },
    });
    const parent = makeRecord(PARENT_ID, {
      lifecycle: {
        childRefCount: 1,
        softDeleted: true,
        forkedFrom: GRANDPARENT_ID,
        forkOffset: "",
      },
    });
    const child = makeRecord(STREAM_ID, {
      lifecycle: { childRefCount: 0, forkedFrom: PARENT_ID, forkOffset: "" },
    });
    const stub = makeStub([grandparent, parent, child]);
    const { mutators } = makeMutators();
    const service = new StreamGcService(stub.store, mutators);

    await service.delete(STREAM_ID);

    const deletes = stub.events.filter((e) => e.kind === "delete").map((e) => e.detail);
    expect(deletes).toEqual([STREAM_ID, PARENT_ID, GRANDPARENT_ID]);
  });
});
