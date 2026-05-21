/**
 * Focused unit coverage for AppendService extracted from StreamProtocol.
 *
 * The full append surface is covered by the conformance suite and the
 * concurrent-append regression test. These cases pin the orchestration
 * order of the extracted service:
 *
 *   record load -> producer load -> producer validate -> early reject
 *     -> closed/content-type/seq lifecycle gate -> frame -> mutator
 *     -> persist accepted producer state -> result shape.
 */

import { describe, it, expect } from "vitest";
import { AppendService, appendedResult } from "../protocol/append-service.ts";
import { ProducerIdempotencyService } from "../protocol/helpers/producer-idempotency-service.ts";
import type { ProducerState, StreamRecord, StreamStoreAdapter } from "../types/storage.ts";

const CONTENT_TYPE = "application/octet-stream";

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function notImplemented(method: string): never {
  throw new Error(`store.${method} not implemented in test stub`);
}

interface Stub {
  store: StreamStoreAdapter;
  records: Map<string, StreamRecord>;
  producerStates: Map<string, ProducerState>;
  setProducerCalls: Array<{ streamId: string; producerId: string; state: ProducerState }>;
}

function makeStub(initialRecords: StreamRecord[] = []): Stub {
  const records = new Map<string, StreamRecord>();
  for (const r of initialRecords) records.set(r.id, r);
  const producerStates = new Map<string, ProducerState>();
  const setProducerCalls: Stub["setProducerCalls"] = [];
  const store: StreamStoreAdapter = {
    async get(streamId) {
      return records.get(streamId) ?? null;
    },
    create: () => notImplemented("create"),
    update: () => notImplemented("update"),
    delete: () => notImplemented("delete"),
    append: () => notImplemented("append"),
    list: () => notImplemented("list"),
    deleteMessages: () => notImplemented("deleteMessages"),
    async getProducerState(streamId, producerId) {
      return producerStates.get(`${streamId}:${producerId}`);
    },
    async setProducerState(streamId, producerId, state) {
      setProducerCalls.push({ streamId, producerId, state });
      producerStates.set(`${streamId}:${producerId}`, state);
    },
    deleteProducerStates: () => notImplemented("deleteProducerStates"),
    incrementChildRefCount: () => notImplemented("incrementChildRefCount"),
    decrementChildRefCount: () => notImplemented("decrementChildRefCount"),
  };
  return { store, records, producerStates, setProducerCalls };
}

function makeRecord(overrides: Partial<StreamRecord> = {}): StreamRecord {
  const { config, lifecycle, ...rest } = overrides;
  return {
    id: "s1",
    currentOffset: "00000000000000000005",
    counter: 5,
    ...rest,
    config: { contentType: CONTENT_TYPE, createdAt: 0, ...config },
    lifecycle: { childRefCount: 0, ...lifecycle },
  };
}

interface MutatorCall {
  kind: "appendMessages" | "closeRecord";
  streamId: string;
  data: Uint8Array[];
  seq?: string;
}

function makeMutators(nextOffset = "00000000000000000099") {
  const calls: MutatorCall[] = [];
  return {
    calls,
    mutators: {
      async appendMessages(
        streamId: string,
        _record: StreamRecord,
        data: Uint8Array[],
        seq?: string,
      ) {
        calls.push({ kind: "appendMessages", streamId, data, seq });
        return nextOffset;
      },
      async closeRecord(streamId: string, _record: StreamRecord, data: Uint8Array[], seq?: string) {
        calls.push({ kind: "closeRecord", streamId, data, seq });
        return nextOffset;
      },
    },
  };
}

function makeService(stub: Stub, mutators: ReturnType<typeof makeMutators>["mutators"]) {
  const producer = new ProducerIdempotencyService(stub.store);
  return new AppendService(stub.store, producer, mutators);
}

describe("appendedResult", () => {
  it("includes producer fields when validation is accepted", () => {
    expect(
      appendedResult(
        "offset-A",
        { kind: "accepted", proposedState: { epoch: 3, lastSeq: 7 } },
        true,
      ),
    ).toEqual({
      status: "appended",
      nextOffset: "offset-A",
      producerEpoch: 3,
      producerSeq: 7,
      closed: true,
    });
  });

  it("omits producer fields when validation is undefined", () => {
    expect(appendedResult("offset-A", undefined)).toEqual({
      status: "appended",
      nextOffset: "offset-A",
      closed: undefined,
    });
  });

  it("omits producer fields when validation is non-accepted", () => {
    expect(appendedResult("offset-A", { kind: "duplicate", epoch: 1, lastSeq: 0 }, false)).toEqual({
      status: "appended",
      nextOffset: "offset-A",
      closed: false,
    });
  });
});

describe("AppendService.execute", () => {
  it("returns not-found when the stream does not exist", async () => {
    const stub = makeStub([]);
    const m = makeMutators();
    const service = makeService(stub, m.mutators);
    const result = await service.execute("missing", {
      data: bytes("x"),
      contentType: CONTENT_TYPE,
    });
    expect(result).toEqual({ status: "not-found" });
    expect(m.calls).toHaveLength(0);
  });

  it("returns gone when the stream is soft-deleted", async () => {
    const stub = makeStub([makeRecord({ lifecycle: { childRefCount: 0, softDeleted: true } })]);
    const m = makeMutators();
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", { data: bytes("x"), contentType: CONTENT_TYPE });
    expect(result).toEqual({ status: "gone" });
    expect(m.calls).toHaveLength(0);
  });

  it("appends a body and shapes a plain appended result", async () => {
    const stub = makeStub([makeRecord()]);
    const m = makeMutators("00000000000000000010");
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", {
      data: bytes("hello"),
      contentType: CONTENT_TYPE,
    });
    expect(result).toEqual({
      status: "appended",
      nextOffset: "00000000000000000010",
      closed: false,
    });
    expect(m.calls).toHaveLength(1);
    expect(m.calls[0]!.kind).toBe("appendMessages");
    expect(m.calls[0]!.streamId).toBe("s1");
    expect(m.calls[0]!.data).toHaveLength(1);
  });

  it("frames JSON arrays into multiple stored messages before mutation", async () => {
    const stub = makeStub([
      makeRecord({ config: { contentType: "application/json", createdAt: 0 } }),
    ]);
    const m = makeMutators();
    const service = makeService(stub, m.mutators);
    await service.execute("s1", {
      data: bytes('[{"a":1},{"b":2}]'),
      contentType: "application/json",
    });
    expect(m.calls).toHaveLength(1);
    expect(m.calls[0]!.kind).toBe("appendMessages");
    expect(m.calls[0]!.data).toHaveLength(2);
  });

  it("dispatches to closeRecord when close is requested with a body", async () => {
    const stub = makeStub([makeRecord()]);
    const m = makeMutators("00000000000000000011");
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", {
      data: bytes("payload"),
      contentType: CONTENT_TYPE,
      close: true,
    });
    expect(result).toEqual({
      status: "appended",
      nextOffset: "00000000000000000011",
      closed: true,
    });
    expect(m.calls).toHaveLength(1);
    expect(m.calls[0]!.kind).toBe("closeRecord");
  });

  it("dispatches to closeRecord with an empty body when close-only on an open stream", async () => {
    const stub = makeStub([makeRecord()]);
    const m = makeMutators("00000000000000000005");
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", {
      data: new Uint8Array(0),
      contentType: CONTENT_TYPE,
      close: true,
    });
    expect(result).toEqual({
      status: "appended",
      nextOffset: "00000000000000000005",
      closed: true,
    });
    expect(m.calls).toHaveLength(1);
    expect(m.calls[0]!.kind).toBe("closeRecord");
    expect(m.calls[0]!.data).toEqual([]);
  });

  it("returns idempotent close for a close-only request on an already closed stream", async () => {
    const stub = makeStub([makeRecord({ lifecycle: { childRefCount: 0, closed: true } })]);
    const m = makeMutators();
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", {
      data: new Uint8Array(0),
      contentType: CONTENT_TYPE,
      close: true,
    });
    expect(result).toEqual({
      status: "appended",
      nextOffset: "00000000000000000005",
      closed: true,
    });
    expect(m.calls).toHaveLength(0);
  });

  it("rejects appends to closed streams with conflict/closed", async () => {
    const stub = makeStub([makeRecord({ lifecycle: { childRefCount: 0, closed: true } })]);
    const m = makeMutators();
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", {
      data: bytes("x"),
      contentType: CONTENT_TYPE,
    });
    expect(result).toEqual({
      status: "conflict",
      conflictReason: "closed",
      closed: true,
      nextOffset: "00000000000000000005",
    });
    expect(m.calls).toHaveLength(0);
  });

  it("rejects content-type mismatches without mutating", async () => {
    const stub = makeStub([makeRecord()]);
    const m = makeMutators();
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", {
      data: bytes("x"),
      contentType: "text/plain",
    });
    expect(result).toEqual({ status: "conflict", conflictReason: "content-type" });
    expect(m.calls).toHaveLength(0);
  });

  it("rejects non-monotonic seq with conflict/sequence", async () => {
    const stub = makeStub([makeRecord({ lifecycle: { childRefCount: 0, lastSeq: "10" } })]);
    const m = makeMutators();
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", {
      data: bytes("x"),
      contentType: CONTENT_TYPE,
      seq: "10",
    });
    expect(result).toEqual({ status: "conflict", conflictReason: "sequence" });
    expect(m.calls).toHaveLength(0);
  });

  it("validates and persists producer state on accepted appends", async () => {
    const stub = makeStub([makeRecord()]);
    const m = makeMutators("00000000000000000020");
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", {
      data: bytes("payload"),
      contentType: CONTENT_TYPE,
      producer: { producerId: "p1", producerEpoch: 0, producerSeq: 0 },
    });
    expect(result).toEqual({
      status: "appended",
      nextOffset: "00000000000000000020",
      producerEpoch: 0,
      producerSeq: 0,
      closed: false,
    });
    expect(m.calls).toHaveLength(1);
    expect(stub.setProducerCalls).toEqual([
      { streamId: "s1", producerId: "p1", state: { epoch: 0, lastSeq: 0 } },
    ]);
  });

  it("returns duplicate without mutating or persisting on duplicate seq", async () => {
    const stub = makeStub([makeRecord({ lifecycle: { childRefCount: 0, closed: true } })]);
    stub.producerStates.set("s1:p1", { epoch: 0, lastSeq: 5 });
    const m = makeMutators();
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", {
      data: bytes("payload"),
      contentType: CONTENT_TYPE,
      producer: { producerId: "p1", producerEpoch: 0, producerSeq: 5 },
    });
    expect(result).toEqual({
      status: "duplicate",
      nextOffset: "00000000000000000005",
      producerEpoch: 0,
      producerSeq: 5,
      closed: true,
    });
    expect(m.calls).toHaveLength(0);
    expect(stub.setProducerCalls).toHaveLength(0);
  });

  it("returns producer-gap without mutating", async () => {
    const stub = makeStub([makeRecord()]);
    const m = makeMutators();
    const service = makeService(stub, m.mutators);
    const result = await service.execute("s1", {
      data: bytes("payload"),
      contentType: CONTENT_TYPE,
      producer: { producerId: "p1", producerEpoch: 0, producerSeq: 5 },
    });
    expect(result).toEqual({ status: "producer-gap", expectedSeq: 0, receivedSeq: 5 });
    expect(m.calls).toHaveLength(0);
    expect(stub.setProducerCalls).toHaveLength(0);
  });

  it("does not persist producer state when the mutator throws", async () => {
    const stub = makeStub([makeRecord()]);
    const failingMutators = {
      async appendMessages(): Promise<string> {
        throw new Error("boom");
      },
      async closeRecord(): Promise<string> {
        throw new Error("boom");
      },
    };
    const service = makeService(stub, failingMutators);
    await expect(
      service.execute("s1", {
        data: bytes("payload"),
        contentType: CONTENT_TYPE,
        producer: { producerId: "p1", producerEpoch: 0, producerSeq: 0 },
      }),
    ).rejects.toThrow("boom");
    expect(stub.setProducerCalls).toHaveLength(0);
  });
});
