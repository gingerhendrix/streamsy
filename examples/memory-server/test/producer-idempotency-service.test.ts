/**
 * Unit coverage for the ProducerIdempotencyService extracted from
 * StreamProtocol. Pins the state-transition decisions previously made by
 * the inline `validateProducer` / `acceptProducer` helpers and the
 * `AppendResult` shapes produced for rejections.
 */

import { describe, it, expect } from "vitest";
import {
  ProducerIdempotencyService,
  rejectionToAppendResult,
  validateProducer,
} from "../../../packages/core/src/protocol/helpers/producer-idempotency-service.ts";
import type {
  ProducerState,
  StreamStoreAdapter,
} from "../../../packages/core/src/types/storage.ts";

function notImplemented(method: string): never {
  throw new Error(`store.${method} not implemented in test stub`);
}

function createStubStore(initial?: Map<string, ProducerState>): {
  store: StreamStoreAdapter;
  states: Map<string, ProducerState>;
  setCalls: Array<{ streamId: string; producerId: string; state: ProducerState }>;
} {
  const states = initial ?? new Map<string, ProducerState>();
  const setCalls: Array<{ streamId: string; producerId: string; state: ProducerState }> = [];
  const store: StreamStoreAdapter = {
    get: () => notImplemented("get"),
    create: () => notImplemented("create"),
    update: () => notImplemented("update"),
    delete: () => notImplemented("delete"),
    append: () => notImplemented("append"),
    list: () => notImplemented("list"),
    deleteMessages: () => notImplemented("deleteMessages"),
    async getProducerState(streamId, producerId) {
      return states.get(`${streamId}:${producerId}`);
    },
    async setProducerState(streamId, producerId, state) {
      setCalls.push({ streamId, producerId, state });
      states.set(`${streamId}:${producerId}`, state);
    },
    deleteProducerStates: () => notImplemented("deleteProducerStates"),
    incrementChildRefCount: () => notImplemented("incrementChildRefCount"),
    decrementChildRefCount: () => notImplemented("decrementChildRefCount"),
  };
  return { store, states, setCalls };
}

describe("validateProducer", () => {
  describe("with no prior producer state", () => {
    it("accepts the first append at seq 0", () => {
      expect(validateProducer(undefined, 0, 0)).toEqual({
        kind: "accepted",
        proposedState: { epoch: 0, lastSeq: 0 },
      });
    });

    it("accepts the first append at any epoch when seq is 0", () => {
      expect(validateProducer(undefined, 7, 0)).toEqual({
        kind: "accepted",
        proposedState: { epoch: 7, lastSeq: 0 },
      });
    });

    it("reports a gap when the first observed seq is non-zero", () => {
      expect(validateProducer(undefined, 0, 3)).toEqual({
        kind: "gap",
        expectedSeq: 0,
        receivedSeq: 3,
      });
    });
  });

  describe("with a stored producer state", () => {
    const state: ProducerState = { epoch: 2, lastSeq: 5 };

    it("rejects a lower epoch as stale", () => {
      expect(validateProducer(state, 1, 0)).toEqual({ kind: "stale-epoch", currentEpoch: 2 });
      expect(validateProducer(state, 1, 999)).toEqual({ kind: "stale-epoch", currentEpoch: 2 });
    });

    it("accepts a higher epoch only with seq 0", () => {
      expect(validateProducer(state, 3, 0)).toEqual({
        kind: "accepted",
        proposedState: { epoch: 3, lastSeq: 0 },
      });
    });

    it("rejects a higher epoch with non-zero seq as invalid-epoch-seq", () => {
      expect(validateProducer(state, 3, 1)).toEqual({ kind: "invalid-epoch-seq" });
      expect(validateProducer(state, 3, 6)).toEqual({ kind: "invalid-epoch-seq" });
    });

    it("reports a duplicate when same-epoch seq <= lastSeq", () => {
      expect(validateProducer(state, 2, 5)).toEqual({
        kind: "duplicate",
        epoch: 2,
        lastSeq: 5,
      });
      expect(validateProducer(state, 2, 0)).toEqual({
        kind: "duplicate",
        epoch: 2,
        lastSeq: 5,
      });
    });

    it("accepts the next same-epoch sequence", () => {
      expect(validateProducer(state, 2, 6)).toEqual({
        kind: "accepted",
        proposedState: { epoch: 2, lastSeq: 6 },
      });
    });

    it("reports a gap when same-epoch seq jumps past lastSeq + 1", () => {
      expect(validateProducer(state, 2, 8)).toEqual({
        kind: "gap",
        expectedSeq: 6,
        receivedSeq: 8,
      });
    });
  });
});

describe("rejectionToAppendResult", () => {
  it("shapes a duplicate result with currentOffset and producer fields", () => {
    expect(
      rejectionToAppendResult({ kind: "duplicate", epoch: 2, lastSeq: 5 }, "offset-A", true),
    ).toEqual({
      status: "duplicate",
      nextOffset: "offset-A",
      producerEpoch: 2,
      producerSeq: 5,
      closed: true,
    });
  });

  it("shapes a stale-epoch result with the current epoch only", () => {
    expect(
      rejectionToAppendResult({ kind: "stale-epoch", currentEpoch: 4 }, "offset-A", false),
    ).toEqual({ status: "stale-epoch", currentEpoch: 4 });
  });

  it("shapes a producer-gap result with expected and received seq", () => {
    expect(
      rejectionToAppendResult(
        { kind: "gap", expectedSeq: 6, receivedSeq: 9 },
        "offset-A",
        false,
      ),
    ).toEqual({ status: "producer-gap", expectedSeq: 6, receivedSeq: 9 });
  });

  it("shapes an invalid-epoch-seq result", () => {
    expect(
      rejectionToAppendResult({ kind: "invalid-epoch-seq" }, "offset-A", false),
    ).toEqual({ status: "invalid-epoch-seq" });
  });
});

describe("ProducerIdempotencyService", () => {
  it("loads producer state through the adapter", async () => {
    const { store } = createStubStore(
      new Map([["s1:p1", { epoch: 1, lastSeq: 4 }]]),
    );
    const service = new ProducerIdempotencyService(store);
    expect(await service.load("s1", "p1")).toEqual({ epoch: 1, lastSeq: 4 });
    expect(await service.load("s1", "p2")).toBeUndefined();
  });

  it("validate delegates to the pure validator", () => {
    const { store } = createStubStore();
    const service = new ProducerIdempotencyService(store);
    expect(service.validate(undefined, 0, 0)).toEqual({
      kind: "accepted",
      proposedState: { epoch: 0, lastSeq: 0 },
    });
    expect(service.validate({ epoch: 1, lastSeq: 0 }, 0, 0)).toEqual({
      kind: "stale-epoch",
      currentEpoch: 1,
    });
  });

  it("persistIfAccepted writes only on accepted validation with producer options", async () => {
    const { store, setCalls } = createStubStore();
    const service = new ProducerIdempotencyService(store);
    const producer = { producerId: "p1", producerEpoch: 0, producerSeq: 0 };
    await service.persistIfAccepted("s1", producer, {
      kind: "accepted",
      proposedState: { epoch: 0, lastSeq: 0 },
    });
    expect(setCalls).toEqual([
      { streamId: "s1", producerId: "p1", state: { epoch: 0, lastSeq: 0 } },
    ]);
  });

  it("persistIfAccepted is a no-op when producer options are missing", async () => {
    const { store, setCalls } = createStubStore();
    const service = new ProducerIdempotencyService(store);
    await service.persistIfAccepted("s1", undefined, {
      kind: "accepted",
      proposedState: { epoch: 0, lastSeq: 0 },
    });
    expect(setCalls).toHaveLength(0);
  });

  it("persistIfAccepted is a no-op for non-accepted validations", async () => {
    const { store, setCalls } = createStubStore();
    const service = new ProducerIdempotencyService(store);
    const producer = { producerId: "p1", producerEpoch: 0, producerSeq: 0 };
    await service.persistIfAccepted("s1", producer, undefined);
    await service.persistIfAccepted("s1", producer, {
      kind: "duplicate",
      epoch: 0,
      lastSeq: 0,
    });
    await service.persistIfAccepted("s1", producer, { kind: "stale-epoch", currentEpoch: 1 });
    await service.persistIfAccepted("s1", producer, {
      kind: "gap",
      expectedSeq: 1,
      receivedSeq: 5,
    });
    await service.persistIfAccepted("s1", producer, { kind: "invalid-epoch-seq" });
    expect(setCalls).toHaveLength(0);
  });
});
