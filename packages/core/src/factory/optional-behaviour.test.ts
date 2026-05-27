/**
 * Optional-behaviour fixtures.
 *
 * Compose `Stream` instances that intentionally omit optional members and
 * assert that the require-style helpers surface the omission as a
 * `not-supported` protocol result with the documented feature id. Streams
 * that supply the member pass it through unchanged.
 *
 * These fixtures encode the architecture guarantee that optionality is real
 * without introducing a capability enum or a public dependency bag.
 */
import { describe, expect, it } from "vitest";
import { isNotSupported } from "../types/factory.ts";
import type { StoredMessage, StreamRecord } from "../types/storage.ts";
import { composeStream } from "./compose-stream.ts";
import {
  requireEventHub,
  requireExpiryScheduler,
  requireMutationCoordinator,
  requireProducerStore,
  requireReferenceTracker,
} from "./require-deps.ts";

function emptyRecordStore() {
  return {
    getRecord: async () => null,
    createRecord: async () => ({ status: "created" as const }),
    updateRecord: async (): Promise<StreamRecord> => ({
      id: "x",
      config: { contentType: "application/octet-stream", createdAt: 0 },
      lifecycle: { childRefCount: 0 },
      currentOffset: "0_0",
      counter: 0,
    }),
    deleteRecord: async () => {},
  };
}

function emptyMessageStore() {
  return {
    appendMessages: async () => {},
    listMessages: async (): Promise<StoredMessage[]> => [],
    deleteMessages: async () => {},
  };
}

function minimalStream(id: string) {
  return composeStream({
    id,
    recordStore: emptyRecordStore(),
    messageStore: emptyMessageStore(),
  });
}

describe("optional-behaviour fixtures", () => {
  describe("stream without producer store", () => {
    it("require helper returns producer-idempotency not-supported", () => {
      const stream = minimalStream("no-producer");
      const result = requireProducerStore(stream);
      expect(isNotSupported(result)).toBe(true);
      if (isNotSupported(result)) {
        expect(result.feature).toBe("producer-idempotency");
      }
    });

    it("forwards the caller-supplied message", () => {
      const stream = minimalStream("no-producer");
      const result = requireProducerStore(stream, "memory adapter does not persist producer state");
      expect(isNotSupported(result)).toBe(true);
      if (isNotSupported(result)) {
        expect(result.message).toBe("memory adapter does not persist producer state");
      }
    });

    it("returns the producer store when supplied", async () => {
      const stream = composeStream({
        id: "with-producer",
        recordStore: emptyRecordStore(),
        messageStore: emptyMessageStore(),
        producerStore: {
          getProducerState: async () => ({ epoch: 1, lastSeq: 7 }),
          setProducerState: async () => {},
          deleteProducerStates: async () => {},
        },
      });
      const result = requireProducerStore(stream);
      expect(isNotSupported(result)).toBe(false);
      if (!isNotSupported(result)) {
        const state = await result.getProducerState("p1");
        expect(state).toEqual({ epoch: 1, lastSeq: 7 });
      }
    });
  });

  describe("stream without reference tracker (forks disabled)", () => {
    it("require helper returns fork not-supported", () => {
      const result = requireReferenceTracker(minimalStream("no-forks"));
      expect(isNotSupported(result)).toBe(true);
      if (isNotSupported(result)) {
        expect(result.feature).toBe("fork");
      }
    });

    it("returns the reference tracker when supplied", async () => {
      const stream = composeStream({
        id: "forks",
        recordStore: emptyRecordStore(),
        messageStore: emptyMessageStore(),
        referenceTracker: {
          incrementChildRefCount: async () => 1,
          decrementChildRefCount: async () => 0,
        },
      });
      const result = requireReferenceTracker(stream);
      expect(isNotSupported(result)).toBe(false);
      if (!isNotSupported(result)) {
        expect(await result.incrementChildRefCount()).toBe(1);
      }
    });
  });

  describe("stream without live-read events", () => {
    it("require helper returns live-read not-supported", () => {
      const result = requireEventHub(minimalStream("no-events"));
      expect(isNotSupported(result)).toBe(true);
      if (isNotSupported(result)) {
        expect(result.feature).toBe("live-read");
      }
    });

    it("returns the event hub when supplied", async () => {
      const stream = composeStream({
        id: "events",
        recordStore: emptyRecordStore(),
        messageStore: emptyMessageStore(),
        events: {
          waitForEvent: async () => ({ status: "timeout" as const }),
          notify: () => {},
        },
      });
      const result = requireEventHub(stream);
      expect(isNotSupported(result)).toBe(false);
      if (!isNotSupported(result)) {
        expect(await result.waitForEvent({ timeoutMs: 0 })).toEqual({ status: "timeout" });
      }
    });
  });

  describe("stream with lazy expiry only", () => {
    it("require helper returns active-expiry not-supported", () => {
      const result = requireExpiryScheduler(minimalStream("lazy-only"));
      expect(isNotSupported(result)).toBe(true);
      if (isNotSupported(result)) {
        expect(result.feature).toBe("active-expiry");
      }
    });

    it("returns the scheduler when supplied", () => {
      const calls: number[] = [];
      const stream = composeStream({
        id: "active-expiry",
        recordStore: emptyRecordStore(),
        messageStore: emptyMessageStore(),
        expiry: {
          scheduleExpiry: (at) => {
            calls.push(at);
          },
          cancelExpiry: () => {},
        },
      });
      const result = requireExpiryScheduler(stream);
      expect(isNotSupported(result)).toBe(false);
      if (!isNotSupported(result)) {
        result.scheduleExpiry(42);
        expect(calls).toEqual([42]);
      }
    });
  });

  describe("stream without mutation coordinator", () => {
    it("require helper returns mutation-lock not-supported", () => {
      const result = requireMutationCoordinator(minimalStream("no-mutex"));
      expect(isNotSupported(result)).toBe(true);
      if (isNotSupported(result)) {
        expect(result.feature).toBe("mutation-lock");
      }
    });

    it("returns the coordinator when supplied", async () => {
      const stream = composeStream({
        id: "mutex",
        recordStore: emptyRecordStore(),
        messageStore: emptyMessageStore(),
        mutations: { withMutationLock: async (fn) => fn() },
      });
      const result = requireMutationCoordinator(stream);
      expect(isNotSupported(result)).toBe(false);
      if (!isNotSupported(result)) {
        expect(await result.withMutationLock(async () => "value")).toBe("value");
      }
    });
  });

  describe("a fully minimal stream omits every optional member", () => {
    it("returns not-supported for every require helper", () => {
      const stream = minimalStream("minimal");
      expect(isNotSupported(requireProducerStore(stream))).toBe(true);
      expect(isNotSupported(requireReferenceTracker(stream))).toBe(true);
      expect(isNotSupported(requireMutationCoordinator(stream))).toBe(true);
      expect(isNotSupported(requireEventHub(stream))).toBe(true);
      expect(isNotSupported(requireExpiryScheduler(stream))).toBe(true);
    });
  });
});
