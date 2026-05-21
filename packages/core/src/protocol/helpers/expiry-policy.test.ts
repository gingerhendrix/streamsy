/**
 * Focused unit coverage for ExpiryPolicy extracted from StreamProtocol.
 *
 * The lifecycle/expiry surface is exercised end-to-end via the conformance
 * suite. These cases pin the bounded extraction's contract:
 *
 *   - TTL effective deadline uses injected clock.now().
 *   - Expires-At absolute deadline uses injected clock.date(value).getTime().
 *   - TTL takes precedence over Expires-At when both are present.
 *   - scheduleExpiry forwards persisted lifecycle.expiresAtMs to the adapter.
 *   - touch updates lifecycle.expiresAtMs and reschedules for append/close/read.
 *   - touch is a no-op for live-read and for records with no ttlSeconds.
 *   - expireIfNeeded only invokes the injected handler when expired.
 *   - The scheduled-expiry handler injected here mirrors how StreamProtocol
 *     wires `(id) => this.handleScheduledExpiry(id)` into the policy.
 */

import { describe, expect, it } from "vitest";
import { ExpiryPolicy, type TouchReason } from "../../protocol/helpers/expiry-policy.ts";
import type {
  Clock,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
  StreamStoreAdapter,
} from "../../types/storage.ts";

function makeClock(initialNow: number): Clock & { setNow: (next: number) => void } {
  let nowMs = initialNow;
  return {
    now: () => nowMs,
    date: (value?: number | string) => new Date(value ?? nowMs),
    setNow: (next: number) => {
      nowMs = next;
    },
  };
}

interface ScheduleCall {
  streamId: StreamId;
  at: number;
  callback?: () => Promise<void>;
}

interface UpdateCall {
  streamId: StreamId;
  patch: StreamRecordPatch;
}

interface Stub {
  store: StreamStoreAdapter;
  records: Map<StreamId, StreamRecord>;
  scheduleCalls: ScheduleCall[];
  updateCalls: UpdateCall[];
}

function notImplemented(method: string): never {
  throw new Error(`store.${method} not implemented in test stub`);
}

function makeStub(records: StreamRecord[] = [], opts: { withScheduler?: boolean } = {}): Stub {
  const recordMap = new Map<StreamId, StreamRecord>();
  for (const r of records) recordMap.set(r.id, r);
  const scheduleCalls: ScheduleCall[] = [];
  const updateCalls: UpdateCall[] = [];
  const withScheduler = opts.withScheduler ?? true;
  const store: StreamStoreAdapter = {
    async get(streamId) {
      return recordMap.get(streamId) ?? null;
    },
    create: () => notImplemented("create"),
    async update(streamId, patch) {
      updateCalls.push({ streamId, patch });
      const existing = recordMap.get(streamId);
      if (!existing) throw new Error(`record missing in stub: ${streamId}`);
      const merged: StreamRecord = {
        ...existing,
        ...patch,
        config: { ...existing.config, ...patch.config },
        lifecycle: { ...existing.lifecycle, ...patch.lifecycle },
      };
      recordMap.set(streamId, merged);
      return merged;
    },
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
  if (withScheduler) {
    store.scheduleExpiry = (streamId, at, callback) => {
      scheduleCalls.push({ streamId, at, callback });
    };
  }
  return { store, records: recordMap, scheduleCalls, updateCalls };
}

function makeRecord(overrides: Partial<StreamRecord> = {}): StreamRecord {
  const { config, lifecycle, ...rest } = overrides;
  return {
    id: "stream-1",
    currentOffset: "00000000000000000000",
    counter: 0,
    ...rest,
    config: { contentType: "application/octet-stream", createdAt: 0, ...config },
    lifecycle: { childRefCount: 0, ...lifecycle },
  };
}

describe("ExpiryPolicy.computeExpiresAtMs", () => {
  it("derives ttlSeconds deadline from injected clock.now()", () => {
    const clock = makeClock(1_000);
    const stub = makeStub();
    const policy = new ExpiryPolicy(stub.store, clock, async () => {});
    expect(policy.computeExpiresAtMs({ ttlSeconds: 60 })).toBe(1_000 + 60_000);
    clock.setNow(5_000);
    expect(policy.computeExpiresAtMs({ ttlSeconds: 60 })).toBe(5_000 + 60_000);
  });

  it("derives expiresAt deadline from injected clock.date(value).getTime()", () => {
    const clock = makeClock(0);
    const stub = makeStub();
    const policy = new ExpiryPolicy(stub.store, clock, async () => {});
    const iso = "2025-06-01T12:34:56.000Z";
    expect(policy.computeExpiresAtMs({ expiresAt: iso })).toBe(new Date(iso).getTime());
  });

  it("prefers ttlSeconds over expiresAt when both are present", () => {
    const clock = makeClock(2_000);
    const stub = makeStub();
    const policy = new ExpiryPolicy(stub.store, clock, async () => {});
    const iso = "2099-01-01T00:00:00.000Z";
    expect(policy.computeExpiresAtMs({ ttlSeconds: 30, expiresAt: iso })).toBe(2_000 + 30_000);
  });

  it("returns undefined when neither ttlSeconds nor expiresAt is set", () => {
    const clock = makeClock(0);
    const stub = makeStub();
    const policy = new ExpiryPolicy(stub.store, clock, async () => {});
    expect(policy.computeExpiresAtMs({})).toBeUndefined();
  });
});

describe("ExpiryPolicy.scheduleExpiry", () => {
  it("forwards persisted expiresAtMs and a handler to the adapter", async () => {
    const clock = makeClock(0);
    const calls: string[] = [];
    const handler = async (id: StreamId) => {
      calls.push(id);
    };
    const stub = makeStub();
    const policy = new ExpiryPolicy(stub.store, clock, handler);
    const record = makeRecord({ lifecycle: { childRefCount: 0, expiresAtMs: 12_345 } });

    await policy.scheduleExpiry(record);

    expect(stub.scheduleCalls).toHaveLength(1);
    expect(stub.scheduleCalls[0]!.streamId).toBe(record.id);
    expect(stub.scheduleCalls[0]!.at).toBe(12_345);
    await stub.scheduleCalls[0]!.callback?.();
    expect(calls).toEqual([record.id]);
  });

  it("is a no-op when lifecycle.expiresAtMs is absent", async () => {
    const clock = makeClock(0);
    const stub = makeStub();
    const policy = new ExpiryPolicy(stub.store, clock, async () => {});
    const record = makeRecord();

    await policy.scheduleExpiry(record);

    expect(stub.scheduleCalls).toHaveLength(0);
  });

  it("does not throw when adapter has no scheduleExpiry capability", async () => {
    const clock = makeClock(0);
    const stub = makeStub([], { withScheduler: false });
    const policy = new ExpiryPolicy(stub.store, clock, async () => {});
    const record = makeRecord({ lifecycle: { childRefCount: 0, expiresAtMs: 9_999 } });

    await expect(policy.scheduleExpiry(record)).resolves.toBeUndefined();
  });
});

describe("ExpiryPolicy.touch", () => {
  const reasons: Array<Exclude<TouchReason, "live-read">> = ["append", "close", "read"];
  for (const reason of reasons) {
    it(`updates lifecycle.expiresAtMs and reschedules for ${reason}`, async () => {
      const clock = makeClock(10_000);
      const stub = makeStub([
        makeRecord({
          config: { contentType: "application/octet-stream", createdAt: 0, ttlSeconds: 60 },
          lifecycle: { childRefCount: 0, expiresAtMs: 999 },
        }),
      ]);
      const policy = new ExpiryPolicy(stub.store, clock, async () => {});

      const record = stub.records.get("stream-1")!;
      await policy.touch("stream-1", record, reason);

      expect(stub.updateCalls).toEqual([
        { streamId: "stream-1", patch: { lifecycle: { expiresAtMs: 70_000 } } },
      ]);
      expect(stub.scheduleCalls).toHaveLength(1);
      expect(stub.scheduleCalls[0]).toMatchObject({ streamId: "stream-1", at: 70_000 });
      expect(typeof stub.scheduleCalls[0]!.callback).toBe("function");
    });
  }

  it("is a no-op for live-read so live polling does not extend TTL", async () => {
    const clock = makeClock(10_000);
    const stub = makeStub([
      makeRecord({
        config: { contentType: "application/octet-stream", createdAt: 0, ttlSeconds: 60 },
        lifecycle: { childRefCount: 0, expiresAtMs: 70_000 },
      }),
    ]);
    const policy = new ExpiryPolicy(stub.store, clock, async () => {});

    const record = stub.records.get("stream-1")!;
    await policy.touch("stream-1", record, "live-read");

    expect(stub.updateCalls).toHaveLength(0);
    expect(stub.scheduleCalls).toHaveLength(0);
  });

  it("is a no-op when the record has no ttlSeconds (Expires-At only)", async () => {
    const clock = makeClock(10_000);
    const stub = makeStub([
      makeRecord({
        config: {
          contentType: "application/octet-stream",
          createdAt: 0,
          expiresAt: "2099-01-01T00:00:00.000Z",
        },
        lifecycle: {
          childRefCount: 0,
          expiresAtMs: new Date("2099-01-01T00:00:00.000Z").getTime(),
        },
      }),
    ]);
    const policy = new ExpiryPolicy(stub.store, clock, async () => {});

    const record = stub.records.get("stream-1")!;
    for (const reason of ["append", "close", "read"] as const) {
      await policy.touch("stream-1", record, reason);
    }

    expect(stub.updateCalls).toHaveLength(0);
    expect(stub.scheduleCalls).toHaveLength(0);
  });

  it("calls the injected handler via the scheduled callback", async () => {
    const clock = makeClock(10_000);
    const stub = makeStub([
      makeRecord({
        config: { contentType: "application/octet-stream", createdAt: 0, ttlSeconds: 60 },
        lifecycle: { childRefCount: 0, expiresAtMs: 0 },
      }),
    ]);
    const handlerCalls: string[] = [];
    const policy = new ExpiryPolicy(stub.store, clock, async (id) => {
      handlerCalls.push(id);
    });

    const record = stub.records.get("stream-1")!;
    await policy.touch("stream-1", record, "append");
    await stub.scheduleCalls[0]!.callback?.();

    expect(handlerCalls).toEqual(["stream-1"]);
  });
});

describe("ExpiryPolicy.expireIfNeeded", () => {
  it("invokes the handler when the persisted deadline is at or past the clock", async () => {
    const clock = makeClock(100);
    const stub = makeStub([
      makeRecord({
        lifecycle: { childRefCount: 0, expiresAtMs: 50 },
      }),
    ]);
    const handlerCalls: string[] = [];
    const policy = new ExpiryPolicy(stub.store, clock, async (id) => {
      handlerCalls.push(id);
    });

    await policy.expireIfNeeded("stream-1");
    expect(handlerCalls).toEqual(["stream-1"]);
  });

  it("invokes the handler at the boundary (now === expiresAtMs)", async () => {
    const clock = makeClock(50);
    const stub = makeStub([
      makeRecord({
        lifecycle: { childRefCount: 0, expiresAtMs: 50 },
      }),
    ]);
    const handlerCalls: string[] = [];
    const policy = new ExpiryPolicy(stub.store, clock, async (id) => {
      handlerCalls.push(id);
    });

    await policy.expireIfNeeded("stream-1");
    expect(handlerCalls).toEqual(["stream-1"]);
  });

  it("does not invoke the handler when the deadline is in the future", async () => {
    const clock = makeClock(10);
    const stub = makeStub([
      makeRecord({
        lifecycle: { childRefCount: 0, expiresAtMs: 1_000 },
      }),
    ]);
    const handlerCalls: string[] = [];
    const policy = new ExpiryPolicy(stub.store, clock, async (id) => {
      handlerCalls.push(id);
    });

    await policy.expireIfNeeded("stream-1");
    expect(handlerCalls).toEqual([]);
  });

  it("does not invoke the handler when the record has no expiresAtMs", async () => {
    const clock = makeClock(10_000);
    const stub = makeStub([makeRecord()]);
    const handlerCalls: string[] = [];
    const policy = new ExpiryPolicy(stub.store, clock, async (id) => {
      handlerCalls.push(id);
    });

    await policy.expireIfNeeded("stream-1");
    expect(handlerCalls).toEqual([]);
  });

  it("does not invoke the handler when the record is missing", async () => {
    const clock = makeClock(10_000);
    const stub = makeStub();
    const handlerCalls: string[] = [];
    const policy = new ExpiryPolicy(stub.store, clock, async (id) => {
      handlerCalls.push(id);
    });

    await policy.expireIfNeeded("stream-missing");
    expect(handlerCalls).toEqual([]);
  });
});

describe("ExpiryPolicy.isExpired", () => {
  it("returns true when expiresAtMs is at or past the clock", () => {
    const clock = makeClock(100);
    const stub = makeStub();
    const policy = new ExpiryPolicy(stub.store, clock, async () => {});
    expect(policy.isExpired(makeRecord({ lifecycle: { childRefCount: 0, expiresAtMs: 99 } }))).toBe(
      true,
    );
    expect(
      policy.isExpired(makeRecord({ lifecycle: { childRefCount: 0, expiresAtMs: 100 } })),
    ).toBe(true);
  });

  it("returns false when expiresAtMs is in the future or absent", () => {
    const clock = makeClock(100);
    const stub = makeStub();
    const policy = new ExpiryPolicy(stub.store, clock, async () => {});
    expect(
      policy.isExpired(makeRecord({ lifecycle: { childRefCount: 0, expiresAtMs: 101 } })),
    ).toBe(false);
    expect(policy.isExpired(makeRecord())).toBe(false);
  });
});
