import { describe, expect, it } from "vitest";
import type { AppendPlan, StorageAppendResult } from "../../types/storage-adapter.ts";
import type { BoundStream } from "./bind-stream.ts";
import type {
  AwaitChangeResult,
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  StreamRecord,
} from "../../types/storage.ts";
import { ExpiryPolicy } from "./expiry-policy.ts";

const record: StreamRecord = {
  id: "s",
  config: { contentType: "application/json", ttlSeconds: 30, createdAt: 1_000 },
  lifecycle: { expiresAtMs: 31_000 },
  currentOffset: "0000000000000001_0000000000000000",
  counter: 1,
};

class TestStream implements BoundStream {
  readonly id = record.id;
  appends: AppendPlan[] = [];
  scheduled: number[] = [];

  constructor(private readonly appendResult: StorageAppendResult) {}

  getRecord(): Promise<StreamRecord | null> {
    return Promise.resolve(record);
  }

  listMessages(_options?: ListMessagesOptions): Promise<StoredMessage[]> {
    return Promise.resolve([]);
  }

  getProducerState(_producerId: string): Promise<ProducerState | undefined> {
    return Promise.resolve(undefined);
  }

  append(plan: AppendPlan): Promise<StorageAppendResult> {
    this.appends.push(plan);
    return Promise.resolve(this.appendResult);
  }

  awaitChange(): Promise<AwaitChangeResult> {
    // ExpiryPolicy never waits; the stub satisfies the required seam method only.
    return Promise.resolve({
      status: "timeout",
      snapshot: {
        present: true,
        currentOffset: record.currentOffset,
        closed: false,
        softDeleted: false,
      },
    });
  }

  scheduleExpiry(at: number): Promise<void> | void {
    this.scheduled.push(at);
  }

  cancelExpiry(): Promise<void> | void {}
}

describe("ExpiryPolicy.touch", () => {
  it("schedules read-side TTL renewal through after-commit effects after a successful commit", async () => {
    const touched: StreamRecord = {
      ...record,
      lifecycle: { ...record.lifecycle, expiresAtMs: 40_000 },
    };
    const stream = new TestStream({ status: "appended", record: touched });
    const policy = new ExpiryPolicy({
      resolve: () => stream,
      clock: { now: () => 10_000, date: (value) => new Date(value ?? 0) },
      onScheduledExpiry: async () => {},
    });

    await expect(policy.touch(stream, record, "read")).resolves.toBe(touched);

    expect(stream.appends).toEqual([
      {
        preconditions: { expectedOffset: record.currentOffset },
        recordPatch: { lifecycle: { expiresAtMs: 40_000 } },
      },
    ]);
    expect(stream.scheduled).toEqual([40_000]);
  });

  it("does not schedule read-side TTL renewal when the commit precondition fails", async () => {
    const stream = new TestStream({ status: "precondition-failed", record, reason: "offset" });
    const policy = new ExpiryPolicy({
      resolve: () => stream,
      clock: { now: () => 10_000, date: (value) => new Date(value ?? 0) },
      onScheduledExpiry: async () => {},
    });

    await expect(policy.touch(stream, record, "read")).resolves.toBe(record);

    expect(stream.appends).toEqual([
      {
        preconditions: { expectedOffset: record.currentOffset },
        recordPatch: { lifecycle: { expiresAtMs: 40_000 } },
      },
    ]);
    expect(stream.scheduled).toEqual([]);
  });
});
