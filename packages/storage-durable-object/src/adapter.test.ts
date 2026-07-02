import { describe, expect, it } from "vitest";
import {
  buildChangeSnapshot,
  changeSnapshotDiffers,
  StreamProtocol,
  ZERO_OFFSET,
} from "@streamsy/core";
import type {
  AppendPlan,
  AwaitChangeOptions,
  AwaitChangeResult,
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
} from "@streamsy/core";
import {
  createDurableObjectStorageAdapter,
  type DurableObjectStorageAdapterOptions,
} from "./adapter.ts";

type FactoryNamespace = DurableObjectStorageAdapterOptions["namespace"];

const CONTENT_TYPE = "application/octet-stream";

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function text(value: Uint8Array): string {
  return new TextDecoder().decode(value);
}

type FailureReason = "offset" | "closed" | "producer";

interface WritePlan {
  createRecord?: StreamRecord;
  preconditions: AppendPlan["preconditions"];
  messages?: StoredMessage[];
  recordPatch?: StreamRecordPatch;
}

type WriteResult =
  | { status: "committed"; record: StreamRecord }
  | { status: "precondition-failed"; record: StreamRecord | null; reason?: FailureReason };

interface FakeStubState {
  boundId?: StreamId;
  initCalls: StreamId[];
  record: StreamRecord | null;
  messages: StoredMessage[];
  producers: Map<string, ProducerState>;
  children: Set<StreamId>;
  expiry: { at?: number; cancelled: boolean };
  awaitOptions: AwaitChangeOptions[];
}

/**
 * Fake Durable Object stub mirroring the real `DurableObjectStreamStorage` RPC
 * surface: every stream-facing method takes `streamId` first and self-initializes
 * on first access (no separate `init`).
 */
class FakeStub {
  readonly state: FakeStubState;
  private waiters = new Set<() => void>();

  constructor(state: FakeStubState) {
    this.state = state;
  }

  private ensureInit(streamId: StreamId): void {
    if (this.state.boundId && this.state.boundId !== streamId) {
      throw new Error(`Durable Object already initialized for stream ${this.state.boundId}`);
    }
    if (!this.state.boundId) {
      this.state.boundId = streamId;
      this.state.initCalls.push(streamId);
    }
  }

  async getRecord(streamId: StreamId): Promise<StreamRecord | null> {
    this.ensureInit(streamId);
    return this.state.record;
  }

  async applyMutation(streamId: StreamId, plan: WritePlan): Promise<WriteResult> {
    this.ensureInit(streamId);
    let record = this.state.record;
    if (plan.createRecord) {
      if (plan.createRecord.id !== streamId)
        throw new Error(
          `Record id ${plan.createRecord.id} does not match bound stream ${streamId}`,
        );
      if (record) return { status: "precondition-failed", record };
      this.state.record = plan.createRecord;
      record = plan.createRecord;
    }
    if (!record) return { status: "precondition-failed", record: null };
    if (
      plan.preconditions.expectedOffset !== undefined &&
      plan.preconditions.expectedOffset !== record.currentOffset
    )
      return { status: "precondition-failed", record, reason: "offset" };
    if (
      plan.preconditions.expectedClosed !== undefined &&
      plan.preconditions.expectedClosed !== (record.lifecycle.closed === true)
    )
      return { status: "precondition-failed", record, reason: "closed" };

    const producer = plan.preconditions.producer;
    if (producer) {
      const current = this.state.producers.get(producer.producerId);
      if (!producerStatesEqual(current, producer.expected))
        return { status: "precondition-failed", record, reason: "producer" };
    }

    if (plan.messages) this.state.messages.push(...plan.messages);
    let updated = record;
    if (plan.recordPatch) {
      updated = mergeRecord(record, plan.recordPatch);
      this.state.record = updated;
    }
    if (producer) this.state.producers.set(producer.producerId, producer.next);
    this.wake();
    return { status: "committed", record: updated };
  }

  async append(streamId: StreamId, plan: AppendPlan) {
    const out = await this.applyMutation(streamId, {
      preconditions: plan.preconditions,
      messages: plan.messages,
      recordPatch: plan.recordPatch,
    });
    if (out.status === "committed") return { status: "appended" as const, record: out.record };
    return out.reason !== undefined
      ? { status: "precondition-failed" as const, record: out.record, reason: out.reason }
      : { status: "precondition-failed" as const, record: out.record };
  }

  async listMessages(
    streamId: StreamId,
    options: ListMessagesOptions = {},
  ): Promise<StoredMessage[]> {
    this.ensureInit(streamId);
    let out = this.state.messages;
    if (options.after) out = out.filter((m) => m.offset > options.after!);
    if (options.until) out = out.filter((m) => m.offset <= options.until!);
    if (options.limit !== undefined) out = out.slice(0, options.limit);
    return out;
  }

  async getProducerState(
    streamId: StreamId,
    producerId: string,
  ): Promise<ProducerState | undefined> {
    this.ensureInit(streamId);
    return this.state.producers.get(producerId);
  }

  async purgeSelf(streamId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    this.state.record = null;
    this.state.messages = [];
    this.state.producers.clear();
    this.state.children.clear();
    this.state.expiry.cancelled = true;
    this.wake();
  }

  async softDelete(streamId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    if (!this.state.record) throw new Error("Stream not found");
    this.state.record = mergeRecord(this.state.record, { lifecycle: { softDeleted: true } });
    this.wake();
  }

  async addChildEdge(streamId: StreamId, childId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    this.state.children.add(childId);
  }

  async dropChildEdge(streamId: StreamId, childId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    this.state.children.delete(childId);
  }

  async countChildEdges(streamId: StreamId): Promise<number> {
    this.ensureInit(streamId);
    return this.state.children.size;
  }

  async awaitChange(streamId: StreamId, options: AwaitChangeOptions): Promise<AwaitChangeResult> {
    this.ensureInit(streamId);
    this.state.awaitOptions.push(options);
    const start = Date.now();
    while (true) {
      const snapshot = buildChangeSnapshot(this.state.record);
      if (changeSnapshotDiffers(snapshot, options)) return { status: "changed", snapshot };
      const remaining = options.timeoutMs - (Date.now() - start);
      if (remaining <= 0) return { status: "timeout", snapshot };
      await this.waitForWake(remaining);
    }
  }

  private waitForWake(timeoutMs: number): Promise<void> {
    return new Promise<void>((resolve) => {
      const finish = () => {
        clearTimeout(timer);
        this.waiters.delete(finish);
        resolve();
      };
      const timer = setTimeout(finish, timeoutMs);
      this.waiters.add(finish);
    });
  }

  private wake(): void {
    const snapshot = [...this.waiters];
    this.waiters.clear();
    for (const finish of snapshot) finish();
  }

  async scheduleExpiry(streamId: StreamId, at: number): Promise<void> {
    this.ensureInit(streamId);
    this.state.expiry.at = at;
    this.state.expiry.cancelled = false;
  }

  async cancelExpiry(streamId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    this.state.expiry.cancelled = true;
  }
}

function createFakeNamespace() {
  const stubs = new Map<string, FakeStub>();
  const ids = new Map<string, { name: string; toString(): string }>();

  function ensureStub(name: string): FakeStub {
    let stub = stubs.get(name);
    if (!stub) {
      stub = new FakeStub({
        initCalls: [],
        record: null,
        messages: [],
        producers: new Map(),
        children: new Set(),
        expiry: { cancelled: false },
        awaitOptions: [],
      });
      stubs.set(name, stub);
    }
    return stub;
  }

  const namespace = {
    idFromName(name: string) {
      let id = ids.get(name);
      if (!id) {
        id = { name, toString: () => name };
        ids.set(name, id);
      }
      return id;
    },
    get(id: { name: string }) {
      return ensureStub(id.name);
    },
  };

  return {
    namespace: namespace as unknown as FactoryNamespace,
    stubFor: (name: string) => ensureStub(name),
    has: (name: string) => stubs.has(name),
  };
}

function newRecord(id: string, forkedFrom?: string): StreamRecord {
  return {
    id,
    config: { contentType: CONTENT_TYPE, createdAt: 0 },
    lifecycle: { forkedFrom, forkOffset: forkedFrom ? "0_0" : undefined },
    currentOffset: "0_0",
    counter: 0,
  };
}

function mergeRecord(record: StreamRecord, patch: StreamRecordPatch): StreamRecord {
  return {
    ...record,
    config: { ...record.config, ...patch.config },
    lifecycle: { ...record.lifecycle, ...patch.lifecycle },
    currentOffset: patch.currentOffset ?? record.currentOffset,
    counter: patch.counter ?? record.counter,
  };
}

function producerStatesEqual(
  left: ProducerState | undefined,
  right: ProducerState | undefined,
): boolean {
  if (!left || !right) return left === right;
  return left.epoch === right.epoch && left.lastSeq === right.lastSeq;
}

describe("createDurableObjectStorageAdapter", () => {
  it("self-initializes the routed stub on the first per-stream call", async () => {
    const fake = createFakeNamespace();
    const adapter = createDurableObjectStorageAdapter({ namespace: fake.namespace });

    expect(await adapter.getRecord("alpha")).toBeNull();
    expect(fake.stubFor("alpha").state.boundId).toBe("alpha");
    expect(fake.stubFor("alpha").state.initCalls).toEqual(["alpha"]);
  });

  it("routes create, append, read, producer, awaitChange, and expiry to the bound stub", async () => {
    const fake = createFakeNamespace();
    const adapter = createDurableObjectStorageAdapter({ namespace: fake.namespace });

    const created = await adapter.create({ record: newRecord("alpha") });
    expect(created.status).toBe("created");

    const appended = await adapter.append("alpha", {
      preconditions: {
        expectedOffset: "0_0",
        producer: { producerId: "p1", expected: undefined, next: { epoch: 0, lastSeq: 0 } },
      },
      messages: [{ data: bytes("hello"), offset: "1_0", timestamp: 1 }],
      recordPatch: { currentOffset: "1_0", counter: 1 },
    });
    expect(appended.status).toBe("appended");
    expect(await adapter.getRecord("alpha")).toMatchObject({ id: "alpha", currentOffset: "1_0" });
    expect(text((await adapter.listMessages("alpha"))[0]!.data)).toBe("hello");
    expect(await adapter.getProducerState("alpha", "p1")).toEqual({ epoch: 0, lastSeq: 0 });

    // awaitChange routes to the stub and wakes on a later append. Its options are
    // plain, serializable data (no AbortSignal).
    const waiting = adapter.awaitChange!("alpha", { fromOffset: "1_0", timeoutMs: 1_000 });
    await new Promise((resolve) => setTimeout(resolve, 0));
    const advanced = await adapter.append("alpha", {
      preconditions: { expectedOffset: "1_0" },
      messages: [{ data: bytes("world"), offset: "2_0", timestamp: 2 }],
      recordPatch: { currentOffset: "2_0", counter: 2 },
    });
    expect(advanced.status).toBe("appended");
    const changed = await waiting;
    expect(changed.status).toBe("changed");
    expect(changed.snapshot).toMatchObject({ present: true, currentOffset: "2_0", closed: false });
    expect(fake.stubFor("alpha").state.awaitOptions).toContainEqual({
      fromOffset: "1_0",
      timeoutMs: 1_000,
    });

    await adapter.scheduleExpiry("alpha", 123_456);
    expect(fake.stubFor("alpha").state.expiry).toEqual({ at: 123_456, cancelled: false });
    await adapter.cancelExpiry("alpha");
    expect(fake.stubFor("alpha").state.expiry.cancelled).toBe(true);
  });

  it("awaitChange returns changed immediately when state already advanced, else times out", async () => {
    const fake = createFakeNamespace();
    const adapter = createDurableObjectStorageAdapter({ namespace: fake.namespace });
    await adapter.create({ record: newRecord("alpha") });
    await adapter.append("alpha", {
      preconditions: { expectedOffset: "0_0" },
      messages: [{ data: bytes("a"), offset: "1_0", timestamp: 1 }],
      recordPatch: { currentOffset: "1_0", counter: 1 },
    });

    const immediate = await adapter.awaitChange!("alpha", { fromOffset: "0_0", timeoutMs: 1_000 });
    expect(immediate.status).toBe("changed");

    const timed = await adapter.awaitChange!("alpha", { fromOffset: "1_0", timeoutMs: 10 });
    expect(timed.status).toBe("timeout");
    if (timed.status !== "timeout") throw new Error("expected timeout");
    expect(timed.snapshot).toMatchObject({ present: true, currentOffset: "1_0" });
  });

  it("routes operations for different ids to different self-initialized stubs", async () => {
    const fake = createFakeNamespace();
    const adapter = createDurableObjectStorageAdapter({ namespace: fake.namespace });

    await adapter.create({
      record: newRecord("a"),
      initialMessages: [{ data: bytes("a-1"), offset: "1_0", timestamp: 1 }],
    });
    await adapter.create({
      record: newRecord("b"),
      initialMessages: [{ data: bytes("b-1"), offset: "1_0", timestamp: 1 }],
    });

    expect(fake.stubFor("a").state.initCalls).toEqual(["a"]);
    expect(fake.stubFor("b").state.initCalls).toEqual(["b"]);
    expect((await adapter.listMessages("a")).map((m) => text(m.data))).toEqual(["a-1"]);
    expect((await adapter.listMessages("b")).map((m) => text(m.data))).toEqual(["b-1"]);
  });

  it("uses create/fork/delete verbs with parent-owned lineage edges", async () => {
    const fake = createFakeNamespace();
    const adapter = createDurableObjectStorageAdapter({ namespace: fake.namespace });

    await adapter.create({ record: newRecord("parent") });
    const forked = await adapter.fork?.({
      child: newRecord("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: "0_0" },
    });
    expect(forked?.status).toBe("created");
    expect(await fake.stubFor("parent").countChildEdges("parent")).toBe(1);

    const retained = await adapter.delete({ streamId: "parent", reason: "delete" });
    expect(retained.status).toBe("retained-soft-deleted");
    expect(fake.stubFor("parent").state.record?.lifecycle.softDeleted).toBe(true);

    const purged = await adapter.delete({ streamId: "child", reason: "delete" });
    expect(purged.status).toBe("purged");
    expect(fake.stubFor("parent").state.record).toBeNull();
  });

  it("does not add a parent edge when fork finds an unrelated existing child id", async () => {
    const fake = createFakeNamespace();
    const adapter = createDurableObjectStorageAdapter({ namespace: fake.namespace });
    await adapter.create({ record: newRecord("parent") });
    await adapter.create({ record: newRecord("child", "other-parent") });

    const conflict = await adapter.fork?.({
      child: newRecord("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: "0_0" },
    });
    expect(conflict?.status).toBe("exists");
    expect(await fake.stubFor("parent").countChildEdges("parent")).toBe(0);
  });

  it("re-converges a missing parent edge when fork is retried after child create", async () => {
    const fake = createFakeNamespace();
    const adapter = createDurableObjectStorageAdapter({ namespace: fake.namespace });
    await adapter.create({ record: newRecord("parent") });
    await adapter.create({ record: newRecord("child", "parent") });
    expect(await fake.stubFor("parent").countChildEdges("parent")).toBe(0);

    const retried = await adapter.fork?.({
      child: newRecord("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: "0_0" },
    });
    expect(retried?.status).toBe("exists");
    expect(await fake.stubFor("parent").countChildEdges("parent")).toBe(1);

    await adapter.fork?.({
      child: newRecord("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: "0_0" },
    });
    expect(await fake.stubFor("parent").countChildEdges("parent")).toBe(1);
  });

  it("does not create stubs for ids that are never used", async () => {
    const fake = createFakeNamespace();
    const adapter = createDurableObjectStorageAdapter({ namespace: fake.namespace });
    void adapter;
    expect(fake.has("anything")).toBe(false);
  });

  it("forks at a binary sub-offset, materializing the prefix into the child", async () => {
    const fake = createFakeNamespace();
    const adapter = createDurableObjectStorageAdapter({ namespace: fake.namespace });
    const protocol = new StreamProtocol({ storage: { adapter } });

    const src = await protocol.create("src", {
      contentType: "text/plain",
      initialData: bytes("hello"),
    });
    expect(src.status).toBe("created");

    const fork = await protocol.create("fork", {
      contentType: "text/plain",
      forkedFrom: "src",
      forkOffset: ZERO_OFFSET,
      forkSubOffset: 3,
    });
    expect(fork.status).toBe("created");
    if (fork.status !== "created") throw new Error("expected fork created");

    const read = await fork.stream.read({});
    if (read.status !== "ok") throw new Error("expected read ok");
    expect(read.messages.map((m) => text(m.data)).join("")).toBe("hel");
    expect(fake.stubFor("fork").state.record?.lifecycle.forkSubOffset).toBe(3);

    const mismatch = await protocol.create("fork", {
      contentType: "text/plain",
      forkedFrom: "src",
      forkOffset: ZERO_OFFSET,
      forkSubOffset: 2,
    });
    expect(mismatch.status).toBe("conflict");
  });
});
