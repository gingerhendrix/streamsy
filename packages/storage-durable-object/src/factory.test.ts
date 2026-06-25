import { describe, expect, it } from "vitest";
import type {
  CommitResult,
  ListMessagesOptions,
  MutationPlan,
  ProducerState,
  StoredMessage,
  StreamEventType,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
  WaitForEventOptions,
  WaitForEventResult,
} from "@streamsy/core";
import {
  createDurableObjectStreamFactory,
  type DurableObjectStreamFactoryOptions,
} from "./factory.ts";

type FactoryNamespace = DurableObjectStreamFactoryOptions["namespace"];

const CONTENT_TYPE = "application/octet-stream";

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function text(value: Uint8Array): string {
  return new TextDecoder().decode(value);
}

interface FakeStubState {
  initializedAs?: StreamId;
  initCalls: StreamId[];
  record: StreamRecord | null;
  messages: StoredMessage[];
  producers: Map<string, ProducerState>;
  children: Set<StreamId>;
  expiry: { at?: number; cancelled: boolean };
  waitOptions: WaitForEventOptions[];
  notifications: StreamEventType[];
}

class FakeStub {
  readonly state: FakeStubState;
  private waiters = new Set<(result: WaitForEventResult) => void>();

  constructor(state: FakeStubState) {
    this.state = state;
  }

  get id(): StreamId {
    if (!this.state.initializedAs) throw new Error("Durable Object stream is not initialized");
    return this.state.initializedAs;
  }

  async init(streamId: StreamId): Promise<void> {
    this.state.initCalls.push(streamId);
    if (this.state.initializedAs && this.state.initializedAs !== streamId) {
      throw new Error(`Durable Object already initialized for stream ${this.state.initializedAs}`);
    }
    this.state.initializedAs = streamId;
  }

  async getRecord(): Promise<StreamRecord | null> {
    return this.state.record;
  }

  async commit(plan: MutationPlan): Promise<CommitResult> {
    let record = this.state.record;
    if (plan.createRecord) {
      if (plan.createRecord.id !== this.id)
        throw new Error(`Record id ${plan.createRecord.id} does not match bound stream ${this.id}`);
      if (record) return { status: "precondition-failed", record };
      this.state.record = plan.createRecord;
      record = plan.createRecord;
    }
    if (!record) return { status: "precondition-failed", record: null };
    if (
      plan.preconditions.expectedOffset !== undefined &&
      plan.preconditions.expectedOffset !== record.currentOffset
    )
      return { status: "precondition-failed", record };
    if (
      plan.preconditions.expectedClosed !== undefined &&
      plan.preconditions.expectedClosed !== (record.lifecycle.closed === true)
    )
      return { status: "precondition-failed", record };

    const producer = plan.preconditions.producer;
    if (producer) {
      const current = this.state.producers.get(producer.producerId);
      if (!producerStatesEqual(current, producer.expected))
        return { status: "precondition-failed", record };
    }

    if (plan.appendMessages) this.state.messages.push(...plan.appendMessages);
    let updated = record;
    if (plan.recordPatch) {
      updated = mergeRecord(record, plan.recordPatch);
      this.state.record = updated;
    }
    if (producer) this.state.producers.set(producer.producerId, producer.next);
    return { status: "committed", record: updated };
  }

  async listMessages(options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    let out = this.state.messages;
    if (options.after) out = out.filter((m) => m.offset > options.after!);
    if (options.until) out = out.filter((m) => m.offset <= options.until!);
    if (options.limit !== undefined) out = out.slice(0, options.limit);
    return out;
  }

  async getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.state.producers.get(producerId);
  }

  async purgeSelf(): Promise<void> {
    this.state.record = null;
    this.state.messages = [];
    this.state.producers.clear();
    this.state.children.clear();
    this.state.expiry.cancelled = true;
  }

  async softDelete(): Promise<void> {
    if (!this.state.record) throw new Error("Stream not found");
    this.state.record = mergeRecord(this.state.record, { lifecycle: { softDeleted: true } });
  }

  async addChildEdge(childId: StreamId): Promise<void> {
    this.state.children.add(childId);
  }

  async dropChildEdge(childId: StreamId): Promise<void> {
    this.state.children.delete(childId);
  }

  async countChildEdges(): Promise<number> {
    return this.state.children.size;
  }

  async waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    this.state.waitOptions.push(options);
    return new Promise<WaitForEventResult>((resolve) => {
      const timer = setTimeout(() => {
        this.waiters.delete(wrapped);
        resolve({ status: "timeout" });
      }, options.timeoutMs);
      const wrapped = (result: WaitForEventResult) => {
        clearTimeout(timer);
        resolve(result);
      };
      this.waiters.add(wrapped);
    });
  }

  notify(type: StreamEventType): void {
    this.state.notifications.push(type);
    const snapshot = [...this.waiters];
    this.waiters.clear();
    for (const resolve of snapshot) resolve({ status: "notified", type });
  }

  async scheduleExpiry(at: number): Promise<void> {
    this.state.expiry.at = at;
    this.state.expiry.cancelled = false;
  }

  async cancelExpiry(): Promise<void> {
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
        waitOptions: [],
        notifications: [],
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

describe("createDurableObjectStreamFactory", () => {
  it("initializes the routed Durable Object stub and returns a typed stream proxy", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });

    const stream = await factory.getStream("alpha");

    expect(stream).not.toBe(fake.stubFor("alpha"));
    expect(stream.id).toBe("alpha");
    expect(await stream.getRecord()).toBeNull();
    expect(fake.stubFor("alpha").state.initCalls).toEqual(["alpha"]);
  });

  it("routes commit, read, producer, notify, and expiry operations to the bound stub", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });
    const stream = await factory.getStream("alpha");

    const created = await stream.commit({
      createRecord: newRecord("alpha"),
      preconditions: {
        producer: {
          producerId: "p1",
          expected: undefined,
          next: { epoch: 0, lastSeq: 0 },
        },
      },
      appendMessages: [{ data: bytes("hello"), offset: "1_0", timestamp: 1 }],
      recordPatch: { currentOffset: "1_0", counter: 1 },
    });
    expect(created.status).toBe("committed");
    expect(await stream.getRecord()).toMatchObject({ id: "alpha", currentOffset: "1_0" });
    expect(text((await stream.listMessages())[0]!.data)).toBe("hello");
    expect(await stream.getProducerState("p1")).toEqual({ epoch: 0, lastSeq: 0 });

    const waiting = stream.waitForEvent({ timeoutMs: 1_000 });
    await new Promise((resolve) => setTimeout(resolve, 0));
    await stream.notify("message");
    await expect(waiting).resolves.toEqual({ status: "notified", type: "message" });

    await stream.scheduleExpiry(123_456);
    expect(fake.stubFor("alpha").state.expiry).toEqual({ at: 123_456, cancelled: false });
    await stream.cancelExpiry();
    expect(fake.stubFor("alpha").state.expiry.cancelled).toBe(true);
  });

  it("routes operations for different ids to different initialized stubs", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });

    const a = await factory.getStream("a");
    const b = await factory.getStream("b");
    await a.commit({
      createRecord: newRecord("a"),
      preconditions: {},
      appendMessages: [{ data: bytes("a-1"), offset: "1_0", timestamp: 1 }],
    });
    await b.commit({
      createRecord: newRecord("b"),
      preconditions: {},
      appendMessages: [{ data: bytes("b-1"), offset: "1_0", timestamp: 1 }],
    });

    expect(fake.stubFor("a").state.initCalls).toEqual(["a"]);
    expect(fake.stubFor("b").state.initCalls).toEqual(["b"]);
    expect((await a.listMessages()).map((m) => text(m.data))).toEqual(["a-1"]);
    expect((await b.listMessages()).map((m) => text(m.data))).toEqual(["b-1"]);
  });

  it("uses factory create/fork/delete verbs with parent-owned lineage edges", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });

    await factory.create({ record: newRecord("parent") });
    const forked = await factory.fork?.({
      child: newRecord("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: "0_0" },
    });
    expect(forked?.status).toBe("created");
    expect(await fake.stubFor("parent").countChildEdges()).toBe(1);

    const retained = await factory.delete({ streamId: "parent", reason: "delete" });
    expect(retained.status).toBe("retained-soft-deleted");
    expect(fake.stubFor("parent").state.record?.lifecycle.softDeleted).toBe(true);

    const purged = await factory.delete({ streamId: "child", reason: "delete" });
    expect(purged.status).toBe("purged");
    expect(fake.stubFor("parent").state.record).toBeNull();
  });

  it("does not add a parent edge when fork finds an unrelated existing child id", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });
    await factory.create({ record: newRecord("parent") });

    const child = await factory.getStream("child");
    await child.commit({ createRecord: newRecord("child", "other-parent"), preconditions: {} });

    const conflict = await factory.fork?.({
      child: newRecord("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: "0_0" },
    });
    expect(conflict?.status).toBe("exists");
    expect(await fake.stubFor("parent").countChildEdges()).toBe(0);
  });

  it("re-converges a missing parent edge when fork is retried after child create", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });
    await factory.create({ record: newRecord("parent") });

    const child = await factory.getStream("child");
    await child.commit({ createRecord: newRecord("child", "parent"), preconditions: {} });
    expect(await fake.stubFor("parent").countChildEdges()).toBe(0);

    const retried = await factory.fork?.({
      child: newRecord("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: "0_0" },
    });
    expect(retried?.status).toBe("exists");
    expect(await fake.stubFor("parent").countChildEdges()).toBe(1);

    await factory.fork?.({
      child: newRecord("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: "0_0" },
    });
    expect(await fake.stubFor("parent").countChildEdges()).toBe(1);
  });

  it("does not create stubs for ids that are never used", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });
    void factory;
    expect(fake.has("anything")).toBe(false);
  });
});
