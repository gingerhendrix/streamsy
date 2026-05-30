import { describe, it, expect } from "vitest";
import type {
  ListMessagesOptions,
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
  lockEvents: { type: "acquire" | "release"; key: string; token?: string }[];
  expiry: { at?: number; cancelled: boolean };
  waitOptions: WaitForEventOptions[];
  notifications: StreamEventType[];
}

/** Minimal in-process stand-in for a DurableObjectStub exposing storage RPC methods. */
class FakeStub {
  readonly state: FakeStubState;
  private nextToken = 0;
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

  async createRecord(record: StreamRecord) {
    if (record.id !== this.id)
      throw new Error(`Record id ${record.id} does not match bound stream ${this.id}`);
    if (this.state.record) return { status: "exists" as const, record: this.state.record };
    this.state.record = record;
    return { status: "created" as const };
  }

  async updateRecord(patch: StreamRecordPatch): Promise<StreamRecord> {
    if (!this.state.record) throw new Error("Stream not found");
    this.state.record = {
      ...this.state.record,
      config: { ...this.state.record.config, ...patch.config },
      lifecycle: { ...this.state.record.lifecycle, ...patch.lifecycle },
      currentOffset: patch.currentOffset ?? this.state.record.currentOffset,
      counter: patch.counter ?? this.state.record.counter,
    };
    return this.state.record;
  }

  async deleteRecord(): Promise<void> {
    this.state.record = null;
    this.state.messages = [];
    this.state.producers.clear();
    this.state.expiry.cancelled = true;
  }

  async appendMessages(messages: StoredMessage[]): Promise<void> {
    if (!this.state.record) throw new Error("Stream not found");
    this.state.messages.push(...messages);
  }

  async listMessages(options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    let out = this.state.messages;
    if (options.after) out = out.filter((m) => m.offset > options.after!);
    if (options.until) out = out.filter((m) => m.offset <= options.until!);
    if (options.limit !== undefined) out = out.slice(0, options.limit);
    return out;
  }

  async deleteMessages(): Promise<void> {
    this.state.messages = [];
  }

  async getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.state.producers.get(producerId);
  }

  async setProducerState(producerId: string, state: ProducerState): Promise<void> {
    if (!this.state.record) throw new Error("Stream not found");
    this.state.producers.set(producerId, state);
  }

  async deleteProducerStates(): Promise<void> {
    this.state.producers.clear();
  }

  async incrementChildRefCount(): Promise<number> {
    if (!this.state.record) throw new Error("Stream not found");
    const next = this.state.record.lifecycle.childRefCount + 1;
    this.state.record = {
      ...this.state.record,
      lifecycle: { ...this.state.record.lifecycle, childRefCount: next },
    };
    return next;
  }

  async decrementChildRefCount(): Promise<number> {
    if (!this.state.record) throw new Error("Stream not found");
    const next = Math.max(0, this.state.record.lifecycle.childRefCount - 1);
    this.state.record = {
      ...this.state.record,
      lifecycle: { ...this.state.record.lifecycle, childRefCount: next },
    };
    return next;
  }

  async acquireLock(key: string): Promise<string> {
    const token = `t${++this.nextToken}`;
    this.state.lockEvents.push({ type: "acquire", key, token });
    return token;
  }

  async releaseLock(key: string, token: string): Promise<void> {
    this.state.lockEvents.push({ type: "release", key, token });
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
        lockEvents: [],
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

function newRecord(id: string): StreamRecord {
  return {
    id,
    config: { contentType: CONTENT_TYPE, createdAt: 0 },
    lifecycle: { childRefCount: 0 },
    currentOffset: "0_0",
    counter: 0,
  };
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

  it("routes record/message/producer/reference operations to the bound stub", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });
    const stream = await factory.getStream("alpha");

    expect(await stream.createRecord(newRecord("alpha"))).toEqual({ status: "created" });
    expect(await stream.getRecord()).toMatchObject({ id: "alpha" });
    await expect(stream.createRecord(newRecord("wrong"))).rejects.toThrow(
      "Record id wrong does not match bound stream alpha",
    );

    await stream.appendMessages([{ data: bytes("hello"), offset: "1_0", timestamp: 1 }]);
    const messages = await stream.listMessages();
    expect(messages).toHaveLength(1);
    expect(text(messages[0]!.data)).toBe("hello");

    await stream.setProducerState("p1", { epoch: 0, lastSeq: 0 });
    expect(await stream.getProducerState("p1")).toEqual({ epoch: 0, lastSeq: 0 });
    await stream.deleteProducerStates();
    expect(await stream.getProducerState("p1")).toBeUndefined();

    expect(await stream.incrementChildRefCount()).toBe(1);
    expect(await stream.incrementChildRefCount()).toBe(2);
    expect(await stream.decrementChildRefCount()).toBe(1);

    await stream.deleteMessages();
    expect(await stream.listMessages()).toEqual([]);

    await stream.deleteRecord();
    expect(await stream.getRecord()).toBeNull();
  });

  it("routes operations for different ids to different initialized stubs", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });

    const a = await factory.getStream("a");
    const b = await factory.getStream("b");
    await a.createRecord(newRecord("a"));
    await b.createRecord(newRecord("b"));
    await a.appendMessages([{ data: bytes("a-1"), offset: "1_0", timestamp: 1 }]);
    await b.appendMessages([{ data: bytes("b-1"), offset: "1_0", timestamp: 1 }]);

    expect(fake.stubFor("a").state.initCalls).toEqual(["a"]);
    expect(fake.stubFor("b").state.initCalls).toEqual(["b"]);
    expect((await a.listMessages()).map((m) => text(m.data))).toEqual(["a-1"]);
    expect((await b.listMessages()).map((m) => text(m.data))).toEqual(["b-1"]);
  });

  it("uses a local mutation lock helper on the returned stub", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });
    const stream = await factory.getStream("locked");

    const result = await stream.withMutationLock(async () => "value");
    expect(result).toBe("value");
    const events = fake.stubFor("locked").state.lockEvents;
    expect(events).toHaveLength(2);
    expect(events[0]).toMatchObject({ type: "acquire", key: "stream:locked" });
    expect(events[1]).toMatchObject({ type: "release", key: "stream:locked" });
    expect(events[0]!.token).toBe(events[1]!.token);
  });

  it("releases the local mutation lock helper even when the callback throws", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });
    const stream = await factory.getStream("locked");

    await expect(
      stream.withMutationLock(async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    expect(fake.stubFor("locked").state.lockEvents.map((e) => e.type)).toEqual([
      "acquire",
      "release",
    ]);
  });

  it("routes live-read notification through the initialized stub", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });
    const stream = await factory.getStream("notify");

    const waiting = stream.waitForEvent({ timeoutMs: 1_000 });
    await new Promise((resolve) => setTimeout(resolve, 0));
    await stream.notify("message");
    await expect(waiting).resolves.toEqual({ status: "notified", type: "message" });
    expect(fake.stubFor("notify").state.notifications).toEqual(["message"]);
  });

  it("routes active expiry scheduling through the initialized stub", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });
    const stream = await factory.getStream("expiry");

    await stream.scheduleExpiry(123_456);
    expect(fake.stubFor("expiry").state.expiry).toEqual({ at: 123_456, cancelled: false });

    await stream.cancelExpiry();
    expect(fake.stubFor("expiry").state.expiry.cancelled).toBe(true);
  });

  it("does not create stubs for ids that are never used", async () => {
    const fake = createFakeNamespace();
    const factory = createDurableObjectStreamFactory({ namespace: fake.namespace });
    void factory;
    expect(fake.has("anything")).toBe(false);
  });
});
