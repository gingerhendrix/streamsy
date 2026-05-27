import { describe, expect, it } from "vitest";
import type { StoredMessage, StreamRecord, StreamStoreAdapter } from "../types/storage.ts";
import {
  isNotSupported,
  notSupported,
  type NotSupportedResult,
  type Stream,
} from "../types/factory.ts";
import { composeStream } from "./compose-stream.ts";
import { createStreamFactoryFromAdapter } from "./adapter-stream-factory.ts";

function makeRecord(id: string): StreamRecord {
  return {
    id,
    config: { contentType: "application/octet-stream", createdAt: 0 },
    lifecycle: { childRefCount: 0 },
    currentOffset: "0_0",
    counter: 0,
  };
}

function makeMessage(offset: string): StoredMessage {
  return { data: new Uint8Array([1]), offset, timestamp: 0 };
}

describe("composeStream", () => {
  it("binds record and message operations to the stream id", async () => {
    const calls: string[] = [];
    const stream = composeStream({
      id: "abc",
      recordStore: {
        getRecord: async () => {
          calls.push("get");
          return makeRecord("abc");
        },
        createRecord: async () => {
          calls.push("create");
          return { status: "created" };
        },
        updateRecord: async () => {
          calls.push("update");
          return makeRecord("abc");
        },
        deleteRecord: async () => {
          calls.push("delete-record");
        },
      },
      messageStore: {
        appendMessages: async () => {
          calls.push("append");
        },
        listMessages: async () => {
          calls.push("list");
          return [makeMessage("1_0")];
        },
        deleteMessages: async () => {
          calls.push("delete-messages");
        },
      },
    });

    expect(stream.id).toBe("abc");
    const record = await stream.getRecord();
    expect(record?.id).toBe("abc");
    await stream.createRecord(makeRecord("abc"));
    await stream.updateRecord({ counter: 1 });
    await stream.appendMessages([makeMessage("1_0")]);
    const messages = await stream.listMessages();
    expect(messages).toHaveLength(1);
    await stream.deleteMessages();
    await stream.deleteRecord();

    expect(calls).toEqual([
      "get",
      "create",
      "update",
      "append",
      "list",
      "delete-messages",
      "delete-record",
    ]);
  });

  it("surfaces optional dependencies only when supplied", () => {
    const minimal = composeStream({
      id: "min",
      recordStore: emptyRecordStore(),
      messageStore: emptyMessageStore(),
    });
    expect(minimal.producers).toBeUndefined();
    expect(minimal.references).toBeUndefined();
    expect(minimal.mutations).toBeUndefined();
    expect(minimal.events).toBeUndefined();
    expect(minimal.expiry).toBeUndefined();

    const full = composeStream({
      id: "full",
      recordStore: emptyRecordStore(),
      messageStore: emptyMessageStore(),
      producerStore: {
        getProducerState: async () => undefined,
        setProducerState: async () => {},
        deleteProducerStates: async () => {},
      },
      referenceTracker: {
        incrementChildRefCount: async () => 1,
        decrementChildRefCount: async () => 0,
      },
      mutations: { withMutationLock: async (fn) => fn() },
      events: { waitForEvent: async () => ({ status: "timeout" }), notify: () => {} },
      expiry: { scheduleExpiry: () => {}, cancelExpiry: () => {} },
    });
    expect(full.producers).toBeDefined();
    expect(full.references).toBeDefined();
    expect(full.mutations).toBeDefined();
    expect(full.events).toBeDefined();
    expect(full.expiry).toBeDefined();
  });
});

describe("createStreamFactoryFromAdapter", () => {
  it("binds adapter calls to the requested stream id", async () => {
    const adapter = makeAdapterSpy();
    const factory = createStreamFactoryFromAdapter(adapter.adapter);
    const stream = (await factory.getStream("s1")) as Stream;

    expect(stream.id).toBe("s1");

    await stream.getRecord();
    await stream.createRecord(makeRecord("s1"));
    await stream.updateRecord({ counter: 2 });
    await stream.deleteRecord();
    await stream.appendMessages([makeMessage("1_0")]);
    await stream.listMessages({ limit: 5 });
    await stream.deleteMessages();
    await stream.producers!.getProducerState("p");
    await stream.producers!.setProducerState("p", { epoch: 1, lastSeq: 0 });
    await stream.producers!.deleteProducerStates();
    await stream.references!.incrementChildRefCount();
    await stream.references!.decrementChildRefCount();

    expect(adapter.calls).toEqual([
      ["get", "s1"],
      ["create", "s1"],
      ["update", "s1"],
      ["delete", "s1"],
      ["append", "s1"],
      ["list", "s1"],
      ["deleteMessages", "s1"],
      ["getProducerState", "s1"],
      ["setProducerState", "s1"],
      ["deleteProducerStates", "s1"],
      ["incrementChildRefCount", "s1"],
      ["decrementChildRefCount", "s1"],
    ]);
  });

  it("forwards mutation locks under the per-stream lock key", async () => {
    let lockKey: string | undefined;
    const adapter: StreamStoreAdapter = {
      ...emptyAdapter(),
      withLock: async (key, fn) => {
        lockKey = key;
        return fn();
      },
    };
    const stream = await createStreamFactoryFromAdapter(adapter).getStream("s2");
    const result = await stream.mutations!.withMutationLock(async () => "ok");
    expect(lockKey).toBe("stream:s2");
    expect(result).toBe("ok");
  });

  it("omits optional members when the underlying adapter does not implement them", async () => {
    const adapter = emptyAdapter();
    delete (adapter as { withLock?: unknown }).withLock;
    delete (adapter as { waitForEvent?: unknown }).waitForEvent;
    delete (adapter as { notify?: unknown }).notify;
    delete (adapter as { scheduleExpiry?: unknown }).scheduleExpiry;
    delete (adapter as { cancelExpiry?: unknown }).cancelExpiry;
    const stream = await createStreamFactoryFromAdapter(adapter).getStream("s3");
    expect(stream.mutations).toBeUndefined();
    expect(stream.events).toBeUndefined();
    expect(stream.expiry).toBeUndefined();
  });
});

describe("NotSupportedResult helpers", () => {
  it("constructs results with and without a message", () => {
    expect(notSupported("fork")).toEqual({ status: "not-supported", feature: "fork" });
    expect(notSupported("fork", "disabled by config")).toEqual({
      status: "not-supported",
      feature: "fork",
      message: "disabled by config",
    });
  });

  it("narrows ambient union results", () => {
    const value: { status: "ok" } | NotSupportedResult = notSupported("live-read");
    expect(isNotSupported(value)).toBe(true);
    if (isNotSupported(value)) {
      expect(value.feature).toBe("live-read");
    }
    expect(isNotSupported({ status: "ok" })).toBe(false);
    expect(isNotSupported(null)).toBe(false);
    expect(isNotSupported(undefined)).toBe(false);
    expect(isNotSupported("string")).toBe(false);
  });
});

function emptyRecordStore() {
  return {
    getRecord: async () => null,
    createRecord: async () => ({ status: "created" as const }),
    updateRecord: async () => makeRecord("x"),
    deleteRecord: async () => {},
  };
}

function emptyMessageStore() {
  return {
    appendMessages: async () => {},
    listMessages: async () => [],
    deleteMessages: async () => {},
  };
}

function emptyAdapter(): StreamStoreAdapter {
  return {
    get: async () => null,
    create: async () => ({ status: "created" }),
    update: async (id) => makeRecord(id),
    delete: async () => {},
    append: async () => {},
    list: async () => [],
    deleteMessages: async () => {},
    getProducerState: async () => undefined,
    setProducerState: async () => {},
    deleteProducerStates: async () => {},
    incrementChildRefCount: async () => 1,
    decrementChildRefCount: async () => 0,
    withLock: async (_key, fn) => fn(),
    waitForEvent: async () => ({ status: "timeout" }),
    notify: () => {},
    scheduleExpiry: () => {},
    cancelExpiry: () => {},
  };
}

function makeAdapterSpy(): {
  adapter: StreamStoreAdapter;
  calls: Array<[string, string]>;
} {
  const calls: Array<[string, string]> = [];
  const record = (method: string) => (id: string) => {
    calls.push([method, id]);
  };
  const base = emptyAdapter();
  return {
    calls,
    adapter: {
      ...base,
      get: async (id) => {
        record("get")(id);
        return base.get(id);
      },
      create: async (rec) => {
        record("create")(rec.id);
        return base.create(rec);
      },
      update: async (id, patch) => {
        record("update")(id);
        return base.update(id, patch);
      },
      delete: async (id) => {
        record("delete")(id);
      },
      append: async (id, messages) => {
        record("append")(id);
        await base.append(id, messages);
      },
      list: async (id, options) => {
        record("list")(id);
        return base.list(id, options);
      },
      deleteMessages: async (id) => {
        record("deleteMessages")(id);
      },
      getProducerState: async (id, producerId) => {
        record("getProducerState")(id);
        return base.getProducerState(id, producerId);
      },
      setProducerState: async (id, producerId, state) => {
        record("setProducerState")(id);
        await base.setProducerState(id, producerId, state);
      },
      deleteProducerStates: async (id) => {
        record("deleteProducerStates")(id);
      },
      incrementChildRefCount: async (id) => {
        record("incrementChildRefCount")(id);
        return base.incrementChildRefCount(id);
      },
      decrementChildRefCount: async (id) => {
        record("decrementChildRefCount")(id);
        return base.decrementChildRefCount(id);
      },
    },
  };
}
