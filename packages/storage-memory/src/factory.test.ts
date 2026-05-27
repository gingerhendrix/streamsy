/**
 * Unit coverage for the native memory `StreamFactory`.
 *
 * Asserts that:
 * - the factory returns protocol-facing `Stream` objects bound to one id;
 * - bound record/message/producer/reference operations target the right
 *   underlying entry without exposing a public dependency bag;
 * - the factory and the adapter can share the same in-process state, which
 *   keeps parity with the existing `MemoryStreamStore` data model;
 * - the per-stream mutation lock serializes overlapping callers;
 * - live-read notification and active expiry are routed through the
 *   composed `Stream`;
 * - composing without a producer store leaves `stream.producers`
 *   undefined, demonstrating the optionality story for adapter authors.
 */
import { describe, it, expect } from "vitest";
import { composeStream, StreamProtocol, type Stream } from "@streamsy/core";
import { createMemoryStreamFactory, MemoryStreamStore } from "./index.ts";

const CONTENT_TYPE = "application/octet-stream";

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function text(value: Uint8Array): string {
  return new TextDecoder().decode(value);
}

describe("createMemoryStreamFactory", () => {
  it("returns a Stream bound to the requested id and persists round-trips", async () => {
    const factory = createMemoryStreamFactory();
    const stream = (await factory.getStream("alpha")) as Stream;
    expect(stream.id).toBe("alpha");

    const create = await stream.createRecord({
      id: "alpha",
      config: { contentType: CONTENT_TYPE, createdAt: 0 },
      lifecycle: { childRefCount: 0 },
      currentOffset: "0_0",
      counter: 0,
    });
    expect(create.status).toBe("created");

    const record = await stream.getRecord();
    expect(record?.id).toBe("alpha");

    await stream.appendMessages([{ data: bytes("hello"), offset: "1_0", timestamp: 1 }]);
    const messages = await stream.listMessages();
    expect(messages).toHaveLength(1);
    expect(text(messages[0]!.data)).toBe("hello");

    await stream.producers!.setProducerState("p1", { epoch: 0, lastSeq: 0 });
    expect(await stream.producers!.getProducerState("p1")).toEqual({ epoch: 0, lastSeq: 0 });
    await stream.producers!.deleteProducerStates();
    expect(await stream.producers!.getProducerState("p1")).toBeUndefined();

    expect(await stream.references!.incrementChildRefCount()).toBe(1);
    expect(await stream.references!.incrementChildRefCount()).toBe(2);
    expect(await stream.references!.decrementChildRefCount()).toBe(1);

    await stream.deleteMessages();
    expect(await stream.listMessages()).toEqual([]);

    await stream.deleteRecord();
    expect(await stream.getRecord()).toBeNull();
  });

  it("isolates Streams created for different ids", async () => {
    const factory = createMemoryStreamFactory();
    const a = await factory.getStream("a");
    const b = await factory.getStream("b");

    await a.createRecord({
      id: "a",
      config: { contentType: CONTENT_TYPE, createdAt: 0 },
      lifecycle: { childRefCount: 0 },
      currentOffset: "0_0",
      counter: 0,
    });
    await b.createRecord({
      id: "b",
      config: { contentType: CONTENT_TYPE, createdAt: 0 },
      lifecycle: { childRefCount: 0 },
      currentOffset: "0_0",
      counter: 0,
    });

    await a.appendMessages([{ data: bytes("a-1"), offset: "1_0", timestamp: 1 }]);
    await b.appendMessages([{ data: bytes("b-1"), offset: "1_0", timestamp: 1 }]);

    const aMessages = await a.listMessages();
    const bMessages = await b.listMessages();
    expect(aMessages.map((m) => text(m.data))).toEqual(["a-1"]);
    expect(bMessages.map((m) => text(m.data))).toEqual(["b-1"]);
  });

  it("shares state with a parallel MemoryStreamStore when wired explicitly", async () => {
    const state = new MemoryStreamStore();
    const factory = createMemoryStreamFactory({ state });

    const stream = await factory.getStream("shared");
    await stream.createRecord({
      id: "shared",
      config: { contentType: CONTENT_TYPE, createdAt: 0 },
      lifecycle: { childRefCount: 0 },
      currentOffset: "0_0",
      counter: 0,
    });
    await stream.appendMessages([{ data: bytes("via-factory"), offset: "1_0", timestamp: 1 }]);

    // The adapter sees writes made through the factory because they share state.
    const adapterRecord = await state.get("shared");
    expect(adapterRecord?.id).toBe("shared");
    const adapterMessages = await state.list("shared");
    expect(adapterMessages.map((m) => text(m.data))).toEqual(["via-factory"]);

    // And vice versa: writes through the adapter are visible through the factory.
    await state.append("shared", [{ data: bytes("via-adapter"), offset: "2_0", timestamp: 2 }]);
    const viaFactory = await stream.listMessages();
    expect(viaFactory.map((m) => text(m.data))).toEqual(["via-factory", "via-adapter"]);
  });

  it("matches adapter results for representative operations (parity check)", async () => {
    // Same starting record applied via both code paths must produce equal
    // reads back. This is a coarse parity check, not a full conformance pass
    // (the dedicated memory conformance suite covers that).
    const adapterStore = new MemoryStreamStore();
    const factory = createMemoryStreamFactory();
    const factoryStream = await factory.getStream("p");

    const record = {
      id: "p",
      config: { contentType: CONTENT_TYPE, createdAt: 0 },
      lifecycle: { childRefCount: 0 },
      currentOffset: "0_0",
      counter: 0,
    } as const;

    await adapterStore.create(record);
    await factoryStream.createRecord(record);

    const message = { data: bytes("payload"), offset: "1_0", timestamp: 1 };
    await adapterStore.append("p", [message]);
    await factoryStream.appendMessages([message]);

    expect(await adapterStore.get("p")).toEqual(await factoryStream.getRecord());
    expect(await adapterStore.list("p")).toEqual(await factoryStream.listMessages());
  });

  it("serializes overlapping callers through the per-stream mutation lock", async () => {
    const factory = createMemoryStreamFactory();
    const stream = await factory.getStream("locked");
    const order: string[] = [];

    let releaseFirst!: () => void;
    const first = stream.mutations!.withMutationLock(async () => {
      order.push("first-start");
      await new Promise<void>((resolve) => {
        releaseFirst = resolve;
      });
      order.push("first-end");
    });

    // Wait for first to enter before scheduling the second so the lock
    // chain is exercised deterministically.
    await new Promise((resolve) => setTimeout(resolve, 0));
    const second = stream.mutations!.withMutationLock(async () => {
      order.push("second");
    });

    releaseFirst();
    await Promise.all([first, second]);
    expect(order).toEqual(["first-start", "first-end", "second"]);
  });

  it("routes live-read notification through the composed Stream", async () => {
    const factory = createMemoryStreamFactory();
    const stream = await factory.getStream("notify");
    const wait = stream.events!.waitForEvent({ timeoutMs: 1_000 });
    await stream.events!.notify("message");
    await expect(wait).resolves.toEqual({ status: "notified", type: "message" });
  });

  it("routes active expiry scheduling through the composed Stream", async () => {
    const factory = createMemoryStreamFactory();
    const stream = await factory.getStream("expiry");
    const fired = new Promise<void>((resolve) => {
      stream.expiry!.scheduleExpiry(Date.now(), async () => resolve());
    });
    await fired;
    // Cancelling after firing must still be a no-op rather than throwing.
    await stream.expiry!.cancelExpiry();
  });

  it("integrates with StreamProtocol via createStreamFactoryFromAdapter parity", async () => {
    // The native factory is intended to back protocol code once protocol
    // migration lands. Until then, confirm that a `MemoryStreamStore`
    // shared with the factory drives the existing protocol surface so that
    // future migration can rely on the same in-process data model.
    const state = new MemoryStreamStore();
    const factory = createMemoryStreamFactory({ state });
    const protocol = new StreamProtocol(state);

    await protocol.create("integration", { contentType: CONTENT_TYPE });
    await protocol.append("integration", {
      contentType: CONTENT_TYPE,
      data: bytes("via-protocol"),
    });

    const stream = await factory.getStream("integration");
    const record = await stream.getRecord();
    expect(record?.id).toBe("integration");
    const messages = await stream.listMessages();
    expect(messages.map((m) => text(m.data))).toEqual(["via-protocol"]);
  });
});

describe("composeStream optionality", () => {
  it("omits stream.producers when no producer store is supplied", () => {
    const stream = composeStream({
      id: "no-producer",
      recordStore: {
        getRecord: async () => null,
        createRecord: async () => ({ status: "created" as const }),
        updateRecord: async () => ({
          id: "no-producer",
          config: { contentType: CONTENT_TYPE, createdAt: 0 },
          lifecycle: { childRefCount: 0 },
          currentOffset: "0_0",
          counter: 0,
        }),
        deleteRecord: async () => {},
      },
      messageStore: {
        appendMessages: async () => {},
        listMessages: async () => [],
        deleteMessages: async () => {},
      },
    });

    expect(stream.id).toBe("no-producer");
    expect(stream.producers).toBeUndefined();
    expect(stream.references).toBeUndefined();
    expect(stream.mutations).toBeUndefined();
    expect(stream.events).toBeUndefined();
    expect(stream.expiry).toBeUndefined();
  });
});
