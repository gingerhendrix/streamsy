import { describe, expect, it } from "vitest";
import { StreamProtocol, ZERO_OFFSET } from "@streamsy/core";
import type { StreamRecord } from "@streamsy/core";
import { createDurableObjectStorageAdapter } from "./adapter.ts";
import { createFakeNamespace } from "./testing/in-memory-namespace.ts";

const CONTENT_TYPE = "application/octet-stream";

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function text(value: Uint8Array): string {
  return new TextDecoder().decode(value);
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
    const waiting = adapter.awaitChange("alpha", { fromOffset: "1_0", timeoutMs: 1_000 });
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
    expect(fake.stubFor("alpha").state.expiry.cancelled).toBe(false);
    fake.stubFor("alpha").state.record = null;
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

    const immediate = await adapter.awaitChange("alpha", { fromOffset: "0_0", timeoutMs: 1_000 });
    expect(immediate.status).toBe("changed");

    const timed = await adapter.awaitChange("alpha", { fromOffset: "1_0", timeoutMs: 10 });
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
