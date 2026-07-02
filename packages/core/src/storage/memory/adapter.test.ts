import { describe, expect, it } from "vitest";
import type { StreamRecord } from "../../types/storage.ts";
import { StreamProtocol, ZERO_OFFSET } from "../../protocol.ts";
import { createMemoryStorageAdapter } from "./adapter.ts";

const OFFSET_1 = "0000000000000001_0000000000000000";

function record(id: string, forkedFrom?: string): StreamRecord {
  return {
    id,
    config: { contentType: "text/plain", createdAt: 1 },
    lifecycle: { forkedFrom, forkOffset: forkedFrom ? ZERO_OFFSET : undefined },
    currentOffset: ZERO_OFFSET,
    counter: 0,
  };
}

describe("createMemoryStorageAdapter", () => {
  it("exposes flat per-stream reads bound to ids", async () => {
    const adapter = createMemoryStorageAdapter();
    expect(await adapter.getRecord("s")).toBeNull();
  });

  it("appends messages, record patch, and producer CAS atomically under offset preconditions", async () => {
    const adapter = createMemoryStorageAdapter();
    const created = await adapter.create({ record: record("s") });
    expect(created.status).toBe("created");

    const appended = await adapter.append("s", {
      preconditions: {
        expectedOffset: ZERO_OFFSET,
        producer: { producerId: "p", expected: undefined, next: { epoch: 1, lastSeq: 0 } },
      },
      messages: [{ data: new TextEncoder().encode("a"), offset: OFFSET_1, timestamp: 1 }],
      recordPatch: { currentOffset: OFFSET_1, counter: 1 },
    });

    expect(appended.status).toBe("appended");
    expect(await adapter.getProducerState("s", "p")).toEqual({ epoch: 1, lastSeq: 0 });
    expect(await adapter.listMessages("s")).toHaveLength(1);

    // A stale offset precondition fails and writes nothing.
    const stale = await adapter.append("s", {
      preconditions: { expectedOffset: ZERO_OFFSET },
      recordPatch: { lifecycle: { closed: true } },
    });

    expect(stale.status).toBe("precondition-failed");
    if (stale.status === "precondition-failed") expect(stale.reason).toBe("offset");
    expect((await adapter.getRecord("s"))?.lifecycle.closed).toBeUndefined();
  });

  it("uses adapter fork/delete verbs with in-memory lineage edges", async () => {
    const adapter = createMemoryStorageAdapter();
    await adapter.create({ record: record("parent") });
    const forked = await adapter.fork?.({
      child: record("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: ZERO_OFFSET },
    });
    expect(forked?.status).toBe("created");

    const retained = await adapter.delete({ streamId: "parent", reason: "delete" });
    expect(retained.status).toBe("retained-soft-deleted");
    expect((await adapter.getRecord("parent"))?.lifecycle.softDeleted).toBe(true);

    const purged = await adapter.delete({ streamId: "child", reason: "delete" });
    expect(purged.status).toBe("purged");
    expect(await adapter.getRecord("parent")).toBeNull();
  });

  it("retains soft-deleted fork ancestors until descendants purge", async () => {
    const adapter = createMemoryStorageAdapter();
    const protocol = new StreamProtocol({ storage: { adapter } });

    await seedThreeLevelForkWithMessages(protocol);

    const childDelete = await adapter.delete({ streamId: "child", reason: "delete" });
    expect(childDelete.status).toBe("retained-soft-deleted");

    const parentDelete = await adapter.delete({ streamId: "parent", reason: "delete" });
    expect(parentDelete.status).toBe("retained-soft-deleted");
    expect((await adapter.getRecord("parent"))?.lifecycle.softDeleted).toBe(true);
    expect((await adapter.getRecord("child"))?.lifecycle.softDeleted).toBe(true);

    const grandchild = await protocol.get("grandchild");
    expect(grandchild.status).toBe("ok");
    if (grandchild.status !== "ok") throw new Error("expected grandchild");
    const read = await grandchild.stream.read({});
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected read ok");
    expect(read.messages.map((message) => new TextDecoder().decode(message.data))).toEqual([
      "parent",
      "child",
    ]);

    const grandchildDelete = await adapter.delete({ streamId: "grandchild", reason: "delete" });
    expect(grandchildDelete.status).toBe("purged");
    expect(await adapter.getRecord("grandchild")).toBeNull();
    expect(await adapter.getRecord("child")).toBeNull();
    expect(await adapter.getRecord("parent")).toBeNull();
  });
});

async function seedThreeLevelForkWithMessages(protocol: StreamProtocol): Promise<void> {
  const parent = await protocol.create("parent", { contentType: "text/plain" });
  expect(parent.status).toBe("created");
  if (parent.status !== "created") throw new Error("expected parent created");
  const parentAppend = await parent.stream.append({
    contentType: "text/plain",
    data: new TextEncoder().encode("parent"),
  });
  expect(parentAppend.status).toBe("appended");
  if (parentAppend.status !== "appended") throw new Error("expected parent append");

  const child = await protocol.create("child", {
    contentType: "text/plain",
    forkedFrom: "parent",
    forkOffset: parentAppend.offset,
  });
  expect(child.status).toBe("created");
  if (child.status !== "created") throw new Error("expected child created");
  const childAppend = await child.stream.append({
    contentType: "text/plain",
    data: new TextEncoder().encode("child"),
  });
  expect(childAppend.status).toBe("appended");
  if (childAppend.status !== "appended") throw new Error("expected child append");

  const grandchild = await protocol.create("grandchild", {
    contentType: "text/plain",
    forkedFrom: "child",
    forkOffset: childAppend.offset,
  });
  expect(grandchild.status).toBe("created");
}
