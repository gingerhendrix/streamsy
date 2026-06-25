import { describe, expect, it } from "vitest";
import type { StreamRecord } from "../../types/storage.ts";
import { StreamProtocol, ZERO_OFFSET } from "../../protocol.ts";
import { createMemoryStreamFactory } from "./factory.ts";

function record(id: string, forkedFrom?: string): StreamRecord {
  return {
    id,
    config: { contentType: "text/plain", createdAt: 1 },
    lifecycle: { forkedFrom, forkOffset: forkedFrom ? ZERO_OFFSET : undefined },
    currentOffset: ZERO_OFFSET,
    counter: 0,
  };
}

describe("createMemoryStreamFactory", () => {
  it("returns storage streams bound to ids", async () => {
    const factory = createMemoryStreamFactory();
    const stream = await factory.getStream("s");
    expect(stream.id).toBe("s");
    expect(await stream.getRecord()).toBeNull();
  });

  it("commits create, message, record, and producer mutations atomically", async () => {
    const factory = createMemoryStreamFactory();
    const stream = await factory.getStream("s");
    const committed = await stream.commit({
      createRecord: record("s"),
      preconditions: {
        producer: {
          producerId: "p",
          expected: undefined,
          next: { epoch: 1, lastSeq: 0 },
        },
      },
      appendMessages: [
        {
          data: new TextEncoder().encode("a"),
          offset: "0000000000000001_0000000000000000",
          timestamp: 1,
        },
      ],
      recordPatch: {
        currentOffset: "0000000000000001_0000000000000000",
        counter: 1,
      },
    });

    expect(committed.status).toBe("committed");
    expect(await stream.getProducerState("p")).toEqual({ epoch: 1, lastSeq: 0 });
    expect(await stream.listMessages()).toHaveLength(1);

    const stale = await stream.commit({
      preconditions: { expectedOffset: ZERO_OFFSET },
      recordPatch: { lifecycle: { closed: true } },
    });

    expect(stale.status).toBe("precondition-failed");
    expect((await stream.getRecord())?.lifecycle.closed).toBeUndefined();
  });

  it("uses factory fork/delete verbs with in-memory lineage edges", async () => {
    const factory = createMemoryStreamFactory();
    await factory.create({ record: record("parent") });
    const forked = await factory.fork?.({
      child: record("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: ZERO_OFFSET },
    });
    expect(forked?.status).toBe("created");

    const retained = await factory.delete({ streamId: "parent", reason: "delete" });
    expect(retained.status).toBe("retained-soft-deleted");
    expect((await (await factory.getStream("parent")).getRecord())?.lifecycle.softDeleted).toBe(
      true,
    );

    const purged = await factory.delete({ streamId: "child", reason: "delete" });
    expect(purged.status).toBe("purged");
    expect(await (await factory.getStream("parent")).getRecord()).toBeNull();
  });

  it("retains soft-deleted fork ancestors until descendants purge", async () => {
    const factory = createMemoryStreamFactory();
    const protocol = new StreamProtocol({ storage: { factory } });

    await seedThreeLevelForkWithMessages(protocol);

    const childDelete = await factory.delete({ streamId: "child", reason: "delete" });
    expect(childDelete.status).toBe("retained-soft-deleted");

    const parentDelete = await factory.delete({ streamId: "parent", reason: "delete" });
    expect(parentDelete.status).toBe("retained-soft-deleted");
    expect((await (await factory.getStream("parent")).getRecord())?.lifecycle.softDeleted).toBe(
      true,
    );
    expect((await (await factory.getStream("child")).getRecord())?.lifecycle.softDeleted).toBe(
      true,
    );

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

    const grandchildDelete = await factory.delete({ streamId: "grandchild", reason: "delete" });
    expect(grandchildDelete.status).toBe("purged");
    expect(await (await factory.getStream("grandchild")).getRecord()).toBeNull();
    expect(await (await factory.getStream("child")).getRecord()).toBeNull();
    expect(await (await factory.getStream("parent")).getRecord()).toBeNull();
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
