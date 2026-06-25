import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { StreamProtocol, ZERO_OFFSET, type StreamRecord } from "@streamsy/core";
import { createSqliteStreamFactory } from "./index.ts";

const tempDirs: string[] = [];

function tempDbPath(): string {
  const dir = mkdtempSync(join(tmpdir(), "streamsy-sqlite-"));
  tempDirs.push(dir);
  return join(dir, "streamsy.sqlite");
}

afterEach(() => {
  while (tempDirs.length) rmSync(tempDirs.pop()!, { recursive: true, force: true });
});

function newRecord(id: string): StreamRecord {
  return {
    id,
    config: { contentType: "text/plain", ttlSeconds: 60, createdAt: 1000 },
    lifecycle: { expiresAtMs: 61000 },
    currentOffset: ZERO_OFFSET,
    counter: 0,
  };
}

describe("SqliteStream record store", () => {
  test("returns null for an unknown stream", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    expect(stream.id).toBe("s");
    expect(await stream.getRecord()).toBeNull();
    factory.close();
  });

  test("creates a record idempotently", async () => {
    const factory = createSqliteStreamFactory();
    const created = await factory.create({ record: newRecord("s") });
    expect(created.status).toBe("created");
    const again = await factory.create({ record: newRecord("s") });
    expect(again.status).toBe("exists");
    if (again.status !== "exists") throw new Error("expected exists");
    expect(again.record.config.contentType).toBe("text/plain");
    expect(again.record.config.ttlSeconds).toBe(60);
    factory.close();
  });

  test("rejects a record whose id does not match", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    await expect(
      stream.commit({ createRecord: newRecord("other"), preconditions: {} }),
    ).rejects.toThrow();
    factory.close();
  });

  test("merges partial patches without clobbering other fields", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    await factory.create({ record: newRecord("s") });
    await stream.commit({
      preconditions: { expectedOffset: ZERO_OFFSET },
      recordPatch: { lifecycle: { forkedFrom: "parent", forkOffset: ZERO_OFFSET } },
    });
    const committed = await stream.commit({
      preconditions: { expectedOffset: ZERO_OFFSET },
      recordPatch: { lifecycle: { closed: true, closedAt: 5 } },
    });
    if (committed.status !== "committed") throw new Error("expected committed");
    const updated = committed.record;
    expect(updated.lifecycle.forkedFrom).toBe("parent");
    expect(updated.lifecycle.closed).toBe(true);
    expect(updated.lifecycle.closedAt).toBe(5);
    factory.close();
  });

  test("uses factory fork/delete verbs with reverse-index lineage", async () => {
    const factory = createSqliteStreamFactory();
    await factory.create({ record: newRecord("parent") });
    const child = newRecord("child");
    child.lifecycle.forkedFrom = "parent";
    child.lifecycle.forkOffset = ZERO_OFFSET;
    const forked = await factory.fork?.({
      child,
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
    factory.close();
  });

  test("reverse-index lineage retains soft-deleted fork ancestors until descendants purge", async () => {
    const sqlite = createSqliteStreamFactory();
    const protocol = new StreamProtocol({ storage: { factory: sqlite } });

    await seedThreeLevelForkWithMessages(protocol);

    const childDelete = await sqlite.delete({ streamId: "child", reason: "delete" });
    expect(childDelete.status).toBe("retained-soft-deleted");

    const parentDelete = await sqlite.delete({ streamId: "parent", reason: "delete" });
    expect(parentDelete.status).toBe("retained-soft-deleted");
    expect((await (await sqlite.getStream("parent")).getRecord())?.lifecycle.softDeleted).toBe(
      true,
    );
    expect((await (await sqlite.getStream("child")).getRecord())?.lifecycle.softDeleted).toBe(true);

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

    const grandchildDelete = await sqlite.delete({ streamId: "grandchild", reason: "delete" });
    expect(grandchildDelete.status).toBe("purged");
    expect(await (await sqlite.getStream("grandchild")).getRecord()).toBeNull();
    expect(await (await sqlite.getStream("child")).getRecord()).toBeNull();
    expect(await (await sqlite.getStream("parent")).getRecord()).toBeNull();
    sqlite.close();
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

describe("SqliteStream message store", () => {
  test("round-trips binary data and orders by offset", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    await factory.create({ record: newRecord("s") });
    const a = new Uint8Array([0, 1, 2, 255]);
    const b = new Uint8Array([9, 8, 7]);
    await stream.commit({
      preconditions: { expectedOffset: ZERO_OFFSET },
      appendMessages: [
        { offset: "0000000000000001_0000000000000000", timestamp: 10, data: a },
        { offset: "0000000000000002_0000000000000000", timestamp: 20, data: b },
      ],
      recordPatch: {
        currentOffset: "0000000000000002_0000000000000000",
        counter: 2,
      },
    });
    const all = await stream.listMessages();
    expect(all).toHaveLength(2);
    expect(all[0]!.data).toEqual(a);
    expect(all[1]!.data).toEqual(b);

    const after = await stream.listMessages({ after: "0000000000000001_0000000000000000" });
    expect(after).toHaveLength(1);
    expect(after[0]!.timestamp).toBe(20);

    const limited = await stream.listMessages({ limit: 1 });
    expect(limited).toHaveLength(1);
    expect(limited[0]!.timestamp).toBe(10);
    factory.close();
  });

  test("commit append without a record fails its precondition", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("missing");
    const result = await stream.commit({
      preconditions: { expectedOffset: ZERO_OFFSET },
      appendMessages: [
        { offset: "0000000000000001_0000000000000000", timestamp: 1, data: new Uint8Array([1]) },
      ],
    });
    expect(result).toEqual({ status: "precondition-failed", record: null });
    factory.close();
  });

  test("factory delete purges messages and producers", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    await stream.commit({
      createRecord: newRecord("s"),
      preconditions: {
        producer: {
          producerId: "p",
          expected: undefined,
          next: { epoch: 1, lastSeq: 3 },
        },
      },
      appendMessages: [
        { offset: "0000000000000001_0000000000000000", timestamp: 1, data: new Uint8Array([1]) },
      ],
      recordPatch: {
        currentOffset: "0000000000000001_0000000000000000",
        counter: 1,
      },
    });
    await factory.delete({ streamId: "s", reason: "delete" });

    const fresh = await factory.getStream("s");
    expect(await fresh.getRecord()).toBeNull();
    expect(await fresh.listMessages()).toHaveLength(0);
    expect(await fresh.getProducerState("p")).toBeUndefined();
    factory.close();
  });
});

describe("SqliteStream producer store", () => {
  test("upserts and reads producer state", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    await factory.create({ record: newRecord("s") });
    expect(await stream.getProducerState("p")).toBeUndefined();
    await stream.commit({
      preconditions: {
        producer: {
          producerId: "p",
          expected: undefined,
          next: { epoch: 1, lastSeq: 5 },
        },
      },
    });
    expect(await stream.getProducerState("p")).toEqual({ epoch: 1, lastSeq: 5 });
    await stream.commit({
      preconditions: {
        producer: {
          producerId: "p",
          expected: { epoch: 1, lastSeq: 5 },
          next: { epoch: 2, lastSeq: 0 },
        },
      },
    });
    expect(await stream.getProducerState("p")).toEqual({ epoch: 2, lastSeq: 0 });
    factory.close();
  });
});

describe("persistence", () => {
  test("survives closing and reopening the database file", async () => {
    const path = tempDbPath();
    const first = createSqliteStreamFactory({ filename: path });
    const stream = await first.getStream("s");
    await stream.commit({
      createRecord: newRecord("s"),
      preconditions: {},
      appendMessages: [
        { offset: "0000000000000001_0000000000000000", timestamp: 1, data: new Uint8Array([42]) },
      ],
      recordPatch: {
        currentOffset: "0000000000000001_0000000000000000",
        counter: 1,
      },
    });
    first.close();

    const second = createSqliteStreamFactory({ filename: path });
    const reopened = await second.getStream("s");
    const record = await reopened.getRecord();
    expect(record?.config.contentType).toBe("text/plain");
    const messages = await reopened.listMessages();
    expect(messages).toHaveLength(1);
    expect(messages[0]!.data).toEqual(new Uint8Array([42]));
    second.close();
  });
});
