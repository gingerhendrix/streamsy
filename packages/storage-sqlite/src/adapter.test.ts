import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { StreamProtocol, ZERO_OFFSET, type StreamRecord } from "@streamsy/core";
import { createSqliteStorageAdapter } from "./index.ts";

const tempDirs: string[] = [];
const OFFSET_1 = "0000000000000001_0000000000000000";
const OFFSET_2 = "0000000000000002_0000000000000000";

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
    const adapter = createSqliteStorageAdapter();
    expect(await adapter.getRecord("s")).toBeNull();
    adapter.close();
  });

  test("creates a record idempotently", async () => {
    const adapter = createSqliteStorageAdapter();
    const created = await adapter.create({ record: newRecord("s") });
    expect(created.status).toBe("created");
    const again = await adapter.create({ record: newRecord("s") });
    expect(again.status).toBe("exists");
    if (again.status !== "exists") throw new Error("expected exists");
    expect(again.record.config.contentType).toBe("text/plain");
    expect(again.record.config.ttlSeconds).toBe(60);
    adapter.close();
  });

  test("merges partial patches without clobbering other fields", async () => {
    const adapter = createSqliteStorageAdapter();
    await adapter.create({ record: newRecord("s") });
    await adapter.append("s", {
      preconditions: { expectedOffset: ZERO_OFFSET },
      recordPatch: { lifecycle: { forkedFrom: "parent", forkOffset: ZERO_OFFSET } },
    });
    const appended = await adapter.append("s", {
      preconditions: { expectedOffset: ZERO_OFFSET },
      recordPatch: { lifecycle: { closed: true, closedAt: 5 } },
    });
    if (appended.status !== "appended") throw new Error("expected appended");
    const updated = appended.record;
    expect(updated.lifecycle.forkedFrom).toBe("parent");
    expect(updated.lifecycle.closed).toBe(true);
    expect(updated.lifecycle.closedAt).toBe(5);
    adapter.close();
  });

  test("uses fork/delete verbs with reverse-index lineage", async () => {
    const adapter = createSqliteStorageAdapter();
    await adapter.create({ record: newRecord("parent") });
    const child = newRecord("child");
    child.lifecycle.forkedFrom = "parent";
    child.lifecycle.forkOffset = ZERO_OFFSET;
    const forked = await adapter.fork?.({
      child,
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
    adapter.close();
  });

  test("reverse-index lineage retains soft-deleted fork ancestors until descendants purge", async () => {
    const adapter = createSqliteStorageAdapter();
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
    adapter.close();
  });

  test("forks at a binary sub-offset, persisting the materialized prefix and sub-offset", async () => {
    const adapter = createSqliteStorageAdapter();
    const protocol = new StreamProtocol({ storage: { adapter } });

    const src = await protocol.create("src", {
      contentType: "text/plain",
      initialData: new TextEncoder().encode("hello"),
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
    expect(read.messages.map((m) => new TextDecoder().decode(m.data)).join("")).toBe("hel");

    // Sub-offset persists across a fresh lookup and drives idempotency.
    const forkRecord = await adapter.getRecord("fork");
    expect(forkRecord?.lifecycle.forkSubOffset).toBe(3);

    const again = await protocol.create("fork", {
      contentType: "text/plain",
      forkedFrom: "src",
      forkOffset: ZERO_OFFSET,
      forkSubOffset: 3,
    });
    expect(again.status).toBe("exists");

    const mismatch = await protocol.create("fork", {
      contentType: "text/plain",
      forkedFrom: "src",
      forkOffset: ZERO_OFFSET,
      forkSubOffset: 2,
    });
    expect(mismatch.status).toBe("conflict");
    adapter.close();
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
    const adapter = createSqliteStorageAdapter();
    await adapter.create({ record: newRecord("s") });
    const a = new Uint8Array([0, 1, 2, 255]);
    const b = new Uint8Array([9, 8, 7]);
    await adapter.append("s", {
      preconditions: { expectedOffset: ZERO_OFFSET },
      messages: [
        { offset: OFFSET_1, timestamp: 10, data: a },
        { offset: OFFSET_2, timestamp: 20, data: b },
      ],
      recordPatch: { currentOffset: OFFSET_2, counter: 2 },
    });
    const all = await adapter.listMessages("s");
    expect(all).toHaveLength(2);
    expect(all[0]!.data).toEqual(a);
    expect(all[1]!.data).toEqual(b);

    const after = await adapter.listMessages("s", { after: OFFSET_1 });
    expect(after).toHaveLength(1);
    expect(after[0]!.timestamp).toBe(20);

    const limited = await adapter.listMessages("s", { limit: 1 });
    expect(limited).toHaveLength(1);
    expect(limited[0]!.timestamp).toBe(10);
    adapter.close();
  });

  test("append without a record fails its precondition", async () => {
    const adapter = createSqliteStorageAdapter();
    const result = await adapter.append("missing", {
      preconditions: { expectedOffset: ZERO_OFFSET },
      messages: [{ offset: OFFSET_1, timestamp: 1, data: new Uint8Array([1]) }],
      recordPatch: { currentOffset: OFFSET_1, counter: 1 },
    });
    expect(result).toEqual({ status: "precondition-failed", record: null, reason: "offset" });
    adapter.close();
  });

  test("delete purges messages and producers", async () => {
    const adapter = createSqliteStorageAdapter();
    await adapter.create({ record: newRecord("s") });
    await adapter.append("s", {
      preconditions: {
        expectedOffset: ZERO_OFFSET,
        producer: { producerId: "p", expected: undefined, next: { epoch: 1, lastSeq: 3 } },
      },
      messages: [{ offset: OFFSET_1, timestamp: 1, data: new Uint8Array([1]) }],
      recordPatch: { currentOffset: OFFSET_1, counter: 1 },
    });
    await adapter.delete({ streamId: "s", reason: "delete" });

    expect(await adapter.getRecord("s")).toBeNull();
    expect(await adapter.listMessages("s")).toHaveLength(0);
    expect(await adapter.getProducerState("s", "p")).toBeUndefined();
    adapter.close();
  });
});

describe("SqliteStream producer store", () => {
  test("upserts and reads producer state", async () => {
    const adapter = createSqliteStorageAdapter();
    await adapter.create({ record: newRecord("s") });
    expect(await adapter.getProducerState("s", "p")).toBeUndefined();
    await adapter.append("s", {
      preconditions: {
        producer: { producerId: "p", expected: undefined, next: { epoch: 1, lastSeq: 5 } },
      },
      recordPatch: {},
    });
    expect(await adapter.getProducerState("s", "p")).toEqual({ epoch: 1, lastSeq: 5 });
    await adapter.append("s", {
      preconditions: {
        producer: {
          producerId: "p",
          expected: { epoch: 1, lastSeq: 5 },
          next: { epoch: 2, lastSeq: 0 },
        },
      },
      recordPatch: {},
    });
    expect(await adapter.getProducerState("s", "p")).toEqual({ epoch: 2, lastSeq: 0 });
    adapter.close();
  });
});

describe("persistence", () => {
  test("survives closing and reopening the database file", async () => {
    const path = tempDbPath();
    const first = createSqliteStorageAdapter({ filename: path });
    await first.create({
      record: newRecord("s"),
      initialMessages: [{ offset: OFFSET_1, timestamp: 1, data: new Uint8Array([42]) }],
    });
    first.close();

    const second = createSqliteStorageAdapter({ filename: path });
    const record = await second.getRecord("s");
    expect(record?.config.contentType).toBe("text/plain");
    const messages = await second.listMessages("s");
    expect(messages).toHaveLength(1);
    expect(messages[0]!.data).toEqual(new Uint8Array([42]));
    second.close();
  });
});
