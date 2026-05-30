import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ZERO_OFFSET, type StreamRecord } from "@streamsy/core";
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
    lifecycle: { childRefCount: 0, expiresAtMs: 61000 },
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
    const stream = await factory.getStream("s");
    const created = await stream.createRecord(newRecord("s"));
    expect(created.status).toBe("created");
    const again = await stream.createRecord(newRecord("s"));
    expect(again.status).toBe("exists");
    if (again.status !== "exists") throw new Error("expected exists");
    expect(again.record.config.contentType).toBe("text/plain");
    expect(again.record.config.ttlSeconds).toBe(60);
    factory.close();
  });

  test("rejects a record whose id does not match", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    await expect(stream.createRecord(newRecord("other"))).rejects.toThrow();
    factory.close();
  });

  test("merges partial patches without clobbering other fields", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    await stream.createRecord(newRecord("s"));
    await stream.updateRecord({ lifecycle: { forkedFrom: "parent", forkOffset: ZERO_OFFSET } });
    const updated = await stream.updateRecord({ lifecycle: { closed: true, closedAt: 5 } });
    expect(updated.lifecycle.forkedFrom).toBe("parent");
    expect(updated.lifecycle.closed).toBe(true);
    expect(updated.lifecycle.closedAt).toBe(5);
    expect(updated.lifecycle.childRefCount).toBe(0);
    factory.close();
  });

  test("tracks child ref counts and clamps at zero", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    await stream.createRecord(newRecord("s"));
    expect(await stream.incrementChildRefCount()).toBe(1);
    expect(await stream.incrementChildRefCount()).toBe(2);
    expect(await stream.decrementChildRefCount()).toBe(1);
    expect(await stream.decrementChildRefCount()).toBe(0);
    expect(await stream.decrementChildRefCount()).toBe(0);
    factory.close();
  });
});

describe("SqliteStream message store", () => {
  test("round-trips binary data and orders by offset", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    await stream.createRecord(newRecord("s"));
    const a = new Uint8Array([0, 1, 2, 255]);
    const b = new Uint8Array([9, 8, 7]);
    await stream.appendMessages([
      { offset: "0000000000000001_0000000000000000", timestamp: 10, data: a },
      { offset: "0000000000000002_0000000000000000", timestamp: 20, data: b },
    ]);
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

  test("appending without a record throws", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("missing");
    await expect(
      stream.appendMessages([
        { offset: "0000000000000001_0000000000000000", timestamp: 1, data: new Uint8Array([1]) },
      ]),
    ).rejects.toThrow();
    factory.close();
  });

  test("deleteRecord cascades to messages and producers", async () => {
    const factory = createSqliteStreamFactory();
    const stream = await factory.getStream("s");
    await stream.createRecord(newRecord("s"));
    await stream.appendMessages([
      { offset: "0000000000000001_0000000000000000", timestamp: 1, data: new Uint8Array([1]) },
    ]);
    await stream.setProducerState("p", { epoch: 1, lastSeq: 3 });
    await stream.deleteRecord();

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
    await stream.createRecord(newRecord("s"));
    expect(await stream.getProducerState("p")).toBeUndefined();
    await stream.setProducerState("p", { epoch: 1, lastSeq: 5 });
    expect(await stream.getProducerState("p")).toEqual({ epoch: 1, lastSeq: 5 });
    await stream.setProducerState("p", { epoch: 2, lastSeq: 0 });
    expect(await stream.getProducerState("p")).toEqual({ epoch: 2, lastSeq: 0 });
    factory.close();
  });
});

describe("persistence", () => {
  test("survives closing and reopening the database file", async () => {
    const path = tempDbPath();
    const first = createSqliteStreamFactory({ filename: path });
    const stream = await first.getStream("s");
    await stream.createRecord(newRecord("s"));
    await stream.appendMessages([
      { offset: "0000000000000001_0000000000000000", timestamp: 1, data: new Uint8Array([42]) },
    ]);
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
