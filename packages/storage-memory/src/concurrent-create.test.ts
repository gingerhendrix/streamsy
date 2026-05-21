/**
 * Concurrency coverage for StreamProtocol.create.
 *
 * These tests deliberately make store.create(record) visible before it returns.
 * A competing append/create then starts during the old pre-lock race window.
 * StreamProtocol must hold the same per-stream mutation lock across the whole
 * create flow so initial-data writes and closed-on-create lifecycle updates are
 * serialized before the competing mutation observes the stream.
 */

import { describe, it, expect } from "vitest";
import { StreamProtocol } from "@streamsy/core";
import { MemoryStreamStore } from "@streamsy/storage-memory";
import type { StreamRecord } from "@streamsy/core";

const CONTENT_TYPE = "text/plain";

interface Deferred<T = void> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

function deferred<T = void>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function text(value: Uint8Array): string {
  return new TextDecoder().decode(value);
}

class PausingCreateStore extends MemoryStreamStore {
  readonly createVisible = deferred<StreamRecord>();
  readonly releaseCreate = deferred();
  private paused = false;

  constructor(private pauseStreamId: string) {
    super();
  }

  override async create(record: StreamRecord) {
    const result = await super.create(record);
    if (result.status === "created" && record.id === this.pauseStreamId && !this.paused) {
      this.paused = true;
      this.createVisible.resolve(record);
      await this.releaseCreate.promise;
    }
    return result;
  }
}

describe("StreamProtocol concurrent create", () => {
  it("serializes create(initialData) before a racing append", async () => {
    const store = new PausingCreateStore("create-initial-race");
    const protocol = new StreamProtocol(store);

    const createPromise = protocol.create("create-initial-race", {
      contentType: CONTENT_TYPE,
      initialData: bytes("initial"),
    });
    await store.createVisible.promise;

    const appendPromise = protocol.append("create-initial-race", {
      contentType: CONTENT_TYPE,
      data: bytes("append"),
    });

    store.releaseCreate.resolve();

    const [created, appended] = await Promise.all([createPromise, appendPromise]);
    expect(created.status).toBe("created");
    expect(appended.status).toBe("appended");

    const read = await protocol.read("create-initial-race", {});
    expect(read.status).toBe("ok");
    expect(read.messages.map((message) => text(message.data))).toEqual(["initial", "append"]);

    const offsets = read.messages.map((message) => message.offset);
    expect(new Set(offsets).size).toBe(offsets.length);
    expect(offsets[1]! > offsets[0]!).toBe(true);
  });

  it("serializes closed create before a racing append", async () => {
    const store = new PausingCreateStore("closed-create-race");
    const protocol = new StreamProtocol(store);

    const createPromise = protocol.create("closed-create-race", {
      contentType: CONTENT_TYPE,
      initialData: bytes("final"),
      closed: true,
    });
    await store.createVisible.promise;

    const appendPromise = protocol.append("closed-create-race", {
      contentType: CONTENT_TYPE,
      data: bytes("too-late"),
    });

    store.releaseCreate.resolve();

    const [created, append] = await Promise.all([createPromise, appendPromise]);
    expect(created).toMatchObject({ status: "created", closed: true });
    expect(append).toMatchObject({ status: "conflict", conflictReason: "closed", closed: true });

    const read = await protocol.read("closed-create-race", {});
    expect(read.status).toBe("ok");
    expect(read.messages.map((message) => text(message.data))).toEqual(["final"]);
    expect(read.closed).toBe(true);
  });

  it("serializes conflicting concurrent creates", async () => {
    const store = new PausingCreateStore("create-create-race");
    const protocol = new StreamProtocol(store);

    const firstCreate = protocol.create("create-create-race", { contentType: CONTENT_TYPE });
    await store.createVisible.promise;

    const secondCreate = protocol.create("create-create-race", {
      contentType: CONTENT_TYPE,
      closed: true,
    });

    store.releaseCreate.resolve();

    const [first, second] = await Promise.all([firstCreate, secondCreate]);
    expect(first.status).toBe("created");
    expect(second).toMatchObject({ status: "conflict", conflictReason: "config-mismatch" });

    const meta = await protocol.metadata("create-create-race");
    expect(meta.status).toBe("ok");
    expect(meta.closed).toBe(false);
  });

  it("serializes fork target initial data before a racing target append", async () => {
    const store = new PausingCreateStore("fork-target-race");
    const protocol = new StreamProtocol(store);

    await protocol.create("fork-source", {
      contentType: CONTENT_TYPE,
      initialData: bytes("source"),
    });

    const forkCreate = protocol.create("fork-target-race", {
      contentType: CONTENT_TYPE,
      forkedFrom: "fork-source",
      initialData: bytes("fork-initial"),
    });
    await store.createVisible.promise;

    const append = protocol.append("fork-target-race", {
      contentType: CONTENT_TYPE,
      data: bytes("target-append"),
    });

    store.releaseCreate.resolve();

    const [created, appended] = await Promise.all([forkCreate, append]);
    expect(created.status).toBe("created");
    expect(appended.status).toBe("appended");

    const read = await protocol.read("fork-target-race", {});
    expect(read.status).toBe("ok");
    expect(read.messages.map((message) => text(message.data))).toEqual([
      "source",
      "fork-initial",
      "target-append",
    ]);

    const targetOffsets = read.messages.slice(1).map((message) => message.offset);
    expect(new Set(targetOffsets).size).toBe(targetOffsets.length);
    expect(targetOffsets[1]! > targetOffsets[0]!).toBe(true);
  });
});
