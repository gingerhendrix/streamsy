import { afterEach, describe, expect, test } from "bun:test";
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { StreamProtocol } from "@streamsy/core";
import { createSqliteStreamFactory } from "./index.ts";

const encode = (s: string) => new TextEncoder().encode(s);
const tempDirs: string[] = [];
const appendWorkerPath = fileURLToPath(new URL("./sqlite-append-worker.ts", import.meta.url));

function tempDbPath(): string {
  const dir = mkdtempSync(join(tmpdir(), "streamsy-sqlite-"));
  tempDirs.push(dir);
  return join(dir, "streamsy.sqlite");
}

afterEach(() => {
  while (tempDirs.length) rmSync(tempDirs.pop()!, { recursive: true, force: true });
});

describe("sqlite protocol", () => {
  test("create is idempotent through the protocol", async () => {
    const protocol = new StreamProtocol({ storage: { factory: createSqliteStreamFactory() } });
    const first = await protocol.create("s", { contentType: "text/plain" });
    const second = await protocol.create("s", { contentType: "text/plain" });
    expect(first.status).toBe("created");
    expect(second.status).toBe("exists");
  });

  test("concurrent appends serialize without losing messages", async () => {
    const protocol = new StreamProtocol({ storage: { factory: createSqliteStreamFactory() } });
    await protocol.create("s", { contentType: "text/plain" });
    const lookup = await protocol.get("s");
    if (lookup.status !== "ok") throw new Error("lookup failed");
    const results = await Promise.all(
      Array.from({ length: 32 }, (_, i) =>
        lookup.stream.append({ contentType: "text/plain", data: encode(`m${i}`) }),
      ),
    );
    expect(results.every((result) => result.status === "appended")).toBe(true);
    const read = await lookup.stream.read({});
    if (read.status !== "ok") throw new Error("read failed");
    expect(read.messages).toHaveLength(32);
    const offsets = read.messages.map((m) => m.offset);
    expect(new Set(offsets).size).toBe(32);
    expect(offsets).toEqual(
      Array.from({ length: 32 }, (_, i) => `${String(i + 1).padStart(16, "0")}_0000000000000000`),
    );
  });

  test("producer idempotency: duplicate seq does not double-append", async () => {
    const protocol = new StreamProtocol({ storage: { factory: createSqliteStreamFactory() } });
    await protocol.create("s", { contentType: "text/plain" });
    const lookup = await protocol.get("s");
    if (lookup.status !== "ok") throw new Error("lookup failed");
    const producer = { producerId: "p", producerEpoch: 1, producerSeq: 0 };
    const first = await lookup.stream.append({
      contentType: "text/plain",
      data: encode("a"),
      producer,
    });
    const duplicate = await lookup.stream.append({
      contentType: "text/plain",
      data: encode("a"),
      producer,
    });
    expect(first.status).toBe("appended");
    expect(duplicate.status).toBe("duplicate");
    const read = await lookup.stream.read({});
    if (read.status !== "ok") throw new Error("read failed");
    expect(read.messages).toHaveLength(1);
  });

  test("producer idempotency: stale epoch is rejected", async () => {
    const protocol = new StreamProtocol({ storage: { factory: createSqliteStreamFactory() } });
    await protocol.create("s", { contentType: "text/plain" });
    const lookup = await protocol.get("s");
    if (lookup.status !== "ok") throw new Error("lookup failed");
    await lookup.stream.append({
      contentType: "text/plain",
      data: encode("a"),
      producer: { producerId: "p", producerEpoch: 2, producerSeq: 0 },
    });
    const stale = await lookup.stream.append({
      contentType: "text/plain",
      data: encode("b"),
      producer: { producerId: "p", producerEpoch: 1, producerSeq: 0 },
    });
    expect(stale.status).toBe("stale-epoch");
  });

  test("long-poll live read times out then observes a later append", async () => {
    const protocol = new StreamProtocol({
      storage: { factory: createSqliteStreamFactory() },
      longPollTimeoutMs: 150,
    });
    await protocol.create("s", { contentType: "text/plain" });
    const lookup = await protocol.get("s");
    if (lookup.status !== "ok") throw new Error("lookup failed");

    const timed = await lookup.stream.readLive({ offset: "0", mode: "long-poll" });
    if (timed.status === "not-supported") throw new Error("live read unsupported");
    expect(timed.status).toBe("timeout");

    const live = lookup.stream.readLive({ offset: "0", mode: "long-poll" });
    await lookup.stream.append({ contentType: "text/plain", data: encode("hello") });
    const result = await live;
    if (result.status === "not-supported") throw new Error("live read unsupported");
    expect(result.status).toBe("ok");
    expect(result.messages).toHaveLength(1);
  });
});

test("expectedOffset CAS: stale append conflicts and retries cleanly", async () => {
  const protocol = new StreamProtocol({ storage: { factory: createSqliteStreamFactory() } });
  await protocol.create("s", { contentType: "text/plain" });
  const lookup = await protocol.get("s");
  if (lookup.status !== "ok") throw new Error("lookup failed");
  const head = await lookup.stream.append({ contentType: "text/plain", data: encode("a") });
  if (head.status !== "appended") throw new Error("expected appended");

  const winner = await lookup.stream.append({
    contentType: "text/plain",
    data: encode("b"),
    expectedOffset: head.offset,
  });
  expect(winner.status).toBe("appended");

  const loser = await lookup.stream.append({
    contentType: "text/plain",
    data: encode("c"),
    expectedOffset: head.offset,
  });
  if (loser.status !== "conflict" || loser.conflictReason !== "expected-offset")
    throw new Error("expected expected-offset conflict");

  const retry = await lookup.stream.append({
    contentType: "text/plain",
    data: encode("c"),
    expectedOffset: loser.offset,
  });
  expect(retry.status).toBe("appended");

  const read = await lookup.stream.read({});
  if (read.status !== "ok") throw new Error("read failed");
  expect(read.messages).toHaveLength(3);
});

test("multi-writer file-backed appends retry cleanly across database handles", async () => {
  const path = tempDbPath();
  const firstFactory = createSqliteStreamFactory({ filename: path });
  const secondFactory = createSqliteStreamFactory({ filename: path });
  const first = new StreamProtocol({ storage: { factory: firstFactory } });
  const second = new StreamProtocol({ storage: { factory: secondFactory } });

  await first.create("s", { contentType: "text/plain" });
  const firstLookup = await first.get("s");
  const secondLookup = await second.get("s");
  if (firstLookup.status !== "ok" || secondLookup.status !== "ok") throw new Error("lookup failed");

  const writes = Array.from({ length: 8 }, (_, i) => {
    const stream = i % 2 === 0 ? firstLookup.stream : secondLookup.stream;
    return stream.append({ contentType: "text/plain", data: encode(`m${i}`) });
  });
  const results = await Promise.all(writes);
  expect(results.every((result) => result.status === "appended")).toBe(true);

  const read = await firstLookup.stream.read({});
  if (read.status !== "ok") throw new Error("read failed");
  expect(read.messages.map((message) => message.offset)).toEqual([
    "0000000000000001_0000000000000000",
    "0000000000000002_0000000000000000",
    "0000000000000003_0000000000000000",
    "0000000000000004_0000000000000000",
    "0000000000000005_0000000000000000",
    "0000000000000006_0000000000000000",
    "0000000000000007_0000000000000000",
    "0000000000000008_0000000000000000",
  ]);

  firstFactory.close();
  secondFactory.close();
});

test("multi-process file-backed appends retry cleanly across database handles", async () => {
  const path = tempDbPath();
  const factory = createSqliteStreamFactory({ filename: path, busyTimeoutMs: 10_000 });
  const protocol = new StreamProtocol({ storage: { factory } });
  await protocol.create("s", { contentType: "text/plain" });

  const dir = tempDirs.at(-1)!;
  const startPath = join(dir, "start");
  const readyA = join(dir, "a.ready");
  const readyB = join(dir, "b.ready");
  const countPerProcess = 16;
  const first = spawnWorker(path, "s", "a", countPerProcess, readyA, startPath);
  const second = spawnWorker(path, "s", "b", countPerProcess, readyB, startPath);

  await waitForFile(readyA);
  await waitForFile(readyB);
  writeFileSync(startPath, "go");

  const [firstExit, secondExit] = await Promise.all([waitForExit(first), waitForExit(second)]);
  expect(firstExit).toEqual({ code: 0, stderr: "" });
  expect(secondExit).toEqual({ code: 0, stderr: "" });

  const lookup = await protocol.get("s");
  if (lookup.status !== "ok") throw new Error("lookup failed");
  const read = await lookup.stream.read({});
  if (read.status !== "ok") throw new Error("read failed");
  expect(read.messages).toHaveLength(countPerProcess * 2);
  const offsets = read.messages.map((message) => message.offset);
  expect(new Set(offsets).size).toBe(countPerProcess * 2);
  expect(offsets).toEqual(
    Array.from(
      { length: countPerProcess * 2 },
      (_, i) => `${String(i + 1).padStart(16, "0")}_0000000000000000`,
    ),
  );

  factory.close();
});

function spawnWorker(
  dbPath: string,
  streamId: string,
  label: string,
  count: number,
  readyPath: string,
  startPath: string,
) {
  return spawn(
    "bun",
    [appendWorkerPath, dbPath, streamId, label, String(count), readyPath, startPath],
    {
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
}

async function waitForFile(path: string): Promise<void> {
  for (let i = 0; i < 1_000; i++) {
    if (existsSync(path)) return;
    await delay(5);
  }
  throw new Error(`timed out waiting for ${path}`);
}

function waitForExit(
  child: ReturnType<typeof spawn>,
): Promise<{ code: number | null; stderr: string }> {
  return new Promise((resolve, reject) => {
    let stderr = "";
    child.stderr?.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", reject);
    child.on("close", (code) => resolve({ code, stderr: stderr.trim() }));
  });
}
