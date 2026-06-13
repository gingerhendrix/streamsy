import { describe, expect, test } from "bun:test";
import { StreamProtocol } from "@streamsy/core";
import { createSqliteStreamFactory } from "./index.ts";

const encode = (s: string) => new TextEncoder().encode(s);

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
    await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        lookup.stream.append({ contentType: "text/plain", data: encode(`m${i}`) }),
      ),
    );
    const read = await lookup.stream.read({});
    if (read.status !== "ok") throw new Error("read failed");
    expect(read.messages).toHaveLength(10);
    const offsets = read.messages.map((m) => m.offset);
    expect(new Set(offsets).size).toBe(10);
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
