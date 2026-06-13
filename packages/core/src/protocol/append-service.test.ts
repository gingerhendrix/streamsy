import { describe, expect, it } from "vitest";
import { StreamProtocol } from "../protocol.ts";
import { createMemoryStreamFactory } from "../storage/memory/factory.ts";
import type { ProtocolStream } from "../types/protocol.ts";
import { ZERO_OFFSET } from "./helpers/offset-generator.ts";

const encode = (s: string) => new TextEncoder().encode(s);

async function createStream(contentType = "text/plain"): Promise<ProtocolStream> {
  const protocol = new StreamProtocol({ storage: { factory: createMemoryStreamFactory() } });
  const created = await protocol.create("s", { contentType });
  if (created.status !== "created") throw new Error("expected create");
  return created.stream;
}

describe("append offset", () => {
  it("returns the exact offset of a single appended message", async () => {
    const stream = await createStream();
    const result = await stream.append({ contentType: "text/plain", data: encode("a") });
    expect(result.status).toBe("appended");
    if (result.status !== "appended") throw new Error("expected appended");

    const read = await stream.read({});
    if (read.status !== "ok") throw new Error("expected read");
    expect(read.messages.at(-1)!.offset).toBe(result.offset);
  });

  it("returns the offset of the last message for a multi-message JSON append", async () => {
    const stream = await createStream("application/json");
    const result = await stream.append({
      contentType: "application/json",
      data: encode(JSON.stringify([1, 2, 3])),
    });
    if (result.status !== "appended") throw new Error("expected appended");

    const read = await stream.read({});
    if (read.status !== "ok") throw new Error("expected read");
    expect(read.messages).toHaveLength(3);
    expect(read.messages.at(-1)!.offset).toBe(result.offset);
  });

  it("keeps the tail offset for a body-less close append", async () => {
    const stream = await createStream();
    const appended = await stream.append({ contentType: "text/plain", data: encode("a") });
    if (appended.status !== "appended") throw new Error("expected appended");

    const closed = await stream.append({
      contentType: "text/plain",
      data: new Uint8Array(),
      close: true,
    });
    if (closed.status !== "appended") throw new Error("expected appended");
    expect(closed.closed).toBe(true);
    expect(closed.offset).toBe(appended.offset);
  });

  it("keeps the tail offset when closing an already-closed stream", async () => {
    const stream = await createStream();
    const appended = await stream.append({ contentType: "text/plain", data: encode("a") });
    if (appended.status !== "appended") throw new Error("expected appended");
    await stream.append({ contentType: "text/plain", data: new Uint8Array(), close: true });

    const again = await stream.append({
      contentType: "text/plain",
      data: new Uint8Array(),
      close: true,
    });
    if (again.status !== "appended") throw new Error("expected appended");
    expect(again.closed).toBe(true);
    expect(again.offset).toBe(appended.offset);
  });

  it("returns the tail offset on duplicate producer appends", async () => {
    const stream = await createStream();
    const producer = { producerId: "p", producerEpoch: 1, producerSeq: 0 };
    const first = await stream.append({
      contentType: "text/plain",
      data: encode("a"),
      producer,
    });
    if (first.status !== "appended") throw new Error("expected appended");

    const duplicate = await stream.append({
      contentType: "text/plain",
      data: encode("a"),
      producer,
    });
    expect(duplicate.status).toBe("duplicate");
    if (duplicate.status !== "duplicate") throw new Error("expected duplicate");
    expect(duplicate.offset).toBe(first.offset);
  });
});

describe("append expectedOffset (CAS)", () => {
  it("appends when expectedOffset matches the tail", async () => {
    const stream = await createStream();
    const first = await stream.append({ contentType: "text/plain", data: encode("a") });
    if (first.status !== "appended") throw new Error("expected appended");

    const result = await stream.append({
      contentType: "text/plain",
      data: encode("b"),
      expectedOffset: first.offset,
    });
    expect(result.status).toBe("appended");
  });

  it("conflicts with the actual tail when expectedOffset is stale", async () => {
    const stream = await createStream();
    const first = await stream.append({ contentType: "text/plain", data: encode("a") });
    const second = await stream.append({ contentType: "text/plain", data: encode("b") });
    if (first.status !== "appended" || second.status !== "appended")
      throw new Error("expected appended");

    const result = await stream.append({
      contentType: "text/plain",
      data: encode("c"),
      expectedOffset: first.offset,
    });
    expect(result).toEqual({
      status: "conflict",
      conflictReason: "expected-offset",
      offset: second.offset,
    });

    const read = await stream.read({});
    if (read.status !== "ok") throw new Error("expected read");
    expect(read.messages).toHaveLength(2);
  });

  it("treats ZERO_OFFSET as 'still empty'", async () => {
    const stream = await createStream();
    const onEmpty = await stream.append({
      contentType: "text/plain",
      data: encode("a"),
      expectedOffset: ZERO_OFFSET,
    });
    expect(onEmpty.status).toBe("appended");

    const onNonEmpty = await stream.append({
      contentType: "text/plain",
      data: encode("b"),
      expectedOffset: ZERO_OFFSET,
    });
    expect(onNonEmpty.status).toBe("conflict");
    if (onNonEmpty.status !== "conflict") throw new Error("expected conflict");
    expect(onNonEmpty.conflictReason).toBe("expected-offset");
  });

  it("reports closed before expected-offset on a closed stream", async () => {
    const stream = await createStream();
    await stream.append({ contentType: "text/plain", data: encode("a") });
    await stream.append({ contentType: "text/plain", data: new Uint8Array(), close: true });

    const result = await stream.append({
      contentType: "text/plain",
      data: encode("b"),
      expectedOffset: ZERO_OFFSET,
    });
    if (result.status !== "conflict") throw new Error("expected conflict");
    expect(result.conflictReason).toBe("closed");
  });

  it("reports content-type before expected-offset", async () => {
    const stream = await createStream();
    await stream.append({ contentType: "text/plain", data: encode("a") });

    const result = await stream.append({
      contentType: "application/json",
      data: encode("{}"),
      expectedOffset: ZERO_OFFSET,
    });
    if (result.status !== "conflict") throw new Error("expected conflict");
    expect(result.conflictReason).toBe("content-type");
  });

  it("applies the precondition to close-with-data appends", async () => {
    const stream = await createStream();
    const first = await stream.append({ contentType: "text/plain", data: encode("a") });
    if (first.status !== "appended") throw new Error("expected appended");

    const stale = await stream.append({
      contentType: "text/plain",
      data: encode("b"),
      close: true,
      expectedOffset: ZERO_OFFSET,
    });
    if (stale.status !== "conflict") throw new Error("expected conflict");
    expect(stale.conflictReason).toBe("expected-offset");

    const matched = await stream.append({
      contentType: "text/plain",
      data: encode("b"),
      close: true,
      expectedOffset: first.offset,
    });
    expect(matched.status).toBe("appended");
    if (matched.status !== "appended") throw new Error("expected appended");
    expect(matched.closed).toBe(true);
  });

  it("applies the precondition to close-only appends on an open stream", async () => {
    const stream = await createStream();
    const first = await stream.append({ contentType: "text/plain", data: encode("a") });
    if (first.status !== "appended") throw new Error("expected appended");
    await stream.append({ contentType: "text/plain", data: encode("b") });

    const stale = await stream.append({
      contentType: "text/plain",
      data: new Uint8Array(),
      close: true,
      expectedOffset: first.offset,
    });
    if (stale.status !== "conflict") throw new Error("expected conflict");
    expect(stale.conflictReason).toBe("expected-offset");

    const metadata = await stream.metadata();
    if (metadata.status !== "ok") throw new Error("expected metadata");
    expect(metadata.closed).toBeFalsy();
  });

  it("keeps close-only on an already-closed stream idempotent regardless of expectedOffset", async () => {
    const stream = await createStream();
    const appended = await stream.append({ contentType: "text/plain", data: encode("a") });
    if (appended.status !== "appended") throw new Error("expected appended");
    await stream.append({ contentType: "text/plain", data: new Uint8Array(), close: true });

    const again = await stream.append({
      contentType: "text/plain",
      data: new Uint8Array(),
      close: true,
      expectedOffset: ZERO_OFFSET,
    });
    if (again.status !== "appended") throw new Error("expected appended");
    expect(again.closed).toBe(true);
    expect(again.offset).toBe(appended.offset);
  });

  it("does not advance producer state on a CAS failure", async () => {
    const stream = await createStream();
    const first = await stream.append({ contentType: "text/plain", data: encode("a") });
    if (first.status !== "appended") throw new Error("expected appended");

    const producer = { producerId: "p", producerEpoch: 1, producerSeq: 0 };
    const stale = await stream.append({
      contentType: "text/plain",
      data: encode("b"),
      producer,
      expectedOffset: ZERO_OFFSET,
    });
    if (stale.status !== "conflict") throw new Error("expected conflict");
    expect(stale.conflictReason).toBe("expected-offset");

    // Same epoch/seq retried with the correct precondition: accepted as a
    // first append (not a duplicate) because the failed CAS persisted nothing.
    const retried = await stream.append({
      contentType: "text/plain",
      data: encode("b"),
      producer,
      expectedOffset: first.offset,
    });
    expect(retried.status).toBe("appended");
  });

  it("supports the materialize-validate-append retry loop under interleaving", async () => {
    const stream = await createStream();
    const head = await stream.append({ contentType: "text/plain", data: encode("a") });
    if (head.status !== "appended") throw new Error("expected appended");

    // A wins the race at head H.
    const a = await stream.append({
      contentType: "text/plain",
      data: encode("from-a"),
      expectedOffset: head.offset,
    });
    if (a.status !== "appended") throw new Error("expected appended");

    // B conditioned on the old head loses...
    const b = await stream.append({
      contentType: "text/plain",
      data: encode("from-b"),
      expectedOffset: head.offset,
    });
    if (b.status !== "conflict" || b.conflictReason !== "expected-offset")
      throw new Error("expected expected-offset conflict");

    // ...and succeeds after re-reading the head from the conflict result.
    const retry = await stream.append({
      contentType: "text/plain",
      data: encode("from-b"),
      expectedOffset: b.offset,
    });
    expect(retry.status).toBe("appended");

    const read = await stream.read({});
    if (read.status !== "ok") throw new Error("expected read");
    expect(read.messages.map((m) => new TextDecoder().decode(m.data))).toEqual([
      "a",
      "from-a",
      "from-b",
    ]);
  });
});
