import { describe, expect, it } from "vitest";
import { StreamProtocol } from "../protocol.ts";
import { createMemoryStreamFactory } from "../storage/memory/factory.ts";
import type { ProtocolStream } from "../types/protocol.ts";

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
