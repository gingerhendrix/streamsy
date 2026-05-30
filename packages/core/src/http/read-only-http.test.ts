import { describe, expect, it, vi } from "vitest";
import {
  createReadOnlyHttpHandler,
  ReadOnlyHttpHandler,
  StreamProtocol,
  type ProtocolStream,
  type StreamProtocolFactory,
} from "../index.ts";
import { createInMemoryFactory } from "../protocol/test-memory-factory.ts";

const enc = new TextEncoder();

async function createProtocolWithStream() {
  const protocol = new StreamProtocol({ storage: { factory: createInMemoryFactory() } });
  const created = await protocol.create("example", { contentType: "text/plain" });
  expect(created.status).toBe("created");
  if (created.status !== "created") throw new Error("expected created stream");
  await created.stream.append({ data: enc.encode("hi"), contentType: "text/plain" });
  return protocol;
}

describe("ReadOnlyHttpHandler", () => {
  it("serves existing streams through GET and HEAD", async () => {
    const protocol = await createProtocolWithStream();
    const handler = createReadOnlyHttpHandler({ protocol });

    const get = await handler.fetch(new Request("http://x/example", { method: "GET" }));
    expect(get.status).toBe(200);
    expect(get.headers.get("content-type")).toBe("text/plain");
    const nextOffset = get.headers.get("stream-next-offset");
    expect(nextOffset).toBeTruthy();
    expect(await get.text()).toBe("hi");

    const head = await handler.fetch(new Request("http://x/example", { method: "HEAD" }));
    expect(head.status).toBe(200);
    expect(head.headers.get("content-type")).toBe("text/plain");
    expect(head.headers.get("stream-next-offset")).toBe(nextOffset);
    expect(await head.text()).toBe("");
  });

  it("rejects write methods without binding or mutating the stream", async () => {
    const protocol = await createProtocolWithStream();
    const getSpy = vi.spyOn(protocol, "get");
    const createSpy = vi.spyOn(protocol, "create");
    const handler = new ReadOnlyHttpHandler({ protocol });

    for (const method of ["PUT", "POST", "DELETE", "PATCH"]) {
      const response = await handler.fetch(
        new Request("http://x/example", { method, body: method === "GET" ? undefined : "ignored" }),
      );
      expect(response.status).toBe(405);
      expect(await response.text()).toBe("Method not allowed");
    }

    expect(getSpy).not.toHaveBeenCalled();
    expect(createSpy).not.toHaveBeenCalled();

    const stillReadable = await handler.fetch(new Request("http://x/example", { method: "GET" }));
    expect(stillReadable.status).toBe(200);
    expect(await stillReadable.text()).toBe("hi");
  });

  it("preserves path prefixes and missing-stream behavior for allowed reads", async () => {
    const protocol = await createProtocolWithStream();
    const handler = createReadOnlyHttpHandler({ protocol, pathPrefix: "/public" });

    const get = await handler.fetch(new Request("http://x/public/example", { method: "GET" }));
    expect(get.status).toBe(200);
    expect(await get.text()).toBe("hi");

    const missing = await handler.fetch(new Request("http://x/public/missing", { method: "GET" }));
    expect(missing.status).toBe(404);
  });

  it("can expose a read-write protocol stream through a read-only HTTP facade", async () => {
    let appendCalls = 0;
    let deleteCalls = 0;
    const stream: ProtocolStream = {
      id: "example",
      append: async () => {
        appendCalls++;
        return { status: "appended", nextOffset: "1_0" };
      },
      read: async () => ({
        status: "ok",
        messages: [{ offset: "1_0", data: enc.encode("hi"), timestamp: 0 }],
        nextOffset: "1_0",
        upToDate: true,
      }),
      readLive: async () => ({
        status: "timeout",
        messages: [],
        nextOffset: "1_0",
        upToDate: true,
        cursor: "c1",
      }),
      metadata: async () => ({ status: "ok", contentType: "text/plain", nextOffset: "1_0" }),
      delete: async () => {
        deleteCalls++;
        return { status: "ok" };
      },
    };
    const create = vi.fn<StreamProtocolFactory["create"]>(async () => ({
      status: "exists",
      stream,
      contentType: "text/plain",
      nextOffset: "1_0",
    }));
    const get = vi.fn<StreamProtocolFactory["get"]>(async () => ({ status: "ok", stream }));
    const protocol: StreamProtocolFactory = { create, get };
    const handler = createReadOnlyHttpHandler({ protocol });

    const response = await handler.fetch(new Request("http://x/example", { method: "GET" }));
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("hi");

    expect(create).not.toHaveBeenCalled();
    expect(appendCalls).toBe(0);
    expect(deleteCalls).toBe(0);
  });
});
