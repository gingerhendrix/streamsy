import { describe, expect, it } from "vitest";
import { HttpHandler, StreamProtocol } from "../../index.ts";
import { createMemoryStreamFactory } from "../../storage/memory/factory.ts";

function append(handler: HttpHandler, body: string, expectedOffset?: string) {
  return handler.fetch(
    new Request("http://x/s", {
      method: "POST",
      headers: {
        "content-type": "text/plain",
        ...(expectedOffset !== undefined ? { "stream-expected-offset": expectedOffset } : {}),
      },
      body,
    }),
  );
}

describe("HTTP services with bound protocol streams", () => {
  it("resolves streams through protocol get for existing-stream operations", async () => {
    const protocol = new StreamProtocol({ storage: { factory: createMemoryStreamFactory() } });
    const handler = new HttpHandler({ protocol });
    const put = await handler.fetch(
      new Request("http://x/example", { method: "PUT", headers: { "content-type": "text/plain" } }),
    );
    expect(put.status).toBe(201);
    const post = await handler.fetch(
      new Request("http://x/example", {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "hi",
      }),
    );
    expect(post.status).toBe(204);
    const head = await handler.fetch(new Request("http://x/example", { method: "HEAD" }));
    expect(head.status).toBe(200);
    const get = await handler.fetch(new Request("http://x/example", { method: "GET" }));
    expect(get.status).toBe(200);
    expect(await get.text()).toBe("hi");
  });
});

describe("append expectedOffset over HTTP (Streamsy extension)", () => {
  async function setup() {
    const protocol = new StreamProtocol({ storage: { factory: createMemoryStreamFactory() } });
    const handler = new HttpHandler({ protocol });
    const put = await handler.fetch(
      new Request("http://x/s", { method: "PUT", headers: { "content-type": "text/plain" } }),
    );
    expect(put.status).toBe(201);
    return handler;
  }

  it("appends when the stream-expected-offset header matches the tail", async () => {
    const handler = await setup();
    const first = await append(handler, "a");
    expect(first.status).toBe(204);
    const head = first.headers.get("stream-next-offset")!;

    const second = await append(handler, "b", head);
    expect(second.status).toBe(204);
    expect(second.headers.get("stream-next-offset")).not.toBe(head);
  });

  it("returns 409 with the actual tail on a stale precondition", async () => {
    const handler = await setup();
    const first = await append(handler, "a");
    const stale = first.headers.get("stream-next-offset")!;
    const second = await append(handler, "b");
    const head = second.headers.get("stream-next-offset")!;

    const conflict = await append(handler, "c", stale);
    expect(conflict.status).toBe(409);
    expect(conflict.headers.get("stream-closed")).toBeNull();
    expect(conflict.headers.get("stream-next-offset")).toBe(head);
    expect(await conflict.text()).toBe("Expected offset mismatch");
  });

  it("rejects a malformed stream-expected-offset header with 400", async () => {
    const handler = await setup();
    const response = await append(handler, "a", "not-an-offset");
    expect(response.status).toBe(400);
    expect(await response.text()).toBe("Invalid expected offset");
  });
});
