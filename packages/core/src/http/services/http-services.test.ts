import { describe, expect, it } from "vitest";
import { HttpHandler, StreamProtocol } from "../../index.ts";
import { createMemoryStorageAdapter } from "../../storage/memory/adapter.ts";

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
    const protocol = new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
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
    const protocol = new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
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

const ZERO = "0000000000000000_0000000000000000";

async function sourceHandler(body: string, contentType = "text/plain"): Promise<HttpHandler> {
  const protocol = new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  const handler = new HttpHandler({ protocol });
  const res = await handler.fetch(
    new Request("http://x/src", { method: "PUT", headers: { "content-type": contentType }, body }),
  );
  expect(res.status).toBe(201);
  return handler;
}

function forkRequest(handler: HttpHandler, headers: Record<string, string>, body?: string) {
  return handler.fetch(new Request("http://x/fork", { method: "PUT", headers, body }));
}

describe("fork sub-offset over HTTP", () => {
  it("materializes a binary sub-offset prefix into the fork", async () => {
    const handler = await sourceHandler("hello");
    const res = await forkRequest(handler, {
      "content-type": "text/plain",
      "stream-forked-from": "/src",
      "stream-fork-offset": ZERO,
      "stream-fork-sub-offset": "3",
    });
    expect(res.status).toBe(201);
    const read = await handler.fetch(new Request("http://x/fork?offset=-1", { method: "GET" }));
    expect(await read.text()).toBe("hel");
  });

  it("appends the initial body after the materialized prefix", async () => {
    const handler = await sourceHandler("hello");
    const res = await forkRequest(
      handler,
      {
        "content-type": "text/plain",
        "stream-forked-from": "/src",
        "stream-fork-offset": ZERO,
        "stream-fork-sub-offset": "3",
      },
      "XY",
    );
    expect(res.status).toBe(201);
    const read = await handler.fetch(new Request("http://x/fork?offset=-1", { method: "GET" }));
    expect(await read.text()).toBe("helXY");
  });

  it("is idempotent on matching sub-offset and conflicts on mismatch", async () => {
    const handler = await sourceHandler("hello");
    const headers = {
      "content-type": "text/plain",
      "stream-forked-from": "/src",
      "stream-fork-offset": ZERO,
      "stream-fork-sub-offset": "2",
    };
    expect((await forkRequest(handler, headers)).status).toBe(201);
    expect((await forkRequest(handler, headers)).status).toBe(200);
    expect((await forkRequest(handler, { ...headers, "stream-fork-sub-offset": "3" })).status).toBe(
      409,
    );
  });

  it("rejects a sub-offset header without Stream-Forked-From (even when 0)", async () => {
    const handler = await sourceHandler("data");
    const res = await forkRequest(handler, {
      "content-type": "text/plain",
      "stream-fork-sub-offset": "0",
    });
    expect(res.status).toBe(400);
  });

  it("rejects a positive sub-offset without Stream-Fork-Offset", async () => {
    const handler = await sourceHandler("data");
    const res = await forkRequest(handler, {
      "content-type": "text/plain",
      "stream-forked-from": "/src",
      "stream-fork-sub-offset": "1",
    });
    expect(res.status).toBe(400);
  });

  it("rejects malformed sub-offset values", async () => {
    const handler = await sourceHandler("data");
    for (const bad of ["-1", "abc", "1.5", "05", "+1"]) {
      const res = await forkRequest(handler, {
        "content-type": "text/plain",
        "stream-forked-from": "/src",
        "stream-fork-offset": ZERO,
        "stream-fork-sub-offset": bad,
      });
      expect(res.status).toBe(400);
    }
  });
});
