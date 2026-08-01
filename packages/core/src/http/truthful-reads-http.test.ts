import { describe, expect, it } from "vitest";
import { createHttpHandler } from "../http.ts";
import { createReadOnlyHttpHandler } from "../read-only-http.ts";
import { StreamProtocol } from "../protocol.ts";
import { createMemoryStorageAdapter } from "../storage/memory/adapter.ts";

const ZERO = "0000000000000000_0000000000000000";

function harness(options: { now?: () => number; cacheVisibility?: "private" | "public" } = {}) {
  const now = options.now ?? (() => 0);
  const protocol = new StreamProtocol({
    storage: { adapter: createMemoryStorageAdapter() },
    clock: { now, date: (value) => new Date(value ?? now()) },
    longPollTimeoutMs: 5,
  });
  const handler = createHttpHandler({ protocol, cacheVisibility: options.cacheVisibility });
  const readOnly = createReadOnlyHttpHandler({
    protocol,
    cacheVisibility: options.cacheVisibility,
  });
  return { protocol, handler, readOnly };
}

async function create(
  handler: ReturnType<typeof createHttpHandler>,
  options: { ttlSeconds?: number; body?: string } = {},
) {
  const response = await handler.fetch(
    new Request("http://x/s", {
      method: "PUT",
      headers: {
        "content-type": "text/plain",
        ...(options.ttlSeconds === undefined ? {} : { "stream-ttl": String(options.ttlSeconds) }),
      },
      body: options.body,
    }),
  );
  expect(response.status).toBe(201);
  return response;
}

async function append(handler: ReturnType<typeof createHttpHandler>, body: string) {
  const response = await handler.fetch(
    new Request("http://x/s", {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body,
    }),
  );
  expect(response.status).toBe(204);
  return response.headers.get("stream-next-offset")!;
}

describe("truthful read cache policy", () => {
  it("defaults stable catch-up to private and requires explicit public opt-in", async () => {
    const privateHarness = harness();
    await create(privateHarness.handler, { body: "a" });
    const privateRead = await privateHarness.handler.fetch(new Request("http://x/s?offset=-1"));
    expect(privateRead.headers.get("cache-control")).toBe(
      "private, max-age=60, stale-while-revalidate=300",
    );
    expect(privateRead.headers.get("etag")).toBeTruthy();

    const publicHarness = harness({ cacheVisibility: "public" });
    await create(publicHarness.handler, { body: "a" });
    const publicRead = await publicHarness.handler.fetch(new Request("http://x/s?offset=-1"));
    expect(publicRead.headers.get("cache-control")).toBe(
      "public, max-age=60, stale-while-revalidate=300",
    );
  });

  it("gives stable long-poll 200 responses closure-sensitive etags and configured visibility", async () => {
    const open = harness();
    await create(open.handler);
    await append(open.handler, "a");
    const openRead = await open.handler.fetch(
      new Request(`http://x/s?offset=${ZERO}&live=long-poll`),
    );
    expect(openRead.status).toBe(200);
    expect(openRead.headers.get("cache-control")).toContain("private");
    const openEtag = openRead.headers.get("etag");
    expect(openEtag).toBeTruthy();

    const closed = harness();
    await create(closed.handler);
    const lookup = await closed.protocol.get("s");
    if (lookup.status !== "ok") throw new Error("expected stream");
    await lookup.stream.append({
      data: new TextEncoder().encode("a"),
      contentType: "text/plain",
      close: true,
    });
    const closedRead = await closed.handler.fetch(
      new Request(`http://x/s?offset=${ZERO}&live=long-poll`),
    );
    expect(closedRead.status).toBe(200);
    expect(closedRead.headers.get("stream-closed")).toBe("true");
    expect(closedRead.headers.get("etag")).not.toBe(openEtag);
  });

  it("marks long-poll 204 and literal-now long-poll responses no-store", async () => {
    const { handler } = harness();
    await create(handler);
    const stable = await handler.fetch(new Request(`http://x/s?offset=${ZERO}&live=long-poll`));
    expect(stable.status).toBe(204);
    expect(stable.headers.get("cache-control")).toBe("no-store");
    expect(stable.headers.get("etag")).toBeNull();

    const literalNow = await handler.fetch(new Request("http://x/s?offset=now&live=long-poll"));
    expect(literalNow.status).toBe(204);
    expect(literalNow.headers.get("cache-control")).toBe("no-store");
    expect(literalNow.headers.get("etag")).toBeNull();
  });

  it("applies no-store to HEAD 200, 404, and 410 in both HTTP facades", async () => {
    const { handler, readOnly } = harness();
    await create(handler);
    for (const facade of [handler, readOnly]) {
      const present = await facade.fetch(new Request("http://x/s", { method: "HEAD" }));
      expect(present.status).toBe(200);
      expect(present.headers.get("cache-control")).toBe("no-store");
      const missing = await facade.fetch(new Request("http://x/missing", { method: "HEAD" }));
      expect(missing.status).toBe(404);
      expect(missing.headers.get("cache-control")).toBe("no-store");
    }

    expect(
      (
        await handler.fetch(
          new Request("http://x/child", {
            method: "PUT",
            headers: { "content-type": "text/plain", "stream-forked-from": "/s" },
          }),
        )
      ).status,
    ).toBe(201);
    expect((await handler.fetch(new Request("http://x/s", { method: "DELETE" }))).status).toBe(204);
    for (const facade of [handler, readOnly]) {
      const gone = await facade.fetch(new Request("http://x/s", { method: "HEAD" }));
      expect(gone.status).toBe(410);
      expect(gone.headers.get("cache-control")).toBe("no-store");
    }
  });

  it("handles browser preflight for conditional reads in both HTTP facades", async () => {
    const { handler, readOnly } = harness();
    for (const facade of [handler, readOnly]) {
      const response = await facade.fetch(
        new Request("http://x/s", {
          method: "OPTIONS",
          headers: {
            origin: "https://app.test",
            "access-control-request-method": "GET",
            "access-control-request-headers": "If-None-Match",
          },
        }),
      );
      expect(response.status).toBe(204);
      expect(response.headers.get("access-control-allow-origin")).toBe("*");
      expect(response.headers.get("access-control-allow-headers")?.toLowerCase()).toContain(
        "if-none-match",
      );
    }
  });
});

describe("truthful sliding TTL reads", () => {
  it("renews offset=now at origin processing start while HEAD remains non-touching", async () => {
    let time = 0;
    const active = harness({ now: () => time });
    await create(active.handler, { ttlSeconds: 10 });
    time = 9_000;
    const nowRead = await active.handler.fetch(new Request("http://x/s?offset=now"));
    expect(nowRead.status).toBe(200);
    expect(nowRead.headers.get("cache-control")).toBe("no-store");
    expect(nowRead.headers.get("etag")).toBeNull();
    time = 15_000;
    expect((await active.handler.fetch(new Request("http://x/s", { method: "HEAD" }))).status).toBe(
      200,
    );

    time = 0;
    const passive = harness({ now: () => time });
    await create(passive.handler, { ttlSeconds: 10 });
    time = 9_000;
    expect(
      (await passive.handler.fetch(new Request("http://x/s", { method: "HEAD" }))).status,
    ).toBe(200);
    time = 11_000;
    expect(
      (await passive.handler.fetch(new Request("http://x/s", { method: "HEAD" }))).status,
    ).toBe(404);
  });

  it("renews a long-poll when origin processing begins", async () => {
    let time = 0;
    const { handler } = harness({ now: () => time });
    await create(handler, { ttlSeconds: 10 });
    time = 9_000;
    const read = await handler.fetch(new Request(`http://x/s?offset=${ZERO}&live=long-poll`));
    expect(read.status).toBe(204);
    time = 15_000;
    expect((await handler.fetch(new Request("http://x/s", { method: "HEAD" }))).status).toBe(200);
  });
});
