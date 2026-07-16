import { createHttpHandler, createMemoryStorageAdapter, StreamProtocol } from "@streamsy/core";
import type { ClientReadResult, JsonValue } from "@streamsy/core";
import { describe, expect, it, vi } from "vitest";
import { officialProtocolClient } from "./client.ts";
import { protocolPathUrl } from "./url.ts";

const noRetry = { initialDelay: 1, maxDelay: 1, multiplier: 1, maxRetries: 0 };

function makeHarness(options: { headers?: Record<string, string | (() => string)> } = {}) {
  const protocol = new StreamProtocol({
    storage: { adapter: createMemoryStorageAdapter() },
    longPollTimeoutMs: 50,
  });
  const handler = createHttpHandler({ protocol, pathPrefix: "/streams" });
  const requests: Request[] = [];
  const fetch = (async (input: Parameters<typeof globalThis.fetch>[0], init?: RequestInit) => {
    const request = new Request(input, init);
    requests.push(request.clone());
    return handler.fetch(request);
  }) as typeof globalThis.fetch;
  const client = officialProtocolClient({
    urlFor: (id) => protocolPathUrl("https://stream.test/streams", id),
    fetch,
    headers: options.headers,
    backoffOptions: noRetry,
    warnOnHttp: false,
  });
  return { client, protocol, requests };
}

async function okSession<T extends JsonValue>(result: ClientReadResult<T>) {
  if (result.status !== "ok") throw new Error(`expected ok read, got ${result.status}`);
  return result.session;
}

describe("officialProtocolClient", () => {
  it("constructs cold official handles", async () => {
    const { client, requests } = makeHarness();
    const handle = client.stream("cold/path");
    expect(handle.id).toBe("cold/path");
    expect(requests).toHaveLength(0);
    await client.close();
  });

  it("maps create-only, head, append, catch-up batches, close, and errors to results", async () => {
    const { client } = makeHarness();
    const handle = client.stream("text");

    expect(await handle.head()).toEqual({ status: "not-found" });
    expect(await handle.create({ contentType: "text/plain", initialData: "a" })).toMatchObject({
      status: "created",
    });
    expect(await handle.create({ contentType: "application/octet-stream" })).toEqual({
      status: "conflict",
    });
    expect(await handle.append("b", { contentType: "text/plain" })).toEqual({ status: "appended" });

    expect(await handle.head()).toMatchObject({
      status: "ok",
      contentType: "text/plain",
      closed: false,
    });

    const session = await okSession(await handle.read());
    const batches = [];
    for await (const batch of session) batches.push(batch);
    expect(batches).toHaveLength(1);
    expect(batches[0]).toMatchObject({
      kind: "text",
      text: "ab",
      upToDate: true,
      streamClosed: false,
    });
    await expect(session.done).resolves.toEqual({ status: "done" });

    const result = await handle.close({ finalData: "c" });
    if (result.status !== "closed") throw new Error("expected closed");
    const resumed = await okSession(await handle.read({ offset: batches[0]!.offset }));
    const final = await resumed[Symbol.asyncIterator]().next();
    expect(final.value).toMatchObject({
      kind: "text",
      text: "c",
      offset: result.finalOffset,
      streamClosed: true,
    });
    expect(await handle.append("d", { contentType: "text/plain" })).toEqual({ status: "closed" });
    await client.close();
  });

  it("uses official JSON and binary subscriptions, including empty checkpoints", async () => {
    const { client } = makeHarness();
    const json = client.stream("json");
    await json.create({ contentType: "application/json; charset=utf-8" });
    await json.append('{"n":1}', { contentType: "application/json" });
    await json.append('{"n":2}', { contentType: "application/json" });
    await json.append('{"n":3}', { contentType: "application/json" });
    const jsonSession = await okSession(await json.read<{ readonly n: number }>());
    const jsonBatch = await jsonSession[Symbol.asyncIterator]().next();
    expect(jsonBatch.value).toMatchObject({ kind: "json", items: [{ n: 1 }, { n: 2 }, { n: 3 }] });

    const bytes = client.stream("bytes");
    await bytes.create();
    await bytes.append(new Uint8Array([1, 2]), { contentType: "application/octet-stream" });
    await bytes.append(new Uint8Array([3]), { contentType: "application/octet-stream" });
    const byteSession = await okSession(await bytes.read());
    const byteBatch = await byteSession[Symbol.asyncIterator]().next();
    expect(byteBatch.value).toMatchObject({
      kind: "bytes",
      data: new Uint8Array([1, 2, 3]),
      upToDate: true,
    });

    const empty = client.stream("empty");
    await empty.create({ contentType: "text/plain" });
    const emptySession = await okSession(await empty.read());
    const emptyBatch = await emptySession[Symbol.asyncIterator]().next();
    expect(emptyBatch.value).toMatchObject({ kind: "text", text: "", upToDate: true });
    await client.close();
  });

  it("keeps official dynamic headers dynamic and disables append coalescing by default", async () => {
    let token = 0;
    const { client, requests } = makeHarness({
      headers: { authorization: () => `Bearer ${++token}` },
    });
    const handle = client.stream("headers");
    await handle.create({ contentType: "text/plain" });
    await Promise.all([
      handle.append("a", { contentType: "text/plain" }),
      handle.append("b", { contentType: "text/plain" }),
    ]);

    expect(requests.filter((request) => request.method === "POST")).toHaveLength(2);
    expect(requests.map((request) => request.headers.get("authorization"))).toEqual([
      "Bearer 1",
      "Bearer 2",
      "Bearer 3",
    ]);
    await client.close();
  });

  it("cancels live reads and reports client-closed after client close", async () => {
    const { client } = makeHarness();
    const handle = client.stream("live");
    await handle.create({ contentType: "text/plain" });
    const session = await okSession(await handle.read({ live: "long-poll" }));
    const iterator = session[Symbol.asyncIterator]();
    await iterator.next();
    await client.close("shutdown");

    await expect(session.done).resolves.toEqual({ status: "cancelled" });
    expect(await handle.head()).toMatchObject({ status: "error", code: "client-closed" });
  });

  it("delegates binary SSE decoding to the official response", async () => {
    const { client } = makeHarness();
    const handle = client.stream("binary-sse");
    await handle.create();
    const session = await okSession(await handle.read({ live: "sse" }));
    const iterator = session[Symbol.asyncIterator]();
    await iterator.next();
    await handle.append(new Uint8Array([0, 127, 255]), { contentType: "application/octet-stream" });
    let delivered = await iterator.next();
    while (
      !delivered.done &&
      delivered.value.kind === "bytes" &&
      delivered.value.data.byteLength === 0
    ) {
      delivered = await iterator.next();
    }
    expect(delivered.value).toMatchObject({ kind: "bytes", data: new Uint8Array([0, 127, 255]) });
    session.cancel();
    await client.close();
  });

  it("delivers an empty EOF batch for a close-only SSE transition", async () => {
    const { client } = makeHarness();
    const handle = client.stream("close-only-sse");
    await handle.create({ contentType: "text/plain" });
    const session = await okSession(await handle.read({ live: "sse" }));
    const iterator = session[Symbol.asyncIterator]();
    await iterator.next();
    await handle.close();

    let delivered = await iterator.next();
    while (!delivered.done && !delivered.value.streamClosed) {
      delivered = await iterator.next();
    }
    expect(delivered.value).toMatchObject({ kind: "text", text: "", streamClosed: true });
    await expect(session.done).resolves.toEqual({ status: "done" });
    await client.close();
  });

  it("normalizes transport and abort failures to results", async () => {
    const network = officialProtocolClient({
      urlFor: () => "https://stream.test/fail",
      fetch: vi.fn(async () => {
        throw new TypeError("offline");
      }) as unknown as typeof globalThis.fetch,
      backoffOptions: noRetry,
    });
    expect(await network.stream("x").head()).toMatchObject({
      status: "error",
      code: "transport",
      retryable: true,
    });
    await network.close();

    const { client } = makeHarness();
    const controller = new AbortController();
    controller.abort("stop");
    expect(await client.stream("x").head({ signal: controller.signal })).toMatchObject({
      status: "error",
      code: "aborted",
    });
    await client.close();
  });
});

describe("protocolPathUrl", () => {
  it("safely appends encoded id segments", () => {
    expect(protocolPathUrl("https://example.com/api/streams", "tenant a/topic#1").toString()).toBe(
      "https://example.com/api/streams/tenant%20a/topic%231",
    );
  });

  it.each(["", "/a", "a/", "a//b", ".", "a/../b"])("rejects unsafe id %j", (id) => {
    expect(() => protocolPathUrl("https://example.com/streams", id)).toThrow(TypeError);
  });
});
