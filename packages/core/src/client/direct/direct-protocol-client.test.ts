import { describe, expect, it, vi } from "vitest";
import { createMemoryStorageAdapter } from "../../storage/memory/adapter.ts";
import { StreamProtocol } from "../../protocol.ts";
import { directProtocolClient, hasStreamsyProtocol } from "./client.ts";

function makeClient() {
  const protocol = new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  return { protocol, client: directProtocolClient(protocol) };
}

describe("directProtocolClient", () => {
  it("creates cold handles and exposes the exact Streamsy refinement", async () => {
    const { protocol, client } = makeClient();
    const get = vi.spyOn(protocol, "get");
    const handle = client.stream("cold");

    expect(handle.id).toBe("cold");
    expect(get).not.toHaveBeenCalled();
    expect(hasStreamsyProtocol(client)).toBe(true);
    expect(client.streamsy).toBe(protocol);

    await client.close();
  });

  it("maps head, create, conflict, append, batches, close, and EOF as result objects", async () => {
    const { client } = makeClient();
    const handle = client.stream("text");

    expect(await handle.head()).toEqual({ status: "not-found" });
    expect(
      await handle.create({ contentType: "text/plain; charset=utf-8", initialData: "a" }),
    ).toEqual({ status: "created", contentType: "text/plain; charset=utf-8" });
    expect(await handle.create({ contentType: "text/plain; charset=utf-8" })).toEqual({
      status: "conflict",
    });

    expect(await handle.head()).toMatchObject({
      status: "ok",
      contentType: "text/plain; charset=utf-8",
      closed: false,
    });

    expect(await handle.append("b", { contentType: "text/plain" })).toEqual({ status: "appended" });
    const read = await handle.read();
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected ok read");
    const batches = [];
    for await (const batch of read.session) batches.push(batch);
    expect(batches).toHaveLength(1);
    expect(batches[0]).toMatchObject({
      kind: "text",
      text: "ab",
      upToDate: true,
      streamClosed: false,
    });
    expect(read.session.offset).toBe(batches[0]!.offset);
    await expect(read.session.done).resolves.toEqual({ status: "done" });

    const close = await handle.close({ finalData: "c" });
    expect(close.status).toBe("closed");
    if (close.status !== "closed") throw new Error("expected closed");
    expect(close.finalOffset).not.toBe("");

    const closed = await handle.read({ offset: batches[0]!.offset });
    if (closed.status !== "ok") throw new Error("expected ok read");
    const final = await closed.session[Symbol.asyncIterator]().next();
    expect(final.value).toMatchObject({ kind: "text", text: "c", streamClosed: true });
    await expect(closed.session.done).resolves.toEqual({ status: "done" });

    expect(await handle.append("d", { contentType: "text/plain" })).toEqual({ status: "closed" });
    // Close-only re-close is idempotent and returns the same final offset.
    expect(await handle.close()).toEqual({ status: "closed", finalOffset: close.finalOffset });
    await client.close();
  });

  it("returns bad-request when a direct append omits the content type", async () => {
    const { client } = makeClient();
    const handle = client.stream("no-ct");
    await handle.create({ contentType: "text/plain" });
    expect(await handle.append("x")).toMatchObject({ status: "error", code: "bad-request" });
    await client.close();
  });

  it("uses content-aware byte and JSON batches and delivers empty checkpoints", async () => {
    const { client } = makeClient();
    const bytes = client.stream("bytes");
    await bytes.create();
    await bytes.append(new Uint8Array([1, 2]), { contentType: "application/octet-stream" });
    await bytes.append(new Uint8Array([3]), { contentType: "application/octet-stream" });
    const byteRead = await bytes.read();
    if (byteRead.status !== "ok") throw new Error("expected ok");
    const byteBatch = await byteRead.session[Symbol.asyncIterator]().next();
    expect(byteBatch.value).toMatchObject({
      kind: "bytes",
      data: new Uint8Array([1, 2, 3]),
      upToDate: true,
    });

    const json = client.stream("json");
    await json.create({ contentType: "application/json; charset=utf-8" });
    await json.append('{"n":1}', { contentType: "application/json" });
    await json.append('[{"n":2},{"n":3}]', { contentType: "application/json" });
    const jsonRead = await json.read<{ readonly n: number }>();
    if (jsonRead.status !== "ok") throw new Error("expected ok");
    const jsonBatch = await jsonRead.session[Symbol.asyncIterator]().next();
    expect(jsonBatch.value).toMatchObject({ kind: "json", items: [{ n: 1 }, { n: 2 }, { n: 3 }] });

    const empty = client.stream("empty");
    await empty.create({ contentType: "text/plain" });
    const emptyRead = await empty.read();
    if (emptyRead.status !== "ok") throw new Error("expected ok");
    const emptyBatch = await emptyRead.session[Symbol.asyncIterator]().next();
    expect(emptyBatch.value).toMatchObject({ kind: "text", text: "", upToDate: true });
    await client.close();
  });

  it("cancels live sessions and reports client-closed after client close", async () => {
    const { client } = makeClient();
    const handle = client.stream("live");
    await handle.create({ contentType: "text/plain" });
    const read = await handle.read({ live: "long-poll" });
    if (read.status !== "ok") throw new Error("expected ok");
    const iterator = read.session[Symbol.asyncIterator]();
    await iterator.next();
    const waiting = iterator.next();
    read.session.cancel("done");

    await expect(waiting).resolves.toEqual({ done: true, value: undefined });
    await expect(read.session.done).resolves.toEqual({ status: "cancelled" });
    await client.close();
    expect(await handle.head()).toMatchObject({ status: "error", code: "client-closed" });
  });

  it("returns an aborted failure for already-aborted requests", async () => {
    const { client } = makeClient();
    const controller = new AbortController();
    controller.abort("stop");
    expect(await client.stream("x").head({ signal: controller.signal })).toMatchObject({
      status: "error",
      code: "aborted",
    });
    await client.close();
  });
});
