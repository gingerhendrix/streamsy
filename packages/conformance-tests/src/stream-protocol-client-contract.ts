import type {
  ClientReadResult,
  JsonValue,
  StreamProtocolClient,
  StreamReadSession,
} from "@streamsy/core";
import { afterEach, beforeEach, expect, it } from "vitest";

export interface StreamProtocolClientContractHarness {
  client: StreamProtocolClient;
}

export type MakeStreamProtocolClientHarness = () =>
  | StreamProtocolClientContractHarness
  | Promise<StreamProtocolClientContractHarness>;

function session<T extends JsonValue>(result: ClientReadResult<T>): StreamReadSession<T> {
  if (result.status !== "ok") throw new Error(`expected an ok read, got ${result.status}`);
  return result.session;
}

async function firstBatch<T extends JsonValue>(result: ClientReadResult<T>) {
  const value = (await session(result)[Symbol.asyncIterator]().next()).value;
  if (!value) throw new Error("expected a batch");
  return value;
}

/**
 * Shared common-denominator behavior for direct and official client adapters.
 * Every outcome is asserted as a result object; iteration never throws.
 */
export function runStreamProtocolClientContract(
  makeHarness: MakeStreamProtocolClientHarness,
): void {
  let harness: StreamProtocolClientContractHarness;

  beforeEach(async () => {
    harness = await makeHarness();
  });

  afterEach(async () => {
    await harness.client.close();
  });

  it("provides cold handles and HEAD-grade metadata", async () => {
    const handle = harness.client.stream("metadata");
    expect(handle.id).toBe("metadata");
    expect(await handle.head()).toEqual({ status: "not-found" });

    expect(await handle.create({ contentType: "text/plain", ttlSeconds: 60 })).toMatchObject({
      status: "created",
    });
    expect(await handle.head()).toMatchObject({
      status: "ok",
      contentType: "text/plain",
      closed: false,
    });
  });

  it("uses create-only conflict semantics", async () => {
    const handle = harness.client.stream("create-conflict");
    expect(await handle.create({ contentType: "text/plain" })).toMatchObject({ status: "created" });
    expect(await handle.create({ contentType: "application/octet-stream" })).toEqual({
      status: "conflict",
    });
  });

  it("delivers text batches and resumes after delivered offsets", async () => {
    const handle = harness.client.stream("resume");
    expect(await handle.create({ contentType: "text/plain", initialData: "a" })).toMatchObject({
      status: "created",
    });
    expect(await handle.append("b", { contentType: "text/plain" })).toMatchObject({
      status: "appended",
      offset: expect.any(String),
    });
    const first = await firstBatch(await handle.read());
    expect(first).toMatchObject({ kind: "text", text: "ab", upToDate: true });

    expect(await handle.append("c", { contentType: "text/plain" })).toMatchObject({
      status: "appended",
      offset: expect.any(String),
    });
    const resumed = await firstBatch(await handle.read({ offset: first.offset }));
    expect(resumed).toMatchObject({ kind: "text", text: "c", upToDate: true });
  });

  it("delivers JSON items, concatenated bytes, and empty up-to-date batches", async () => {
    const json = harness.client.stream("json");
    await json.create({ contentType: "application/json; charset=utf-8" });
    await json.append('{"n":1}', { contentType: "application/json" });
    await json.append('{"n":2}', { contentType: "application/json" });
    expect(await firstBatch(await json.read<{ readonly n: number }>())).toMatchObject({
      kind: "json",
      items: [{ n: 1 }, { n: 2 }],
    });

    const bytes = harness.client.stream("bytes");
    await bytes.create();
    await bytes.append(new Uint8Array([1, 2]), { contentType: "application/octet-stream" });
    await bytes.append(new Uint8Array([3]), { contentType: "application/octet-stream" });
    expect(await firstBatch(await bytes.read())).toMatchObject({
      kind: "bytes",
      data: new Uint8Array([1, 2, 3]),
    });

    const empty = harness.client.stream("empty");
    await empty.create({ contentType: "text/plain" });
    expect(await firstBatch(await empty.read())).toMatchObject({
      kind: "text",
      text: "",
      upToDate: true,
    });
  });

  it("observes atomic final-data close as EOF", async () => {
    const handle = harness.client.stream("closed");
    await handle.create({ contentType: "text/plain", initialData: "a" });
    const offset = (await firstBatch(await handle.read())).offset;
    const close = await handle.close({ finalData: "z" });
    if (close.status !== "closed") throw new Error(`expected closed, got ${close.status}`);

    const final = await firstBatch(await handle.read({ offset }));
    expect(final).toMatchObject({
      kind: "text",
      text: "z",
      offset: close.finalOffset,
      streamClosed: true,
    });
  });

  it("preserves producer, CAS, and append-conflict outcomes without payload verification", async () => {
    const handle = harness.client.stream("rich-append");
    await handle.create({ contentType: "text/plain" });
    const initial = await handle.head();
    if (initial.status !== "ok" || initial.offset === undefined) {
      throw new Error("expected initial offset");
    }
    const producer = { producerId: "lane", producerEpoch: 1, producerSeq: 0 };

    const accepted = await handle.append("original", {
      contentType: "text/plain",
      producer,
      expectedOffset: initial.offset,
    });
    expect(accepted).toMatchObject({
      status: "appended",
      offset: expect.any(String),
      producerEpoch: 1,
      producerSeq: 0,
    });
    const changedRetry = await handle.append("changed", {
      contentType: "text/plain",
      producer,
      expectedOffset: initial.offset,
    });
    expect(changedRetry).toMatchObject({
      status: "duplicate",
      offset: expect.any(String),
      producerEpoch: 1,
      producerSeq: 0,
    });
    expect(changedRetry).not.toHaveProperty("verified");
    expect(changedRetry).not.toHaveProperty("verifiedDuplicate");
    expect(
      await handle.append("gap", {
        contentType: "text/plain",
        producer: { ...producer, producerSeq: 2 },
      }),
    ).toEqual({ status: "producer-gap", expectedSeq: 1, receivedSeq: 2 });
    expect(
      await handle.append("invalid-new-epoch", {
        contentType: "text/plain",
        producer: { ...producer, producerEpoch: 2, producerSeq: 1 },
      }),
    ).toEqual({ status: "invalid-epoch-seq" });
    expect(
      await handle.append("new-epoch", {
        contentType: "text/plain",
        producer: { ...producer, producerEpoch: 2, producerSeq: 0 },
      }),
    ).toMatchObject({ status: "appended", producerEpoch: 2, producerSeq: 0 });
    expect(
      await handle.append("stale", {
        contentType: "text/plain",
        producer: { ...producer, producerSeq: 1 },
      }),
    ).toEqual({ status: "stale-epoch", currentEpoch: 2 });

    const read = await firstBatch(await handle.read());
    expect(read).toMatchObject({ kind: "text", text: "originalnew-epoch" });

    const cas = harness.client.stream("cas-conflict");
    await cas.create({ contentType: "text/plain" });
    const empty = await cas.head();
    if (empty.status !== "ok" || empty.offset === undefined) throw new Error("expected offset");
    await cas.append("racer", { contentType: "text/plain" });
    const conflict = await cas.append("loser", {
      contentType: "text/plain",
      expectedOffset: empty.offset,
    });
    expect(conflict).toMatchObject({
      status: "conflict",
      conflictReason: "expected-offset",
      offset: expect.any(String),
    });
    if (conflict.status !== "conflict" || conflict.conflictReason !== "expected-offset") {
      throw new Error("expected offset conflict");
    }
    expect(conflict.offset).not.toBe(empty.offset);

    expect(await cas.append("wrong-content", { contentType: "application/octet-stream" })).toEqual({
      status: "conflict",
      conflictReason: "content-type",
    });
    await cas.append("sequenced", { contentType: "text/plain", seq: "b" });
    expect(await cas.append("old-sequence", { contentType: "text/plain", seq: "a" })).toEqual({
      status: "conflict",
      conflictReason: "sequence",
    });
    const closed = await cas.close();
    if (closed.status !== "closed") throw new Error("expected close");
    expect(await cas.append("after-close", { contentType: "text/plain" })).toEqual({
      status: "closed",
      offset: closed.finalOffset,
    });
  });

  it("cancels live sessions without failing them", async () => {
    const handle = harness.client.stream("cancel");
    await handle.create({ contentType: "text/plain" });
    const read = await handle.read({ live: "long-poll" });
    const active = session(read);
    const iterator = active[Symbol.asyncIterator]();
    await iterator.next();
    const waiting = iterator.next();
    active.cancel();
    await expect(waiting).resolves.toEqual({ done: true, value: undefined });
    await expect(active.done).resolves.toEqual({ status: "cancelled" });
  });

  it.each(["long-poll", "sse"] as const)("delivers %s live updates", async (live) => {
    const handle = harness.client.stream(`live-${live}`);
    await handle.create({ contentType: "text/plain" });
    const active = session(await handle.read({ live }));
    const iterator = active[Symbol.asyncIterator]();
    expect((await iterator.next()).value).toMatchObject({ text: "", upToDate: true });

    await handle.append("update", { contentType: "text/plain" });
    let delivered = await iterator.next();
    while (!delivered.done && delivered.value.kind === "text" && delivered.value.text === "") {
      delivered = await iterator.next();
    }
    expect(delivered.value).toMatchObject({ kind: "text", text: "update", upToDate: true });
    active.cancel();
  });

  it("breaking iteration cancels the session", async () => {
    const handle = harness.client.stream("break");
    await handle.create({ contentType: "text/plain" });
    const active = session(await handle.read({ live: "long-poll" }));
    for await (const batch of active) {
      void batch;
      break;
    }
    await expect(active.done).resolves.toEqual({ status: "cancelled" });
  });

  it("client close cancels sessions and rejects later operations", async () => {
    const handle = harness.client.stream("dispose");
    await handle.create({ contentType: "text/plain" });
    const active = session(await handle.read({ live: "long-poll" }));
    await active[Symbol.asyncIterator]().next();
    await harness.client.close();

    await expect(active.done).resolves.toEqual({ status: "cancelled" });
    expect(await handle.head()).toMatchObject({ status: "error", code: "client-closed" });
  });
}
