/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- Exercise the installed patched runtime at its HTTP boundary with a controlled native Response body. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { Array as Arr, Effect } from "effect";
import { stream } from "@durable-streams/client";
import { durableStreamConnection } from "@durable-streams/tanstack-ai-transport";

const url = "http://raw-replay.test/responses/controlled";

const chunks = [
  { type: "TEXT_MESSAGE_CONTENT", messageId: "message", delta: "partial" },
  { type: "CUSTOM", name: "durable-response.stopped", value: { by: "reader", reason: "Enough" } },
  { type: "RUN_FINISHED", threadId: "thread", runId: "run", outcome: { type: "cancelled" } },
];

const turn = () => Effect.runPromise(Effect.sleep(1));

const bodyGate = (body: string, failure?: Error) => {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();

  class DelayedResponse extends Response {
    override async text() {
      entered.resolve();
      await release.promise;

      if (failure) throw failure;

      return body;
    }
  }

  const response = new DelayedResponse(null, {
    headers: {
      "content-type": "application/json",
      "stream-next-offset": "0000000000000001_0000000000000001",
      "stream-up-to-date": "true",
      "stream-closed": "true",
    },
  });

  return { response, entered: entered.promise, release: () => release.resolve() };
};

const raw = () =>
  durableStreamConnection({ sendUrl: url, readUrl: url, emitSnapshotOnSubscribe: false });

afterEach(() => vi.unstubAllGlobals());

describe("patched 0.0.10 raw native item consumption", () => {
  it("establishes that native close notification precedes the gated final callback", async () => {
    const gate = bodyGate(JSON.stringify(chunks));

    const response = await stream({
      url,
      json: true,
      live: "sse",
      fetch: Object.assign(async () => gate.response, { preconnect: fetch.preconnect }),
    });

    const delivered = Promise.withResolvers<unknown>();
    const unsubscribe = response.subscribeJson((batch) => delivered.resolve(batch.items));

    try {
      await gate.entered;
      await response.closed;
      expect(await Promise.race([delivered.promise, Promise.resolve("not consumed")])).toBe(
        "not consumed",
      );
      gate.release();
      expect(await delivered.promise).toEqual(chunks);
    } finally {
      gate.release();
      unsubscribe();
    }
  });

  it.each([
    { name: "valid", body: JSON.stringify(chunks), expected: chunks },
    { name: "empty", body: "[]", expected: [] },
  ])("waits for delayed $name closed bodies, then completes in raw order", async (test) => {
    const gate = bodyGate(test.body);
    vi.stubGlobal("fetch", async () => gate.response);
    let settled = false;

    const result = Array.fromAsync(raw().subscribe()).finally(() => {
      settled = true;
    });

    try {
      await gate.entered;
      await turn();
      expect(settled).toBe(false);
      gate.release();
      expect(await result).toEqual(test.expected);
    } finally {
      gate.release();
    }
  });

  it.each([
    { name: "malformed JSON", body: "{", failure: undefined, message: /Failed to parse JSON/ },
    {
      name: "failed body",
      body: "",
      failure: Error("Final body failed"),
      message: /Final body failed/,
    },
  ])("propagates delayed $name failure instead of dropping it or hanging", async (test) => {
    const gate = bodyGate(test.body, test.failure);
    vi.stubGlobal("fetch", async () => gate.response);
    const result = Array.fromAsync(raw().subscribe());
    const assertion = expect(result).rejects.toThrow(test.message);

    try {
      await gate.entered;
      gate.release();
      await assertion;
    } finally {
      gate.release();
    }
  });

  it("propagates a live SSE failure without an unhandled lifecycle rejection", async () => {
    const reading = Promise.withResolvers<void>();
    const source = Promise.withResolvers<ReadableStreamDefaultController<Uint8Array>>();

    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        source.resolve(controller);
      },
      pull() {
        reading.resolve();
      },
    });

    vi.stubGlobal(
      "fetch",
      async () =>
        new Response(body, {
          headers: { "content-type": "text/event-stream", "stream-next-offset": "0" },
        }),
    );
    const assertion = expect(Array.fromAsync(raw().subscribe())).rejects.toThrow("Live SSE failed");
    await reading.promise;
    (await source.promise).error(Error("Live SSE failed"));
    await assertion;
    await turn();
  });

  it("abort wakes a pending body read and removes its listener without yielding late items", async () => {
    const gate = bodyGate(JSON.stringify(chunks));
    vi.stubGlobal("fetch", async () => gate.response);
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const result = Array.fromAsync(raw().subscribe(controller.signal));

    try {
      await gate.entered;
      controller.abort("Reader detached");
      expect(await result).toEqual([]);
      expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
      gate.release();
      await turn();
    } finally {
      gate.release();
    }
  });

  it("consumer return cancels remaining native items; repeated subscriptions retain the explicit start", async () => {
    const requests: string[] = [];
    vi.stubGlobal("fetch", async (input: string | URL) => {
      requests.push(String(input));
      const gate = bodyGate(JSON.stringify(chunks));
      gate.release();

      return gate.response;
    });

    const connection = durableStreamConnection({
      sendUrl: url,
      readUrl: url,
      initialOffset: "explicit-start",
      emitSnapshotOnSubscribe: false,
    });

    const reader = connection.subscribe()[Symbol.asyncIterator]();
    expect((await reader.next()).value).toEqual(chunks[0]);
    await reader.return?.();
    expect(await Array.fromAsync(connection.subscribe())).toEqual(chunks);
    expect(requests).toHaveLength(2);
    expect(Arr.map(requests, (request) => new URL(request).searchParams.get("offset"))).toEqual([
      "explicit-start",
      "explicit-start",
    ]);
  });
});
