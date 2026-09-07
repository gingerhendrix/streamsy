import { expect, test } from "bun:test";
import { Effect, Layer, Option, Stream } from "effect";
import { HttpServerRequest } from "effect/unstable/http";
import { Offset, Storage, StreamId, ZERO_OFFSET } from "@streamsy/core";
import type { StoredMessage, StreamRecord } from "@streamsy/core";
import { decodeFrames } from "./fork-frames.ts";
import { forkSource } from "./fork-source.ts";

const sourceId = StreamId.make("src");

const makeMessages = (count: number): ReadonlyArray<StoredMessage> => {
  return Array.from({ length: count }, (_, index) => {
    const offset = `0000000000000000_${String(index + 1).padStart(16, "0")}`;
    return { offset: Offset.make(offset), timestamp: index, data: new Uint8Array([index]) };
  });
};

const makeRecord = (currentOffset: StoredMessage["offset"], softDeleted = false): StreamRecord => ({
  id: sourceId,
  config: { contentType: "text/plain", createdAt: 100 },
  lifecycle: { closed: false, softDeleted },
  currentOffset,
});

const makeStorage = (
  record: Option.Option<StreamRecord>,
  messages: ReadonlyArray<StoredMessage>,
  calls: Array<number>,
) =>
  Storage.of({
    capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
    record: (id) => Effect.succeed(id === sourceId ? record : Option.none()),
    messages: (id, window) => {
      const limit = Math.max(0, Math.trunc(window.limit ?? messages.length));
      calls.push(limit);
      if (id !== sourceId) return Effect.succeed([]);
      return Effect.succeed(
        messages
          .filter(
            (message) =>
              (window.after === undefined || message.offset > window.after) &&
              (window.until === undefined || message.offset <= window.until),
          )
          .slice(0, limit),
      );
    },
    producer: () => Effect.succeed(Option.none()),
    mutate: () => Effect.die("unused"),
    changes: () => Stream.empty,
    nextExpiry: Effect.succeed(Option.none()),
  });

const runExport = async (url: string, storage: typeof Storage.Service) =>
  Effect.runPromise(
    forkSource({ pathPrefix: "/streams", copyOnForkMaxBytes: 8 * 1024 * 1024 }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(Storage, storage),
          Layer.succeed(
            HttpServerRequest.HttpServerRequest,
            HttpServerRequest.fromWeb(new Request(url)),
          ),
        ),
      ),
    ),
  );

test("fork-source pages a 17-message prefix in groups of 16", async () => {
  const messages = makeMessages(17);
  const calls: Array<number> = [];
  const result = await runExport(
    `https://streamsy.internal/fork-source?stream=src&budget=10000&tail=0`,
    makeStorage(Option.some(makeRecord(messages[16]?.offset ?? ZERO_OFFSET)), messages, calls),
  );
  expect(result.status).toBe(200);
  expect(decodeFrames(new Uint8Array(await result.arrayBuffer()))).toEqual(messages);
  expect(calls).toEqual([16, 16]);
});

test("fork-source includes a bounded sub-offset tail and honors the exact frame boundary", async () => {
  const messages = makeMessages(2);
  const forkOffset = messages[0]?.offset ?? ZERO_OFFSET;
  const calls: Array<number> = [];
  const exact = await runExport(
    `https://streamsy.internal/fork-source?stream=src&until=${forkOffset}&tail=0&budget=46`,
    makeStorage(Option.some(makeRecord(messages[1]?.offset ?? ZERO_OFFSET)), messages, calls),
  );
  expect(exact.headers.get("streamsy-frames-truncated")).toBeNull();
  const first = messages[0];
  if (first === undefined) throw new Error("missing first test message");
  expect(decodeFrames(new Uint8Array(await exact.arrayBuffer()))).toEqual([first]);

  const withTail = await runExport(
    `https://streamsy.internal/fork-source?stream=src&until=${forkOffset}&tail=1&budget=92`,
    makeStorage(Option.some(makeRecord(messages[1]?.offset ?? ZERO_OFFSET)), messages, []),
  );
  expect(decodeFrames(new Uint8Array(await withTail.arrayBuffer()))).toEqual(messages);

  const truncated = await runExport(
    `https://streamsy.internal/fork-source?stream=src&until=${forkOffset}&tail=0&budget=45`,
    makeStorage(Option.some(makeRecord(messages[1]?.offset ?? ZERO_OFFSET)), messages, []),
  );
  expect(truncated.headers.get("streamsy-frames-truncated")).toBe("1");
  expect(new Uint8Array(await truncated.arrayBuffer())).toHaveLength(0);
});

test("fork-source omits frames when until is beyond the source tail", async () => {
  const messages = makeMessages(1);
  const result = await runExport(
    "https://streamsy.internal/fork-source?stream=src&until=0000000000000001_0000000000000000&tail=2&budget=1000",
    makeStorage(Option.some(makeRecord(messages[0]?.offset ?? ZERO_OFFSET)), messages, []),
  );
  expect(result.status).toBe(200);
  expect(result.headers.get("streamsy-frames-omitted")).toBe("until-beyond-tail");
  expect(new Uint8Array(await result.arrayBuffer())).toHaveLength(0);
});

test("fork-source classifies missing and soft-deleted sources", async () => {
  const missing = await runExport(
    "https://streamsy.internal/fork-source?stream=src&tail=0&budget=1000",
    makeStorage(Option.none(), [], []),
  );
  expect(missing.status).toBe(404);
  expect(missing.headers.get("streamsy-fork-source")).toBe("1");

  const deleted = await runExport(
    "https://streamsy.internal/fork-source?stream=src&tail=0&budget=1000",
    makeStorage(Option.some(makeRecord(ZERO_OFFSET, true)), [], []),
  );
  expect(deleted.status).toBe(410);
  expect(deleted.headers.get("streamsy-fork-source")).toBe("1");
});

test("fork-source validates its query", async () => {
  const storage = makeStorage(Option.none(), [], []);
  for (const query of ["", "stream=src&tail=10001&budget=0", "stream=src&tail=0&budget=-1"]) {
    const response = await runExport(`https://streamsy.internal/fork-source?${query}`, storage);
    expect(response.status).toBe(400);
  }
});
