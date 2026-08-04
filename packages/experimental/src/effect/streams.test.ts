import {
  StreamProtocol,
  createMemoryStorageAdapter,
  directProtocolClient,
  type ClientFailure,
} from "@streamsy/core";
import { Effect, Exit, Schema } from "effect";
import { describe, expect, test } from "vitest";
import { bindStream } from "../binding.ts";
import { streamIdentity } from "../causal.ts";
import { StreamAppendError, StreamReadError } from "./errors.ts";
import { TestStreams, TestStreamsLayer } from "./testing.ts";
import { AppendStreams, AppendStreamsLive, ReadStreams, ReadStreamsLive } from "./streams.ts";

describe("Effect stream capabilities", () => {
  test("schema-backed faults preserve client classification and unknown append durability", () => {
    const failure: ClientFailure = {
      status: "error",
      code: "busy",
      message: "temporarily unavailable",
      retryable: true,
    };
    const read = Schema.decodeUnknownSync(StreamReadError)(StreamReadError.from("open", failure));
    const append = Schema.decodeUnknownSync(StreamAppendError)(
      StreamAppendError.from("appendJsonBatch", failure),
    );

    expect(read).toMatchObject({ code: "busy", retryable: true });
    expect(append).toMatchObject({ code: "busy", retryable: true, durability: "unknown" });
  });

  test("Live layers adapt the fixed Promise client while preserving protocol outcomes", async () => {
    const client = directProtocolClient(
      new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } }),
    );
    const binding = bindStream({
      identity: streamIdentity("facts"),
      client,
      streamId: "facts",
    });
    await client.stream("facts").create({ contentType: "application/json" });

    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const append = yield* AppendStreams;
        const read = yield* ReadStreams;
        const appended = yield* append.appendJsonBatch(binding, [1]);
        const opened = yield* read.open(binding);
        if (opened.status !== "ok") return { appended, opened };
        const first = yield* opened.session.next;
        const second = yield* opened.session.next;
        const ended = yield* opened.session.done;
        return { appended, first, second, ended };
      }).pipe(Effect.provide(ReadStreamsLive), Effect.provide(AppendStreamsLive)),
    );

    expect(Exit.isSuccess(exit)).toBe(true);
    if (Exit.isSuccess(exit)) {
      expect(exit.value).toMatchObject({
        appended: { status: "appended" },
        first: { done: false, value: { kind: "json", items: [1] } },
        second: { done: true },
        ended: { status: "done" },
      });
    }
    await client.close();
  });

  test("Test layer supplies the same handlers through production and control tags", async () => {
    const client = directProtocolClient(
      new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } }),
    );
    const binding = bindStream({ identity: streamIdentity("test"), client, streamId: "test" });
    const handlers = {
      read: {
        open: () => Effect.succeed({ status: "not-found" as const }),
      },
      append: {
        append: () =>
          Effect.succeed({
            status: "duplicate" as const,
            offset: "00000001",
            producerEpoch: 1,
            producerSeq: 0,
          }),
        appendJsonBatch: () =>
          Effect.succeed({
            status: "duplicate" as const,
            offset: "00000001",
            producerEpoch: 1,
            producerSeq: 0,
          }),
      },
    };
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const reads = yield* ReadStreams;
        const appends = yield* AppendStreams;
        const controls = yield* TestStreams;
        return {
          read: yield* reads.open(binding),
          append: yield* appends.append(binding, "x"),
          sameRead: reads === controls.read,
          sameAppend: appends === controls.append,
        };
      }).pipe(Effect.provide(TestStreamsLayer(handlers))),
    );
    expect(result).toEqual({
      read: { status: "not-found" },
      append: {
        status: "duplicate",
        offset: "00000001",
        producerEpoch: 1,
        producerSeq: 0,
      },
      sameRead: true,
      sameAppend: true,
    });
    await client.close();
  });
});
