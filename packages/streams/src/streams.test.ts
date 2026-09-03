import {
  ClientReadSession,
  StreamProtocol,
  createMemoryStorageAdapter,
  directProtocolClient,
  type ClientFailure,
  type ClientReadResult,
  type JsonValue,
  type StreamProtocolClient,
  type StreamProtocolHandle,
} from "@streamsy/core";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import { describe, expect, test } from "vitest";
import { bindStream } from "./binding.ts";
import { streamIdentity } from "./identity.ts";
import { TestStreams, TestStreamsLayer } from "./testing.ts";
import {
  AppendStreams,
  AppendStreamsLive,
  CreateStreams,
  CreateStreamsLive,
  ReadStreams,
  ReadStreamsLive,
  StreamAppendError,
  StreamCreateError,
  StreamReadError,
  type StreamCancellationReason,
} from "./streams.ts";
import { provideTestLayers } from "./testing.ts";

const StreamTestLive = Layer.mergeAll(CreateStreamsLive, ReadStreamsLive, AppendStreamsLive);

describe("Effect stream capabilities", () => {
  test("schema-backed write faults preserve client classification and unknown durability", () => {
    const failure: ClientFailure = {
      status: "error",
      code: "busy",
      message: "temporarily unavailable",
      retryable: true,
    };
    const create = Schema.decodeUnknownSync(StreamCreateError)(
      StreamCreateError.from("create", failure),
    );
    const read = Schema.decodeUnknownSync(StreamReadError)(StreamReadError.from("open", failure));
    const append = Schema.decodeUnknownSync(StreamAppendError)(
      StreamAppendError.from("appendJsonBatch", failure),
    );

    expect(create).toMatchObject({ code: "busy", retryable: true, durability: "unknown" });
    expect(read).toMatchObject({ code: "busy", retryable: true });
    expect(append).toMatchObject({ code: "busy", retryable: true, durability: "unknown" });
  });

  test.each([
    { status: "error" },
    { status: "error", code: "busy" },
    { status: "error", code: "not-real", message: 1, retryable: "yes" },
  ])("partial unknown failures cannot defect during tagged-error construction", (failure) => {
    expect(() => StreamCreateError.from("create", failure)).not.toThrow();
    expect(() => StreamReadError.from("open", failure)).not.toThrow();
    expect(() => StreamAppendError.from("append", failure)).not.toThrow();
    expect(StreamCreateError.from("create", failure)).toMatchObject({
      code: "unknown",
      retryable: false,
      durability: "unknown",
    });
    expect(StreamReadError.from("open", failure)).toMatchObject({
      code: "unknown",
      retryable: false,
    });
    expect(StreamAppendError.from("append", failure)).toMatchObject({
      code: "unknown",
      retryable: false,
      durability: "unknown",
    });
  });

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect runner callback at the test boundary.
  test("Live layers adapt the fixed Promise client while preserving protocol outcomes", async () => {
    const client = directProtocolClient(
      new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } }),
    );
    const binding = bindStream({
      identity: streamIdentity("facts"),
      client,
      streamId: "facts",
    });
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const create = yield* CreateStreams;
        const append = yield* AppendStreams;
        const read = yield* ReadStreams;
        const created = yield* create.create(binding, { contentType: "application/json" });
        const appended = yield* append.appendJsonBatch(binding, [1]);
        const opened = yield* read.open(binding);
        if (opened.status !== "ok") return { appended, opened };
        const first = yield* opened.session.next;
        const second = yield* opened.session.next;
        const ended = yield* opened.session.done;
        return { created, appended, first, second, ended };
      }).pipe(Effect.scoped, (effect) => provideTestLayers(effect, StreamTestLive)),
    );

    expect(Exit.isSuccess(exit)).toBe(true);
    if (Exit.isSuccess(exit)) {
      expect(exit.value).toMatchObject({
        created: { status: "created", contentType: "application/json" },
        appended: { status: "appended" },
        first: { done: false, value: { kind: "json", items: [1] } },
        second: { done: true },
        ended: { status: "done" },
      });
    }
    await client.close();
  });

  test.each(["early-return", "typed-failure", "missing-start-offset"] as const)(
    "a successful Live read acquisition releases exactly once on %s",
    // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect runner callback at the test boundary.
    async (scenario) => {
      let cancelled = 0;
      const binding = sessionBinding({ scenario, onCancel: () => cancelled++ });
      const exit = await Effect.runPromiseExit(
        Effect.gen(function* () {
          const reads = yield* ReadStreams;
          const opened = yield* reads.open(binding);
          if (opened.status !== "ok") return opened;
          if (scenario === "early-return") return opened.status;
          if (scenario === "missing-start-offset") {
            return yield* new StreamReadError({
              operation: "invariant",
              failure: opened,
              message: "missing start offset",
              code: "unknown",
              retryable: false,
            });
          }
          yield* opened.session.next;
          return yield* opened.session.done;
        }).pipe(Effect.scoped, (effect) => provideTestLayers(effect, ReadStreamsLive)),
      );

      expect(Exit.isSuccess(exit)).toBe(scenario === "early-return");
      expect(cancelled).toBe(1);
      await binding.client.close();
    },
  );

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect runner callback at the test boundary.
  test("interruption of a blocked Live pull releases its session exactly once", async () => {
    let cancelled = 0;
    const binding = sessionBinding({ scenario: "blocked", onCancel: () => cancelled++ });
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const acquired = yield* Deferred.make<void>();
        const fiber = yield* Effect.gen(function* () {
          const reads = yield* ReadStreams;
          const opened = yield* reads.open(binding);
          if (opened.status !== "ok") return opened;
          yield* Deferred.succeed(acquired, undefined);
          return yield* opened.session.next;
        }).pipe(
          Effect.scoped,
          (effect) => provideTestLayers(effect, ReadStreamsLive),
          Effect.forkChild,
        );
        yield* Deferred.await(acquired);
        yield* Fiber.interrupt(fiber);
        return yield* Fiber.await(fiber);
      }),
    );

    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
    expect(cancelled).toBe(1);
    await binding.client.close();
  });

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest executes this Promise-returning Effect runner callback at the test boundary.
  test("Test layer supplies the same handlers through production and control tags", async () => {
    const client = directProtocolClient(
      new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } }),
    );
    const binding = bindStream({ identity: streamIdentity("test"), client, streamId: "test" });
    const handlers = {
      create: {
        create: () => Effect.succeed({ status: "conflict" as const }),
      },
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
        const creates = yield* CreateStreams;
        const reads = yield* ReadStreams;
        const appends = yield* AppendStreams;
        const controls = yield* TestStreams;
        return {
          create: yield* creates.create(binding),
          read: yield* reads.open(binding),
          append: yield* appends.append(binding, "x"),
          sameCreate: creates === controls.create,
          sameRead: reads === controls.read,
          sameAppend: appends === controls.append,
        };
      }).pipe(Effect.scoped, (effect) => provideTestLayers(effect, TestStreamsLayer(handlers))),
    );
    expect(result).toEqual({
      create: { status: "conflict" },
      read: { status: "not-found" },
      append: {
        status: "duplicate",
        offset: "00000001",
        producerEpoch: 1,
        producerSeq: 0,
      },
      sameCreate: true,
      sameRead: true,
      sameAppend: true,
    });
    await client.close();
  });
});

function unusedClientOperation(): Promise<never> {
  return Promise.reject(new Error("unused client operation"));
}

function sessionBinding(options: {
  readonly scenario: "early-return" | "typed-failure" | "missing-start-offset" | "blocked";
  readonly onCancel: () => void;
}) {
  const client: StreamProtocolClient = {
    stream(streamId: string): StreamProtocolHandle {
      return {
        id: streamId,
        head: unusedClientOperation,
        create: unusedClientOperation,
        append: unusedClientOperation,
        appendJsonBatch: unusedClientOperation,
        close: unusedClientOperation,
        read: <T extends JsonValue>() => {
          const session = new ClientReadSession<T>({ startOffset: "-1" });
          const originalCancel = session.cancel.bind(session);
          session.cancel = (reason?: StreamCancellationReason) => {
            options.onCancel();
            originalCancel(reason);
          };
          if (options.scenario === "typed-failure") {
            session.end({
              status: "error",
              code: "transport",
              message: "read failed",
              retryable: true,
            });
          }
          if (options.scenario === "missing-start-offset") {
            Object.defineProperty(session, "startOffset", { value: undefined });
          }
          return Promise.resolve<ClientReadResult<T>>({ status: "ok", session });
        },
      };
    },
    close: () => Promise.resolve(),
  };
  return bindStream({ identity: streamIdentity("session"), client, streamId: "session" });
}
