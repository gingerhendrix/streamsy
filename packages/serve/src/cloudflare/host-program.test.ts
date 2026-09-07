import { expect, test } from "bun:test";
import { Deferred, Effect, Fiber, Layer, Option, Stream } from "effect";
import { Storage, StorageFault, StreamsReader, StreamsWriter } from "@streamsy/core";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Alarm } from "./alarm.ts";
import { HostCommand } from "./host-command.ts";
import { withMutationReconciliation } from "./host-program.ts";
import { hostProgram } from "./host-program.ts";

const failingStorage = Storage.of({
  capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
  record: () => Effect.succeed(Option.none()),
  messages: () => Effect.succeed([]),
  producer: () => Effect.succeed(Option.none()),
  mutate: () => Effect.die("unused"),
  changes: () => Stream.empty,
  nextExpiry: Effect.fail(
    new StorageFault({
      operation: "test.nextExpiry",
      message: "test failure",
      retryable: true,
    }),
  ),
});

test("mutation reconciliation runs after an interrupted committed mutation", async () => {
  const events: Array<string> = [];
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const committed = yield* Deferred.make<void>();
      const fiber = yield* Effect.forkChild(
        withMutationReconciliation(
          Effect.gen(function* () {
            yield* Effect.uninterruptible(
              Effect.sync(() => events.push("mutation committed")).pipe(
                Effect.andThen(Deferred.succeed(committed, undefined)),
              ),
            );
            return yield* Effect.never;
          }),
          Effect.sync(() => events.push("reconciled")),
        ),
      );
      yield* Deferred.await(committed);
      yield* Fiber.interrupt(fiber);
    }),
  );

  expect(result).toBeUndefined();
  expect(events).toEqual(["mutation committed", "reconciled"]);
});

test("a reconciliation defect cannot replace a committed mutation result", async () => {
  const response = await Effect.runPromise(
    withMutationReconciliation(Effect.succeed("committed"), Effect.die("reconcile defect")),
  );

  expect(response).toBe("committed");
});

test("an alarm StorageFault keeps the standard response security headers", async () => {
  const response = await Effect.runPromise(
    hostProgram({}).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(Storage, failingStorage),
          Layer.succeed(StreamsReader, {
            head: () => Effect.die("unused"),
            read: () => Effect.die("unused"),
            readNext: () => Effect.die("unused"),
          }),
          Layer.succeed(StreamsWriter, {
            create: () => Effect.die("unused"),
            fork: () => Effect.die("unused"),
            append: () => Effect.die("unused"),
            remove: () => Effect.die("unused"),
          }),
          // SAFETY: HostCommand is present, so the ordinary request branch is unreachable.
          Layer.succeed(
            HttpServerRequest.HttpServerRequest,
            {} as HttpServerRequest.HttpServerRequest,
          ),
          Layer.succeed(HostCommand, { _tag: "ExpireDue" }),
          Layer.succeed(Alarm, {
            current: Effect.succeed(Option.none()),
            arm: () => Effect.void,
            clear: Effect.void,
          }),
        ),
      ),
    ),
  );
  const web = HttpServerResponse.toWeb(response);

  expect(web.status).toBe(500);
  expect(web.headers.get("x-content-type-options")).toBe("nosniff");
  expect(web.headers.get("cross-origin-resource-policy")).toBe("cross-origin");
});
