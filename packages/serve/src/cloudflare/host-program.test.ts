import { expect, test } from "bun:test";
import { Deferred, Effect, Exit, Fiber, Layer, Option, Stream } from "effect";
import { Storage, StorageFault } from "@streamsy/core";
import { Alarm } from "./alarm.ts";
import { withMutationReconciliation } from "./host-program.ts";
import { alarm } from "./host-program.ts";

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

test("an alarm exposes StorageFault for platform retry without an HTTP request", async () => {
  const result = await Effect.runPromiseExit(
    alarm.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(Storage, failingStorage),
          Layer.succeed(Alarm, {
            current: Effect.succeed(Option.none()),
            arm: () => Effect.void,
            clear: Effect.void,
          }),
        ),
      ),
    ),
  );
  expect(Exit.isFailure(result)).toBe(true);
  if (Exit.isFailure(result))
    expect(result.cause.reasons).toMatchObject([
      { _tag: "Fail", error: { _tag: "StorageFault", operation: "test.nextExpiry" } },
    ]);
});

test("an alarm sweeps before reconciling without reader, writer, or request services", async () => {
  const actions: Array<string> = [];
  const storage = Storage.of({
    ...failingStorage,
    nextExpiry: Effect.sync(() => {
      actions.push("next expiry");
      return Option.none();
    }),
  });
  await Effect.runPromise(
    alarm.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(Storage, storage),
          Layer.succeed(Alarm, {
            current: Effect.succeed(Option.none()),
            arm: () => Effect.void,
            clear: Effect.sync(() => {
              actions.push("clear");
            }),
          }),
        ),
      ),
    ),
  );
  expect(actions).toEqual(["next expiry", "next expiry", "clear"]);
});
