import { expect, it } from "bun:test";
import { Cause, Deferred, Effect, Exit, Fiber, PubSub, Stream } from "effect";
import { TestClock } from "effect/testing";
import { changes } from "./changes.ts";
import type { State } from "./state.ts";
import { StreamId } from "../../schema/index.ts";

for (const push of [true, false]) {
  it(`interruption releases ${push ? "push subscription" : "polling reads"} while the owner stays alive`, () =>
    expect(
      Effect.gen(function* () {
        const bus = yield* Effect.acquireRelease(PubSub.unbounded<StreamId>(), PubSub.shutdown);
        const id = StreamId.make("s");
        let reads = 0;
        class ObservedEntries extends Map<StreamId, import("./state.ts").Entry> {
          override get(key: StreamId) {
            reads++;
            return super.get(key);
          }
        }
        const state: State = { entries: new ObservedEntries(), children: new Map(), deadlines: [] };
        const ready = yield* Deferred.make<void>();
        const hold = yield* Deferred.make<void>();
        const fiber = yield* changes(state, bus, id, push, 25).pipe(
          Stream.runForEach(() =>
            Effect.gen(function* () {
              yield* Deferred.succeed(ready, undefined);
              // Keep a live subscription without draining its bus to make retention observable.
              if (push) yield* Deferred.await(hold);
            }),
          ),
          Effect.forkScoped,
        );
        yield* Deferred.await(ready);
        yield* PubSub.publish(bus, id);
        expect(yield* PubSub.size(bus)).toBe(push ? 1 : 0);
        yield* TestClock.adjust(25);
        expect(reads).toBe(push ? 1 : 2);
        yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);
        expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
        expect(yield* PubSub.size(bus)).toBe(0);
        const stoppedAt = reads;
        yield* PubSub.publish(bus, id);
        yield* PubSub.publish(bus, id);
        yield* TestClock.adjust(100);
        expect(yield* PubSub.size(bus)).toBe(0);
        expect(reads).toBe(stoppedAt);
        expect(yield* PubSub.isShutdown(bus)).toBe(false);
      }).pipe(Effect.provide(TestClock.layer()), Effect.scoped, Effect.runPromiseExit),
    ).resolves.toMatchObject({ _tag: "Success" }));
}
