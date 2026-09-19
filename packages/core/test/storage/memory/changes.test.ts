import { expect, it } from "bun:test";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Queue, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ZERO_OFFSET } from "../../../src/offset/index.ts";
import { createNotifier, type Notifier } from "../../../src/storage/memory/notifier.ts";
import { changes } from "../../../src/storage/memory/changes.ts";
import type { State } from "../../../src/storage/memory/state.ts";
import { StreamId } from "../../../src/schema/index.ts";

const pending = (bus: Notifier) =>
  [...bus.queues].reduce((n, queue) => n + Queue.sizeUnsafe(queue), 0);

for (const push of [true, false]) {
  it(`interruption releases ${push ? "push subscription" : "polling reads"} while the owner stays alive`, () =>
    expect(
      Effect.gen(function* () {
        const context = yield* Layer.build(TestClock.layer());
        yield* Effect.gen(function* () {
          const bus = yield* createNotifier;
          const id = StreamId.make("s");
          let reads = 0;
          class ObservedEntries extends Map<
            StreamId,
            import("../../../src/storage/memory/state.ts").Entry
          > {
            override get(key: StreamId) {
              reads++;
              return super.get(key);
            }
          }
          const state: State = {
            entries: new ObservedEntries(),
            children: new Map(),
            deadlines: [],
          };
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
          yield* bus.publish;
          expect(pending(bus)).toBe(push ? 1 : 0);
          yield* TestClock.adjust(25);
          expect(reads).toBe(push ? 1 : 2);
          yield* Fiber.interrupt(fiber);
          const exit = yield* Fiber.await(fiber);
          expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
          expect(pending(bus)).toBe(0);
          expect(bus.queues.size).toBe(0);
          const stoppedAt = reads;
          yield* bus.publish;
          yield* bus.publish;
          yield* TestClock.adjust(100);
          expect(pending(bus)).toBe(0);
          expect(bus.queues.size).toBe(0);
          expect(reads).toBe(stoppedAt);
          expect(bus.closed).toBe(false);
        }).pipe(Effect.provide(context));
      }).pipe(Effect.scoped, Effect.runPromiseExit),
    ).resolves.toMatchObject({ _tag: "Success" }));
}

it("a slow subscriber retains one coalesced wake and observes its latest authoritative state", () =>
  expect(
    Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const bus = yield* createNotifier;
          const id = StreamId.make("slow");
          const state: State = { entries: new Map(), children: new Map(), deadlines: [] };
          const ready = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          let seen = 0;
          const fiber = yield* changes(state, bus, id, true, 25).pipe(
            Stream.tap(() =>
              Effect.gen(function* () {
                if (++seen === 1) {
                  yield* Deferred.succeed(ready, undefined);
                  yield* Deferred.await(release);
                }
              }),
            ),
            Stream.take(2),
            Stream.runCollect,
            Effect.forkScoped,
          );
          yield* Deferred.await(ready);
          // The relevant update is followed by unrelated commits. Their coalesced wake
          // must still re-read this stream instead of being discarded by an id filter.
          state.entries.set(id, {
            record: {
              id,
              config: { contentType: "text/plain", createdAt: 0 },
              lifecycle: { closed: true, softDeleted: false },
              currentOffset: ZERO_OFFSET,
            },
            messages: [],
            producers: new Map(),
          });
          for (let i = 0; i < 1000; i++) yield* bus.publish;
          expect(pending(bus)).toBe(1);
          yield* Deferred.succeed(release, undefined);
          const values = yield* Fiber.join(fiber);
          expect(values[1]).toMatchObject({ present: true, closed: true });
          expect(pending(bus)).toBe(0);
          expect(bus.queues.size).toBe(0);
        }),
      ),
    ),
  ).resolves.toEqual(Exit.succeed(undefined)));

it("timeout removes the actual queue subscription while the memory owner remains open", () =>
  expect(
    Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(TestClock.layer());
          yield* Effect.gen(function* () {
            const bus = yield* createNotifier;
            const state: State = { entries: new Map(), children: new Map(), deadlines: [] };
            const ready = yield* Deferred.make<void>();
            const fiber = yield* changes(state, bus, StreamId.make("timeout"), true, 25).pipe(
              Stream.tap(() => Deferred.succeed(ready, undefined)),
              Stream.filter((value) => value.present),
              Stream.take(1),
              Stream.runCollect,
              Effect.timeoutOption(100),
              Effect.forkScoped,
            );
            yield* Deferred.await(ready);
            yield* TestClock.adjust(100);
            expect(Option.isNone(yield* Fiber.join(fiber))).toBe(true);
            yield* bus.publish;
            expect(pending(bus)).toBe(0);
            expect(bus.queues.size).toBe(0);
            expect(bus.closed).toBe(false);
          }).pipe(Effect.provide(context));
        }),
      ),
    ),
  ).resolves.toEqual(Exit.succeed(undefined)));
