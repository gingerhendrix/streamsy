import { expect, it } from "bun:test";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, PubSub, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ZERO_OFFSET } from "../../offset/index.ts";
import { changes } from "./changes.ts";
import type { State } from "./state.ts";
import { StreamId } from "../../schema/index.ts";

for (const push of [true, false]) {
  it(`interruption releases ${push ? "push subscription" : "polling reads"} while the owner stays alive`, () =>
    expect(
      Effect.gen(function* () {
        const context = yield* Layer.build(TestClock.layer());
        yield* Effect.gen(function* () {
          const bus = yield* Effect.acquireRelease(PubSub.dropping<void>(1), PubSub.shutdown);
          const id = StreamId.make("s");
          let reads = 0;
          class ObservedEntries extends Map<StreamId, import("./state.ts").Entry> {
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
          yield* PubSub.publish(bus, undefined);
          expect(yield* PubSub.size(bus)).toBe(push ? 1 : 0);
          yield* TestClock.adjust(25);
          expect(reads).toBe(push ? 1 : 2);
          yield* Fiber.interrupt(fiber);
          const exit = yield* Fiber.await(fiber);
          expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
          expect(yield* PubSub.size(bus)).toBe(0);
          const stoppedAt = reads;
          yield* PubSub.publish(bus, undefined);
          yield* PubSub.publish(bus, undefined);
          yield* TestClock.adjust(100);
          expect(yield* PubSub.size(bus)).toBe(0);
          expect(reads).toBe(stoppedAt);
          expect(yield* PubSub.isShutdown(bus)).toBe(false);
        }).pipe(Effect.provide(context));
      }).pipe(Effect.scoped, Effect.runPromiseExit),
    ).resolves.toMatchObject({ _tag: "Success" }));
}

it("a slow subscriber retains one coalesced wake and observes its latest authoritative state", () =>
  expect(
    Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const bus = yield* Effect.acquireRelease(PubSub.dropping<void>(1), PubSub.shutdown);
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
          for (let i = 0; i < 1000; i++) yield* PubSub.publish(bus, undefined);
          expect(yield* PubSub.size(bus)).toBe(1);
          yield* Deferred.succeed(release, undefined);
          const values = yield* Fiber.join(fiber);
          expect(values[1]).toMatchObject({ present: true, closed: true });
          expect(yield* PubSub.size(bus)).toBe(0);
        }),
      ),
    ),
  ).resolves.toEqual(Exit.succeed(undefined)));

it("timeout removes the actual PubSub subscription while the memory owner remains open", () =>
  expect(
    Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(TestClock.layer());
          yield* Effect.gen(function* () {
            const bus = yield* Effect.acquireRelease(PubSub.dropping<void>(1), PubSub.shutdown);
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
            yield* PubSub.publish(bus, undefined);
            expect(yield* PubSub.size(bus)).toBe(0);
            expect(yield* PubSub.isShutdown(bus)).toBe(false);
          }).pipe(Effect.provide(context));
        }),
      ),
    ),
  ).resolves.toEqual(Exit.succeed(undefined)));
