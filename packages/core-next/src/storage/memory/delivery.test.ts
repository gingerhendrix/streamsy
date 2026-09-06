import { expect, it } from "bun:test";
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { StreamId } from "../../schema/index.ts";
import { ZERO_OFFSET } from "../../offset/index.ts";
import { Storage } from "../storage.ts";
import { layer } from "./layer.ts";
import { readNext } from "../../protocol/read.ts";
import { touch } from "../../protocol/expiry.ts";
import { append } from "../../protocol/append.ts";

for (const sameId of [false, true]) {
  for (const slowFirst of [false, true]) {
    for (const wake of ["unrelated", "ttl"] as const) {
      it(`readNext delivers before timeout: ${wake}, sameId=${sameId}, slowFirst=${slowFirst}`, () =>
        expect(
          Effect.gen(function* () {
            const context = yield* Layer.build(Layer.mergeAll(layer(), TestClock.layer()));
            yield* Effect.gen(function* () {
              const storage = yield* Storage;
              const id = StreamId.make("fast");
              const other = StreamId.make("other");
              for (const key of [id, other]) {
                yield* storage.mutate({
                  operations: [
                    {
                      _tag: "Create",
                      record: {
                        id: key,
                        config: {
                          contentType: "text/plain",
                          createdAt: 0,
                          ttlSeconds: key === other ? 60 : undefined,
                        },
                        lifecycle: { closed: false, softDeleted: false, expiresAtMs: 60_000 },
                        currentOffset: ZERO_OFFSET,
                      },
                      initialMessages: [],
                    },
                  ],
                });
              }
              const slowReady = yield* Deferred.make<void>();
              const hold = yield* Deferred.make<void>();
              const slow = storage.changes(sameId ? id : other).pipe(
                Stream.runForEach(() =>
                  Effect.andThen(Deferred.succeed(slowReady, undefined), Deferred.await(hold)),
                ),
                Effect.forkScoped,
              );
              const initial = yield* Deferred.make<void>();
              const unchanged = yield* Deferred.make<void>();
              let snapshots = 0;
              const observed = Storage.of({
                ...storage,
                changes: (key) =>
                  storage.changes(key).pipe(
                    Stream.tap((value) =>
                      Effect.gen(function* () {
                        snapshots++;
                        if (snapshots > 2) return;
                        expect(value.currentOffset).toBe(ZERO_OFFSET);
                        yield* Deferred.succeed(snapshots === 1 ? initial : unchanged, undefined);
                      }),
                    ),
                  ),
              });
              const fast = readNext(observed, id, { offset: ZERO_OFFSET }, 30_000).pipe(
                Effect.forkScoped,
              );
              if (slowFirst) {
                yield* slow;
                yield* Deferred.await(slowReady);
              }
              const reader = yield* fast;
              yield* Deferred.await(initial);
              if (!slowFirst) {
                yield* slow;
                yield* Deferred.await(slowReady);
              }
              if (wake === "ttl") {
                const record = yield* storage.record(other);
                if (Option.isNone(record)) throw new Error("missing fixture");
                yield* touch(storage, record.value);
              } else {
                yield* append(storage, other, {
                  data: new TextEncoder().encode("unrelated"),
                  contentType: "text/plain",
                });
              }
              yield* Deferred.await(unchanged);
              // rc.112 adjust(0) yields through a child fiber; it is not a quiescence barrier.
              // The unchanged snapshot proves W1 was taken; a new wake now has room.
              yield* TestClock.adjust(0);
              expect(snapshots).toBe(2);
              const before = yield* Clock.currentTimeMillis;
              yield* append(storage, id, {
                data: new TextEncoder().encode("relevant"),
                contentType: "text/plain",
              });
              for (let turns = 0; turns < 100 && reader.pollUnsafe() === undefined; turns++) {
                yield* Effect.yieldNow;
              }
              expect(reader.pollUnsafe()).toMatchObject({
                _tag: "Success",
                value: { status: "ok", messages: [{ data: new TextEncoder().encode("relevant") }] },
              });
              expect(yield* Clock.currentTimeMillis).toBe(before);
              yield* Fiber.join(reader);
            }).pipe(Effect.provide(context));
          }).pipe(Effect.scoped, Effect.runPromiseExit),
        ).resolves.toEqual(Exit.succeed(undefined)));
    }
  }
}

it("memory layer shutdown ends pending and late changes outside the owner scope", () =>
  expect(
    Effect.gen(function* () {
      const clock = yield* Layer.build(TestClock.layer());
      const owner = yield* Scope.make();
      const context = yield* Layer.build(layer()).pipe(Scope.provide(owner));
      const storage = Context.get(context, Storage);
      const pull = yield* Stream.toPull(storage.changes(StreamId.make("owner")));
      yield* pull;
      const pending = yield* pull.pipe(Effect.forkScoped);
      yield* TestClock.adjust(0).pipe(Effect.provide(clock));
      expect(pending.pollUnsafe()).toBeUndefined();
      yield* Scope.close(owner, Exit.void);
      const stopped = yield* Fiber.await(pending);
      expect(Exit.isFailure(stopped) && Cause.hasInterruptsOnly(stopped.cause)).toBe(true);
      const late = yield* storage
        .changes(StreamId.make("late"))
        .pipe(Stream.runCollect, Effect.exit);
      expect(Exit.isFailure(late) && Cause.hasInterruptsOnly(late.cause)).toBe(true);
    }).pipe(Effect.scoped, Effect.runPromiseExit),
  ).resolves.toEqual(Exit.succeed(undefined)));

it("a rejected mutation emits no wake while a subsequent commit does", () =>
  expect(
    Effect.gen(function* () {
      const context = yield* Layer.build(layer());
      const storage = Context.get(context, Storage);
      const id = StreamId.make("rejected");
      const pull = yield* Stream.toPull(storage.changes(id));
      expect((yield* pull)[0]).toMatchObject({ present: false });
      const rejected = yield* storage.mutate({
        operations: [{ _tag: "Delete", streamId: id, reason: "delete" }],
      });
      expect(rejected._tag).toBe("Rejected");
      const pending = yield* pull.pipe(Effect.forkScoped);
      for (let turns = 0; turns < 100; turns++) yield* Effect.yieldNow;
      expect(pending.pollUnsafe()).toBeUndefined();
      yield* storage.mutate({
        operations: [
          {
            _tag: "Create",
            record: {
              id,
              config: { contentType: "text/plain", createdAt: 0 },
              lifecycle: { closed: false, softDeleted: false },
              currentOffset: ZERO_OFFSET,
            },
            initialMessages: [],
          },
        ],
      });
      expect((yield* Fiber.join(pending))[0]).toMatchObject({ present: true });
    }).pipe(Effect.scoped, Effect.runPromiseExit),
  ).resolves.toEqual(Exit.succeed(undefined)));
