import { expect, it } from "vitest";
import { Effect, Exit, Queue, Scope, Stream } from "effect";
import { ZERO_OFFSET } from "../../../src/offset/index.ts";
import { StreamId } from "../../../src/schema/index.ts";
import { changes } from "../../../src/storage/memory/changes.ts";
import { createNotifier } from "../../../src/storage/memory/notifier.ts";
import type { Entry, State } from "../../../src/storage/memory/state.ts";

const entry = (id: StreamId, closed = false): Entry => ({
  record: {
    id,
    config: { contentType: "text/plain", createdAt: 0 },
    lifecycle: { closed, softDeleted: false },
    currentOffset: ZERO_OFFSET,
  },
  messages: [],
  producers: new Map(),
});

const check = <E>(program: Effect.Effect<void, E, Scope.Scope>) =>
  expect(program.pipe(Effect.scoped, Effect.runPromiseExit)).resolves.toEqual(
    Exit.succeed(undefined),
  );

for (const sameId of [false, true]) {
  for (const slowIndex of [0, 1]) {
    it(`independent burst retention and quiescent close/deletion: sameId=${sameId}, slow=${slowIndex}`, () =>
      check(
        Effect.gen(function* () {
          const bus = yield* createNotifier;
          const child = yield* Scope.make();
          const ids = [StreamId.make("a"), StreamId.make(sameId ? "a" : "b")];

          const state: State = {
            entries: new Map(ids.map((id) => [id, entry(id)])),
            children: new Map(),
            deadlines: [],
          };

          const pulls = [];

          for (const id of ids) {
            const pull = yield* Stream.toPull(changes(state, bus, id, true, 25)).pipe(
              Scope.provide(child),
            );

            yield* pull;
            pulls.push(pull);
          }

          const slow = pulls[slowIndex];
          const fast = pulls[1 - slowIndex];
          const fastId = ids[1 - slowIndex];

          if (!slow || !fast || !fastId) throw new Error("missing fixture");
          yield* bus.publish;
          expect((yield* fast)[0]).toMatchObject({ present: true, closed: false });
          state.entries.set(fastId, entry(fastId, true));

          for (let i = 0; i < 1000; i++) yield* bus.publish;
          expect([...bus.queues].map(Queue.sizeUnsafe)).toEqual([1, 1]);
          expect((yield* fast)[0]).toMatchObject({ present: true, closed: true });
          // No subsequent publication is needed to deliver the close or deletion.
          state.entries.delete(fastId);
          yield* bus.publish;
          expect((yield* fast)[0]).toMatchObject({ present: false, closed: false });
          const slowValue = (yield* slow)[0];
          expect(slowValue).toMatchObject({ present: !sameId, closed: false });
          // Purge/recreate remains a current snapshot, not a promise to replay transitions.
          state.entries.set(fastId, entry(fastId));
          yield* bus.publish;
          expect((yield* fast)[0]).toMatchObject({
            present: true,
            currentOffset: ZERO_OFFSET,
            closed: false,
          });
          yield* Scope.close(child, Exit.void);
          expect(bus.queues.size).toBe(0);
          yield* bus.publish;
          expect(bus.queues.size).toBe(0);
          expect(bus.closed).toBe(false);
        }),
      ));
  }
}

it("registration precedes the first snapshot even when a commit follows its state lookup", () =>
  check(
    Effect.gen(function* () {
      const bus = yield* createNotifier;
      const id = StreamId.make("race");
      let reads = 0;

      class RacingEntries extends Map<StreamId, Entry> {
        override get(key: StreamId) {
          const before = super.get(key);

          if (++reads === 1) {
            expect(bus.queues.size).toBe(1);
            this.set(key, entry(key, true));

            // Inject a synchronous commit/publication between lookup and returned snapshot.
            for (const queue of bus.queues) Queue.offerUnsafe(queue, undefined);
          }

          return before;
        }
      }

      const state: State = { entries: new RacingEntries(), children: new Map(), deadlines: [] };

      const values = yield* changes(state, bus, id, true, 25).pipe(
        Stream.take(2),
        Stream.runCollect,
      );

      expect(values).toMatchObject([{ present: false }, { present: true, closed: true }]);
      expect(bus.queues.size).toBe(0);
    }),
  ));

it("a commit between taking a wake and reading is visible and leaves its next wake pending", () =>
  check(
    Effect.gen(function* () {
      const bus = yield* createNotifier;
      const id = StreamId.make("take-read");
      let reads = 0;

      class RacingEntries extends Map<StreamId, Entry> {
        override get(key: StreamId) {
          if (++reads === 2) {
            expect([...bus.queues].map(Queue.sizeUnsafe)).toEqual([0]);
            this.set(key, entry(key, true));

            for (const queue of bus.queues) Queue.offerUnsafe(queue, undefined);
          }

          return super.get(key);
        }
      }

      const state: State = {
        entries: new RacingEntries([[id, entry(id)]]),
        children: new Map(),
        deadlines: [],
      };

      const pull = yield* Stream.toPull(changes(state, bus, id, true, 25));
      expect((yield* pull)[0]).toMatchObject({ closed: false });
      yield* bus.publish;
      expect((yield* pull)[0]).toMatchObject({ closed: true });
      expect([...bus.queues].map(Queue.sizeUnsafe)).toEqual([1]);
      expect((yield* pull)[0]).toMatchObject({ closed: true });
      expect([...bus.queues].map(Queue.sizeUnsafe)).toEqual([0]);
    }),
  ));
