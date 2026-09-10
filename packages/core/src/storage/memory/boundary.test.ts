/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own their host Layers, including the deliberate foreign-owner case. */
import { expect, test } from "bun:test";
import { Deferred, Effect, Exit, Fiber, Option, Schema, Stream } from "effect";
import * as Streams from "../../toolkit/streams.ts";
import * as StreamRef from "../../toolkit/ref.ts";
import { Storage } from "../storage.ts";
import { MemoryCommitBoundary } from "./boundary.ts";

const sink = StreamRef.json("fused-sink", { schema: Schema.String });
const turns = Effect.forEach([1, 2, 3, 4, 5], () => Effect.yieldNow, { discard: true });

for (const outcome of ["commit", "failure", "defect", "interrupt"]) {
  test(`memory fused boundary: ${outcome}`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const boundary = yield* MemoryCommitBoundary;
        const storage = yield* Storage;
        expect((yield* Streams.create(sink)).status).toBe("created");
        yield* boundary.write("state", "0");
        yield* boundary.write("checkpoint", "0");
        const read = Effect.gen(function* () {
          const batches = yield* Streams.read(sink).pipe(Stream.runCollect);
          return {
            output: batches.flatMap((batch) => batch.items),
            state: Option.getOrThrow(yield* boundary.read("state")),
            checkpoint: Option.getOrThrow(yield* boundary.read("checkpoint")),
          };
        });
        const before = { output: [], state: "0", checkpoint: "0" };
        const after = { output: ["one"], state: "1", checkpoint: "1" };
        const pull = yield* Stream.toPull(storage.changes(sink.id));
        yield* pull;
        const wake = yield* pull.pipe(Effect.forkScoped);
        const written = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const transaction = yield* boundary
          .withTransaction(
            Effect.gen(function* () {
              yield* boundary.withTransaction(
                Effect.gen(function* () {
                  expect((yield* Streams.append(sink, ["one"])).status).toBe("appended");
                  yield* boundary.write("state", "1");
                  yield* boundary.write("checkpoint", "1");
                }),
              );
              expect(yield* read).toEqual(after);
              yield* Deferred.succeed(written, undefined);
              yield* Deferred.await(release);
              if (outcome === "failure") return yield* Effect.fail("after-sink");
              if (outcome === "defect") return yield* Effect.die("after-sink");
              return undefined;
            }),
          )
          .pipe(Effect.forkScoped);
        yield* Deferred.await(written);
        expect(yield* read).toEqual(before);
        yield* turns;
        expect(wake.pollUnsafe()).toBeUndefined();
        if (outcome === "interrupt") yield* Fiber.interrupt(transaction);
        else yield* Deferred.succeed(release, undefined);
        const exit = yield* Fiber.await(transaction);
        expect(Exit.isSuccess(exit)).toBe(outcome === "commit");
        expect(yield* read).toEqual(outcome === "commit" ? after : before);
        if (outcome === "commit") {
          const snapshots = yield* Fiber.join(wake);
          expect(snapshots[0]?.currentOffset).not.toBe("0000000000000000_0000000000000000");
        } else {
          yield* turns;
          expect(wake.pollUnsafe()).toBeUndefined();
          // A later successful transaction proves rollback released ownership and the permit.
          yield* boundary.withTransaction(Streams.append(sink, ["later"]));
          yield* Fiber.join(wake);
        }
      }).pipe(Effect.provide(Streams.layerMemory()), Effect.scoped, Effect.timeout("5 seconds")),
    ));
}

test("memory owner rejects inherited child-fiber use and separate owners", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const boundary = yield* MemoryCommitBoundary;
      yield* boundary.withTransaction(
        Effect.gen(function* () {
          const child = yield* boundary.write("state", "child").pipe(Effect.forkScoped);
          expect(Exit.isFailure(yield* Fiber.await(child))).toBe(true);
          const foreign = yield* Effect.gen(function* () {
            return yield* (yield* MemoryCommitBoundary).write("state", "foreign");
          }).pipe(Effect.provide(Streams.layerMemory()), Effect.exit);
          expect(Exit.isFailure(foreign)).toBe(true);
        }),
      );
      expect(Option.isNone(yield* boundary.read("state"))).toBe(true);
    }).pipe(Effect.provide(Streams.layerMemory()), Effect.scoped, Effect.timeout("5 seconds")),
  ));

test("memory outer owners serialize without losing stream output or records", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const boundary = yield* MemoryCommitBoundary;
      yield* Streams.create(sink);
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const entered = yield* Deferred.make<void>();
      const first = yield* boundary
        .withTransaction(
          Effect.gen(function* () {
            yield* Streams.append(sink, ["first"]);
            yield* boundary.write("state", "first");
            yield* Deferred.succeed(held, undefined);
            yield* Deferred.await(release);
          }),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(held);
      const second = yield* boundary
        .withTransaction(
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            expect(Option.getOrThrow(yield* boundary.read("state"))).toBe("first");
            yield* Streams.append(sink, ["second"]);
            yield* boundary.write("state", "second");
          }),
        )
        .pipe(Effect.forkScoped);
      yield* turns;
      expect(yield* Deferred.isDone(entered)).toBe(false);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      expect(Option.getOrThrow(yield* boundary.read("state"))).toBe("second");
      const batches = yield* Streams.read(sink).pipe(Stream.runCollect);
      expect(batches.flatMap((batch) => batch.items)).toEqual(["first", "second"]);
    }).pipe(Effect.provide(Streams.layerMemory()), Effect.scoped, Effect.timeout("5 seconds")),
  ));
