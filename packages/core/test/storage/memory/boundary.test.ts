/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own their host Layers, including the deliberate foreign-owner case. */
import { expect, test } from "bun:test";
import { Cause, Deferred, Effect, Exit, Fiber, Option, Schema, Stream } from "effect";
import { MemoryStorage } from "../../../src/index.ts";
import * as Streams from "../../../src/toolkit/streams.ts";
import * as StreamAddress from "../../../src/toolkit/address.ts";
import { StreamId } from "../../../src/schema/index.ts";
import { Storage } from "../../../src/storage/storage.ts";
import { CommitBoundary } from "../../../src/storage/commit-boundary.ts";
import { OwnerRecords } from "../../../src/internal/owner-records.ts";

const sink = StreamAddress.json("fused-sink", { schema: Schema.String });

const turns = Effect.forEach([1, 2, 3, 4, 5], () => Effect.yieldNow, { discard: true });

for (const outcome of ["commit", "failure", "defect", "interrupt", "mutation-rejection"]) {
  test(`memory fused boundary: ${outcome}`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const boundary = yield* CommitBoundary;
        const records = yield* OwnerRecords;
        const storage = yield* Storage;
        expect((yield* Streams.create(sink))._tag).toBe("Created");
        yield* records.write("state", "0");
        yield* records.write("checkpoint", "0");

        const read = Effect.gen(function* () {
          const batches = yield* Streams.read(sink).pipe(Stream.runCollect);

          return {
            output: batches.flatMap((batch) => batch.items),
            state: Option.getOrThrow(yield* records.read("state")),
            checkpoint: Option.getOrThrow(yield* records.read("checkpoint")),
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
                  expect((yield* Streams.append(sink, ["one"]))._tag).toBe("Appended");
                  yield* records.write("state", "1");
                  yield* records.write("checkpoint", "1");
                }),
              );
              expect(yield* read).toEqual(after);
              yield* Deferred.succeed(written, undefined);
              yield* Deferred.await(release);

              if (outcome === "mutation-rejection")
                return yield* storage.mutate({
                  operations: [
                    { _tag: "Delete", streamId: StreamId.make("missing"), reason: "delete" },
                  ],
                });

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

        if (outcome === "mutation-rejection") {
          expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(false);
          expect(
            Exit.isFailure(exit) && Option.getOrThrow(Cause.findErrorOption(exit.cause)),
          ).toMatchObject({ _tag: "MutationRejected", reason: "not-found" });
        }

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
      }).pipe(
        Effect.provide(MemoryStorage.layerProtocol()),
        Effect.scoped,
        Effect.timeout("5 seconds"),
      ),
    ));
}

test("memory owner rejects inherited child-fiber use and separate owners", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const boundary = yield* CommitBoundary;
      const records = yield* OwnerRecords;
      yield* boundary.withTransaction(
        Effect.gen(function* () {
          const child = yield* records.write("state", "child").pipe(Effect.forkScoped);
          expect(Exit.isFailure(yield* Fiber.await(child))).toBe(true);

          const foreign = yield* Effect.gen(function* () {
            return yield* (yield* OwnerRecords).write("state", "foreign");
          }).pipe(Effect.provide(MemoryStorage.layerProtocol()), Effect.exit);

          expect(Exit.isFailure(foreign)).toBe(true);
        }),
      );
      expect(Option.isNone(yield* records.read("state"))).toBe(true);
    }).pipe(
      Effect.provide(MemoryStorage.layerProtocol()),
      Effect.scoped,
      Effect.timeout("5 seconds"),
    ),
  ));

test("memory outer owners serialize without losing stream output or records", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const boundary = yield* CommitBoundary;
      const records = yield* OwnerRecords;
      yield* Streams.create(sink);
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const entered = yield* Deferred.make<void>();

      const first = yield* boundary
        .withTransaction(
          Effect.gen(function* () {
            yield* Streams.append(sink, ["first"]);
            yield* records.write("state", "first");
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
            expect(Option.getOrThrow(yield* records.read("state"))).toBe("first");
            yield* Streams.append(sink, ["second"]);
            yield* records.write("state", "second");
          }),
        )
        .pipe(Effect.forkScoped);

      yield* turns;
      expect(yield* Deferred.isDone(entered)).toBe(false);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      expect(Option.getOrThrow(yield* records.read("state"))).toBe("second");
      const batches = yield* Streams.read(sink).pipe(Stream.runCollect);
      expect(batches.flatMap((batch) => batch.items)).toEqual(["first", "second"]);
    }).pipe(
      Effect.provide(MemoryStorage.layerProtocol()),
      Effect.scoped,
      Effect.timeout("5 seconds"),
    ),
  ));
