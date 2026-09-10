/* oxlint-disable eslint/no-underscore-dangle -- Effect results and stream refs use public tagged variants. */
/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own complete host graphs and deterministic test services. */
import { expect, test } from "bun:test";
import { Deferred, Effect, Fiber, Layer, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import { Streams, ZERO_OFFSET } from "@streamsy/core";
import { Commit, Projection, type Source } from "../src/index.ts";
import { layerMemory } from "../src/memory.ts";
import { minimal } from "../src/examples/minimal.ts";
import {
  composition,
  definition,
  failAfterSink,
  initialize,
  input,
  inspect,
  output,
} from "./scenarios.ts";

const run = <A, E>(
  body: Effect.Effect<
    A,
    E,
    Commit | import("@streamsy/core").StreamsReader | import("@streamsy/core").StreamsWriter
  >,
) => Effect.runPromise(body.pipe(Effect.provide(layerMemory())));

test("fresh ordered catch-up, fused rollback and new projection restart", () =>
  run(
    Effect.gen(function* () {
      const result = yield* composition;
      expect(result.first.status).toBe("limit-reached");
      expect(result.failed).toBe("Failure");
      expect(result.after).toEqual(result.before);
      expect(result.final.status).toBe("caught-up");
      expect(result.restart.items).toBe(0);
      expect(result.stored.output).toEqual([1, 3]);
      expect(result.stored.state.encoded).toBe("3");
      expect(result.stored.state.revision).toBe(result.stored.checkpoint.revision);
    }),
  ));

test("zero-output boundary advances state and checkpoint without sink progress", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const projection = yield* definition;
      const first = yield* Projection.pass(projection, { items: 1 });
      const second = yield* Projection.pass(projection, { items: 1 });
      expect(second.checkpoint.sinkPosition).toBe(first.checkpoint.sinkPosition);
      expect(second.checkpoint.sourcePosition).not.toBe(first.checkpoint.sourcePosition);
      expect((yield* inspect).state.encoded).toBe("0");
    }),
  ));

test("first commit failure leaves no output, state or checkpoint", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      expect((yield* failAfterSink)._tag).toBe("Failure");
      const owner = yield* Commit;
      expect(Option.isNone(yield* owner.states.load("sum"))).toBe(true);
      expect(Option.isNone(yield* owner.checkpoints.load("sum"))).toBe(true);
      expect(yield* Streams.read(output).pipe(Streams.items, Stream.runCollect)).toEqual([]);
    }),
  ));

test("byte limit is truthful and resumable without accepting an oversized boundary", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const projection = yield* definition;
      const limited = yield* Projection.catchUp(projection, { bytes: 1 });
      expect(limited.status).toBe("limit-reached");
      expect(limited.items).toBe(0);
      expect(limited.checkpoint.sourcePosition).toBe(ZERO_OFFSET);
      const resumed = yield* Projection.catchUp(projection, { bytes: 100 });
      expect(resumed.items).toBe(3);
      expect(resumed.bytes).toBe(4);
    }),
  ));

test("boundary limit counts accepted boundaries", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const base = yield* definition;
      const projection = {
        ...base,
        source: { ...base.source, pull: (after: string) => base.source.pull(after, { items: 1 }) },
      };
      const limited = yield* Projection.catchUp(projection, { boundaries: 2 });
      expect(limited.status).toBe("limit-reached");
      expect(limited.boundaries).toBe(2);
      expect(limited.items).toBe(2);
      expect((yield* Projection.catchUp(projection)).items).toBe(1);
    }),
  ));

for (const change of ["version", "generation", "source", "sink"] as const) {
  test(`stored ${change} mismatch stops without resetting`, () =>
    run(
      Effect.gen(function* () {
        yield* initialize;
        const projection = yield* definition;
        yield* Projection.catchUp(projection);
        const before = yield* inspect;
        const changed = {
          ...projection,
          identity: {
            id: projection.identity.id,
            version: projection.identity.version,
            generation: projection.identity.generation,
            source: projection.identity.source,
            sink: projection.identity.sink,
            [change]: "different",
          },
        };
        const result = yield* Projection.catchUp(changed).pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") expect(result.failure.reason).toBe("identity-mismatch");
        expect(yield* inspect).toEqual(before);
      }),
    ));
}

test("closed source drains available work before terminal outcome", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      yield* Streams.append(input, [], { close: true });
      const result = yield* definition.pipe(Effect.flatMap(Projection.catchUp));
      expect(result.status).toBe("source-closed");
      expect(result.items).toBe(3);
    }),
  ));

test("missing history preserves accepted state", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const projection = yield* definition;
      yield* Projection.pass(projection, { items: 1 });
      const before = yield* inspect;
      yield* Streams.remove(input);
      const result = yield* Projection.catchUp(projection).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure.reason).toBe("history-unavailable");
      expect(yield* inspect).toEqual(before);
    }),
  ));

test("invalid stored state and mismatched revisions stop", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const projection = yield* definition;
      yield* Projection.catchUp(projection);
      const owner = yield* Commit;
      const before = yield* inspect;
      for (const state of [
        { revision: before.state.revision, encoded: '"wrong"' },
        { encoded: before.state.encoded, revision: 42 },
      ]) {
        yield* owner.states.save("sum", state);
        const result = yield* Projection.catchUp(projection).pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") expect(result.failure.reason).toBe("invalid-state");
      }
    }),
  ));

test("sink expected-offset conflict does not advance checkpoint", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      yield* Streams.append(output, [99]);
      const result = yield* definition.pipe(Effect.flatMap(Projection.catchUp), Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure.reason).toBe("sink-conflict");
      expect(Option.isNone(yield* (yield* Commit).checkpoints.load("sum"))).toBe(true);
    }),
  ));

test("follow repairs a missed wake and closes; cancellation releases parked waits", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      const base = yield* definition;
      const waiting = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      const source: Source<number> = {
        ...base.source,
        wait: () =>
          Deferred.succeed(waiting, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(released, undefined)),
          ),
      };
      const fiber = yield* Projection.follow({ ...base, source }, { repairIntervalMs: 100 });
      yield* Deferred.await(waiting);
      yield* Streams.append(input, [7], { close: true });
      yield* TestClock.adjust(100);
      const result = yield* Fiber.join(fiber);
      expect(result.status).toBe("source-closed");
      expect((yield* inspect).output).toEqual([7]);
      yield* Deferred.await(released);
      const cancelled = yield* Deferred.make<void>();
      const parked = yield* Deferred.make<void>();
      const idle: Source<number> = {
        identity: "idle",
        initialPosition: "0",
        pull: () =>
          Effect.succeed({
            status: "boundary",
            items: [],
            endPosition: "0",
            upToDate: true,
            closed: false,
          }),
        wait: () =>
          Deferred.succeed(parked, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(cancelled, undefined)),
          ),
      };
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Projection.follow(Projection.make({ ...base, id: "idle", source: idle }));
          yield* Deferred.await(parked);
        }),
      );
      yield* Deferred.await(cancelled);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerMemory(), TestClock.layer()))),
  ));

test("invalid limits fail before reading", () =>
  run(
    Effect.gen(function* () {
      const projection = yield* definition;
      for (const limits of [{ items: 0 }, { bytes: -1 }, { boundaries: Infinity }]) {
        const result = yield* Projection.catchUp(projection, limits).pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") expect(result.failure.reason).toBe("invalid-limits");
      }
    }),
  ));

test("compiled minimal example accepts three inputs", () =>
  run(
    Effect.gen(function* () {
      const result = yield* minimal;
      expect(result.status).toBe("caught-up");
      expect(result.items).toBe(3);
    }),
  ));

test("foreign Commit is rejected before reading source", () =>
  run(
    Effect.gen(function* () {
      const projection = yield* definition;
      const owner = yield* Commit;
      const foreign = Commit.of({ ...owner });
      const result = yield* Projection.catchUp(projection).pipe(
        Effect.provideService(Commit, foreign),
        Effect.result,
      );
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure.reason).toBe("unsupported-composition");
    }),
  ));

test("malformed cursor stops before pulling source", () =>
  run(
    Effect.gen(function* () {
      yield* initialize;
      const projection = yield* definition;
      const result = yield* projection.source.pull("invalid", { items: 1 }).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure.reason).toBe("invalid-state");
    }),
  ));

test("nonadvancing source is rejected without a commit", () =>
  run(
    Effect.gen(function* () {
      const projection = yield* definition;
      const source: Source<number> = {
        ...projection.source,
        pull: () =>
          Effect.succeed({
            status: "boundary",
            items: [1],
            endPosition: ZERO_OFFSET,
            upToDate: false,
            closed: false,
          }),
      };
      const result = yield* Projection.catchUp({ ...projection, source }).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure.reason).toBe("invalid-source");
    }),
  ));

test("follow paces an oversized boundary even when source wait would return immediately", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const base = yield* definition;
      const pulled = yield* Deferred.make<void>();
      let pulls = 0;
      const source: Source<number> = {
        ...base.source,
        pull: () =>
          Effect.gen(function* () {
            pulls += 1;
            yield* Deferred.succeed(pulled, undefined);
            return pulls === 1
              ? ({ status: "limit-reached" } as const)
              : ({
                  status: "boundary",
                  items: [],
                  endPosition: ZERO_OFFSET,
                  upToDate: true,
                  closed: true,
                } as const);
          }),
        wait: () => Effect.void,
      };
      const fiber = yield* Projection.follow({ ...base, source }, { repairIntervalMs: 100 });
      yield* Deferred.await(pulled);
      yield* Effect.yieldNow;
      expect(pulls).toBe(1);
      yield* TestClock.adjust(100);
      expect((yield* Fiber.join(fiber)).status).toBe("source-closed");
      expect(pulls).toBe(2);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerMemory(), TestClock.layer()))),
  ));
