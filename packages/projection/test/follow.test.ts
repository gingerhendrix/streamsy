import { layer } from "@effect/vitest";
import { Streamsy } from "@streamsy/core";
import { Commit } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { ProjectionRetry } from "@streamsy/projection";
/* oxlint-disable effecttsgo/strict-effect-provide -- Bun owns the runtime boundary; each test assembles the complete host graph with a test clock. */
import { expect, test } from "vitest";
import {
  Array,
  Duration,
  Deferred,
  Effect,
  Fiber,
  Exit,
  Option,
  Layer,
  Queue,
  Schema,
  Schedule,
  Stream,
  type Scope,
} from "effect";
import { TestClock } from "effect/testing";
import { StorageFault, TransportFault, type StreamsFault } from "@streamsy/core";
import { Streams, StreamsReader, StreamsWriter, type Reader } from "@streamsy/core/protocol";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { layerSqlite } from "./support/sqlite.ts";
import { memoryHost } from "./support/memory-host.ts";
import {
  input,
  output,
  positives as transactionalPositives,
  readAll,
} from "./support/scenarios.ts";

type Services =
  | CheckpointStore
  | import("@streamsy/core").Streamsy.Wildcard
  | StreamsReader
  | StreamsWriter
  | Scope.Scope;

const positives = { ...transactionalPositives, commit: Commit.atLeastOnce };

const run = <A, E>(body: Effect.Effect<A, E, Services>) =>
  Effect.runPromise(
    body.pipe(
      Effect.scoped,
      Effect.provide(FamilyFixture.withFamilies(Layer.merge(memoryHost(), TestClock.layer()))),
    ),
  );

const withReader = <A, E, R>(
  body: Effect.Effect<A, E, R | StreamsReader>,
  patch: (reader: Reader<StreamsFault>) => Partial<Reader<StreamsFault>>,
) =>
  Effect.gen(function* () {
    const reader = yield* StreamsReader;

    return yield* body.pipe(
      FamilyFixture.provideReader(StreamsReader.of({ ...reader, ...patch(reader) })),
    );
  });

/** Runs a follower to its last element: the terminal close. */
const lastOf = <A, E, R>(stream: Stream.Stream<A, E, R>) =>
  Stream.runLast(stream).pipe(Effect.map(Option.getOrThrow));

/**
 * Runs a follower in the test scope and keeps each element in a queue. The queue has no
 * bound, so the follower does not wait for the test to read an element.
 */
const observe = <A, E, R>(stream: Stream.Stream<A, E, R>) =>
  Effect.gen(function* () {
    const seen = yield* Queue.unbounded<A>();

    const fiber = yield* stream.pipe(
      Stream.runForEach((element) => Queue.offer(seen, element)),
      Effect.forkScoped,
    );

    /** Waits, without a second writer, for the first element that matches. */
    const until = (predicate: (element: A) => boolean) =>
      Stream.fromQueue(seen).pipe(Stream.takeUntil(predicate), Stream.runCollect);

    return { seen, fiber, until };
  });

/** A wake hint that parks forever and reports when it is parked and when it is released. */
const parkedHint = Effect.gen(function* () {
  const parked = yield* Deferred.make<void>();
  const released = yield* Deferred.make<void>();

  const patch = (): Partial<Reader<StreamsFault>> => ({
    readNext: () =>
      Deferred.succeed(parked, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Deferred.succeed(released, undefined)),
      ),
  });

  return { parked, released, patch };
});

test("follow repairs a missed wake and closes", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      const hint = yield* parkedHint;

      const fiber = yield* withReader(
        lastOf(Projection.follow(positives, { repairIntervalMs: 100 })),
        hint.patch,
      ).pipe(Effect.forkScoped);

      yield* Deferred.await(hint.parked);
      yield* Streams.append(input, [7], { close: true });
      yield* TestClock.adjust(100);
      const result = yield* Fiber.join(fiber);
      expect(result.status).toBe("source-closed");
      expect(yield* readAll(output)).toEqual([7]);
      yield* Deferred.await(hint.released);
    }),
  ));

test("follow cancellation releases parked waits", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      const hint = yield* parkedHint;
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withReader(Stream.runDrain(Projection.follow(positives)), hint.patch).pipe(
            Effect.forkScoped,
          );
          yield* Deferred.await(hint.parked);
        }),
      );
      yield* Deferred.await(hint.released);
      expect((yield* (yield* CheckpointStore).load(Projection.key(positives))).token).toBe("0");
    }),
  ));

test("follow defaults to a 30 second repair interval", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      const hint = yield* parkedHint;

      const fiber = yield* withReader(lastOf(Projection.follow(positives)), hint.patch).pipe(
        Effect.forkScoped,
      );

      yield* Deferred.await(hint.parked);
      yield* Streams.append(input, [7], { close: true });
      yield* TestClock.adjust(29_999);
      expect(yield* readAll(output)).toEqual([]);
      yield* TestClock.adjust(1);
      expect((yield* Fiber.join(fiber)).status).toBe("source-closed");
      expect(yield* readAll(output)).toEqual([7]);
    }),
  ));

test("a write to the second input wakes a two-input follower", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const a = FamilyFixture.json("wake-a", { schema: Schema.Finite });
      const b = FamilyFixture.json("wake-b", { schema: Schema.Finite });
      const target = FamilyFixture.json("wake-out", { schema: Schema.String });
      yield* Streams.create(a);
      yield* Streams.create(b);
      yield* Streams.create(target);
      yield* Streams.append(a, [1]);

      const projection = Projection.process({
        commit: Commit.transaction,
        id: "wake",
        params: {},
        inputs: { a, b },
        process: (batch) =>
          Effect.gen(function* () {
            const entries = Array.map(
              Projection.items(batch),
              (entry) => `${entry.input}:${entry.item}`,
            );

            if (Array.isReadonlyArrayNonEmpty(entries)) yield* Streams.append(target, entries);
          }),
      }).at({});

      const parked = yield* Deferred.make<void>();
      let hints = 0;

      const fiber = yield* withReader(
        Projection.follow(projection, { repairIntervalMs: 10_000 }).pipe(
          Stream.take(2),
          Stream.runCollect,
        ),
        (reader) => ({
          readNext: (id, options) =>
            Effect.gen(function* () {
              hints += 1;

              if (hints === 2) yield* Deferred.succeed(parked, undefined);

              return yield* reader.readNext(id, options);
            }),
        }),
      ).pipe(Effect.forkScoped);

      // Both inputs are parked on their hints after the first run drains `a`.
      yield* Deferred.await(parked);
      expect(yield* readAll(target)).toEqual(["a:1"]);
      yield* Streams.append(b, [2]);
      // The second element is the committed cycle that the write to `b` woke.
      const [first, second] = yield* Fiber.join(fiber);
      expect(first).toMatchObject({ status: "caught-up", units: 1, items: 1 });
      expect(second).toMatchObject({ status: "caught-up", units: 1, items: 1 });
      expect(second?.offsets.b).toBe((yield* Streams.head(b)).nextOffset);
      const owner = yield* CheckpointStore;
      expect((yield* owner.load(Projection.key(projection))).token).toBe("2");
      expect(yield* readAll(target)).toEqual(["a:1", "b:2"]);
    }).pipe(
      Effect.scoped,
      Effect.provide(FamilyFixture.withFamilies(Layer.merge(layerSqlite(), TestClock.layer()))),
    ),
  ));

test("a closed and drained input beside an open one does not wake every cycle", () =>
  run(
    Effect.gen(function* () {
      const a = FamilyFixture.json("closed-a", { schema: Schema.Finite });
      const b = FamilyFixture.json("open-b", { schema: Schema.Finite });
      const target = FamilyFixture.json("closed-out", { schema: Schema.String });
      yield* Streams.create(a);
      yield* Streams.create(b);
      yield* Streams.create(target);
      yield* Streams.append(a, [1], { close: true });
      yield* Streams.append(b, [2]);

      const projection = Projection.process({
        commit: Commit.atLeastOnce,
        id: "closed-open",
        params: {},
        inputs: { a, b },
        process: (batch) =>
          Effect.gen(function* () {
            const entries = Array.map(
              Projection.items(batch),
              (entry) => `${entry.input}:${entry.item}`,
            );

            if (Array.isReadonlyArrayNonEmpty(entries)) yield* Streams.append(target, entries);
          }),
      }).at({});

      let reads = 0;
      let hints = 0;

      const fiber = yield* withReader(
        Stream.runDrain(Projection.follow(projection, { repairIntervalMs: 100 })),
        (reader) => ({
          read: (id, options) =>
            Effect.suspend(() => {
              reads += 1;

              return reader.read(id, options);
            }),
          readNext: (id, options) =>
            Effect.suspend(() => {
              hints += 1;

              return reader.readNext(id, options);
            }),
        }),
      ).pipe(Effect.forkScoped);

      // The first run is one non-empty pass and one empty pass, then one hint per input.
      const settle = (expectedHints: number) =>
        Effect.gen(function* () {
          for (let spins = 0; spins < 1000; spins += 1) {
            if (hints >= expectedHints) break;
            yield* Effect.yieldNow;
          }

          for (let spins = 0; spins < 50; spins += 1) yield* Effect.yieldNow;
        });

      yield* settle(2);
      expect(yield* readAll(target)).toEqual(["a:1", "b:2"]);
      expect(reads).toBe(4);
      expect(hints).toBe(2);
      // The closed input answered at once; without the clock moving nothing else runs.
      yield* TestClock.adjust(100);
      yield* settle(4);
      expect(reads).toBe(6);
      expect(hints).toBe(4);
      yield* TestClock.adjust(100);
      yield* settle(6);
      expect(reads).toBe(8);
      expect(hints).toBe(6);
      // A write to the open input still wakes the follower before the interval.
      yield* Streams.append(b, [3]);
      yield* settle(8);
      expect(yield* readAll(target)).toEqual(["a:1", "b:2", "b:3"]);
      expect(reads).toBe(12);
      yield* Fiber.interrupt(fiber);
    }),
  ));

test("a completed wake hint echoes its cursor on the next hint for that input", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      const cursors: (string | undefined)[] = [];
      const second = yield* Deferred.make<void>();

      const fiber = yield* withReader(
        Stream.runDrain(Projection.follow(positives, { repairIntervalMs: 10_000 })),
        () => ({
          readNext: (_id, options) => {
            cursors.push(options.cursor);

            return cursors.length === 1
              ? Effect.succeed({
                  contentType: "application/json",
                  messages: [],
                  nextOffset: options.offset,
                  upToDate: true,
                  closed: false,
                  cursor: "cursor-1",
                  timedOut: true,
                })
              : Deferred.succeed(second, undefined).pipe(Effect.andThen(Effect.never));
          },
        }),
      ).pipe(Effect.forkScoped);

      yield* Deferred.await(second);
      expect(cursors).toEqual([undefined, "cursor-1"]);
      yield* Fiber.interrupt(fiber);
    }),
  ));

test("follow rejects invalid options at the first pull, before reading", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      let reads = 0;

      for (const options of [
        { repairIntervalMs: 0 },
        { repairIntervalMs: -1 },
        { repairIntervalMs: Infinity },
        { repairIntervalMs: 1.5 },
        { limit: 0 },
      ]) {
        // Building the stream does no work; the failure comes when it runs.
        const follower = Projection.follow(positives, options);

        const result = yield* withReader(Stream.runDrain(follower), (reader) => ({
          read: (id, readOptions) =>
            Effect.suspend(() => {
              reads += 1;

              return reader.read(id, readOptions);
            }),
        })).pipe(Effect.result);

        expect(result._tag).toBe("Failure");

        if (result._tag === "Failure")
          expect(result.failure).toMatchObject({ phase: "load", reason: "invalid-options" });
      }

      expect(reads).toBe(0);
    }),
  ));

const sum = Projection.fold({
  id: "follow-sum",
  params: {},
  inputs: { input },
  state: { schema: Schema.Finite, initial: 0 },
  fold: (total, { item }) => total + item,
}).at({});

test("follow emits the first cycle, each commit and the close, and skips idle wakes", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      const hints = yield* Queue.unbounded<number>();
      let count = 0;

      const fiber = yield* withReader(
        Stream.runCollect(Projection.follow(sum, { repairIntervalMs: 100 })),
        (reader) => ({
          readNext: (id, options) =>
            Effect.suspend(() => Queue.offer(hints, (count += 1))).pipe(
              Effect.andThen(reader.readNext(id, options)),
            ),
        }),
      ).pipe(Effect.forkScoped);

      // The first cycle commits nothing, emits, and parks on hint 1.
      expect(yield* Queue.take(hints)).toBe(1);
      // Two repair wakes find no input and emit nothing.
      yield* TestClock.adjust(100);
      expect(yield* Queue.take(hints)).toBe(2);
      yield* TestClock.adjust(100);
      expect(yield* Queue.take(hints)).toBe(3);
      yield* Streams.append(input, [7]);
      expect(yield* Queue.take(hints)).toBe(4);
      yield* Streams.append(input, [8], { close: true });
      const cycles = yield* Fiber.join(fiber);

      expect(
        Array.map(cycles, ({ status, units, items, state }) => ({ status, units, items, state })),
      ).toEqual([
        { status: "caught-up", units: 0, items: 0, state: 0 },
        { status: "caught-up", units: 1, items: 1, state: 7 },
        { status: "source-closed", units: 1, items: 1, state: 15 },
      ]);
      expect(cycles[2]?.offsets.input).toBe((yield* Streams.head(input)).nextOffset);
    }),
  ));

test("a slow consumer gets one cycle for the input that arrived while it worked", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      let calls = 0;

      const counted = Projection.process({
        commit: Commit.atLeastOnce,
        id: "follow-slow-consumer",
        params: {},
        inputs: { input },
        process: () =>
          Effect.sync(() => {
            calls += 1;
          }),
      }).at({});

      const seen = yield* Queue.unbounded<{ readonly units: number; readonly items: number }>();

      const fiber = yield* Projection.follow(counted).pipe(
        Stream.runForEach((progress) =>
          Queue.offer(seen, progress).pipe(Effect.andThen(Effect.sleep("1 second"))),
        ),
        Effect.forkScoped,
      );

      expect(yield* Queue.take(seen)).toMatchObject({ units: 0, items: 0 });
      // The consumer is busy, so the follower does not pull, wait or run.
      yield* Streams.append(input, [1]);
      yield* Streams.append(input, [2]);
      yield* Streams.append(input, [3]);

      for (let spins = 0; spins < 50; spins += 1) yield* Effect.yieldNow;
      expect(calls).toBe(0);
      yield* TestClock.adjust("1 second");
      expect(yield* Queue.take(seen)).toMatchObject({ units: 1, items: 3 });
      expect(calls).toBe(1);
      yield* Fiber.interrupt(fiber);
    }),
  ));

test("a consumer that takes one element ends the follower before any wait", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      yield* Streams.append(input, [7]);
      const hint = yield* parkedHint;

      const cycles = yield* withReader(
        Projection.follow(positives).pipe(Stream.take(1), Stream.runCollect),
        hint.patch,
      );

      expect(Array.map(cycles, ({ status, units }) => [status, units])).toEqual([["caught-up", 1]]);
      expect(yield* Deferred.isDone(hint.parked)).toBe(false);
    }),
  ));

test("interrupting the consumer stops the follower and releases its parked wait", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      const hint = yield* parkedHint;
      const first = yield* Deferred.make<number>();

      const fiber = yield* withReader(
        Projection.follow(positives).pipe(
          Stream.runForEach((progress) => Deferred.succeed(first, progress.units)),
        ),
        hint.patch,
      ).pipe(Effect.forkScoped);

      expect(yield* Deferred.await(first)).toBe(0);
      yield* Deferred.await(hint.parked);
      yield* Fiber.interrupt(fiber);
      yield* Deferred.await(hint.released);
      yield* Streams.append(input, [7]);

      for (let spins = 0; spins < 50; spins += 1) yield* Effect.yieldNow;
      expect(yield* readAll(output)).toEqual([]);
    }),
  ));

for (const point of ["read", "append"] as const) {
  test(`follow retries a TransportFault on ${point} and retains each item once`, () =>
    run(
      Effect.gen(function* () {
        yield* Streams.create(input);
        yield* Streams.append(input, [1, 2, 3], { close: true });
        const reader = yield* StreamsReader;
        const writer = yield* StreamsWriter;
        let failures = 0;

        const lost = new TransportFault({
          reason: "response",
          status: 503,
          operation: point,
          message: "lost reply",
        });

        const projection = Projection.emit({
          commit: Commit.producer,
          id: `retry-${point}`,
          params: {},
          inputs: { input },
          outputs: { output: output },
          emit: (entry) => Effect.succeed({ output: [entry.item] }),
        }).at({});

        const result = yield* lastOf(
          Projection.follow(projection, { retry: Schedule.recurs(1) }),
        ).pipe(
          FamilyFixture.provideReader({
            ...reader,
            read: (id, options) =>
              Effect.suspend(() => {
                if (point === "read" && failures++ === 0) return Effect.fail(lost);

                return reader.read(id, options);
              }),
          }),
          FamilyFixture.provideWriter({
            ...writer,
            append: (id, options) =>
              Effect.suspend(() => {
                const append = writer.append(id, options);

                return point === "append" && failures++ === 0
                  ? append.pipe(Effect.andThen(Effect.fail(lost)))
                  : append;
              }),
          }),
        );

        expect(result.status).toBe("source-closed");
        expect(yield* readAll(output)).toEqual([1, 2, 3]);
      }),
    ));
}

test("follow does not retry non-storage faults or handler failures", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);

      for (const error of [
        new ProjectionFault({ phase: "process", reason: "invalid-output", message: "invalid" }),
        "handler-error",
      ] as const) {
        let calls = 0;

        const projection = Projection.process({
          commit: Commit.atLeastOnce,
          id: "terminal",
          params: {},
          inputs: { input },
          process: () =>
            Effect.suspend(() => {
              calls += 1;

              return Effect.fail(error);
            }),
        }).at({});

        const result = yield* Stream.runDrain(
          Projection.follow(projection, { retry: Schedule.recurs(3) }),
        ).pipe(Effect.result);

        expect(result._tag).toBe("Failure");

        if (result._tag === "Failure") expect(result.failure).toBe(error);
        expect(calls).toBe(1);
      }
    }),
  ));

test("default follow backoff retries a read after the clock advances", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.create(output);
      yield* Streams.append(input, [7], { close: true });
      const failed = yield* Deferred.make<void>();
      let reads = 0;

      const fiber = yield* withReader(lastOf(Projection.follow(positives)), (reader) => ({
        read: (id, options) =>
          Effect.suspend(() => {
            reads += 1;

            return reads === 1
              ? Deferred.succeed(failed, undefined).pipe(
                  Effect.andThen(
                    Effect.fail(
                      new TransportFault({
                        reason: "request",
                        operation: "read",
                        message: "offline",
                      }),
                    ),
                  ),
                )
              : reader.read(id, options);
          }),
      })).pipe(Effect.forkScoped);

      yield* Deferred.await(failed);
      yield* TestClock.adjust(240);
      expect((yield* Fiber.join(fiber)).status).toBe("source-closed");
      expect(yield* readAll(output)).toEqual([7]);
    }),
  ));

test("default retry remains jittered, capped and unbounded after many failures", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const step = yield* Schedule.toStep(yield* ProjectionRetry);

      for (let attempt = 0; attempt < 1100; attempt += 1) {
        const [, delay] = yield* step(attempt, undefined);
        const millis = Duration.toMillis(delay);
        expect(millis).toBeLessThanOrEqual(30_000);
        expect(millis).toBeGreaterThanOrEqual(attempt === 0 ? 160 : 1);

        if (attempt === 0) expect(millis).toBeLessThanOrEqual(240);

        if (attempt >= 8) expect(millis).toBeGreaterThanOrEqual(24_000);
      }
    }),
  ));

test("follow resets a finite retry budget after a successful cycle between outages", () =>
  run(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);
      const parked = yield* Deferred.make<void>();
      let reads = 0;

      const projection = Projection.emit({
        commit: Commit.producer,
        id: "separate-outages",
        params: {},
        inputs: { input },
        outputs: { output: output },
        emit: (entry) => Effect.succeed({ output: [entry.item] }),
      }).at({});

      const fiber = yield* withReader(
        lastOf(Projection.follow(projection, { retry: Schedule.recurs(1) })),
        (reader) => ({
          read: (id, options) =>
            Effect.suspend(() => {
              reads += 1;

              return reads === 1 || reads === 4
                ? Effect.fail(
                    new TransportFault({
                      reason: "request",
                      operation: "read",
                      message: "one failure per outage",
                    }),
                  )
                : reader.read(id, options);
            }),
          readNext: (id, options) =>
            Deferred.succeed(parked, undefined).pipe(Effect.andThen(reader.readNext(id, options))),
        }),
      ).pipe(Effect.forkScoped);

      yield* Deferred.await(parked);
      expect(yield* readAll(output)).toEqual([1]);
      yield* Streams.append(input, [2], { close: true });
      expect((yield* Fiber.join(fiber)).status).toBe("source-closed");
      expect(reads).toBe(6);
      expect(yield* readAll(output)).toEqual([1, 2]);
    }),
  ));

for (const explicit of [false, true]) {
  test(`follow ${explicit ? "prefers options.retry over" : "uses"} ProjectionRetry`, () =>
    run(
      Effect.gen(function* () {
        yield* Streams.create(input);
        const reader = yield* StreamsReader;
        let attempts = 0;

        const broken = {
          ...reader,
          read: () =>
            Effect.suspend(() => {
              attempts += 1;

              return Effect.fail(
                new TransportFault({
                  reason: "request",
                  operation: "read",
                  message: "offline",
                }),
              );
            }),
        };

        const result = yield* Stream.runDrain(
          Projection.follow(positives, explicit ? { retry: Schedule.recurs(2) } : {}),
        ).pipe(
          Effect.provideService(ProjectionRetry, Schedule.recurs(0)),
          FamilyFixture.provideReader(broken),
          Effect.result,
        );

        expect(result._tag).toBe("Failure");
        expect(attempts).toBe(explicit ? 3 : 1);
        // run stays single-shot: one read, no retry.
        yield* Projection.run(positives).pipe(FamilyFixture.provideReader(broken), Effect.result);
        expect(attempts).toBe(explicit ? 4 : 2);
      }),
    ));
}

for (const cause of [
  new StorageFault({ operation: "read", message: "store closed", retryable: false }),
  new TransportFault({ operation: "read", reason: "configuration", message: "bad URL" }),
]) {
  test(`follow stops on a storage failure with a non-retryable ${cause.name} cause`, () =>
    run(
      Effect.gen(function* () {
        yield* Streams.create(input);
        let reads = 0;

        const failure = yield* withReader(
          Stream.runDrain(Projection.follow(positives, { retry: Schedule.recurs(3) })),
          () => ({
            read: () =>
              Effect.suspend(() => {
                reads += 1;

                return Effect.fail(cause);
              }),
          }),
        ).pipe(Effect.flip);

        expect(failure).toBeInstanceOf(ProjectionFault);
        expect(failure).toMatchObject({ reason: "storage-failure", cause, retryable: false });
        expect(reads).toBe(1);
      }),
    ));
}

class FollowRoute2 extends Streamsy.json<FollowRoute2>()(
  "on-change.test/FollowRoute2",
  "change-stream-scope/:workspaceId",
  {
    params: { workspaceId: Schema.String },
    schema: Schema.Finite,
  },
) {}

class FollowRoute1 extends Streamsy.json<FollowRoute1>()(
  "on-change.test/FollowRoute1",
  "change-stream/:workspaceId",
  {
    params: { workspaceId: Schema.String },
    schema: Schema.Finite,
  },
) {}

layer(FamilyFixture.withFamilies(memoryHost(), FollowRoute1, FollowRoute2))((it) => {
  it.effect("changes during an in-flight run coalesce to one follow-up run", () =>
    Effect.gen(function* () {
      const source = FamilyFixture.json("change-coalesce-input", {
        schema: Schema.Finite,
      });

      const target = FamilyFixture.json("change-coalesce-output", {
        schema: Schema.Finite,
      });

      yield* Streams.create(source);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const second = yield* Deferred.make<void>();
      let calls = 0;

      const projection = Projection.mapAccumBatch({
        commit: Commit.producer,
        id: "change-coalesce",
        params: {},
        inputs: { input: source },
        outputs: { output: target },
        state: { schema: Schema.Null, initial: null },
        mapAccumBatch: (_state, batch) =>
          Effect.gen(function* () {
            calls += 1;

            if (calls === 1) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            } else yield* Deferred.succeed(second, undefined);

            return [null, { output: batch.input.items }] as const;
          }),
      }).at({});

      const follower = yield* observe(Projection.follow(projection));
      yield* Streams.append(source, [1]);
      yield* Deferred.await(entered);
      yield* Streams.append(source, [2]);
      yield* Streams.append(source, [3]);
      yield* Streams.append(source, [4]);
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(second);
      const end = (yield* Streams.head(source)).nextOffset;
      yield* follower.until((progress) => progress.offsets.input === end);
      expect(calls).toBe(2);
      expect(yield* readAll(target)).toEqual([1, 2, 3, 4]);
      const tail = yield* Streams.head(source);
      const loaded = yield* (yield* CheckpointStore).load(Projection.key(projection));
      expect(Option.getOrThrow(loaded.record).inputs.input).toBe(tail.nextOffset);
      yield* Fiber.interrupt(follower.fiber);
    }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost({ readLimit: 1 })))((it) => {
  it.effect("a limited wake drains the backlog before waiting for another change", () =>
    Effect.gen(function* () {
      const source = FamilyFixture.json("change-limit-input", {
        schema: Schema.Finite,
      });

      yield* Streams.create(source);
      yield* Streams.append(source, [1, 2, 3]);
      const seen: number[] = [];
      const drained = yield* Deferred.make<void>();

      const projection = Projection.process({
        commit: Commit.atLeastOnce,
        id: "change-limit",
        params: {},
        inputs: { input: source },
        process: (batch) =>
          Effect.gen(function* () {
            seen.push(...batch.input.items);

            if (seen.length === 3) yield* Deferred.succeed(drained, undefined);
          }),
      }).at({});

      const follower = yield* observe(Projection.follow(projection, { limit: 1 }));
      yield* Deferred.await(drained);
      const end = (yield* Streams.head(source)).nextOffset;
      const cycles = yield* follower.until((progress) => progress.offsets.input === end);
      // Each limited cycle emits; the empty cycle that finds the backlog drained does not.
      expect(Array.map(cycles, ({ status, units }) => [status, units])).toEqual([
        ["limit-reached", 1],
        ["limit-reached", 1],
        ["limit-reached", 1],
      ]);
      expect(seen).toEqual([1, 2, 3]);
      const tail = yield* Streams.head(source);
      const loaded = yield* (yield* CheckpointStore).load(Projection.key(projection));
      expect(Option.getOrThrow(loaded.record).inputs.input).toBe(tail.nextOffset);
      yield* Fiber.interrupt(follower.fiber);
    }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), FollowRoute1, FollowRoute2))((it) => {
  it.effect("a stream follows its fixed address set independently", () =>
    Effect.gen(function* () {
      const seen: string[] = [];
      const a = yield* Deferred.make<void>();
      const b = yield* Deferred.make<void>();

      const declared = Projection.process({
        commit: Commit.atLeastOnce,
        id: "change-stream",
        params: { workspaceId: Schema.String },
        inputs: { facts: FollowRoute1 },
        process: (_batch, unit) =>
          Effect.gen(function* () {
            const workspaceId = unit.params.workspaceId;
            seen.push(workspaceId);
            yield* Deferred.succeed(workspaceId === "a" ? a : b, undefined);
          }),
      });

      const aRef = FollowRoute1.at({ workspaceId: "a" });
      const bRef = FollowRoute1.at({ workspaceId: "b" });
      yield* Streams.create(aRef);
      yield* Streams.create(bRef);

      const fibers = yield* Effect.forEach(["a", "b"], (workspaceId) =>
        Stream.runDrain(Projection.follow(declared.at({ workspaceId }))).pipe(Effect.forkScoped),
      );

      yield* Streams.append(aRef, [1]);
      yield* Deferred.await(a);
      expect(seen).toEqual(["a"]);
      yield* Streams.append(bRef, [2]);
      yield* Deferred.await(b);
      expect(seen).toEqual(["a", "b"]);
      yield* Effect.forEach(fibers, Fiber.interrupt);
    }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), FollowRoute1, FollowRoute2))((it) => {
  it.effect("closing the caller scope interrupts every stream address fiber", () =>
    Effect.gen(function* () {
      const declared = Projection.process({
        commit: Commit.atLeastOnce,
        id: "change-stream-scope",
        params: { workspaceId: Schema.String },
        inputs: { facts: FollowRoute2 },
        process: () => Effect.void,
      });

      for (const workspaceId of ["a", "b"]) yield* Streams.create(FollowRoute2.at({ workspaceId }));
      const parked = yield* Deferred.make<void>();
      let hints = 0;
      const route = yield* FollowRoute2;

      const fibers = yield* Effect.scoped(
        Effect.gen(function* () {
          const followers = yield* Effect.forEach(["a", "b"], (workspaceId) =>
            Stream.runDrain(Projection.follow(declared.at({ workspaceId }))).pipe(
              Effect.provideService(FollowRoute2, {
                ...route,
                wire: {
                  ...route.wire,
                  readNext: (id, options) =>
                    Effect.gen(function* () {
                      hints += 1;

                      if (hints === 2) yield* Deferred.succeed(parked, undefined);

                      return yield* route.wire.readNext(id, options);
                    }),
                },
              }),
              Effect.forkScoped,
            ),
          );

          yield* Deferred.await(parked);

          return followers;
        }),
      );

      for (const fiber of fibers) expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true);
    }),
  );
});
