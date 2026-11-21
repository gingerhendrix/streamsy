import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Deferred, Effect, Exit, Fiber, Option, Schema } from "effect";
import { Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";
import { TestClock } from "effect/testing";
import { withFamilies } from "./support/families.ts";
import { memoryHost } from "./support/memory-host.ts";

class Source extends Streamsy.json<Source>()("CatchUp/Source", "catch-up/source/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

class Output extends Streamsy.json<Output>()("CatchUp/Output", "catch-up/output/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

const host = withFamilies(memoryHost({ readLimit: 1 }), Source, Output);

const declaration = Projection.process({
  id: "catch-up",
  params: { id: Schema.String },
  inputs: { source: Source },
  commit: Commit.atLeastOnce,
  process: () => Effect.void,
});

const historyUnavailable = { phase: "read", reason: "history-unavailable", input: "source" };

layer(host)((it) => {
  it.effect("compares a persisted two-input vector while one source keeps advancing", () =>
    Effect.gen(function* () {
      const a = Source.at({ id: "a" });
      const b = Source.at({ id: "b" });

      const bound = Projection.process({
        id: "catch-up-multi",
        params: {},
        inputs: { a, b },
        commit: Commit.atLeastOnce,
        process: () => Effect.void,
      }).at({});

      yield* Streams.create(a);
      yield* Streams.create(b);
      const first = yield* Streams.append(a, ["a1"]);
      yield* Streams.append(b, ["b1"]);
      const second = yield* Streams.append(b, ["b2"]);
      const captured = yield* Projection.tails(bound);
      expect(captured).toEqual({
        a: { stream: a.id, offset: first.offset, closed: false },
        b: { stream: b.id, offset: second.offset, closed: false },
      });

      const json = Schema.fromJsonString(Projection.Tails);
      const fixed = yield* Schema.decodeEffect(json)(yield* Schema.encodeEffect(json)(captured));
      const ahead = yield* Streams.append(a, ["a2"]);
      yield* Projection.pass(bound);
      expect(yield* Projection.lag(bound, fixed)).toMatchObject({
        caughtUp: false,
        inputs: { a: { behind: false }, b: { behind: true } },
      });

      yield* Streams.append(a, ["a3"]);
      yield* Projection.pass(bound);
      expect(yield* Projection.lag(bound, fixed)).toEqual({
        caughtUp: true,
        inputs: {
          a: { committed: ahead.offset, target: first.offset, behind: false },
          b: { committed: second.offset, target: second.offset, behind: false },
        },
      });
      expect((yield* Projection.lag(bound)).caughtUp).toBe(false);
      yield* Projection.caughtUp(bound, fixed);
    }),
  );

  it.effect(
    "an empty open input is ready before provisioning, and a captured close must commit",
    () =>
      Effect.gen(function* () {
        const params = { id: "empty" };

        const bound = Projection.mapAccumBatch({
          id: "catch-up-empty",
          params: { id: Schema.String },
          inputs: { source: Source },
          outputs: { output: Output },
          commit: Commit.producer,
          state: { schema: Schema.Null, initial: null },
          mapAccumBatch: () => Effect.succeed([null, {}]),
        }).at(params);

        const source = yield* Source;
        yield* source.create(params);
        const open = yield* Projection.tails(bound);
        expect(open.source).toEqual({
          stream: bound.inputs.source.id,
          offset: ZERO_OFFSET,
          closed: false,
        });
        expect((yield* (yield* CheckpointStore).load(bound)).record).toEqual(Option.none());
        expect(yield* Projection.lag(bound, open)).toEqual({
          caughtUp: true,
          inputs: { source: { committed: ZERO_OFFSET, target: ZERO_OFFSET, behind: false } },
        });
        expect(yield* (yield* Output).head(params).pipe(Effect.flip)).toMatchObject({
          _tag: "StreamNotFound",
        });

        yield* source.append(params, [], { close: true });
        const closed = yield* Projection.tails(bound);
        expect(closed.source).toMatchObject({ offset: ZERO_OFFSET, closed: true });
        expect((yield* Projection.lag(bound, open)).caughtUp).toBe(true);
        expect(yield* Projection.lag(bound, closed)).toEqual({
          caughtUp: false,
          inputs: { source: { committed: ZERO_OFFSET, target: ZERO_OFFSET, behind: true } },
        });
        yield* Projection.pass(bound);
        expect((yield* Projection.lag(bound, closed)).caughtUp).toBe(true);
      }),
  );

  it.effect("producer output can be visible while its input is still pending", () =>
    Effect.gen(function* () {
      const params = { id: "producer" };

      const bound = Projection.mapAccumBatch({
        id: "catch-up-producer",
        params: { id: Schema.String },
        inputs: { source: Source },
        outputs: { output: Output },
        commit: Commit.producer,
        state: { schema: Schema.Null, initial: null },
        mapAccumBatch: (_state, batch) => Effect.succeed([null, { output: batch.source.items }]),
      }).at(params);

      const source = yield* Source;
      const output = yield* Output;
      yield* source.create(params);
      yield* output.create(params);
      const empty = yield* Projection.tails(bound);
      yield* source.append(params, ["one"]);
      const fixed = yield* Projection.tails(bound);
      const store = yield* CheckpointStore;
      const appended = yield* Deferred.make<void>();
      const settle = yield* Deferred.make<void>();

      const run = yield* Projection.pass(bound).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: (key, token, record) =>
            record.pending === undefined
              ? Deferred.succeed(appended, undefined).pipe(
                  Effect.andThen(Deferred.await(settle)),
                  Effect.andThen(store.compareAndSet(key, token, record)),
                )
              : store.compareAndSet(key, token, record),
        }),
        Effect.forkScoped,
      );

      yield* Deferred.await(appended);
      expect((yield* output.head(params)).nextOffset).not.toBe(ZERO_OFFSET);
      expect(yield* Projection.lag(bound, fixed)).toMatchObject({
        caughtUp: false,
        inputs: { source: { committed: ZERO_OFFSET, behind: true } },
      });
      expect((yield* Projection.lag(bound, empty)).caughtUp).toBe(false);

      yield* Deferred.succeed(settle, undefined);
      yield* Fiber.join(run);
      expect((yield* Projection.lag(bound, fixed)).caughtUp).toBe(true);
    }),
  );

  it.effect("reports missing, gone, mismatched and regressed inputs through typed faults", () =>
    Effect.gen(function* () {
      const params = { id: "faults" };
      const bound = declaration.at(params);
      const address = bound.inputs.source;
      const source = yield* Source;
      expect(yield* Effect.flip(Projection.tails(bound))).toMatchObject({
        ...historyUnavailable,
        cause: { _tag: "StreamNotFound" },
      });

      yield* source.create(params);
      yield* source.append(params, ["one"]);
      yield* Projection.pass(bound);
      const fixed = yield* Projection.tails(bound);
      const other = Source.at({ id: "other" });
      const wrongStream = { source: { ...fixed.source, stream: other.id } };

      for (const invalid of [{}, { ...fixed, extra: fixed.source }, wrongStream]) {
        expect(yield* Effect.flip(Projection.lag(bound, invalid))).toMatchObject({
          phase: "load",
          reason: "identity-mismatch",
        });
      }

      yield* Streams.create(other);
      const changed = { ...bound, inputs: { source: other } };
      expect(yield* Effect.flip(Projection.lag(changed))).toMatchObject({
        phase: "load",
        reason: "identity-mismatch",
      });

      yield* source.remove(params);
      expect(yield* Effect.flip(Projection.lag(bound, fixed))).toMatchObject({
        ...historyUnavailable,
        cause: { _tag: "StreamNotFound" },
      });
      yield* source.create(params);
      expect(yield* Effect.flip(Projection.lag(bound, fixed))).toMatchObject(historyUnavailable);
      yield* source.append(params, ["one"]);
      yield* (yield* StreamsWriter).fork(Source.at({ id: "child" }).id, address.id);
      yield* source.remove(params);
      expect(yield* Effect.flip(Projection.lag(bound, fixed))).toMatchObject({
        ...historyUnavailable,
        cause: { _tag: "StreamGone" },
      });
    }),
  );

  it.effect("checks ended, retired and malformed checkpoints through the bound load", () =>
    Effect.gen(function* () {
      const params = { id: "checkpoint-faults" };
      const bound = declaration.at(params);
      const source = yield* Source;

      yield* source.create(params);
      yield* source.append(params, ["one"]);
      yield* Projection.pass(bound);

      const fixed = yield* Projection.tails(bound);
      const store = yield* CheckpointStore;
      const key = Projection.key(bound);
      const loaded = yield* store.load(key);
      const { identity, inputs, adapters } = Option.getOrThrow(loaded.record);

      const token = yield* store.compareAndSet(key, loaded.token, {
        identity,
        inputs: { source: "bad offset" },
        adapters,
      });

      expect(yield* Effect.flip(Projection.lag(bound, fixed))).toMatchObject({
        phase: "load",
        reason: "invalid-record",
        input: "source",
      });

      yield* store.compareAndSet(key, token, { identity, inputs, adapters, ended: true });
      expect(yield* Effect.flip(Projection.lag(bound, fixed))).toMatchObject({
        phase: "load",
        reason: "history-unavailable",
      });
      yield* Projection.retire(declaration, params, { generation: bound.generation });
      expect(yield* Effect.flip(Projection.caughtUp(bound, fixed))).toMatchObject({
        phase: "load",
        reason: "retired-generation",
      });
    }),
  );

  it.effect("polls, keeps the implicit target fixed and is interruptible", () =>
    Effect.gen(function* () {
      const params = { id: "poll" };
      const bound = declaration.at(params);
      const source = yield* Source;
      yield* source.create(params);
      yield* source.append(params, ["one"]);
      const loaded = yield* Deferred.make<void>();

      const waiting = {
        ...bound,
        commit: {
          load: (key: Parameters<typeof bound.commit.load>[0]) =>
            bound.commit.load(key).pipe(Effect.tap(() => Deferred.succeed(loaded, undefined))),
        },
      };

      const waiter = yield* Projection.caughtUp(waiting).pipe(Effect.forkScoped);
      yield* Deferred.await(loaded);
      yield* source.append(params, ["two"]);
      yield* Projection.pass(bound);
      yield* TestClock.adjust("1 second");
      yield* Fiber.join(waiter);
      expect((yield* Projection.lag(bound)).caughtUp).toBe(false);

      const cancelled = yield* Projection.caughtUp(bound).pipe(Effect.forkScoped);
      yield* TestClock.adjust("1 second");
      expect(cancelled.pollUnsafe()).toBeUndefined();
      yield* Fiber.interrupt(cancelled);
      const exit = yield* Fiber.await(cancelled);
      expect(Exit.hasInterrupts(exit)).toBe(true);
    }),
  );
});
