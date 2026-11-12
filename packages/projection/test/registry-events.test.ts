import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Effect, Layer, Option, Order, Schedule, Schema, Stream } from "effect";
import { compare, Streamsy } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Protocol } from "@streamsy/core/protocol";
import { Commit, Projection, ProjectionWakeup, type RunnerEvent } from "@streamsy/projection";
import { TestClock } from "effect/testing";
import { memoryHost } from "./support/memory-host.ts";

class Source extends Streamsy.json<Source>()("RegistryEvents/Source", "source/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

const local = memoryHost({ readLimit: 1 });

const host = Protocol.bind(local, Streamsy.group(Source)).pipe(Layer.provideMerge(local));

const paramsKey = Schema.encodeSync(Schema.fromJsonString(Schema.Struct({ id: Schema.String })));

const projection = (id: string, process: Effect.Effect<void, string> = Effect.void) =>
  Projection.process({
    id,
    params: { id: Schema.String },
    inputs: { source: Source },
    commit: Commit.atLeastOnce,
    process: () => process,
  });

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
  for (const durable of [false, true]) {
    it.effect(
      `Ran reports each bounded run's status, units and offsets (durable: ${durable})`,
      () =>
        Effect.gen(function* () {
          const params = { id: `ran-${durable}` };
          const key = paramsKey(params);
          const source = yield* Source;
          yield* source.create(params);
          const first = yield* source.append(params, ["one"]);
          const second = yield* source.append(params, ["two"]);
          const declaration = projection(`ran-${durable}`);
          const registry = yield* Projection.Registry;
          const pull = yield* Stream.toPull(registry.events);
          yield* Layer.build(Projection.attach(declaration, { durable, limit: 1 }));
          yield* registry.schedule(declaration.at(params));

          const events = yield* Stream.fromPull(Effect.succeed(pull)).pipe(
            Stream.take(3),
            Stream.runCollect,
          );

          expect(events).toEqual([
            {
              _tag: "Ran",
              projection: declaration.id,
              params: key,
              status: "limit-reached",
              units: 1,
              offsets: { source: first.offset },
            },
            {
              _tag: "Ran",
              projection: declaration.id,
              params: key,
              status: "limit-reached",
              units: 1,
              offsets: { source: second.offset },
            },
            {
              _tag: "Ran",
              projection: declaration.id,
              params: key,
              status: "caught-up",
              units: 0,
              offsets: { source: second.offset },
            },
          ]);
        }),
    );
  }

  it.effect("durable Failed uses the persisted mark's attempts and due time", () =>
    Effect.gen(function* () {
      const params = { id: `durable-failed` };
      const key = paramsKey(params);
      const source = yield* Source;
      yield* source.create(params);
      yield* source.append(params, ["one"]);
      const declaration = projection("durable-failed", Effect.fail("offline"));
      const registry = yield* Projection.Registry;
      const pull = yield* Stream.toPull(registry.events);
      const marks = yield* ProjectionMarks;
      yield* marks.defer(declaration.id, key, 3, 60_000);
      yield* Layer.build(Projection.attach(declaration, { durable: true }));
      yield* registry.retry(declaration);
      const event = (yield* pull)[0];
      const mark = (yield* marks.read(declaration.id, [key])).get(key)!;
      expect(event).toEqual({
        _tag: "Failed",
        projection: declaration.id,
        params: key,
        attempts: mark.attempts,
        dueAt: Option.some(mark.dueAt),
      });
      expect(mark).toMatchObject({ attempts: 4, dueAt: 8_000 });
    }),
  );

  for (const exhausted of [false, true]) {
    it.effect(
      `non-durable Failed reports binding attempts and ${exhausted ? "no exhausted deadline" : "the pass deadline"}`,
      () =>
        Effect.gen(function* () {
          const params = { id: `memory-failed-${exhausted}` };
          const key = paramsKey(params);
          const source = yield* Source;
          yield* source.create(params);
          yield* source.append(params, ["one"]);
          const declaration = projection("memory-failed", Effect.fail("offline"));
          const registry = yield* Projection.Registry;
          const pull = yield* Stream.toPull(registry.events);
          yield* Layer.build(
            Projection.attach(declaration, {
              retry: exhausted ? Schedule.recurs(0) : Schedule.spaced("1 second"),
            }),
          );
          yield* registry.schedule(declaration.at(params));
          expect((yield* pull)[0]).toEqual({
            _tag: "Failed",
            projection: declaration.id,
            params: key,
            attempts: 1,
            dueAt: exhausted ? Option.none() : Option.some(1_000),
          });

          if (exhausted) yield* registry.retry(declaration);
          else yield* TestClock.adjust(1_000);
          expect((yield* pull)[0]).toEqual({
            _tag: "Failed",
            projection: declaration.id,
            params: key,
            attempts: 2,
            dueAt: exhausted ? Option.none() : Option.some(2_000),
          });
        }),
    );
  }

  for (const durable of [false, true]) {
    it.effect(
      `Dropped replaces Failed when the source went before its first run (durable: ${durable})`,
      () =>
        Effect.gen(function* () {
          const source = yield* Source;
          const params = { id: `removed-${durable}` };
          const key = paramsKey(params);
          yield* source.create(params);
          yield* source.append(params, ["one"]);
          yield* source.remove(params);
          const declaration = projection("dropped");
          const registry = yield* Projection.Registry;
          const pull = yield* Stream.toPull(registry.events);
          yield* Layer.build(Projection.attach(declaration, { durable }));
          yield* registry.schedule(declaration.at(params));
          expect(yield* pull).toEqual([
            { _tag: "Dropped", projection: declaration.id, params: key },
          ]);
          expect(yield* registry.retry(declaration)).toBe(0);
        }),
    );
  }

  it.effect("events combines two runners and a late subscriber sees only later runs", () =>
    Effect.gen(function* () {
      const params = { id: `two-runners` };
      const key = paramsKey(params);
      const source = yield* Source;
      yield* source.create(params);
      const receipt = yield* source.append(params, ["one"]);
      const one = projection("one");
      const two = projection("two");
      const registry = yield* Projection.Registry;
      const pull = yield* Stream.toPull(registry.events);
      yield* Layer.build(
        Layer.mergeAll(Projection.attach(one), Projection.attach(two, { durable: true })),
      );
      yield* registry.schedule(one.at(params));
      yield* registry.schedule(two.at(params));

      const early = yield* Stream.fromPull(Effect.succeed(pull)).pipe(
        Stream.take(2),
        Stream.runCollect,
      );

      expect(
        Array.sort(
          Array.map(early, (event) => event.projection),
          Order.String,
        ),
      ).toEqual(["one", "two"]);

      for (const event of early)
        expect(event).toMatchObject({ _tag: "Ran", units: 1, offsets: { source: receipt.offset } });
      const late = yield* Stream.toPull(registry.events);
      yield* registry.schedule(one.at(params));
      expect(yield* late).toEqual([
        {
          _tag: "Ran",
          projection: "one",
          params: key,
          status: "caught-up",
          units: 0,
          offsets: { source: receipt.offset },
        },
      ]);
    }),
  );

  for (const race of ["before-check", "after-check"] as const) {
    it.effect(`read-your-writes subscribes before checking the checkpoint (${race})`, () =>
      Effect.gen(function* () {
        const params = { id: `read-write-${race}` };
        const key = paramsKey(params);
        const source = yield* Source;
        yield* source.create(params);
        const receipt = yield* source.append(params, ["one"]);
        const declaration = projection("read-write");
        const binding = declaration.at(params);
        const registry = yield* Projection.Registry;
        const wakeup = yield* ProjectionWakeup.attach(declaration);
        // toPull acquires the subscription now, before reading the checkpoint.
        const pull = yield* Stream.toPull(registry.events);

        if (race === "before-check") {
          yield* registry.schedule(binding);
          yield* wakeup.run;
        }

        const snapshot = yield* Projection.read(binding);
        const caughtUp = (offset: string) => compare(offset, receipt.offset) >= 0;

        if (caughtUp(snapshot.offsets.source)) {
          expect(race).toBe("before-check");

          return;
        }

        // Complete between the checkpoint read and consuming the subscribed stream.
        yield* registry.schedule(binding);
        yield* wakeup.run;

        const observed = yield* Stream.fromPull(Effect.succeed(pull)).pipe(
          Stream.filter(
            (event) =>
              event._tag === "Ran" &&
              event.projection === binding.id &&
              event.params === key &&
              event.offsets.source !== undefined &&
              caughtUp(event.offsets.source),
          ),
          Stream.runHead,
        );

        expect(Option.isSome(observed)).toBe(true);
      }),
    );
  }

  it.effect("a slow subscriber buffers events without blocking a pass", () =>
    Effect.gen(function* () {
      const params = { id: "slow" };
      const source = yield* Source;
      yield* source.create(params);
      yield* source.append(params, ["one"]);
      const declaration = projection("slow");
      const wakeup = yield* ProjectionWakeup.attach(declaration);
      const registry = yield* Projection.Registry;
      const pull = yield* Stream.toPull(registry.events);

      for (let n = 0; n < 100; n += 1) {
        yield* registry.schedule(declaration.at(params));
        yield* wakeup.run;
      }

      const events: ReadonlyArray<RunnerEvent> = yield* Stream.fromPull(Effect.succeed(pull)).pipe(
        Stream.take(100),
        Stream.runCollect,
      );

      expect(events).toHaveLength(100);
      expect(Array.every(events, (event) => event._tag === "Ran")).toBe(true);
    }),
  );
});
