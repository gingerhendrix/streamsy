import { canonicalParams } from "../src/unit.ts";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import {
  Array,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Order,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { Streamsy } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { Protocol } from "@streamsy/core/protocol";
import {
  Commit,
  NotAttached,
  Projection,
  ProjectionFault,
  ProjectionWakeup,
  RunnerEvent,
} from "@streamsy/projection";
import { TestClock } from "effect/testing";
import { memoryHost } from "./support/memory-host.ts";

class Source extends Streamsy.json<Source>()("RegistryEvents/Source", "source/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

const local = memoryHost({ readLimit: 1 });

const host = Protocol.bind(local, Streamsy.group(Source)).pipe(Layer.provideMerge(local));

const projection = (id: string, process: Effect.Effect<void, string> = Effect.void) =>
  Projection.process({
    id,
    params: { id: Schema.String },
    inputs: { source: Source },
    commit: Commit.atLeastOnce,
    process: () => process,
  });

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
  it.effect("isFor matches encoded params by value and tagged matchers are exported", () =>
    Effect.sync(() => {
      const binding = { id: "encoded", params: { tenant: "01", name: "ann" } };

      const event = RunnerEvent.Dropped({
        projection: "encoded",
        params: { name: "ann", tenant: "01" },
      });

      expect(Projection.Registry.isFor(binding)(event)).toBe(true);
      expect(Projection.Registry.isFor({ ...binding, id: "other" })(event)).toBe(false);
      expect(
        Projection.Registry.isFor({ ...binding, params: { tenant: "1", name: "ann" } })(event),
      ).toBe(false);
      expect(Projection.Registry.isFor({ ...binding, params: { tenant: "01" } })(event)).toBe(
        false,
      );
      expect(Projection.RunnerEvent.$is("Dropped")(event)).toBe(true);
      expect(
        RunnerEvent.$match(event, {
          Ran: () => "ran",
          Failed: () => "failed",
          Dropped: () => "dropped",
        }),
      ).toBe("dropped");
    }),
  );

  for (const durable of [false, true]) {
    it.effect(
      `Ran reports each bounded run's status, units and offsets (durable: ${durable})`,
      () =>
        Effect.gen(function* () {
          const params = { id: `ran-${durable}` };
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
              params,
              status: "limit-reached",
              units: 1,
              offsets: { source: first.offset },
            },
            {
              _tag: "Ran",
              projection: declaration.id,
              params,
              status: "limit-reached",
              units: 1,
              offsets: { source: second.offset },
            },
            {
              _tag: "Ran",
              projection: declaration.id,
              params,
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
      const key = canonicalParams(params);
      const source = yield* Source;
      yield* source.create(params);
      yield* source.append(params, ["one"]);
      const declaration = projection("durable-failed", Effect.fail("offline"));
      const registry = yield* Projection.Registry;
      const pull = yield* Stream.toPull(registry.events);
      const marks = yield* ProjectionMarks;
      yield* marks.fail(declaration.id, key, {
        attempts: 3,
        dueAt: Option.some(60_000),
        failure: { at: 1, reason: Option.none(), message: "failed" },
      });
      yield* Layer.build(Projection.attach(declaration, { durable: true }));
      yield* registry.retry(declaration);
      const event = (yield* pull)[0];
      const mark = (yield* marks.read(declaration.id, [key])).get(key)!;
      expect(event).toEqual({
        _tag: "Failed",
        projection: declaration.id,
        params,
        attempts: mark.attempts,
        dueAt: Option.some(mark.dueAt),
      });
      expect(mark).toMatchObject({ attempts: 4, dueAt: 8_000 });
    }),
  );

  it.effect("non-durable Failed publishes before the rest of its pass finishes", () =>
    Effect.gen(function* () {
      const source = yield* Source;

      for (const id of ["early-failure", "suspended-peer"]) {
        yield* source.create({ id });
        yield* source.append({ id }, ["one"]);
      }

      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      const declaration = Projection.process({
        id: "immediate-failure",
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: (_batch, unit) =>
          unit.params.id === "early-failure"
            ? Effect.fail("offline")
            : Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
      });

      const registry = yield* Projection.Registry;
      const pull = yield* Stream.toPull(registry.events);
      const wakeup = yield* ProjectionWakeup.attach(declaration, { durable: false });
      yield* registry.schedule(declaration.at({ id: "early-failure" }));
      yield* registry.schedule(declaration.at({ id: "suspended-peer" }));
      const pass = yield* Effect.exit(wakeup.run).pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      expect((yield* pull)[0]).toMatchObject({
        _tag: "Failed",
        attempts: 1,
        dueAt: Option.some(1_000),
      });
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(pass);
    }),
  );

  for (const exhausted of [false, true]) {
    it.effect(
      `non-durable Failed reports binding attempts and a binding deadline (exhausted: ${exhausted})`,
      () =>
        Effect.gen(function* () {
          yield* TestClock.setTime(0);
          const params = { id: `memory-failed-${exhausted}` };
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
            params,
            attempts: 1,
            dueAt: Option.some(1_000),
          });

          yield* TestClock.adjust(1_000);
          expect((yield* pull)[0]).toEqual({
            _tag: "Failed",
            projection: declaration.id,
            params,
            attempts: 2,
            dueAt: Option.some(3_000),
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
          yield* source.create(params);
          yield* source.append(params, ["one"]);
          yield* source.remove(params);
          const declaration = projection("dropped");
          const registry = yield* Projection.Registry;
          const pull = yield* Stream.toPull(registry.events);
          yield* Layer.build(Projection.attach(declaration, { durable }));
          yield* registry.schedule(declaration.at(params));
          expect(yield* pull).toEqual([{ _tag: "Dropped", projection: declaration.id, params }]);
          expect(yield* registry.retry(declaration)).toBe(0);
        }),
    );
  }

  it.effect("events combines two runners and a late subscriber sees only later runs", () =>
    Effect.gen(function* () {
      const params = { id: `two-runners` };
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
          params,
          status: "caught-up",
          units: 0,
          offsets: { source: receipt.offset },
        },
      ]);
    }),
  );

  for (const race of ["before-subscribe", "during-read", "after-read"] as const) {
    it.effect(`awaitOffset covers a completed pass ${race}`, () =>
      Effect.gen(function* () {
        const params = { id: `read-write-${race}` };
        const source = yield* Source;
        yield* source.create(params);
        const receipt = yield* source.append(params, ["one"]);
        const declaration = projection("read-write");
        const binding = declaration.at(params);
        const registry = yield* Projection.Registry;
        const wakeup = yield* ProjectionWakeup.attach(declaration);

        const run = registry
          .schedule(binding)
          .pipe(Effect.andThen(wakeup.run), Effect.orDie, Effect.asVoid);

        const readDone = yield* Deferred.make<void>();

        const readable = {
          ...binding,
          commit: {
            ...binding.commit,
            load: (key: Parameters<typeof binding.commit.load>[0]) =>
              binding.commit
                .load(key)
                .pipe(
                  Effect.tap(() =>
                    race === "during-read"
                      ? run
                      : Deferred.succeed(readDone, undefined).pipe(Effect.asVoid),
                  ),
                ),
          },
        };

        if (race === "before-subscribe") yield* run;

        const waiting = yield* registry
          .awaitOffset(readable, "source", receipt.offset)
          .pipe(Effect.forkScoped);

        if (race === "after-read") {
          yield* Deferred.await(readDone);
          expect(waiting.pollUnsafe()).toBeUndefined();
          yield* run;
        }

        yield* Fiber.join(waiting);
        // Equality and a later checkpoint both satisfy an already completed wait.
        yield* registry.awaitOffset(binding, "source", "-1");
      }),
    );
  }

  for (const durable of [false, true]) {
    it.effect(
      `awaitOffset rereads progress committed before a later unit fails (durable: ${durable})`,
      () =>
        Effect.gen(function* () {
          const declaration = Projection.process({
            id: "partial-progress",
            params: { id: Schema.String },
            inputs: { source: Source },
            commit: Commit.atLeastOnce,
            process: (batch) =>
              batch.source.items[0] === "two" ? Effect.fail("offline") : Effect.void,
          });

          const source = yield* Source;
          const params = { id: `partial-${durable}` };
          yield* source.create(params);
          const receipt = yield* source.append(params, ["one"]);
          yield* source.append(params, ["two"]);
          const registry = yield* Projection.Registry;
          const wakeup = yield* ProjectionWakeup.attach(declaration, { durable });
          const binding = declaration.at(params);
          const readDone = yield* Deferred.make<void>();

          const readable = {
            ...binding,
            commit: {
              ...binding.commit,
              load: (key: Parameters<typeof binding.commit.load>[0]) =>
                binding.commit
                  .load(key)
                  .pipe(Effect.tap(() => Deferred.succeed(readDone, undefined))),
            },
          };

          const waiting = yield* registry
            .awaitOffset(readable, "source", receipt.offset)
            .pipe(Effect.forkScoped);

          yield* Deferred.await(readDone);
          expect(waiting.pollUnsafe()).toBeUndefined();
          yield* registry.schedule(binding);
          const exit = yield* Effect.exit(wakeup.run);
          expect(Exit.isSuccess(exit)).toBe(true);
          yield* Fiber.join(waiting);
          expect((yield* Projection.read(binding)).offsets.source).toBe(receipt.offset);
        }),
    );
  }

  for (const objectHost of [false, true]) {
    for (const [attached, requested] of [
      [1, 2],
      [2, 1],
    ] as const) {
      for (const field of ["version", "generation"] as const) {
        const declare = (identity: number) =>
          Projection.process({
            id: "wait-identity",
            [field]: identity,
            params: { id: Schema.String },
            inputs: { source: Source },
            commit: Commit.atLeastOnce,
            process: () => Effect.void,
          });

        it.effect(
          `awaitOffset rejects ${field} ${requested} against attached ${attached} (object host: ${objectHost})`,
          () =>
            Effect.gen(function* () {
              const declaration = declare(attached);

              if (objectHost) yield* ProjectionWakeup.attach(declaration);
              else yield* Layer.build(Projection.attach(declaration));
              const binding = declare(requested).at({ id: "a" });
              let read = false;

              const readable = {
                ...binding,
                commit: {
                  ...binding.commit,
                  load: (key: Parameters<typeof binding.commit.load>[0]) =>
                    Effect.suspend(() => {
                      read = true;

                      return binding.commit.load(key);
                    }),
                },
              };

              const registry = yield* Projection.Registry;
              // Even an already-reached offset cannot hide a declaration mismatch.
              const fault = yield* Effect.flip(registry.awaitOffset(readable, "source", "-1"));
              expect(fault).toMatchObject({
                _tag: "ProjectionFault",
                phase: "load",
                reason: "identity-mismatch",
              });
              expect(read).toBe(false);
            }),
        );
      }
    }
  }

  it.effect("awaitOffset fails with NotAttached and preserves checkpoint read failures", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const declaration = projection("wait-errors");
      const binding = declaration.at({ id: "a" });
      expect(yield* Effect.flip(registry.awaitOffset(binding, "source", "-1"))).toEqual(
        new NotAttached({ projection: declaration.id }),
      );
      yield* ProjectionWakeup.attach(declaration);

      const fault = new ProjectionFault({
        phase: "load",
        reason: "storage-failure",
        message: "offline",
      });

      const broken = { ...binding, commit: { ...binding.commit, load: () => Effect.fail(fault) } };
      expect(yield* Effect.flip(registry.awaitOffset(broken, "source", "-1"))).toBe(fault);
    }),
  );

  it.effect("awaitOffset ignores other params and offsets below the target", () =>
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const secondUnit = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      const declaration = Projection.process({
        id: "wait-filter",
        params: { id: Schema.String },
        inputs: { source: Source },
        commit: Commit.atLeastOnce,
        process: (batch) =>
          batch.source.items[0] === "two"
            ? Deferred.succeed(secondUnit, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void,
      });

      const source = yield* Source;

      for (const id of ["a", "b"]) {
        yield* source.create({ id });
        yield* source.append({ id }, ["one"]);
      }

      const receipt = yield* source.append({ id: "a" }, ["two"]);
      const wakeup = yield* ProjectionWakeup.attach(declaration, { limit: 1 });
      const readDone = yield* Deferred.make<void>();
      const binding = declaration.at({ id: "a" });

      const readable = {
        ...binding,
        commit: {
          ...binding.commit,
          load: (key: Parameters<typeof binding.commit.load>[0]) =>
            binding.commit.load(key).pipe(Effect.tap(() => Deferred.succeed(readDone, undefined))),
        },
      };

      const waiting = yield* registry
        .awaitOffset(readable, "source", receipt.offset)
        .pipe(Effect.forkScoped);

      yield* Deferred.await(readDone);
      yield* registry.schedule(declaration.at({ id: "b" }));
      yield* wakeup.run;
      expect(waiting.pollUnsafe()).toBeUndefined();
      yield* registry.schedule(binding);
      yield* wakeup.run;
      expect(waiting.pollUnsafe()).toBeUndefined();
      const pass = yield* wakeup.run.pipe(Effect.forkScoped);
      yield* Deferred.await(secondUnit);
      // The first Ran for this binding is still below the target.
      expect(waiting.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(pass);
      yield* Fiber.join(waiting);
    }),
  );

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
      expect(Array.every(events, RunnerEvent.$is("Ran"))).toBe(true);
    }),
  );
});
