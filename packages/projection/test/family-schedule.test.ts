import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Scope } from "effect";
import {
  CommitBoundary,
  DurableState,
  Offset,
  Storage,
  StorageFault,
  Streamsy,
  type StreamId,
  ZERO_OFFSET,
} from "@streamsy/core";
import { OwnerRecords, ProjectionMarks } from "@streamsy/core/internal";
import { SqlError } from "effect/sql";
import { Protocol } from "@streamsy/core/protocol";
import {
  Commit,
  InvalidBinding,
  NotAttached,
  Projection,
  ProjectionWakeup,
} from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";
import { eventually } from "./support/eventually.ts";
import { canonicalParams } from "../src/unit.ts";
import { triggersOf } from "../src/triggers.ts";

class Source extends Streamsy.json<Source>()("Family/Source", "family/:id", {
  params: { id: Schema.FiniteFromString },
  schema: Schema.String,
}) {}

class Wake extends Streamsy.wake<Wake>()("Family/Wake", "family-wake", { params: {} }) {}

const local = memoryHost();

const host = Projection.Registry.layer.pipe(
  Layer.provideMerge(
    Protocol.bind(local, Streamsy.group(Source, Wake)).pipe(Layer.provideMerge(local)),
  ),
);

const declaration = (
  id: string,
  seen: Array<number>,
  generation = 1,
  work: Effect.Effect<void> = Effect.void,
) =>
  Projection.process({
    id,
    generation,
    params: { id: Schema.FiniteFromString },
    inputs: { source: Source },
    commit: Commit.atLeastOnce,
    process: (_batch, unit) =>
      Effect.sync(() => {
        seen.push(unit.params.id);
      }).pipe(Effect.andThen(work), Effect.as(Projection.wakeAt(60_000))),
  });

const populate = (count: number) =>
  Effect.gen(function* () {
    const source = yield* Source;

    for (let id = 0; id < count; id++) {
      yield* source.create({ id });
      yield* source.append({ id }, ["one"]);
    }
  });

const populateWake = (count: number) =>
  Effect.gen(function* () {
    const wake = yield* Wake;
    yield* wake.create({});

    return yield* wake.append(
      {},
      DurableState.changes(
        Wake.address({}),
        {},
        Array.makeBy(count, (id) =>
          DurableState.upsert("stream", {
            stream: Source.at({ id }).id,
            offset: Offset.make(ZERO_OFFSET),
          }),
        ),
      ),
    );
  });

const settled = (count: number) =>
  Effect.flatMap(Projection.Registry, (registry) =>
    eventually(
      registry.status,
      (statuses) => statuses[0]?.marks.sleeping === count && statuses[0]?.active === 0,
    ),
  );

const match = (id: StreamId) => Option.some([id, id] as const);

const it = {
  effect: (
    name: string,
    run: () => Effect.Effect<void, unknown, Layer.Success<typeof host> | Scope.Scope>,
  ) => layer(host)((test) => test.effect(name, run)),
};

{
  for (const durable of [false, true]) {
    it.effect(`schedule 1,000 bindings once, including a sleeping member (${durable})`, () =>
      Effect.gen(function* () {
        yield* populate(1_000);
        const seen: Array<number> = [];
        const projection = declaration("thousand", seen);
        yield* Layer.build(Projection.attach(projection, { durable, batch: 1_000 }));
        const registry = yield* Projection.Registry;
        const bindings = Array.makeBy(1_000, (id) => projection.at({ id }));
        yield* registry.schedule(bindings[0]);
        expect((yield* settled(1))[0]?.marks.sleeping).toBe(1);
        // The family call wakes a real sleeping mark without rerunning its handler.
        yield* registry.schedule([bindings[0], ...bindings]);
        expect((yield* settled(1_000))[0]?.marks.sleeping).toBe(1_000);
        expect(seen).toHaveLength(1_000);
        expect(new Set(seen).size).toBe(1_000);
        yield* registry.schedule(bindings);
        expect((yield* settled(1_000))[0]?.marks.sleeping).toBe(1_000);
        expect(seen).toHaveLength(1_000);
      }),
    );

    it.effect(`rescan 500 members after a fold fix (${durable})`, () =>
      Effect.gen(function* () {
        yield* populate(500);
        yield* populateWake(500);
        const seen: Array<number> = [];
        const initial = declaration("five-hundred", seen);
        const scope = yield* Scope.fork(yield* Scope.Scope);
        yield* Layer.build(
          Projection.attach(initial, { durable, triggers: [Wake], batch: 500 }),
        ).pipe(Scope.provide(scope));
        expect((yield* settled(500))[0]?.marks.sleeping).toBe(500);
        expect(seen).toHaveLength(500); // First attach reads from zero without rescan.
        yield* Scope.close(scope, Exit.void);
        seen.length = 0;
        const fixed = declaration(initial.id, seen, 2);
        yield* Layer.build(Projection.attach(fixed, { durable, triggers: [Wake], batch: 500 }));
        const registry = yield* Projection.Registry;
        yield* registry.rescan(fixed, Wake);
        expect((yield* settled(500))[0]?.marks.sleeping).toBe(500);
        expect(
          yield* eventually(
            Effect.sync(() => new Set(seen).size),
            (size) => size === 500,
          ),
        ).toBe(500);
      }),
    );
  }

  it.effect("durable rescan survives stopping before a pass and process-host restart", () =>
    Effect.gen(function* () {
      yield* populate(500);
      yield* populateWake(500);
      const seen: Array<number> = [];
      const entered = yield* Deferred.make<void>();
      let paused = false;

      const projection = declaration(
        "restart-rescan",
        seen,
        1,
        Effect.suspend(() =>
          paused
            ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
            : Effect.void,
        ),
      );

      const scope = yield* Scope.fork(yield* Scope.Scope);

      yield* Layer.build(
        Projection.attach(projection, {
          durable: true,
          triggers: [Wake],
          batch: 500,
        }),
      ).pipe(Scope.provide(scope));
      expect((yield* settled(500))[0]?.marks.sleeping).toBe(500);
      // Hold the current pass in a binding so the reset cannot be ingested before shutdown.
      paused = true;
      yield* (yield* Source).append({ id: 0 }, ["hold this pass"]);
      yield* Deferred.await(entered);
      const registry = yield* Projection.Registry;
      yield* registry.rescan(projection.id, Wake.address({}).id);
      yield* Scope.close(scope, Exit.void);
      seen.length = 0;
      const fixed = declaration(projection.id, seen, 2);
      yield* Layer.build(Projection.attach(fixed, { durable: true, triggers: [Wake], batch: 500 }));
      expect((yield* settled(500))[0]?.marks.sleeping).toBe(500);
      expect(
        yield* eventually(
          Effect.sync(() => new Set(seen).size),
          (size) => size === 500,
        ),
      ).toBe(500);
    }),
  );

  it.effect(
    "schedule validates the whole array, coalesces keys and rejects mixed ids before writing",
    () =>
      Effect.gen(function* () {
        const registry = yield* Projection.Registry;
        yield* registry.schedule([]); // No attached runner required.
        yield* populate(2);
        const seen: Array<number> = [];
        const projection = declaration("validate-array", seen);
        const runner = yield* ProjectionWakeup.attach(projection);
        const marks = yield* ProjectionMarks;
        const valid = projection.at({ id: 1 });
        const invalid = { id: projection.id, params: { id: "1.0" } };
        expect(
          yield* Effect.flip(registry.schedule([valid, invalid, projection.at({ id: 2 })])),
        ).toEqual(new InvalidBinding({ projection: projection.id, params: invalid.params }));
        expect((yield* marks.summary(projection.id, 0)).fresh).toBe(0);
        const mixed = yield* Effect.exit(registry.schedule([valid, { id: "other", params: {} }]));
        expect(Exit.isFailure(mixed) && String(mixed.cause)).toContain("one projection id");
        expect((yield* marks.summary(projection.id, 0)).fresh).toBe(0);
        yield* registry.schedule([valid, valid]);
        expect(
          (yield* marks.read(projection.id, [canonicalParams(valid.params)])).get(
            canonicalParams(valid.params),
          )?.seq,
        ).toBe(1);
        yield* runner.run;
        expect(seen).toEqual([1]);
        const unknown = yield* Effect.exit(registry.rescan(projection, Wake));
        expect(Exit.isFailure(unknown) && String(unknown.cause)).toContain(
          `Projection ${projection.id} has no trigger ${Wake.address({}).id}`,
        );
        expect(yield* Effect.flip(registry.rescan("missing", Wake))).toEqual(
          new NotAttached({ projection: "missing" }),
        );
      }),
  );

  it.effect(
    "rescan waits for an in-flight read, then rereads every row without losing its cursor",
    () =>
      Effect.gen(function* () {
        yield* populateWake(500);
        const marks = yield* ProjectionMarks;
        const storage = yield* Storage;
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let gated = true;

        const triggers = yield* triggersOf(
          "concurrent-rescan",
          [Wake],
          (id) => Option.some([id, id] as const),
          marks,
        ).pipe(
          Effect.provideService(Storage, {
            ...storage,
            messages: (id, options) =>
              storage.messages(id, options).pipe(
                Effect.tap(() =>
                  Effect.suspend(() => {
                    if (!gated) return Effect.void;
                    gated = false;

                    return Deferred.succeed(entered, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                    );
                  }),
                ),
              ),
          }),
        );

        const pass = yield* triggers.read.pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        const rescan = yield* triggers.rescan(Wake.address({}).id).pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        expect(rescan.pollUnsafe()).toBeUndefined();
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(pass);
        yield* Fiber.join(rescan);
        expect(yield* triggers.next).toBe(true);
        yield* triggers.read;
        const rows = yield* marks.due("concurrent-rescan", 0, 500);
        expect(rows).toHaveLength(500);
        expect(Array.every(rows, (row) => row.seq === 2)).toBe(true);
        expect(yield* triggers.next).toBe(false);
        const records = yield* OwnerRecords;
        let saved = "";

        const restarted = yield* triggersOf(
          "concurrent-rescan",
          [Wake],
          (id) => Option.some([id, id] as const),
          marks,
        ).pipe(
          Effect.provideService(OwnerRecords, {
            ...records,
            read: (key) =>
              records.read(key).pipe(
                Effect.tap((value) =>
                  Effect.sync(() => {
                    if (Option.isSome(value)) saved = value.value;
                  }),
                ),
              ),
          }),
        );

        expect(yield* restarted.next).toBe(false);
        const tail = yield* storage.record(Wake.address({}).id);
        expect(Option.isSome(tail) && saved).toBe(
          Option.isSome(tail) ? tail.value.currentOffset : undefined,
        );
      }),
  );
  it.effect(
    "schedule writes chunks of 100 and requests the alarm host once; later faults retain earlier marks",
    () =>
      Effect.gen(function* () {
        const projection = declaration("chunked", []);
        const marks = yield* ProjectionMarks;
        const sizes: Array<number> = [];
        let broken = true;

        const runner = yield* ProjectionWakeup.attach(projection).pipe(
          Effect.provideService(ProjectionMarks, {
            ...marks,
            mark: (id, keys) =>
              Effect.suspend(() => {
                sizes.push(keys.length);

                return broken && sizes.length === 2
                  ? Effect.fail(
                      new SqlError.SqlError({
                        reason: new SqlError.ConnectionError({
                          cause: undefined,
                          message: "second chunk",
                        }),
                      }),
                    )
                  : marks.mark(id, keys);
              }),
          }),
        );

        let requests = 0;
        yield* runner.bind({
          retry: Effect.sync(() => {
            requests++;
          }),
          reconcile: Effect.void,
          state: Effect.succeed({ failures: 0, retryAt: Option.none() }),
        });
        const registry = yield* Projection.Registry;
        const bindings = Array.makeBy(201, (id) => projection.at({ id }));
        expect((yield* Effect.flip(registry.schedule(bindings))).message).toBe("second chunk");
        expect(sizes).toEqual([100, 100]);
        expect((yield* marks.summary(projection.id, 0)).fresh).toBe(100);
        expect((yield* registry.status)[0]?.queued).toBe(0);
        expect(requests).toBe(0);
        broken = false;
        sizes.length = 0;
        yield* registry.schedule(bindings);
        expect(sizes).toEqual([100, 100, 1]);
        expect((yield* marks.summary(projection.id, 0)).fresh).toBe(201);
        expect(requests).toBe(1);
        yield* registry.schedule([]);
        expect(requests).toBe(1);
      }),
  );

  it.effect("rescan clears a stopped trigger, and a still-invalid row stops it again", () =>
    Effect.gen(function* () {
      yield* populate(1);
      yield* populateWake(1);
      const seen: Array<number> = [];
      const projection = declaration("stopped-rescan", seen);
      const storage = yield* Storage;
      let broken = true;

      const runner = yield* ProjectionWakeup.attach(projection, { triggers: [Wake] }).pipe(
        Effect.provideService(Storage, {
          ...storage,
          messages: (id, options) =>
            storage.messages(id, options).pipe(
              Effect.map((messages) =>
                broken && id === Wake.address({}).id
                  ? Array.map(messages, (message) => ({
                      offset: message.offset,
                      timestamp: message.timestamp,
                      data: new TextEncoder().encode("{}"),
                    }))
                  : messages,
              ),
            ),
        }),
      );

      const registry = yield* Projection.Registry;
      yield* runner.run;
      expect(Option.isSome((yield* registry.status)[0]!.triggers[0]!.failure)).toBe(true);
      yield* registry.rescan(projection, Wake);
      expect((yield* registry.status)[0]!.triggers[0]!.failure).toEqual(Option.none());
      yield* runner.run;
      expect(Option.isSome((yield* registry.status)[0]!.triggers[0]!.failure)).toBe(true);
      broken = false;
      yield* registry.rescan(projection.id, Wake.address({}).id);
      yield* runner.run;
      expect(seen).toEqual([0]);
      expect((yield* registry.status)[0]!.triggers[0]!.failure).toEqual(Option.none());
      const unknown = yield* Effect.exit(registry.rescan(projection, "absent-wake"));
      expect(Exit.isFailure(unknown) && String(unknown.cause)).toContain(
        `Projection ${projection.id} has no trigger absent-wake`,
      );
    }),
  );

  it.effect("a failed rescan rolls back its cursor and drops its cached position", () =>
    Effect.gen(function* () {
      yield* populateWake(1);
      const marks = yield* ProjectionMarks;
      const records = yield* OwnerRecords;
      const boundary = yield* CommitBoundary;
      let fail = false;
      let reads = 0;

      const triggers = yield* triggersOf(
        "rollback-rescan",
        [Wake],
        (id) => Option.some([id, id] as const),
        marks,
      ).pipe(
        Effect.provideService(OwnerRecords, {
          ...records,
          read: (key) =>
            records.read(key).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  reads++;
                }),
              ),
            ),
        }),
        Effect.provideService(CommitBoundary, {
          withTransaction: (body) =>
            boundary.withTransaction(
              body.pipe(
                Effect.tap(() =>
                  Effect.suspend(() =>
                    fail
                      ? Effect.fail(
                          new StorageFault({
                            operation: "test.rescan",
                            retryable: true,
                            message: "rollback",
                          }),
                        )
                      : Effect.void,
                  ),
                ),
              ),
            ),
        }),
      );

      yield* triggers.read;
      expect(yield* triggers.next).toBe(false);
      expect(reads).toBe(1);
      fail = true;
      expect((yield* Effect.flip(triggers.rescan(Wake.address({}).id))).message).toBe("rollback");
      fail = false;
      expect(yield* triggers.next).toBe(false);
      expect(reads).toBe(2);
      yield* triggers.rescan(Wake.address({}).id);
      expect(yield* triggers.next).toBe(true);
    }),
  );
  it.effect("a deadline cursor load cannot overwrite a concurrent rescan", () =>
    Effect.gen(function* () {
      yield* populateWake(1);
      const marks = yield* ProjectionMarks;
      const initial = yield* triggersOf("deadline-rescan", [Wake], match, marks);
      yield* initial.read;
      const records = yield* OwnerRecords;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      const triggers = yield* triggersOf("deadline-rescan", [Wake], match, marks).pipe(
        Effect.provideService(OwnerRecords, {
          ...records,
          read: (key) =>
            records
              .read(key)
              .pipe(
                Effect.tap(() =>
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                  ),
                ),
              ),
        }),
      );

      const check = yield* triggers.next.pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const reset = yield* triggers.rescan(Wake.address({}).id).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      expect(reset.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(check)).toBe(false);
      yield* Fiber.join(reset);
      expect(yield* triggers.next).toBe(true);
      yield* triggers.read;
      expect(yield* triggers.next).toBe(false);
    }),
  );
}
