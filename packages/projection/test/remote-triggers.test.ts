import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Option, Schedule, Schema, Scope } from "effect";
import { TestClock } from "effect/testing";
import {
  CommitBoundary,
  InvalidReadRequest,
  StreamGone,
  DurableState,
  MemoryStorage,
  Offset,
  Storage,
  StorageFault,
  StreamId,
  Streamsy,
  TransportFault,
  ZERO_OFFSET,
} from "@streamsy/core";
import { OwnerRecords, ProjectionMarks } from "@streamsy/core/internal";
import { Protocol } from "@streamsy/core/protocol";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";
import { triggersOf } from "../src/triggers.ts";
import { encodeKey } from "../src/unit.ts";
import { memoryHost } from "./support/memory-host.ts";
import { eventually } from "./support/eventually.ts";

class Source extends Streamsy.json<Source>()("RemoteTriggers/Source", "source/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

class Wake extends Streamsy.wake<Wake>()("RemoteTriggers/Wake", "wake", { params: {} }) {}

class OtherWake extends Streamsy.wake<OtherWake>()("RemoteTriggers/OtherWake", "other-wake", {
  params: {},
}) {}

class LocalWake extends Streamsy.wake<LocalWake>()("RemoteTriggers/LocalWake", "local-wake", {
  params: {},
}) {}

const remote = MemoryStorage.layerProtocol({ readLimit: 1 });

const host = (local: Layer.Layer<Layer.Success<ReturnType<typeof memoryHost>>, StorageFault>) =>
  Projection.Registry.layer.pipe(
    Layer.provideMerge(
      Layer.merge(
        Protocol.bind(local, Streamsy.group(Source, LocalWake)),
        Protocol.bind(remote, Streamsy.group(Wake, OtherWake)),
      ).pipe(Layer.provideMerge(local)),
    ),
  );

const source = (id: string) =>
  Effect.gen(function* () {
    const service = yield* Source;
    yield* service.create({ id });
    yield* service.append({ id }, [id]);
  });

const request = (id: string, wake: typeof Wake | typeof OtherWake = Wake) =>
  Effect.gen(function* () {
    const service = yield* Effect.service<Wake | OtherWake, Streamsy.Api<{}, Streamsy.WakeEvent>>(
      wake,
    );

    return yield* service.append(
      {},
      DurableState.changes(wake.address({}), {}, [
        DurableState.upsert("stream", {
          stream: Source.at({ id }).id,
          offset: Offset.make(ZERO_OFFSET),
        }),
      ]),
    );
  });

const declaration = (id: string, seen: Array<string>, generation = 1) =>
  Projection.process({
    id,
    generation,
    params: { id: Schema.String },
    inputs: { source: Source },
    commit: Commit.atLeastOnce,
    process: (_batch, unit) =>
      Effect.sync(() => {
        seen.push(unit.params.id);
      }),
  });

const match = (id: StreamId) => Option.some([id, id] as const);

const cursorKey = (projection: string, generation = 1) =>
  encodeKey([
    "streamsy.projection.trigger.v1",
    projection,
    String(generation),
    Wake.address({}).id,
  ]);

const offline = new TransportFault({
  operation: "test.remote",
  reason: "request",
  message: "owner B offline",
});

for (const [label, local] of [
  ["memory", memoryHost()],
  [
    "sqlite",
    CheckpointStore.layer.pipe(
      Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
    ),
  ],
] as const) {
  const testHost = host(local);

  const it = {
    effect: (
      name: string,
      run: () => Effect.Effect<void, unknown, Layer.Success<typeof testHost> | Scope.Scope>,
    ) => layer(testHost)((test) => test.effect(name, run)),
  };

  it.effect(`${label}: any unwrapped owner service keeps a wrapped local trigger local`, () =>
    Effect.gen(function* () {
      const storage = yield* Storage;
      const records = yield* OwnerRecords;
      const boundary = yield* CommitBoundary;
      const marks = yield* ProjectionMarks;

      for (const keep of ["storage", "records", "boundary"] as const) {
        const triggers = yield* triggersOf("wrapped", 1, [LocalWake], match, marks, "wakeup").pipe(
          Effect.provideService(Storage, keep === "storage" ? storage : { ...storage }),
          Effect.provideService(OwnerRecords, keep === "records" ? records : { ...records }),
          Effect.provideService(CommitBoundary, keep === "boundary" ? boundary : { ...boundary }),
        );

        expect(triggers.status()).toEqual([
          { stream: LocalWake.address({}).id, failure: Option.none() },
        ]);
        expect(yield* triggers.next).toBe(false);
      }
    }),
  );

  for (const durable of [false, true]) {
    it.effect(
      `${label}: follower resumes within a generation and replays for a new one (${durable})`,
      () =>
        Effect.gen(function* () {
          yield* source("first");
          yield* source("second");
          const wake = yield* Wake;
          yield* wake.create({});
          const seen: Array<string> = [];
          const projection = declaration(`restart-${durable}`, seen);
          const parked = yield* Deferred.make<void>();
          const ended = yield* Deferred.make<void>();
          let polls = 0;
          const firstScope = yield* Scope.fork(yield* Scope.Scope);
          yield* Layer.build(Projection.attach(projection, { durable, triggers: [Wake] })).pipe(
            Scope.provide(firstScope),
            Effect.provideService(Wake, {
              ...wake,
              wire: {
                ...wake.wire,
                readNext: (id, options) =>
                  Effect.sync(() => {
                    polls++;
                  }).pipe(
                    Effect.andThen(Deferred.succeed(parked, undefined)),
                    Effect.andThen(wake.wire.readNext(id, options)),
                    Effect.onInterrupt(() => Deferred.succeed(ended, undefined)),
                  ),
              },
            }),
          );
          yield* Deferred.await(parked);
          const appended = yield* request("first");
          expect(
            yield* eventually(
              Effect.sync(() => seen.length),
              (n) => n === 1,
            ),
          ).toBe(1);
          const records = yield* OwnerRecords;
          expect(yield* records.read(cursorKey(projection.id))).toEqual(
            Option.some(appended.offset),
          );
          yield* Scope.close(firstScope, Exit.void);
          yield* Deferred.await(ended);
          const stoppedPolls = polls;
          const appendedLater = yield* request("second");
          const secondScope = yield* Scope.fork(yield* Scope.Scope);
          const reads: Array<string | undefined> = [];
          yield* Layer.build(Projection.attach(projection, { durable, triggers: [Wake] })).pipe(
            Scope.provide(secondScope),
            Effect.provideService(Wake, {
              ...wake,
              wire: {
                ...wake.wire,
                read: (id, options) => {
                  reads.push(options?.offset);

                  return wake.wire.read(id, options);
                },
              },
            }),
          );
          expect(
            yield* eventually(
              Effect.sync(() => seen.length),
              (n) => n === 2,
            ),
          ).toBe(2);
          expect(seen).toEqual(["first", "second"]);
          expect(reads[0]).toBe(appended.offset);
          expect(yield* records.read(cursorKey(projection.id))).toEqual(
            Option.some(appendedLater.offset),
          );
          expect(polls).toBe(stoppedPolls);
          yield* Scope.close(secondScope, Exit.void);

          const replayed: Array<string> = [];
          const next = declaration(projection.id, replayed, 2);
          yield* Layer.build(Projection.attach(next, { durable, triggers: [Wake] }));
          expect(
            yield* eventually(
              Effect.sync(() => replayed.length),
              (n) => n === 2,
            ),
          ).toBe(2);
          expect(replayed).toEqual(["first", "second"]);
          expect(yield* records.read(cursorKey(next.id, next.generation))).toEqual(
            Option.some(appendedLater.offset),
          );
          expect(yield* records.read(cursorKey(projection.id))).toEqual(
            Option.some(appendedLater.offset),
          );
        }),
    );
  }

  it.effect(`${label}: process attachment drains all remote pages into bindings`, () =>
    Effect.gen(function* () {
      yield* (yield* Wake).create({});

      for (const id of ["one", "two", "three"]) {
        yield* source(id);
        yield* request(id);
      }

      const seen: Array<string> = [];
      yield* Layer.build(
        Projection.attach(declaration("all-pages", seen), { durable: true, triggers: [Wake] }),
      );
      expect(
        yield* eventually(
          Effect.sync(() => seen.length),
          (n) => n === 3,
        ),
      ).toBe(3);
      expect(seen).toEqual(["one", "two", "three"]);
    }),
  );

  it.effect(`${label}: rescan waits for an in-flight remote page and persists zero`, () =>
    Effect.gen(function* () {
      const wake = yield* Wake;
      yield* wake.create({});
      yield* request("one");
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      const triggers = yield* triggersOf("race", 1, [Wake], match, yield* ProjectionMarks).pipe(
        Effect.provideService(Wake, {
          ...wake,
          wire: {
            ...wake.wire,
            read: (id, options) =>
              wake.wire
                .read(id, options)
                .pipe(
                  Effect.tap(() =>
                    Deferred.succeed(entered, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                    ),
                  ),
                ),
          },
        }),
      );

      const read = yield* triggers.readRemote.pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const reset = yield* triggers.rescan(Wake.address({}).id).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      expect(reset.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(read);
      yield* Fiber.join(reset);
      expect(yield* (yield* OwnerRecords).read(cursorKey("race"))).toEqual(
        Option.some(ZERO_OFFSET),
      );
      expect(yield* triggers.next).toBe(true);
    }),
  );

  it.effect(
    `${label}: remote pages use page cursors outside the local transaction; next reads no network`,
    () =>
      Effect.gen(function* () {
        const wake = yield* Wake;
        yield* wake.create({});
        const offsets: Array<string> = [];

        for (const id of ["one", "two", "three"]) offsets.push((yield* request(id)).offset);
        const marks = yield* ProjectionMarks;
        const boundary = yield* CommitBoundary;
        let inTransaction = false;
        let reads = 0;

        const triggers = yield* triggersOf("pages", 1, [Wake], match, marks).pipe(
          Effect.provideService(Wake, {
            ...wake,
            wire: {
              ...wake.wire,
              read: (id, options) => {
                expect(inTransaction).toBe(false);
                reads++;

                return wake.wire.read(id, options);
              },
            },
          }),
          Effect.provideService(CommitBoundary, {
            withTransaction: (body) =>
              boundary.withTransaction(
                Effect.sync(() => {
                  inTransaction = true;
                }).pipe(
                  Effect.andThen(body),
                  Effect.ensuring(
                    Effect.sync(() => {
                      inTransaction = false;
                    }),
                  ),
                ),
              ),
          }),
        );

        expect(yield* triggers.next).toBe(true);
        expect(reads).toBe(0);
        const records = yield* OwnerRecords;

        for (let page = 0; page < 3; page++) {
          yield* triggers.readRemote;
          expect((yield* marks.summary("pages", 0)).fresh).toBe(page + 1);
          expect(yield* records.read(cursorKey("pages"))).toEqual(Option.some(offsets[page]));
          expect(yield* triggers.next).toBe(page < 2);
        }

        expect(reads).toBe(3);
      }),
  );

  it.effect(`${label}: a failed cursor commit rolls back remote marks and reloads the cursor`, () =>
    Effect.gen(function* () {
      const wake = yield* Wake;
      yield* wake.create({});
      const appended = yield* request("atomic");
      const records = yield* OwnerRecords;
      const marks = yield* ProjectionMarks;
      let broken = true;

      const triggers = yield* triggersOf("atomic", 1, [Wake], match, marks).pipe(
        Effect.provideService(OwnerRecords, {
          ...records,
          write: (key, value) =>
            records.write(key, value).pipe(
              Effect.andThen(
                Effect.suspend(() =>
                  broken
                    ? Effect.fail(
                        new StorageFault({
                          operation: "test.cursor",
                          retryable: true,
                          message: "rollback",
                        }),
                      )
                    : Effect.void,
                ),
              ),
            ),
        }),
      );

      expect((yield* Effect.flip(triggers.readRemote)).message).toBe("rollback");
      expect(yield* records.read(cursorKey("atomic"))).toEqual(Option.none());
      expect((yield* marks.summary("atomic", 0)).fresh).toBe(0);
      expect(yield* triggers.next).toBe(false);
      broken = false;
      yield* triggers.readRemote;
      expect((yield* marks.summary("atomic", 0)).fresh).toBe(1);
      expect(yield* records.read(cursorKey("atomic"))).toEqual(Option.some(appended.offset));
    }),
  );
}

const testHost = host(memoryHost());

const it = {
  effect: (
    name: string,
    run: () => Effect.Effect<void, unknown, Layer.Success<typeof testHost> | Scope.Scope>,
  ) => layer(testHost)((test) => test.effect(name, run)),
};

{
  it.effect(
    "B unavailable fails the pass after local bindings run, retries without a commit, and recovers",
    () =>
      Effect.gen(function* () {
        const wake = yield* Wake;
        yield* wake.create({});
        yield* source("remote");
        yield* request("remote");
        const seen: Array<string> = [];
        const projection = declaration("offline", seen);
        let broken = true;
        let reads = 0;
        let polls = 0;
        yield* Layer.build(
          Projection.attach(projection, {
            durable: true,
            triggers: [Wake],
            passRetry: Schedule.spaced("10 millis"),
          }),
        ).pipe(
          Effect.provideService(Wake, {
            ...wake,
            wire: {
              ...wake.wire,
              read: (id, options) =>
                Effect.suspend(() => {
                  reads++;

                  return broken ? Effect.fail(offline) : wake.wire.read(id, options);
                }),
              readNext: (id, options) =>
                Effect.suspend(() => {
                  polls++;

                  return broken ? Effect.fail(offline) : wake.wire.readNext(id, options);
                }),
            },
          }),
        );
        const registry = yield* Projection.Registry;
        const status = registry.status.pipe(Effect.map((statuses) => statuses[0]!));
        expect((yield* eventually(status, (s) => s.state === "retrying")).state).toBe("retrying");
        yield* source("local");
        yield* TestClock.adjust("10 millis");
        expect(
          yield* eventually(
            Effect.sync(() => seen.includes("local")),
            (done) => done,
          ),
        ).toBe(true);
        expect((yield* status).failure).toMatchObject({
          value: { retryable: true, message: expect.stringContaining("wake") },
        });
        expect(reads).toBe(2);
        expect(polls).toBeLessThanOrEqual(2);
        broken = false;
        yield* TestClock.adjust("10 millis");
        expect(
          yield* eventually(
            Effect.sync(() => seen.includes("remote")),
            (done) => done,
          ),
        ).toBe(true);
        expect((yield* eventually(status, (s) => s.failures === 0)).state).toBe("running");
      }),
  );

  it.effect("a missing B stream is harmless and its later creation wakes A", () =>
    Effect.gen(function* () {
      yield* source("new");
      const seen: Array<string> = [];
      yield* Layer.build(
        Projection.attach(declaration("missing", seen), {
          triggers: [Wake],
          passRetry: Schedule.spaced("10 millis"),
        }),
      );
      const registry = yield* Projection.Registry;
      const status = registry.status.pipe(Effect.map((statuses) => statuses[0]!));
      expect(
        Option.isSome((yield* eventually(status, (s) => Option.isSome(s.lastPassAt))).lastPassAt),
      ).toBe(true);
      expect((yield* status).failures).toBe(0);
      yield* (yield* Wake).create({});
      yield* request("new");
      yield* TestClock.adjust("10 millis");
      expect(
        yield* eventually(
          Effect.sync(() => seen.length),
          (n) => n === 1,
        ),
      ).toBe(1);
    }),
  );

  for (const fault of ["decode", "cursor", "request"] as const) {
    it.effect(
      `${fault} faults stop only that remote trigger; rescan and retry clear the stop`,
      () =>
        Effect.gen(function* () {
          const wake = yield* Wake;
          yield* wake.create({});
          yield* (yield* OtherWake).create({});
          yield* source("broken");
          yield* source("healthy");
          yield* request("broken");
          yield* request("healthy", OtherWake);
          let broken = true;
          const seen: Array<string> = [];
          const projection = declaration(`poison-${fault}`, seen);
          yield* Layer.build(
            Projection.attach(projection, { durable: true, triggers: [Wake, OtherWake] }),
          ).pipe(
            Effect.provideService(Wake, {
              ...wake,
              wire: {
                ...wake.wire,
                read: (id, options) =>
                  broken && fault === "request"
                    ? Effect.fail(new InvalidReadRequest({ id, message: "bad request" }))
                    : wake.wire.read(id, options).pipe(
                        Effect.map((page) =>
                          !broken
                            ? page
                            : {
                                ...page,
                                ...(fault === "cursor"
                                  ? { nextOffset: "invalid" }
                                  : { messages: [{ data: new TextEncoder().encode("{}") }] }),
                              },
                        ),
                      ),
              },
            }),
          );
          const registry = yield* Projection.Registry;
          expect(
            yield* eventually(
              Effect.sync(() => seen.includes("healthy")),
              (done) => done,
            ),
          ).toBe(true);
          expect((yield* registry.status)[0]!.triggers[0]!.failure).toMatchObject({
            value: { reason: Option.some("invalid-source"), retryable: false },
          });
          expect(yield* (yield* OwnerRecords).read(cursorKey(projection.id))).toEqual(
            Option.none(),
          );
          yield* source("local");
          expect(
            yield* eventually(
              Effect.sync(() => seen.includes("local")),
              (done) => done,
            ),
          ).toBe(true);
          yield* registry.retry(projection);
          expect((yield* registry.status)[0]!.triggers[0]!.failure).toEqual(Option.none());
          yield* eventually(registry.status, (s) => Option.isSome(s[0]!.triggers[0]!.failure));
          broken = false;
          yield* registry.rescan(projection, Wake);
          expect(
            yield* eventually(
              Effect.sync(() => seen.includes("broken")),
              (done) => done,
            ),
          ).toBe(true);
          expect((yield* registry.status)[0]!.triggers[0]!.failure).toEqual(Option.none());
        }),
    );
  }

  it.effect(
    "a failing remote still attempts later remote pages; gone streams keep their cursor",
    () =>
      Effect.gen(function* () {
        const wake = yield* Wake;
        yield* wake.create({});
        yield* (yield* OtherWake).create({});
        yield* request("one");
        yield* request("two", OtherWake);
        const marks = yield* ProjectionMarks;

        const triggers = yield* triggersOf("partial", 1, [Wake, OtherWake], match, marks).pipe(
          Effect.provideService(Wake, {
            ...wake,
            wire: { ...wake.wire, read: () => Effect.fail(offline) },
          }),
        );

        expect((yield* Effect.flip(triggers.readRemote)).retryable).toBe(true);
        expect((yield* marks.summary("partial", 0)).fresh).toBe(1);
        expect(yield* triggers.next).toBe(false);

        const gone = yield* triggersOf("gone", 1, [Wake], match, marks).pipe(
          Effect.provideService(Wake, {
            ...wake,
            wire: { ...wake.wire, read: (id) => Effect.fail(new StreamGone({ id })) },
          }),
        );

        yield* gone.readRemote;
        expect(yield* gone.next).toBe(false);
        expect(yield* (yield* OwnerRecords).read(cursorKey("gone"))).toEqual(Option.none());
      }),
  );
}
