import { it as baseIt, layer } from "@effect/vitest";
import { expect } from "vitest";
import {
  Array,
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { SqlClient } from "effect/sql";
import { CommitBoundary, MemoryStorage, Streamsy } from "@streamsy/core";
import {
  CurrentOwner,
  enlist,
  isForeignOwner,
  ownerOf,
  OwnerRecords,
} from "@streamsy/core/internal";
import { Protocol, Streams, StreamsWriter } from "@streamsy/core/protocol";
import { BunStorage } from "@streamsy/storage/bun";
import {
  CheckpointStore,
  Commit,
  Transaction,
  Projection,
  ProjectionFault,
} from "@streamsy/projection";
import { withFamilies } from "./support/families.ts";

class Input extends Streamsy.json<Input>()("owner/Input", "owner/input", {
  params: {},
  schema: Schema.Finite,
}) {}

class Sink extends Streamsy.json<Sink>()("owner/Sink", "owner/sink/:generation", {
  params: { generation: Schema.FiniteFromString },
  schema: Schema.Finite,
}) {}

class Routed extends Streamsy.json<Routed>()("owner/Routed", "owner/routed/:bucket/:generation", {
  params: { bucket: Schema.String, generation: Schema.FiniteFromString },
  schema: Schema.Finite,
}) {}

class Accepted extends Streamsy.wake<Accepted>()("owner/Accepted", "owner/accepted/:generation", {
  params: { generation: Schema.FiniteFromString },
}) {}

const services = Layer.mergeAll(CheckpointStore.layer, Transaction.layer);

const sqlite = withFamilies(
  services.pipe(Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } }))),
  Input,
  Sink,
  Routed,
  Accepted,
);

const memory = withFamilies(
  services.pipe(Layer.provideMerge(MemoryStorage.layerProtocol())),
  Input,
  Sink,
  Routed,
  Accepted,
);

const generation = 1;

const outputParams = { generation };

const common = { params: {}, generation, inputs: { input: Input } };

const outputs = { sink: Projection.perGeneration(Sink, "generation") };

const accepted = Projection.perGeneration(Accepted, "generation");

const missing = { _tag: "StreamNotFound" };

const seed = Effect.gen(function* () {
  const input = yield* Input;
  yield* input.create({});

  return yield* input.append({}, [1]);
});

const sqlProcess = Projection.process({
  commit: Commit.transaction,
  id: "sql",
  params: {},
  inputs: { input: Input },
  process: () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe("INSERT INTO effects (value) VALUES (1)");
    }),
}).at({});

const conflict = new ProjectionFault({
  phase: "checkpoint",
  reason: "token-conflict",
  message: "lost compare-and-set",
});

const rejectSettle = <A, E, R>(body: Effect.Effect<A, E, CheckpointStore | R>) =>
  CheckpointStore.use((store) =>
    body.pipe(
      Effect.provideService(CheckpointStore, {
        ...store,
        compareAndSet: () => Effect.fail(conflict),
      }),
    ),
  );

for (const [name, host] of [
  ["memory", memory],
  ["SQLite", sqlite],
] as const) {
  layer(host, { buildPerTest: true })(`transaction commit on ${name}`, (it) => {
    it.effect("preserves handler failures and defects after a generation-bound append", () =>
      Effect.gen(function* () {
        yield* seed;
        const sink = yield* Sink;
        const store = yield* CheckpointStore;
        const failed = "handler failed";
        const defect = new Error("handler defect");

        const broken = (after: Effect.Effect<void, string>) =>
          Projection.process({
            ...common,
            id: "owner-handler-failure",
            outputs,
            commit: Commit.transaction,
            process: Projection.each(({ item }) =>
              sink.append(outputParams, [item]).pipe(Effect.andThen(after)),
            ),
          }).at({});

        const typed = broken(Effect.fail(failed));
        expect(yield* Projection.run(typed).pipe(Effect.flip)).toBe(failed);
        expect((yield* store.load(typed)).token).toBe("0");
        expect(yield* sink.head(outputParams).pipe(Effect.flip)).toMatchObject(missing);

        const died = yield* Projection.run(broken(Effect.die(defect))).pipe(Effect.exit);
        expect(Exit.isFailure(died) && Cause.squash(died.cause)).toBe(defect);
        expect((yield* store.load(typed)).token).toBe("0");
        expect(yield* sink.head(outputParams).pipe(Effect.flip)).toMatchObject(missing);

        yield* Projection.run(broken(Effect.void));
        expect((yield* store.load(typed)).token).toBe("1");
        expect((yield* sink.collect(outputParams)).items).toEqual([1]);
      }),
    );

    it.effect(
      "commits folded state, output and handler writes together after a failed settle",
      () =>
        Effect.gen(function* () {
          yield* seed;
          const records = yield* OwnerRecords;
          const sink = yield* Sink;
          const store = yield* CheckpointStore;

          const projection = Projection.mapAccumBatch({
            ...common,
            id: "owner-settle",
            outputs,
            commit: Commit.transaction,
            state: { schema: Schema.Finite, initial: 10 },
            mapAccumBatch: (state, batch) =>
              Effect.gen(function* () {
                const next = state + batch.input.items.length;
                yield* records.write("handler", String(next));

                return [next, { sink: batch.input.items }] as const;
              }),
          }).at({});

          expect(yield* rejectSettle(Projection.run(projection)).pipe(Effect.flip)).toBe(conflict);
          expect((yield* store.load(projection)).token).toBe("0");
          expect((yield* Projection.read(projection)).state).toBe(10);
          expect(yield* records.read("handler")).toEqual(Option.none());
          expect(yield* sink.head(outputParams).pipe(Effect.flip)).toMatchObject(missing);

          yield* Projection.run(projection);
          expect((yield* store.load(projection)).token).toBe("1");
          expect((yield* Projection.read(projection)).state).toBe(11);
          expect(yield* records.read("handler")).toEqual(Option.some("11"));
          expect((yield* sink.collect(outputParams)).items).toEqual([1]);
        }),
    );

    it.effect("rejects a checkpointer or declared output owned by another transaction", () =>
      Effect.gen(function* () {
        yield* seed;
        const store = yield* CheckpointStore;
        const sink = yield* Sink;
        let calls = 0;

        const handler = () =>
          Effect.sync(() => {
            calls += 1;
          });

        const noOutputs = Projection.process({
          ...common,
          id: "foreign-checkpointer",
          commit: Commit.transaction,
          process: handler,
        }).at({});

        const withOutput = Projection.process({
          ...common,
          id: "foreign-output",
          commit: Commit.transaction,
          outputs,
          process: handler,
        }).at({});

        expect(
          yield* Projection.run(noOutputs).pipe(
            Effect.provideService(CheckpointStore, { ...store, load: (key) => store.load(key) }),
            Effect.flip,
          ),
        ).toMatchObject({ phase: "checkpoint", reason: "unsupported-composition" });
        expect(
          yield* Projection.run(withOutput).pipe(
            Effect.provideService(Sink, { ...sink, owner: {} }),
            Effect.flip,
          ),
        ).toMatchObject({ phase: "load", reason: "unsupported-composition" });
        expect(calls).toBe(0);
        expect((yield* store.load(noOutputs)).token).toBe("0");
        expect((yield* store.load(withOutput)).token).toBe("0");
      }),
    );

    it.effect("rolls back a successful unit when the enclosing transaction fails", () =>
      Effect.gen(function* () {
        yield* seed;
        const boundary = yield* CommitBoundary;
        const sink = yield* Sink;
        const store = yield* CheckpointStore;

        const projection = Projection.process({
          ...common,
          id: "outer-rollback",
          commit: Commit.transaction,
          outputs,
          process: Projection.each(({ item }) => sink.append(outputParams, [item])),
        }).at({});

        const result = yield* boundary
          .withTransaction(
            Effect.gen(function* () {
              expect((yield* Projection.run(projection)).units).toBe(1);
              expect((yield* store.load(projection)).token).toBe("1");
              expect((yield* sink.collect(outputParams)).items).toEqual([1]);

              return yield* Effect.fail("outer failed");
            }),
          )
          .pipe(Effect.flip);

        expect(result).toBe("outer failed");
        expect((yield* store.load(projection)).token).toBe("0");
        expect(yield* sink.head(outputParams).pipe(Effect.flip)).toMatchObject(missing);
      }),
    );

    it.effect("commits routed output, accepted rows and timers in the same unit", () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(0);
        const tail = yield* seed;
        const routed = yield* Routed;
        const wakes = yield* Accepted;
        const store = yield* CheckpointStore;
        const params = { generation, bucket: "day" };

        const declaration = Projection.process({
          ...common,
          id: "owner-routed",
          commit: Commit.transactionWithCleanup(() => Effect.void),
          outputs: {
            routed: Projection.perGeneration(Routed, "generation", { routeBy: ["bucket"] }),
          },
          accepted,
          prepare: () => Effect.succeed("day"),
          process: (batch, unit, { prepared, outputs: { routed: out } }) =>
            Effect.gen(function* () {
              const target = yield* out.open({ bucket: prepared });
              yield* Streams.append(target, [
                Option.getOrElse(unit.due, () => batch.input.items.length),
              ]).pipe(Effect.provideService(StreamsWriter, routed.wire));

              return Option.isNone(unit.due) ? Projection.wakeAt(100) : undefined;
            }),
        });

        const projection = declaration.at({});

        expect(yield* rejectSettle(Projection.run(projection)).pipe(Effect.flip)).toBe(conflict);
        expect((yield* store.load(projection)).token).toBe("0");
        expect(yield* routed.head(params).pipe(Effect.flip)).toMatchObject(missing);
        expect(yield* wakes.head(outputParams).pipe(Effect.flip)).toMatchObject(missing);

        expect((yield* Projection.run(projection)).wakeAt).toEqual(Option.some(100));
        expect((yield* routed.collect(params)).items).toEqual([1]);
        const acceptedRows = (yield* wakes.collect(outputParams)).items;
        expect(acceptedRows).toMatchObject([
          { value: { stream: Input.at({}).id, offset: tail.offset } },
        ]);
        yield* TestClock.adjust(99);
        expect((yield* Projection.run(projection)).units).toBe(0);
        yield* TestClock.adjust(1);
        expect((yield* Projection.run(projection)).wakeAt).toEqual(Option.none());
        expect((yield* routed.collect(params)).items).toEqual([1, 100]);
        expect((yield* wakes.collect(outputParams)).items).toEqual(acceptedRows);
        expect(yield* Projection.retire(declaration, {}, { generation })).toMatchObject({
          streams: [
            {
              output: "@streamsy/accepted",
              id: Accepted.at(outputParams).id,
              result: "removed",
            },
            { output: "routed", id: Routed.at(params).id, result: "removed" },
          ],
        });
        expect(yield* routed.head(params).pipe(Effect.flip)).toMatchObject(missing);
      }),
    );

    for (const kind of ["unrouted", "routed"] as const) {
      const declare = (
        id: string,
        cleanup: Effect.Effect<void, ProjectionFault>,
        unit: Effect.Effect<void> = Effect.void,
      ) => {
        const commit = Commit.transactionWithCleanup(() => cleanup);
        const spec = { ...common, id, commit };

        return kind === "unrouted"
          ? Projection.process({ ...spec, process: () => unit })
          : Projection.process({
              ...spec,
              outputs: {
                routed: Projection.perGeneration(Routed, "generation", { routeBy: ["bucket"] }),
              },
              process: (_batch, _unit, { outputs: { routed } }) =>
                routed.open({ bucket: "a" }).pipe(Effect.andThen(unit)),
            });
      };

      for (const created of ["checkpoint", "tombstone"] as const) {
        it.effect(`${kind} forget rolls back when a ${created} appears after it loaded none`, () =>
          Effect.gen(function* () {
            yield* seed;
            const store = yield* CheckpointStore;
            const records = yield* OwnerRecords;
            const id = `appeared-${kind}-${created}`;

            // A memory or SQLite owner runs one transaction at a time, so the cleanup itself
            // creates the record that another SQL host can commit at this point.
            const bound = declare(
              id,
              Effect.gen(function* () {
                yield* records.write("cleanup", id).pipe(Effect.orDie);

                yield* created === "checkpoint"
                  ? store.compareAndSet(key, "0", {
                      identity: { inputs: {} },
                      inputs: {},
                      adapters: {},
                    })
                  : store.retire!(key);
              }),
            ).at({});

            const key = Projection.key(bound);
            const route = Routed.at({ bucket: "a", generation }).id;
            yield* store.routes!.add(key, "routed", route);

            expect(yield* Projection.forget(bound).pipe(Effect.flip)).toMatchObject({
              phase: "checkpoint",
              reason: "token-conflict",
              retryable: true,
            });
            expect((yield* store.load(key)).token).toBe("0");
            expect(yield* records.read("cleanup")).toEqual(Option.none());
            expect(yield* store.routes!.state(key, "routed", route)).toBe("live");
          }),
        );
      }

      it.effect(`${kind} forget and a first unit run one after the other`, () =>
        Effect.gen(function* () {
          yield* seed;
          const store = yield* CheckpointStore;
          const records = yield* OwnerRecords;
          const cleaning = yield* Deferred.make<void>();
          const resume = yield* Deferred.make<void>();

          const bound = declare(
            `serial-${kind}`,
            Deferred.succeed(cleaning, undefined).pipe(
              Effect.andThen(Deferred.await(resume)),
              Effect.andThen(records.remove("unit")),
              Effect.orDie,
            ),
            records.write("unit", kind).pipe(Effect.orDie),
          ).at({});

          const forget = yield* Projection.forget(bound).pipe(Effect.forkChild);
          yield* Deferred.await(cleaning);
          const unit = yield* Projection.run(bound).pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          yield* Deferred.succeed(resume, undefined);
          yield* Fiber.join(forget);
          yield* Fiber.join(unit);
          expect((yield* store.load(bound)).token).toBe("1");
          expect(yield* records.read("unit")).toEqual(Option.some(kind));

          yield* Projection.forget(bound);
          expect((yield* store.load(bound)).token).toBe("0");
          expect(yield* records.read("unit")).toEqual(Option.none());
        }),
      );

      it.effect(`${kind} forget of an absent checkpoint still runs cleanup`, () =>
        Effect.gen(function* () {
          const records = yield* OwnerRecords;
          const id = `absent-${kind}`;
          const bound = declare(id, records.write("cleanup", id).pipe(Effect.orDie)).at({});
          yield* Projection.forget(bound);
          expect(yield* records.read("cleanup")).toEqual(Option.some(id));
        }),
      );
    }

    for (const operation of ["forget", "retire"] as const) {
      it.effect(`owner cleanup joins ${operation} and rolls back on failure`, () =>
        Effect.gen(function* () {
          yield* seed;
          const fail = yield* Ref.make(true);
          const records = yield* OwnerRecords;
          const store = yield* CheckpointStore;
          const sink = yield* Sink;

          const declaration = Projection.process({
            ...common,
            id: `owner-${operation}`,
            commit: Commit.transactionWithCleanup((key) =>
              Effect.gen(function* () {
                const ownerRecords = yield* OwnerRecords;
                const checkpoints = yield* CheckpointStore;

                if (operation === "forget") expect((yield* checkpoints.load(key)).token).toBe("0");
                else
                  expect(
                    yield* checkpoints.load(key).pipe(Effect.flip, Effect.orDie),
                  ).toMatchObject({
                    reason: "retired-generation",
                  });

                yield* ownerRecords.write("cleanup", key.id).pipe(Effect.orDie);

                if (yield* Ref.get(fail)) return yield* conflict;

                return yield* Effect.void;
              }),
            ),
            outputs,
            process: () => sink.append(outputParams, [1]),
          });

          const projection = declaration.at({});
          yield* Projection.run(projection);
          const before = yield* store.load(projection);

          const cleanup =
            operation === "forget"
              ? Projection.forget(projection)
              : Projection.retire(declaration, {}, { generation }).pipe(Effect.asVoid);

          expect(yield* cleanup.pipe(Effect.flip)).toBe(conflict);
          expect(yield* store.load(projection)).toEqual(before);
          expect(yield* records.read("cleanup")).toEqual(Option.none());
          expect((yield* sink.collect(outputParams)).items).toEqual([1]);

          yield* Ref.set(fail, false);
          yield* cleanup;
          expect(yield* records.read("cleanup")).toEqual(Option.some(projection.id));

          if (operation === "forget") expect((yield* store.load(projection)).token).toBe("0");
          else {
            expect(yield* store.load(projection).pipe(Effect.flip)).toMatchObject({
              reason: "retired-generation",
            });
            expect(yield* sink.head(outputParams).pipe(Effect.flip)).toMatchObject(missing);
          }
        }),
      );
    }
  });
}

layer(sqlite)((it) => {
  it.effect(
    "captured SQLite client joins the transaction commit without an ambient SqlClient",
    () =>
      Effect.gen(function* () {
        yield* seed;
        const sql = yield* SqlClient.SqlClient;
        yield* sql`CREATE TABLE effects (value INTEGER NOT NULL)`;

        const projection = Projection.process({
          ...common,
          id: "owner-captured-sql",
          commit: Commit.transaction,
          outputs,
          process: () => sql`INSERT INTO effects VALUES (1)`.pipe(Effect.asVoid),
        }).at({});

        const run = Projection.run(projection).pipe(
          Effect.updateContext(
            (context: Context.Context<CheckpointStore | Transaction | Input | Sink>) =>
              Context.omit(SqlClient.SqlClient)(context),
          ),
        );

        expect(yield* rejectSettle(run).pipe(Effect.flip)).toBe(conflict);
        expect(yield* sql`SELECT * FROM effects`).toEqual([]);
        const store = yield* CheckpointStore;
        expect((yield* store.load(projection)).token).toBe("0");
        yield* run;
        expect(yield* sql`SELECT * FROM effects`).toEqual([{ value: 1 }]);
        expect((yield* store.load(projection)).token).toBe("1");
      }),
  );
});

layer(sqlite, { buildPerTest: true })((it) => {
  it.effect("retire cleanup sees the transaction owner over the ambient owner", () =>
    Effect.gen(function* () {
      yield* seed;
      const tx = yield* Transaction;
      const ambient = {};
      let cleanupOwner: object | undefined;

      const declaration = Projection.process({
        ...common,
        id: "retire-current-owner",
        commit: Commit.transactionWithCleanup(() =>
          Effect.map(CurrentOwner, (owner) => {
            cleanupOwner = owner;
          }),
        ),
        process: () => Effect.void,
      });

      expect(
        yield* Projection.retire(declaration, {}, { generation }).pipe(
          Effect.provideService(CurrentOwner, ambient),
        ),
      ).toMatchObject({ record: "retired" });
      expect(cleanupOwner).toBe(tx.owner);
    }),
  );

  it.effect("retire cannot enter an application-owned raw SQLite transaction", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      let called = false;

      const declaration = Projection.process({
        ...common,
        id: "retire-raw-sql",
        commit: Commit.transactionWithCleanup(() =>
          Effect.sync(() => {
            called = true;
          }),
        ),
        process: () => Effect.void,
      });

      const exit = yield* sql
        .withTransaction(Projection.retire(declaration, {}, { generation }))
        .pipe(Effect.exit);

      expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toMatchObject({
        message: "Commit boundary cannot enter an unowned ambient SQL transaction",
      });
      expect(called).toBe(false);
    }),
  );

  it.effect("enlist reports a typed error inside a SQLite owner transaction", () =>
    Effect.gen(function* () {
      const boundary = yield* CommitBoundary;
      const participant = { onCommit: () => {}, onRollback: () => {} };

      const exit = yield* boundary
        .withTransaction(enlist(ownerOf(boundary), participant))
        .pipe(Effect.exit);

      expect(Exit.isFailure(exit) && Cause.findErrorOption(exit.cause)).toMatchObject({
        _tag: "Some",
        value: {
          _tag: "MemoryEnlistFault",
          message: "Enlisting requires the active memory owner fiber",
        },
      });
    }),
  );

  const foreignSink = Layer.build(MemoryStorage.layer(Streamsy.group(Sink))).pipe(
    Effect.map(Context.get(Sink)),
  );

  it.effect("transaction commit rejects a foreign handler write and rolls back its output", () =>
    Effect.gen(function* () {
      yield* seed;
      const local = yield* Sink;
      const foreign = yield* foreignSink;
      const store = yield* CheckpointStore;
      yield* foreign.create(outputParams);

      const projection = Projection.process({
        ...common,
        id: "foreign-handler-write",
        outputs,
        commit: Commit.transaction,
        process: () =>
          local
            .append(outputParams, [1])
            .pipe(Effect.andThen(() => foreign.append(outputParams, [2]))),
      }).at({});

      expect(yield* Projection.run(projection).pipe(Effect.flip)).toMatchObject({
        phase: "process",
        reason: "unsupported-composition",
      });
      expect((yield* store.load(projection)).token).toBe("0");
      expect(yield* local.head(outputParams).pipe(Effect.flip)).toMatchObject(missing);
      expect((yield* foreign.collect(outputParams)).items).toEqual([]);
    }),
  );

  for (const operation of ["forget", "retire"] as const) {
    it.effect(`${operation} rejects a foreign write in owner cleanup`, () =>
      Effect.gen(function* () {
        yield* seed;
        const local = yield* Sink;
        const foreign = yield* foreignSink;
        const store = yield* CheckpointStore;
        yield* foreign.create(outputParams);

        const declaration = Projection.process({
          ...common,
          id: `foreign-cleanup-${operation}`,
          outputs,
          commit: Commit.transactionWithCleanup(() =>
            foreign.append(outputParams, [2]).pipe(Effect.asVoid, Effect.orDie),
          ),
          process: () => local.append(outputParams, [1]),
        });

        const projection = declaration.at({});
        yield* Projection.run(projection);
        const before = yield* store.load(projection);

        const cleanup =
          operation === "forget"
            ? Projection.forget(projection)
            : Projection.retire(declaration, {}, { generation }).pipe(Effect.asVoid);

        const exit = yield* cleanup.pipe(
          Effect.provideService(CurrentOwner, foreign.owner),
          Effect.exit,
        );

        expect(Exit.isFailure(exit) && isForeignOwner(Cause.squash(exit.cause))).toBe(true);
        expect(yield* store.load(projection)).toEqual(before);
        expect((yield* local.collect(outputParams)).items).toEqual([1]);
        expect((yield* foreign.collect(outputParams)).items).toEqual([]);
      }),
    );

    it.effect(`${operation} rejects a foreign checkpoint store`, () =>
      Effect.gen(function* () {
        yield* seed;
        const store = yield* CheckpointStore;

        const foreign = Context.get(
          yield* Layer.build(CheckpointStore.layerMemory),
          CheckpointStore,
        );

        let called = false;

        const declaration = Projection.process({
          ...common,
          id: `foreign-checkpoint-${operation}`,
          commit: Commit.transactionWithCleanup(() =>
            Effect.sync(() => {
              called = true;
            }),
          ),
          process: () => Effect.void,
        });

        const projection = declaration.at({});
        yield* Projection.run(projection);
        const before = yield* store.load(projection);
        yield* foreign.compareAndSet(projection, "0", Option.getOrThrow(before.record));
        const foreignBefore = yield* foreign.load(projection);

        const cleanup =
          operation === "forget"
            ? Projection.forget(projection)
            : Projection.retire(declaration, {}, { generation }).pipe(Effect.asVoid);

        expect(
          yield* cleanup.pipe(Effect.provideService(CheckpointStore, foreign), Effect.flip),
        ).toMatchObject({
          phase: "checkpoint",
          reason: "unsupported-composition",
          message: `The checkpoint store for ${projection.id} does not share the transaction owner`,
        });
        expect(called).toBe(false);
        expect(yield* foreign.load(projection)).toEqual(foreignBefore);
        expect(yield* store.load(projection)).toEqual(before);
      }),
    );

    for (const enclosing of [false, true]) {
      it.effect(
        `captured SQLite client uses a savepoint during ${operation} ${enclosing ? "inside the owner" : "standalone"}`,
        () =>
          Effect.gen(function* () {
            yield* seed;
            const sql = yield* SqlClient.SqlClient;
            const boundary = yield* CommitBoundary;
            const sink = yield* Sink;
            const store = yield* CheckpointStore;
            yield* sql`CREATE TABLE cleanup_effects (id TEXT NOT NULL)`;

            const declaration = Projection.process({
              ...common,
              id: `savepoint-${operation}`,
              outputs,
              commit: Commit.transactionWithCleanup((key) =>
                sql
                  .withTransaction(
                    Effect.gen(function* () {
                      const transaction = yield* Effect.serviceOption(sql.transactionService);
                      expect(Option.map(transaction, ([, depth]) => depth)).toEqual(
                        Option.some(enclosing ? 2 : 1),
                      );
                      yield* sql`INSERT INTO cleanup_effects VALUES (${key.id})`;
                    }),
                  )
                  .pipe(Effect.orDie),
              ),
              process: () => sink.append(outputParams, [1]),
            });

            const projection = declaration.at({});
            yield* Projection.run(projection);
            const before = yield* store.load(projection);

            const cleanup =
              operation === "forget"
                ? Projection.forget(projection)
                : Projection.retire(declaration, {}, { generation }).pipe(Effect.asVoid);

            const checkCleaned = Effect.gen(function* () {
              expect(yield* sql`SELECT id FROM cleanup_effects`).toEqual([{ id: projection.id }]);

              if (operation === "forget") {
                expect((yield* store.load(projection)).token).toBe("0");
              } else {
                expect(yield* store.load(projection).pipe(Effect.flip, Effect.orDie)).toMatchObject(
                  {
                    reason: "retired-generation",
                  },
                );
                expect(
                  yield* sink.head(outputParams).pipe(Effect.flip, Effect.orDie),
                ).toMatchObject(missing);
              }
            });

            if (enclosing) {
              const outer = boundary.withTransaction(
                sql
                  .withTransaction(cleanup.pipe(Effect.andThen(checkCleaned)))
                  .pipe(Effect.andThen(Effect.fail("later failure"))),
              );

              expect(yield* outer.pipe(Effect.flip)).toBe("later failure");
              expect(yield* sql`SELECT id FROM cleanup_effects`).toEqual([]);
              expect(yield* store.load(projection)).toEqual(before);
              expect((yield* sink.collect(outputParams)).items).toEqual([1]);
            } else {
              yield* cleanup;
              yield* checkCleaned;
            }
          }),
      );
    }
  }
});

layer(sqlite, { buildPerTest: true })((it) => {
  it.effect("non-foreign-owner defects remain defects on SQLite", () =>
    Effect.gen(function* () {
      yield* seed;
      const defect = new Error("different defect");
      const broken = { ...sqlProcess, process: () => Effect.die(defect) };
      const exit = yield* Projection.run(broken).pipe(Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);

      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(defect);
    }),
  );

  it.effect("transaction checks all declared outputs before running the handler", () =>
    Effect.gen(function* () {
      yield* seed;
      let calls = 0;

      const base = Projection.emit({
        commit: Commit.producer,
        id: "owner",
        params: {},
        inputs: { input: Input },
        outputs: { output: Sink.at(outputParams) },
        emit: () =>
          Effect.sync(() => {
            calls += 1;

            return { output: [2] };
          }),
      }).at({});

      const tx = yield* Transaction;

      const result = yield* Projection.run({ ...base, commit: Commit.transaction }).pipe(
        Effect.provideService(Transaction, { ...tx, owner: {} }),
        Effect.result,
      );

      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { phase: "load", reason: "unsupported-composition" },
      });
      expect(calls).toBe(0);
      expect((yield* (yield* CheckpointStore).load(base)).token).toBe("0");
    }),
  );

  it.effect(
    "transaction commits declared outputs and state together and rolls both back on conflict",
    () =>
      Effect.gen(function* () {
        yield* seed;
        const sink = yield* Sink;
        yield* sink.create(outputParams);

        const base = Projection.mapAccumBatch({
          commit: Commit.producer,
          id: "transaction-outputs",
          params: {},
          inputs: { input: Input },
          outputs: { output: Sink.at(outputParams) },
          state: { schema: Schema.Finite, initial: 0 },
          mapAccumBatch: (state, batch) =>
            Effect.succeed([
              state + batch.input.items.length,
              { output: batch.input.items },
            ] as const),
        }).at({});

        const projection = { ...base, commit: Commit.transaction };
        const store = yield* CheckpointStore;

        const failed = yield* Projection.run(projection).pipe(
          Effect.provideService(CheckpointStore, {
            ...store,
            compareAndSet: () =>
              Effect.fail(
                new ProjectionFault({
                  phase: "checkpoint",
                  reason: "token-conflict",
                  message: "lost compare-and-set",
                }),
              ),
          }),
          Effect.result,
        );

        expect(failed).toMatchObject({ _tag: "Failure", failure: { reason: "token-conflict" } });
        expect((yield* Projection.read(projection)).state).toBe(0);
        expect(
          Array.flatMap(yield* Stream.runCollect(sink.read(outputParams)), (batch) => batch.items),
        ).toEqual([]);
        yield* Projection.run(projection);
        expect((yield* Projection.read(projection)).state).toBe(1);
        expect(
          Array.flatMap(yield* Stream.runCollect(sink.read(outputParams)), (batch) => batch.items),
        ).toEqual([1]);
      }),
  );
});

baseIt.effect("SQL from the host rolls back with the checkpoint", () =>
  Effect.gen(function* () {
    const sqlLayer = BunStorage.layer(Streamsy.group(), { client: { filename: ":memory:" } });

    const host = Layer.merge(
      Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(Layer.provideMerge(sqlLayer)),
      Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(Input)),
    );

    const context = yield* Layer.build(host);

    return yield* Effect.gen(function* () {
      yield* seed;
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe("CREATE TABLE effects (value INTEGER NOT NULL)");
      const store = yield* CheckpointStore;

      const failure = new ProjectionFault({
        phase: "checkpoint",
        reason: "storage-failure",
        message: "before checkpoint",
      });

      const result = yield* Projection.run(sqlProcess).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: () => Effect.fail(failure),
        }),
        Effect.result,
      );

      expect(result).toMatchObject({ _tag: "Failure", failure });
      expect(yield* sql.unsafe("SELECT * FROM effects")).toEqual([]);
      expect((yield* store.load(sqlProcess)).token).toBe("0");
      yield* Projection.run(sqlProcess);
      expect(yield* sql.unsafe("SELECT * FROM effects")).toEqual([{ value: 1 }]);

      const folded = Projection.fold({
        id: "sql-fold",
        params: {},
        inputs: { input: Input },
        state: { schema: Schema.Finite, initial: 10 },
        commit: Commit.transaction,
        fold: (previous, { item }) =>
          Effect.gen(function* () {
            const client = yield* SqlClient.SqlClient;
            yield* client`INSERT INTO effects (value) VALUES (${previous + item})`;

            return previous + item;
          }),
      }).at({});

      const failedFold = yield* Projection.run(folded).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: () => Effect.fail(failure),
        }),
        Effect.result,
      );

      expect(failedFold).toMatchObject({ _tag: "Failure", failure });
      expect((yield* Projection.read(folded)).state).toBe(10);
      expect(yield* sql.unsafe("SELECT * FROM effects")).toEqual([{ value: 1 }]);
      yield* Projection.run(folded);
      expect((yield* Projection.read(folded)).state).toBe(11);
      expect(yield* sql.unsafe("SELECT * FROM effects")).toEqual([{ value: 1 }, { value: 11 }]);
    }).pipe(Effect.provideContext(context));
  }),
);
