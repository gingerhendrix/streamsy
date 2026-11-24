import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Cause, Context, Effect, Exit, Layer, Option, Ref, Schema } from "effect";
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
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { BunStorage } from "@streamsy/storage/bun";
import {
  CheckpointStore,
  Commit,
  OwnerTransaction,
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

const services = Layer.mergeAll(CheckpointStore.layer, OwnerTransaction.layer);

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
  layer(host, { buildPerTest: true })(`owner commit on ${name}`, (it) => {
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
            commit: Commit.ownerTransaction,
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
            commit: Commit.ownerTransaction,
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
          commit: Commit.ownerTransaction,
          process: handler,
        }).at({});

        const withOutput = Projection.process({
          ...common,
          id: "foreign-output",
          commit: Commit.ownerTransaction,
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
          commit: Commit.ownerTransaction,
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
          commit: Commit.ownerTransactionWithCleanup(() => Effect.void),
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
          routed: { routed: [{ id: Routed.at(params).id, result: "removed" }] },
        });
        expect(yield* routed.head(params).pipe(Effect.flip)).toMatchObject(missing);
      }),
    );

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
            commit: Commit.ownerTransactionWithCleanup((key) =>
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
  it.effect("captured SQLite client joins the owner commit without an ambient SqlClient", () =>
    Effect.gen(function* () {
      yield* seed;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE effects (value INTEGER NOT NULL)`;

      const projection = Projection.process({
        ...common,
        id: "owner-captured-sql",
        commit: Commit.ownerTransaction,
        outputs,
        process: () => sql`INSERT INTO effects VALUES (1)`.pipe(Effect.asVoid),
      }).at({});

      const run = Projection.run(projection).pipe(
        Effect.updateContext(
          (context: Context.Context<CheckpointStore | OwnerTransaction | Input | Sink>) =>
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
      const tx = yield* OwnerTransaction;
      const ambient = {};
      let cleanupOwner: object | undefined;

      const declaration = Projection.process({
        ...common,
        id: "retire-current-owner",
        commit: Commit.ownerTransactionWithCleanup(() =>
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
        commit: Commit.ownerTransactionWithCleanup(() =>
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

  it.effect("owner commit rejects a foreign handler write and rolls back its output", () =>
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
        commit: Commit.ownerTransaction,
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
          commit: Commit.ownerTransactionWithCleanup(() =>
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
          commit: Commit.ownerTransactionWithCleanup(() =>
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
              commit: Commit.ownerTransactionWithCleanup((key) =>
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
