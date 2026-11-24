/* oxlint-disable effecttsgo/async-function, effecttsgo/strict-effect-provide -- Bun provisions each test host inside its fresh database scope. */
import { describe, expect, test } from "vitest";
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
  Schema,
  type Scope,
} from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { recordKey } from "@streamsy/projection/checkpoint";
import { SqlStorage } from "@streamsy/storage";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  Transaction,
} from "@streamsy/projection";
import {
  dialectClient,
  freshDatabase,
  holdStatements,
  sqlDialects,
  type StatementMatch,
  withRepeatableRead,
} from "../../../storage/test/dialects/support.ts";

const TIMEOUT_MS = 30_000;

class Source extends Streamsy.json<Source>()("FirstUnit/Source", "first-unit/source", {
  params: {},
  schema: Schema.Finite,
}) {}

class Output extends Streamsy.json<Output>()(
  "FirstUnit/Output",
  "first-unit/output/:route/:generation",
  {
    params: { route: Schema.String, generation: Schema.FiniteFromString },
    schema: Schema.Finite,
  },
) {}

const group = Streamsy.group(Source, Output);

const hostLayer = (dialect: (typeof sqlDialects)["pg" | "mysql"], url: string) =>
  Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(SqlStorage.layer(group)),
    Layer.provideMerge(dialectClient(dialect, url, { maxConnections: 1 })),
  );

/** The unit writes one app row, and the cleanup deletes it with the checkpoint. */
const commit = Commit.transactionWithCleanup(() =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DELETE FROM unit_rows WHERE id = 1`;
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ProjectionFault({
          phase: "checkpoint",
          reason: "storage-failure",
          message: "Cannot delete app rows",
          cause,
        }),
    ),
  ),
);

const routed = Projection.process({
  id: "first-unit-routed",
  params: { tenant: Schema.String },
  inputs: { source: Source },
  outputs: { out: Projection.perGeneration(Output, "generation", { routeBy: ["route"] }) },
  commit,
  process: (batch, _unit, { outputs: { out } }) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const items = batch.source.items;

      if (!Array.isReadonlyArrayNonEmpty(items)) return;
      yield* sql`INSERT INTO unit_rows (id) VALUES (1)`;
      const { wire } = yield* Output;
      const address = yield* out.open({ route: "one" });
      yield* Streams.append(address, items).pipe(Effect.provideService(StreamsWriter, wire));
    }),
});

const plain = Projection.process({
  id: "first-unit-plain",
  params: { tenant: Schema.String },
  inputs: { source: Source },
  commit,
  process: (batch) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      if (batch.source.items.length === 0) return;
      yield* sql`INSERT INTO unit_rows (id) VALUES (1)`;
    }),
});

const kinds = { plain, routed };

const tenant = { tenant: "a" };

const route = { route: "one", generation: 1 };

const isCleanup: StatementMatch = (query) => query.startsWith("DELETE FROM unit_rows");

const isRouteList: StatementMatch = (query) =>
  query.startsWith("SELECT owner_key") && query.includes("owner_key >=");

const namesCheckpoint = (params: ReadonlyArray<unknown>) =>
  Array.some(params, (param) => String(param).startsWith('["streamsy.projection.v1"'));

const isCheckpointInsert: StatementMatch = (query, params) =>
  query.startsWith("INSERT INTO streamsy_owner_records") && namesCheckpoint(params);

/** The checkpoint read that starts a runner's own forget, after its final unit settled. */
const afterSettle = (): StatementMatch => {
  let settled = false;

  return (query, params) => {
    if (isCheckpointInsert(query, params)) settled = true;

    return settled && query.startsWith("SELECT owner_value") && namesCheckpoint(params);
  };
};

const tokenConflict = {
  _tag: "ProjectionFault",
  phase: "checkpoint",
  reason: "token-conflict",
  retryable: true,
};

const levels = [
  { name: "Postgres", dialect: sqlDialects.pg, repeatableRead: false },
  { name: "MySQL default", dialect: sqlDialects.mysql, repeatableRead: false },
  { name: "MySQL REPEATABLE READ", dialect: sqlDialects.mysql, repeatableRead: true },
] as const;

const fixture = (level: (typeof levels)[number], baseUrl: string) =>
  Effect.gen(function* () {
    const url = yield* freshDatabase(level.dialect, baseUrl);
    const left = yield* Layer.build(hostLayer(level.dialect, url));
    const right = yield* Layer.build(hostLayer(level.dialect, url));
    const sql = Context.get(left, SqlClient.SqlClient);
    yield* sql`CREATE TABLE unit_rows (id INTEGER NOT NULL PRIMARY KEY)`;
    const source = Context.get(left, Source);
    yield* source.create({});
    yield* source.append({}, [1]);

    // Hosts share no fiber context, so each transaction belongs to one host.
    const on =
      (context: typeof left) =>
      <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        (level.repeatableRead
          ? withRepeatableRead(Context.get(context, SqlClient.SqlClient), effect)
          : effect
        ).pipe(Effect.provide(context));

    const store = Context.get(left, CheckpointStore);

    return {
      left,
      right,
      sql,
      source,
      output: Context.get(left, Output),
      store,
      on,
      rows: Effect.map(sql`SELECT id FROM unit_rows`, (rows) => rows.length),
      routeState: store.routes!.state(routed.at(tenant), "out", Output.at(route).id),
    };
  });

const swallowing = Commit.transactionWithCleanup(() =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DELETE FROM unit_rows`;
    yield* sql`DELETE FROM missing_table`;
  }).pipe(Effect.ignore),
);

/** A swallowed statement error aborts a Postgres transaction, and its COMMIT then rolls back. */
export const registerSwallowedCleanup = () => {
  const { url } = sqlDialects.pg;

  describe.skipIf(url === undefined)("a cleanup that swallows a Postgres error", () => {
    for (const kind of ["plain", "routed"] as const)
      for (const state of ["present", "absent", "retired"] as const)
        test(
          `${kind}: forget of a binding that is ${state} fails and keeps its rows`,
          async () => {
            if (url === undefined) return;

            await Effect.runPromise(
              Effect.scoped(
                Effect.gen(function* () {
                  const f = yield* fixture(levels[0], url);
                  const binding = { ...kinds[kind].at(tenant), commit: swallowing };

                  if (state === "retired")
                    yield* Projection.retire(kinds[kind], tenant, { generation: 1 }).pipe(
                      f.on(f.left),
                    );
                  else if (state === "present") yield* Projection.run(binding).pipe(f.on(f.left));

                  if (state !== "present") yield* f.sql`INSERT INTO unit_rows (id) VALUES (1)`;

                  const stored = f.store.load(binding).pipe(
                    Effect.match({
                      onFailure: ({ reason }) => reason,
                      onSuccess: (loaded) => loaded,
                    }),
                  );

                  const before = yield* stored;

                  expect(
                    yield* Projection.forget(binding).pipe(f.on(f.left), Effect.flip),
                  ).toMatchObject({ phase: "checkpoint", reason: "storage-failure" });
                  expect(yield* stored).toEqual(before);
                  expect(yield* f.rows).toBe(1);

                  if (state === "present" && kind === "routed")
                    expect(yield* f.routeState).toBe("live");
                }),
              ),
            );
          },
          TIMEOUT_MS,
        );
  });
};

/** MySQL rolls back a deadlocked transaction. Its COMMIT still succeeds. */
export const registerSwallowedDeadlock = () => {
  const { url } = sqlDialects.mysql;

  describe.skipIf(url === undefined)("a cleanup that swallows a MySQL deadlock", () => {
    for (const kind of ["plain", "routed"] as const)
      test(
        `${kind}: forget of a present binding fails and keeps its checkpoint`,
        async () => {
          if (url === undefined) return;

          await Effect.runPromise(
            Effect.scoped(
              Effect.gen(function* () {
                const f = yield* fixture(levels[1], url);
                const ours = yield* Deferred.make<void>();
                const theirs = yield* Deferred.make<void>();
                const right = Context.get(f.right, SqlClient.SqlClient);

                const binding = {
                  ...kinds[kind].at(tenant),
                  commit: Commit.transactionWithCleanup(() =>
                    Effect.gen(function* () {
                      const sql = yield* SqlClient.SqlClient;
                      yield* sql`UPDATE unit_locks SET n = 1 WHERE id = 1`;
                      yield* Deferred.succeed(ours, undefined);
                      yield* Deferred.await(theirs);
                      // The other transaction now waits for row 1, and it changed more rows.
                      yield* Effect.sleep("300 millis");
                      yield* sql`UPDATE unit_locks SET n = 1 WHERE id = 2`;
                    }).pipe(Effect.ignore),
                  ),
                };

                yield* f.sql`CREATE TABLE unit_locks (id INTEGER NOT NULL PRIMARY KEY, n INTEGER)`;

                for (let id = 1; id <= 50; id++)
                  yield* f.sql`INSERT INTO unit_locks VALUES (${id}, 0)`;

                yield* Projection.run(binding).pipe(f.on(f.left));
                const before = yield* f.store.load(binding);

                const other = yield* right
                  .withTransaction(
                    Effect.gen(function* () {
                      yield* right`UPDATE unit_locks SET n = 2 WHERE id > 1`;
                      yield* Deferred.await(ours);
                      yield* Deferred.succeed(theirs, undefined);
                      yield* right`UPDATE unit_locks SET n = 2 WHERE id = 1`;
                    }),
                  )
                  .pipe(Effect.forkScoped);

                const forgotten = yield* Projection.forget(binding).pipe(f.on(f.left), Effect.exit);

                yield* Fiber.join(other);
                expect(yield* f.store.load(binding)).toEqual(before);
                expect(Exit.isFailure(forgotten) && Cause.squash(forgotten.cause)).toMatchObject(
                  tokenConflict,
                );

                if (kind === "routed") expect(yield* f.routeState).toBe("live");
              }),
            ),
          );
        },
        TIMEOUT_MS,
      );
  });
};

/** `forget` and `retire` beside the first unit of a binding on another host. */
export const registerFirstUnitRaces = () => {
  for (const level of levels) {
    const baseUrl = level.dialect.url;

    const run = <A, E>(
      body: (f: Effect.Success<ReturnType<typeof fixture>>) => Effect.Effect<A, E, Scope.Scope>,
    ) =>
      baseUrl === undefined
        ? Promise.resolve()
        : Effect.runPromise(
            Effect.scoped(Effect.flatMap(fixture(level, baseUrl), body)).pipe(Effect.asVoid),
          );

    describe.skipIf(baseUrl === undefined)(`first-unit races on ${level.name}`, () => {
      for (const kind of ["plain", "routed"] as const) {
        const spec = kinds[kind];
        const binding = spec.at(tenant);

        test(
          `${kind}: forget that loaded no checkpoint conflicts with a unit committed before its cleanup`,
          () =>
            run((f) =>
              Effect.gen(function* () {
                const gate = yield* holdStatements(isCleanup);

                const forget = yield* Projection.forget(binding).pipe(
                  gate.hold,
                  f.on(f.right),
                  Effect.flip,
                  Effect.forkScoped,
                );

                yield* gate.reached();

                const unit = yield* Projection.run(binding, { limit: 1 }).pipe(
                  f.on(f.left),
                  Effect.exit,
                  Effect.forkScoped,
                );

                // Routed forget at REPEATABLE READ holds the route gap, so the unit waits for it.
                if (kind === "routed" && level.repeatableRead) yield* Effect.sleep("300 millis");
                else yield* Fiber.await(unit);
                yield* gate.release();
                const fault = yield* Fiber.join(forget);
                expect((yield* Fiber.join(unit))._tag).toBe("Success");

                if (kind === "routed" && level.repeatableRead)
                  expect(fault).toMatchObject({ _tag: "ProjectionFault", retryable: true });
                else expect(fault).toMatchObject(tokenConflict);
                expect(Option.isSome((yield* f.store.load(binding)).record)).toBe(true);
                expect(yield* f.rows).toBe(1);

                if (kind === "routed") expect(yield* f.routeState).toBe("live");

                yield* Projection.forget(binding).pipe(f.on(f.right));
                expect((yield* f.store.load(binding)).token).toBe("0");
                expect(yield* f.rows).toBe(0);

                if (kind === "routed") expect(yield* f.routeState).toBe("forgotten");
              }),
            ),
          TIMEOUT_MS,
        );

        test(
          `${kind}: retire conflicts with a first unit committed before its tombstone`,
          () =>
            run((f) =>
              Effect.gen(function* () {
                const gate = yield* holdStatements(isCheckpointInsert);
                const retire = Projection.retire(spec, tenant, { generation: 1 });

                const raced = yield* retire.pipe(
                  gate.hold,
                  f.on(f.right),
                  Effect.flip,
                  Effect.forkScoped,
                );

                yield* gate.reached();
                yield* Projection.run(binding, { limit: 1 }).pipe(f.on(f.left));
                yield* gate.release();
                expect(yield* Fiber.join(raced)).toMatchObject(tokenConflict);
                expect(Option.isSome((yield* f.store.load(binding)).record)).toBe(true);
                expect(yield* f.rows).toBe(1);

                expect((yield* retire.pipe(f.on(f.right))).record).toBe("retired");
                expect(yield* f.rows).toBe(0);
              }),
            ),
          TIMEOUT_MS,
        );

        test(
          `${kind}: retire beside an open first unit keeps the checkpoint and rows together`,
          () =>
            run((f) =>
              Effect.gen(function* () {
                const gate = yield* holdStatements(isCheckpointInsert);
                const retire = Projection.retire(spec, tenant, { generation: 1 });

                const unit = yield* Projection.run(binding, { limit: 1 }).pipe(
                  gate.hold,
                  f.on(f.left),
                  Effect.exit,
                  Effect.forkScoped,
                );

                yield* gate.reached();
                const raced = yield* retire.pipe(f.on(f.right), Effect.exit, Effect.forkScoped);
                // On MySQL retire waits for the unit's rows. On Postgres it can finish.
                yield* Effect.sleep("300 millis");
                yield* gate.release();
                const unitExit = yield* Fiber.join(unit);
                const retireExit = yield* Fiber.join(raced);
                const loaded = yield* f.store.load(binding).pipe(Effect.exit);
                const live = Exit.isSuccess(loaded) && Option.isSome(loaded.value.record);

                expect([unitExit._tag, retireExit._tag]).toEqual(
                  live ? ["Success", "Failure"] : ["Failure", "Success"],
                );
                expect(yield* f.rows).toBe(live ? 1 : 0);

                expect((yield* retire.pipe(f.on(f.right))).record).toBe(
                  live ? "retired" : "already-retired",
                );
                expect(yield* f.rows).toBe(0);
                expect(yield* f.store.load(binding).pipe(Effect.flip)).toMatchObject({
                  reason: "retired-generation",
                });
              }),
            ),
          TIMEOUT_MS,
        );

        test(
          `${kind}: a runner's forget after a gone input beside a recreated binding's first unit`,
          () =>
            run((f) =>
              Effect.gen(function* () {
                yield* Projection.run(binding).pipe(f.on(f.left));
                yield* f.source.remove({});
                const gate = yield* holdStatements(afterSettle(), isCleanup);

                const runner = yield* Projection.run(binding).pipe(
                  gate.hold,
                  f.on(f.right),
                  Effect.exit,
                  Effect.forkScoped,
                );

                yield* gate.reached(0);
                expect(Option.getOrThrow((yield* f.store.load(binding)).record).ended).toBe(true);

                // Another host forgets the ended binding first, so the runner loads no checkpoint.
                yield* Projection.forget(binding).pipe(f.on(f.left));
                yield* gate.release(0);
                yield* gate.reached(1);
                yield* f.source.create({});
                yield* f.source.append({}, [2]);

                if (kind === "routed") yield* f.output.remove(route);

                const unit = yield* Projection.run(binding, { limit: 1 }).pipe(
                  f.on(f.left),
                  Effect.exit,
                  Effect.forkScoped,
                );

                // The runner has locked the forgotten route record, so a routed unit waits for it.
                if (kind === "routed") yield* Effect.sleep("300 millis");
                else yield* Fiber.await(unit);
                yield* gate.release(1);
                const runnerExit = yield* Fiber.join(runner);
                const unitExit = yield* Fiber.join(unit);

                if (kind === "plain") {
                  expect(unitExit._tag).toBe("Success");
                  expect(
                    Exit.isFailure(runnerExit) && Cause.squash(runnerExit.cause),
                  ).toMatchObject(tokenConflict);
                }

                const live = Option.isSome((yield* f.store.load(binding)).record);
                expect(live).toBe(Exit.isSuccess(unitExit));
                expect(yield* f.rows).toBe(live ? 1 : 0);

                // The runner's next pass finds the new unit, or runs it.
                yield* Projection.run(binding).pipe(f.on(f.right));
                expect(yield* f.rows).toBe(1);
                expect(Option.isSome((yield* f.store.load(binding)).record)).toBe(true);

                if (kind === "routed") {
                  expect(yield* f.routeState).toBe("live");
                  expect((yield* f.output.collect(route)).items).toEqual([2]);
                }
              }),
            ),
          TIMEOUT_MS,
        );
      }

      test(
        "routed forget that loaded no checkpoint rolls back when a unit commits before its route listing",
        () =>
          run((f) =>
            Effect.gen(function* () {
              const binding = routed.at(tenant);
              const gate = yield* holdStatements(isRouteList);

              const forget = yield* Projection.forget(binding).pipe(
                gate.hold,
                f.on(f.right),
                Effect.flip,
                Effect.forkScoped,
              );

              yield* gate.reached();
              yield* Projection.run(binding, { limit: 1 }).pipe(f.on(f.left));
              yield* gate.release();
              expect(yield* Fiber.join(forget)).toMatchObject(tokenConflict);
              expect(Option.isSome((yield* f.store.load(binding)).record)).toBe(true);
              expect(yield* f.routeState).toBe("live");
              expect(yield* f.rows).toBe(1);

              yield* f.source.append({}, [2]);
              yield* f.sql`DELETE FROM unit_rows`;
              yield* Projection.run(binding).pipe(f.on(f.left));
              expect((yield* f.output.collect(route)).items).toEqual([1, 2]);
            }),
          ),
        TIMEOUT_MS,
      );

      test(
        "routed forget of an absent checkpoint marks every listed route forgotten",
        () =>
          run((f) =>
            Effect.gen(function* () {
              const binding = routed.at(tenant);
              yield* Projection.run(binding).pipe(f.on(f.left));
              // A bare key deletes the checkpoint and leaves the route live.
              yield* Projection.forget({ ...Projection.key(binding), commit: binding.commit }).pipe(
                f.on(f.left),
              );
              expect((yield* f.store.load(binding)).token).toBe("0");
              expect(yield* f.routeState).toBe("live");

              yield* Projection.forget(binding).pipe(f.on(f.right));
              expect((yield* f.store.load(binding)).token).toBe("0");
              expect(yield* f.routeState).toBe("forgotten");
            }),
          ),
        TIMEOUT_MS,
      );

      if (level.dialect.name === "mysql") {
        for (const race of [false, true]) {
          test(
            `routed forget of an absent checkpoint with a legacy hashed route key ${race ? "rolls back when a unit commits before its route listing" : "leaves the route as it is"}`,
            () =>
              run((f) =>
                Effect.gen(function* () {
                  const params = { tenant: "x".repeat(600) };
                  const binding = routed.at(params);
                  const state = f.store.routes!.state(binding, "out", Output.at(route).id);
                  yield* Projection.run(binding).pipe(f.on(f.left));
                  yield* Projection.forget({
                    ...Projection.key(binding),
                    commit: binding.commit,
                  }).pipe(f.on(f.left));
                  const prefix = '["streamsy.projection.routed.v1",%';
                  yield* f.sql`UPDATE streamsy_owner_records SET full_key = NULL WHERE owner_key LIKE ${prefix}`;
                  expect(yield* f.store.routes!.list(binding).pipe(Effect.flip)).toMatchObject({
                    reason: "unsupported-composition",
                  });

                  const snapshot = f.sql`SELECT * FROM streamsy_owner_records ORDER BY owner_key`;
                  const before = yield* snapshot;
                  expect(before).toHaveLength(1);

                  if (race) {
                    // A second legacy key makes the checkpoint's own range unlistable. The plain
                    // read of the second load still finds the checkpoint at the default level.
                    if (!level.repeatableRead)
                      yield* f.sql`INSERT INTO streamsy_owner_records ${f.sql.insert({
                        owner_key: `${recordKey(binding).slice(0, 447)}#${"a".repeat(64)}`,
                        owner_value: "{}",
                      })}`;

                    const gate = yield* holdStatements(isRouteList);

                    const forget = yield* Projection.forget(binding).pipe(
                      gate.hold,
                      f.on(f.right),
                      Effect.flip,
                      Effect.forkScoped,
                    );

                    yield* gate.reached();
                    // The live route record lets the unit append to the stream it already owns.
                    yield* Projection.run(binding, { limit: 1 }).pipe(f.on(f.left));
                    yield* gate.release();
                    expect(yield* Fiber.join(forget)).toMatchObject(tokenConflict);
                    expect(Option.isSome((yield* f.store.load(binding)).record)).toBe(true);
                    expect(yield* f.rows).toBe(1);
                  } else {
                    yield* Projection.forget(binding).pipe(f.on(f.right));
                    expect(yield* snapshot).toEqual(before);
                    expect((yield* f.store.load(binding)).token).toBe("0");
                  }

                  expect(yield* state).toBe("live");
                }),
              ),
            TIMEOUT_MS,
          );
        }
      }
    });
  }
};
