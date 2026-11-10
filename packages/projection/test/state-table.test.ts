import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, StreamAddress, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { Protocol, Streams } from "@streamsy/core/protocol";
import { bindingOf } from "../src/runner.ts";
import { Change, message } from "../src/state-table-message.ts";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  StateTable,
  Transaction,
} from "@streamsy/projection";
import { layerSqlite } from "./support/sqlite.ts";

const Row = Schema.Struct({ id: Schema.String, count: Schema.Finite });

class Workspace extends Streamsy.state<Workspace>()("StateTable/Workspace", "state-table/:id", {
  params: { id: Schema.String },
  collections: { issue: { schema: Row, key: "id" }, note: { schema: Row, key: "id" } },
}) {}

class Dates extends Streamsy.state<Dates>()("StateTable/Dates", "state-table-dates", {
  params: {},
  collections: {
    date: { schema: Schema.Struct({ id: Schema.String, date: Schema.Date }), key: "id" },
  },
}) {}

class Facts extends Streamsy.json<Facts>()("StateTable/Facts", "state-table-facts", {
  params: {},
  schema: Row,
}) {}

class Numbered extends Streamsy.state<Numbered>()(
  "StateTable/Numbered",
  "state-table-numbered/:id",
  {
    params: { id: Schema.FiniteFromString },
    collections: { issue: { schema: Row, key: "id" } },
  },
) {}

const WorkspaceTable = StateTable.define(Workspace);

const DateTable = StateTable.define(Dates);

type Event = StreamAddress.CollectionsEvent<typeof Workspace.collections>;

type Entry = DurableState.Entry<typeof Workspace.collections>;

const host = () => {
  const local = layerSqlite(3);

  const streams = Protocol.bind(local, Streamsy.group(Workspace, Dates, Facts, Numbered)).pipe(
    Layer.provideMerge(local),
  );

  return Layer.effectDiscard(Effect.flatMap(SqlClient.SqlClient, StateTable.migrate)).pipe(
    Layer.provideMerge(streams),
  );
};

const transaction = <A, E, R>(body: Effect.Effect<A, E, R>) =>
  Effect.flatMap(Transaction, (tx) => tx.withTransaction(body));

const eventually = <A, E, R>(read: Effect.Effect<A, E, R>, check: (value: A) => boolean) =>
  Effect.gen(function* () {
    for (let turn = 0; turn < 1000; turn += 1) {
      const value = yield* read;

      if (check(value)) return value;
      yield* Effect.yieldNow;
    }

    return yield* read;
  });

const upsert = (id: string, count: number): Entry => DurableState.upsert("issue", { id, count });

const database = (id: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    const rows =
      yield* sql`SELECT * FROM streamsy_state_rows WHERE stream_id = ${id} ORDER BY collection, row_key`;

    const heads = yield* sql`SELECT * FROM streamsy_state_heads WHERE stream_id = ${id}`;

    return { rows, heads };
  });

// Seeded histories give reproducible random operations and append/page boundaries.
const history = (seed: number): ReadonlyArray<ReadonlyArray<Entry>> => {
  let random = seed;

  const next = (n: number) => {
    random = (Math.imul(random, 1664525) + 1013904223) >>> 0;

    return random % n;
  };

  const groups: Entry[][] = [[upsert("old", 1)], [StreamAddress.snapshotStart()]];

  for (let i = 0; i < 100; i += 1) {
    const entries: Entry[] = [];

    for (let j = 0, size = next(7) + 1; j < size; j += 1) {
      const key = String(next(17));
      const collection = next(2) === 0 ? "issue" : "note";

      switch (next(12)) {
        case 0:
          entries.push(StreamAddress.reset());
          break;
        case 1:
          entries.push(StreamAddress.snapshotStart());
          break;
        case 2:
          entries.push(StreamAddress.snapshotEnd());
          break;
        case 3:
          entries.push(DurableState.remove(collection, key));
          break;
        default:
          entries.push(DurableState.upsert(collection, { id: key, count: next(10000) }));
      }
    }

    groups.push(entries);
  }

  groups.push([StreamAddress.snapshotEnd()]);

  return groups;
};

for (const mode of ["run", "durable", "memory"] as const) {
  layer(host())((it) => {
    it.effect(
      `random histories agree with materialize under ${mode}`,
      () =>
        Effect.gen(function* () {
          if (mode !== "run")
            yield* Layer.build(Projection.attach(WorkspaceTable, { durable: mode === "durable" }));
          const stream = yield* Workspace;

          for (let seed = 1; seed <= 20; seed += 1) {
            const params = { id: `${mode}-${seed}` };
            const table = WorkspaceTable.at(params);
            const address = Workspace.at(params);
            yield* stream.create(params);
            let expected = DurableState.materialized(address);

            for (const entries of history(seed)) {
              const changes = DurableState.changes(address, {}, entries);
              expected = DurableState.materialize(expected, changes);

              if (!Array.isReadonlyArrayNonEmpty(changes)) continue;
              const written = yield* stream.append(params, changes);

              if (mode === "run") yield* Projection.run(table);
              else
                expect(
                  yield* eventually(table.offset(), (offset) => offset === written.offset),
                ).toBe(written.offset);

              for (const collection of ["issue", "note"] as const) {
                const actual = yield* table.list(collection);
                expect(new Map(Array.map(actual.rows, ({ key, value }) => [key, value]))).toEqual(
                  expected.rows[collection],
                );
              }
            }
          }
        }),
      30000,
    );
  });
}

layer(host())((it) => {
  it.effect(
    "transformed State parameters work with durable attachment and pending-mark decoding",
    () =>
      Effect.gen(function* () {
        const declaration = StateTable.define(Numbered);
        const table = declaration.at({ id: 42 });
        const bound = bindingOf(declaration, '{"id":"42"}');
        expect(Option.isSome(bound)).toBe(true);

        if (Option.isSome(bound)) expect(bound.value.inputs.state.id).toBe(table.inputs.state.id);
        expect(Option.isNone(bindingOf(declaration, '{"id":"bad"}'))).toBe(true);
        yield* Layer.build(Projection.attach(declaration, { durable: true }));
        const stream = yield* Numbered;
        yield* stream.create({ id: 42 });

        const result = yield* stream.append(
          { id: 42 },
          DurableState.changes(Numbered.at({ id: 42 }), {}, [
            DurableState.upsert("issue", { id: "a", count: 42 }),
          ]),
        );

        expect(yield* eventually(table.offset(), (offset) => offset === result.offset)).toBe(
          result.offset,
        );
        expect((yield* table.get("issue", "a")).value).toEqual({ id: "a", count: 42 });
      }),
  );

  it.effect(
    "snapshot epochs persist across pages, repeated starts, resets and unmatched ends",
    () =>
      Effect.gen(function* () {
        const params = { id: "snapshot-pages" };
        const stream = yield* Workspace;
        const table = WorkspaceTable.at(params);
        const address = Workspace.at(params);
        yield* stream.create(params);
        let expected = DurableState.materialized(address);

        const groups = [
          [upsert("old", 1), upsert("kept", 2), StreamAddress.snapshotStart()],
          [
            upsert("kept", 3),
            DurableState.upsert("note", { id: "note", count: 4 }),
            upsert("deleted", 5),
          ],
          [DurableState.remove("issue", "deleted"), StreamAddress.snapshotEnd()],
          [StreamAddress.snapshotEnd()],
          [StreamAddress.snapshotStart(), upsert("discarded", 6), StreamAddress.snapshotStart()],
          [upsert("final", 7), StreamAddress.snapshotEnd()],
          [StreamAddress.snapshotStart(), upsert("reset", 8), StreamAddress.reset()],
          [upsert("after-reset", 9), StreamAddress.snapshotEnd()],
        ] as const;

        for (const entries of groups) {
          const events = DurableState.changes(address, {}, entries);
          expected = DurableState.materialize(expected, events);
          yield* stream.append(params, events);
          yield* Projection.run(table);
          const state = yield* database(address.id);
          expect(state.heads[0]).toMatchObject({
            snapshot_open: expected.snapshot === undefined ? 0 : 1,
          });

          for (const collection of ["issue", "note"] as const)
            expect(
              new Map(
                Array.map((yield* table.list(collection)).rows, ({ key, value }) => [key, value]),
              ),
            ).toEqual(expected.rows[collection]);
        }
      }),
  );

  it.effect(
    "stored values are canonical JSON: whitespace, escapes, scalar spelling and duplicate keys are resolved",
    () =>
      Effect.gen(function* () {
        const params = { id: "raw-json" };
        const stream = yield* Workspace;
        const table = WorkspaceTable.at(params);
        const sql = yield* SqlClient.SqlClient;
        yield* stream.create(params);

        const values = [
          '{ "id":"a", "count":1e2, "nested": [true, null, {"value":"}\\\""}] }',
          '"string\\u0020value"',
          "1.00e+2",
          "false",
          "null",
          '[1, {"value": [2,3]}]',
        ];

        const expectedValues: string[] = [];

        for (const [index, value] of values.entries()) {
          const json = `{"value":"ignored","headers":{"operation":"insert"},"type":"issue","key":"${index}","v\\u0061lue": ${value} }`;
          const codec = Schema.fromJsonString(Schema.Unknown);

          const canonical = yield* Schema.encodeEffect(codec)(
            yield* Schema.decodeEffect(codec)(value),
          );

          expect(yield* message(json)).toEqual(
            Change.Upsert({ collection: "issue", key: String(index), value: canonical }),
          );
          expectedValues.push(canonical);
          yield* stream.wire.append(table.inputs.state.id, {
            data: new TextEncoder().encode(json),
            contentType: "application/json",
          });
        }

        yield* Projection.run(table);

        const actual = yield* sql<{
          value: string;
        }>`SELECT value FROM streamsy_state_rows WHERE stream_id = ${table.inputs.state.id} ORDER BY row_key`;

        expect(Array.map(actual, (row) => row.value)).toEqual(expectedValues);
        const before = yield* database(table.inputs.state.id);
        yield* StateTable.rebuild(table);
        expect(yield* database(table.inputs.state.id)).toEqual(before);
      }),
  );

  it.effect("a failed rebuild restores the old index and head", () =>
    Effect.gen(function* () {
      const params = { id: "failed-rebuild" };
      const table = WorkspaceTable.at(params);
      const stream = yield* Workspace;
      const sql = yield* SqlClient.SqlClient;
      yield* stream.create(params);
      yield* stream.append(
        params,
        DurableState.changes(Workspace.at(params), {}, [upsert("before", 1)]),
      );
      yield* Projection.run(table);
      const before = yield* database(table.inputs.state.id);
      const store = yield* CheckpointStore;
      const checkpoint = yield* store.load(table);
      yield* sql.unsafe(
        "CREATE TRIGGER fail_rebuild BEFORE INSERT ON streamsy_state_rows WHEN NEW.row_key = 'before' BEGIN SELECT RAISE(ABORT, 'rebuild failure'); END",
      );
      expect(Exit.isFailure(yield* StateTable.rebuild(table).pipe(Effect.exit))).toBe(true);
      yield* sql.unsafe("DROP TRIGGER fail_rebuild");
      expect(yield* database(table.inputs.state.id)).toEqual(before);
      expect(yield* store.load(table)).toEqual(checkpoint);
    }),
  );

  it.effect("migrate is idempotent, declarations bind params, and missing heads read empty", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* StateTable.migrate(sql);
      const table = WorkspaceTable.at({ id: "empty" });
      expect(WorkspaceTable.inputs.state).toBe(Workspace);
      expect(table.id).toBe(`state-table/${Workspace.key}`);
      expect(table.commit).toBe(Commit.transaction);
      expect(table.params).toEqual({ id: "empty" });
      expect(WorkspaceTable.parse(table.inputs.state.id)).toEqual(Option.some({ id: "empty" }));
      expect(yield* table.offset()).toBe(ZERO_OFFSET);
      expect(yield* table.get("issue", "missing")).toEqual({
        offset: ZERO_OFFSET,
        value: undefined,
      });
      expect(yield* table.getMany("issue", ["missing"])).toEqual({
        offset: ZERO_OFFSET,
        rows: new Map(),
      });
      expect(yield* table.list("issue")).toEqual({
        offset: ZERO_OFFSET,
        rows: [],
        next: undefined,
      });
    }),
  );

  it.effect("sync covers whole streams, small gaps, and repeated calls without a table run", () =>
    Effect.gen(function* () {
      const params = { id: "sync" };
      const stream = yield* Workspace;
      const table = WorkspaceTable.at(params);
      yield* stream.create(params);

      for (let i = 0; i < 3; i += 1) {
        const events = DurableState.changes(
          Workspace.at(params),
          {},
          Array.makeBy(i === 0 ? 10 : 1, (j) => upsert(String(j), i + j)),
        );

        if (!Array.isReadonlyArrayNonEmpty(events)) throw new Error("Expected changes");
        const tail = yield* stream.append(params, events);
        yield* transaction(table.sync());
        expect(yield* table.offset()).toBe(tail.offset);
        expect((yield* table.get("issue", "0")).value?.count).toBe(i);
      }

      const before = yield* database(table.inputs.state.id);
      yield* transaction(table.sync());
      expect(yield* database(table.inputs.state.id)).toEqual(before);
      expect((yield* Projection.run(table)).items).toBe(12);
      expect(yield* database(table.inputs.state.id)).toEqual(before);
    }),
  );

  it.effect("sync and rebuild over a missing stream fail with a history-unavailable fault", () =>
    Effect.gen(function* () {
      const table = WorkspaceTable.at({ id: "never-created" });

      const missing = {
        _tag: "Failure",
        failure: { _tag: "ProjectionFault", phase: "read", reason: "history-unavailable" },
      };

      expect(yield* transaction(table.sync()).pipe(Effect.result)).toMatchObject(missing);
      expect(yield* StateTable.rebuild(table).pipe(Effect.result)).toMatchObject(missing);
      expect(yield* table.get("issue", "a")).toEqual({ offset: ZERO_OFFSET, value: undefined });
    }),
  );

  it.effect("sync clears a head past the tail after stream deletion and recreation", () =>
    Effect.gen(function* () {
      const params = { id: "recreated" };
      const stream = yield* Workspace;
      const table = WorkspaceTable.at(params);
      yield* stream.create(params);
      yield* stream.append(
        params,
        DurableState.changes(Workspace.at(params), {}, [
          upsert("old", 1),
          upsert("old", 2),
          upsert("old", 3),
        ]),
      );
      yield* transaction(table.sync());
      yield* stream.remove(params);
      yield* stream.create(params);
      yield* stream.append(
        params,
        DurableState.changes(Workspace.at(params), {}, [upsert("new", 4)]),
      );
      yield* transaction(table.sync());
      expect((yield* table.list("issue")).rows).toEqual([
        { key: "new", value: { id: "new", count: 4 } },
      ]);
    }),
  );

  it.effect(
    "the two-case head rule covers equal, missing zero, inside, missing nonzero, and past a slice",
    () =>
      Effect.gen(function* () {
        const stream = yield* Workspace;
        const sql = yield* SqlClient.SqlClient;

        for (const scenario of [
          "equal",
          "missing-zero",
          "inside",
          "missing-nonzero",
          "past",
        ] as const) {
          const params = { id: scenario };
          const table = WorkspaceTable.at(params);
          yield* stream.create(params);
          yield* stream.append(
            params,
            DurableState.changes(Workspace.at(params), {}, [upsert("a", 1)]),
          );

          if (scenario === "equal" || scenario === "missing-nonzero") yield* Projection.run(table);

          if (scenario === "inside") yield* transaction(table.sync());

          if (scenario === "missing-nonzero") {
            yield* sql`DELETE FROM streamsy_state_rows WHERE stream_id = ${table.inputs.state.id}`;
            yield* sql`DELETE FROM streamsy_state_heads WHERE stream_id = ${table.inputs.state.id}`;
          }

          yield* stream.append(
            params,
            DurableState.changes(Workspace.at(params), {}, [
              upsert("b", 2),
              upsert("c", 3),
              upsert("d", 4),
            ]),
          );

          if (scenario === "past") yield* transaction(table.sync());
          let reads = 0;

          const patched = {
            ...stream,
            wire: {
              ...stream.wire,
              read: (...args: Parameters<typeof stream.wire.read>) => {
                reads += 1;

                return stream.wire.read(...args);
              },
            },
          };

          yield* Projection.run(table).pipe(Effect.provideService(Workspace, patched));
          expect((yield* table.list("issue")).rows.length).toBe(4);
          // Three runner reads (two pages plus caught-up). Only mismatch cases replay the stream too.
          expect(reads).toBe(
            scenario === "equal"
              ? 2
              : scenario === "inside" || scenario === "missing-nonzero"
                ? 4
                : 3,
          );
        }
      }),
  );

  it.effect("failures during apply and checkpoint save roll back rows, head and checkpoint", () =>
    Effect.gen(function* () {
      const params = { id: "atomic" };
      const stream = yield* Workspace;
      const table = WorkspaceTable.at(params);
      const sql = yield* SqlClient.SqlClient;
      const store = yield* CheckpointStore;
      yield* stream.create(params);
      yield* stream.append(
        params,
        DurableState.changes(Workspace.at(params), {}, [upsert("a", 1)]),
      );
      yield* Projection.run(table);
      const before = yield* database(table.inputs.state.id);
      const checkpoint = yield* store.load(table);
      yield* stream.append(
        params,
        DurableState.changes(Workspace.at(params), {}, [
          StreamAddress.snapshotStart(),
          upsert("b", 2),
          upsert("c", 3),
        ]),
      );
      yield* sql.unsafe(
        "CREATE TRIGGER fail_state_row BEFORE INSERT ON streamsy_state_rows WHEN NEW.row_key = 'c' BEGIN SELECT RAISE(ABORT, 'test failure'); END",
      );
      expect(Exit.isFailure(yield* Projection.run(table).pipe(Effect.exit))).toBe(true);
      expect(yield* database(table.inputs.state.id)).toEqual(before);
      expect(yield* store.load(table)).toEqual(checkpoint);
      yield* sql.unsafe("DROP TRIGGER fail_state_row");

      const failure = new ProjectionFault({
        phase: "checkpoint",
        reason: "storage-failure",
        message: "save failed",
      });

      expect(
        Exit.isFailure(
          yield* Projection.run(table).pipe(
            Effect.provideService(CheckpointStore, {
              ...store,
              compareAndSet: () => Effect.fail(failure),
            }),
            Effect.exit,
          ),
        ),
      ).toBe(true);
      expect(yield* database(table.inputs.state.id)).toEqual(before);
      expect(yield* store.load(table)).toEqual(checkpoint);
      yield* Projection.run(table);
      expect((yield* table.list("issue")).rows.length).toBe(3);
    }),
  );

  it.effect(
    "rebuild is byte identical after deletes and resets and leaves the checkpoint alone",
    () =>
      Effect.gen(function* () {
        const params = { id: "rebuild" };
        const stream = yield* Workspace;
        const table = WorkspaceTable.at(params);
        const store = yield* CheckpointStore;
        yield* stream.create(params);

        const entries = [
          upsert("a", 1),
          StreamAddress.reset(),
          upsert("a", 2),
          upsert("b", 3),
          DurableState.remove("issue", "b"),
          StreamAddress.snapshotStart(),
          upsert("c", 4),
          StreamAddress.snapshotEnd(),
        ] as const;

        yield* stream.append(params, DurableState.changes(Workspace.at(params), {}, entries));
        yield* Projection.run(table, { limit: 1 });
        const checkpoint = yield* store.load(table);
        yield* StateTable.rebuild(table);
        const before = yield* database(table.inputs.state.id);
        expect(yield* store.load(table)).toEqual(checkpoint);
        yield* Projection.run(table);
        expect(yield* database(table.inputs.state.id)).toEqual(before);
        const applied = yield* store.load(table);
        yield* StateTable.rebuild(table);
        expect(yield* database(table.inputs.state.id)).toEqual(before);
        expect(yield* store.load(table)).toEqual(applied);
        expect((yield* Projection.run(table)).items).toBe(0);
        yield* StateTable.reset(table);
        expect((yield* table.list("issue")).rows.length).toBe(1);
        yield* Projection.run(table);
        expect((yield* table.list("issue")).rows).toEqual([]);
      }),
  );

  it.effect("paging returns each key once; following from the first page's offset converges", () =>
    Effect.gen(function* () {
      const params = { id: "paging" };
      const stream = yield* Workspace;
      const table = WorkspaceTable.at(params);
      const address = Workspace.at(params);
      yield* stream.create(params);
      const entries = Array.makeBy(113, (i) => upsert(String(i).padStart(3, "0"), i));
      yield* stream.append(params, DurableState.changes(address, {}, entries));
      yield* Projection.run(table);
      const keys: string[] = [];
      let after: string | undefined;

      do {
        const page = yield* table.list("issue", { after, limit: 7 });
        keys.push(...Array.map(page.rows, (row) => row.key));
        after = page.next;
      } while (after !== undefined);

      expect(keys).toEqual(Array.map(entries, (_, i) => String(i).padStart(3, "0")));
      const many = yield* table.getMany("issue", [...keys, "missing", keys[0] ?? ""]);
      expect(many.rows.size).toBe(113);
      expect((yield* table.get("issue", "missing")).value).toBeUndefined();
      const first = yield* table.list("issue", { limit: 7 });

      const changes = DurableState.changes(address, {}, [
        upsert("000", 999),
        DurableState.remove("issue", "008"),
        upsert("200", 200),
      ]);

      yield* stream.append(params, changes);
      yield* Projection.run(table);
      const second = yield* table.list("issue", { after: first.next });
      const initial = DurableState.materialized(address);

      const rows: Event[] = Array.map([...first.rows, ...second.rows], (row) => ({
        type: "issue",
        key: row.key,
        value: row.value,
        headers: { operation: "upsert" },
      }));

      const since = yield* stream
        .follow(params, { offset: first.offset })
        .pipe(Streams.items, Stream.take(changes.length), Stream.runCollect);

      const caughtUp = DurableState.materialize(DurableState.materialize(initial, rows), since);

      expect(caughtUp.rows.issue).toEqual(
        new Map(Array.map((yield* table.list("issue")).rows, ({ key, value }) => [key, value])),
      );
    }),
  );

  it.effect("Date values remain stored JSON and invalid current rows fail only when read", () =>
    Effect.gen(function* () {
      const stream = yield* Dates;
      const table = DateTable.at({});
      yield* stream.create({});
      const date = yield* Schema.decodeEffect(Schema.DateFromString)("2026-11-09T00:00:00.000Z");
      yield* stream.append(
        {},
        DurableState.changes(Dates.at({}), {}, [DurableState.upsert("date", { id: "date", date })]),
      );
      yield* Projection.run(table);
      expect((yield* table.get("date", "date")).value).toEqual({ id: "date", date });
      const sql = yield* SqlClient.SqlClient;
      expect(
        (yield* sql<{
          value: string;
        }>`SELECT value FROM streamsy_state_rows WHERE stream_id = ${table.inputs.state.id}`)[0]
          ?.value,
      ).toBe('{"id":"date","date":"2026-11-09T00:00:00.000Z"}');
      const invalid = '{ "id": "bad", "date": 42, "nested": [1e2, {"value":"\\\"}"}] }';
      yield* stream.wire.append(Dates.at({}).id, {
        contentType: "application/json",
        data: new TextEncoder().encode(
          `{"type":"date","key":"bad","value":${invalid},"headers":{"operation":"update"}}`,
        ),
      });
      yield* Projection.run(table);
      expect(
        (yield* sql<{
          value: string;
        }>`SELECT value FROM streamsy_state_rows WHERE stream_id = ${table.inputs.state.id} AND row_key = 'bad'`)[0]
          ?.value,
      ).toBe(
        yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(
          yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(invalid),
        ),
      );
      expect(Exit.isFailure(yield* table.get("date", "bad").pipe(Effect.exit))).toBe(true);
      expect(Exit.isFailure(yield* table.getMany("date", ["bad"]).pipe(Effect.exit))).toBe(true);
      expect(Exit.isFailure(yield* table.list("date").pipe(Effect.exit))).toBe(true);
      yield* StateTable.rebuild(table);
      expect(
        (yield* sql<{
          value: string;
        }>`SELECT value FROM streamsy_state_rows WHERE stream_id = ${table.inputs.state.id} AND row_key = 'bad'`)[0]
          ?.value,
      ).toBe(
        yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(
          yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(invalid),
        ),
      );
    }),
  );

  it.effect("an unmodified producer emit drives a table attached on the owner", () =>
    Effect.gen(function* () {
      yield* Layer.build(Projection.attach(WorkspaceTable, { durable: true }));
      const facts = yield* Facts;
      const params = { id: "producer" };
      const table = WorkspaceTable.at(params);
      const output = Workspace.at(params);
      yield* (yield* Workspace).create(params);

      const emitted = Projection.emit({
        id: "state-table-producer",
        params: {},
        inputs: { facts: Facts },
        outputs: { workspace: output },
        commit: Commit.producer,
        emit: ({ item }) => ({ workspace: [DurableState.upsert("issue", item)] }),
      }).at({});

      yield* facts.create({});
      yield* facts.append(
        {},
        Array.makeBy(40, (i) => ({ id: String(i % 7), count: i })),
      );
      yield* Projection.run(emitted);
      const stream = yield* Workspace;
      const tail = yield* stream.head(params);
      expect(yield* eventually(table.offset(), (offset) => offset === tail.nextOffset)).toBe(
        tail.nextOffset,
      );
      const events = yield* stream.collect(params);
      const expected = DurableState.materialize(DurableState.materialized(output), events.items);
      expect(
        new Map(Array.map((yield* table.list("issue")).rows, ({ key, value }) => [key, value])),
      ).toEqual(expected.rows.issue);
    }),
  );
});
