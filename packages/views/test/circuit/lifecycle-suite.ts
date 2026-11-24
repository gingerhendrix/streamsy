/* oxlint-disable effecttsgo/strict-effect-provide -- Each test provisions its own database. */
import { expect, it } from "@effect/vitest";
import { Array, Effect, Layer, Option, Order, Record, Schema } from "effect";
import { SqlClient, SqlError } from "effect/sql";
import {
  circuitHost,
  inBackend,
  memoryDump,
  migrateReader,
  migrateViews,
  readSql,
  type Fixture,
} from "../support/memory.ts";
import { withTestReader } from "../support/backends.ts";
import { DurableState, StreamAddress, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { NonRetryable, Projection, StateTable } from "@streamsy/projection";
import { PlanChanged, View, ViewBackend } from "../../src/index.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- Inject a failure after the real memory backend clears its state.
import { currentBackend, makeViewBackend } from "../../src/view-backend.ts";
import { compilePlan } from "../../src/relation.ts";
import { keyExpression } from "../../src/expression.ts";
import { asRowKey, canonicalJson, encodeRowKey } from "../../src/runtime/key.ts";
import { fullRecompute } from "../support/full-recompute.ts";
import {
  Shop,
  group,
  Results,
  Table,
  DownstreamRows,
  changedViews,
  pinnedResults,
  circuit,
  views,
} from "./support/shop.ts";

const params = { shop: "a" };

const first = { ...params, generation: 1 };

const second = { ...params, generation: 2 };

const order = { orderId: "o1", customerId: "c1", total: 10 };

const customer = { customerId: "c1", name: "One", tier: "gold" };

const seed = Effect.gen(function* () {
  yield* migrateViews;
  yield* migrateViews;
  yield* migrateReader;
  const input = yield* Shop;
  yield* input.create(params);
  yield* input.append(
    params,
    DurableState.changes(Shop.at(params), {}, [
      DurableState.upsert("order", order),
      DurableState.upsert("customer", customer),
    ]),
  );
  yield* Projection.run(circuit().at(params));
});

const update = Effect.gen(function* () {
  yield* (yield* Shop).append(
    params,
    DurableState.changes(Shop.at(params), {}, [
      DurableState.upsert("order", { ...order, total: 20 }),
    ]),
  );
});

const failCleanup = (sql: SqlClient.SqlClient, name: string) =>
  sql.onDialectOrElse({
    pg: () =>
      Effect.gen(function* () {
        yield* sql`CREATE FUNCTION fail_views_cleanup() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'cleanup failed'; END; $$`;
        yield* sql`CREATE TRIGGER ${sql(name)} BEFORE DELETE ON streamsy_view_circuits
        FOR EACH ROW EXECUTE FUNCTION fail_views_cleanup()`;
      }),
    mysql: () =>
      Effect.gen(function* () {
        yield* sql`CREATE TABLE ${sql(name)} (
          ns BINARY(32) PRIMARY KEY, FOREIGN KEY (ns) REFERENCES streamsy_view_circuits (ns)
        ) ENGINE=InnoDB`;
        yield* sql`INSERT INTO ${sql(name)} (ns) SELECT ns FROM streamsy_view_circuits`;
      }),
    orElse: () =>
      Effect.asVoid(sql`CREATE TRIGGER ${sql(name)} BEFORE DELETE ON streamsy_view_circuits
      BEGIN SELECT RAISE(ABORT, 'cleanup failed'); END`),
  });

const restoreCleanup = (sql: SqlClient.SqlClient, name: string) =>
  sql.onDialectOrElse({
    pg: () => sql`DROP TRIGGER ${sql(name)} ON streamsy_view_circuits`,
    mysql: () => sql`DROP TABLE ${sql(name)}`,
    orElse: () => sql`DROP TRIGGER ${sql(name)}`,
  });

const withCleanupFailure = <A, E, R>(name: string, run: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const sql = yield* Effect.serviceOption(SqlClient.SqlClient);

    if (Option.isSome(sql)) {
      yield* failCleanup(sql.value, name);

      return yield* run.pipe(Effect.ensuring(restoreCleanup(sql.value, name).pipe(Effect.orDie)));
    }

    const backend = yield* currentBackend;

    const broken = makeViewBackend(
      Effect.succeed({
        ...backend,
        clear: (namespace) =>
          backend.clear(namespace).pipe(
            Effect.andThen(
              new SqlError.SqlError({
                reason: new SqlError.ConnectionError({
                  cause: undefined,
                  message: "cleanup failed",
                }),
              }),
            ),
          ),
      }),
    );

    return yield* run.pipe(Effect.provideService(ViewBackend, broken));
  });

const state = Effect.gen(function* () {
  const client = yield* Effect.serviceOption(SqlClient.SqlClient);

  if (Option.isNone(client)) return yield* memoryDump;

  const sql = client.value;

  const server = () =>
    Effect.gen(function* () {
      const hash = sql.onDialectOrElse({
        mysql: () => sql`LOWER(HEX(ns))`,
        orElse: () => sql`encode(ns, 'hex')`,
      });

      return {
        integrals:
          yield* sql`SELECT *, ${hash} AS namespace_id FROM streamsy_view_integrals ORDER BY ns, relation, row_hash`,
        arrangements:
          yield* sql`SELECT *, ${hash} AS namespace_id FROM streamsy_view_arrangements ORDER BY ns, arrangement, index_hash, ord_prefix, row_hash`,
        accumulators:
          yield* sql`SELECT *, ${hash} AS namespace_id FROM streamsy_view_accumulators ORDER BY ns, node, group_hash`,
        owners:
          yield* sql`SELECT *, ${hash} AS namespace_id FROM streamsy_view_key_owners ORDER BY ns, relation, output_hash`,
        circuits: yield* sql<{
          namespace: string;
          namespace_id: string;
          plan_hash: string;
        }>`SELECT *, ${hash} AS namespace_id FROM streamsy_view_circuits ORDER BY namespace`,
      };
    });

  return yield* sql.onDialectOrElse({
    pg: server,
    mysql: server,
    orElse: () =>
      Effect.gen(function* () {
        return {
          integrals:
            yield* sql`SELECT *, namespace AS namespace_id FROM streamsy_view_integrals ORDER BY namespace, relation, row_key`,
          arrangements:
            yield* sql`SELECT *, namespace AS namespace_id FROM streamsy_view_arrangements ORDER BY namespace, arrangement, index_key, sort_key, row_key`,
          accumulators:
            yield* sql`SELECT *, namespace AS namespace_id FROM streamsy_view_accumulators ORDER BY namespace, node, group_key`,
          owners:
            yield* sql`SELECT *, namespace AS namespace_id FROM streamsy_view_key_owners ORDER BY namespace, relation, output_key`,
          circuits: yield* sql<{
            namespace: string;
            namespace_id: string;
            plan_hash: string;
          }>`SELECT *, namespace AS namespace_id FROM streamsy_view_circuits ORDER BY namespace`,
        };
      }),
  });
});

const resultRows = <C extends keyof typeof Results.collections>(
  address: typeof first,
  collection: C,
) =>
  Effect.gen(function* () {
    const table = Table.at(address);
    const rows = yield* readSql(Projection.run(table).pipe(Effect.andThen(table.list(collection))));

    if (Option.isSome(rows))
      return new Map(Array.map(rows.value.rows, ({ key, value }) => [key, value] as const));

    const { items } = yield* (yield* Results).collect(address);
    const materialized = DurableState.materialize(DurableState.materialized(Results), items);

    return materialized.rows[collection];
  });

const checkRecompute = (
  address: typeof first,
  orders: ReadonlyArray<typeof order>,
  selected: typeof views,
) =>
  Effect.gen(function* () {
    const output = yield* Results;

    const result = DurableState.materialize(
      DurableState.materialized(Results),
      (yield* output.collect(address)).items,
    );

    const sources = {
      [canonicalJson([Shop.key, "order"])]: Array.map(orders, (row) => ({
        key: row.orderId,
        row,
      })),
      [canonicalJson([Shop.key, "customer"])]: [{ key: customer.customerId, row: customer }],
    };

    for (const [name, relation] of Record.toEntries(selected)) {
      const plan = compilePlan(name, {
        kind: "key",
        input: relation.expression,
        key: keyExpression(Results.collections[name].key),
      });

      const expected = fullRecompute({ plan, sources }).rows;
      expect(result.rows[name]).toEqual(new Map(Array.map(expected, ({ key, row }) => [key, row])));
    }

    return result.rows;
  });

const downstream = (sourceGeneration: number, generation: number) =>
  View.circuit({
    id: "downstream",
    generation,
    params: Shop.paramCodecs,
    output: Projection.perGeneration(DownstreamRows, "generation"),
    views: { goldOrder: View.from(pinnedResults(sourceGeneration), "goldOrder") },
  }).at(params);

const NumericRow = Schema.Struct({ id: Schema.Finite, n: Schema.Finite });

class Numeric extends Streamsy.state<Numeric>()("ViewsTest/Numeric", "numeric", {
  params: {},
  collections: { row: { schema: NumericRow, key: "id" } },
}) {}

class NumericResult extends Streamsy.state<NumericResult>()(
  "ViewsTest/NumericResult",
  "numeric/result/g/:generation",
  {
    params: { generation: Schema.FiniteFromString },
    collections: { row: { schema: NumericRow, key: "id" } },
  },
) {}

const numericCircuit = View.circuit({
  id: "numeric",
  params: {},
  output: Projection.perGeneration(NumericResult, "generation"),
  views: { row: View.from(Numeric, "row") },
});

const NumericTable = StateTable.define(NumericResult);

const numericIds = Effect.gen(function* () {
  const table = NumericTable.at({ generation: 1 });
  const rows = yield* readSql(Projection.run(table).pipe(Effect.andThen(table.list("row"))));

  if (Option.isSome(rows)) return Array.map(rows.value.rows, ({ value }) => value.id);

  const { items } = yield* (yield* NumericResult).collect({ generation: 1 });
  const materialized = DurableState.materialize(DurableState.materialized(NumericResult), items);

  return Array.sort(
    Array.map(Array.fromIterable(materialized.rows.row.values()), ({ id }) => id),
    Order.Number,
  );
});

const numericUpsert = (id: number, n: number) => DurableState.upsert("row", { id, n });

const WireKey = Schema.Union([Schema.String, Schema.Finite]);

class WireInput extends Streamsy.state<WireInput>()("ViewsTest/WireInput", "wire/input", {
  params: {},
  collections: {
    row: { schema: Schema.Struct({ id: Schema.String, target: WireKey }), key: "id" },
  },
}) {}

class WireOutput extends Streamsy.state<WireOutput>()(
  "ViewsTest/WireOutput",
  "wire/output/g/:generation",
  {
    params: { generation: Schema.FiniteFromString },
    collections: {
      row: { schema: Schema.Struct({ id: Schema.String, target: WireKey }), key: "target" },
    },
  },
) {}

export function registerLifecycle(fixture: Fixture) {
  const host = circuitHost(fixture, group);
  const numericHost = circuitHost(fixture, Streamsy.group(Numeric, NumericResult));

  it.live("a changed plan faults without changing operator state, output or checkpoint", () => {
    return Effect.gen(function* () {
      yield* seed;
      const before = yield* state;
      const checkpoint = yield* Projection.read(circuit().at(params));
      const head = yield* (yield* Results).head(first);
      yield* update;
      const fault = yield* Projection.run(circuit(1, true).at(params)).pipe(Effect.flip);
      expect(fault).toBeInstanceOf(PlanChanged);
      expect(yield* state).toEqual(before);
      expect(yield* Projection.read(circuit().at(params))).toEqual(checkpoint);
      expect((yield* (yield* Results).head(first)).nextOffset).toBe(head.nextOffset);
      yield* Projection.run(circuit().at(params));
      expect((yield* resultRows(first, "customerTotals")).get("c1")?.revenue).toBe(20);
    }).pipe(Effect.provide(host), withTestReader(fixture));
  });

  it.live(
    "a hot join key rolls back operators, output and checkpoint at the configured limit",
    () =>
      Effect.gen(function* () {
        yield* seed;
        const input = yield* Shop;
        yield* input.append(
          params,
          DurableState.changes(Shop.at(params), {}, [
            DurableState.upsert("order", { ...order, orderId: "o2" }),
            DurableState.upsert("order", { ...order, orderId: "o3" }),
            DurableState.upsert("order", { ...order, orderId: "o4" }),
          ]),
        );
        yield* Projection.run(circuit().at(params));
        const limited = circuit(1, false, 3).at(params);
        const before = yield* state;
        const checkpoint = yield* Projection.read(limited);
        const output = yield* Results;
        const head = yield* output.head(first);
        yield* input.append(
          params,
          DurableState.changes(Shop.at(params), {}, [
            DurableState.upsert("customer", { customerId: "c1", name: "Renamed", tier: "gold" }),
          ]),
        );

        const fault = yield* Projection.run(limited).pipe(Effect.flip);
        expect(fault).toMatchObject({
          _tag: "ViewDataFault",
          key: "c1",
          reason: 'maxAffectedKeys (3) exceeded at key "c1"',
          [NonRetryable]: true,
        });
        expect(yield* state).toEqual(before);
        expect(yield* Projection.read(limited)).toEqual(checkpoint);
        expect((yield* output.head(first)).nextOffset).toBe(head.nextOffset);

        expect((yield* Projection.run(circuit(1, false, 100).at(params))).units).toBe(1);
        expect((yield* resultRows(first, "goldOrder")).get("o1")?.customerName).toBe("Renamed");
      }).pipe(Effect.provide(host), withTestReader(fixture)),
  );

  it.live("rebuild drops filtered keys while generation-pinned readers keep their rows", () =>
    Effect.gen(function* () {
      yield* seed;
      const input = yield* Shop;
      const output = yield* Results;
      const small = { ...order, orderId: "o2", total: 1 };

      yield* input.append(
        params,
        DurableState.changes(Shop.at(params), {}, [DurableState.upsert("order", small)]),
      );
      yield* Projection.run(circuit().at(params));
      const before = yield* resultRows(first, "goldOrder");
      expect([...before.keys()]).toEqual(["o1", "o2"]);
      const oldStream = yield* output.collect(first);
      expect(yield* output.head(second).pipe(Effect.flip)).toMatchObject({
        _tag: "StreamNotFound",
      });

      yield* Projection.run(circuit(2, true).at(params));
      const recomputed = yield* checkRecompute(second, [order, small], changedViews);
      expect([...recomputed.goldOrder.keys()]).toEqual(["o1"]);
      expect(yield* output.collect(first)).toEqual(oldStream);
      const rerun = yield* readSql(Projection.run(Table.at(first)));

      if (Option.isSome(rerun)) expect(rerun.value.units).toBe(0);
      expect(yield* resultRows(first, "goldOrder")).toEqual(before);

      for (const name of Record.keys(Results.collections))
        expect(yield* resultRows(second, name)).toEqual(recomputed[name]);
    }).pipe(Effect.provide(host), withTestReader(fixture)),
  );

  it.live("rollback resumes the older checkpoint and updates its own stream", () =>
    Effect.gen(function* () {
      yield* seed;
      const output = yield* Results;
      const old = circuit().at(params);
      const serving = circuit(2, true).at(params);
      const checkpoint = yield* Projection.read(old);

      yield* Projection.run(serving);
      const current = yield* output.collect(second);
      expect(yield* Projection.read(old)).toEqual(checkpoint);
      expect((yield* state).circuits).toHaveLength(2);
      yield* update;

      expect(yield* Projection.run(old)).toMatchObject({ units: 1, items: 1 });
      expect((yield* Projection.read(old)).offsets[Shop.key]).toBe(
        (yield* (yield* Shop).head(params)).nextOffset,
      );
      yield* checkRecompute(first, [{ ...order, total: 20 }], views);
      expect(yield* output.collect(second)).toEqual(current);
    }).pipe(Effect.provide(host), withTestReader(fixture)),
  );

  it.live("a downstream circuit needs a raised generation to repin its input", () =>
    Effect.gen(function* () {
      yield* seed;
      const original = downstream(1, 1);
      yield* Projection.run(original);
      const before = yield* Projection.read(original);

      yield* Projection.run(circuit(2, true).at(params));
      const repinned = downstream(2, 1);
      expect(repinned.inputs[Results.key]?.id).toBe(Results.at(second).id);
      expect(yield* Projection.run(repinned).pipe(Effect.flip)).toMatchObject({
        phase: "load",
        reason: "identity-mismatch",
      });
      expect(yield* Projection.read(original)).toEqual(before);

      const rebuilt = downstream(2, 2);
      expect((yield* Projection.run(rebuilt)).units).toBeGreaterThan(0);
      const output = yield* DownstreamRows;

      const result = DurableState.materialize(
        DurableState.materialized(DownstreamRows),
        (yield* output.collect(second)).items,
      );

      expect(result.rows.goldOrder).toEqual(
        (yield* checkRecompute(second, [order], changedViews)).goldOrder,
      );
    }).pipe(Effect.provide(host), withTestReader(fixture)),
  );

  it.live("retirement atomically clears one generation and fences its checkpoint", () =>
    Effect.gen(function* () {
      yield* seed;
      const old = circuit().at(params);
      const oldNamespace = (yield* state).circuits[0]!.namespace_id;
      const declaration = circuit(2, true);
      const serving = declaration.at(params);
      yield* Projection.run(serving);

      const before = yield* state;
      const output = yield* Results;
      const oldStream = yield* output.collect(first);
      const oldCheckpoint = yield* Projection.read(old);
      const currentStream = yield* output.collect(second);
      const currentCheckpoint = yield* Projection.read(serving);
      const retire = Projection.retire(declaration, params, { generation: 1 });
      expect(yield* withCleanupFailure("fail_retire", retire.pipe(Effect.flip))).toMatchObject({
        phase: "checkpoint",
        reason: "storage-failure",
      });
      expect(yield* state).toEqual(before);
      expect(yield* Projection.read(old)).toEqual(oldCheckpoint);
      expect(yield* output.collect(first)).toEqual(oldStream);

      expect(yield* retire).toEqual({
        record: "retired",
        streams: [{ output: "output", id: Results.at(first).id, result: "removed" }],
      });
      const after = yield* state;

      for (const name of Record.keys(before)) {
        expect(Array.some(before[name], (row) => row.namespace_id === oldNamespace)).toBe(true);
        expect(after[name]).toEqual(
          Array.filter(before[name], (row) => row.namespace_id !== oldNamespace),
        );
      }

      expect(yield* output.head(first).pipe(Effect.flip)).toMatchObject({
        _tag: "StreamNotFound",
      });
      expect(yield* Projection.run(old).pipe(Effect.flip)).toMatchObject({
        phase: "load",
        reason: "retired-generation",
        retryable: false,
      });
      expect(yield* retire).toEqual({
        record: "already-retired",
        streams: [{ output: "output", id: Results.at(first).id, result: "absent" }],
      });
      expect(yield* state).toEqual(after);
      expect(yield* output.collect(second)).toEqual(currentStream);
      expect(yield* Projection.read(serving)).toEqual(currentCheckpoint);
    }).pipe(Effect.provide(host), withTestReader(fixture)),
  );

  it.live(
    "forget clears one namespace, preserves the output and replays into a fresh generation",
    () =>
      Effect.gen(function* () {
        yield* seed;
        const binding = circuit().at(params);
        const neighbor = circuit(2).at(params);
        const output = yield* Results;
        const before = yield* output.collect(first);
        const forgotten = (yield* state).circuits[0]!.namespace_id;
        yield* Projection.run(neighbor);

        const neighborOffset = yield* Projection.read(neighbor);
        const neighborOutput = yield* output.collect(second);
        const both = yield* state;

        const keepNeighbor = Effect.gen(function* () {
          const after = yield* state;

          for (const name of Record.keys(both)) {
            expect(Array.some(both[name], (row) => row.namespace_id === forgotten)).toBe(true);
            expect(after[name]).toEqual(
              Array.filter(both[name], (row) => row.namespace_id !== forgotten),
            );
          }

          expect(yield* Projection.read(neighbor)).toEqual(neighborOffset);
          expect(yield* output.collect(second)).toEqual(neighborOutput);
        });

        yield* Projection.forget(binding);
        yield* keepNeighbor;
        expect((yield* Projection.read(binding)).offsets).toEqual({ [Shop.key]: ZERO_OFFSET });
        expect(yield* output.collect(first)).toEqual(before);
        expect(yield* Projection.run(binding).pipe(Effect.flip)).toMatchObject({
          phase: "process",
          reason: "invalid-output",
          retryable: false,
        });
        yield* keepNeighbor;
        expect((yield* Projection.read(binding)).offsets).toEqual({ [Shop.key]: ZERO_OFFSET });
        yield* Projection.run(circuit(3).at(params));
        yield* checkRecompute({ ...params, generation: 3 }, [order], views);
      }).pipe(Effect.provide(host), withTestReader(fixture)),
  );

  it.live("forget rolls back its checkpoint deletion when state cleanup fails", () => {
    return Effect.gen(function* () {
      yield* seed;
      const before = yield* state;
      const checkpoint = yield* Projection.read(circuit().at(params));
      const forget = Projection.forget(circuit().at(params)).pipe(Effect.flip);
      expect(yield* withCleanupFailure("fail_cleanup", forget)).toMatchObject({
        phase: "checkpoint",
        reason: "storage-failure",
      });
      expect(yield* state).toEqual(before);
      expect(yield* Projection.read(circuit().at(params))).toEqual(checkpoint);
    }).pipe(Effect.provide(host), withTestReader(fixture));
  });

  it.live("failed output appends roll back operators and input checkpoints", () => {
    return Effect.gen(function* () {
      yield* seed;
      const before = yield* state;
      const checkpoint = yield* Projection.read(circuit().at(params));
      yield* (yield* Results).append(first, [], { close: true });
      yield* update;
      yield* Projection.run(circuit().at(params)).pipe(Effect.flip);
      expect(yield* state).toEqual(before);
      expect(yield* Projection.read(circuit().at(params))).toEqual(checkpoint);
    }).pipe(Effect.provide(host), withTestReader(fixture));
  });

  it.live("bindings with identical row keys remain independent", () => {
    return Effect.gen(function* () {
      yield* seed;
      const input = yield* Shop;
      const other = { shop: "b" };
      yield* input.create(other);
      yield* input.append(
        other,
        DurableState.changes(Shop.at(other), {}, [
          DurableState.upsert("order", { ...order, total: 99 }),
        ]),
      );
      yield* Projection.run(circuit().at(other));
      expect((yield* resultRows(first, "customerTotals")).get("c1")?.revenue).toBe(10);
      expect(
        (yield* resultRows({ ...other, generation: 1 }, "customerTotals")).get("c1")?.revenue,
      ).toBe(99);
      expect((yield* state).circuits).toHaveLength(2);
    }).pipe(Effect.provide(host), withTestReader(fixture));
  });

  it.live("reordering view declarations preserves the plan and operator identities", () => {
    return Effect.gen(function* () {
      yield* seed;
      yield* update;

      const reordered = View.circuit({
        id: "shop-views",
        params: { shop: Schema.String },
        output: Projection.perGeneration(Results, "generation"),
        views: {
          topOrder: views.topOrder,
          goldOrder: views.goldOrder,
          customerTotals: views.customerTotals,
          lineage: views.lineage,
        },
      });

      yield* Projection.run(reordered.at(params));
      expect((yield* resultRows(first, "customerTotals")).get("c1")?.revenue).toBe(20);
    }).pipe(Effect.provide(host), withTestReader(fixture));
  });

  it.live("numeric wire keys, reset and snapshots across units follow State semantics", () => {
    return Effect.gen(function* () {
      yield* migrateViews;
      yield* migrateReader;
      const input = yield* Numeric;
      yield* input.create({});

      const append = (
        entries: Array.NonEmptyReadonlyArray<DurableState.Entry<typeof Numeric.collections>>,
      ) =>
        Effect.gen(function* () {
          yield* input.append({}, DurableState.changes(Numeric.at({}), {}, entries));
          yield* Projection.run(numericCircuit.at({}));

          return yield* numericIds;
        });

      expect(
        yield* append([
          DurableState.upsert("row", { id: 1, n: 10 }),
          DurableState.upsert("row", { id: 2, n: 20 }),
        ]),
      ).toEqual([1, 2]);
      const metadata = (yield* state).circuits[0]!;

      const get = (key: string) =>
        inBackend((backend) => backend.store(metadata.namespace).get("r0", key));

      expect(yield* get(encodeRowKey(asRowKey(1)))).toBeUndefined();
      expect(yield* get(encodeRowKey("1"))).toEqual({
        key: "1",
        row: { id: 1, n: 10 },
      });
      expect(
        yield* append([
          StreamAddress.snapshotStart(),
          DurableState.upsert("row", { id: 1, n: 11 }),
        ]),
      ).toEqual([1, 2]);
      expect(
        yield* append([DurableState.upsert("row", { id: 3, n: 30 }), StreamAddress.snapshotEnd()]),
      ).toEqual([1, 3]);
      expect(
        yield* append([StreamAddress.reset(), DurableState.upsert("row", { id: 5, n: 50 })]),
      ).toEqual([5]);
      expect(yield* append([DurableState.remove("row", 5)])).toEqual([]);
    }).pipe(Effect.provide(numericHost), withTestReader(fixture));
  });

  it.live("filters a source integral across snapshots", () =>
    Effect.gen(function* () {
      yield* migrateViews;
      yield* migrateReader;
      const input = yield* Numeric;
      yield* input.create({});

      const binding = View.circuit({
        id: "filtered-numeric",
        params: {},
        output: Projection.perGeneration(NumericResult, "generation"),
        views: { row: View.from(Numeric, "row").where((row) => row.n.gt(10)) },
      }).at({});

      const append = (
        entries: Array.NonEmptyReadonlyArray<DurableState.Entry<typeof Numeric.collections>>,
      ) =>
        Effect.gen(function* () {
          yield* input.append({}, DurableState.changes(Numeric.at({}), {}, entries));
          yield* Projection.run(binding);

          return yield* numericIds;
        });

      expect(
        yield* append([numericUpsert(1, 1), numericUpsert(2, 20), numericUpsert(3, 30)]),
      ).toEqual([2, 3]);
      const metadata = (yield* state).circuits[0]!;

      const integralKeys = inBackend((backend) =>
        backend.store(metadata.namespace).rows("r0"),
      ).pipe(
        Effect.map((rows) =>
          Array.sort(
            Array.map(rows, (row) => String(row.key)),
            Order.String,
          ),
        ),
      );

      expect(yield* integralKeys).toEqual(["2", "3"]);

      expect(
        yield* append([numericUpsert(1, 11), numericUpsert(2, 2), numericUpsert(3, 30)]),
      ).toEqual([1, 3]);
      expect(yield* integralKeys).toEqual(["1", "3"]);
      expect(yield* append([StreamAddress.snapshotStart(), numericUpsert(3, 31)])).toEqual([1, 3]);
      expect(yield* append([numericUpsert(4, 4), StreamAddress.snapshotEnd()])).toEqual([3]);
      expect(yield* integralKeys).toEqual(["3"]);
      expect(
        yield* append([StreamAddress.reset(), numericUpsert(5, 50), numericUpsert(6, 6)]),
      ).toEqual([5]);
      expect(yield* append([DurableState.remove("row", 5)])).toEqual([]);
      expect(yield* integralKeys).toEqual([]);
    }).pipe(Effect.provide(numericHost), withTestReader(fixture)),
  );

  it.live("an attached circuit advances through the normal Projection registry", () => {
    const migrate = Layer.effectDiscard(migrateViews.pipe(Effect.andThen(migrateReader)));

    const attached = Projection.attach(circuit(), { durable: true }).pipe(
      Layer.provideMerge(Projection.Registry.layer),
      Layer.provideMerge(migrate.pipe(Layer.provideMerge(host))),
    );

    return Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const input = yield* Shop;
      yield* input.create(params);

      const receipt = yield* input.append(
        params,
        DurableState.changes(Shop.at(params), {}, [DurableState.upsert("order", order)]),
      );

      yield* registry
        .awaitOffset(circuit().at(params), Shop.key, receipt.offset)
        .pipe(Effect.timeout("5 seconds"));
      expect((yield* resultRows(first, "customerTotals")).get("c1")?.revenue).toBe(10);
    }).pipe(Effect.provide(attached), withTestReader(fixture));
  });

  it.live(
    "rejects distinct IR keys with the same State wire key and preserves the checkpoint",
    () => {
      const declaration = View.circuit({
        id: "wire-key",
        params: {},
        output: Projection.perGeneration(WireOutput, "generation"),
        views: { row: View.from(WireInput, "row") },
      }).at({});

      const local = circuitHost(fixture, Streamsy.group(WireInput, WireOutput));

      return Effect.gen(function* () {
        yield* migrateViews;
        const input = yield* WireInput;
        yield* input.create({});
        yield* input.append(
          {},
          DurableState.changes(WireInput.at({}), {}, [
            DurableState.upsert("row", { id: "one", target: 1 }),
          ]),
        );
        yield* Projection.run(declaration);
        const before = yield* Projection.read(declaration);
        const stream = yield* WireOutput;
        const head = yield* stream.head({ generation: 1 });
        yield* input.append(
          {},
          DurableState.changes(WireInput.at({}), {}, [
            DurableState.upsert("row", { id: "two", target: "1" }),
          ]),
        );
        const fault = yield* Projection.run(declaration).pipe(Effect.flip);
        expect(fault).toMatchObject({
          _tag: "ViewDataFault",
          view: '["output","row"]',
          node: '["output","row"]/key/0',
          key: "1",
          reason: "duplicate output key",
        });
        expect(yield* Projection.read(declaration)).toEqual(before);
        expect((yield* stream.head({ generation: 1 })).nextOffset).toBe(head.nextOffset);
      }).pipe(Effect.provide(local));
    },
  );
}
