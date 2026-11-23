/* oxlint-disable effecttsgo/strict-effect-provide -- Test fixtures select SQL assertions when a SQL reader is present. */
import { CommitBoundary, MemoryStorage, Streamsy } from "@streamsy/core";
import { CheckpointStore, OwnerTransaction, Transaction } from "@streamsy/projection";
import { Array, Effect, Layer, Option, Order } from "effect";
import { SqlClient } from "effect/sql";
import type { Backend } from "../../src/backend.ts";
import { View, ViewBackend } from "../../src/index.ts";
import { canonicalJson } from "../../src/runtime/key.ts";
import { memoryStateOf } from "../../src/memory/backend.ts";
import { currentBackend } from "../../src/view-backend.ts";
import { hostFor, migrateTestReader, type SqlFixture } from "./backends.ts";

export const memory = { name: "Memory", memory: true } as const;

export type Fixture = SqlFixture | typeof memory;

export const memoryHost = <const Keys extends readonly Streamsy.Any[]>(
  group: Streamsy.Group<Keys>,
) =>
  Layer.mergeAll(CheckpointStore.layer, OwnerTransaction.layer, ViewBackend.layerMemory).pipe(
    Layer.provideMerge(MemoryStorage.layer(group, { readLimit: 200 })),
  );

export const circuitHost = <const Keys extends readonly Streamsy.Any[]>(
  fixture: Fixture,
  group: Streamsy.Group<Keys>,
) => ("memory" in fixture ? memoryHost(group) : hostFor(fixture, group));

export const inBackend = <A, E, R>(run: (backend: Backend) => Effect.Effect<A, E, R>) =>
  Effect.flatMap(CommitBoundary, (boundary) =>
    boundary.withTransaction(Effect.flatMap(currentBackend, run)),
  );

const sorted = <A>(rows: readonly A[]) => Array.sortWith(rows, canonicalJson, Order.String);

// oxlint-disable effecttsgo/prefer-schema-over-json -- Compare against the JSON text written by the SQL stores.
export const memoryDump = inBackend((backend) =>
  Effect.sync(() => {
    const namespaces = memoryStateOf(backend);

    if (namespaces === undefined) throw new Error("Expected the memory Views backend");

    const integrals = [];
    const arrangements = [];
    const accumulators = [];
    const owners = [];
    const circuits = [];

    for (const [namespace, state] of namespaces) {
      const identity = { namespace, namespace_id: namespace };

      for (const [relation, rows] of state.integrals)
        for (const [row_key, entry] of rows)
          integrals.push({ ...identity, relation, row_key, value: JSON.stringify(entry) });

      for (const [arrangement, buckets] of state.arrangements)
        for (const entries of buckets.values())
          for (const entry of entries) arrangements.push({ ...identity, arrangement, ...entry });

      for (const [node, groups] of state.accumulators)
        for (const [group_key, accumulator] of groups)
          accumulators.push({ ...identity, node, group_key, value: JSON.stringify(accumulator) });

      for (const [relation, keys] of state.keyOwners)
        for (const [output_key, owner_key] of keys)
          owners.push({ ...identity, relation, output_key, owner_key });

      if (state.hash !== undefined) circuits.push({ ...identity, plan_hash: state.hash });
    }

    return {
      integrals: sorted(integrals),
      arrangements: sorted(arrangements),
      accumulators: sorted(accumulators),
      owners: sorted(owners),
      circuits: sorted(circuits),
    };
  }),
);

// oxlint-enable effecttsgo/prefer-schema-over-json

export const migrateViews = Effect.flatMap(Effect.serviceOption(SqlClient.SqlClient), (sql) =>
  Option.isSome(sql) ? View.migrate(sql.value) : Effect.void,
);

export const migrateReader = Effect.flatMap(Effect.serviceOption(SqlClient.SqlClient), (sql) =>
  Option.isSome(sql) ? migrateTestReader(sql.value) : Effect.void,
);

export const readSql = <A, E, R>(run: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
    const tx = yield* Effect.serviceOption(Transaction);

    if (Option.isNone(sql) || Option.isNone(tx)) return Option.none();

    return Option.some(
      yield* run.pipe(
        Effect.provideService(SqlClient.SqlClient, sql.value),
        Effect.provideService(Transaction, tx.value),
      ),
    );
  });
