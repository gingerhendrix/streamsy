import { Array, Effect, Record, Result, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, Streamsy } from "@streamsy/core";
import { Projection, ProjectionWakeup } from "@streamsy/projection";
import { View } from "../../../src/index.ts";
import { canonicalJson } from "../../../src/runtime/key.ts";

const Fact = Schema.Struct({
  id: Schema.String,
  workspace: Schema.String,
  date: Schema.String,
  key: Schema.String,
  total: Schema.Finite,
});

class RoutedFacts extends Streamsy.state<RoutedFacts>()("Views/RoutedFacts", "views/routed/facts", {
  params: {},
  collections: { fact: { key: "id", schema: Fact } },
}) {}

class RoutedRows extends Streamsy.state<RoutedRows>()(
  "Views/RoutedRows",
  "views/routed/:workspace/:date/g/:generation",
  {
    params: { workspace: Schema.String, date: Schema.String, generation: Schema.FiniteFromString },
    collections: {
      row: { key: "key", schema: Schema.Struct({ key: Schema.String, total: Schema.Finite }) },
    },
  },
) {}

export const routedGroup = Streamsy.group(RoutedFacts, RoutedRows);

const wireRow = View.fn("routed-row", 1, (key: string, total: number) => ({ key, total }));

const view = View.from(RoutedFacts, "fact").select((row) => ({
  workspace: row.workspace,
  date: row.date,
  value: wireRow(row.key, row.total),
}));

const declaration = (generation = 1) =>
  View.circuit({
    id: "routed-proof",
    generation,
    params: {},
    output: Projection.perGeneration(RoutedRows, "generation", { routeBy: ["workspace", "date"] }),
    views: {
      row: View.routed(view, {
        by: ({ workspace, date }) => ({ workspace, date }),
        row: (row) => row.value,
      }),
    },
  });

function compare<A>(actual: A, expected: NoInfer<A>, message: string) {
  if (canonicalJson(actual) !== canonicalJson(expected)) throw new Error(message);
}

/** Same routed oracle under a direct Bun run and a workerd wakeup. */
export const routedProof = Effect.fn("ViewsTest.routedProof")(function* (wakeup: boolean) {
  const sql = yield* SqlClient.SqlClient;
  yield* View.migrate(sql);
  const input = yield* RoutedFacts;
  const output = yield* RoutedRows;
  const spec = declaration();
  const binding = spec.at({});

  const runner = wakeup
    ? yield* ProjectionWakeup.attach(spec, { bindings: Effect.succeed([{}]) })
    : undefined;

  const run = runner?.run ?? Projection.run(binding).pipe(Effect.asVoid);
  const expected = new Map<string, typeof Fact.Type>();

  const scopes = [
    { workspace: "work", date: "one" },
    { workspace: "home", date: "one" },
    { workspace: "home", date: "two" },
  ];

  const histories = (generation: number) =>
    Effect.forEach(scopes, (scope) =>
      output.collect({ ...scope, generation }).pipe(
        Effect.map(({ items }) => items),
        Effect.catchTag("StreamNotFound", () => Effect.succeed([])),
      ),
    );

  const check = Effect.fn("ViewsTest.routedOracle")(function* (generation: number) {
    const actual = yield* histories(generation);

    for (const [index, scope] of scopes.entries()) {
      const rows: Record<string, { readonly key: string; readonly total: number }> = {};

      for (const item of actual[index] ?? []) {
        if (!("type" in item)) continue;

        if (item.headers.operation === "delete") delete rows[item.key];
        else if ("value" in item) rows[item.key] = item.value;
      }

      const full = Record.fromEntries(
        Array.filterMap([...expected.values()], (row) =>
          row.workspace === scope.workspace && row.date === scope.date
            ? Result.succeed([row.key, { key: row.key, total: row.total }] as const)
            : Result.fail(undefined),
        ),
      );

      compare(rows, full, `Routed oracle mismatch for ${scope.workspace}/${scope.date}`);
    }

    return actual;
  });

  const append = (
    facts: Array.NonEmptyReadonlyArray<DurableState.Entry<typeof RoutedFacts.collections>>,
  ) => input.append({}, DurableState.changes(RoutedFacts.at({}), {}, facts));

  yield* input.create({});
  yield* Projection.createOutputs(binding);

  const missing = yield* output
    .head({ workspace: "home", date: "absent", generation: 1 })
    .pipe(Effect.result);

  if (!Result.isFailure(missing) || missing.failure._tag !== "StreamNotFound")
    throw new Error("An untouched route already exists");

  const a = { id: "a", workspace: "work", date: "one", key: "same", total: 1 };
  const b = { id: "b", workspace: "home", date: "one", key: "same", total: 2 };
  const moved = { ...b, date: "two", total: 3 };
  expected.set(a.id, a);
  expected.set(b.id, b);
  yield* append([DurableState.upsert("fact", a), DurableState.upsert("fact", b)]);
  yield* run;
  const initial = yield* check(1);
  const [first] = initial[0] ?? [];
  const [second] = initial[1] ?? [];

  if (
    first === undefined ||
    second === undefined ||
    !("type" in first) ||
    !("type" in second) ||
    first.headers.txid === undefined ||
    first.headers.txid !== second.headers.txid
  )
    throw new Error("The initial routed writes have different transaction ids");

  expected.set(b.id, moved);
  yield* append([DurableState.upsert("fact", moved)]);
  yield* run;
  const updated = yield* check(1);
  const deleted = updated[1]?.at(-1);
  const inserted = updated[2]?.at(-1);

  if (
    deleted === undefined ||
    !("type" in deleted) ||
    deleted.headers.operation !== "delete" ||
    deleted.key !== b.key ||
    inserted === undefined ||
    !("type" in inserted) ||
    inserted.headers.operation !== "upsert" ||
    inserted.key !== b.key ||
    deleted.headers.txid === undefined ||
    deleted.headers.txid !== inserted.headers.txid
  )
    throw new Error("A route move did not delete and upsert under one transaction id");

  yield* Projection.run(declaration(2).at({}));
  yield* check(2);
  compare(yield* histories(1), updated, "Rebuild touched an older generation");

  const before = (yield* Projection.read(binding)).offsets;
  const duplicate = { ...a, id: "duplicate" };
  yield* append([DurableState.upsert("fact", duplicate)]);
  const failed = yield* Projection.run(binding).pipe(Effect.result);

  if (
    !Result.isFailure(failed) ||
    failed.failure._tag !== "ViewDataFault" ||
    failed.failure.reason !== "duplicate output key"
  )
    throw new Error("Duplicate key within one stream was accepted");
  compare((yield* Projection.read(binding)).offsets, before, "Duplicate advanced the checkpoint");
  compare(yield* check(1), updated, "Duplicate changed the routed streams");

  const invalid = { id: "bad", workspace: "home", date: "bad/path", key: "invalid", total: 4 };
  const valid = { ...invalid, id: "valid", workspace: "new", date: "three", key: "valid" };
  yield* append([
    DurableState.remove("fact", duplicate.id),
    DurableState.upsert("fact", valid),
    DurableState.upsert("fact", invalid),
  ]);
  const rejected = yield* Projection.run(binding).pipe(Effect.result);

  if (
    !Result.isFailure(rejected) ||
    rejected.failure._tag !== "ViewDataFault" ||
    rejected.failure.key !== "invalid" ||
    !rejected.failure.reason.includes("not a canonical id segment")
  )
    throw new Error(`Invalid route did not fail as row data: ${String(rejected)}`);
  compare(
    (yield* Projection.read(binding)).offsets,
    before,
    "Invalid route advanced the checkpoint",
  );
  compare(yield* check(1), updated, "Invalid route changed the routed streams");

  const rolledBack = yield* output
    .head({ workspace: valid.workspace, date: valid.date, generation: 1 })
    .pipe(Effect.result);

  if (!Result.isFailure(rolledBack) || rolledBack.failure._tag !== "StreamNotFound")
    throw new Error("Invalid route left a stream from the failed unit");

  yield* Projection.forget(binding);
  compare(yield* histories(1), updated, "Forget changed the routed streams");
  const retired = yield* Projection.retire(spec, {}, { generation: 1 });
  const results = retired.streams;

  if (
    results.length !== 3 ||
    Array.some(results, (stream) => stream.output !== "output" || stream.result !== "removed")
  )
    throw new Error("Retire did not remove all three touched streams");

  const again = yield* Projection.retire(spec, {}, { generation: 1 });
  compare(again, { record: "already-retired", streams: [] }, "Retire is not repeatable");
  compare(yield* histories(1), [[], [], []], "Retired streams still have data");
  yield* check(2);

  const namespaces = yield* sql`SELECT namespace FROM streamsy_view_circuits`;
  const expectedNamespace = canonicalJson([canonicalJson(["routed-proof", 1, {}]), 2]);
  compare(namespaces, [{ namespace: expectedNamespace }], "Retired view namespace remains");

  return { routes: results.length, moved: true, faults: 2 };
});
