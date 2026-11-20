import { Array as Arr, Effect, Schema, Record as Rec } from "effect";
import { SqlClient } from "effect/sql";
import { evaluate, type ExpressionScopes } from "../runtime/expression.ts";
import { asRowKey, encodeRowKey } from "../runtime/key.ts";
import type { JsonObject, RowKey, SqlRelationPlan } from "../ir/contracts.ts";
import { compile, type Program } from "./planner.ts";
import { accepts, aggregate, join, linear, top } from "./operators.ts";
import { delta, difference, runtime, type Delta, type Deltas } from "./runtime.ts";
import { checkData, dataFault, ViewDataFault } from "./data-fault.ts";
import { rekey } from "./key-ownership.ts";
import { legacyRelations } from "./reference.ts";
import { legacyStore, storeFor, type Accumulator, type Entry, type Store } from "./store.ts";

export { ViewDataFault } from "./data-fault.ts";

export { migrate } from "./store.ts";

export { sqlPlanIssues } from "./planner.ts";

export { fullRecompute } from "./reference.ts";

export type StateChange =
  | {
      readonly sourceId: string;
      readonly kind: "upsert";
      readonly key: RowKey;
      readonly value: JsonObject;
    }
  | { readonly sourceId: string; readonly kind: "delete"; readonly key: RowKey };

export type OutputChange =
  | {
      readonly view: string;
      readonly kind: "upsert";
      readonly key: RowKey;
      readonly value: JsonObject;
    }
  | { readonly view: string; readonly kind: "delete"; readonly key: RowKey };

export class SqlEngineFault extends Schema.TaggedError<
  SqlEngineFault,
  { readonly brand: unique symbol }
>()("SqlEngineFault", {
  cause: Schema.Defect(),
}) {}

/** Compile once; each namespace has independent SQL state and a fixed plan.
 * process uses SqlClient.withTransaction, composing with an enclosing host transaction.
 * Input keys must agree with their rows; all relation/output keys must be unique.
 * Circuit outputs use the string key space of Durable State.
 */
export const makeEngine = Effect.fn("ViewsSql.makeEngine")(function* (
  views: Readonly<Record<string, SqlRelationPlan>>,
  stateOutputKeys = false,
) {
  const program: Program = yield* compile(views, stateOutputKeys);

  const sources = new Map(
    Arr.flatMap(program.nodes, (node) =>
      node.kind === "source" ? [[node.sourceId, node] as const] : [],
    ),
  );

  const evaluateUnit = (
    store: Store,
    input: readonly StateChange[],
    parameter: ExpressionScopes["parameter"],
  ) =>
    Effect.gen(function* () {
      const rt = runtime(program, store, parameter);
      const normalized = new Map<string, Map<string, Omit<Delta, "before">>>();
      const filtered = new Map<string, Deltas>();

      for (const c of input) {
        const source = sources.get(c.sourceId);

        if (!source)
          return yield* dataFault({ view: "*", node: c.sourceId })(c.key, "unknown source");

        const fault = rt.fault(source.id);

        const id = checkData(fault, c.key, () => {
          const encoded = encodeRowKey(c.key);

          if (c.kind === "upsert") {
            const raw = asRowKey(evaluate(source.key, { row: c.value, key: c.key, parameter }));

            if (encodeRowKey(source.stateKey ? String(raw) : raw) !== encoded)
              throw fault(c.key, "input key disagrees with source key expression");
          }

          return encoded;
        });

        const entries = normalized.get(source.id) ?? new Map<string, Omit<Delta, "before">>();
        normalized.set(source.id, entries);
        entries.set(id, { key: c.key, after: c.kind === "upsert" ? c.value : undefined });
      }

      for (const node of program.nodes) {
        let changes: Deltas;

        switch (node.kind) {
          case "source": {
            const result = new Map<string, Delta>();
            const pending = normalized.get(node.id);

            const filters = Arr.map(program.pushedFilters.get(node.id) ?? [], (filterNode) => {
              const deltas = new Map<string, Delta>();
              filtered.set(filterNode.id, deltas);

              return { node: filterNode, deltas, fault: rt.fault(filterNode.id) };
            });

            if (pending) {
              const before = yield* store.getMany(node.id, [...pending.keys()]);

              for (const [key, d] of pending) {
                const previous = before.get(key)?.row;
                let retained = filters.length === 0 ? d.after : undefined;

                for (const filter of filters) {
                  const old =
                    previous !== undefined &&
                    accepts(filter.node, d.key, previous, filter.fault, parameter)
                      ? previous
                      : undefined;

                  const next =
                    d.after !== undefined &&
                    accepts(filter.node, d.key, d.after, filter.fault, parameter)
                      ? d.after
                      : undefined;

                  const change = delta(d.key, old, next);

                  if (change) filter.deltas.set(key, change);

                  if (next !== undefined) retained = next;
                }

                // An old namespace can contain rejected rows. They contribute
                // no filtered before image and are removed when touched.
                const item = delta(d.key, previous, retained);

                if (item) result.set(key, item);
              }
            }

            changes = result;
            break;
          }

          case "filter":
          case "project":
            changes =
              filtered.get(node.id) ??
              linear(node, rt.changes.get(node.input) ?? new Map(), rt.fault(node.id), parameter);
            break;
          case "key":
            changes = yield* rekey(
              node,
              rt.changes.get(node.input) ?? new Map(),
              store,
              rt.fault(node.id),
              program.checkedKeys.has(node.id),
              program.stateKeys.has(node.id),
              parameter,
            );
            break;
          case "inner-join":
          case "left-join":
            changes = yield* join(node, program, rt);
            break;
          case "grouped-aggregate":
            changes = yield* aggregate(node, program, rt, store);
            break;
          case "top-n":
            changes = yield* top(node, program, rt);
            break;
          case "reduce-by-key":
            throw new TypeError("unsupported fact fold");
        }

        yield* rt.apply(node.id, changes);
      }

      return rt;
    });

  const sumNodes = Arr.filter(
    program.nodes,
    (node) =>
      node.kind === "grouped-aggregate" &&
      Arr.some(Rec.values(node.aggregates), (a) => a.function === "sum"),
  );

  const needsLayout = program.checkedKeys.size > 0 || sumNodes.length > 0;
  const layoutKey = "r2";

  const process = Effect.fn("ViewsSql.process")(function* (
    namespace: string,
    input: readonly StateChange[],
    parameter?: ExpressionScopes["parameter"],
  ) {
    const sql = yield* SqlClient.SqlClient;
    const store = storeFor(sql, namespace);

    const run = Effect.gen(function* () {
      let before: ReadonlyMap<string, readonly Entry[]> | undefined;
      let changes = input;

      if (needsLayout && !(yield* store.get("@layout", encodeRowKey(layoutKey)))) {
        const sourceRows: Record<string, readonly Entry[]> = {};
        const replay: StateChange[] = [];

        for (const [sourceId, source] of sources) {
          const rows = yield* store.rows(source.id);
          sourceRows[sourceId] = rows;

          for (const { key, row } of rows)
            replay.push({ sourceId, kind: "upsert", key, value: row });
        }

        if (replay.length > 0) {
          const legacy = legacyStore(sql, namespace);
          const sums = new Map<string, ReadonlyMap<string, Accumulator>>();

          for (const node of sumNodes) sums.set(node.id, yield* legacy.accumulators(node.id));
          before = legacyRelations(program, sourceRows, sums, parameter);
          yield* legacy.clearOperators();
          changes = [...replay, ...input];
        }

        yield* store.put("@layout", [{ key: layoutKey, row: {} }]);
      }

      const rt = yield* evaluateUnit(store, changes, parameter);
      const output: OutputChange[] = [];

      for (const [view, relation] of Rec.toEntries(program.outputs)) {
        let deltas = rt.changes.get(relation);

        if (before) {
          const current: Entry[] = [];

          for (const d of deltas?.values() ?? [])
            if (d.after !== undefined) current.push({ key: d.key, row: d.after });
          const location = program.locations.get(relation);
          deltas = difference(
            before.get(relation) ?? [],
            current,
            dataFault({
              view,
              node: location?.node ?? relation,
            }),
          );
        }

        for (const d of deltas?.values() ?? [])
          output.push(
            d.after === undefined
              ? { view, kind: "delete", key: d.key }
              : { view, kind: "upsert", key: d.key, value: d.after },
          );
      }

      return output;
    });

    return yield* sql
      .withTransaction(run)
      .pipe(
        Effect.catchDefect((cause) =>
          Effect.fail(Schema.is(ViewDataFault)(cause) ? cause : new SqlEngineFault({ cause })),
        ),
      );
  });

  return { program, process };
});

export type SqlEngine = Effect.Success<ReturnType<typeof makeEngine>>;
