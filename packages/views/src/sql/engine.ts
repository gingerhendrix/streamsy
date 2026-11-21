import { Array as Arr, Effect, Schema, Record as Rec } from "effect";
import { SqlClient } from "effect/sql";
import { evaluate, type ExpressionScopes } from "../runtime/expression.ts";
import { asRowKey, encodeRowKey } from "../runtime/key.ts";
import type { JsonObject, RowKey, RelationPlan } from "../ir/contracts.ts";
import { compile, type Program } from "./planner.ts";
import { accepts, aggregate, closure, concat, join, linear, top } from "./operators.ts";
import { delta, runtime, type Delta, type Deltas } from "./runtime.ts";
import { checkData, dataFault, ViewDataFault } from "./data-fault.ts";
import { rekey } from "./key-ownership.ts";
import { storeFor, type Store } from "./store.ts";

export { ViewDataFault } from "./data-fault.ts";

export { migrate } from "./store.ts";

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
  views: Readonly<Record<string, RelationPlan>>,
  stateOutputKeys = false,
  maxAffectedKeys?: number,
) {
  if (
    maxAffectedKeys !== undefined &&
    (!Number.isSafeInteger(maxAffectedKeys) || maxAffectedKeys <= 0)
  )
    throw new RangeError("maxAffectedKeys must be a positive safe integer");

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
      const rt = runtime(program, store, parameter, maxAffectedKeys);
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
          case "concat":
            changes = concat(node, rt);
            break;
          case "closure":
            changes = yield* closure(node, program, rt);
            break;
          case "grouped-aggregate":
            changes = yield* aggregate(node, program, rt, store);
            break;
          case "top-n":
            changes = yield* top(node, program, rt);
            break;
        }

        yield* rt.apply(node.id, changes);
      }

      return rt;
    });

  const process = Effect.fn("ViewsSql.process")(function* (
    namespace: string,
    input: readonly StateChange[],
    parameter?: ExpressionScopes["parameter"],
  ) {
    const sql = yield* SqlClient.SqlClient;
    const store = storeFor(sql, namespace);

    const run = Effect.gen(function* () {
      const rt = yield* evaluateUnit(store, input, parameter);
      const output: OutputChange[] = [];

      for (const [view, relation] of Rec.toEntries(program.outputs)) {
        const deltas = rt.changes.get(relation);

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
