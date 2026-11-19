import { Array as Arr, Effect, Schema, Record as Rec } from "effect";
import { SqlClient } from "effect/sql";
import { evaluate } from "../runtime/expression.ts";
import { asRowKey, encodeRowKey } from "../runtime/key.ts";
import type { JsonObject, RowKey, SqlRelationPlan } from "../ir/contracts.ts";
import { compile, type Program } from "./planner.ts";
import { aggregate, join, linear, top } from "./operators.ts";
import { delta, runtime, type Delta, type Deltas } from "./runtime.ts";
import { storeFor } from "./store.ts";

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
 */
export const makeEngine = Effect.fn("ViewsSql.makeEngine")(function* (
  views: Readonly<Record<string, SqlRelationPlan>>,
) {
  const program: Program = yield* compile(views);

  const sources = new Map(
    Arr.flatMap(program.nodes, (node) =>
      node.kind === "source" ? [[node.sourceId, node] as const] : [],
    ),
  );

  const process = Effect.fn("ViewsSql.process")(function* (
    namespace: string,
    input: readonly StateChange[],
  ) {
    const sql = yield* SqlClient.SqlClient;
    const store = storeFor(sql, namespace);

    const run = Effect.gen(function* () {
      const rt = runtime(program, store);
      const normalized = new Map<string, Map<string, Delta>>();

      for (const c of input) {
        const source = sources.get(c.sourceId);

        if (!source) throw new TypeError(`unknown source ${c.sourceId}`);

        if (
          c.kind === "upsert" &&
          encodeRowKey(asRowKey(evaluate(source.key, { row: c.value, key: c.key }))) !==
            encodeRowKey(c.key)
        )
          throw new TypeError("input key disagrees with source key expression");
        const entries = normalized.get(source.id) ?? new Map<string, Delta>();
        normalized.set(source.id, entries);
        const id = encodeRowKey(c.key);
        const seen = entries.get(id);
        const before = seen ? seen.before : (yield* store.get(source.id, id))?.row;
        entries.set(id, { key: c.key, before, after: c.kind === "upsert" ? c.value : undefined });
      }

      for (const node of program.nodes) {
        let changes: Deltas;

        switch (node.kind) {
          case "source": {
            const result = new Map<string, Delta>();

            for (const [key, d] of normalized.get(node.id) ?? []) {
              const item = delta(d.key, d.before, d.after);

              if (item) result.set(key, item);
            }

            changes = result;
            break;
          }

          case "filter":
          case "project":
          case "key":
            changes = linear(node, rt.changes.get(node.input) ?? new Map());
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

      const output: OutputChange[] = [];

      for (const [view, relation] of Rec.toEntries(program.outputs))
        for (const d of rt.changes.get(relation)?.values() ?? [])
          output.push(
            d.after === undefined
              ? { view, kind: "delete", key: d.key }
              : { view, kind: "upsert", key: d.key, value: d.after },
          );

      return output;
    });

    return yield* sql
      .withTransaction(run)
      .pipe(Effect.catchDefect((cause) => Effect.fail(new SqlEngineFault({ cause }))));
  });

  return { program, process };
});

export type SqlEngine = Effect.Success<ReturnType<typeof makeEngine>>;
