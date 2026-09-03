import type { Change, FilterNode, JsonObject } from "../../ir/contracts.ts";
import { evaluate } from "../expression.ts";

/* oxlint-disable anti-slop/no-runtime-typeof -- Predicate evaluation is the runtime validation boundary for the A1 Boolean contract. */

export function filterChange(
  node: FilterNode,
  change: Change<JsonObject>,
  parameters: JsonObject,
): readonly Change<JsonObject>[] {
  const before = change.kind === "enter" ? undefined : accepts(node, change.before, parameters);
  const after = change.kind === "exit" ? undefined : accepts(node, change.after, parameters);
  if (before === true && after === true && change.kind === "update") return [change];
  if (before === true && change.kind !== "enter")
    return [{ kind: "exit", key: change.key, before: change.before }];
  if (after === true && change.kind !== "exit")
    return [{ kind: "enter", key: change.key, after: change.after }];
  return [];
}

function accepts(node: FilterNode, row: JsonObject, parameters: JsonObject): boolean {
  const result = evaluate(node.predicate, { row, parameter: parameters });
  if (typeof result !== "boolean")
    throw new TypeError("filter predicate did not produce a Boolean");
  return result;
}
