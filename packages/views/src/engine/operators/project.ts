import type { Change, JsonObject, JsonValue, ProjectNode } from "../../ir/contracts.ts";
import { transition } from "../change.ts";
import { evaluate } from "../expression.ts";

export function projectChange(
  node: ProjectNode,
  change: Change<JsonObject>,
  parameters: JsonObject,
  decode: (row: JsonObject) => JsonObject,
): readonly Change<JsonObject>[] {
  const before =
    change.kind === "enter" ? undefined : project(node, change.before, parameters, decode);
  const after =
    change.kind === "exit" ? undefined : project(node, change.after, parameters, decode);
  const result = transition(change.key, before, after);
  return result === undefined ? [] : [result];
}

function project(
  node: ProjectNode,
  row: JsonObject,
  parameters: JsonObject,
  decode: (row: JsonObject) => JsonObject,
): JsonObject {
  const output: Record<string, JsonValue> = {};
  for (const [name, expression] of Object.entries(node.fields))
    output[name] = evaluate(expression, { row, parameter: parameters });
  return decode(output);
}
