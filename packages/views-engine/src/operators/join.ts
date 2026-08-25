import type {
  Change,
  Expression,
  InnerJoinNode,
  JsonObject,
  JsonValue,
  LeftJoinNode,
  RowKey,
} from "@streamsy/views-ir";
import { transition } from "../change.ts";
import { evaluate, evaluateOptional, isMissing } from "../expression.ts";
import { encodeRowKey } from "../key.ts";
import { equalitySides } from "../requirements.ts";

export interface KeyedRow {
  readonly key: RowKey;
  readonly row: JsonObject;
}

export interface JoinAccess {
  readonly relation: (relationId: string) => readonly KeyedRow[];
  readonly lookup: (
    relationId: string,
    expression: Expression,
    value: JsonValue,
  ) => readonly KeyedRow[];
}

/* oxlint-disable anti-slop/no-runtime-typeof -- Join predicate evaluation is the runtime validation boundary for the A1 Boolean contract. */

export interface JoinResult {
  readonly changes: readonly Change<JsonObject>[];
  readonly operations: number;
}

export function joinChange(
  node: InnerJoinNode | LeftJoinNode,
  changedInput: string,
  change: Change<JsonObject>,
  access: JoinAccess,
  parameters: JsonObject,
): JoinResult {
  const equality = equalitySides(node.on);
  const leftKeys = new Map<string, RowKey>();
  if (changedInput === node.left) leftKeys.set(encodeRowKey(change.key), change.key);
  else {
    if (change.kind !== "enter")
      collectMatchingLeft(node, equality.left, equality.right, change.before, access, leftKeys);
    if (change.kind !== "exit")
      collectMatchingLeft(node, equality.left, equality.right, change.after, access, leftKeys);
  }

  const currentOutput = access.relation(node.id);
  const changes: Change<JsonObject>[] = [];
  let operations = 0;
  for (const leftKey of leftKeys.values()) {
    const encodedLeft = encodeRowKey(leftKey);
    const before = new Map(
      currentOutput
        .filter((entry) => outputBelongsTo(entry.key, encodedLeft))
        .map((entry) => [encodeRowKey(entry.key), entry]),
    );
    const desired = new Map<string, KeyedRow>();
    const left = access
      .relation(node.left)
      .find((entry) => encodeRowKey(entry.key) === encodedLeft);
    if (left !== undefined) {
      const value = evaluateOptional(equality.left, {
        row: left.row,
        left: left.row,
        parameter: parameters,
      });
      const rights = isMissing(value) ? [] : access.lookup(node.right, equality.right, value);
      operations += 1 + rights.length;
      for (const right of rights) {
        const accepted = evaluate(node.on, {
          left: left.row,
          right: right.row,
          parameter: parameters,
        });
        if (typeof accepted !== "boolean")
          throw new TypeError("join condition did not produce a Boolean");
        if (!accepted) continue;
        const key: RowKey = ["match", encodedLeft, encodeRowKey(right.key)];
        desired.set(encodeRowKey(key), {
          key,
          row: { ...left.row, [node.rightAlias]: right.row },
        });
      }
      if (node.kind === "left-join" && desired.size === 0) {
        const key: RowKey = ["unmatched", encodedLeft];
        desired.set(encodeRowKey(key), { key, row: left.row });
      }
    }
    for (const identity of new Set([...before.keys(), ...desired.keys()])) {
      const previous = before.get(identity);
      const next = desired.get(identity);
      const output = transition(previous?.key ?? next!.key, previous?.row, next?.row);
      if (output !== undefined) changes.push(output);
    }
  }
  return { changes, operations };
}

function collectMatchingLeft(
  node: InnerJoinNode | LeftJoinNode,
  leftExpression: Expression,
  rightExpression: Parameters<typeof evaluateOptional>[0],
  right: JsonObject,
  access: JoinAccess,
  target: Map<string, RowKey>,
): void {
  const value = evaluateOptional(rightExpression, { row: right, right });
  if (isMissing(value)) return;
  for (const left of access.lookup(node.left, leftExpression, value))
    target.set(encodeRowKey(left.key), left.key);
}

function outputBelongsTo(key: RowKey, encodedLeft: string): boolean {
  return Array.isArray(key) && key[1] === encodedLeft;
}
