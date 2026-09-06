import { describe, expect, it } from "bun:test";
import type { Expression } from "./ir/contracts.ts";
import { aggregate, literal, parameterReference, selectors } from "./expression.ts";

interface Row {
  readonly id: string;
  readonly count: number;
  readonly active: boolean;
  readonly assigneeId?: string;
}

describe("expression construction", () => {
  const x = selectors<Row>();

  // Compare the inert IR; fluent helper methods are non-enumerable.
  it("builds comparisons and nested boolean expressions", () => {
    expect<Expression>(x.row.count.gte(2).and(x.row.active.eq(true))).toEqual({
      kind: "variadic",
      operator: "and",
      operands: [
        {
          kind: "binary",
          operator: "greater-than-or-equal",
          left: { kind: "reference", scope: "row", path: ["count"] },
          right: { kind: "literal", value: 2 },
        },
        {
          kind: "binary",
          operator: "equal",
          left: { kind: "reference", scope: "row", path: ["active"] },
          right: { kind: "literal", value: true },
        },
      ],
    });
    expect<Expression>(x.row.id.in("issue-1", "issue-2")).toEqual({
      kind: "variadic",
      operator: "in",
      operands: [
        { kind: "reference", scope: "row", path: ["id"] },
        { kind: "literal", value: "issue-1" },
        { kind: "literal", value: "issue-2" },
      ],
    });
  });

  it("keeps optional presence, value and fallback explicit", () => {
    expect<Expression>(x.row.assigneeId.isPresent()).toEqual({
      kind: "unary",
      operator: "is-present",
      operand: { kind: "reference", scope: "row", path: ["assigneeId"] },
    });
    expect<Expression>(x.row.assigneeId.value.eq("user-1")).toEqual({
      kind: "binary",
      operator: "equal",
      left: {
        kind: "unary",
        operator: "value",
        operand: { kind: "reference", scope: "row", path: ["assigneeId"] },
      },
      right: { kind: "literal", value: "user-1" },
    });
    expect(x.row.assigneeId.orElse("unassigned")).toMatchObject({
      kind: "binary",
      operator: "or-else",
    });
  });

  it("builds parameters, keys, aggregates and ordering as frozen inert data", () => {
    const parameter = parameterReference<string>("projectId");
    const key = x.key(x.row.id, x.row.count);
    expect<Expression>(parameter).toEqual({
      kind: "reference",
      scope: "parameter",
      path: ["projectId"],
    });
    expect<Expression>(key).toEqual({
      kind: "variadic",
      operator: "key",
      operands: [x.row.id, x.row.count],
    });
    expect(aggregate.countWhere(x.row.active)).toEqual({
      kind: "aggregate",
      function: "count-where",
      expression: x.row.active,
    });
    expect(x.row.id.asc()).toEqual({ expression: x.row.id, direction: "ascending" });
    expect(Object.isFrozen(key)).toBe(true);
    expect(Object.isFrozen(literal(1))).toBe(true);
  });
});
