import { describe, expect, it } from "vitest";
import type { Change, Expression, RelationNode, RelationPlan } from "./index.ts";

const expressionKind = (expression: Expression): Expression["kind"] => {
  switch (expression.kind) {
    case "literal":
    case "reference":
    case "unary":
    case "binary":
    case "variadic":
      return expression.kind;
    default: {
      const unreachable: never = expression;
      return unreachable;
    }
  }
};

const nodeKind = (node: RelationNode): RelationNode["kind"] => {
  switch (node.kind) {
    case "source":
    case "filter":
    case "project":
    case "key":
    case "inner-join":
    case "left-join":
    case "grouped-aggregate":
    case "top-n":
    case "reduce-by-key":
      return node.kind;
    default: {
      const unreachable: never = node;
      return unreachable;
    }
  }
};

describe("views IR contracts", () => {
  it("keeps unions exhaustive and composite row keys ordered", () => {
    const change: Change<{ readonly value: number }> = {
      kind: "enter",
      key: ["issue-1", 2, true],
      after: { value: 1 },
    };
    expect(change.key).toEqual(["issue-1", 2, true]);
    expect(
      expressionKind({
        kind: "unary",
        operator: "not",
        operand: { kind: "literal", value: false },
      }),
    ).toBe("unary");
    expect(
      nodeKind({
        kind: "filter",
        id: "view/filter/0",
        schema: { name: "example.Row", version: 1 },
        input: "example.source",
        predicate: { kind: "literal", value: true },
      }),
    ).toBe("filter");
  });

  it("round-trips a representative full plan through JSON", () => {
    const ref = { kind: "reference", scope: "row", path: ["id"] } as const;
    const schema = { name: "example.Row", version: 1 } as const;
    const plan: RelationPlan = {
      version: 2,
      name: "example.view",
      parameters: { limit: { schema: { name: "example.Limit", version: 1 }, maximum: 20 } },
      nodes: [
        {
          kind: "source",
          id: "source",
          schema,
          sourceId: "source",
          partitionBy: ref,
          mode: { kind: "facts", key: ref, order: ref },
        },
        {
          kind: "project",
          id: "example.view/project/0",
          schema,
          input: "source",
          fields: { id: ref },
        },
        {
          kind: "top-n",
          id: "example.view/top/1",
          schema,
          input: "example.view/project/0",
          orderBy: [{ expression: ref, direction: "ascending" }],
          limit: { kind: "reference", scope: "parameter", path: ["limit"] },
          maximum: 20,
        },
      ],
      output: "example.view/top/1",
    };
    expect(JSON.parse(JSON.stringify(plan))).toEqual(plan);
  });
});
