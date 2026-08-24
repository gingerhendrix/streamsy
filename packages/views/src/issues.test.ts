import { Schema } from "effect";
import { describe, expect, it } from "vitest";
import type { RelationNode, RelationPlan } from "@streamsy/views-ir";
import { collectPlanIssues, PlanIssue } from "./index.ts";

/* oxlint-disable anti-slop/require-safety-comment-for-type-assertion, anti-slop/no-chained-type-assertions -- Malformed fixtures intentionally cross the static plan contract to exercise every checker issue code. */

const schema = { name: "Row", version: 1 } as const;
const reference = { kind: "reference", scope: "row", path: ["id"] } as const;
const source = (id = "source", sourceId = id): RelationNode => ({
  kind: "source",
  id,
  sourceId,
  schema,
  key: reference,
  order: reference,
  partitionBy: reference,
});
const plan = (
  nodes: readonly RelationNode[],
  output = nodes.at(-1)?.id ?? "missing",
): RelationPlan => ({
  version: 1,
  name: "fixture",
  nodes,
  output,
});
const codes = (value: RelationPlan): readonly string[] =>
  collectPlanIssues(value).map((issue) => issue.code);

describe("plan issue collection", () => {
  it("covers graph identity, schema and output failures", () => {
    expect(codes(plan([source("a", "same"), source("b", "same")]))).toContain("duplicate-name");
    expect(codes(plan([source(), source()]))).toContain("duplicate-node-id");
    expect(
      codes(
        plan([
          source(),
          {
            kind: "filter",
            id: "filter",
            schema,
            input: "absent",
            predicate: { kind: "literal", value: true },
          },
        ]),
      ),
    ).toContain("unknown-input");
    expect(codes(plan([{ ...source(), schema: { name: "", version: 0 } }]))).toContain(
      "schema-mismatch",
    );
    expect(codes(plan([source()], "absent"))).toContain("invalid-output-key");
  });

  it("covers expression, predicate, join and aggregate failures", () => {
    expect(
      codes(
        plan([
          source(),
          {
            kind: "filter",
            id: "filter",
            schema,
            input: "source",
            predicate: { kind: "reference", scope: "right", path: ["id"] },
          },
        ]),
      ),
    ).toContain("invalid-expression-scope");
    expect(
      codes(
        plan([
          source(),
          {
            kind: "filter",
            id: "filter",
            schema,
            input: "source",
            predicate: { kind: "literal", value: 1 },
          },
        ]),
      ),
    ).toContain("non-boolean-predicate");
    const badJoin: RelationNode = {
      kind: "inner-join",
      id: "join",
      schema,
      left: "left",
      right: "right",
      rightAlias: "",
      on: { kind: "literal", value: 1 },
    };
    const joinCodes = codes(plan([source("left"), source("right"), badJoin]));
    expect(joinCodes).toContain("join-alias-conflict");
    expect(joinCodes).toContain("join-condition-not-boolean");
    expect(
      codes(
        plan([
          source(),
          {
            kind: "grouped-aggregate",
            id: "aggregate",
            schema,
            input: "source",
            groupBy: {},
            aggregates: {},
          },
        ]),
      ),
    ).toContain("aggregate-shape-mismatch");
  });

  it("covers parameter, top, version and JSON failures with schema-backed issues", () => {
    const top: RelationNode = {
      kind: "top-n",
      id: "top",
      schema,
      input: "source",
      orderBy: [],
      limit: { kind: "reference", scope: "parameter", path: ["limit"] },
      maximum: 0,
    };
    const topCodes = codes(plan([source(), top]));
    expect(topCodes).toContain("undeclared-parameter");
    expect(topCodes).toContain("unbounded-top");
    expect(topCodes).toContain("unstable-top-order");
    expect(
      codes(plan([source(), { ...top, limit: { kind: "literal", value: -1 }, maximum: 10 }])),
    ).toContain("invalid-top-limit");
    expect(codes({ ...plan([source()]), version: 2 } as unknown as RelationPlan)).toContain(
      "unsupported-plan-version",
    );
    const nonJson = { ...plan([source()]), runtime: new Date() } as RelationPlan;
    const issues = collectPlanIssues(nonJson);
    expect(issues.map((issue) => issue.code)).toContain("non-json-plan-value");
    for (const issue of issues) expect(Schema.decodeUnknownSync(PlanIssue)(issue)).toEqual(issue);
  });
});

/* oxlint-enable anti-slop/require-safety-comment-for-type-assertion, anti-slop/no-chained-type-assertions */
