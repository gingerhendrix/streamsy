import { Effect, Schema } from "effect";
import { describe, expect, it } from "vitest";
import type { RelationPlan } from "@streamsy/views-ir";
import {
  checkPlan,
  collectPlanIssues,
  defineView,
  encodePlan,
  from,
  parameter,
  planHash,
  selectors,
  source,
  view,
} from "./index.ts";

/* oxlint-disable anti-slop/require-safety-comment-for-type-assertion -- Malformed plan fixtures intentionally cross the static contract to prove runtime rejection. */

const Row = Schema.Struct({ id: Schema.String, projectId: Schema.String, score: Schema.Number });
type Row = typeof Row.Type;
const x = selectors<Row>();
const rows = source("example.rows", {
  schema: Row,
  schemaRef: { name: "example.Row", version: 1 },
  key: x.row.id,
  order: x.row.score,
  partitionBy: x.row.projectId,
});

describe("relation compilation", () => {
  it("lowers filters, projections, keys and bounded top deterministically", () => {
    const declaration = view(
      "example.ranked",
      { schema: Row, schemaRef: { name: "example.Row", version: 1 }, key: x.row.id },
      from(rows)
        .where(x.row.score.gt(0))
        .select({ id: x.row.id, projectId: x.row.projectId, score: x.row.score })
        .keyBy(x.row.id)
        .top({ by: [x.row.score.desc(), x.row.id.asc()], limit: 10 }),
    );
    expect(declaration.plan.nodes.map((node) => node.kind)).toEqual([
      "source",
      "filter",
      "project",
      "key",
      "top-n",
    ]);
    expect(declaration.plan.nodes.map((node) => node.id)).toEqual([
      "example.rows",
      "example.ranked/filter/0",
      "example.ranked/project/1",
      "example.ranked/key/2",
      "example.ranked/top/3",
    ]);
    expect(collectPlanIssues(declaration)).toEqual([]);
    expect(Object.isFrozen(declaration.plan.nodes)).toBe(true);
    expect(declaration.plan).toEqual(
      view(
        "example.ranked",
        { schema: Row, schemaRef: { name: "example.Row", version: 1 }, key: x.row.id },
        from(rows)
          .where(x.row.score.gt(0))
          .select({ id: x.row.id, projectId: x.row.projectId, score: x.row.score })
          .keyBy(x.row.id)
          .top({ by: [x.row.score.desc(), x.row.id.asc()], limit: 10 }),
      ).plan,
    );
  });

  it("records parameter metadata and enforced top bounds", async () => {
    const projectId = parameter("projectId", Schema.String);
    const limit = parameter("limit", Schema.Int, { maximum: 50 });
    const declaration = defineView({
      name: "example.by-project",
      params: { projectId, limit },
      schema: Row,
      schemaRef: { name: "example.Row", version: 1 },
      key: x.row.id,
      query: (params) =>
        from(rows)
          .where(x.row.projectId.eq(params.projectId))
          .top({ by: [x.row.score.desc(), x.row.id.asc()], limit: params.limit }),
    });
    expect(declaration.plan.parameters).toEqual({
      projectId: { schema: { name: "projectId", version: 1 } },
      limit: { schema: { name: "limit", version: 1 }, maximum: 50 },
    });
    expect(declaration.plan.nodes.at(-1)).toMatchObject({ kind: "top-n", maximum: 50 });
    await expect(Effect.runPromise(checkPlan(declaration))).resolves.toMatchObject({
      hash: expect.stringMatching(/^[0-9a-f]{8}$/),
    });
  });
});

describe("checking and canonical identity", () => {
  const schema = { name: "example.Row", version: 1 } as const;
  const raw = (overrides: Partial<RelationPlan> = {}): RelationPlan => ({
    version: 1,
    name: "bad",
    nodes: [
      {
        kind: "source",
        id: "source",
        schema,
        sourceId: "rows",
        key: { kind: "reference", scope: "row", path: ["id"] },
        order: { kind: "reference", scope: "row", path: ["score"] },
        partitionBy: { kind: "reference", scope: "row", path: ["projectId"] },
      },
      {
        kind: "top-n",
        id: "top",
        schema,
        input: "missing",
        orderBy: [
          {
            expression: { kind: "reference", scope: "row", path: ["score"] },
            direction: "descending",
          },
        ],
        limit: { kind: "literal", value: 0 },
        maximum: 0,
      },
    ],
    output: "absent",
    ...overrides,
  });

  it("collects independent issues and fails once through Effect", async () => {
    const issues = collectPlanIssues(raw());
    expect(issues.map((issue) => issue.code)).toEqual([
      "invalid-output-key",
      "unknown-input",
      "invalid-top-limit",
      "unbounded-top",
      "unstable-top-order",
    ]);
    await expect(Effect.runPromise(checkPlan(raw()))).rejects.toMatchObject({
      _tag: "PlanCheckFailed",
      issues,
    });
  });

  it("sorts object keys, preserves array order and rejects non-JSON values", () => {
    const first = raw({ name: "canonical", output: "source", nodes: [raw().nodes[0]!] });
    const reordered = {
      output: first.output,
      nodes: first.nodes,
      name: first.name,
      version: 1,
    } as RelationPlan;
    expect(encodePlan(first)).toBe(encodePlan(reordered));
    expect(planHash(first)).toBe(planHash(reordered));
    expect(planHash(first)).toMatch(/^[0-9a-f]{8}$/);
    expect(() => encodePlan({ ...first, value: Number.NaN } as RelationPlan)).toThrow("non-finite");
    expect(() => encodePlan({ ...first, value: undefined } as RelationPlan)).toThrow("non-JSON");
    expect(() => encodePlan({ ...first, value: 1n } as RelationPlan)).toThrow("non-JSON");
    expect(() => encodePlan({ ...first, value: () => 1 } as RelationPlan)).toThrow("non-JSON");
    expect(() => encodePlan({ ...first, value: Symbol("x") } as RelationPlan)).toThrow("non-JSON");
    expect(() => encodePlan({ ...first, value: new Date() } as RelationPlan)).toThrow("non-plain");
  });
});

/* oxlint-enable anti-slop/require-safety-comment-for-type-assertion */
