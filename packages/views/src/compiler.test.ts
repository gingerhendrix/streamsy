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
  partitionBy: x.row.projectId,
  key: "id",
  mode: "facts",
});

describe("relation compilation", () => {
  it("lowers filters, projections, keys and bounded top deterministically", () => {
    const declaration = view(
      "example.ranked",
      { schema: Row, schemaRef: { name: "example.Row", version: 1 }, key: "id" },
      from(rows)
        .where(x.row.score.gt(0))
        .select({ id: x.row.id, projectId: x.row.projectId, score: x.row.score })
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
        { schema: Row, schemaRef: { name: "example.Row", version: 1 }, key: "id" },
        from(rows)
          .where(x.row.score.gt(0))
          .select({ id: x.row.id, projectId: x.row.projectId, score: x.row.score })
          .top({ by: [x.row.score.desc(), x.row.id.asc()], limit: 10 }),
      ).plan,
    );
  });

  it("encodes frozen fact and state source modes as distinct plan identities", () => {
    const facts = rows;
    const state = source("example.state-rows", {
      schema: Row,
      schemaRef: { name: "example.Row", version: 1 },
      partitionBy: x.row.projectId,
      key: "id",
      mode: "state",
    });
    const factPlan = view(
      "example.source-mode",
      { schema: Row, schemaRef: { name: "example.Row", version: 1 }, key: "id" },
      from(facts),
    ).plan;
    const statePlan = view(
      "example.source-mode",
      { schema: Row, schemaRef: { name: "example.Row", version: 1 }, key: "id" },
      from(state),
    ).plan;
    expect(factPlan.version).toBe(3);
    expect(factPlan.nodes[0]).toMatchObject({ mode: "facts", key: x.row.id });
    expect(statePlan.nodes[0]).toMatchObject({ mode: "state", key: x.row.id });
    expect(Object.isFrozen(statePlan.nodes[0])).toBe(true);
    expect(planHash(factPlan)).not.toBe(planHash(statePlan));
    expect(() => from(state).reduceByKey({ key: "id", reducer: {} as never })).toThrow(
      "fact source",
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
      key: "id",
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
    version: 3,
    name: "bad",
    nodes: [
      {
        kind: "source",
        id: "source",
        schema,
        sourceId: "rows",
        partitionBy: { kind: "reference", scope: "row", path: ["projectId"] },
        key: { kind: "reference", scope: "row", path: ["id"] },
        mode: "facts",
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
      version: 3,
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
