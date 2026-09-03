import { describe, expect, it } from "vitest";
import type {
  Change,
  Expression,
  JsonObject,
  RelationNode,
  RelationPlan,
} from "../ir/contracts.ts";
import { maintainGraph } from "./engine.ts";
import { canonicalJson } from "./key.ts";
import { fullRecompute } from "./reference.ts";
import type { MaintainGraphResult } from "./engine.ts";
import type { StateRow } from "./state.ts";

/* oxlint-disable anti-slop/require-safety-comment-for-type-assertion, typescript/no-unsafe-type-assertion -- Generated fixtures are constructed from the closed issue helper below; assertions recover its known string id and adapt JSON-only results to the canonical test encoder. */

const schema = { name: "issue.Row", version: 1 } as const;
const ref = (scope: "row" | "left" | "right", ...path: string[]): Expression => ({
  kind: "reference",
  scope,
  path,
});
const source = (id: string): RelationNode => ({
  kind: "source",
  id,
  sourceId: id,
  schema,
  partitionBy: ref("row", "workspaceId"),
  key: ref("row", "id"),
  mode: "state",
});
const makePlan = (name: string, nodes: readonly RelationNode[], output: string): RelationPlan => ({
  version: 3,
  name,
  nodes,
  output,
});

const leftJoinPlan = makePlan(
  "generated-left-join",
  [
    source("issues"),
    source("projects"),
    {
      kind: "left-join",
      id: "join",
      left: "issues",
      right: "projects",
      schema,
      on: {
        kind: "binary",
        operator: "equal",
        left: ref("left", "projectId"),
        right: ref("right", "id"),
      },
      rightAlias: "project",
    },
  ],
  "join",
);

const aggregatePlan = makePlan(
  "generated-aggregate",
  [
    source("issues"),
    {
      kind: "filter",
      id: "active",
      input: "issues",
      schema,
      predicate: {
        kind: "binary",
        operator: "not-equal",
        left: ref("row", "status"),
        right: { kind: "literal", value: "deleted" },
      },
    },
    {
      kind: "project",
      id: "projected",
      input: "active",
      schema,
      fields: { id: ref("row", "id"), status: ref("row", "status"), score: ref("row", "score") },
    },
    {
      kind: "grouped-aggregate",
      id: "aggregate",
      input: "projected",
      schema,
      groupBy: { status: ref("row", "status") },
      aggregates: {
        count: { kind: "aggregate", function: "count" },
        total: { kind: "aggregate", function: "sum", expression: ref("row", "score") },
        maximum: { kind: "aggregate", function: "max", expression: ref("row", "score") },
      },
    },
  ],
  "aggregate",
);

const topPlan = makePlan(
  "generated-top",
  [
    source("issues"),
    {
      kind: "top-n",
      id: "top",
      input: "issues",
      schema,
      orderBy: [
        { expression: ref("row", "score"), direction: "descending" },
        { expression: ref("row", "updatedAt"), direction: "descending" },
      ],
      partitionBy: [ref("row", "projectId")],
      limit: { kind: "literal", value: 3 },
      maximum: 3,
    },
  ],
  "top",
);

const plans = [leftJoinPlan, aggregatePlan, topPlan] as const;
const projectRows: readonly StateRow[] = [
  { key: "p1", row: { id: "p1", workspaceId: "w", projectId: "w", name: "One" } },
  { key: "p2", row: { id: "p2", workspaceId: "w", projectId: "w", name: "Two" } },
];

describe("deterministic generated issue-tracker conformance", () => {
  it("matches independent full recomputation for 200 seeds x 100 events", () => {
    for (let seed = 1; seed <= 200; seed++) runSeed(seed);
  }, 30_000);

  it("converges across batch boundaries and a fresh rebuild", () => {
    const rows = [
      issue("i1", "p1", "open", 1, 1),
      issue("i2", "p1", "open", 3, 2),
      issue("i3", "p1", "closed", 2, 3),
    ];
    for (const plan of plans) {
      const preload =
        plan === leftJoinPlan ? [{ sourceId: "projects", changes: enters(projectRows) }] : [];
      const one = maintainGraph({
        plan,
        inputs: [
          ...preload,
          {
            sourceId: "issues",
            changes: enters(rows.map((row) => ({ key: row.id as string, row }))),
          },
        ],
      });
      let split: MaintainGraphResult | undefined;
      if (preload.length > 0) split = maintainGraph({ plan, inputs: preload });
      for (const row of rows)
        split = maintainGraph({
          plan,
          state: split?.state,
          inputs: [
            { sourceId: "issues", changes: [{ kind: "enter", key: row.id as string, after: row }] },
          ],
        });
      expect(normalize(split!)).toBe(normalize(one));
      expect(normalize(rebuild(plan, rows))).toBe(normalize(one));
    }
  });
});

function runSeed(seed: number): void {
  const random = generator(seed);
  const issues = new Map<string, JsonObject>();
  const states = new Map<string, MaintainGraphResult>();
  states.set(
    leftJoinPlan.name,
    maintainGraph({
      plan: leftJoinPlan,
      inputs: [{ sourceId: "projects", changes: enters(projectRows) }],
    }),
  );
  for (let event = 0; event < 100; event++) {
    const id = `i${integer(random, 0, 11)}`;
    const before = issues.get(id);
    let after: JsonObject | undefined;
    if (before === undefined) after = randomIssue(random, id, event);
    else if (random() < 0.12) after = undefined;
    else after = mutateIssue(random, before, event);
    if (after === undefined) issues.delete(id);
    else issues.set(id, after);
    const delta: Change<JsonObject> =
      before === undefined
        ? { kind: "enter", key: id, after: after! }
        : after === undefined
          ? { kind: "exit", key: id, before }
          : { kind: "update", key: id, before, after };

    for (const plan of plans) {
      const result = maintainGraph({
        plan,
        state: states.get(plan.name)?.state,
        inputs: [{ sourceId: "issues", changes: [delta] }],
      });
      states.set(plan.name, result);
      const expected = fullRecompute({
        plan,
        sources: {
          issues: [...issues].map(([key, row]) => ({ key, row })),
          projects: projectRows,
        },
      });
      expect(normalize(result), `seed ${seed}, event ${event}, plan ${plan.name}`).toBe(
        canonicalJson(expected as never),
      );
      if (event % 25 === 24)
        expect(
          normalize(rebuild(plan, [...issues.values()])),
          `rebuild seed ${seed}, event ${event}`,
        ).toBe(normalize(result));
    }
  }
}

function rebuild(plan: RelationPlan, rows: readonly JsonObject[]): MaintainGraphResult {
  return maintainGraph({
    plan,
    inputs: [
      ...(plan === leftJoinPlan ? [{ sourceId: "projects", changes: enters(projectRows) }] : []),
      {
        sourceId: "issues",
        changes: rows.map((row) => ({ kind: "enter" as const, key: row.id as string, after: row })),
      },
    ],
  });
}

function enters(rows: readonly StateRow[]): readonly Change<JsonObject>[] {
  return rows.map((entry) => ({ kind: "enter", key: entry.key, after: entry.row }));
}

function normalize(result: MaintainGraphResult): string {
  return canonicalJson({ rows: result.rows, ordered: result.ordered } as never);
}

function randomIssue(random: () => number, id: string, event: number): JsonObject {
  return issue(
    id,
    `p${integer(random, 1, 2)}`,
    ["open", "closed", "blocked", "deleted"][integer(random, 0, 3)]!,
    integer(random, 0, 4),
    event % 7,
  );
}

function mutateIssue(random: () => number, before: JsonObject, event: number): JsonObject {
  const choice = integer(random, 0, 4);
  if (choice === 0) return { ...before, projectId: before.projectId === "p1" ? "p2" : "p1" };
  if (choice === 1)
    return { ...before, status: ["open", "closed", "blocked", "deleted"][integer(random, 0, 3)]! };
  if (choice === 2) return { ...before, score: integer(random, 0, 4) };
  if (choice === 3) return { ...before, title: `Title ${event}` };
  return { ...before, updatedAt: event % 7 };
}

function issue(
  id: string,
  projectId: string,
  status: string,
  score: number,
  updatedAt: number,
): JsonObject {
  return { id, workspaceId: "w", projectId, status, score, updatedAt, title: `Issue ${id}` };
}

function generator(seed: number): () => number {
  let value = seed >>> 0;
  return () => {
    value ^= value << 13;
    value ^= value >>> 17;
    value ^= value << 5;
    return (value >>> 0) / 0x1_0000_0000;
  };
}

function integer(random: () => number, minimum: number, maximum: number): number {
  return minimum + Math.floor(random() * (maximum - minimum + 1));
}
