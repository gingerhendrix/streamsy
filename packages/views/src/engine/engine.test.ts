import { describe, expect, it } from "vitest";
import type {
  Change,
  Expression,
  JsonObject,
  RelationNode,
  RelationPlan,
} from "../ir/contracts.ts";
import { maintainGraph } from "./engine.ts";
import { OperatorFault } from "./errors.ts";
import { fullRecompute } from "./reference.ts";
import { planRequirements } from "./requirements.ts";
import type { OperatorStateSnapshot } from "./state.ts";

const schema = { name: "test.Row", version: 1 } as const;
const ref = (scope: "row" | "left" | "right" | "parameter", ...path: string[]): Expression => ({
  kind: "reference",
  scope,
  path,
});
const literal = (value: string | number | boolean): Expression => ({ kind: "literal", value });
const source = (id: string): RelationNode => ({
  kind: "source",
  id,
  sourceId: id,
  schema,
  partitionBy: ref("row", "projectId"),
  key: ref("row", "id"),
  mode: "state",
});
const factSource = (id: string): RelationNode => ({
  kind: "source",
  id,
  sourceId: id,
  schema,
  partitionBy: ref("row", "workspaceId"),
  key: ref("row", "id"),
  mode: "facts",
});
const plan = (name: string, nodes: readonly RelationNode[], output: string): RelationPlan => ({
  version: 3,
  name,
  nodes,
  output,
});
const change = (
  key: string,
  before: JsonObject | undefined,
  after: JsonObject | undefined,
): Change<JsonObject> =>
  before === undefined
    ? { kind: "enter", key, after: after! }
    : after === undefined
      ? { kind: "exit", key, before }
      : { kind: "update", key, before, after };

describe("stateless transformations and graph routing", () => {
  const graph = plan(
    "filtered",
    [
      source("issues"),
      {
        kind: "filter",
        id: "filter",
        input: "issues",
        schema,
        predicate: {
          kind: "binary",
          operator: "equal",
          left: ref("row", "status"),
          right: literal("open"),
        },
      },
      {
        kind: "project",
        id: "project",
        input: "filter",
        schema,
        fields: { id: ref("row", "id"), title: ref("row", "title") },
      },
    ],
    "project",
  );

  it("retracts the prior filtered row and suppresses an equal projection update", () => {
    const open = { id: "i1", projectId: "p1", status: "open", title: "One", ignored: 1 };
    const ignored = { ...open, ignored: 2 };
    const closed = { ...ignored, status: "closed" };
    const entered = maintainGraph({
      plan: graph,
      inputs: [{ sourceId: "issues", changes: [change("i1", undefined, open)] }],
    });
    const suppressed = maintainGraph({
      plan: graph,
      state: entered.state,
      inputs: [{ sourceId: "issues", changes: [change("i1", open, ignored)] }],
    });
    const exited = maintainGraph({
      plan: graph,
      state: suppressed.state,
      inputs: [{ sourceId: "issues", changes: [change("i1", ignored, closed)] }],
    });
    expect(suppressed.changes).toEqual([]);
    expect(exited.changes).toEqual([
      { kind: "exit", key: "i1", before: { id: "i1", title: "One" } },
    ]);
    const requirements = planRequirements(graph);
    expect(requirements.find((item) => item.nodeId === "filter")?.intrinsicState).toBe("none");
    expect(requirements.find((item) => item.nodeId === "project")?.intrinsicState).toBe("none");
    expect(requirements.find((item) => item.nodeId === "filter")?.inputFields["issues"]).toEqual([
      "id",
      "status",
      "title",
    ]);
  });

  it("reports predicate and restore failures with typed graph context", () => {
    const bad = plan(
      "bad-filter",
      [
        source("issues"),
        { kind: "filter", id: "filter", input: "issues", schema, predicate: literal(1) },
      ],
      "filter",
    );
    expect(() =>
      maintainGraph({
        plan: bad,
        inputs: [
          { sourceId: "issues", changes: [change("i1", undefined, { id: "i1", projectId: "p1" })] },
        ],
      }),
    ).toThrow(OperatorFault);
  });

  it("treats state inputs as normalized keyed rows without inventing fact order", () => {
    const statePlan = plan(
      "state-source",
      [
        source("issues"),
        {
          kind: "project",
          id: "state-output",
          input: "issues",
          schema,
          fields: { id: ref("row", "id"), projectId: ref("row", "projectId") },
        },
      ],
      "state-output",
    );
    const row = { id: "i1", projectId: "p1", sequence: "not-an-order-contract" };
    const result = maintainGraph({
      plan: statePlan,
      inputs: [{ sourceId: "issues", changes: [change("i1", undefined, row)] }],
    });
    expect(result.rows).toEqual([{ key: "i1", row: { id: "i1", projectId: "p1" } }]);
    expect(planRequirements(statePlan)[0]?.inputFields["issues"]).toEqual(["id", "projectId"]);
    expect(() =>
      maintainGraph({
        plan: statePlan,
        inputs: [{ sourceId: "issues", changes: [change("wrong", undefined, row)] }],
      }),
    ).toThrow(/source key expression/);
    expect(() =>
      fullRecompute({ plan: statePlan, sources: { issues: [{ key: "wrong", row }] } }),
    ).toThrow(/source key expression/);

    const facts = plan(
      "fact-source",
      [
        factSource("events"),
        {
          kind: "project",
          id: "fact-output",
          input: "events",
          schema,
          fields: { id: ref("row", "id") },
        },
      ],
      "fact-output",
    );
    expect(planRequirements(facts)[0]?.inputFields["events"]).toEqual(["id", "workspaceId"]);
  });

  it("wraps projection decode faults with the node, key, and phase", () => {
    expect(() =>
      maintainGraph({
        plan: graph,
        decodeRow: () => {
          throw new TypeError("schema rejected projection");
        },
        inputs: [
          {
            sourceId: "issues",
            changes: [
              change("i1", undefined, {
                id: "i1",
                projectId: "p1",
                status: "open",
                title: "One",
              }),
            ],
          },
        ],
      }),
    ).toThrow(/node project \(project\) project for row "i1"/);
  });
});

describe("inner and left joins", () => {
  const join = (kind: "inner-join" | "left-join") =>
    plan(
      kind,
      [
        source("issues"),
        source("projects"),
        {
          kind,
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

  it("retracts an old pair, enters a new pair, and supports duplicate join values", () => {
    const graph = join("inner-join");
    const p1 = { id: "p1", projectId: "workspace", name: "One" };
    const p2 = { id: "p2", projectId: "workspace", name: "Two" };
    let state: OperatorStateSnapshot | undefined;
    const enteredIssues = maintainGraph({
      plan: graph,
      state,
      inputs: [
        {
          sourceId: "projects",
          changes: [change("p1", undefined, p1), change("p2", undefined, p2)],
        },
      ],
    });
    state = enteredIssues.state;
    expect(
      enteredIssues.patch.operatorIndexes.some((mutation) => mutation.operation === "put"),
    ).toBe(true);
    const i1 = { id: "i1", projectId: "p1", title: "First" };
    const i2 = { id: "i2", projectId: "p1", title: "Second" };
    ({ state } = maintainGraph({
      plan: graph,
      state,
      inputs: [
        { sourceId: "issues", changes: [change("i1", undefined, i1), change("i2", undefined, i2)] },
      ],
    }));
    const moved = { ...i1, projectId: "p2" };
    const result = maintainGraph({
      plan: graph,
      state,
      inputs: [{ sourceId: "issues", changes: [change("i1", i1, moved)] }],
    });
    expect(result.rows).toHaveLength(2);
    expect(result.changes.map((item) => item.kind)).toEqual(["exit", "enter"]);
    expect(result.operations.indexLookups).toBeLessThanOrEqual(2);
    expect(JSON.parse(JSON.stringify(result.state))).toEqual(result.state);
  });

  it("replaces an unmatched row on first match and restores it after the final match", () => {
    const graph = join("left-join");
    const issue = { id: "i1", projectId: "p1", title: "First" };
    let result = maintainGraph({
      plan: graph,
      inputs: [{ sourceId: "issues", changes: [change("i1", undefined, issue)] }],
    });
    expect(result.rows[0]?.key).toEqual(["unmatched", "s2:i1"]);
    const project = { id: "p1", projectId: "workspace", name: "One" };
    result = maintainGraph({
      plan: graph,
      state: result.state,
      inputs: [{ sourceId: "projects", changes: [change("p1", undefined, project)] }],
    });
    expect(result.changes.map((item) => item.kind)).toEqual(["exit", "enter"]);
    result = maintainGraph({
      plan: graph,
      state: result.state,
      inputs: [{ sourceId: "projects", changes: [change("p1", project, undefined)] }],
    });
    expect(result.rows[0]?.key).toEqual(["unmatched", "s2:i1"]);
    expect(result.patch.operatorIndexes.some((mutation) => mutation.operation === "delete")).toBe(
      true,
    );
  });

  it("shares compatible arrangements and retains only declared join fields", () => {
    const requirements = planRequirements(join("left-join"));
    const arrangements = requirements.flatMap((item) => item.arrangements);
    expect(arrangements.map((item) => item.retainedFields)).toEqual([["projectId"], ["id"]]);
    expect(new Set(arrangements.map((item) => item.id)).size).toBe(2);
  });

  it("rejects non-equality join plans and corrupt arrangement snapshots contextually", () => {
    const unsupported = join("inner-join");
    const node = unsupported.nodes.find((candidate) => candidate.kind === "inner-join")!;
    const bad = {
      ...unsupported,
      nodes: unsupported.nodes.map((candidate) =>
        candidate === node ? { ...node, on: literal(true) } : candidate,
      ),
    };
    expect(() => maintainGraph({ plan: bad, inputs: [] })).toThrow(OperatorFault);

    const valid = maintainGraph({
      plan: join("left-join"),
      inputs: [
        {
          sourceId: "issues",
          changes: [change("i1", undefined, { id: "i1", projectId: "p1" })],
        },
      ],
    });
    const arrangement = valid.state.arrangements[0]!;
    const corrupt: OperatorStateSnapshot = {
      ...valid.state,
      arrangements: [
        {
          ...arrangement,
          entries: [{ value: "p1", rowKeys: ["absent"] }],
        },
        ...valid.state.arrangements.slice(1),
      ],
    };
    expect(() => maintainGraph({ plan: join("left-join"), state: corrupt, inputs: [] })).toThrow(
      /references an absent row/,
    );
  });

  it("deduplicates one compatible arrangement across multiple consumers", () => {
    const graph = plan(
      "shared",
      [
        source("issues"),
        source("projects"),
        source("users"),
        {
          kind: "inner-join",
          id: "project-join",
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
        {
          kind: "inner-join",
          id: "user-join",
          left: "issues",
          right: "users",
          schema,
          on: {
            kind: "binary",
            operator: "equal",
            left: ref("left", "projectId"),
            right: ref("right", "id"),
          },
          rightAlias: "user",
        },
      ],
      "project-join",
    );
    const result = maintainGraph({
      plan: graph,
      inputs: [
        {
          sourceId: "issues",
          changes: [
            change("i1", undefined, {
              id: "i1",
              projectId: "p1",
              deadDescription: "must not enter an arrangement",
            }),
          ],
        },
      ],
    });
    expect(planRequirements(graph).flatMap((item) => item.arrangements)).toHaveLength(4);
    expect(result.state.arrangements).toHaveLength(3);
    expect(result.state.arrangements.find((item) => item.relationId === "issues")).toMatchObject({
      retainedFields: ["projectId"],
      entries: [{ value: "p1", rowKeys: ["i1"] }],
    });
    expect(JSON.stringify(result.state.arrangements)).not.toContain("deadDescription");
  });
});

describe("grouped aggregates and exact top", () => {
  const aggregate = plan(
    "aggregate",
    [
      source("issues"),
      {
        kind: "grouped-aggregate",
        id: "aggregate",
        input: "issues",
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

  it("updates both groups, removes the last group, and exposes the next counted maximum", () => {
    const high = { id: "i1", projectId: "p", status: "open", score: 10 };
    const low = { id: "i2", projectId: "p", status: "open", score: 5 };
    let result = maintainGraph({
      plan: aggregate,
      inputs: [
        {
          sourceId: "issues",
          changes: [change("i1", undefined, high), change("i2", undefined, low)],
        },
      ],
    });
    expect(result.rows[0]?.row).toMatchObject({ count: 2, total: 15, maximum: 10 });
    result = maintainGraph({
      plan: aggregate,
      state: result.state,
      inputs: [{ sourceId: "issues", changes: [change("i1", high, undefined)] }],
    });
    expect(result.rows[0]?.row).toMatchObject({ count: 1, total: 5, maximum: 5 });
    result = maintainGraph({
      plan: aggregate,
      state: result.state,
      inputs: [{ sourceId: "issues", changes: [change("i2", low, { ...low, status: "closed" })] }],
    });
    expect(result.changes.map((item) => item.kind)).toEqual(["exit", "enter"]);
    expect(result.state.aggregates[0]?.groups[0]).not.toHaveProperty("members");
    expect(result.state.relations.some((relation) => relation.relationId === "aggregate")).toBe(
      false,
    );
  });

  it("rejects non-finite aggregate input contextually", () => {
    expect(() =>
      maintainGraph({
        plan: aggregate,
        inputs: [
          {
            sourceId: "issues",
            changes: [
              change("i1", undefined, {
                id: "i1",
                projectId: "p",
                status: "open",
                score: Number.NaN,
              }),
            ],
          },
        ],
      }),
    ).toThrow(/node aggregate \(grouped-aggregate\) aggregate/);
  });

  const top = plan(
    "top",
    [
      source("issues"),
      {
        kind: "top-n",
        id: "top",
        input: "issues",
        schema,
        orderBy: [{ expression: ref("row", "score"), direction: "descending" }],
        limit: literal(2),
        maximum: 2,
        partitionBy: [ref("row", "projectId")],
      },
    ],
    "top",
  );

  it("moves outsiders across boundaries, resolves ties by key, and moves partitions", () => {
    const a = { id: "a", projectId: "p1", score: 3 };
    const b = { id: "b", projectId: "p1", score: 2 };
    const c = { id: "c", projectId: "p1", score: 1 };
    let result = maintainGraph({
      plan: top,
      inputs: [
        {
          sourceId: "issues",
          changes: [
            change("a", undefined, a),
            change("b", undefined, b),
            change("c", undefined, c),
          ],
        },
      ],
    });
    expect(result.ordered[0]?.keys).toEqual(["a", "b"]);
    const promoted = { ...c, score: 4 };
    result = maintainGraph({
      plan: top,
      state: result.state,
      inputs: [{ sourceId: "issues", changes: [change("c", c, promoted)] }],
    });
    expect(result.ordered[0]?.keys).toEqual(["c", "a"]);
    expect(result.changes.map((item) => item.kind)).toEqual(["exit", "enter"]);
    const tied = { ...promoted, score: 3 };
    result = maintainGraph({
      plan: top,
      state: result.state,
      inputs: [{ sourceId: "issues", changes: [change("c", promoted, tied)] }],
    });
    expect(result.ordered[0]?.keys).toEqual(["a", "c"]);
    const moved = { ...tied, projectId: "p2" };
    result = maintainGraph({
      plan: top,
      state: result.state,
      inputs: [{ sourceId: "issues", changes: [change("c", tied, moved)] }],
    });
    expect(result.ordered).toHaveLength(2);
    expect(result.state.tops[0]).not.toHaveProperty("ordered");
    expect(result.state.relations.some((relation) => relation.relationId === "top")).toBe(false);
  });

  it("rejects invalid limits and incomparable sort values contextually", () => {
    const invalid = {
      ...top,
      nodes: top.nodes.map((node) =>
        node.kind === "top-n" ? { ...node, limit: literal(0) } : node,
      ),
    };
    expect(() => maintainGraph({ plan: invalid, inputs: [] })).toThrow(/top limit/);
    expect(() =>
      maintainGraph({
        plan: top,
        inputs: [
          {
            sourceId: "issues",
            changes: [
              change("i1", undefined, { id: "i1", projectId: "p", score: { nested: true } }),
            ],
          },
        ],
      }),
    ).toThrow(/top/);
  });
});
