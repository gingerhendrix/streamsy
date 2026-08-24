import { describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { checkPlan, planHash } from "@streamsy/views";
import {
  a1Views,
  assigneeQueue,
  labelCounts,
  projectBoard,
  recentActivity,
} from "../domain/views.ts";

describe("the four inert A1 proof declarations", () => {
  test("all plans check and keep stable golden identities", async () => {
    const hashes: Record<string, string> = {};
    for (const declaration of a1Views) {
      const checked = await Effect.runPromise(checkPlan(declaration));
      hashes[declaration.name] = checked.hash;
      expect(Object.isFrozen(declaration)).toBe(true);
      expect(Object.isFrozen(declaration.plan.nodes)).toBe(true);
      expect(checked.hash).toBe(planHash(declaration.plan));
    }
    expect(hashes).toEqual({
      "issue-tracker.project-board": "2a6e53af",
      "issue-tracker.assignee-queue": "bcb7730a",
      "issue-tracker.label-counts": "ef8ea037",
      "issue-tracker.recent-activity": "79c785a7",
    });
  });

  test("project boards prove parameters, both joins, projection and partitioned bounded top", () => {
    expect(projectBoard.plan.parameters).toEqual({
      projectId: { schema: { name: "issue-tracker.Identifier", version: 1 } },
    });
    expect(projectBoard.plan.nodes.map((node) => node.kind)).toEqual([
      "source",
      "filter",
      "source",
      "inner-join",
      "source",
      "left-join",
      "project",
      "key",
      "top-n",
    ]);
    expect(projectBoard.plan.nodes.at(-1)).toMatchObject({
      kind: "top-n",
      maximum: 100,
      partitionBy: [{ kind: "reference", scope: "row", path: ["status"] }],
      orderBy: [
        { direction: "descending" },
        { direction: "ascending", expression: { path: ["issueId"] } },
      ],
    });
  });

  test("assignee queues guard optional access before comparison and join", () => {
    const filters = assigneeQueue.plan.nodes.filter((node) => node.kind === "filter");
    expect(filters).toHaveLength(2);
    expect(filters[0]).toMatchObject({
      predicate: {
        kind: "unary",
        operator: "is-present",
        operand: { kind: "reference", path: ["assigneeId"] },
      },
    });
    expect(filters[1]).toMatchObject({
      predicate: {
        kind: "binary",
        operator: "equal",
        left: { kind: "unary", operator: "value" },
        right: { kind: "reference", scope: "parameter", path: ["assigneeId"] },
      },
    });
    expect(assigneeQueue.plan.nodes.some((node) => node.kind === "inner-join")).toBe(true);
  });

  test("label counts use a composite-key source, two joins and grouped count", () => {
    const source = labelCounts.plan.nodes.find(
      (node) => node.kind === "source" && node.sourceId === "issue-tracker.issue-labels",
    );
    expect(source).toMatchObject({ key: { kind: "variadic", operator: "key" } });
    expect(labelCounts.plan.nodes.filter((node) => node.kind === "inner-join")).toHaveLength(2);
    expect(labelCounts.plan.nodes.find((node) => node.kind === "grouped-aggregate")).toMatchObject({
      groupBy: {
        labelId: { path: ["label", "labelId"] },
        labelName: { path: ["label", "name"] },
      },
      aggregates: { issueCount: { kind: "aggregate", function: "count" } },
    });
  });

  test("recent activity records workspace and bounded-limit parameters with stable ordering", () => {
    expect(recentActivity.plan.parameters).toEqual({
      workspaceId: { schema: { name: "issue-tracker.Identifier", version: 1 } },
      limit: { schema: { name: "issue-tracker.Sequence", version: 1 }, maximum: 200 },
    });
    expect(recentActivity.plan.nodes.at(-1)).toMatchObject({
      kind: "top-n",
      maximum: 200,
      limit: { kind: "reference", scope: "parameter", path: ["limit"] },
      orderBy: [
        { direction: "descending", expression: { path: ["sequence"] } },
        { direction: "ascending", expression: { path: ["eventId"] } },
      ],
    });
  });
});
