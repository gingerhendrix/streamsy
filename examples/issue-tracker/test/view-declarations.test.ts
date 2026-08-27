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
  test("all plans check and keep stable golden identities", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const hashes: Record<string, string> = {};
        for (const declaration of a1Views) {
          const checked = yield* checkPlan(declaration);
          hashes[declaration.name] = checked.hash;
          expect(Object.isFrozen(declaration)).toBe(true);
          expect(Object.isFrozen(declaration.plan.nodes)).toBe(true);
          expect(checked.hash).toBe(planHash(declaration.plan));
        }
        /**
         * `project-board` and `recent-activity` are byte-identical to their Wave B-i
         * identities: swapping the duplicate `projects`/`users` source declarations
         * for the catalog's own moved no field the plan encodes. `assignee-queue`
         * and `label-counts` moved, and both moves are the contract-freeze audit
         * fixing something that was wrong — a projection of `displayName`, a field
         * the ingested user row never had, and a count that did not exclude
         * detached memberships. See `integration-2-decisions.md`.
         */
        expect(hashes).toEqual({
          "issue-tracker.project-board": "edca38c1",
          "issue-tracker.assignee-queue": "662bbf73",
          "issue-tracker.label-counts": "06f6f6ba",
          "issue-tracker.recent-activity": "428ac64e",
        });
      }),
    ));

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

  test("label counts read attached memberships, two joins and a grouped count", () => {
    const source = labelCounts.plan.nodes.find(
      (node) => node.kind === "source" && node.sourceId === "issue-tracker.issue-labels",
    );
    expect(source).toMatchObject({
      mode: "state",
      key: { kind: "reference", scope: "row", path: ["membershipId"] },
      partitionBy: { kind: "reference", scope: "row", path: ["workspaceId"] },
    });
    // A detached membership stays in the relation, so the plan filters it out
    // before it is joined or counted.
    expect(labelCounts.plan.nodes.filter((node) => node.kind === "filter")[0]).toMatchObject({
      predicate: {
        kind: "binary",
        operator: "equal",
        left: { kind: "reference", scope: "row", path: ["attached"] },
        right: { kind: "literal", value: true },
      },
    });
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
