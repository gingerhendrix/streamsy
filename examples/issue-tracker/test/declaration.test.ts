/**
 * The declaration is inert data, and its plan is a stable identity.
 *
 * These are the properties everything else depends on: if a declaration could
 * mutate, or if the same declaration hashed differently between processes, then
 * the checkpoint, the store and the sink would all be describing something the
 * next host might not agree with.
 */
import { describe, expect, test } from "bun:test";
import { boardIssues, issueEvents, issueLifecycle, issues } from "../domain/declaration.ts";
import { compilePlan } from "../views/dsl.ts";
import { encodePlan, planHash } from "../views/plan.ts";

describe("the issue-tracker declaration", () => {
  test("every declaration node is frozen", () => {
    expect(Object.isFrozen(issueEvents)).toBe(true);
    expect(Object.isFrozen(issueLifecycle)).toBe(true);
    expect(Object.isFrozen(issues)).toBe(true);
    expect(Object.isFrozen(boardIssues)).toBe(true);
    expect(Object.isFrozen(issues.plan)).toBe(true);
  });

  test("the plan lowers to one source and one reduce-by-key node", () => {
    expect(issues.plan.nodes.map((node) => node.kind)).toEqual(["source", "reduce-by-key"]);
    expect(issues.plan.output).toBe("issue-tracker.issues");
    const [sourceNode] = issues.plan.nodes;
    expect(sourceNode?.kind === "source" && sourceNode.sourceId).toBe("issue-tracker.issue-events");
  });

  test("selectors compile to inspectable reference expressions", () => {
    expect(issueEvents.key).toEqual({ kind: "reference", scope: "row", path: ["eventId"] });
    expect(issueEvents.order).toEqual({ kind: "reference", scope: "row", path: ["sequence"] });
    expect(issueLifecycle.evolve["IssueStatusChanged"]).toEqual({
      status: { kind: "reference", scope: "event", path: ["status"] },
      updatedAt: { kind: "reference", scope: "event", path: ["occurredAt"] },
    });
  });

  test("plan encoding is canonical, so an equal plan hashes equally", () => {
    const rebuilt = compilePlan(issues.name, issues.expression);
    expect(encodePlan(rebuilt)).toBe(encodePlan(issues.plan));
    expect(planHash(rebuilt)).toBe(planHash(issues.plan));
  });

  test("a different plan hashes differently", () => {
    const renamed = compilePlan("issue-tracker.other", issues.expression);
    expect(planHash(renamed)).not.toBe(planHash(issues.plan));
  });

  test("the sink declares its own public contract", () => {
    expect(boardIssues.route).toBe("/state/workspaces/:workspaceId/issues");
    expect(boardIssues.params).toEqual(["workspaceId"]);
    expect(boardIssues.protocol).toEqual({
      transport: "durable-state",
      resume: true,
      fallback: "snapshot-then-live",
    });
    expect(boardIssues.auth.value).toBe("issue-tracker:workspace");
    expect(boardIssues.from.name).toBe(issues.name);
  });
});
