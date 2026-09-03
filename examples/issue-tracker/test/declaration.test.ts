/**
 * The declaration is inert data, and its plan is a stable identity.
 *
 * These are the properties everything else depends on: if a declaration could
 * mutate, or if the same declaration hashed differently between processes, then
 * the checkpoint, the store and the sink would all be describing something the
 * next host might not agree with.
 */
import { describe, expect, test } from "bun:test";
import { compilePlan, encodePlan, planHash } from "@streamsy/views";
import { projectBoard } from "../domain/views.ts";
import { catalog } from "../domain/catalog.ts";
import {
  assignmentNotifications,
  boardIssues,
  issueEvents,
  issueLifecycle,
  issues,
  labels,
  projects,
  users,
  workspaceMetadata,
} from "../domain/declaration.ts";

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
    expect(issueLifecycle.evolve["IssueStatusChanged"]).toEqual({
      status: { kind: "reference", scope: "event", path: ["status"] },
      updatedAt: { kind: "reference", scope: "event", path: ["occurredAt"] },
    });
  });

  test("a declared key field lowers to the plan's key expression", () => {
    expect(issueEvents.key).toBe("eventId");
    expect(issueEvents.keyExpression).toEqual({
      kind: "reference",
      scope: "row",
      path: ["eventId"],
    });
    const [sourceNode, reduceNode] = issues.plan.nodes;
    expect(sourceNode?.kind === "source" && sourceNode.key).toEqual(issueEvents.keyExpression);
    expect(reduceNode?.kind === "reduce-by-key" && reduceNode.key).toEqual({
      kind: "reference",
      scope: "row",
      path: ["issueId"],
    });
  });

  test("catalog sources declare independent State modes", () => {
    expect([projects, users, labels, workspaceMetadata].map((source) => source.mode)).toEqual([
      "state",
      "state",
      "state",
      "state",
    ]);
    expect(projects.key).toBe("projectId");
    expect(projects.keyExpression).toEqual({
      kind: "reference",
      scope: "row",
      path: ["projectId"],
    });
    expect(new Set([projects.name, users.name, labels.name, workspaceMetadata.name]).size).toBe(4);
  });

  test("the catalog table is derived from the same declarations", () => {
    const declarations = [
      ["projects", projects],
      ["users", users],
      ["labels", labels],
      ["metadata", workspaceMetadata],
    ] as const;
    for (const [name, declaration] of declarations) {
      expect(declaration.collection.name).toBe(name);
      expect(catalog[name].type).toBe(declaration.collection.type);
      expect(catalog[name].primaryKey).toBe(declaration.key);
    }
  });

  test("plan encoding is canonical, so an equal plan hashes equally", () => {
    const rebuilt = compilePlan(issues.name, issues.expression);
    expect(encodePlan(rebuilt)).toBe(encodePlan(issues.plan));
    expect(planHash(rebuilt)).toBe(planHash(issues.plan));
  });

  test("preserves the accepted Slice 1 canonical plan and hash", () => {
    expect(planHash(issues.plan)).toBe("8e6c39ef");
    expect(encodePlan(issues.plan)).toContain('"output":"issue-tracker.issues"');
  });

  test("a different plan hashes differently", () => {
    const renamed = compilePlan("issue-tracker.other", issues.expression);
    expect(planHash(renamed)).not.toBe(planHash(issues.plan));
  });

  test("the sink declares its own public contract", () => {
    expect(boardIssues.route).toBe("/state/workspaces/:workspaceId/issues");
    expect(Object.keys(boardIssues.params)).toEqual(["workspaceId"]);
    expect(boardIssues.protocol).toEqual({
      sessionVersion: 1,
      durableStateVersion: 1,
      transport: "durable-state",
      resume: true,
      fallback: "snapshot-then-live",
    });
    expect(boardIssues.fingerprint).toMatch(/^[0-9a-f]{8}$/);
    expect(boardIssues.from.name).toBe(projectBoard.name);
  });

  test("the action sink declares a delivery contract, not a route", () => {
    expect(assignmentNotifications.kind).toBe("checked-action-sink");
    expect(Object.isFrozen(assignmentNotifications)).toBe(true);
    expect(assignmentNotifications.key).toBe(issues.key);
    expect(assignmentNotifications.handler).toEqual({
      name: "issue-tracker.notify-assignee",
      version: 1,
    });
    expect(assignmentNotifications.delivery).toEqual({
      maxAttempts: 3,
      initialBackoffMs: 250,
      backoffFactor: 4,
      maxBackoffMs: 30_000,
    });
    expect(assignmentNotifications.fingerprint).toMatch(/^[0-9a-f]{8}$/);
  });

  test("the action sink's idempotency key is a function of durable facts only", () => {
    const notification = {
      workspaceId: "main",
      issueId: "issue-1",
      assigneeId: "ada",
      title: "Declare the issue view",
      status: "todo" as const,
      eventId: "assign-1",
      occurredAt: "2026-08-25T00:00:00.000Z",
    };
    expect(assignmentNotifications.idempotencyKey(notification)).toBe("main/assign-1");
    expect(assignmentNotifications.partitionBy(notification)).toBe("main");
    // Fields that are not part of the delivery's identity cannot change it.
    expect(
      assignmentNotifications.idempotencyKey({ ...notification, status: "done", title: "Renamed" }),
    ).toBe("main/assign-1");
  });

  test("the sink's collection key is the key its relation declares", () => {
    expect(projectBoard.key).toBe("issueId");
    expect(boardIssues.key).toBe(projectBoard.key);
    expect(boardIssues.collection).toEqual({
      name: "issues",
      type: "issue",
      primaryKey: "issueId",
    });
    const keyNode = projectBoard.plan.nodes.find((node) => node.kind === "key");
    expect(keyNode?.key).toEqual(projectBoard.keyExpression);
  });
});
