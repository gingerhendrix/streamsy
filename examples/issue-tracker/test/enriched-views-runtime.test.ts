import { expect, test } from "bun:test";
import { fullRecompute, maintainGraph } from "@streamsy/views-engine";
import { executableViews } from "../domain/views.ts";

const project = {
  projectId: "streamsy",
  workspaceId: "main",
  key: "STR",
  name: "Streamsy",
  updatedAt: "2026-08-25T09:00:00.000Z",
};
const user = {
  userId: "u1",
  workspaceId: "main",
  name: "Ada",
  updatedAt: "2026-08-25T09:00:00.000Z",
};
const issue = {
  issueId: "i1",
  workspaceId: "main",
  projectId: "streamsy",
  title: "Integrate views",
  status: "todo",
  updatedAt: "2026-08-25T10:00:00.000Z",
  assigneeId: "u1",
};

test("registered project board updates joins incrementally and matches full recomputation", () => {
  const plan = executableViews.projectBoard.plan;
  const initialInputs = [
    {
      sourceId: "issue-tracker.issues",
      changes: [{ kind: "enter" as const, key: "i1", after: issue }],
    },
    {
      sourceId: "issue-tracker.projects",
      changes: [{ kind: "enter" as const, key: "streamsy", after: project }],
    },
    {
      sourceId: "issue-tracker.users",
      changes: [{ kind: "enter" as const, key: "u1", after: user }],
    },
  ];
  const initial = maintainGraph({
    plan,
    parameters: { projectId: "streamsy" },
    inputs: initialInputs,
  });
  expect(initial.rows[0]?.row).toMatchObject({ projectName: "Streamsy", assignee: "u1" });

  const renamed = { ...project, name: "Streamsy Platform", updatedAt: "2026-08-25T09:30:00.000Z" };
  const updated = maintainGraph({
    plan,
    state: initial.state,
    parameters: { projectId: "streamsy" },
    inputs: [
      {
        sourceId: "issue-tracker.projects",
        changes: [{ kind: "update", key: "streamsy", before: project, after: renamed }],
      },
    ],
  });
  expect(updated.changes).toHaveLength(1);
  expect(updated.rows[0]?.row.projectName).toBe("Streamsy Platform");

  const oracle = fullRecompute({
    plan,
    parameters: { projectId: "streamsy" },
    sources: {
      "issue-tracker.issues": [{ key: "i1", row: issue }],
      "issue-tracker.projects": [{ key: "streamsy", row: renamed }],
      "issue-tracker.users": [{ key: "u1", row: user }],
    },
  });
  expect(updated.rows).toEqual(oracle.rows);
});

test("registered label counts retract and exact top promotes the next candidate", () => {
  const countPlan = executableViews.labelCounts.plan;
  const label = {
    labelId: "bug",
    workspaceId: "main",
    name: "Bug",
    color: "#d64545",
    updatedAt: "2026-08-25T09:00:00.000Z",
  };
  const membership = {
    membershipId: "i1.bug",
    issueId: "i1",
    labelId: "bug",
    workspaceId: "main",
    attached: true,
    updatedAt: "2026-08-25T10:00:00.000Z",
  };
  const counted = maintainGraph({
    plan: countPlan,
    parameters: { projectId: "streamsy" },
    inputs: [
      {
        sourceId: "issue-tracker.issues",
        changes: [{ kind: "enter", key: "i1", after: issue }],
      },
      {
        sourceId: "issue-tracker.labels",
        changes: [{ kind: "enter", key: "bug", after: label }],
      },
      {
        sourceId: "issue-tracker.issue-labels",
        changes: [{ kind: "enter", key: "i1.bug", after: membership }],
      },
    ],
  });
  expect(counted.rows[0]?.row.issueCount).toBe(1);

  /**
   * Detaching is an *update* to `attached`, not an exit: the membership stays
   * in the relation. The count still retracts, because the plan filters on
   * `attached` before it joins — which is exactly the property the runtime
   * depends on for a label removal to be visible.
   */
  const detached = { ...membership, attached: false, updatedAt: "2026-08-25T10:05:00.000Z" };
  const retracted = maintainGraph({
    plan: countPlan,
    state: counted.state,
    parameters: { projectId: "streamsy" },
    inputs: [
      {
        sourceId: "issue-tracker.issue-labels",
        changes: [{ kind: "update", key: "i1.bug", before: membership, after: detached }],
      },
    ],
  });
  expect(retracted.rows).toEqual([]);

  const activityPlan = executableViews.recentActivity.plan;
  const events = [0, 1, 2].map((sequence) => ({
    type: "IssueStatusChanged",
    eventId: `e${sequence}`,
    workspaceId: "main",
    issueId: "i1",
    sequence,
    occurredAt: `2026-08-25T10:0${sequence}:00.000Z`,
    status: "todo",
  }));
  const activity = maintainGraph({
    plan: activityPlan,
    parameters: { workspaceId: "main", limit: 2 },
    inputs: [
      {
        sourceId: "issue-tracker.issue-events",
        changes: events.map((after) => ({ kind: "enter" as const, key: after.eventId, after })),
      },
    ],
  });
  expect(activity.ordered[0]?.keys).toEqual(["e2", "e1"]);
  const promoted = maintainGraph({
    plan: activityPlan,
    state: activity.state,
    parameters: { workspaceId: "main", limit: 2 },
    inputs: [
      {
        sourceId: "issue-tracker.issue-events",
        changes: [{ kind: "exit", key: "e2", before: events[2]! }],
      },
    ],
  });
  expect(promoted.ordered[0]?.keys).toEqual(["e1", "e0"]);
});
