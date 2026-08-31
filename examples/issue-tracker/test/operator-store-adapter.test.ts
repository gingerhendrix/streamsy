import { expect, test } from "bun:test";
import { Effect } from "effect";
import { planHash } from "@streamsy/views";
import { maintainGraph } from "@streamsy/views-engine";
import { makeMemoryBacking, memoryService } from "@streamsy/views-store";
import { projectBoard } from "../domain/views.ts";
import {
  decodeOperatorSnapshot,
  operatorMaintenanceCommit,
  operatorSnapshotRef,
} from "../server/persistence/operator-store-adapter.ts";

const issue = {
  issueId: "i1",
  workspaceId: "main",
  projectId: "streamsy",
  title: "Atomic board",
  status: "todo",
  updatedAt: "2026-08-25T10:00:00.000Z",
};
const project = {
  projectId: "streamsy",
  workspaceId: "main",
  name: "Streamsy",
  revision: 1,
};

test("A2 patch, output rows, snapshot, history, and A4 cursor commit atomically", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const store = memoryService(makeMemoryBacking());
      const result = maintainGraph({
        plan: projectBoard.plan,
        parameters: { projectId: "streamsy" },
        inputs: [
          {
            sourceId: "issue-tracker.issues",
            changes: [{ kind: "enter", key: "i1", after: issue }],
          },
          {
            sourceId: "issue-tracker.projects",
            changes: [{ kind: "enter", key: "streamsy", after: project }],
          },
        ],
      });
      const hash = planHash(projectBoard.plan);
      const commit = operatorMaintenanceCommit({
        plan: projectBoard.plan,
        planHash: hash,
        partition: "main",
        sourceId: "issue-tracker.inputs",
        expectedCursor: undefined,
        afterExclusiveCursor: "batch-1",
        batchId: "batch-1",
        committedAtMs: 1,
        expectedRevision: 0,
        patch: result.patch,
        snapshot: result.state,
        relationId: projectBoard.name,
        changes: result.changes,
      });

      yield* store.commit(commit);
      expect(
        (yield* store.snapshotRows({ ...commit.identity, id: projectBoard.name })).rows,
      ).toHaveLength(1);
      expect(yield* store.sourceProgress(commit.identity)).toBe("batch-1");
      const stored = yield* store.getOperatorValue(
        operatorSnapshotRef(projectBoard.plan, hash, "main", "issue-tracker.inputs"),
        "state",
      );
      expect(decodeOperatorSnapshot(projectBoard.plan, stored)?.revision).toBe(1);

      const stale = { ...commit, batchId: "batch-2", afterExclusiveCursor: "batch-2" };
      const rejected = yield* Effect.result(store.commit(stale));
      expect(rejected).toMatchObject({
        failure: { _tag: "ViewCursorConflict" },
      });
      expect(yield* store.sourceProgress(commit.identity)).toBe("batch-1");
    }),
  ));

test("adapter rejects a stale A2 base revision before touching A4", () => {
  const result = maintainGraph({ plan: projectBoard.plan, inputs: [] });
  expect(() =>
    operatorMaintenanceCommit({
      plan: projectBoard.plan,
      planHash: planHash(projectBoard.plan),
      partition: "main",
      sourceId: "issue-tracker.inputs",
      expectedCursor: undefined,
      afterExclusiveCursor: "batch-1",
      batchId: "batch-1",
      committedAtMs: 1,
      expectedRevision: 99,
      patch: result.patch,
      snapshot: result.state,
      relationId: projectBoard.name,
      changes: result.changes,
    }),
  ).toThrow("patch revision");
});
