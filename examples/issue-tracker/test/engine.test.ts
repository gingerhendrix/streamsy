/**
 * The engine is a pure function, so it is tested as one.
 *
 * The properties under test are the ones recovery rests on: the same suffix in
 * a different arrival order converges on the same rows, a no-op fold emits no
 * change, and a fold that cannot produce a declared row fails typed instead of
 * writing a partial one.
 */
import { describe, expect, test } from "bun:test";
import { issueLifecycle, issues } from "../domain/declaration.ts";
import { decodeIssueRow, type IssueRow } from "../domain/issue.ts";
import type { JsonObject } from "../views/contracts.ts";
import { maintain, ReducerFault, touchedKeys } from "../views/engine.ts";

const created = (issueId: string, sequence: number, status = "backlog"): JsonObject => ({
  type: "IssueCreated",
  eventId: `e-${issueId}-${sequence}`,
  workspaceId: "main",
  issueId,
  sequence,
  occurredAt: "2026-08-24T10:00:00.000Z",
  title: `Issue ${issueId}`,
  projectId: "streamsy",
  status,
});

const moved = (issueId: string, sequence: number, status: string): JsonObject => ({
  type: "IssueStatusChanged",
  eventId: `e-${issueId}-${sequence}`,
  workspaceId: "main",
  issueId,
  sequence,
  occurredAt: `2026-08-24T10:0${sequence}:00.000Z`,
  status,
});

const run = (items: readonly JsonObject[], current: ReadonlyMap<string, IssueRow> = new Map()) =>
  maintain<IssueRow>({
    plan: issues.plan,
    reducer: issueLifecycle,
    decodeRow: decodeIssueRow,
    current,
    items,
  });

describe("maintain", () => {
  test("a creation enters one keyed row", () => {
    const result = run([created("a", 0)]);
    expect(result.changes).toHaveLength(1);
    expect(result.changes[0]?.kind).toBe("enter");
    expect(result.rows.get("a")?.status).toBe("backlog");
  });

  test("changes are coalesced to one per key, from the batch's entry state", () => {
    const result = run([created("a", 0), moved("a", 1, "todo"), moved("a", 2, "done")]);
    expect(result.changes).toHaveLength(1);
    expect(result.changes[0]?.kind).toBe("enter");
    expect(result.rows.get("a")?.status).toBe("done");
  });

  test("a later batch over prior state produces an update carrying the old row", () => {
    const first = run([created("a", 0)]);
    const second = run([moved("a", 1, "in_progress")], first.rows);
    const [change] = second.changes;
    expect(change?.kind).toBe("update");
    expect(change?.kind === "update" && change.before.status).toBe("backlog");
    expect(change?.kind === "update" && change.after.status).toBe("in_progress");
  });

  test("a fold that changes nothing emits no change", () => {
    const first = run([created("a", 0, "todo")]);
    const second = run([moved("a", 1, "todo")], first.rows);
    // `updatedAt` moves, so the row genuinely differs; the same event replayed
    // at the same instant does not.
    expect(second.changes).toHaveLength(1);
    const third = run([moved("a", 1, "todo")], second.rows);
    expect(third.changes).toHaveLength(0);
  });

  test("the declared source order decides the result, not arrival order", () => {
    const facts = [created("a", 0), moved("a", 1, "todo"), moved("a", 2, "done")];
    const forwards = run(facts);
    const backwards = run([...facts].reverse());
    expect(backwards.rows.get("a")).toEqual(forwards.rows.get("a"));
  });

  test("independent keys are maintained independently", () => {
    const result = run([created("a", 0), created("b", 1), moved("b", 2, "done")]);
    expect([...result.rows.keys()].sort()).toEqual(["a", "b"]);
    expect(result.rows.get("a")?.status).toBe("backlog");
    expect(result.rows.get("b")?.status).toBe("done");
  });

  test("a status change with no creation is a typed reducer fault", () => {
    expect(() => run([moved("ghost", 0, "done")])).toThrow(ReducerFault);
    try {
      run([moved("ghost", 0, "done")]);
    } catch (cause) {
      expect(cause).toBeInstanceOf(ReducerFault);
      // SAFETY: the assertion immediately above proves the instance type.
      // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
      expect((cause as ReducerFault).phase).toBe("decode");
    }
  });

  test("touchedKeys reports exactly the keys a batch can change", () => {
    expect(
      touchedKeys(issues.plan, [created("a", 0), created("b", 1), moved("a", 2, "done")]),
    ).toEqual(["a", "b"]);
  });
});
