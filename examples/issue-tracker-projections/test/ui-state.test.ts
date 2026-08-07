/**
 * Browser-side state rules.
 *
 * These cover the two places the UI could lie: folding durable State streams,
 * and holding optimistic overlays longer than the durable row justifies.
 */
import { describe, expect, it } from "vitest";
import {
  initials,
  projectKeyFrom,
  relativeTime,
  shortPosition,
  shortStream,
} from "../src/lib/format.ts";
import {
  activeOverlays,
  cardSync,
  classifySettlement,
  latestFailure,
  matchesPatch,
  overlayRows,
  syncSummary,
  OVERLAY_HOLD_MS,
  SETTLED_DISPLAY_MS,
  type Mutation,
} from "../src/lib/pending.ts";
import { foldBoardRows, foldProjects, sortBoardRows } from "../src/lib/state.ts";
import { isExpectedTeardown, streamUrl } from "../src/lib/stream.ts";
import type { CoverageReport, ProjectionPassReport } from "../shared/api.ts";
import type { BoardRow } from "../shared/model.ts";

const row = (overrides: Partial<BoardRow> = {}): BoardRow => ({
  issueId: "issue-1",
  issueKey: "SHIP-100",
  title: "Ship it",
  status: "backlog",
  priority: "medium",
  assigneeId: "ada",
  commentCount: 0,
  updatedAt: "2026-01-01T00:00:00.000Z",
  ...overrides,
});

const upsert = (value: BoardRow) => ({
  type: "board-issue",
  key: value.issueId,
  value,
  headers: { operation: "upsert" },
});

describe("State stream folding", () => {
  it("upserts and deletes board rows by key", () => {
    const first = foldBoardRows(new Map(), [upsert(row())]);
    expect(first.get("issue-1")?.title).toBe("Ship it");

    const renamed = foldBoardRows(first, [upsert(row({ title: "Ship it faster" }))]);
    expect(renamed.get("issue-1")?.title).toBe("Ship it faster");

    const removed = foldBoardRows(renamed, [
      { type: "board-issue", key: "issue-1", headers: { operation: "delete" } },
    ]);
    expect(removed.size).toBe(0);
  });

  it("ignores reserved framework lineage rows in the same stream", () => {
    const folded = foldBoardRows(new Map(), [
      { type: "__streamsy.mesh.fan-in.checkpoint.v1", key: "checkpoint", value: { some: "meta" } },
      { type: "__streamsy.mesh.fan-in.member.v1", key: "member:x", value: { through: "1" } },
      upsert(row()),
    ]);
    expect([...folded.keys()]).toEqual(["issue-1"]);
  });

  it("keeps the previous map when nothing in the batch belongs to the collection", () => {
    const previous = foldBoardRows(new Map(), [upsert(row())]);
    expect(foldBoardRows(previous, [{ type: "project", key: "p", value: {} }])).toBe(previous);
  });

  it("folds project rows from the workspace projects stream", () => {
    const projects = foldProjects(new Map(), [
      {
        type: "project",
        key: "launch",
        value: { projectId: "launch", projectKey: "SHIP", name: "Launch" },
        headers: { operation: "upsert" },
      },
    ]);
    expect(projects.get("launch")?.name).toBe("Launch");
  });

  it("sorts a column by recency then by issue key", () => {
    const sorted = sortBoardRows([
      row({ issueId: "a", issueKey: "SHIP-101", updatedAt: "2026-01-01T00:00:00.000Z" }),
      row({ issueId: "b", issueKey: "SHIP-102", updatedAt: "2026-01-02T00:00:00.000Z" }),
      row({ issueId: "c", issueKey: "SHIP-100", updatedAt: "2026-01-02T00:00:00.000Z" }),
    ]);
    expect(sorted.map((entry) => entry.issueId)).toEqual(["c", "b", "a"]);
  });
});

const mutation = (overrides: Partial<Mutation> = {}): Mutation => ({
  commandId: "cmd-1",
  issueId: "issue-1",
  projectId: "launch",
  label: "Move SHIP-100 to Done",
  phase: "syncing",
  patch: { status: "done" },
  startedAt: 1_000,
  ...overrides,
});

describe("optimistic overlays", () => {
  it("paints a pending patch over the durable row", () => {
    const rows = new Map([["issue-1", row()]]);
    const [painted] = overlayRows(rows, [mutation()]);
    expect(painted?.status).toBe("done");
  });

  it("inserts a placeholder card for an issue with no durable row yet", () => {
    const inserted = overlayRows(new Map(), [
      mutation({ issueId: "issue-new", insert: row({ issueId: "issue-new", issueKey: "NEW" }) }),
    ]);
    expect(inserted).toHaveLength(1);
    expect(inserted[0]?.issueKey).toBe("NEW");
  });

  it("keeps a matched overlay only until Synced has been legible", () => {
    const durable = new Map([["issue-1", row({ status: "done" })]]);
    const settled = mutation({ phase: "synced", settledAt: 2_000 });
    expect(activeOverlays([settled], durable, 2_100)).toHaveLength(1);
    expect(activeOverlays([settled], durable, 2_000 + SETTLED_DISPLAY_MS + 1)).toHaveLength(0);
  });

  it("holds an unmatched overlay only until the bounded deadline", () => {
    const stale = new Map([["issue-1", row({ status: "backlog" })]]);
    const settled = mutation({ phase: "synced", settledAt: 2_000 });
    expect(activeOverlays([settled], stale, 2_000 + SETTLED_DISPLAY_MS + 1)).toHaveLength(1);
    expect(activeOverlays([settled], stale, 2_000 + OVERLAY_HOLD_MS + 1)).toHaveLength(0);
  });

  it("keeps a failed overlay until it is retried", () => {
    const durable = new Map([["issue-1", row({ status: "done" })]]);
    const failed = mutation({ phase: "failed", settledAt: 2_000, error: "boom" });
    expect(activeOverlays([failed], durable, 9_999_999)).toHaveLength(1);
  });

  it("treats a comment count as an at-least match", () => {
    expect(matchesPatch(row({ commentCount: 2 }), { commentCount: 2 })).toBe(true);
    expect(matchesPatch(row({ commentCount: 1 }), { commentCount: 2 })).toBe(false);
  });

  it("reports failure over progress on one card", () => {
    const overlays = [mutation(), mutation({ commandId: "cmd-2", phase: "failed" })];
    expect(cardSync("issue-1", overlays)).toBe("failed");
    expect(cardSync("other", overlays)).toBe("idle");
  });

  it("never lets a settled overlay hide an unproven one on the same card", () => {
    const overlays = [
      mutation({ phase: "synced" }),
      mutation({ commandId: "b", phase: "pending" }),
    ];
    expect(cardSync("issue-1", overlays)).toBe("pending");
  });

  it("holds a pending overlay indefinitely: it is accepted but unproven", () => {
    const durable = new Map([["issue-1", row({ status: "done" })]]);
    const pending = mutation({ phase: "pending", settledAt: 2_000 });
    expect(activeOverlays([pending], durable, 9_999_999)).toHaveLength(1);
  });

  it("summarises the workspace sync state", () => {
    expect(syncSummary([]).state).toBe("idle");
    expect(syncSummary([mutation()])).toEqual({ state: "syncing", count: 1 });
    expect(syncSummary([mutation({ phase: "synced" })]).state).toBe("synced");
    expect(syncSummary([mutation({ phase: "pending" })])).toEqual({ state: "pending", count: 1 });
    expect(syncSummary([mutation({ phase: "failed" })]).state).toBe("failed");
    expect(
      syncSummary([mutation({ phase: "synced" }), mutation({ commandId: "b", phase: "pending" })])
        .state,
    ).toBe("pending");
  });
});

const coverage = (overrides: Partial<CoverageReport> = {}): CoverageReport => ({
  status: "proven",
  ack: { stream: "workspaces/main/issues/issue-1/events", position: "8_0" },
  hops: [],
  ...overrides,
});

const pass = (overrides: Partial<ProjectionPassReport> = {}): ProjectionPassReport => ({
  label: "issue-detail",
  status: "caught-up",
  outcome: "caught-up",
  ...overrides,
});

describe("the Synced law", () => {
  it("only calls a command synced when coverage is proven and no pass faulted", () => {
    const settled = classifySettlement("Move SHIP-100 to Done", {
      coverage: coverage(),
      projections: [pass(), pass({ label: "project-board" })],
    });
    expect(settled.phase).toBe("synced");
  });

  it("keeps not-yet and incomparable coverage pending, never synced", () => {
    for (const status of ["not-yet", "incomparable"] as const) {
      const settled = classifySettlement("Move SHIP-100 to Done", {
        coverage: coverage({ status, blockedAt: "project-board" }),
        projections: [pass(), pass({ label: "project-board" })],
      });
      expect(settled.phase).toBe("pending");
      expect(settled.message).toContain(status);
    }
  });

  it("fails a command whose projection faulted, even when coverage reads proven", () => {
    for (const faulted of [
      pass({ label: "project-board", status: "output-conflict", outcome: "faulted" }),
      pass({ label: "project-board", status: "unknown-member", outcome: "faulted" }),
      pass({ label: "issue-detail", status: "StateRestorePoison", outcome: "faulted" }),
    ]) {
      const settled = classifySettlement("Move SHIP-100 to Done", {
        coverage: coverage(),
        projections: [faulted],
      });
      expect(settled.phase).toBe("failed");
      expect(settled.message).toContain(faulted.status);
    }
  });

  it("keeps an exhausted bounded limit pending rather than synced", () => {
    const settled = classifySettlement("Move SHIP-100 to Done", {
      coverage: coverage({ status: "not-yet", blockedAt: "project-board" }),
      projections: [pass({ status: "limit-reached", outcome: "deferred" })],
    });
    expect(settled.phase).toBe("pending");
  });
});

describe("retrying a failed command", () => {
  it("returns the newest failure for the issue, not the oldest command", () => {
    // `activeOverlays` preserves the newest-first mutation order.
    const overlays = [
      mutation({ commandId: "cmd-new", phase: "failed", error: "newest" }),
      mutation({ commandId: "cmd-old", phase: "failed", error: "oldest" }),
    ];
    expect(latestFailure("issue-1", overlays)).toEqual({
      commandId: "cmd-new",
      message: "newest",
    });
  });

  it("ignores non-failed and other-issue mutations", () => {
    const overlays = [
      mutation({ commandId: "cmd-other", issueId: "issue-2", phase: "failed" }),
      mutation({ commandId: "cmd-pending", phase: "pending" }),
      mutation({ commandId: "cmd-failed", phase: "failed", error: "boom" }),
    ];
    expect(latestFailure("issue-1", overlays)?.commandId).toBe("cmd-failed");
    expect(latestFailure("issue-3", overlays)).toBeUndefined();
  });
});

describe("stream tail retry noise", () => {
  it("stays silent for an aborted subscription or a page navigation", () => {
    const failure = new TypeError("Failed to fetch");
    expect(isExpectedTeardown(failure, { aborted: true, navigating: false })).toBe(true);
    expect(isExpectedTeardown(failure, { aborted: false, navigating: true })).toBe(true);
    const aborted = new Error("aborted");
    aborted.name = "AbortError";
    expect(isExpectedTeardown(aborted, { aborted: false, navigating: false })).toBe(true);
  });

  it("still reports a real mid-session read failure", () => {
    expect(
      isExpectedTeardown(new TypeError("Failed to fetch"), {
        aborted: false,
        navigating: false,
      }),
    ).toBe(false);
  });
});

describe("display helpers", () => {
  it("encodes stream paths without collapsing the segments", () => {
    const url = streamUrl("workspaces/main/projects/launch/board", { offset: "-1" });
    expect(url).toContain("/streams/workspaces/main/projects/launch/board?offset=-1");
  });

  it("formats relative times with stable thresholds", () => {
    const base = Date.parse("2026-01-02T00:00:00.000Z");
    expect(relativeTime("2026-01-02T00:00:00.000Z", base)).toBe("just now");
    expect(relativeTime("2026-01-01T23:50:00.000Z", base)).toBe("10m ago");
    expect(relativeTime("2026-01-01T20:00:00.000Z", base)).toBe("4h ago");
    expect(relativeTime("2025-12-30T00:00:00.000Z", base)).toBe("3d ago");
    expect(relativeTime("not-a-date", base)).toBe("unknown");
  });

  it("labels an unassigned issue without inventing a member", () => {
    expect(initials(null)).toBe("–");
    expect(initials("ada")).toBe("AD");
  });

  it("shortens durable positions and stream names for dense rows", () => {
    expect(shortPosition(null)).toBe("—");
    expect(shortPosition("0000000000000042_0000000000000003")).toBe("42_3");
    expect(shortPosition("0000000000000000_0000000000000000")).toBe("0_0");
    expect(shortStream("workspaces/main/projects/launch/board")).toBe("…/projects/launch/board");
  });

  it("derives a stream-safe project key from a name", () => {
    expect(projectKeyFrom("Launch")).toBe("LAUN");
    expect(projectKeyFrom("Q1")).toBe("Q1PR");
    expect(projectKeyFrom("data platform")).toBe("DATA");
  });
});
