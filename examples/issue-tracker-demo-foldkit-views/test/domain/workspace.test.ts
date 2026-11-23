import { Array, Option, Record } from "effect";
import { describe, expect, test } from "vitest";
import { Intent, toPendingWrite } from "../../src/domain/intent.ts";
import {
  applyChanges,
  commentsOn,
  confirmedTxids,
  emptyRows,
  issuesIn,
  statusCounts,
  visibleIssues,
  workspaceView,
} from "../../src/domain/workspace.ts";
import {
  comment,
  issue,
  project,
  stamp,
  upsertComment,
  upsertIssue,
  upsertProject,
} from "../support.ts";

const rows = applyChanges(emptyRows, [
  upsertProject(project),
  upsertIssue(issue("issue_a", { status: "in_progress", updatedAt: "2026-10-25T10:01:00.000Z" })),
  upsertIssue(issue("issue_b", { updatedAt: "2026-10-25T10:03:00.000Z" })),
  upsertIssue(issue("issue_c", { status: "done", updatedAt: "2026-10-25T10:02:00.000Z" })),
]);

const titles = (shown: ReadonlyArray<{ readonly row: { readonly title: string } }>) =>
  Array.map(shown, ({ row }) => row.title);

describe("applyChanges", () => {
  test("upserts rows by key, and a later upsert replaces the row", () => {
    const next = applyChanges(rows, [upsertIssue(issue("issue_a", { title: "Renamed" }))]);

    expect(next.issues["issue_a"]?.title).toBe("Renamed");
    expect(Record.keys(next.issues)).toHaveLength(3);
  });

  test("a delete removes the row", () => {
    const next = applyChanges(rows, [
      { type: "issue", key: "issue_b", headers: { operation: "delete" } },
    ]);

    expect(next.issues["issue_b"]).toBeUndefined();
  });
});

describe("confirmedTxids", () => {
  test("lists each txid once and skips changes without one", () => {
    const txids = confirmedTxids([
      upsertIssue(issue("issue_a"), "tx-1"),
      upsertComment(comment("comment_a", "issue_a", "2026-10-25T10:00:00.000Z"), "tx-1"),
      upsertIssue(issue("issue_b")),
    ]);

    expect(txids).toEqual(["tx-1"]);
  });
});

describe("derived lists (A2)", () => {
  const inProject = issuesIn(workspaceView(rows, []), project.id);

  test("statusCounts counts each status and all issues", () => {
    expect(statusCounts(inProject)).toEqual({ all: 3, open: 1, in_progress: 1, done: 1 });
  });

  test("visibleIssues filters by status and sorts by last update, newest first", () => {
    expect(titles(visibleIssues(inProject, "all"))).toEqual([
      "Issue issue_b",
      "Issue issue_c",
      "Issue issue_a",
    ]);
    expect(titles(visibleIssues(inProject, "done"))).toEqual(["Issue issue_c"]);
  });

  test("commentsOn returns one issue's comments, oldest first", () => {
    const withComments = applyChanges(rows, [
      upsertComment(comment("comment_late", "issue_a", "2026-10-25T10:05:00.000Z")),
      upsertComment(comment("comment_early", "issue_a", "2026-10-25T10:04:00.000Z")),
      upsertComment(comment("comment_other", "issue_b", "2026-10-25T10:00:00.000Z")),
    ]);

    const ids = Array.map(
      commentsOn(workspaceView(withComments, []), "issue_a"),
      ({ row }) => row.id,
    );

    expect(ids).toEqual(["comment_early", "comment_late"]);
  });
});

describe("workspaceView (A4)", () => {
  const rename = toPendingWrite(
    Intent.UpdateIssue({ issueId: "issue_a", patch: { status: "done" } }),
    stamp("tx-rename"),
    Option.some("Ada"),
  );

  const create = toPendingWrite(
    Intent.CreateIssue({ projectId: project.id, title: "Local" }),
    stamp("tx-create"),
    Option.none(),
  );

  test("lays pending writes over the stream rows and marks those rows pending", () => {
    const view = workspaceView(rows, [rename, create]);
    const byId = new Map(Array.map(view.issues, (shown) => [shown.row.id, shown] as const));

    expect(byId.get("issue_a")).toMatchObject({
      isPending: true,
      row: { status: "done", updatedBy: "Ada", updatedAt: "2026-10-25T12:00:00.000Z" },
    });
    expect(byId.get("issue_abcdef12")).toMatchObject({ isPending: true, row: { title: "Local" } });
    expect(byId.get("issue_b")?.isPending).toBe(false);
  });

  test("without the pending write, the stream row shows again", () => {
    const view = workspaceView(rows, []);

    expect(Array.findFirst(view.issues, ({ row }) => row.id === "issue_a")).toEqual(
      Option.some({ row: rows.issues["issue_a"], isPending: false }),
    );
  });

  test("an edit of a missing issue changes nothing", () => {
    const missing = toPendingWrite(
      Intent.UpdateIssue({ issueId: "issue_gone", patch: { title: "x" } }),
      stamp("tx-gone"),
      Option.none(),
    );

    expect(workspaceView(rows, [missing]).issues).toHaveLength(3);
  });
});

describe("toPendingWrite", () => {
  test("an unnamed writer gives an anonymous comment and no updatedBy", () => {
    const pending = toPendingWrite(
      Intent.CreateComment({ issueId: "issue_a", body: "Hi" }),
      stamp("tx-comment"),
      Option.none(),
    );

    expect(pending).toEqual({
      txid: "tx-comment",
      write: {
        _tag: "CreateComment",
        comment: {
          id: "comment_abcdef12",
          issueId: "issue_a",
          author: "anonymous",
          body: "Hi",
          createdAt: "2026-10-25T12:00:00.000Z",
        },
      },
    });
  });
});
