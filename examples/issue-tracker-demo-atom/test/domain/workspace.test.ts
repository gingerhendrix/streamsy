import { Array, Option, Record } from "effect";
import { describe, expect, it } from "vitest";
import { Intent, toPendingWrite } from "../../src/domain/intent.ts";
import {
  applyChanges,
  applyReadEvent,
  commentsByIssue,
  emptyReplica,
  emptyRows,
  issueCountByProject,
  projectsOldestFirst,
  ReadEvent,
  selectedProject,
  statusCounts,
  unconfirmed,
  visibleIssues,
  workspaceView,
  Write,
} from "../../src/domain/workspace.ts";
import {
  byId,
  comment,
  ids,
  issue,
  project,
  stamp,
  upsertComment,
  upsertIssue,
  upsertProject,
} from "../support.ts";

describe("the stream fold", () => {
  it("applies upserts in stream order, so the last value wins", () => {
    const rows = applyChanges(emptyRows, [
      upsertProject(project),
      upsertIssue(issue("a")),
      upsertIssue(issue("a", { title: "Renamed" })),
      upsertComment(comment("c", "a", "2026-11-05T10:00:00.000Z")),
    ]);

    expect(rows.projects[project.id]).toEqual(project);
    expect(rows.issues["a"]?.title).toBe("Renamed");
    expect(rows.comments["c"]?.issueId).toBe("a");
  });

  it("removes a row on delete and ignores an upsert without a value", () => {
    const rows = applyChanges(emptyRows, [
      upsertIssue(issue("a")),
      upsertIssue(issue("b")),
      { type: "issue", key: "a", headers: { operation: "delete" } },
      { type: "issue", key: "b", headers: { operation: "upsert" } },
    ]);

    expect(Record.keys(rows.issues)).toEqual(["b"]);
  });

  it("collects each txid and turns live at the first up-to-date batch", () => {
    const first = applyReadEvent(
      emptyReplica,
      ReadEvent.Batch({ changes: [upsertIssue(issue("a"), "tx-1")], isUpToDate: false }),
    );

    const second = applyReadEvent(first, ReadEvent.Batch({ changes: [], isUpToDate: true }));

    expect(first.isLive).toBe(false);
    expect(second.isLive).toBe(true);
    expect([...second.confirmed]).toEqual(["tx-1"]);
  });

  it("keeps the rows when the read fails", () => {
    const live = applyReadEvent(
      emptyReplica,
      ReadEvent.Batch({ changes: [upsertIssue(issue("a"))], isUpToDate: true }),
    );

    const failed = applyReadEvent(live, ReadEvent.Failed({ error: "stopped" }));

    expect(failed.rows).toBe(live.rows);
    expect(failed.maybeError).toEqual(Option.some("stopped"));
  });
});

const rename = (txid: string, title: string, maybeWriter: Option.Option<string>) =>
  toPendingWrite(Intent.UpdateIssue({ issueId: "a", patch: { title } }), stamp(txid), maybeWriter);

describe("the overlay", () => {
  const rows = applyChanges(emptyRows, [
    upsertProject(project),
    upsertIssue(issue("a", { updatedBy: "Ada" })),
    upsertIssue(issue("b")),
  ]);

  it("marks only the rows that a pending write changes", () => {
    const view = workspaceView(rows, [rename("t1", "Local", Option.some("Bea"))]);

    const a = byId(view.issues, "a");
    const b = byId(view.issues, "b");

    expect(a).toMatchObject({ title: "Local", updatedBy: "Bea", $synced: false });
    expect(b).toMatchObject({ $synced: true });
  });

  it("clears updatedBy for a writer with no name, as the server does", () => {
    const view = workspaceView(rows, [rename("t1", "Local", Option.none())]);

    expect(byId(view.issues, "a")).not.toHaveProperty("updatedBy");
  });

  it("applies pending writes oldest first", () => {
    const view = workspaceView(rows, [
      rename("t1", "First", Option.none()),
      rename("t2", "Second", Option.none()),
    ]);

    expect(byId(view.issues, "a")?.title).toBe("Second");
  });

  it("shows a pending create and ignores an update of a missing row", () => {
    const created = toPendingWrite(
      Intent.CreateIssue({ projectId: project.id, title: "New" }),
      stamp("t1"),
      Option.some("Bea"),
    );

    const lost = {
      txid: "t2",
      write: Write.UpdateIssue({
        issueId: "gone",
        patch: { title: "x" },
        updatedAt: "2026-11-05T12:00:00.000Z",
        updatedBy: Option.none(),
      }),
    };

    const view = workspaceView(rows, [created, lost]);

    expect(view.issues).toHaveLength(3);
    expect(byId(view.issues, "issue_abcdef12")).toMatchObject({
      title: "New",
      updatedBy: "Bea",
      $synced: false,
    });
  });

  it("drops a write when the stream has shown its txid", () => {
    const writes = [rename("t1", "One", Option.none()), rename("t2", "Two", Option.none())];

    expect(Array.map(unconfirmed(writes, new Set(["t1"])), ({ txid }) => txid)).toEqual(["t2"]);
  });

  it("gives an anonymous comment the anonymous author", () => {
    const { write } = toPendingWrite(
      Intent.CreateComment({ issueId: "a", body: "Hello" }),
      stamp("t1"),
      Option.none(),
    );

    expect(write).toMatchObject({ comment: { author: "anonymous", issueId: "a" } });
  });
});

describe("derived lists", () => {
  const older = { ...project, id: "proj_old", createdAt: "2026-11-01T00:00:00.000Z" };
  const sameTimeA = { ...project, id: "proj_a", name: "Alpha" };
  const sameTimeB = { ...project, id: "proj_b", name: "Beta" };

  const view = workspaceView(
    applyChanges(emptyRows, [
      upsertProject(sameTimeB),
      upsertProject(sameTimeA),
      upsertProject(older),
      upsertIssue(issue("a", { updatedAt: "2026-11-05T01:00:00.000Z" })),
      upsertIssue(issue("b", { status: "done", updatedAt: "2026-11-05T03:00:00.000Z" })),
      upsertIssue(issue("c", { status: "done", updatedAt: "2026-11-05T02:00:00.000Z" })),
      upsertComment(comment("late", "a", "2026-11-05T02:00:00.000Z")),
      upsertComment(comment("early", "a", "2026-11-05T01:00:00.000Z")),
    ]),
    [],
  );

  it("sorts projects by time, then by name", () => {
    expect(ids(projectsOldestFirst(view))).toEqual(["proj_old", "proj_a", "proj_b"]);
  });

  it("falls back to the first project when the selection is missing", () => {
    const projects = projectsOldestFirst(view);

    expect(selectedProject(projects, Option.some("proj_b"))).toMatchObject({
      value: { id: "proj_b" },
    });
    expect(selectedProject(projects, Option.some("gone"))).toMatchObject({
      value: { id: "proj_old" },
    });
    expect(Option.isNone(selectedProject([], Option.none()))).toBe(true);
  });

  it("filters issues and sorts them by last update", () => {
    expect(ids(visibleIssues(view.issues, "all"))).toEqual(["b", "c", "a"]);
    expect(ids(visibleIssues(view.issues, "done"))).toEqual(["b", "c"]);
  });

  it("counts statuses and projects, and groups comments oldest first", () => {
    expect(statusCounts(view.issues)).toEqual({ all: 3, open: 1, in_progress: 0, done: 2 });
    expect(issueCountByProject(view).get(project.id)).toBe(3);
    expect(ids(commentsByIssue(view).get("a") ?? [])).toEqual(["early", "late"]);
  });
});
