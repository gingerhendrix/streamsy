import { describe, expect, test } from "bun:test";
import { issueAfter, WorkspaceEvent } from "../../server/events.ts";
import { createdAt, later } from "../support.ts";

const { IssueCreated, IssueRenamed, IssueStatusChanged } = WorkspaceEvent.cases;
const txid = "00000000-0000-4000-8000-000000000000";
const created = IssueCreated.make({
  issueId: "issue_a",
  projectId: "proj_a",
  title: "First",
  status: "open",
  updatedBy: "Ann",
  at: createdAt,
  txid,
});

describe("issueAfter", () => {
  test("a created issue takes both times from the event and names its writer", () => {
    expect(issueAfter(undefined, created)).toEqual({
      id: "issue_a",
      projectId: "proj_a",
      title: "First",
      status: "open",
      createdAt,
      updatedAt: createdAt,
      updatedBy: "Ann",
    });
  });

  test("a rename keeps the other fields and moves updatedAt", () => {
    const before = issueAfter(undefined, created);
    const renamed = IssueRenamed.make({
      issueId: "issue_a",
      title: "Second",
      updatedBy: "Bo",
      at: later,
      txid,
    });
    expect(issueAfter(before, renamed)).toMatchObject({
      title: "Second",
      status: "open",
      createdAt,
      updatedAt: later,
      updatedBy: "Bo",
    });
  });

  test("an edit with no writer drops the previous writer, as the base app does", () => {
    const before = issueAfter(undefined, created);
    const moved = IssueStatusChanged.make({ issueId: "issue_a", status: "done", at: later, txid });
    const after = issueAfter(before, moved);
    expect(after).toMatchObject({ status: "done", updatedAt: later });
    expect(after).not.toHaveProperty("updatedBy");
  });

  test("an edit of an issue that the rows do not hold gives no row", () => {
    const moved = IssueStatusChanged.make({ issueId: "issue_x", status: "done", at: later, txid });
    expect(issueAfter(undefined, moved)).toBeUndefined();
  });

  test("events round-trip through the event stream schema", () => {
    const encoded = JSON.parse(JSON.stringify(created));
    expect(encoded).toMatchObject({ _tag: "IssueCreated", at: createdAt, txid });
  });
});
